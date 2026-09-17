// server.js
// Backend that SimplAI's function calls hit as webhooks, and that pushes
// real-time cobrowsing events to the retailer's webpage over Socket.io.
//
// npm i express socket.io ioredis @socket.io/redis-adapter pg

const express = require('express');
const http = require('http');
const { randomUUID } = require('crypto');
const { Server } = require('socket.io');
const { createAdapter } = require('@socket.io/redis-adapter');
const Redis = require('ioredis');
const { Pool } = require('pg');

const app = express();
app.use(express.json());
app.use(express.static('public')); // serves public/index.html at "/"

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: process.env.ALLOWED_ORIGIN || '*' } // set ALLOWED_ORIGIN in prod
});

// --- Redis: reuse your existing instance, separate key prefix from BullMQ ---
const redisUrl = process.env.REDIS_URL;
const redisOpts = {
  retryStrategy: (times) => (times > 10 ? null : Math.min(times * 200, 5000)), // stop after 10 tries
  maxRetriesPerRequest: 3,
};
const pubClient = new Redis(redisUrl, redisOpts);
const subClient = pubClient.duplicate();

pubClient.on('error', (err) => console.error('redis pubClient error:', err.message));
subClient.on('error', (err) => console.error('redis subClient error:', err.message));

io.adapter(createAdapter(pubClient, subClient));

// Postgres: source of truth for cart + order history
const pg = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});
pg.on('error', (err) => console.error('pg pool error:', err.message));

// --- Session lifecycle ---
// A session is live for a fixed 30 minutes from creation, regardless of
// activity (not a sliding/rolling window). Once checkout completes, the
// session is marked 'completed' immediately, independent of the TTL, so
// the link stops working right away rather than waiting out the clock.
const SESSION_TTL_MS = 30 * 60 * 1000;
const COMPLETED_RETENTION_MS = 10 * 60 * 1000; // how long a completed session
// still resolves (so the page that just checked out can reconnect and see
// "order placed" instead of a broken link) before it's gone entirely.

const sessionKey = (id) => `session:${id}:state`;

async function getState(sessionId) {
  const raw = await pubClient.get(sessionKey(sessionId));
  return raw ? JSON.parse(raw) : null; // null = expired or never existed
}

// Writes state with a TTL derived from state.expiresAt, so the original
// 30-minute wall-clock deadline is preserved rather than reset on every write.
async function setState(sessionId, state) {
  const ttlSeconds = Math.max(1, Math.ceil((state.expiresAt - Date.now()) / 1000));
  await pubClient.set(sessionKey(sessionId), JSON.stringify(state), 'EX', ttlSeconds);
}

function persistToPostgres(sessionId, state) {
  // Fire-and-forget — don't block the realtime path on this.
  pg.query(
    `insert into voice_sessions (session_id, retailer_id, cart, updated_at)
     values ($1, $2, $3, now())
     on conflict (session_id) do update set cart = $3, updated_at = now()`,
    [sessionId, state.retailerId, state.cart]
  ).catch((err) => console.error('postgres persist failed', sessionId, err));
}

// --- Socket.io: one room per session_id ---
io.on('connection', (socket) => {
  const { session_id } = socket.handshake.query;
  if (!session_id) return socket.disconnect();
  socket.join(session_id);
});

// Wraps an async route handler so a thrown/rejected error becomes a JSON
// error response instead of an unhandled rejection that crashes the process.
// Honors err.statusCode if the handler set one (e.g. 404, 409).
function ah(fn) {
  return (req, res) => {
    Promise.resolve(fn(req, res)).catch((err) => {
      console.error(`${req.method} ${req.path} failed:`, err.message);
      if (!res.headersSent) {
        res.status(err.statusCode || 500).json({ ok: false, error: err.message });
      }
    });
  };
}

// Loads state and enforces that the session is still open for new actions:
// throws 404 if expired/never existed, 409 if already checked out. Used by
// every webhook that MUTATES state. The plain read endpoint (GET .../state)
// does NOT use this — it needs to still return a 'completed' session so the
// page can render the locked/placed screen instead of a raw error.
async function requireActiveSession(sessionId) {
  const state = await getState(sessionId);
  if (!state) {
    const err = new Error('This session has expired or does not exist.');
    err.statusCode = 404;
    throw err;
  }
  if (state.status === 'completed') {
    const err = new Error('This order has already been placed. The link is no longer active.');
    err.statusCode = 409;
    throw err;
  }
  return state;
}

// --- REST endpoint the webpage calls on load/reconnect to rehydrate ---
app.get('/session/:id/state', ah(async (req, res) => {
  const state = await getState(req.params.id);
  if (!state) {
    return res.status(404).json({ ok: false, error: 'This session has expired or does not exist.' });
  }
  res.json({ ok: true, ...state });
}));

// --- Webhooks SimplAI's function-calling layer hits during the call ---

app.post('/webhook/add-to-cart', ah(async (req, res) => {
  const { session_id, sku, product_name, price, qty, image_url } = req.body;
  const state = await requireActiveSession(session_id);

  const existing = state.cart[sku];
  state.cart[sku] = {
    product_name,
    price,
    image_url: image_url || existing?.image_url,
    qty: (existing?.qty || 0) + qty,
  };
  await setState(session_id, state);
  persistToPostgres(session_id, state);

  io.to(session_id).emit('cart:update', { cart: state.cart, changed_sku: sku });
  res.json({ ok: true });
}));

app.post('/webhook/show-products', ah(async (req, res) => {
  // Adds new products to the catalog shown on the page — distinct from
  // spotlight, which just highlights something already there. Use this
  // when the bot says "let me also show you..." for items not in the
  // original seeded list.
  const { session_id, products } = req.body; // products: [{id, name, price, image_url}]
  const state = await requireActiveSession(session_id);

  const existingIds = new Set(state.products.map((p) => p.id));
  const newOnes = products.filter((p) => !existingIds.has(p.id));
  state.products = [...state.products, ...newOnes];
  await setState(session_id, state);

  io.to(session_id).emit('products:update', { products: newOnes });
  res.json({ ok: true });
}));

app.post('/webhook/spotlight', ah(async (req, res) => {
  // Pure UI directive — no state change. Fired whenever the bot says
  // "let me show you..." so the page visibly reacts to speech.
  const { session_id, product_id } = req.body;
  await requireActiveSession(session_id); // still validate — no spotlighting a dead session
  io.to(session_id).emit('spotlight', { product_id });
  res.json({ ok: true });
}));

app.post('/webhook/show-offer', ah(async (req, res) => {
  const { session_id, offer } = req.body; // { id, title, discount, sku }
  const state = await requireActiveSession(session_id);
  state.offersShown.push(offer.id);
  await setState(session_id, state);

  io.to(session_id).emit('offer:show', { offer });
  res.json({ ok: true });
}));

app.post('/webhook/checkout', ah(async (req, res) => {
  const { session_id } = req.body;
  const state = await requireActiveSession(session_id);

  // Replace with your real order-creation call (Flipkart Wholesale order API, etc.)
  const orderId = `ORD-${Date.now()}`;
  await pg.query(
    `insert into orders (order_id, session_id, retailer_id, cart, status)
     values ($1, $2, $3, $4, 'placed')`,
    [orderId, session_id, state.retailerId, state.cart]
  );

  // Lock the session immediately — link stops working from this point on,
  // independent of the original 30-minute TTL. Kept around briefly (not
  // deleted outright) so the page that just checked out can still resolve
  // its own state as 'completed' rather than getting a bare 404.
  state.status = 'completed';
  state.orderId = orderId;
  state.expiresAt = Date.now() + COMPLETED_RETENTION_MS;
  await setState(session_id, state);

  io.to(session_id).emit('checkout:complete', { order_id: orderId });
  res.json({ ok: true, order_id: orderId });
}));

// --- Session creation, called BEFORE the call is placed / link is sent ---
// Whatever triggers the call (your dialer, CRM, campaign job) should call
// this first: build seed_products from that retailer's purchase history or
// recommendation logic, then pass them here so the page has content the
// instant it's opened — not just an empty cart waiting for the bot to act.
// session_id is generated here, not supplied by the caller. The session is
// valid for SESSION_TTL_MS from this moment on.
app.post('/session', ah(async (req, res) => {
  const { retailer_id, seed_cart, seed_products } = req.body;
  const session_id = randomUUID();
  const state = {
    retailerId: retailer_id,
    cart: seed_cart || {},
    offersShown: [],
    products: seed_products || [], // [{id, name, price, image_url}]
    status: 'active',
    expiresAt: Date.now() + SESSION_TTL_MS,
  };
  await setState(session_id, state);

  const baseUrl = process.env.PUBLIC_URL || `${req.protocol}://${req.get('host')}`;
  res.json({ ok: true, session_id, url: `${baseUrl}/?s=${session_id}`, expires_at: state.expiresAt });
}));

server.listen(process.env.PORT || 3000);
