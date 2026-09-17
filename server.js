// server.js
// Backend that SimplAI's function calls hit as webhooks, and that pushes
// real-time cobrowsing events to the retailer's webpage over Socket.io.
//
// npm i express socket.io ioredis @socket.io/redis-adapter pg

const express = require('express');
const http = require('http');
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
const pubClient = new Redis(redisUrl);
const subClient = pubClient.duplicate();
io.adapter(createAdapter(pubClient, subClient));

// Postgres: source of truth for cart + order history
const pg = new Pool({ connectionString: process.env.DATABASE_URL });

// Session state lives in Redis for fast reads; Postgres write is fire-and-forget
// so the socket emit never waits on a DB round trip.
const sessionKey = (id) => `session:${id}:state`;

async function getState(sessionId) {
  const raw = await pubClient.get(sessionKey(sessionId));
  return raw ? JSON.parse(raw) : { cart: {}, offersShown: [], retailerId: null };
}

async function setState(sessionId, state) {
  await pubClient.set(sessionKey(sessionId), JSON.stringify(state), 'EX', 60 * 60 * 6); // 6h TTL
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

// --- REST endpoint the webpage calls on load/reconnect to rehydrate ---
app.get('/session/:id/state', async (req, res) => {
  const state = await getState(req.params.id);
  res.json(state);
});

// --- Webhooks SimplAI's function-calling layer hits during the call ---

app.post('/webhook/add-to-cart', async (req, res) => {
  const { session_id, sku, product_name, price, qty } = req.body;
  const state = await getState(session_id);

  state.cart[sku] = { product_name, price, qty: (state.cart[sku]?.qty || 0) + qty };
  await setState(session_id, state);
  persistToPostgres(session_id, state);

  io.to(session_id).emit('cart:update', { cart: state.cart, changed_sku: sku });
  res.json({ ok: true });
});

app.post('/webhook/spotlight', async (req, res) => {
  // Pure UI directive — no state change. Fired whenever the bot says
  // "let me show you..." so the page visibly reacts to speech.
  const { session_id, product_id } = req.body;
  io.to(session_id).emit('spotlight', { product_id });
  res.json({ ok: true });
});

app.post('/webhook/show-offer', async (req, res) => {
  const { session_id, offer } = req.body; // { id, title, discount, sku }
  const state = await getState(session_id);
  state.offersShown.push(offer.id);
  await setState(session_id, state);

  io.to(session_id).emit('offer:show', { offer });
  res.json({ ok: true });
});

app.post('/webhook/checkout', async (req, res) => {
  const { session_id } = req.body;
  const state = await getState(session_id);

  // Replace with your real order-creation call (Flipkart Wholesale order API, etc.)
  const orderId = `ORD-${Date.now()}`;
  await pg.query(
    `insert into orders (order_id, session_id, retailer_id, cart, status)
     values ($1, $2, $3, $4, 'placed')`,
    [orderId, session_id, state.retailerId, state.cart]
  );

  io.to(session_id).emit('checkout:complete', { order_id: orderId });
  res.json({ ok: true, order_id: orderId });
});

// --- Session creation, called when the call starts / retailer is identified ---
app.post('/session', async (req, res) => {
  const { session_id, retailer_id, seed_cart } = req.body;
  const state = { retailerId: retailer_id, cart: seed_cart || {}, offersShown: [] };
  await setState(session_id, state);
  res.json({ ok: true, url: `https://order.karixforge.in/s/${session_id}` });
});

server.listen(process.env.PORT || 3000);
