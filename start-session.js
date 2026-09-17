// start-session.js
// Run this BEFORE placing the call. It:
//   1. Looks up what this retailer is likely to want (purchase history)
//   2. Creates the session with that seeded catalog
//   3. Sends the link over WhatsApp
//   4. Triggers the outbound call with session_id attached as call context
//
// Usage: node start-session.js <retailer_id> <phone_number>

const { Pool } = require('pg');

const pg = new Pool({ connectionString: process.env.DATABASE_URL });
const BACKEND_URL = process.env.BACKEND_URL; // e.g. https://voice-cobrowse-rgc3.onrender.com

// --- 1. Build the seed product list for this retailer ---
async function getRecommendedProducts(retailerId) {
  // Simplest version: their last order's SKUs. Swap this for whatever
  // recommendation logic you actually have (frequently-bought, trending
  // in their category, restock-due items, etc.) — this is just the shape.
  const { rows } = await pg.query(
    `select sku, product_name, price
     from order_items oi
     join orders o on o.order_id = oi.order_id
     where o.retailer_id = $1
     order by o.created_at desc
     limit 10`,
    [retailerId]
  );

  return rows.map((r) => ({ id: r.sku, name: r.product_name, price: r.price }));
}

// --- 2. Create the session on your backend — session_id is generated server-side ---
async function createSession(retailerId, seedProducts) {
  const res = await fetch(`${BACKEND_URL}/session`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      retailer_id: retailerId,
      seed_cart: {},
      seed_products: seedProducts,
    }),
  });
  return res.json(); // { ok: true, session_id, url }
}

// --- 3. Send the link over WhatsApp (via your existing Karix messaging setup) ---
async function sendLinkOverWhatsApp(phone, url) {
  // Reuse whatever template-send function/endpoint you already have for
  // WhatsApp CTA messages. Sketch of the shape:
  //
  // await fetch('https://api.karix.io/v1/messages/whatsapp', {
  //   method: 'POST',
  //   headers: { Authorization: `Bearer ${process.env.KARIX_API_KEY}` },
  //   body: JSON.stringify({
  //     to: phone,
  //     template: 'order_link_template',
  //     variables: { link: url },
  //   }),
  // });
  console.log(`send WhatsApp to ${phone}: ${url}`);
}

// --- 4. Trigger the outbound call with session_id attached ---
async function triggerCall(phone, sessionId, retailerId) {
  // Sketch — replace with SimplAI's actual outbound-call trigger endpoint.
  // The critical part: session_id must be passed as call-level context/
  // metadata here so every tool call during the conversation carries it
  // automatically, rather than relying on the LLM to mention it correctly.
  //
  // await fetch('https://api.simplai.ai/v1/calls', {
  //   method: 'POST',
  //   headers: { Authorization: `Bearer ${process.env.SIMPLAI_API_KEY}` },
  //   body: JSON.stringify({
  //     to: phone,
  //     agent_id: process.env.SIMPLAI_AGENT_ID,
  //     metadata: { session_id: sessionId, retailer_id: retailerId },
  //   }),
  // });
  console.log(`trigger call to ${phone} with session_id=${sessionId}`);
}

// --- Orchestration ---
async function startSession(retailerId, phone) {
  const seedProducts = await getRecommendedProducts(retailerId);

  const { session_id: sessionId, url } = await createSession(retailerId, seedProducts);

  await sendLinkOverWhatsApp(phone, url);
  await triggerCall(phone, sessionId, retailerId);

  return { sessionId, url };
}

if (require.main === module) {
  const [retailerId, phone] = process.argv.slice(2);
  startSession(retailerId, phone)
    .then((r) => console.log('session started:', r))
    .catch((err) => console.error('failed:', err));
}

module.exports = { startSession, getRecommendedProducts };
