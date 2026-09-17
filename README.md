# Voice cobrowse

Real-time cobrowsing webpage driven by voice-bot function calls (SimplAI).
The bot calls webhooks on this backend; the backend pushes Socket.io events
into a per-session room so the retailer's webpage reacts live — cart updates,
spotlighted products, offers, and checkout — without polling.

## Setup

```bash
npm install
cp .env.example .env   # fill in DATABASE_URL, REDIS_URL, ALLOWED_ORIGIN
```

Create the tables:

```sql
create table voice_sessions (session_id text primary key, retailer_id text, cart jsonb, updated_at timestamptz);
create table orders (order_id text primary key, session_id text, retailer_id text, cart jsonb, status text);
```

Run:

```bash
npm start
```

Open `http://localhost:3000/?s=test123` in a browser (server serves
`public/index.html` directly — no separate frontend deploy needed).

## Testing without a live voice bot

```bash
# create a session
curl -X POST http://localhost:3000/session \
  -H 'Content-Type: application/json' \
  -d '{"session_id":"test123","retailer_id":"R001","seed_cart":{}}'

# simulate the bot adding an item
curl -X POST http://localhost:3000/webhook/add-to-cart \
  -H 'Content-Type: application/json' \
  -d '{"session_id":"test123","sku":"SKU1","product_name":"Rice 25kg","price":1200,"qty":2}'

# simulate the bot spotlighting it
curl -X POST http://localhost:3000/webhook/spotlight \
  -H 'Content-Type: application/json' \
  -d '{"session_id":"test123","product_id":"SKU1"}'

# simulate an offer
curl -X POST http://localhost:3000/webhook/show-offer \
  -H 'Content-Type: application/json' \
  -d '{"session_id":"test123","offer":{"id":"O1","title":"10% off bulk rice","discount":"10%"}}'

# complete checkout
curl -X POST http://localhost:3000/webhook/checkout \
  -H 'Content-Type: application/json' \
  -d '{"session_id":"test123"}'
```

Watch the open browser tab react to each call in real time.

## Deploying (Render)

1. Push this folder to a GitHub repo.
2. Provision Postgres (Neon/Supabase) and Redis (Render Redis or Upstash);
   run the table SQL above against Postgres.
3. New Web Service on Render → connect the repo → build `npm install` →
   start `npm start`.
4. Set env vars on the service: `DATABASE_URL`, `REDIS_URL`,
   `ALLOWED_ORIGIN` (your final domain, e.g. `https://order.karixforge.in`).
5. Point SimplAI's four function-call webhook URLs at
   `https://<your-render-domain>/webhook/...`.

## Known gaps to close before production

- `/webhook/checkout`'s Postgres insert has no try/catch — add one.
- `ALLOWED_ORIGIN` defaults to `*` if unset — always set it in production.
- Redis session TTL is 6h — adjust if calls can run longer.
- Order placement in `/webhook/checkout` is a stub — wire in the real
  downstream order API.
