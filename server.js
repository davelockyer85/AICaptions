const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { createClient } = require('@supabase/supabase-js');
const Stripe = require('stripe');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

app.use(express.json());
app.use(express.static('public'));

// ------------------------------------------------------------------
// 1. STRIPE CHECKOUT SESSION ENDPOINT (Fixes empty client_reference_id)
// ------------------------------------------------------------------
app.post('/create-checkout-session', async (req, res) => {
  const { priceId, userId, customerEmail } = req.body;

  // Validate authenticated user presence
  if (!userId || typeof userId !== 'string' || userId.trim() === '') {
    return res.status(401).json({ error: 'You must be logged in to select a paid plan.' });
  }

  try {
    // Construct base session parameters
    const sessionParams = {
      payment_method_types: ['card'],
      line_items: [{ price: priceId, quantity: 1 }],
      mode: priceId.includes('EVENT') ? 'payment' : 'subscription',
      success_url: `${req.headers.origin}/dashboard.html?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${req.headers.origin}/pricing.html`,
      client_reference_id: userId.trim() // Ensured non-empty string
    };

    if (customerEmail && customerEmail.trim() !== '') {
      sessionParams.customer_email = customerEmail.trim();
    }

    const session = await stripe.checkout.sessions.create(sessionParams);
    res.json({ id: session.id, url: session.url });
  } catch (err) {
    console.error('Stripe Checkout Creation Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ------------------------------------------------------------------
// 2. WEBSOCKET CONNECTION & AUTHORIZATION CHECK (Handles Free Pass)
// ------------------------------------------------------------------
wss.on('connection', async (ws, req) => {
  const urlParams = new URLSearchParams(req.url.split('?')[1]);
  const token = urlParams.get('token');

  if (!token) {
    ws.close(4003, 'Stream rejected: Missing authentication token.');
    return;
  }

  // Verify JWT with Supabase
  const { data: { user }, error: authErr } = await supabase.auth.getUser(token);
  if (authErr || !user) {
    ws.close(4003, 'Stream rejected: Invalid or expired auth token.');
    return;
  }

  // Fetch account status from public.users table
  const { data: dbUser, error: userErr } = await supabase
    .from('users')
    .select('plan_tier, subscription_status, streaming_seconds_used, max_streaming_seconds')
    .eq('id', user.id)
    .single();

  if (userErr || !dbUser) {
    ws.close(4003, 'Stream rejected: User profile not found.');
    return;
  }

  // Authorization check (Accepts active subscriptions and valid free passes)
  const isAuthorizedPlan = dbUser.subscription_status === 'active' || dbUser.plan_tier === 'free';
  if (!isAuthorizedPlan) {
    ws.close(4003, 'Stream rejected: Active subscription or free pass required.');
    return;
  }

  // Quota check (5 hours = 18,000s)
  const used = dbUser.streaming_seconds_used || 0;
  const max = dbUser.max_streaming_seconds || 0;
  if (used >= max) {
    ws.close(4006, 'Stream rejected: Monthly or free trial streaming quota exhausted.');
    return;
  }

  // Verify Deepgram API Key presence
  if (!process.env.DEEPGRAM_API_KEY) {
    console.error('CRITICAL: DEEPGRAM_API_KEY environment variable is missing on server.');
    ws.send(JSON.stringify({ error: 'Server configuration error: Deepgram key missing.' }));
    ws.close(1011, 'Internal Server Error: Missing Deepgram Key.');
    return;
  }

  // Proceed with stream setup...
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
