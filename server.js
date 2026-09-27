import express from 'express';
import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import Stripe from 'stripe';
import { createClient as createDeepgramClient, LiveTranscriptionEvents } from '@deepgram/sdk';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Initialize Clients
const stripe = new Stripe(process.env.STRIPE_SECRET_KEY || '');
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY || '';
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_KEY || '';

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("❌ CRITICAL: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.");
}

const deepgram = createDeepgramClient(DEEPGRAM_API_KEY);
const supabase = createSupabaseClient(SUPABASE_URL, SUPABASE_KEY);

// Active Rooms Map: roomId -> { presenterWs, presenterUserId, viewers: Set<WebSocket> }
const rooms = new Map();

function getOrCreateRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, { presenterWs: null, presenterUserId: null, viewers: new Set() });
  }
  return rooms.get(roomId);
}

function safeSend(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(payload);
  }
}

// Stripe Webhook needs raw body parsing
app.post('/stripe-webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;

  try {
    event = stripe.webhooks.constructEvent(
      req.body,
      sig,
      process.env.STRIPE_WEBHOOK_SECRET || ''
    );
  } catch (err) {
    console.error(`Webhook Signature Verification Failed: ${err.message}`);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  if (event.type === 'checkout.session.completed') {
    const session = event.data.object;
    const userId = session.client_reference_id;

    if (userId) {
      // Determine tier and allocation from Stripe Price ID
      const lineItems = await stripe.checkout.sessions.listLineItems(session.id);
      const priceId = lineItems.data[0]?.price?.id;

      let planTier = 'free';
      let maxSeconds = 3600; // 1 hour free
      let allowedRooms = 1;

      if (priceId === process.env.STARTER_PRICE_ID) {
        planTier = 'starter';
        maxSeconds = 36000; // 10 hours
        allowedRooms = 1;
      } else if (priceId === process.env.PRO_PRICE_ID) {
        planTier = 'pro';
        maxSeconds = 180000; // 50 hours
        allowedRooms = 3;
      } else if (priceId === process.env.EVENT_PASS_PRICE_ID) {
        planTier = 'one_time';
        maxSeconds = 86400; // 24 hours pass
        allowedRooms = 2;
      }

      await supabase
        .from('users')
        .update({
          plan_tier: planTier,
          subscription_status: 'active',
          max_streaming_seconds: maxSeconds,
          allowed_rooms: allowedRooms,
          updated_at: new Date().toISOString()
        })
        .eq('id', userId);

      console.log(`✅ User ${userId} upgraded to ${planTier}`);
    }
  }

  res.json({ received: true });
});

app.use(express.json());
app.use(express.static('public'));

app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

// Stripe Checkout Endpoint
app.post('/create-checkout-session', async (req, res) => {
  try {
    const { priceId, userId } = req.body;
    if (!priceId) return res.status(400).json({ error: 'Missing priceId' });

    const oneTimePrices = [
      process.env.EVENT_PASS_PRICE_ID,
      process.env.PRO_EVENT_PASS_PRICE_ID
    ].filter(Boolean);

    const isOneTime = oneTimePrices.includes(priceId);

    const session = await stripe.checkout.sessions.create({
      payment_method_types: ['card'],
      line_items: [{ price: priceId, quantity: 1 }],
      mode: isOneTime ? 'payment' : 'subscription',
      client_reference_id: userId || null,
      success_url: `${process.env.CLIENT_URL || req.headers.origin}/dashboard.html?success=true`,
      cancel_url: `${process.env.CLIENT_URL || req.headers.origin}/pricing.html?canceled=true`,
    });

    res.json({ url: session.url });
  } catch (error) {
    console.error('Stripe Checkout Error:', error);
    res.status(500).json({ error: error.message });
  }
});

// WebSocket Handling
wss.on('connection', async (ws, req) => {
  const urlParams = new URLSearchParams(req.url.split('?')[1] || '');
  const roomId = urlParams.get('room') || urlParams.get('roomId') || 'main-stage';
  const role = urlParams.get('role') || 'viewer';
  const token = urlParams.get('token');
  const lang = urlParams.get('lang') || 'en-US';

  const room = getOrCreateRoom(roomId);

  if (role === 'presenter') {
    if (!token) {
      ws.close(4003, 'Authentication token required');
      return;
    }

    // Authenticate with Supabase
    const { data: { user }, error: authError } = await supabase.auth.getUser(token);
    if (authError || !user) {
      ws.close(4003, 'Invalid authentication token');
      return;
    }

    // Verify Subscription and Streaming Quota
    const { data: dbUser, error: dbError } = await supabase
      .from('users')
      .select('*')
      .eq('id', user.id)
      .single();

    if (dbError || !dbUser) {
      ws.close(4003, 'User profile record not found');
      return;
    }

    if (dbUser.subscription_status !== 'active' && dbUser.plan_tier !== 'free') {
      ws.close(4003, 'Active subscription required');
      return;
    }

    if (dbUser.max_streaming_seconds > 0 && dbUser.streaming_seconds_used >= dbUser.max_streaming_seconds) {
      ws.close(4006, 'Monthly streaming quota exhausted');
      return;
    }

    // Room Conflict Prevention
    const existingPresenter = room.presenterWs;
    if (existingPresenter && existingPresenter.readyState === WebSocket.OPEN) {
      if (room.presenterUserId && room.presenterUserId !== user.id) {
        ws.close(4009, 'This room already has an active presenter');
        return;
      }
      existingPresenter.close(4000, 'Replaced by your new session');
    }

    room.presenterWs = ws;
    room.presenterUserId = user.id;

    // Track Stream Usage Timer
    let streamStartTime = Date.now();
    const timeTrackerInterval = setInterval(async () => {
      const now = Date.now();
      const elapsedSeconds = Math.floor((now - streamStartTime) / 1000);
      streamStartTime = now;

      if (elapsedSeconds > 0) {
        await supabase.rpc('increment_streaming_seconds', {
          user_id_input: user.id,
          seconds_input: elapsedSeconds
        }).catch(async () => {
          // Fallback if RPC function is not created in Supabase
          const { data: current } = await supabase.from('users').select('streaming_seconds_used').eq('id', user.id).single();
          if (current) {
            await supabase.from('users').update({
              streaming_seconds_used: (current.streaming_seconds_used || 0) + elapsedSeconds
            }).eq('id', user.id);
          }
        });
      }
    }, 10000); // Sync every 10s

    let deepgramLive = null;
    let deepgramReady = false;
    const audioQueue = [];

    try {
      deepgramLive = deepgram.listen.live({
        model: 'nova-2',
        language: lang,
        smart_format: true,
        encoding: 'webm/opus',
        sample_rate: 48000,
      });

      deepgramLive.on(LiveTranscriptionEvents.Open, () => {
        deepgramReady = true;
        while (audioQueue.length > 0) {
          deepgramLive.send(audioQueue.shift());
        }
      });

      deepgramLive.on(LiveTranscriptionEvents.Transcript, (data) => {
        const sentence = data.channel?.alternatives?.[0]?.transcript;
        if (sentence && sentence.trim() !== '') {
          const payload = JSON.stringify({
            type: 'caption',
            text: sentence,
            isFinal: data.is_final
          });

          safeSend(ws, payload);
          room.viewers.forEach((viewer) => safeSend(viewer, payload));
        }
      });

      deepgramLive.on(LiveTranscriptionEvents.Error, (err) => {
        console.error(`Deepgram Error [room=${roomId}]:`, err);
        safeSend(ws, JSON.stringify({ type: 'error', message: 'Speech-to-text processing error.' }));
      });

      deepgramLive.on(LiveTranscriptionEvents.Close, () => {
        deepgramReady = false;
      });
    } catch (err) {
      console.error('Failed to initialize Deepgram:', err);
      safeSend(ws, JSON.stringify({ type: 'error', message: 'Failed to start captioning session.' }));
    }

    ws.on('message', (message) => {
      if (typeof message === 'string') return;
      if (!deepgramLive) return;

      if (deepgramReady && deepgramLive.getReadyState() === 1) {
        deepgramLive.send(message);
      } else {
        audioQueue.push(message);
      }
    });

    ws.on('close', () => {
      clearInterval(timeTrackerInterval);

      if (deepgramLive) {
        try { deepgramLive.finish(); } catch (e) {}
      }

      if (room.presenterWs === ws) {
        room.presenterWs = null;
        room.presenterUserId = null;
      }

      if (!room.presenterWs && room.viewers.size === 0) {
        rooms.delete(roomId);
      }
    });

  } else {
    // Spectator / OBS Overlay / Mobile Viewers
    room.viewers.add(ws);

    ws.on('close', () => {
      room.viewers.delete(ws);
      if (!room.presenterWs && room.viewers.size === 0) {
        rooms.delete(roomId);
      }
    });
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Live Caption Server running on port ${PORT}`);
});
