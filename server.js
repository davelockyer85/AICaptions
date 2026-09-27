import express from 'express';
import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import Stripe from 'stripe';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import { createClient as createDeepgramClient, LiveTranscriptionEvents } from '@deepgram/sdk';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// --- ENVIRONMENT VARIABLE VALIDATION ---
const requiredEnv = [
  'STRIPE_SECRET_KEY',
  'SUPABASE_URL',
  'SUPABASE_SERVICE_ROLE_KEY',
  'DEEPGRAM_API_KEY'
];

for (const key of requiredEnv) {
  if (!process.env[key]) {
    console.warn(`⚠️ Warning: Environment variable ${key} is missing from process.env!`);
  }
}

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const stripe = new Stripe(process.env.STRIPE_SECRET_KEY || '');
const supabase = createSupabaseClient(
  process.env.SUPABASE_URL || '',
  process.env.SUPABASE_SERVICE_ROLE_KEY || ''
);
const deepgram = createDeepgramClient(process.env.DEEPGRAM_API_KEY || '');

// Global State Tracking
const rooms = new Map(); // roomId -> Set<WebSocket> (Audience clients)
const deepgramConnections = new Map(); // roomId -> Deepgram Live Connection
const userActivePresenterRooms = new Map(); // userId -> Set<roomId>

// --- 1. STRIPE WEBHOOK (MUST BE DEFINED BEFORE express.json()) ---
app.post('/api/stripe-webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;

  try {
    if (!process.env.STRIPE_WEBHOOK_SECRET) {
      throw new Error("STRIPE_WEBHOOK_SECRET environment variable is missing.");
    }
    event = stripe.webhooks.constructEvent(req.body, sig, process.env.STRIPE_WEBHOOK_SECRET);
  } catch (err) {
    console.error("Webhook signature verification failed:", err.message);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  // Handle successful checkout payments / new subscriptions
  if (event.type === 'checkout.session.completed') {
    const session = event.data.object;
    const userId = session.client_reference_id || session.metadata?.userId;
    const priceId = session.metadata?.priceId;

    if (userId) {
      // Map Stripe Price IDs to streaming capacity limits (in seconds)
      let maxSeconds = 7200; // Default Event Pass (2 Hours)

      if (priceId === 'price_1UJYbIJV4dyhvuKy8pXdPHbU') {
        maxSeconds = 108000; // Starter: 30 hours (30 * 3600)
      } else if (priceId === 'price_1UJtlKJV4dyhvuKy67mqD8tW') {
        maxSeconds = 540000; // Pro: 150 hours (150 * 3600)
      }

      const { error } = await supabase
        .from('users')
        .update({
          subscription_status: 'active',
          max_streaming_seconds: maxSeconds,
          streaming_seconds_used: 0, // Reset usage counter upon purchase/renewal
          stripe_customer_id: session.customer
        })
        .eq('id', userId);

      if (error) {
        console.error('Failed to update Supabase user account via Webhook:', error);
      } else {
        console.log(`Successfully activated subscription & limits for user: ${userId}`);
      }
    }
  }

  // Handle subscription cancellations
  if (event.type === 'customer.subscription.deleted') {
    const subscription = event.data.object;
    const customerId = subscription.customer;

    const { error } = await supabase
      .from('users')
      .update({ subscription_status: 'canceled' })
      .eq('stripe_customer_id', customerId);

    if (error) {
      console.error('Failed to revoke subscription in Supabase:', error);
    }
  }

  res.json({ received: true });
});

// --- 2. MIDDLEWARES & STATIC SERVING ---
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// --- 3. REST ENDPOINT: CREATE STRIPE CHECKOUT SESSION ---
app.post('/api/create-checkout-session', async (req, res) => {
  try {
    const { priceId, mode, token } = req.body;

    if (!priceId) {
      return res.status(400).json({ error: 'Missing priceId in request payload' });
    }

    // Authenticate user via Supabase session token
    let userId = null;
    if (token) {
      const { data: { user } } = await supabase.auth.getUser(token);
      if (user) userId = user.id;
    }

    const session = await stripe.checkout.sessions.create({
      payment_method_types: ['card'],
      line_items: [{ price: priceId, quantity: 1 }],
      mode: mode || 'subscription',
      client_reference_id: userId,
      metadata: { userId, priceId },
      success_url: `${req.headers.origin || 'https://aicaptions-tkoc.onrender.com'}/dashboard.html?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${req.headers.origin || 'https://aicaptions-tkoc.onrender.com'}/pricing.html`,
    });

    res.json({ url: session.url });
  } catch (err) {
    console.error('Stripe Session Creation Failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// --- 4. WEBSOCKET REAL-TIME CAPTIONING ENGINE ---
wss.on('connection', async (ws, req) => {
  const urlParams = new URLSearchParams(req.url.split('?')[1]);
  const role = urlParams.get('role'); // "presenter" or "audience"
  const roomId = urlParams.get('roomId');
  const token = urlParams.get('token');

  if (!roomId || !role) {
    ws.send(JSON.stringify({ type: 'error', message: 'Missing roomId or role parameter' }));
    return ws.close(4000, 'Missing parameters');
  }

  // --- AUDIENCE CLIENT HANDLER ---
  if (role === 'audience') {
    if (!rooms.has(roomId)) {
      rooms.set(roomId, new Set());
    }
    rooms.get(roomId).add(ws);

    ws.on('close', () => {
      const room = rooms.get(roomId);
      if (room) {
        room.delete(ws);
        if (room.size === 0) rooms.delete(roomId);
      }
    });
    return;
  }

  // --- PRESENTER CLIENT HANDLER ---
  if (role === 'presenter') {
    let userId = null;

    try {
      if (!token) {
        ws.send(JSON.stringify({ type: 'error', message: 'Authentication token required' }));
        return ws.close(4001, 'Auth required');
      }

      // Verify Supabase User Token
      const { data: authData, error: authError } = await supabase.auth.getUser(token);
      if (authError || !authData?.user) {
        console.error('Auth verification error:', authError);
        ws.send(JSON.stringify({ type: 'error', message: 'Invalid or expired authentication token' }));
        return ws.close(4001, 'Invalid token');
      }

      userId = authData.user.id;

      // Query database profile for subscription and quota validation
   const isSubscriptionActive = dbUser.subscription_status === 'active';
      const isOneTimePassValid = dbUser.one_time_expires_at && new Date(dbUser.one_time_expires_at) > new Date();

      if (!isSubscriptionActive && !isOneTimePassValid) {
        ws.send(JSON.stringify({ type: 'error', message: 'Active subscription or valid pass required' }));
        return ws.close(4002, 'Subscription inactive');
      }

      if (dbUser.subscription_status !== 'active') {
        ws.send(JSON.stringify({ type: 'error', message: 'Active subscription required to start streaming' }));
        return ws.close(4002, 'Subscription inactive');
      }

      const maxAllowedSeconds = Number(dbUser.max_streaming_seconds || dbUser.max_streaming_settings) || 7200;
      const usedSeconds = Number(dbUser.streaming_seconds_used) || 0;

      if (usedSeconds >= maxAllowedSeconds) {
        ws.send(JSON.stringify({ type: 'error', message: 'Monthly streaming quota exhausted' }));
        return ws.close(4003, 'Quota exceeded');
      }

      // Establish Deepgram Nova-2 Live Engine Connection
      const dgConnection = deepgram.listen.live({
        model: 'nova-2',
        language: 'en-US',
        smart_format: true,
        interim_results: true
      });

      let isDeepgramReady = false;
      const audioBufferQueue = [];

      dgConnection.on(LiveTranscriptionEvents.Open, () => {
        console.log(`Deepgram WebSocket connected for room: ${roomId}`);
        isDeepgramReady = true;

        // Flush any buffered audio chunks queued during initial handshake
        while (audioBufferQueue.length > 0) {
          const chunk = audioBufferQueue.shift();
          try {
            dgConnection.send(chunk);
          } catch (e) {
            console.error('Error flushing buffered audio to Deepgram:', e);
          }
        }
      });

      dgConnection.on(LiveTranscriptionEvents.Transcript, (data) => {
        const transcript = data.channel?.alternatives[0]?.transcript;
        if (transcript) {
          const messagePayload = JSON.stringify({
            type: 'transcript',
            text: transcript,
            isFinal: data.is_final
          });

          // Send transcript back to presenter
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(messagePayload);
          }

          // Broadcast transcript to connected audience members
          const audienceRoom = rooms.get(roomId);
          if (audienceRoom) {
            audienceRoom.forEach((client) => {
              if (client.readyState === WebSocket.OPEN) {
                client.send(messagePayload);
              }
            });
          }
        }
      });

      dgConnection.on(LiveTranscriptionEvents.Error, (err) => {
        console.error('Deepgram Connection Error:', err);
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'error', message: 'Deepgram transcription error: ' + (err.message || 'Stream processing failed') }));
        }
      });

      dgConnection.on(LiveTranscriptionEvents.Close, () => {
        console.log(`Deepgram connection closed for room: ${roomId}`);
      });

      deepgramConnections.set(roomId, dgConnection);

      if (!userActivePresenterRooms.has(userId)) {
        userActivePresenterRooms.set(userId, new Set());
      }
      userActivePresenterRooms.get(userId).add(roomId);

      // Handle Incoming Binary Audio Stream Chunks
      ws.on('message', (data) => {
        if (Buffer.isBuffer(data) || data instanceof ArrayBuffer) {
          if (isDeepgramReady) {
            try {
              dgConnection.send(data);
            } catch (err) {
              console.error('Error transmitting chunk to Deepgram:', err);
            }
          } else if (audioBufferQueue.length < 100) {
            audioBufferQueue.push(data);
          }
        }
      });

      // Cleanup Presenter Disconnects
      ws.on('close', () => {
        console.log(`Presenter disconnected from room: ${roomId}`);
        const activeDg = deepgramConnections.get(roomId);
        if (activeDg) {
          activeDg.finish();
          deepgramConnections.delete(roomId);
        }

        const userRooms = userActivePresenterRooms.get(userId);
        if (userRooms) {
          userRooms.delete(roomId);
          if (userRooms.size === 0) userActivePresenterRooms.delete(userId);
        }
      });

    } catch (err) {
      console.error('Server WebSocket presenter error:', err);
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'error', message: 'Internal server error starting stream session' }));
      }
      ws.close(1011, 'Server error');
    }
  }
});

// --- 5. START SERVER ---
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`AICaptions Server running on port ${PORT}`);
});
