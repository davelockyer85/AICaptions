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
const stripe = process.env.STRIPE_SECRET_KEY ? new Stripe(process.env.STRIPE_SECRET_KEY) : null;
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY || '';
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_KEY || '';

if (!DEEPGRAM_API_KEY) {
  console.error("❌ CRITICAL: Missing DEEPGRAM_API_KEY environment variable.");
}

const deepgram = createDeepgramClient(DEEPGRAM_API_KEY);
const supabase = (SUPABASE_URL && SUPABASE_KEY) ? createSupabaseClient(SUPABASE_URL, SUPABASE_KEY) : null;

// Room state: roomId -> { presenterWs, presenterUserId, viewers: Set<WebSocket> }
const rooms = new Map();

function getOrCreateRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, { presenterWs: null, presenterUserId: null, viewers: new Set() });
  }
  return rooms.get(roomId);
}

// Per-account concurrent-room tracking
const activeRoomsByUser = new Map();

function addUserRoom(userId, roomId) {
  if (!userId) return;
  if (!activeRoomsByUser.has(userId)) activeRoomsByUser.set(userId, new Set());
  activeRoomsByUser.get(userId).add(roomId);
}

function removeUserRoom(userId, roomId) {
  if (!userId) return;
  const set = activeRoomsByUser.get(userId);
  if (!set) return;
  set.delete(roomId);
  if (set.size === 0) activeRoomsByUser.delete(userId);
}

function safeSend(ws, payload) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(payload);
  }
}

app.use(express.json());
app.use(express.static('public'));

// Render Health Check
app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

// Stripe Checkout Endpoint (Handles both /api/create-checkout-session and /create-checkout-session)
app.post(['/api/create-checkout-session', '/create-checkout-session'], async (req, res) => {
  if (!stripe) {
    return res.status(500).json({ error: 'Stripe API key not configured on server.' });
  }
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

// WebSocket Connection Handler
wss.on('connection', async (ws, req) => {
  const urlParams = new URLSearchParams(req.url.split('?')[1] || '');
  const roomId = urlParams.get('room') || urlParams.get('roomId') || 'main-stage';
  const role = urlParams.get('role') || 'viewer';
  const token = urlParams.get('token');
  const lang = urlParams.get('lang') || 'en-US';

  const room = getOrCreateRoom(roomId);

  if (role === 'presenter') {
    let authenticatedUser = null;

    // Validate token if provided, but continue gracefully in guest mode if missing/invalid
    if (token && supabase) {
      try {
        const { data: { user }, error } = await supabase.auth.getUser(token);
        if (!error && user) {
          authenticatedUser = user;

          const { data: dbUser } = await supabase
            .from('users')
            .select('*')
            .eq('id', user.id)
            .single();

          if (dbUser) {
            // Event Pass expiry check
            if (dbUser.plan_tier === 'one_time') {
              const expiresAt = dbUser.event_pass_expires_at ? new Date(dbUser.event_pass_expires_at) : null;
              if (expiresAt && (Number.isNaN(expiresAt.getTime()) || expiresAt.getTime() <= Date.now())) {
                ws.close(4005, 'Event Pass has expired');
                return;
              }
            }

            // Quota check
            const usedSeconds = dbUser.streaming_seconds_used || 0;
            if (dbUser.max_streaming_seconds != null && usedSeconds >= dbUser.max_streaming_seconds) {
              ws.close(4006, 'Monthly streaming quota exhausted');
              return;
            }

            // Concurrent stage check
            const userAlreadyOwnsThisRoom = activeRoomsByUser.get(user.id)?.has(roomId) || false;
            if (!userAlreadyOwnsThisRoom) {
              const currentRoomCount = activeRoomsByUser.get(user.id)?.size || 0;
              const allowedRooms = dbUser.allowed_rooms ?? 1;
              if (currentRoomCount >= allowedRooms) {
                ws.close(4004, 'Maximum concurrent stages limit reached');
                return;
              }
            }
          }
        }
      } catch (authErr) {
        console.warn(`[room=${roomId}] Token check warning, continuing as guest:`, authErr.message);
      }
    }

    // Start tracking active streaming time when WebSocket connects/streams
const usageTimer = setInterval(async () => {
  if (!authenticatedUser) return;

  // RPC call to increment usage by 10 seconds atomically
  const { data: dbUser, error } = await supabase.rpc('increment_user_usage', {
    user_id_param: authenticatedUser.id,
    seconds_param: 10
  });

  if (error) {
    console.error('Error incrementing usage:', error);
    return;
  }

  // Enforce limit if quota was reached during stream
  if (dbUser && dbUser.max_streaming_seconds != null && dbUser.streaming_seconds_used >= dbUser.max_streaming_seconds) {
    ws.close(4006, 'Monthly streaming quota exhausted');
    clearInterval(usageTimer);
  }
}, 10000); // Pulse every 10 seconds

// Ensure timer is cleaned up on disconnect
ws.on('close', () => {
  clearInterval(usageTimer);
});

    const userId = authenticatedUser ? authenticatedUser.id : `guest-${Date.now()}`;

    // Replace existing active presenter in this room
    const existingPresenter = room.presenterWs;
    if (existingPresenter && existingPresenter.readyState === WebSocket.OPEN) {
      if (room.presenterUserId && room.presenterUserId !== userId) {
        ws.close(4009, 'This room already has an active presenter');
        return;
      }
      existingPresenter.close(4000, 'Replaced by your new session');
    }

    room.presenterWs = ws;
    room.presenterUserId = userId;
    if (authenticatedUser) addUserRoom(userId, roomId);

    let deepgramLive = null;
    let deepgramReady = false;
    const audioQueue = [];

    try {
      // Stream details are auto-detected by Deepgram from the incoming WebM header
      deepgramLive = deepgram.listen.live({
        model: 'nova-2',
        language: lang,
        smart_format: true,
        punctuate: true,
        interim_results: true,
      });

      deepgramLive.on(LiveTranscriptionEvents.Open, () => {
        deepgramReady = true;
        console.log(`[room=${roomId}] Deepgram connection open and ready`);
        while (audioQueue.length > 0) {
          deepgramLive.send(audioQueue.shift());
        }
      });

      deepgramLive.on(LiveTranscriptionEvents.Transcript, (data) => {
        const transcript = data.channel?.alternatives?.[0]?.transcript;
        if (transcript && transcript.trim() !== '') {
          console.log(`[room=${roomId}] Transcript: "${transcript}"`);
          const payload = JSON.stringify({
            type: 'caption',
            text: transcript,
            isFinal: data.is_final
          });
          safeSend(ws, payload);
          room.viewers.forEach((viewer) => safeSend(viewer, payload));
        }
      });

      deepgramLive.on(LiveTranscriptionEvents.Error, (err) => {
        console.error(`Deepgram Error [room=${roomId}]:`, err);
        safeSend(ws, JSON.stringify({ type: 'error', message: 'Speech-to-text error occurred.' }));
      });

      deepgramLive.on(LiveTranscriptionEvents.Close, () => {
        deepgramReady = false;
        console.warn(`Deepgram connection closed [room=${roomId}]`);
      });
    } catch (err) {
      console.error('Failed to initialize Deepgram:', err);
      safeSend(ws, JSON.stringify({ type: 'error', message: 'Failed to start captioning session.' }));
    }

    ws.on('message', (message) => {
      if (typeof message === 'string') return; // Ignore non-binary control messages
      if (!deepgramLive) return;

      if (deepgramReady && deepgramLive.getReadyState() === 1) {
        deepgramLive.send(message);
      } else {
        audioQueue.push(message);
      }
    });

    ws.on('close', () => {
      if (deepgramLive) {
        try { deepgramLive.finish(); } catch (e) { /* already closed */ }
      }

      if (room.presenterWs === ws) {
        room.presenterWs = null;
        room.presenterUserId = null;
        if (authenticatedUser) removeUserRoom(userId, roomId);
        console.log(`[room=${roomId}] Presenter disconnected`);
      }
    });

  } else {
    // Viewer connection (OBS Overlay / Mobile View)
    room.viewers.add(ws);
    console.log(`[room=${roomId}] Viewer connected (${room.viewers.size} total)`);

    ws.on('close', () => {
      room.viewers.delete(ws);
      console.log(`[room=${roomId}] Viewer disconnected (${room.viewers.size} left)`);
    });
  }
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
  console.log(`AICaptions server listening on port ${PORT}`);
});
