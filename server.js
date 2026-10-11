import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import { createClient, LiveTranscriptionEvents } from "@deepgram/sdk";
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";
import Stripe from "stripe";
import { createClient as createSupabaseClient } from "@supabase/supabase-js";

dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server });

// --- Environment & Client Initialization -------------------------------
const DEEPGRAM_KEY = process.env.DEEPGRAM_API_KEY;
if (!DEEPGRAM_KEY) {
  console.error("[WARNING] DEEPGRAM_API_KEY is missing in environment variables!");
}
const deepgram = createClient(DEEPGRAM_KEY);

const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);
const supabase = createSupabaseClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY
);

// --- Translation Helper ------------------------------------------------
async function translateText(text, targetLang) {
  if (!targetLang || targetLang === "en") return text;
  try {
    return text;
  } catch (err) {
    console.error("[Translation Error]", err);
    return text;
  }
}

// --- Room State & Tracking ---------------------------------------------
// roomId -> { presenterWs, presenterUserId, viewers: Set<WebSocket>, targetOverlayLang, dgLive, audioQueue, isDgReady }
const rooms = new Map();

function getOrCreateRoom(roomId) {
  if (!rooms.has(roomId)) {
    console.log(`[Room Created] Initializing room: ${roomId}`);
    rooms.set(roomId, {
      presenterWs: null,
      presenterUserId: null,
      viewers: new Set(),
      targetOverlayLang: "en",
      dgLive: null,
      audioQueue: [],
      isDgReady: false
    });
  }
  return rooms.get(roomId);
}

function cleanupRoom(roomId) {
  const room = rooms.get(roomId);
  if (room && room.viewers.size === 0 && !room.presenterWs && !room.dgLive) {
    console.log(`[Room Destroyed] Cleaning up empty room: ${roomId}`);
    rooms.delete(roomId);
  }
}

// --- Per-account concurrent-room tracking ------------------------------
const activeRoomsByUser = new Map();

function addUserRoom(userId, roomId) {
  if (!activeRoomsByUser.has(userId)) activeRoomsByUser.set(userId, new Set());
  activeRoomsByUser.get(userId).add(roomId);
}

function removeUserRoom(userId, roomId) {
  const set = activeRoomsByUser.get(userId);
  if (!set) return;
  set.delete(roomId);
  if (set.size === 0) activeRoomsByUser.delete(userId);
}

function safeSend(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(payload);
  }
}

// ==========================================
// 1. STRIPE WEBHOOK ENDPOINT (Must be BEFORE express.json())
// ==========================================
app.post('/webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;

  try {
    event = stripe.webhooks.constructEvent(req.body, sig, process.env.STRIPE_WEBHOOK_SECRET);
  } catch (err) {
    console.error(`Webhook signature verification failed: ${err.message}`);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  if (event.type === 'checkout.session.completed') {
    const session = event.data.object;
    const userId = session.client_reference_id || session.metadata?.userId;
    const mode = session.mode; // 'payment' or 'subscription'

    if (userId) {
      const isOneTime = mode === 'payment';
      const planTier = isOneTime ? 'one_time' : 'subscriber';
      
      const expiresAt = isOneTime ? new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString() : null;
      const maxStreamingSeconds = isOneTime ? 36000 : null;
      const allowedRooms = isOneTime ? 1 : 3;

      const { error } = await supabase
        .from('users')
        .update({
          plan_tier: planTier,
          event_pass_expires_at: expiresAt,
          max_streaming_seconds: maxStreamingSeconds,
          allowed_rooms: allowedRooms,
          stripe_customer_id: session.customer,
          stripe_subscription_id: session.subscription || null,
          updated_at: new Date()
        })
        .eq('id', userId);

      if (error) {
        console.error('Error updating user plan in Supabase from webhook:', error.message);
      } else {
        console.log(`Successfully updated user ${userId} to tier: ${planTier} (mode: ${mode})`);
      }
    } else {
      console.warn('Webhook received checkout.session.completed, but no userId found in metadata/client_reference_id.');
    }
  }

  res.json({ received: true });
});

// Standard Middleware & Static Files
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Render Health Check
app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

// Stripe Checkout Endpoint
app.post('/create-checkout-session', async (req, res) => {
  try {
    const { priceId, userId, mode } = req.body;
    if (!priceId) return res.status(400).json({ error: 'Missing priceId' });

    const oneTimePrices = [
      process.env.EVENT_PASS_PRICE_ID,
      process.env.PRO_EVENT_PASS_PRICE_ID
    ].filter(Boolean);

    let sessionMode = mode;
    if (!sessionMode) {
      sessionMode = oneTimePrices.includes(priceId.trim()) ? 'payment' : 'subscription';
    }

    console.log(`Creating session for priceId: ${priceId} with mode: ${sessionMode}`);

    const session = await stripe.checkout.sessions.create({
      payment_method_types: ['card'],
      line_items: [{ price: priceId.trim(), quantity: 1 }],
      mode: sessionMode,
      client_reference_id: userId || null,
      metadata: {
        userId: userId || ''
      },
      success_url: `${process.env.CLIENT_URL || req.headers.origin}/dashboard.html?success=true`,
      cancel_url: `${process.env.CLIENT_URL || req.headers.origin}/pricing.html?canceled=true`,
    });

    res.json({ url: session.url });
  } catch (error) {
    console.error('Stripe Checkout Error:', error.raw ? error.raw.message : error.message);
    res.status(400).json({ error: error.message });
  }
});

// ==========================================
// WebSocket Handler (Presenters, Viewers & Deepgram Captions)
// ==========================================
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

    const { data: { user }, error } = await supabase.auth.getUser(token);
    if (error || !user) {
      ws.close(4003, 'Invalid authentication token');
      return;
    }

    // Load account record from 'users' table
    const { data: dbUser, error: dbError } = await supabase
      .from('users')
      .select('*')
      .eq('id', user.id)
      .single();

    if (dbError || !dbUser) {
      console.error('Failed to load user account record:', dbError);
      ws.close(4003, 'No account record found');
      return;
    }

    // Event Pass expiry check
    if (dbUser.plan_tier === 'one_time') {
      const expiresAt = dbUser.event_pass_expires_at ? new Date(dbUser.event_pass_expires_at) : null;
      if (!expiresAt || Number.isNaN(expiresAt.getTime()) || expiresAt.getTime() <= Date.now()) {
        ws.close(4005, 'Event Pass has expired');
        return;
      }
    }

    // Streaming quota check
    const usedSeconds = dbUser.streaming_seconds_used || 0;
    if (dbUser.max_streaming_seconds != null && usedSeconds >= dbUser.max_streaming_seconds) {
      ws.close(4006, 'Monthly streaming quota exhausted');
      return;
    }

    // Concurrent-stage limit check
    const userAlreadyOwnsThisRoom = activeRoomsByUser.get(user.id)?.has(roomId) || false;
    if (!userAlreadyOwnsThisRoom) {
      const currentRoomCount = activeRoomsByUser.get(user.id)?.size || 0;
      const allowedRooms = dbUser.allowed_rooms ?? 1;
      if (currentRoomCount >= allowedRooms) {
        ws.close(4004, 'Maximum concurrent stages limit reached');
        return;
      }
    }

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
    addUserRoom(user.id, roomId);

    const sessionStart = Date.now();

    try {
      room.dgLive = deepgram.listen.live({
        model: 'nova-2',
        language: lang,
        smart_format: true,
        encoding: 'webm/opus',
        sample_rate: 48000,
      });

      room.dgLive.on(LiveTranscriptionEvents.Open, () => {
        room.isDgReady = true;
        while (room.audioQueue.length > 0) {
          room.dgLive.send(room.audioQueue.shift());
        }
      });

      room.dgLive.on(LiveTranscriptionEvents.Transcript, async (data) => {
        const sentence = data.channel?.alternatives?.[0]?.transcript;
        if (sentence && sentence.trim() !== '') {
          const translatedText = await translateText(sentence, room.targetOverlayLang);
          const payload = JSON.stringify({
            type: 'caption',
            text: translatedText,
            isFinal: data.is_final
          });

          safeSend(ws, payload);
          room.viewers.forEach((viewer) => safeSend(viewer, payload));
        }
      });

      room.dgLive.on(LiveTranscriptionEvents.Error, (err) => {
        console.error(`Deepgram Error [room=${roomId}]:`, err);
        safeSend(ws, JSON.stringify({ type: 'error', message: 'Speech-to-text error occurred.' }));
      });

      room.dgLive.on(LiveTranscriptionEvents.Close, () => {
        room.isDgReady = false;
        console.warn(`Deepgram connection closed [room=${roomId}]`);
      });
    } catch (err) {
      console.error('Failed to initialize Deepgram:', err);
      safeSend(ws, JSON.stringify({ type: 'error', message: 'Failed to start captioning session.' }));
    }

    ws.on('message', (message) => {
      if (typeof message === 'string') return;
      if (!room.dgLive) return;

      if (room.isDgReady && room.dgLive.getReadyState() === 1) {
        room.dgLive.send(message);
      } else {
        room.audioQueue.push(message);
      }
    });

    ws.on('close', () => {
      if (room.dgLive) {
        try { room.dgLive.finish(); } catch (e) { /* already closed */ }
        room.dgLive = null;
      }

      if (room.presenterWs === ws) {
        room.presenterWs = null;
        room.presenterUserId = null;
      }
      removeUserRoom(user.id, roomId);
      cleanupRoom(roomId);

      const elapsedSeconds = Math.round((Date.now() - sessionStart) / 1000);
      if (elapsedSeconds > 0) {
        supabase
          .from('users')
          .select('streaming_seconds_used')
          .eq('id', user.id)
          .single()
          .then(({ data: freshUser, error: readErr }) => {
            if (readErr || !freshUser) {
              console.error('Failed to read current usage before update:', readErr);
              return null;
            }
            return supabase
              .from('users')
              .update({ streaming_seconds_used: (freshUser.streaming_seconds_used || 0) + elapsedSeconds })
              .eq('id', user.id);
          })
          .then((res) => {
            if (res?.error) console.error('Failed to update streaming_seconds_used:', res.error);
          })
          .catch((e) => console.error('Usage tracking failed:', e));
      }
    });

  } else {
    // Viewer / Overlay role
    room.viewers.add(ws);

    ws.on('close', () => {
      room.viewers.delete(ws);
      cleanupRoom(roomId);
    });
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
