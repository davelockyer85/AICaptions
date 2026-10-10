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

// --- Room state -------------------------------------------------------
// roomId -> { presenterWs, presenterUserId, viewers: Set<WebSocket> }
const rooms = new Map();

function getOrCreateRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, { presenterWs: null, presenterUserId: null, viewers: new Set() });
  }
  return rooms.get(roomId);
}

// --- Per-account concurrent-room tracking ------------------------------
// userId -> Set<roomId> of rooms this account currently has a live
// presenter in. This is what `allowed_rooms` (plan-tier concurrent stage
// limit) is checked against.
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

app.use(express.json());
app.use(express.static('public'));

// Render Health Check
app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

// Stripe Checkout Endpoint
app.post('/create-checkout-session', async (req, res) => {
  try {
    const { priceId, userId, mode } = req.body;
    if (!priceId) return res.status(400).json({ error: 'Missing priceId' });

    // 1. Determine mode: prioritize mode sent from frontend, fallback to checking env vars
    const oneTimePrices = [
      process.env.EVENT_PASS_PRICE_ID,
      process.env.PRO_EVENT_PASS_PRICE_ID
    ].filter(Boolean);

    let sessionMode = mode;

    if (!sessionMode) {
      sessionMode = oneTimePrices.includes(priceId.trim()) ? 'payment' : 'subscription';
    }

    // 2. Debug log to verify what is being sent to Stripe
    console.log(`Creating session for priceId: ${priceId} with mode: ${sessionMode}`);

    const session = await stripe.checkout.sessions.create({
      payment_method_types: ['card'],
      line_items: [{ price: priceId.trim(), quantity: 1 }],
      mode: sessionMode, // 'payment' or 'subscription'
      client_reference_id: userId || null,
      success_url: `${process.env.CLIENT_URL || req.headers.origin}/dashboard.html?success=true`,
      cancel_url: `${process.env.CLIENT_URL || req.headers.origin}/pricing.html?canceled=true`,
    });

    res.json({ url: session.url });
  } catch (error) {
    // Log error details clearly in server console
    console.error('Stripe Checkout Error:', error.raw ? error.raw.message : error.message);
    res.status(400).json({ error: error.message });
  }
});

// WebSocket Handler
wss.on('connection', async (ws, req) => {
  const urlParams = new URLSearchParams(req.url.split('?')[1] || '');
  // dashboard.html sends `room=`, not `roomId=` - accept both so a client
  // using either name works instead of every session silently defaulting
  // to 'main-stage'.
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

    // Load the account's plan/usage record. This is the source of truth
    // for whether this session is allowed to start at all.
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

    // Event Pass expiry - one-time plans only. Fails closed: a missing or
    // unparsable expiry is treated as expired rather than as unlimited.
    if (dbUser.plan_tier === 'one_time') {
      const expiresAt = dbUser.event_pass_expires_at ? new Date(dbUser.event_pass_expires_at) : null;
      if (!expiresAt || Number.isNaN(expiresAt.getTime()) || expiresAt.getTime() <= Date.now()) {
        ws.close(4005, 'Event Pass has expired');
        return;
      }
    }

    // Streaming quota - applies to every plan. `max_streaming_seconds` of
    // null/undefined means unlimited; 0 means no allowance at all.
    const usedSeconds = dbUser.streaming_seconds_used || 0;
    if (dbUser.max_streaming_seconds != null && usedSeconds >= dbUser.max_streaming_seconds) {
      ws.close(4006, 'Monthly streaming quota exhausted');
      return;
    }

    // Concurrent-stage limit (allowed_rooms). Reconnecting to a room this
    // account already owns doesn't count as a new room, so it never
    // trips this check.
    const userAlreadyOwnsThisRoom = activeRoomsByUser.get(user.id)?.has(roomId) || false;
    if (!userAlreadyOwnsThisRoom) {
      const currentRoomCount = activeRoomsByUser.get(user.id)?.size || 0;
      const allowedRooms = dbUser.allowed_rooms ?? 1;
      if (currentRoomCount >= allowedRooms) {
        ws.close(4004, 'Maximum concurrent stages limit reached');
        return;
      }
    }

    // Guard: if a DIFFERENT authenticated user already owns this room's
    // active presenter slot, refuse the connection instead of silently
    // kicking the legitimate presenter off. The same user reconnecting
    // (page refresh, dropped connection) is still allowed to take over.
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
        // Flush any audio chunks that arrived before Deepgram finished connecting
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
      if (typeof message === 'string') return; // control/text frames, not audio
      if (!deepgramLive) return;

      if (deepgramReady && deepgramLive.getReadyState() === 1) {
        deepgramLive.send(message);
      } else {
        // Buffer briefly instead of dropping audio while Deepgram connects
        audioQueue.push(message);
      }
    });

    ws.on('close', () => {
      if (deepgramLive) {
        try { deepgramLive.finish(); } catch (e) { /* already closed */ }
      }

      // Only clear the room's presenter slot if this socket is STILL the
      // registered presenter. Without this check, a replaced (kicked)
      // socket's delayed 'close' event can wipe out a newer presenter's
      // active session for the same room - this was the root cause of
      // sessions breaking when a second user connected.
      if (room.presenterWs === ws) {
        room.presenterWs = null;
        room.presenterUserId = null;
      }
      removeUserRoom(user.id, roomId);

      if (!room.presenterWs && room.viewers.size === 0) {
        rooms.delete(roomId);
      }

      // Record usage. We re-read the current value rather than reusing the
      // snapshot taken at connect time, because a Pro account can run
      // several concurrent sessions (that's the whole point of
      // allowed_rooms > 1) - reusing a stale value would let simultaneous
      // sessions clobber each other's usage as they each close. This
      // narrows the race but doesn't fully eliminate it; a Postgres
      // increment function/RPC would be the bulletproof fix if you want
      // one - happy to write that if it'd help.
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
    // Viewer / OBS Overlay / Mobile View
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
  console.log(`🚀 Server running on port ${PORT}`);
});
