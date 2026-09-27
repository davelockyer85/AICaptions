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

// Initialize Stripe & Client Fallbacks
const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);

const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("❌ CRITICAL: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY/SUPABASE_KEY.");
}

const deepgram = createDeepgramClient(DEEPGRAM_API_KEY);
const supabase = createSupabaseClient(SUPABASE_URL, SUPABASE_KEY);

// Active rooms state management: room -> Set<WebSocket>
const activeRooms = new Map();

// ==================================================================
// 1. STRIPE WEBHOOK ENDPOINT (Must come BEFORE express.json() middleware)
// ==================================================================
app.post(
  "/api/stripe/webhook",
  express.raw({ type: "application/json" }),
  async (req, res) => {
    const sig = req.headers["stripe-signature"];
    let event;

    try {
      event = stripe.webhooks.constructEvent(
        req.body,
        sig,
        process.env.STRIPE_WEBHOOK_SECRET
      );
    } catch (err) {
      console.error("⚠️ Webhook signature verification failed:", err.message);
      return res.status(400).send(`Webhook Error: ${err.message}`);
    }

    if (event.type === "checkout.session.completed") {
      const session = event.data.object;
      const userId = session.client_reference_id;

      if (userId) {
        const isSubscription = session.mode === "subscription";
        const { error } = await supabase
          .from("users")
          .update({
            subscription_status: "active",
            plan_tier: isSubscription ? "starter" : "one_time",
            max_streaming_seconds: isSubscription ? 108000 : 7200 // 30 hrs vs 2 hrs
          })
          .eq("id", userId);

        if (error) {
          console.error("Error updating user record after checkout:", error);
        }
      }
    }

    res.json({ received: true });
  }
);

// Standard Middlewares
app.use(express.json());
app.use(express.static('public')); // Serves HTML dashboard files

// ==================================================================
// 2. STRIPE CHECKOUT SESSION ENDPOINT
// ==================================================================
app.post("/create-checkout-session", async (req, res) => {
  const { priceId, userId, customerEmail } = req.body;

  if (!userId || typeof userId !== "string" || userId.trim() === "") {
    return res.status(401).json({ error: "You must be logged in to subscribe." });
  }

  try {
    const sessionParams = {
      payment_method_types: ["card"],
      line_items: [{ price: priceId, quantity: 1 }],
      mode: priceId.includes("EVENT") ? "payment" : "subscription",
      success_url: `${req.headers.origin}/dashboard.html?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${req.headers.origin}/pricing.html`,
      client_reference_id: userId.trim()
    };

    if (customerEmail && typeof customerEmail === "string" && customerEmail.trim() !== "") {
      sessionParams.customer_email = customerEmail.trim();
    }

    const session = await stripe.checkout.sessions.create(sessionParams);
    res.json({ id: session.id, url: session.url });
  } catch (err) {
    console.error("Stripe Checkout Session Error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ==================================================================
// 3. WEBSOCKET CONNECTION & MULTI-ROOM ROUTING HANDLER
// ==================================================================
wss.on('connection', async (ws, req) => {
  try {
    const urlParams = new URLSearchParams(req.url.split('?')[1]);
    const room = urlParams.get('roomId') || urlParams.get('room') || 'main-stage';
    const role = urlParams.get('role') || 'viewer';
    const lang = urlParams.get('lang') || 'en-US';
    const token = urlParams.get('token');

    // A. Audience / Viewer Connection Handling
    if (role === 'viewer' || role === 'audience') {
      addClientToRoom(room, ws);
      ws.on('close', () => removeClientFromRoom(room, ws));
      return;
    }

    // B. Presenter Connection Handling
    if (role === 'presenter') {
      // 1. Authenticate Token
      if (!token) {
        console.warn(`⚠️ Presenter rejected in [${room}]: Missing session token`);
        ws.close(4003, 'Unauthorized: Missing session token');
        return;
      }

      const { data: { user }, error: authErr } = await supabase.auth.getUser(token);
      if (authErr || !user) {
        console.error(`❌ Presenter auth failure in [${room}]:`, authErr?.message || "Invalid token");
        ws.close(4003, 'Unauthorized: Invalid auth session');
        return;
      }

      // 2. Fetch User Profile & Quota Check
      const { data: dbUser, error: dbErr } = await supabase
        .from("users")
        .select("plan_tier, subscription_status, streaming_seconds_used, max_streaming_seconds")
        .eq("id", user.id)
        .single();

      if (dbErr || !dbUser) {
        console.error(`❌ User lookup failed for ID ${user.id}:`, dbErr?.message);
        ws.close(4003, 'Stream rejected: User profile not found');
        return;
      }

      const isAuthorized =
        dbUser.subscription_status === "active" ||
        dbUser.plan_tier === "free" ||
        dbUser.plan_tier === "one_time";

      if (!isAuthorized) {
        console.warn(`⚠️ Unauthorized user ${user.id}. Tier: ${dbUser.plan_tier}, Status: ${dbUser.subscription_status}`);
        ws.close(4003, 'Stream rejected: Active subscription required');
        return;
      }

      const used = dbUser.streaming_seconds_used || 0;
      let max = dbUser.max_streaming_seconds;

      if (max === null || max === undefined || max === 0) {
        max = dbUser.plan_tier === "free" ? 600 : 0; // 10 minute free allowance
      }

      if (max > 0 && used >= max) {
        console.warn(`⚠️ Quota exhausted for user ${user.id}: Used ${used}s / Allowed ${max}s`);
        ws.close(4006, 'Stream rejected: Streaming quota exhausted');
        return;
      }

      // 3. Open Deepgram Live Transcription Connection (Nova-3 auto-detects browser WebM/Opus)
      const dgConnection = deepgram.listen.live({
        model: 'nova-3',
        language: lang,
        smart_format: true,
        interim_results: true
      });

      let isDeepgramReady = false;

      dgConnection.on(LiveTranscriptionEvents.Open, () => {
        console.log(`🎙️ Deepgram connected for room [${room}]`);
        isDeepgramReady = true;
      });

      dgConnection.on(LiveTranscriptionEvents.Transcript, (data) => {
        const transcript = data.channel?.alternatives[0]?.transcript;
        if (transcript) {
          const payload = {
            type: 'caption',
            text: transcript,
            isFinal: data.is_final
          };

          // Broadcast transcript to viewers in this room
          broadcastToRoom(room, payload);

          // Echo back to presenter console
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify(payload));
          }
        }
      });

      dgConnection.on(LiveTranscriptionEvents.Error, (err) => {
        console.error('❌ Deepgram Error:', err);
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ error: "Transcription service error occurred." }));
        }
      });

      dgConnection.on(LiveTranscriptionEvents.Close, () => {
        console.log(`🔌 Deepgram connection closed for room [${room}]`);
        isDeepgramReady = false;
      });

      // Pass incoming audio chunks from browser to Deepgram
      ws.on('message', (chunk) => {
        if (isDeepgramReady && dgConnection.getReadyState() === 1) {
          dgConnection.send(chunk);
        }
      });

      ws.on('close', (code, reason) => {
        console.log(`🔌 Presenter disconnected from room [${room}]. Code: ${code}`);
        isDeepgramReady = false;
        if (dgConnection) dgConnection.finish();
      });
    }

  } catch (err) {
    console.error('WebSocket connection error:', err);
    ws.close(1011, 'Internal Server Error');
  }
});

// Helper Functions for Room State
function addClientToRoom(room, ws) {
  if (!activeRooms.has(room)) activeRooms.set(room, new Set());
  activeRooms.get(room).add(ws);
}

function removeClientFromRoom(room, ws) {
  if (activeRooms.has(room)) {
    activeRooms.get(room).delete(ws);
    if (activeRooms.get(room).size === 0) activeRooms.delete(room);
  }
}

function broadcastToRoom(room, payload) {
  const clients = activeRooms.get(room);
  if (clients) {
    const msg = JSON.stringify(payload);
    clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(msg);
      }
    });
  }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 Server listening on port ${PORT}`);
});
