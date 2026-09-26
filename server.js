import "dotenv/config";
import express from "express";
import http from "http";
import { WebSocketServer, WebSocket } from "ws";
import Stripe from "stripe";
import { createClient as createSupabaseClient } from "@supabase/supabase-js";
import { createClient as createDeepgramClient, LiveTranscriptionEvents } from "@deepgram/sdk";

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);
const supabase = createSupabaseClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const deepgram = createDeepgramClient(process.env.DEEPGRAM_API_KEY);

const rooms = new Map(); // roomId -> Set<WebSocket>
const deepgramConnections = new Map(); // roomId -> Deepgram Live Connection
const userActivePresenterRooms = new Map(); // userId -> Set<roomId>

// Updated Tier Quotas
const SECONDS_LIMITS = {
  one_time: 2 * 3600,     // 2 Hours (7,200 seconds)
  starter: 30 * 3600,     // 30 Hours (108,000 seconds)
  pro: 150 * 3600         // 150 Hours (540,000 seconds)
};

const ROOM_LIMITS = {
  one_time: 1,
  starter: 2,
  pro: 10
};

// ============================================================================
// 1. STRIPE WEBHOOK ENDPOINT
// ============================================================================
app.post("/api/stripe/webhook", express.raw({ type: "application/json" }), async (req, res) => {
  const sig = req.headers["stripe-signature"];
  let event;

  try {
    event = stripe.webhooks.constructEvent(req.body, sig, process.env.STRIPE_WEBHOOK_SECRET);
  } catch (err) {
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  if (event.type === "checkout.session.completed") {
    const session = event.data.object;
    const userId = session.client_reference_id;
    const planTier = session.metadata?.planTier || "starter";

    if (userId) {
      if (session.mode === "payment") {
        // Event Pass Activation ($29)
        await supabase.from("users").update({
          subscription_status: "active",
          plan_tier: "one_time",
          allowed_rooms: ROOM_LIMITS.one_time,
          max_streaming_seconds: SECONDS_LIMITS.one_time,
          streaming_seconds_used: 0,
          one_time_expires_at: new Date(Date.now() + 24 * 3600 * 1000).toISOString(), // 24hr window
          updated_at: new Date().toISOString()
        }).eq("id", userId);
      } else {
        // Subscription Activation (Starter $49 or Pro $149)
        await supabase.from("users").update({
          subscription_status: "active",
          plan_tier: planTier,
          stripe_customer_id: session.customer,
          allowed_rooms: ROOM_LIMITS[planTier] || 2,
          max_streaming_seconds: SECONDS_LIMITS[planTier] || SECONDS_LIMITS.starter,
          streaming_seconds_used: 0,
          updated_at: new Date().toISOString()
        }).eq("id", userId);
      }
    }
  }

  if (event.type === "customer.subscription.deleted") {
    const subscription = event.data.object;
    await supabase.from("users").update({
      subscription_status: "inactive",
      plan_tier: "free",
      allowed_rooms: 0,
      updated_at: new Date().toISOString()
    }).eq("stripe_customer_id", subscription.customer);
  }

  res.json({ received: true });
});

app.use(express.json());
app.use(express.static("public"));

// ============================================================================
// 2. CHECKOUT CREATION ENDPOINT
// ============================================================================
app.post("/api/create-checkout-session", async (req, res) => {
  try {
    const { priceId, userId, userEmail, checkoutType } = req.body;

    const isOneTime = checkoutType === "one_time";
    const mode = isOneTime ? "payment" : "subscription";

    let planTier = "starter";
    if (isOneTime) planTier = "one_time";
    else if (priceId.includes("PRO")) planTier = "pro";

    const session = await stripe.checkout.sessions.create({
      mode: mode,
      payment_method_types: ["card"],
      customer_email: userEmail,
      client_reference_id: userId,
      metadata: { planTier: planTier },
      line_items: [{ price: priceId, quantity: 1 }],
      success_url: `${req.headers.origin}/dashboard.html?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${req.headers.origin}/pricing.html`,
    });

    res.json({ url: session.url });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ============================================================================
// 3. DEEPGRAM NOVA-3 STT ENGINE
// ============================================================================
function getOrCreateDeepgramConnection(roomId) {
  if (deepgramConnections.has(roomId)) return deepgramConnections.get(roomId);

  const dgSocket = deepgram.listen.live({
    model: "nova-3",
    language: "en",
    smart_format: true,
    interim_results: true,
  });

  dgSocket.on(LiveTranscriptionEvents.Transcript, (data) => {
    const transcript = data.channel?.alternatives?.[0]?.transcript;
    if (transcript) {
      const payload = JSON.stringify({ type: "caption", text: transcript, isFinal: data.is_final });
      const roomClients = rooms.get(roomId);
      if (roomClients) {
        roomClients.forEach((client) => {
          if (client.readyState === WebSocket.OPEN) client.send(payload);
        });
      }
    }
  });

  deepgramConnections.set(roomId, dgSocket);
  return dgSocket;
}

// ============================================================================
// 4. WEBSOCKET GATING & SESSION TRACKING
// ============================================================================
wss.on("connection", async (ws, req) => {
  const urlParams = new URLSearchParams(req.url.replace(/^.*\?/, ""));
  const roomId = urlParams.get("room") || "default-stage";
  const role = urlParams.get("role") || "audience";
  const token = urlParams.get("token");

  let presenterUser = null;
  let sessionStartTime = null;

  if (role === "presenter") {
    if (!token) return ws.close(4001, "Auth token required");

    const { data: { user }, error } = await supabase.auth.getUser(token);
    if (error || !user) return ws.close(4002, "Invalid auth token");

    const { data: dbUser } = await supabase.from("users").select("*").eq("id", user.id).single();

    if (!dbUser || dbUser.subscription_status !== "active") {
      return ws.close(4003, "Active subscription or event pass required");
    }

    if (dbUser.plan_tier === "one_time" && dbUser.one_time_expires_at) {
      if (new Date() > new Date(dbUser.one_time_expires_at)) {
        return ws.close(4005, "Event pass has expired");
      }
    }

    if (dbUser.streaming_seconds_used >= dbUser.max_streaming_seconds) {
      return ws.close(4006, "Streaming hours quota exhausted for billing period");
    }

    const activeUserRooms = userActivePresenterRooms.get(user.id) || new Set();
    if (!activeUserRooms.has(roomId) && activeUserRooms.size >= dbUser.allowed_rooms) {
      return ws.close(4004, `Max room limit (${dbUser.allowed_rooms}) reached for plan`);
    }

    activeUserRooms.add(roomId);
    userActivePresenterRooms.set(user.id, activeUserRooms);
    presenterUser = dbUser;
    sessionStartTime = Date.now();
  }

  if (!rooms.has(roomId)) rooms.set(roomId, new Set());
  rooms.get(roomId).add(ws);

  let dgSocket = role === "presenter" ? getOrCreateDeepgramConnection(roomId) : null;

  ws.on("message", (data) => {
    if (role === "presenter" && dgSocket && dgSocket.getReadyState() === 1) {
      dgSocket.send(data);
    }
  });

  ws.on("close", async () => {
    const roomClients = rooms.get(roomId);
    if (roomClients) {
      roomClients.delete(ws);
      if (roomClients.size === 0) {
        rooms.delete(roomId);
        if (dgSocket) {
          dgSocket.finish();
          deepgramConnections.delete(roomId);
        }
      }
    }

    if (role === "presenter" && presenterUser && sessionStartTime) {
      const elapsedSeconds = Math.ceil((Date.now() - sessionStartTime) / 1000);
      const activeUserRooms = userActivePresenterRooms.get(presenterUser.id);
      if (activeUserRooms) {
        activeUserRooms.delete(roomId);
        if (activeUserRooms.size === 0) userActivePresenterRooms.delete(presenterUser.id);
      }

      const { data } = await supabase.from("users").select("streaming_seconds_used").eq("id", presenterUser.id).single();
      if (data) {
        await supabase.from("users").update({
          streaming_seconds_used: (data.streaming_seconds_used || 0) + elapsedSeconds
        }).eq("id", presenterUser.id);
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
