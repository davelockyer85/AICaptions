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

// Initialize Stripe, Supabase (Service Role), and Deepgram Clients
const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);

const supabase = createSupabaseClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const deepgram = createDeepgramClient(process.env.DEEPGRAM_API_KEY);

// State Management: Active Rooms & Deepgram WebSocket Connections
const rooms = new Map(); // roomId -> Set<WebSocket>
const deepgramConnections = new Map(); // roomId -> Deepgram Live Connection

// ============================================================================
// 1. STRIPE WEBHOOK ENDPOINT (Must come BEFORE express.json() middleware)
// ============================================================================
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
      console.error(`❌ Webhook Signature Error: ${err.message}`);
      return res.status(400).send(`Webhook Error: ${err.message}`);
    }

    try {
      // 1. New Subscription Successful
      if (event.type === "checkout.session.completed") {
        const session = event.data.object;
        const userId = session.client_reference_id;
        const stripeCustomerId = session.customer;

        console.log(`✅ Subscription checkout completed for User ID: ${userId}`);

        if (userId) {
          await supabase
            .from("users")
            .update({
              subscription_status: "active",
              stripe_customer_id: stripeCustomerId,
              updated_at: new Date().toISOString(),
            })
            .eq("id", userId);
        }
      }

      // 2. Subscription Status Updated (Renewals, Cancellations, Past Due)
      if (event.type === "customer.subscription.updated") {
        const subscription = event.data.object;
        const status = subscription.status;
        const stripeCustomerId = subscription.customer;

        await supabase
          .from("users")
          .update({
            subscription_status: status === "active" ? "active" : "inactive",
            updated_at: new Date().toISOString(),
          })
          .eq("stripe_customer_id", stripeCustomerId);
      }

      // 3. Subscription Deleted / Expired
      if (event.type === "customer.subscription.deleted") {
        const subscription = event.data.object;
        const stripeCustomerId = subscription.customer;

        console.log(`⚠️ Subscription canceled for customer: ${stripeCustomerId}`);

        await supabase
          .from("users")
          .update({
            subscription_status: "inactive",
            updated_at: new Date().toISOString(),
          })
          .eq("stripe_customer_id", stripeCustomerId);
      }

      res.json({ received: true });
    } catch (dbErr) {
      console.error("❌ Database sync error in webhook:", dbErr);
      res.status(500).json({ error: "Webhook DB sync failed" });
    }
  }
);

// ============================================================================
// 2. MIDDLEWARE & STATIC FILES
// ============================================================================
app.use(express.json());
app.use(express.static("public"));

// Helper Middleware for Protected HTTP Endpoints
async function requireActiveSubscription(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: "Missing authorization header" });

  const token = authHeader.split(" ")[1];
  const { data: { user }, error } = await supabase.auth.getUser(token);

  if (error || !user) return res.status(401).json({ error: "Invalid auth token" });

  const { data: dbUser } = await supabase
    .from("users")
    .select("subscription_status")
    .eq("id", user.id)
    .single();

  if (dbUser?.subscription_status !== "active") {
    return res.status(403).json({ error: "Active paid subscription required" });
  }

  req.user = user;
  next();
}

// ============================================================================
// 3. STRIPE CHECKOUT SESSION ENDPOINT
// ============================================================================
app.post("/api/create-checkout-session", async (req, res) => {
  try {
    const { priceId, userId, userEmail } = req.body;

    if (!priceId || !userId) {
      return res.status(400).json({ error: "Missing required parameters: priceId or userId" });
    }

    const session = await stripe.checkout.sessions.create({
      mode: "subscription",
      payment_method_types: ["card"],
      customer_email: userEmail,
      client_reference_id: userId, // Pass Supabase User ID for webhook matching
      line_items: [{ price: priceId, quantity: 1 }],
      success_url: `${req.headers.origin}/dashboard.html?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${req.headers.origin}/pricing.html`,
    });

    res.json({ url: session.url });
  } catch (error) {
    console.error("❌ Stripe Checkout Error:", error);
    res.status(500).json({ error: error.message });
  }
});

// ============================================================================
// 4. DEEPGRAM NOVA-3 STT ENGINE
// ============================================================================
function getOrCreateDeepgramConnection(roomId) {
  if (deepgramConnections.has(roomId)) {
    return deepgramConnections.get(roomId);
  }

  console.log(`🎙️ Initializing Deepgram Nova-3 for room: ${roomId}`);

  const dgSocket = deepgram.listen.live({
    model: "nova-3",
    language: "en",
    smart_format: true,
    interim_results: true,
  });

  dgSocket.on(LiveTranscriptionEvents.Open, () => {
    console.log(`⚡ Deepgram connected for room: ${roomId}`);
  });

  dgSocket.on(LiveTranscriptionEvents.Transcript, (data) => {
    const transcript = data.channel?.alternatives?.[0]?.transcript;
    if (transcript) {
      const isFinal = data.is_final;
      const payload = JSON.stringify({
        type: "caption",
        text: transcript,
        isFinal: isFinal,
      });

      // Broadcast transcript to overlays and audience members in the room
      const roomClients = rooms.get(roomId);
      if (roomClients) {
        roomClients.forEach((client) => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(payload);
          }
        });
      }
    }
  });

  dgSocket.on(LiveTranscriptionEvents.Error, (err) => {
    console.error(`❌ Deepgram Error [Room ${roomId}]:`, err);
  });

  dgSocket.on(LiveTranscriptionEvents.Close, () => {
    console.log(`🔌 Deepgram connection closed for room: ${roomId}`);
    deepgramConnections.delete(roomId);
  });

  deepgramConnections.set(roomId, dgSocket);
  return dgSocket;
}

// ============================================================================
// 5. WEBSOCKET SERVER & PRESENTER ACCESS GATING
// ============================================================================
wss.on("connection", async (ws, req) => {
  const urlParams = new URLSearchParams(req.url.replace(/^.*\?/, ""));
  const roomId = urlParams.get("room") || "default-stage";
  const role = urlParams.get("role") || "audience";
  const token = urlParams.get("token");

  // Gate audio stream creation for presenters
  if (role === "presenter") {
    if (!token) {
      console.warn(`🔒 Presenter rejected [Room: ${roomId}]: Missing Token`);
      ws.close(4001, "Authentication token required");
      return;
    }

    // Verify user authentication with Supabase
    const { data: { user }, error } = await supabase.auth.getUser(token);
    if (error || !user) {
      console.warn(`🔒 Presenter rejected [Room: ${roomId}]: Invalid Token`);
      ws.close(4002, "Invalid authentication token");
      return;
    }

    // Verify active subscription status in database
    const { data: dbUser } = await supabase
      .from("users")
      .select("subscription_status")
      .eq("id", user.id)
      .single();

    if (dbUser?.subscription_status !== "active") {
      console.warn(`🔒 Presenter rejected [Room: ${roomId}]: Inactive Subscription`);
      ws.close(4003, "Active paid subscription required to stream captions");
      return;
    }
  }

  console.log(`🔌 Client connected to room: [${roomId}] as (${role})`);

  // Track connected clients per room
  if (!rooms.has(roomId)) {
    rooms.set(roomId, new Set());
  }
  rooms.get(roomId).add(ws);

  let dgSocket = null;
  if (role === "presenter") {
    dgSocket = getOrCreateDeepgramConnection(roomId);
  }

  // Route incoming audio chunks to Deepgram Nova-3
  ws.on("message", (data) => {
    if (role === "presenter" && dgSocket && dgSocket.getReadyState() === 1) {
      dgSocket.send(data);
    }
  });

  // Handle disconnection and clean up idle Deepgram connections
  ws.on("close", () => {
    console.log(`❌ Client disconnected from room: [${roomId}]`);
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
  });
});

// ============================================================================
// 6. START SERVER
// ============================================================================
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 AICaptions Server running on port ${PORT}`);
});
