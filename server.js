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

// Initialize Clients
const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);

const supabase = createSupabaseClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const deepgram = createDeepgramClient(process.env.DEEPGRAM_API_KEY);

// State Management: Rooms & Deepgram Connections
const rooms = new Map(); // roomId -> Set<WebSocket>
const deepgramConnections = new Map(); // roomId -> Deepgram Live Connection

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
        // Upgrade account in Supabase
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
app.use(express.static("public"));

// ==================================================================
// 2. STRIPE CHECKOUT SESSION ENDPOINT (Guards client_reference_id)
// ==================================================================
app.post("/create-checkout-session", async (req, res) => {
  const { priceId, userId, customerEmail } = req.body;

  // Prevent empty or unauthenticated client_reference_id error
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
// 3. WEBSOCKET CONNECTION & AUTHORIZATION HANDLER
// ==================================================================
wss.on("connection", async (ws, req) => {
  const urlParams = new URLSearchParams(req.url.split("?")[1]);
  const token = urlParams.get("token");
  const roomId = urlParams.get("roomId") || "default";
  const role = urlParams.get("role") || "viewer";

  // A. Audience Viewers Connection
  if (role === "viewer") {
    if (!rooms.has(roomId)) {
      rooms.set(roomId, new Set());
    }
    rooms.get(roomId).add(ws);

    ws.on("close", () => {
      if (rooms.has(roomId)) {
        rooms.get(roomId).delete(ws);
      }
    });
    return;
  }

  // B. Presenter Connection: Authentication Check
  if (!token) {
    ws.close(4003, "Stream rejected: Missing authentication token.");
    return;
  }

  const { data: { user }, error: authErr } = await supabase.auth.getUser(token);
  if (authErr || !user) {
    ws.close(4003, "Stream rejected: Invalid or expired token.");
    return;
  }

  // C. Fetch Plan & Quota Info
  const { data: dbUser, error: dbErr } = await supabase
    .from("users")
    .select("plan_tier, subscription_status, streaming_seconds_used, max_streaming_seconds")
    .eq("id", user.id)
    .single();

  if (dbErr || !dbUser) {
    ws.close(4003, "Stream rejected: User profile not found.");
    return;
  }

  // Allow Active Subscriptions, One-Time Passes, and Free Passes
  const isAuthorized =
    dbUser.subscription_status === "active" ||
    dbUser.plan_tier === "free" ||
    dbUser.plan_tier === "one_time";

  if (!isAuthorized) {
    ws.close(4003, "Stream rejected: Active subscription required.");
    return;
  }

  // Quota Exhaustion Check
  const used = dbUser.streaming_seconds_used || 0;
  const max = dbUser.max_streaming_seconds || 0;
  if (used >= max) {
    ws.close(4006, "Stream rejected: Streaming quota exhausted.");
    return;
  }

  // D. Deepgram API Key Check
  if (!process.env.DEEPGRAM_API_KEY) {
    console.error("❌ CRITICAL: DEEPGRAM_API_KEY environment variable missing.");
    ws.send(JSON.stringify({ error: "Server configuration error: Deepgram API Key missing." }));
    ws.close(1011, "Internal Server Error: Missing Deepgram API Key.");
    return;
  }

 // E. Initialize Deepgram Connection
  try {
    // 1. Omit encoding/sample_rate so Deepgram auto-detects browser WebM/Opus audio
    const deepgramLive = deepgram.listen.live({
      model: "nova-3",
      language: "en-US",
      smart_format: true,
      interim_results: true
    });

    let isDeepgramReady = false;

    // 2. Set ready flag ONLY when Deepgram socket emits Open
    deepgramLive.on(LiveTranscriptionEvents.Open, () => {
      console.log(`🎙️ Deepgram connection opened for room: ${roomId}`);
      isDeepgramReady = true;
    });

    deepgramLive.on(LiveTranscriptionEvents.Transcript, (data) => {
      const captionText = data.channel?.alternatives[0]?.transcript;
      if (captionText && rooms.has(roomId)) {
        const payload = JSON.stringify({
          type: "caption",
          text: captionText,
          isFinal: data.is_final
        });

        rooms.get(roomId).forEach((client) => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(payload);
          }
        });
      }
    });

    deepgramLive.on(LiveTranscriptionEvents.Error, (err) => {
      console.error("❌ Deepgram Error:", err);
      ws.send(JSON.stringify({ error: "Transcription error occurred." }));
    });

    deepgramLive.on(LiveTranscriptionEvents.Close, () => {
      console.log(`Deepgram connection closed for room: ${roomId}`);
      isDeepgramReady = false;
    });

    deepgramConnections.set(roomId, deepgramLive);

    // 3. Forward audio chunks ONLY when Deepgram is verified open
    ws.on("message", (message) => {
      if (isDeepgramReady && deepgramLive.getReadyState() === 1) {
        deepgramLive.send(message);
      }
    });

    ws.on("close", (code, reason) => {
      console.warn(`⚠️ Presenter disconnected. Code: ${code}, Reason: ${reason.toString()}`);
      isDeepgramReady = false;
      if (deepgramConnections.has(roomId)) {
        deepgramConnections.get(roomId).finish();
        deepgramConnections.delete(roomId);
      }
    });
  } catch (err) {
    console.error("Failed to establish Deepgram connection:", err.message);
    ws.close(1011, "Failed to connect to Deepgram transcription service.");
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 Server listening on port ${PORT}`);
});
