import express from "express";
import http from "http";
import { WebSocketServer, WebSocket } from "ws";
import Stripe from "stripe";
import { createClient as createSupabaseClient } from "@supabase/supabase-js";
import { createClient as createDeepgramClient, LiveTranscriptionEvents } from "@deepgram/sdk";

// --- ENVIRONMENT & CLIENT INITIALIZATION ---
const requiredEnv = [
  "STRIPE_SECRET_KEY",
  "SUPABASE_URL",
  "SUPABASE_SERVICE_ROLE_KEY",
  "DEEPGRAM_API_KEY"
];

for (const key of requiredEnv) {
  if (!process.env[key]) {
    console.warn(`⚠️ Warning: Environment variable ${key} is missing!`);
  }
}

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const stripe = new Stripe(process.env.STRIPE_SECRET_KEY || "");
const supabase = createSupabaseClient(
  process.env.SUPABASE_URL || "",
  process.env.SUPABASE_SERVICE_ROLE_KEY || ""
);
const deepgram = createDeepgramClient(process.env.DEEPGRAM_API_KEY || "");

// State Tracking Maps
const rooms = new Map(); // roomId -> Set<WebSocket>
const deepgramConnections = new Map(); // roomId -> Deepgram Live Connection
const userActivePresenterRooms = new Map(); // userId -> Set<roomId>

// Middlewares
app.use(express.json());
app.use(express.static("public"));

// --- STRIPE CHECKOUT ENDPOINT ---
app.post("/api/create-checkout-session", async (req, res) => {
  try {
    const { priceId, mode } = req.body;

    if (!priceId) {
      return res.status(400).json({ error: "Missing priceId in request payload" });
    }

    const session = await stripe.checkout.sessions.create({
      payment_method_types: ["card"],
      line_items: [{ price: priceId, quantity: 1 }],
      mode: mode || "subscription",
      success_url: `${req.headers.origin || "https://aicaptions-tkoc.onrender.com"}/dashboard.html?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${req.headers.origin || "https://aicaptions-tkoc.onrender.com"}/pricing.html`,
    });

    res.json({ url: session.url });
  } catch (err) {
    console.error("Stripe Session Creation Failed:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// --- WEBSOCKET STREAMING HANDLER ---
wss.on("connection", async (ws, req) => {
  const urlParams = new URLSearchParams(req.url.split("?")[1]);
  const role = urlParams.get("role"); // "presenter" or "audience"
  const roomId = urlParams.get("roomId");
  const token = urlParams.get("token");

  if (!roomId || !role) {
    ws.send(JSON.stringify({ type: "error", message: "Missing roomId or role" }));
    return ws.close(4000, "Missing parameters");
  }

  // AUDIENCE ROOM SUBSCRIPTION
  if (role === "audience") {
    if (!rooms.has(roomId)) {
      rooms.set(roomId, new Set());
    }
    rooms.get(roomId).add(ws);

    ws.on("close", () => {
      const room = rooms.get(roomId);
      if (room) {
        room.delete(ws);
        if (room.size === 0) rooms.delete(roomId);
      }
    });
    return;
  }

  // PRESENTER BROADCAST SETUP
  if (role === "presenter") {
    let userId = null;

    try {
      if (!token) {
        ws.send(JSON.stringify({ type: "error", message: "Authentication token required" }));
        return ws.close(4001, "Auth required");
      }

      const { data: authData, error: authError } = await supabase.auth.getUser(token);
      if (authError || !authData.user) {
        console.error("Auth error:", authError);
        ws.send(JSON.stringify({ type: "error", message: "Invalid or expired token" }));
        return ws.close(4001, "Invalid token");
      }

      userId = authData.user.id;

      const { data: dbUser, error: dbError } = await supabase
        .from("users")
        .select("*")
        .eq("id", userId)
        .single();

      if (dbError || !dbUser) {
        console.error("DB User fetch error:", dbError);
        ws.send(JSON.stringify({ type: "error", message: "User profile not found" }));
        return ws.close(4002, "User not found");
      }

      if (dbUser.subscription_status !== "active") {
        ws.send(JSON.stringify({ type: "error", message: "Active subscription required to stream" }));
        return ws.close(4002, "Subscription inactive");
      }

      const maxAllowedSeconds = Number(dbUser.max_streaming_seconds || dbUser.max_streaming_settings) || 7200;
      const usedSeconds = Number(dbUser.streaming_seconds_used) || 0;

      if (usedSeconds >= maxAllowedSeconds) {
        ws.send(JSON.stringify({ type: "error", message: "Streaming quota exceeded" }));
        return ws.close(4003, "Quota exceeded");
      }

      // Initialize Deepgram Live Connection
      const dgConnection = deepgram.listen.live({
        model: "nova-2",
        language: "en-US",
        smart_format: true,
        interim_results: true
      });

      let isDeepgramReady = false;
      const audioBufferQueue = [];

      dgConnection.on(LiveTranscriptionEvents.Open, () => {
        console.log(`Deepgram connected for room: ${roomId}`);
        isDeepgramReady = true;

        while (audioBufferQueue.length > 0) {
          const chunk = audioBufferQueue.shift();
          try {
            dgConnection.send(chunk);
          } catch (e) {
            console.error("Error flushing audio buffer:", e);
          }
        }
      });

      dgConnection.on(LiveTranscriptionEvents.Transcript, (data) => {
        const transcript = data.channel?.alternatives[0]?.transcript;
        if (transcript) {
          const messagePayload = JSON.stringify({
            type: "transcript",
            text: transcript,
            isFinal: data.is_final
          });

          if (ws.readyState === WebSocket.OPEN) {
            ws.send(messagePayload);
          }

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
        console.error("Deepgram Error:", err);
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: "error", message: "Deepgram error: " + (err.message || "Transcription failed") }));
        }
      });

      dgConnection.on(LiveTranscriptionEvents.Close, () => {
        console.log(`Deepgram closed for room: ${roomId}`);
      });

      deepgramConnections.set(roomId, dgConnection);

      if (!userActivePresenterRooms.has(userId)) {
        userActivePresenterRooms.set(userId, new Set());
      }
      userActivePresenterRooms.get(userId).add(roomId);

      ws.on("message", (data) => {
        if (Buffer.isBuffer(data) || data instanceof ArrayBuffer) {
          if (isDeepgramReady) {
            try {
              dgConnection.send(data);
            } catch (err) {
              console.error("Error sending chunk to Deepgram:", err);
            }
          } else if (audioBufferQueue.length < 100) {
            audioBufferQueue.push(data);
          }
        }
      });

      ws.on("close", () => {
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
      console.error("Server WebSocket presenter error:", err);
      ws.send(JSON.stringify({ type: "error", message: "Internal server error starting stream" }));
      ws.close(1011, "Server error");
    }
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
