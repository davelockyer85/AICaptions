import express from "express";
import http from "http";
import { WebSocketServer, WebSocket } from "ws";
import Stripe from "stripe";
import { createClient as createSupabaseClient } from "@supabase/supabase-js";
import { createClient as createDeepgramClient, LiveTranscriptionEvents } from "@deepgram/sdk";

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.json());
app.use(express.static("public"));

// Initialization
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

// Stripe Price ID to Tier Map
const PRICE_TIER_MAP = {
  "price_1UJtjIjV4dyhvuKyHFiWUnuI": "one_time", // $49 Event Pass
  "price_1UJYbIJV4dyhvuKy8pXdPHbU": "starter",  // $129 Starter Plan
  "price_1UJtlKJV4dyhvuKy67mqD8tW": "pro"       // $259 Pro Plan
};

// Quotas & Limits
const SECONDS_LIMITS = {
  one_time: 2 * 3600,  // 2 Hours
  starter: 30 * 3600,  // 30 Hours
  pro: 150 * 3600      // 150 Hours
};

const ROOM_LIMITS = {
  one_time: 1,
  starter: 2,
  pro: 10
};

// WebSocket Handler
wss.on("connection", async (ws, req) => {
  const urlParams = new URLSearchParams(req.url.split("?")[1]);
  const role = urlParams.get("role"); // "presenter" or "audience"
  const roomId = urlParams.get("roomId");
  const token = urlParams.get("token");

  if (!roomId || !role) {
    ws.send(JSON.stringify({ type: "error", message: "Missing roomId or role" }));
    return ws.close(4000, "Missing parameters");
  }

  // --- AUDIENCE CONNECTION ---
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

  // --- PRESENTER CONNECTION ---
  if (role === "presenter") {
    let userId = null;

    try {
      // 1. Verify User Auth Token
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

      // 2. Fetch User Profile & Subscription Check
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

      if ((dbUser.streaming_seconds_used || 0) >= (dbUser.max_streaming_seconds || 7200)) {
        ws.send(JSON.stringify({ type: "error", message: "Streaming quota exceeded" }));
        return ws.close(4003, "Quota exceeded");
      }

      // 3. Setup Deepgram Live Client
      const dgConnection = deepgram.listen.live({
        model: "nova-2",
        language: "en-US",
        smart_format: true,
        interim_results: true,
        encoding: "webm-opus"
      });

      let isDeepgramReady = false;
      const audioBufferQueue = [];

      dgConnection.on(LiveTranscriptionEvents.Open, () => {
        console.log(`Deepgram connected for room: ${roomId}`);
        isDeepgramReady = true;

        // Flush any audio chunks received while waiting for Deepgram open
        while (audioBufferQueue.length > 0) {
          const chunk = audioBufferQueue.shift();
          dgConnection.send(chunk);
        }
      });

      // Handle Transcripts from Deepgram -> Broadcast to Audience
      dgConnection.on(LiveTranscriptionEvents.Transcript, (data) => {
        const transcript = data.channel?.alternatives[0]?.transcript;
        if (transcript) {
          const messagePayload = JSON.stringify({
            type: "transcript",
            text: transcript,
            isFinal: data.is_final
          });

          // Send back to presenter for live preview
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(messagePayload);
          }

          // Broadcast to audience room
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
      });

      dgConnection.on(LiveTranscriptionEvents.Close, () => {
        console.log(`Deepgram closed for room: ${roomId}`);
      });

      deepgramConnections.set(roomId, dgConnection);

      // Track active room for presenter
      if (!userActivePresenterRooms.has(userId)) {
        userActivePresenterRooms.set(userId, new Set());
      }
      userActivePresenterRooms.get(userId).add(roomId);

      // 4. Listen for Audio Data from Presenter Client
      ws.on("message", (data) => {
        if (Buffer.isBuffer(data) || data instanceof ArrayBuffer) {
          if (isDeepgramReady) {
            dgConnection.send(data);
          } else {
            audioBufferQueue.push(data);
          }
        }
      });

      // 5. Cleanup on Presenter Disconnect
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
