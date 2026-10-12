import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import { createClient } from "@deepgram/sdk";
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

// --- Client & API Initialization ---------------------------------------
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

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// --- Stripe Checkout Session Endpoint ----------------------------------
app.post("/create-checkout-session", async (req, res) => {
  try {
    const { priceId, userId, userEmail, mode } = req.body;
    const session = await stripe.checkout.sessions.create({
      payment_method_types: ["card"],
      mode: mode || "subscription",
      customer_email: userEmail,
      line_items: [{ price: priceId, quantity: 1 }],
      success_url: `${req.protocol}://${req.get("host")}/dashboard.html?success=true`,
      cancel_url: `${req.protocol}://${req.get("host")}/pricing.html?canceled=true`,
      metadata: { user_id: userId }
    });
    res.json({ url: session.url });
  } catch (err) {
    console.error("Stripe error:", err);
    res.status(500).json({ error: err.message });
  }
});

// --- In-Memory Room & Client Management ---------------------------------
const rooms = new Map(); // roomId -> Set of WebSocket clients (viewers, overlays)

// --- WebSocket Connection Routing --------------------------------------
wss.on("connection", (ws, req) => {
  const parsedUrl = new URL(req.url, `http://${req.headers.host}`);
  const pathname = parsedUrl.pathname;
  const room = parsedUrl.searchParams.get("room") || "main-stage";

  // 1. Presenter Ingest Stream (/ws/ingest)
  if (pathname.includes("/ws/ingest")) {
    const spokenLang = parsedUrl.searchParams.get("spokenLang") || "en";
    console.log(`[Presenter Connected] Room: ${room}, Spoken Language: ${spokenLang}`);

    if (!rooms.has(room)) {
      rooms.set(room, new Set());
    }

    // Initialize Deepgram Live Transcription with the presenter's selected spoken language
    const dgConnection = deepgram.listen.live({
      model: "nova-3",
      language: spokenLang, // Dynamically sets language (e.g. 'fr', 'es', 'en')
      smart_format: true,
      encoding: "linear16",
      sample_rate: 16000
    });

    dgConnection.on("open", () => {
      console.log(`[Deepgram] Connected for room ${room} using language: ${spokenLang}`);
    });

    dgConnection.on("transcript", async (data) => {
      const sentence = data.channel?.alternatives?.[0]?.transcript;
      if (sentence && sentence.trim() !== "") {
        const isFinal = data.is_final;
        const payload = JSON.stringify({ text: sentence, isFinal });

        // Broadcast transcription to all room clients (overlays, viewers, attendees)
        if (rooms.has(room)) {
          for (const client of rooms.get(room)) {
            if (client.readyState === WebSocket.OPEN) {
              client.send(payload);
            }
          }
        }

        // Optional: Save final transcripts to Supabase
        if (isFinal) {
          try {
            await supabase.from("transcripts").insert([
              { room_id: room, text: sentence, created_at: new Date() }
            ]);
          } catch (dbErr) {
            // Ignore DB errors if table doesn't exist
          }
        }
      }
    });

    dgConnection.on("error", (err) => {
      console.error("[Deepgram Error]:", err);
    });

    dgConnection.on("close", () => {
      console.log(`[Deepgram] Closed for room ${room}`);
    });

    ws.on("message", (message) => {
      // Forward raw audio chunks from presenter mic to Deepgram
      if (dgConnection.getReadyState() === 1) {
        dgConnection.send(message);
      }
    });

    ws.on("close", () => {
      console.log(`[Presenter Disconnected] Room: ${room}`);
      try {
        dgConnection.finish();
      } catch (e) {}
    });

    return;
  }

  // 2. Viewer, Attendee, & Overlay Streams (/ws/viewer, /ws/attendee, /ws/overlay)
  if (
    pathname.includes("/ws/viewer") ||
    pathname.includes("/ws/attendee") ||
    pathname.includes("/ws/overlay")
  ) {
    console.log(`[Client Connected] Room: ${room} on ${pathname}`);

    if (!rooms.has(room)) {
      rooms.set(room, new Set());
    }
    rooms.get(room).add(ws);

    ws.on("close", () => {
      if (rooms.has(room)) {
        rooms.get(room).delete(ws);
        if (rooms.get(room).size === 0) {
          rooms.delete(room);
        }
      }
      console.log(`[Client Disconnected] Room: ${room}`);
    });
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
