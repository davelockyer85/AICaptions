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

// Initialize Stripe & Service Clients
const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
    console.error("❌ CRITICAL: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.");
}

const deepgram = createDeepgramClient(DEEPGRAM_API_KEY);
const supabase = createSupabaseClient(SUPABASE_URL, SUPABASE_KEY);

// Track active rooms (simplified room management)
const rooms = new Map(); // roomId -> { presenterWs, viewers: Set }

app.use(express.json());
app.use(express.static('public'));

// ---------------------------------------------------------
// STRIPE CHECKOUT ROUTE
// ---------------------------------------------------------
app.post("/create-checkout-session", async (req, res) => {
  try {
    const { priceId, userId } = req.body;

    if (!priceId) {
      return res.status(400).json({ error: "Missing priceId" });
    }

    // Compare incoming price ID against our one-time products
    const oneTimePrices = [
      process.env.EVENT_PASS_PRICE_ID,
      process.env.PRO_EVENT_PASS_PRICE_ID
    ].filter(Boolean);

    const isOneTimePurchase = oneTimePrices.includes(priceId);

    const session = await stripe.checkout.sessions.create({
      payment_method_types: ["card"],
      line_items: [
        {
          price: priceId,
          quantity: 1,
        },
      ],
      // Dynamically switch modes
      mode: isOneTimePurchase ? "payment" : "subscription",
      client_reference_id: userId || null,
      success_url: `${process.env.CLIENT_URL || req.headers.origin}/dashboard.html?success=true`,
      cancel_url: `${process.env.CLIENT_URL || req.headers.origin}/pricing.html?canceled=true`,
    });

    res.json({ url: session.url });
  } catch (error) {
    console.error("Stripe session error:", error);
    res.status(500).json({ error: error.message });
  }
});

// ---------------------------------------------------------
// WEBSOCKET & DEEPGRAM AUDIO STREAMING
// ---------------------------------------------------------
wss.on('connection', async (ws, req) => {
  const urlParams = new URLSearchParams(req.url.split('?')[1]);
  const roomId = urlParams.get('roomId') || 'main-stage';
  const role = urlParams.get('role') || 'viewer';
  const token = urlParams.get('token');
  const lang = urlParams.get('lang') || 'en-US';

  // --- PRESENTER LOGIC ---
  if (role === 'presenter') {
    if (!token) {
      ws.close(4003, "Authentication token missing");
      return;
    }
    
    // Verify user identity securely via Supabase
    const { data: { user }, error } = await supabase.auth.getUser(token);
    if (error || !user) {
      ws.close(4003, "Invalid or expired token");
      return;
    }

    if (!rooms.has(roomId)) {
      rooms.set(roomId, { presenterWs: null, viewers: new Set() });
    }
    const room = rooms.get(roomId);
    
    // Close existing presenter if someone new connects
    if (room.presenterWs && room.presenterWs.readyState === WebSocket.OPEN) {
      room.presenterWs.close(4000, "New presenter connected");
    }
    room.presenterWs = ws;

    let deepgramLive;
    try {
      deepgramLive = deepgram.listen.live({
        model: 'nova-2',
        language: lang,
        smart_format: true,
        encoding: 'webm/opus',
        sample_rate: 48000,
      });

      deepgramLive.on(LiveTranscriptionEvents.Transcript, (data) => {
        const sentence = data.channel?.alternatives?.[0]?.transcript;
        if (sentence && sentence.trim() !== "") {
          const payload = JSON.stringify({
            type: 'caption',
            text: sentence,
            isFinal: data.is_final
          });

          // Echo back to presenter
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(payload);
          }

          // Broadcast to all viewers
          room.viewers.forEach(viewer => {
            if (viewer.readyState === WebSocket.OPEN) {
              viewer.send(payload);
            }
          });
        }
      });
    } catch (dgErr) {
      console.error("Deepgram connection error:", dgErr);
    }

    ws.on('message', (message) => {
      // Forward raw audio blob to Deepgram
      if (deepgramLive && typeof message !== 'string') {
        deepgramLive.send(message);
      }
    });

    ws.on('close', () => {
      if (deepgramLive) deepgramLive.finish();
      rooms.delete(roomId);
    });

  // --- VIEWER LOGIC ---
  } else {
    if (!rooms.has(roomId)) {
      rooms.set(roomId, { presenterWs: null, viewers: new Set() });
    }
    const room = rooms.get(roomId);
    room.viewers.add(ws);

    ws.on('close', () => {
      room.viewers.delete(ws);
    });
  }
});

// ---------------------------------------------------------
// SERVER BOOT
// ---------------------------------------------------------
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(🚀 Server running on port ${PORT});
});
