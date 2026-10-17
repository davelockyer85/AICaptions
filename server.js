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

// --- Google Translate Helper ------------------------------------------------
async function translateText(text, sourceLang, targetLang) {
  if (!text || !targetLang || targetLang === sourceLang) return text;
  const sl = (sourceLang || "en").split("-")[0];
  const tl = (targetLang || "en").split("-")[0];
  const url = `https://translate.googleapis.com/translate_a/single?client=gtx&sl=${sl}&tl=${tl}&dt=t&q=${encodeURIComponent(text)}`;

  try {
    const response = await fetch(url);
    const data = await response.json();
    if (data && data[0]) {
      return data[0].map(item => item[0]).join('');
    }
  } catch (err) {
    console.error("[Translation Error]", err);
  }
  return text;
}

// --- Multi-Tenant Rooms Store ------------------------------------------
const rooms = new Map();

function getOrCreateRoom(roomId) {
  if (!rooms.has(roomId)) {
    console.log(`[Room Created] Initializing room: ${roomId}`);
    rooms.set(roomId, {
      spokenLang: "en-US",
      translateEnabled: false,
      targetOverlayLang: "en",
      overlays: new Set(),
      attendees: new Set(),
      dgLive: null,
      audioQueue: [],
      isDgReady: false,
      overlayStyles: {
        fontSize: '32px',
        fontFamily: 'sans-serif',
        fontWeight: 'normal',
        fontStyle: 'normal',
        animation: 'none',
        textColor: '#ffffff'
      }
    });
  }
  return rooms.get(roomId);
}

function cleanupRoom(roomId) {
  const room = rooms.get(roomId);
  if (room && room.overlays.size === 0 && room.attendees.size === 0 && !room.dgLive) {
    console.log(`[Room Destroyed] Cleaning up empty room: ${roomId}`);
    rooms.delete(roomId);
  }
}

// ==========================================
// 1. STRIPE WEBHOOK ENDPOINT
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
    const mode = session.mode;

    if (userId) {
      const isOneTime = mode === 'payment';
      const planTier = isOneTime ? 'one_time' : 'subscriber';
      const expiresAt = isOneTime ? new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString() : null;
      const maxStreamingSeconds = isOneTime ? 36000 : null;
      const allowedRooms = isOneTime ? 1 : 3;

      await supabase
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
    }
  }

  res.json({ received: true });
});

// --- Standard Express Middleware & Static Files ------------------------
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

// ==========================================
// 2. STRIPE CHECKOUT SESSION ENDPOINT
// ==========================================
app.post('/create-checkout-session', async (req, res) => {
  try {
    const { priceId, userId, mode } = req.body;
    if (!priceId) return res.status(400).json({ error: 'Missing priceId' });

    const oneTimePrices = [
      process.env.EVENT_PASS_PRICE_ID,
      process.env.PRO_EVENT_PASS_PRICE_ID
    ].filter(Boolean);

    let sessionMode = mode || (oneTimePrices.includes(priceId.trim()) ? 'payment' : 'subscription');

    const session = await stripe.checkout.sessions.create({
      payment_method_types: ['card'],
      line_items: [{ price: priceId.trim(), quantity: 1 }],
      mode: sessionMode,
      client_reference_id: userId || null,
      metadata: { userId: userId || '' },
      success_url: `${process.env.CLIENT_URL || req.headers.origin}/dashboard.html?success=true`,
      cancel_url: `${process.env.CLIENT_URL || req.headers.origin}/pricing.html?canceled=true`,
    });

    res.json({ url: session.url });
  } catch (error) {
    console.error('Stripe Checkout Error:', error.message);
    res.status(400).json({ error: error.message });
  }
});

// ==========================================
// 3. WEBSOCKET HANDLERS (Captions, Rooms & Styles)
// ==========================================
wss.on("connection", (ws, req) => {
  const urlObj = new URL(req.url, `http://${req.headers.host}`);
  const pathname = urlObj.pathname;
  const roomId = urlObj.searchParams.get("room") || "default";
  
  // Extract spoken language and translation flag sent from presenter screen
  const spokenLangParam = urlObj.searchParams.get("spokenLang") || "en-US";
  const translateParam = urlObj.searchParams.get("translate") === "true";

  const room = getOrCreateRoom(roomId);
  room.spokenLang = spokenLangParam;
  room.translateEnabled = translateParam;

  // ROUTE A: Stage Microphones / Audio Ingest & Style Controls
  if (pathname === "/ws/ingest") {
    console.log(`[Ingest] Presenter audio connected to room: ${roomId} | Spoken Lang: ${room.spokenLang} | Translate: ${room.translateEnabled}`);
    room.audioQueue = [];
    room.isDgReady = false;

    room.dgLive = deepgram.listen.live({
      model: "nova-3",
      language: room.spokenLang, // Dynamically uses presenter's language choice
      smart_format: true,
      interim_results: true,
      endpointing: 300
    });

    room.dgLive.on(LiveTranscriptionEvents.Open, () => {
      console.log(`[Deepgram] Room ${roomId} STT connected. Flushing queue...`);
      room.isDgReady = true;
      while (room.audioQueue.length > 0) {
        const chunk = room.audioQueue.shift();
        if (room.dgLive && room.dgLive.getReadyState() === 1) {
          room.dgLive.send(chunk);
        }
      }
    });

    room.dgLive.on(LiveTranscriptionEvents.Error, (err) => {
      console.error(`[Deepgram Error - Room ${roomId}]`, err);
    });

    room.dgLive.on(LiveTranscriptionEvents.Transcript, async (data) => {
      const transcript = data.channel?.alternatives?.[0]?.transcript;
      const isFinal = data.is_final;

      if (transcript && transcript.trim().length > 0) {
        let overlayText = transcript;
        
        // Translate server-side if Google Translate toggle is enabled by presenter
        if (room.translateEnabled && room.targetOverlayLang && room.targetOverlayLang !== room.spokenLang) {
          overlayText = await translateText(transcript, room.spokenLang, room.targetOverlayLang);
        }

        const overlayPayload = JSON.stringify({
          text: overlayText,
          original: transcript,
          isFinal,
          lang: room.targetOverlayLang,
          styles: room.overlayStyles
        });

        // Broadcast ONLY to overlays in THIS room
        room.overlays.forEach((client) => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(overlayPayload);
          }
        });

        // Broadcast to mobile attendees
        if (isFinal) {
          for (const attendee of room.attendees) {
            if (attendee.readyState === WebSocket.OPEN) {
              const lang = attendee.language || "en";
              let translated = transcript;
              if (lang !== room.spokenLang) {
                translated = await translateText(transcript, room.spokenLang, lang);
              }
              attendee.send(JSON.stringify({
                text: translated,
                original: transcript,
                isFinal: true,
                lang: lang
              }));
            }
          }
        }
      }
    });

    ws.on("message", (message, isBinary) => {
      if (!isBinary) {
        try {
          const controlData = JSON.parse(message.toString());
          if (controlData.type === "set_language") {
            room.targetOverlayLang = controlData.lang;
            console.log(`[Room ${roomId}] Switched overlay language to: ${room.targetOverlayLang}`);
          } else if (controlData.type === "update_styles") {
            room.overlayStyles = controlData.styles;
            const stylePayload = JSON.stringify({ type: 'styles_update', styles: room.overlayStyles });
            room.overlays.forEach((client) => {
              if (client.readyState === WebSocket.OPEN) {
                client.send(stylePayload);
              }
            });
          }
        } catch (e) {}
        return;
      }

      if (room.isDgReady && room.dgLive && room.dgLive.getReadyState() === 1) {
        room.dgLive.send(message);
      } else {
        if (room.audioQueue.length < 500) {
          room.audioQueue.push(message);
        }
      }
    });

    ws.on("close", () => {
      console.log(`[Ingest] Presenter disconnected from room: ${roomId}`);
      if (room.dgLive) {
        room.dgLive.finish();
        room.dgLive = null;
      }
      room.isDgReady = false;
      cleanupRoom(roomId);
    });
  }

  // ROUTE B: Stage Video Overlay (OBS / vMix)
  else if (pathname === "/ws/overlay") {
    console.log(`[Overlay] OBS connected to room: ${roomId}`);
    room.overlays.add(ws);

    if (room.overlayStyles) {
      ws.send(JSON.stringify({ type: 'styles_update', styles: room.overlayStyles }));
    }

    ws.on("close", () => {
      room.overlays.delete(ws);
      cleanupRoom(roomId);
    });
  }

  // ROUTE C: Mobile Audience (QR Code Viewers)
  else if (pathname.startsWith("/ws/attendee")) {
    console.log(`[Attendee] Mobile viewer connected to room: ${roomId}`);
    ws.language = urlObj.searchParams.get("lang") || "en";
    room.attendees.add(ws);

    ws.on("message", (msg) => {
      try {
        const data = JSON.parse(msg);
        if (data.type === "set_language") {
          ws.language = data.lang;
        }
      } catch (e) {}
    });

    ws.on("close", () => {
      room.attendees.delete(ws);
      cleanupRoom(roomId);
    });
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});
