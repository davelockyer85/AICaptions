import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import { createClient, LiveTranscriptionEvents } from "@deepgram/sdk";
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";
import Stripe from "stripe";
import { createClient as createSupabaseClient } from "@supabase/supabase-js";
import { Resend } from 'resend';

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
const resend = new Resend(process.env.RESEND_API_KEY);

// --- Google Translate Helper ------------------------------------------------
async function translateText(text, sourceLang, targetLang) {
  if (!text || !targetLang || targetLang === sourceLang) return text;
  const sl = (sourceLang || "en").split("-")[0];
  const tl = (targetLang || "en").split("-")[0];
  const url = `https://translate.googleapis.com/translate_a/single?client=gtx&sl=${sl}&tl=${tl}&dt=t&q=${encodeURIComponent(text)}`;

  try {
    const response = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" }
    });
    
    const rawText = await response.text();
    if (!rawText.startsWith('[')) {
      console.error("[Translation Blocked] Google returned HTML instead of JSON.");
      return text;
    }

    const data = JSON.parse(rawText);
    if (data && data[0]) {
      return data[0].map(item => item[0]).join('');
    }
  } catch (err) {
    console.error("[Translation Error]", err.message);
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
      showQr: false,
      overlays: new Set(),
      attendees: new Set(),
      dgLive: null,
      audioQueue: [],
      isDgReady: false,
      userId: null,
      startTime: null,
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
        .from('subscriptions')
        .update({
          plan_tier: planTier,
          event_pass_expires_at: expiresAt,
          seconds_used: maxStreamingSeconds,
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

// ==========================================
// STRIPE WEBHOOK ENDPOINT (API Route)
// ==========================================
app.post('/api/stripe/webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;

  try {
    event = stripe.webhooks.constructEvent(req.body, sig, process.env.STRIPE_WEBHOOK_SECRET);
  } catch (err) {
    console.error(`⚠️ Webhook signature verification failed: ${err.message}`);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  if (event.type === 'checkout.session.completed') {
    const session = event.data.object;
    const userId = session.client_reference_id || session.metadata?.userId;
    const subscriptionId = session.subscription || null;

    if (userId) {
      const { error } = await supabase
        .from('subscriptions')
        .upsert({
          user_id: userId,
          stripe_subscription_id: subscriptionId,
          status: 'active',
          plan_type: 'starter',
          updated_at: new Date()
        }, { onConflict: 'user_id' });

      if (error) {
        console.error('Failed to update Supabase subscriptions:', error);
      } else {
        console.log(`Successfully updated subscription in Supabase for user: ${userId}`);
      }
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

app.post('/api/toggle-qr', express.json(), (req, res) => {
  const { room: roomId, show } = req.body;
  const room = getOrCreateRoom(roomId);
  room.showQr = show;

  const qrPayload = JSON.stringify({ type: 'qr_update', show: room.showQr });
  room.overlays.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(qrPayload);
    }
  });

  console.log(`[Room ${roomId}] QR Code visibility set to: ${room.showQr} (via HTTP)`);
  res.json({ success: true, show: room.showQr });
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
  
  const spokenLangParam = urlObj.searchParams.get("spokenLang") || "en-US";
  const translateParam = urlObj.searchParams.get("translate") === "true";

  const room = getOrCreateRoom(roomId);
  room.spokenLang = spokenLangParam;
  room.translateEnabled = translateParam;

  // ROUTE A: Stage Microphones / Audio Ingest & Style Controls
  if (pathname === "/ws/ingest") {
    room.userId = urlObj.searchParams.get("userId");
    room.startTime = Date.now();

    console.log(`[Ingest] Presenter audio connected to room: ${roomId} | User: ${room.userId} | Spoken Lang: ${room.spokenLang} | Translate: ${room.translateEnabled}`);
    room.audioQueue = [];
    room.isDgReady = false;

    room.dgLive = deepgram.listen.live({
      model: "nova-3",
      language: room.spokenLang,
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

        room.overlays.forEach((client) => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(overlayPayload);
          }
        });

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
          } else if (controlData.type === "toggle_qr") {
            room.showQr = controlData.show;
            const qrPayload = JSON.stringify({ type: 'qr_update', show: room.showQr });
            room.overlays.forEach((client) => {
              if (client.readyState === WebSocket.OPEN) {
                client.send(qrPayload);
              }
            });
            console.log(`[Room ${roomId}] QR Code visibility set to: ${room.showQr}`);
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

    ws.on("close", async () => {
      console.log(`[Ingest] Presenter disconnected from room: ${roomId}`);
      if (room.dgLive) {
        room.dgLive.finish();
        room.dgLive = null;
      }
      room.isDgReady = false;

      if (room.userId && room.startTime) {
        const elapsedSeconds = Math.round((Date.now() - room.startTime) / 1000);
        if (elapsedSeconds > 0) {
          try {
            const { data: sub } = await supabase
              .from('subscriptions')
              .select('seconds_used')
              .eq('user_id', room.userId)
              .single();

            const currentUsed = sub?.seconds_used || 0;
            const newSecondsUsed = currentUsed + elapsedSeconds;

            await supabase
              .from('subscriptions')
              .update({ seconds_used: newSecondsUsed })
              .eq('user_id', room.userId);

            console.log(`[Usage] Updated user ${room.userId}: +${elapsedSeconds}s (Total: ${newSecondsUsed}s)`);
          } catch (err) {
            console.error('[Usage] Failed to update seconds_used:', err);
          }
        }
      }

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
    ws.send(JSON.stringify({ type: 'qr_update', show: room.showQr }));

    ws.on("close", () => {
      room.overlays.delete(ws);
      cleanupRoom(roomId);
    });
  }

  // ROUTE C: Mobile Audience (QR Code Viewers)
  else if (pathname.startsWith("/ws/attendee")) {
    console.log(`[Attendee] Mobile viewer connected to room: ${roomId}`);
    
    ws.language = ws.language || urlObj.searchParams.get("lang") || "en";
    room.attendees.add(ws);

    ws.on("message", (msg) => {
      try {
        const data = JSON.parse(msg);
        if (data.type === "set_language") {
          ws.language = data.lang;
          console.log(`[Attendee] Viewer switched language to: ${ws.language}`);
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
