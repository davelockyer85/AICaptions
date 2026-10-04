import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import { createClient, LiveTranscriptionEvents } from "@deepgram/sdk";
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";

dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static(path.join(__dirname, "public")));

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

const DEEPGRAM_KEY = process.env.DEEPGRAM_API_KEY;
if (!DEEPGRAM_KEY) {
  console.error("[WARNING] DEEPGRAM_API_KEY is missing in environment variables!");
}
const deepgram = createClient(DEEPGRAM_KEY);

// Multi-Tenant Rooms Store
const rooms = new Map();

function getOrCreateRoom(roomId) {
  if (!rooms.has(roomId)) {
    console.log(`[Room Created] Initializing room: ${roomId}`);
    rooms.set(roomId, {
      targetOverlayLang: "en",
      overlays: new Set(),
      attendees: new Set(),
      dgLive: null,
      audioQueue: [],
      isDgReady: false
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

// In-memory Translation Cache to optimize MyMemory API performance
const translationCache = new Map();

// Free Real-time Translation Helper (MyMemory API)
async function translateText(text, targetLang) {
  if (!text || !targetLang) return text;

  // Clean language code (e.g. 'es-ES' -> 'es')
  const cleanLang = targetLang.split("-")[0].toLowerCase();
  if (cleanLang === "en") return text;

  const cacheKey = `${cleanLang}:${text.trim()}`;
  if (translationCache.has(cacheKey)) {
    return translationCache.get(cacheKey);
  }

  try {
    const res = await fetch(
      `https://api.mymemory.translated.net/get?q=${encodeURIComponent(text)}&langpair=en|${cleanLang}`
    );
    const data = await res.json();
    const translated = data.responseData?.translatedText || text;

    // Store in cache
    translationCache.set(cacheKey, translated);
    if (translationCache.size > 2000) {
      const firstKey = translationCache.keys().next().value;
      translationCache.delete(firstKey);
    }

    return translated;
  } catch (err) {
    console.error("[Translation Error]", err.message);
    return text;
  }
}

wss.on("connection", (ws, req) => {
  const urlObj = new URL(req.url, `http://${req.headers.host}`);
  let pathname = urlObj.pathname;
  const roomId = urlObj.searchParams.get("room") || "default";
  const role = urlObj.searchParams.get("role"); // Fallback for route handling
  const urlLang = urlObj.searchParams.get("lang") || urlObj.searchParams.get("target_lang");

  const room = getOrCreateRoom(roomId);

  // Parse initial language selection from URL parameters
  if (urlLang) {
    room.targetOverlayLang = urlLang;
    console.log(`[Room ${roomId}] Initialized target overlay language to: ${room.targetOverlayLang}`);
  }

  // Route matching logic
  const isIngest = pathname === "/ws/ingest" || role === "presenter";
  const isOverlay = pathname === "/ws/overlay" || role === "overlay";
  const isAttendee = pathname.startsWith("/ws/attendee") || role === "attendee";

  // ROUTE A: Stage Microphones / Audio Ingest
  if (isIngest) {
    console.log(`[Ingest] Presenter audio connected to room: ${roomId}`);

    room.audioQueue = [];
    room.isDgReady = false;

    room.dgLive = deepgram.listen.live({
      model: "nova-3",
      language: "en-US",
      smart_format: true,
      interim_results: true,
      endpointing: 300
    });

    room.dgLive.on(LiveTranscriptionEvents.Open, () => {
      console.log(`[Deepgram] Room ${roomId} STT connected. Flushing queue...`);
      room.isDgReady = true;

      while (room.audioQueue.length > 0) {
        room.dgLive.send(room.audioQueue.shift());
      }
    });

    room.dgLive.on(LiveTranscriptionEvents.Error, (err) => {
      console.error(`[Deepgram Error - Room ${roomId}]`, err);
    });

    room.dgLive.on(LiveTranscriptionEvents.Transcript, async (data) => {
      const transcript = data.channel.alternatives[0]?.transcript;
      const isFinal = data.is_final;

      if (transcript && transcript.trim().length > 0) {
        let overlayText = transcript;

        // Translate BOTH interim and final results to prevent raw English flashes
        if (room.targetOverlayLang !== "en") {
          overlayText = await translateText(transcript, room.targetOverlayLang);
        }

        // Standardized JSON payload supporting all frontend overlay requirements
        const overlayPayload = JSON.stringify({
          type: "caption",
          text: overlayText,
          translation: overlayText,
          translations: {
            [room.targetOverlayLang]: overlayText
          },
          original: transcript,
          isFinal: isFinal,
          is_final: isFinal,
          lang: room.targetOverlayLang
        });

        // Broadcast to overlays in THIS room
        room.overlays.forEach((client) => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(overlayPayload);
          }
        });

        // Broadcast to mobile attendees in THIS room
        if (isFinal) {
          room.attendees.forEach(async (attendee) => {
            if (attendee.readyState === WebSocket.OPEN) {
              const targetLang = attendee.language || "en";
              const translated = await translateText(transcript, targetLang);
              attendee.send(
                JSON.stringify({
                  type: "caption",
                  text: translated,
                  translation: translated,
                  translations: { [targetLang]: translated },
                  original: transcript,
                  isFinal: true
                })
              );
            }
          });
        }
      }
    });

    ws.on("message", (message, isBinary) => {
      if (!isBinary) {
        try {
          const controlData = JSON.parse(message.toString());
          if (controlData.type === "set_language" || controlData.type === "config") {
            const newLang = controlData.lang || controlData.target_language;
            if (newLang) {
              room.targetOverlayLang = newLang;
              console.log(`[Room ${roomId}] Switched overlay language to: ${room.targetOverlayLang}`);
            }
          }
        } catch (e) {}
        return;
      }

      if (room.isDgReady && room.dgLive.getReadyState() === 1) {
        room.dgLive.send(message);
      } else {
        room.audioQueue.push(message);
      }
    });

    ws.on("close", () => {
      console.log(`[Ingest] Presenter disconnected from room: ${roomId}`);
      if (room.dgLive) {
        room.dgLive.finish();
        room.dgLive = null;
      }
      cleanupRoom(roomId);
    });
  }

  // ROUTE B: Stage Video Overlay (OBS / vMix)
  else if (isOverlay) {
    console.log(`[Overlay] OBS connected to room: ${roomId}`);
    room.overlays.add(ws);

    // Listen for language change events directly from overlay clients
    ws.on("message", (msg) => {
      try {
        const data = JSON.parse(msg.toString());
        if (data.type === "set_language" || data.type === "config") {
          const newLang = data.lang || data.target_language;
          if (newLang) {
            room.targetOverlayLang = newLang;
            console.log(`[Room ${roomId}] Overlay updated language to: ${room.targetOverlayLang}`);
          }
        }
      } catch (e) {}
    });

    ws.on("close", () => {
      room.overlays.delete(ws);
      cleanupRoom(roomId);
    });
  }

  // ROUTE C: Mobile Audience (QR Code Viewers)
  else if (isAttendee) {
    console.log(`[Attendee] Mobile viewer connected to room: ${roomId}`);
    ws.language = urlObj.searchParams.get("lang") || "en";

    room.attendees.add(ws);

    ws.on("message", (msg) => {
      try {
        const data = JSON.parse(msg.toString());
        if (data.type === "set_language" || data.type === "config") {
          ws.language = data.lang || data.target_language;
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
server.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
