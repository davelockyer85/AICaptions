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
// Map<roomId, { targetOverlayLang, overlays: Set, attendees: Set, dgLive, audioQueue, isDgReady }>
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

// Free Real-time Translation Helper (MyMemory API)
async function translateText(text, targetLang) {
  if (!targetLang || targetLang.startsWith("en")) return text;
  try {
    const res = await fetch(
      `https://api.mymemory.translated.net/get?q=${encodeURIComponent(text)}&langpair=en|${targetLang}`
    );
    const data = await res.json();
    return data.responseData?.translatedText || text;
  } catch (err) {
    console.error("[Translation Error]", err.message);
    return text;
  }
}

wss.on("connection", (ws, req) => {
  const urlObj = new URL(req.url, `http://${req.headers.host}`);
  const pathname = urlObj.pathname;
  const roomId = urlObj.searchParams.get("room") || urlObj.searchParams.get("roomId") || "main-stage";
  const role = urlObj.searchParams.get("role");
  const spokenLang = urlObj.searchParams.get("lang") || "en-US";

  const room = getOrCreateRoom(roomId);

  // Match via path or query role
  const isIngest = pathname === "/ws/ingest" || role === "presenter" || role === "ingest";
  const isOverlay = pathname === "/ws/overlay" || role === "overlay";
  const isAttendee = pathname.startsWith("/ws/attendee") || role === "attendee" || role === "viewer";

  // ROUTE A: Stage Microphones / Audio Ingest (Presenter)
  if (isIngest) {
    console.log(`[Ingest] Presenter audio connected to room: ${roomId} (Lang: ${spokenLang})`);

    // Clean up existing Deepgram instance if presenter reconnected
    if (room.dgLive) {
      try { room.dgLive.finish(); } catch (e) {}
      room.dgLive = null;
    }

    room.audioQueue = [];
    room.isDgReady = false;

    // Create a room-specific Deepgram STT stream using spoken language from presenter
    room.dgLive = deepgram.listen.live({
      model: "nova-3",
      language: spokenLang,
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

        // Translate if room target language is not English
        if (room.targetOverlayLang !== "en" && isFinal) {
          overlayText = await translateText(transcript, room.targetOverlayLang);
        }

        const overlayPayload = JSON.stringify({
          text: overlayText,
          original: transcript,
          isFinal,
          lang: room.targetOverlayLang
        });

        // Broadcast ONLY to overlays in THIS room
        room.overlays.forEach((client) => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(overlayPayload);
          }
        });

        // Broadcast ONLY to mobile attendees in THIS room
        if (isFinal) {
          room.attendees.forEach(async (attendee) => {
            if (attendee.readyState === WebSocket.OPEN) {
              const translated = await translateText(transcript, attendee.language || "en");
              attendee.send(
                JSON.stringify({ text: translated, original: transcript })
              );
            }
          });
        }
      }
    });

    ws.on("message", (message, isBinary) => {
      // Handle JSON control messages (e.g. language change for this room)
      if (!isBinary) {
        try {
          const controlData = JSON.parse(message.toString());
          if (controlData.type === "set_language") {
            room.targetOverlayLang = controlData.lang;
            console.log(`[Room ${roomId}] Switched overlay language to: ${room.targetOverlayLang}`);
          }
        } catch (e) {}
        return;
      }

      // Handle binary microphone audio stream
      if (room.isDgReady && room.dgLive && room.dgLive.getReadyState() === 1) {
        room.dgLive.send(message);
      } else {
        room.audioQueue.push(message);
      }
    });

    ws.on("close", () => {
      console.log(`[Ingest] Presenter disconnected from room: ${roomId}`);
      if (room.dgLive) {
        try { room.dgLive.finish(); } catch (e) {}
        room.dgLive = null;
      }
      cleanupRoom(roomId);
    });
  }

  // ROUTE B: Stage Video Overlay (OBS / vMix)
  else if (isOverlay) {
    console.log(`[Overlay] OBS connected to room: ${roomId}`);
    room.overlays.add(ws);

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
  } else {
    console.warn(`[WebSocket] Unrecognized connection route or role: pathname=${pathname}, role=${role}`);
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
