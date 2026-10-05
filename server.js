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

// Working Translation Helper using Google Translate single endpoint
async function translateText(text, targetLang) {
  if (!targetLang || targetLang === "en") return text;
  try {
    const url = `https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl=${encodeURIComponent(targetLang)}&dt=t&q=${encodeURIComponent(text)}`;
    const res = await fetch(url);
    const data = await res.json();
    if (Array.isArray(data) && Array.isArray(data[0])) {
      return data[0].map((item) => item[0]).join('');
    }
    return text;
  } catch (err) {
    console.error("[Translation Error]", err);
    return text;
  }
}

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

wss.on("connection", (ws, req) => {
  const urlObj = new URL(req.url, `http://${req.headers.host}`);
  const pathname = urlObj.pathname;
  const roomId = urlObj.searchParams.get("room") || "default";

  const room = getOrCreateRoom(roomId);

  // ROUTE A: Stage Microphones / Audio Ingest
  if (pathname === "/ws/ingest") {
    const spokenLang = urlObj.searchParams.get("spokenLang") || urlObj.searchParams.get("lang") || "en-US";
    console.log(`[Ingest] Presenter connected to room: ${roomId} (Spoken Language: ${spokenLang})`);

    room.audioQueue = [];
    room.isDgReady = false;

    // Create room-specific Deepgram STT stream using spoken language from presenter
    room.dgLive = deepgram.listen.live({
      model: "nova-3",
      language: spokenLang,
      smart_format: true,
      interim_results: true,
      endpointing: 300
    });

    room.dgLive.on(LiveTranscriptionEvents.Open, () => {
      console.log(`[Deepgram] Room ${roomId} STT connected. Flushing audio queue...`);
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

        // Translate overlay text if room target language is not English and result is final
        if (room.targetOverlayLang !== "en" && isFinal) {
          overlayText = await translateText(transcript, room.targetOverlayLang);
        }

        const overlayPayload = JSON.stringify({
          text: overlayText,
          original: transcript,
          isFinal,
          lang: room.targetOverlayLang
        });

        // Broadcast to all stage overlays in this room
        room.overlays.forEach((client) => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(overlayPayload);
          }
        });

        // Broadcast to all mobile audience viewers in this room
        if (isFinal) {
          for (const attendee of room.attendees) {
            if (attendee.readyState === WebSocket.OPEN) {
              const lang = attendee.language || "en";
              const translated = lang === "en" ? transcript : await translateText(transcript, lang);
              attendee.send(
                JSON.stringify({
                  text: translated,
                  original: transcript,
                  isFinal: true,
                  lang: lang
                })
              );
            }
          }
        }
      }
    });

    ws.on("message", (message, isBinary) => {
      // Handle JSON control messages (language switching, etc.)
      if (!isBinary) {
        try {
          const controlData = JSON.parse(message.toString());
          if (controlData.type === "set_language") {
            room.targetOverlayLang = controlData.lang;
            console.log(`[Room ${roomId}] Switched overlay language to: ${room.targetOverlayLang}`);

            // Broadcast language switch event to overlay clients
            const langNotifyPayload = JSON.stringify({
              type: "language_changed",
              lang: room.targetOverlayLang
            });

            room.overlays.forEach((client) => {
              if (client.readyState === WebSocket.OPEN) {
                client.send(langNotifyPayload);
              }
            });
          }
        } catch (e) {}
        return;
      }

      // Handle binary microphone audio stream
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

    ws.on("close", () => {
      room.overlays.delete(ws);
      cleanupRoom(roomId);
    });
  }

  // ROUTE C: Mobile Audience (QR Code Viewers)
  else if (pathname.startsWith("/ws/attendee")) {
    const viewerLang = urlObj.searchParams.get("lang") || "en";
    console.log(`[Attendee] Mobile viewer connected to room: ${roomId} (Lang: ${viewerLang})`);
    ws.language = viewerLang;

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
server.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
