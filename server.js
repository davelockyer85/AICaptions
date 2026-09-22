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

// 1. Serve static frontend files from 'public' directory
app.use(express.static(path.join(__dirname, "public")));

// Explicit root route handler to guarantee index.html loads on Render
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// 2. Initialize Deepgram SDK
const DEEPGRAM_KEY = process.env.DEEPGRAM_API_KEY;
if (!DEEPGRAM_KEY) {
  console.error("[WARNING] DEEPGRAM_API_KEY is missing in environment variables!");
}
const deepgram = createClient(DEEPGRAM_KEY);

// 3. Client Tracking Registries
const clients = {
  overlays: new Set(),
  attendees: new Set()
};

// 4. Free Real-time Translation Helper (MyMemory API)
async function translateText(text, targetLang) {
  if (!targetLang || targetLang === "en") return text;
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

// 5. WebSocket Connection Router
wss.on("connection", (ws, req) => {
  const url = req.url;

  // ROUTE A: Stage Microphones / Audio Ingest
  if (url === "/ws/ingest") {
    console.log("[Ingest] Presenter audio connected.");

    const dgLive = deepgram.listen.live({
      model: "nova-3",
      language: "en-US",
      smart_format: true,
      interim_results: true,
      endpointing: 300
    });

    dgLive.on(LiveTranscriptionEvents.Open, () => {
      console.log("[Deepgram] Live STT socket connected.");
    });

    dgLive.on(LiveTranscriptionEvents.Error, (err) => {
      console.error("[Deepgram Error]", err);
    });

    dgLive.on(LiveTranscriptionEvents.Close, () => {
      console.log("[Deepgram] Connection closed.");
    });

    // Handle transcription events from Deepgram
    dgLive.on(LiveTranscriptionEvents.Transcript, async (data) => {
      const transcript = data.channel.alternatives[0]?.transcript;
      const isFinal = data.is_final;

      if (transcript && transcript.trim().length > 0) {
        // Broadcast raw text immediately to OBS / stage overlays
        const overlayPayload = JSON.stringify({ text: transcript, isFinal });
        clients.overlays.forEach((client) => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(overlayPayload);
          }
        });

        // Broadcast translated text to mobile attendees on final sentences
        if (isFinal) {
          clients.attendees.forEach(async (attendee) => {
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

    // Forward binary audio chunks from presenter microphone to Deepgram
    ws.on("message", (chunk) => {
      if (dgLive.getReadyState() === 1) { // 1 = OPEN
        dgLive.send(chunk);
      }
    });

    ws.on("close", () => {
      console.log("[Ingest] Presenter disconnected.");
      dgLive.finish();
    });
  }

  // ROUTE B: Stage Video Overlay (OBS / vMix)
  else if (url === "/ws/overlay") {
    console.log("[Overlay] OBS / Stage display connected.");
    clients.overlays.add(ws);
    ws.on("close", () => clients.overlays.delete(ws));
  }

  // ROUTE C: Mobile Audience (QR Code Viewers)
  else if (url.startsWith("/ws/attendee")) {
    console.log("[Attendee] Mobile viewer connected.");
    const params = new URLSearchParams(url.split("?")[1]);
    ws.language = params.get("lang") || "en";

    clients.attendees.add(ws);

    // Dynamic language switching from mobile client
    ws.on("message", (msg) => {
      try {
        const data = JSON.parse(msg);
        if (data.type === "set_language") {
          ws.language = data.lang;
        }
      } catch (e) {}
    });

    ws.on("close", () => clients.attendees.delete(ws));
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
