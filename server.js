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

const DEEPGRAM_KEY = process.env.DEEPGRAM_API_KEY;
if (!DEEPGRAM_KEY) {
  console.error("Error: DEEPGRAM_API_KEY is missing in .env");
}
const deepgram = createClient(DEEPGRAM_KEY);

// Active client connections
const clients = {
  presenter: null,
  overlays: new Set(),
  attendees: new Set()
};

// Simple real-time translation using MyMemory API
async function translateText(text, targetLang) {
  if (!targetLang || targetLang === "en") return text;
  try {
    const res = await fetch(
      `https://api.mymemory.translated.net/get?q=${encodeURIComponent(text)}&langpair=en|${targetLang}`
    );
    const data = await res.json();
    return data.responseData?.translatedText || text;
  } catch (err) {
    return text;
  }
}

// WebSocket Router
wss.on("connection", (ws, req) => {
  const url = req.url;

  // 1. Stage Microphones / Audio Ingest
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
      console.log("[Deepgram] Live connection open.");
    });

    dgLive.on(LiveTranscriptionEvents.Transcript, async (data) => {
      const transcript = data.channel.alternatives[0]?.transcript;
      const isFinal = data.is_final;

      if (transcript && transcript.trim().length > 0) {
        // Send raw transcript to stage overlays immediately
        const overlayPayload = JSON.stringify({ text: transcript, isFinal });
        clients.overlays.forEach((client) => {
          if (client.readyState === WebSocket.OPEN) client.send(overlayPayload);
        });

        // Broadcast to mobile attendees with translation on final phrases
        if (isFinal) {
          clients.attendees.forEach(async (attendee) => {
            if (attendee.readyState === WebSocket.OPEN) {
              const translated = await translateText(transcript, attendee.language || "en");
              attendee.send(JSON.stringify({ text: translated, original: transcript }));
            }
          });
        }
      }
    });

    ws.on("message", (chunk) => dgLive.send(chunk));
    ws.on("close", () => {
      console.log("[Ingest] Presenter disconnected.");
      dgLive.finish();
    });
  } 
  
  // 2. Stage Video Overlay (OBS / vMix)
  else if (url === "/ws/overlay") {
    console.log("[Overlay] OBS / Switcher connected.");
    clients.overlays.add(ws);
    ws.on("close", () => clients.overlays.delete(ws));
  } 

  // 3. Mobile Attendees (QR Code Audience)
  else if (url.startsWith("/ws/attendee")) {
    console.log("[Attendee] Mobile viewer connected.");
    const params = new URLSearchParams(url.split("?")[1]);
    ws.language = params.get("lang") || "en";
    
    clients.attendees.add(ws);
    
    ws.on("message", (msg) => {
      try {
        const data = JSON.parse(msg);
        if (data.type === "set_language") ws.language = data.lang;
      } catch (e) {}
    });

    ws.on("close", () => clients.attendees.delete(ws));
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});