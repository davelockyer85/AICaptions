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

const clients = {
  overlays: new Set(),
  attendees: new Set()
};

// Global active target language for stage overlay (default: English)
let targetOverlayLang = "en";

// Free Real-time Translation Helper (MyMemory API)
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

wss.on("connection", (ws, req) => {
  const url = req.url;

  // ROUTE A: Stage Microphones / Audio Ingest
  if (url === "/ws/ingest") {
    console.log("[Ingest] Presenter audio connected.");

    const audioQueue = [];
    let isDgReady = false;

    const dgLive = deepgram.listen.live({
      model: "nova-3",
      language: "en-US",
      smart_format: true,
      interim_results: true,
      endpointing: 300
    });

    dgLive.on(LiveTranscriptionEvents.Open, () => {
      console.log("[Deepgram] Connected. Flushing buffered audio...");
      isDgReady = true;

      while (audioQueue.length > 0) {
        dgLive.send(audioQueue.shift());
      }
    });

    dgLive.on(LiveTranscriptionEvents.Error, (err) => {
      console.error("[Deepgram Error]", err);
    });

    dgLive.on(LiveTranscriptionEvents.Transcript, async (data) => {
      const transcript = data.channel.alternatives[0]?.transcript;
      const isFinal = data.is_final;

      if (transcript && transcript.trim().length > 0) {
        let overlayText = transcript;

        // Translate finalized phrases if target language is not English
        if (targetOverlayLang !== "en" && isFinal) {
          overlayText = await translateText(transcript, targetOverlayLang);
        }

        const overlayPayload = JSON.stringify({
          text: overlayText,
          original: transcript,
          isFinal,
          lang: targetOverlayLang
        });

        clients.overlays.forEach((client) => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(overlayPayload);
          }
        });

        // Broadcast to mobile attendees with their selected language
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

    // Handle incoming audio chunks OR JSON control messages (e.g. language change)
    ws.on("message", (message, isBinary) => {
      if (!isBinary) {
        try {
          const controlData = JSON.parse(message.toString());
          if (controlData.type === "set_language") {
            targetOverlayLang = controlData.lang;
            console.log(`[Presenter] Target overlay language switched to: ${targetOverlayLang}`);
          }
        } catch (e) {}
        return;
      }

      if (isDgReady && dgLive.getReadyState() === 1) {
        dgLive.send(message);
      } else {
        audioQueue.push(message);
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
