const WebSocket = require('ws');
const http = require('http');
const url = require('url');
const { Translate } = require('@google-cloud/translate').v2;

const translate = new Translate(); // Or your preferred translation service
const server = http.createServer();
const wss = new WebSocket.Server({ server });

// Store active rooms and client roles
// Map: roomId -> { presenters: Set, viewers: Set, targetLang: string, spokenLang: string }
const rooms = new Map();

wss.on('connection', (ws, req) => {
  const parsedUrl = url.parse(req.url, true);
  const { room = 'main-stage', role = 'viewer', lang = 'en-US', target_lang = 'none' } = parsedUrl.query;

  // Initialize room state if it doesn't exist
  if (!rooms.has(room)) {
    rooms.set(room, {
      presenters: new Set(),
      viewers: new Set(),
      spokenLang: lang,
      targetLang: target_lang
    });
  }

  const roomState = rooms.get(room);

  if (role === 'presenter') {
    roomState.presenters.add(ws);
    roomState.spokenLang = lang;
    roomState.targetLang = target_lang;
    console.log(`[Server] Presenter connected to room: ${room} (Spoken: ${lang}, Target: ${target_lang})`);
  } else {
    roomState.viewers.add(ws);
    console.log(`[Server] Viewer/Overlay connected to room: ${room}`);
  }

  // Handle incoming data from Presenter or Overlay
  ws.on('message', async (message) => {
    // 1. Handle JSON Control / Config Messages (e.g. Tab Switching)
    if (typeof message === 'string' || message instanceof Buffer && isJson(message)) {
      try {
        const payload = JSON.parse(message.toString());
        
        if (payload.type === 'config' && payload.targetLang) {
          roomState.targetLang = payload.targetLang;
          console.log(`[Server] Room ${room} target language updated to: ${payload.targetLang}`);
          return;
        }
      } catch (e) {
        // Not JSON config, treat as audio chunk
      }
    }

    // 2. If Presenter sends raw audio data, process transcription & translation
    if (role === 'presenter') {
      // --- STT & TRANSLATION PIPELINE HERE ---
      // Example simulated flow once STT returns raw text:
      const rawTranscript = "Hello and welcome to the live keynote speech."; 
      
      let translatedText = rawTranscript;

      // Translate if target language is selected and not 'none'
      if (roomState.targetLang && roomState.targetLang !== 'none') {
        try {
          // Standardize language code (e.g., 'es-ES' -> 'es')
          const targetCode = roomState.targetLang.split('-')[0]; 
          const [translation] = await translate.translate(rawTranscript, targetCode);
          translatedText = translation;
        } catch (err) {
          console.error("[Server] Translation error:", err.message);
        }
      }

      // 3. Broadcast to all Viewers/Overlays in the room
      const broadcastPayload = JSON.stringify({
        type: 'caption',
        text: rawTranscript,
        translation: translatedText,
        translations: {
          [roomState.targetLang]: translatedText
        }
      });

      roomState.viewers.forEach((viewerWs) => {
        if (viewerWs.readyState === WebSocket.OPEN) {
          viewerWs.send(broadcastPayload);
        }
      });
    }
  });

  ws.on('close', () => {
    if (role === 'presenter') {
      roomState.presenters.delete(ws);
    } else {
      roomState.viewers.delete(ws);
    }
    if (roomState.presenters.size === 0 && roomState.viewers.size === 0) {
      rooms.delete(room);
    }
  });
});

function isJson(buffer) {
  try {
    const str = buffer.toString();
    return str.startsWith('{') && str.endsWith('}');
  } catch {
    return false;
  }
}

server.listen(8080, () => {
  console.log('[Server] Captions WebSocket server running on port 8080');
});
