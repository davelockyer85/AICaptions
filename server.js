import express from 'express';
import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import { createClient as createDeepgramClient, LiveTranscriptionEvents } from '@deepgram/sdk';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Environment Variables
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const deepgram = createDeepgramClient(DEEPGRAM_API_KEY);
const supabase = createSupabaseClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

app.use(express.static('public')); // Serves HTML dashboard files

// Active rooms state management
const activeRooms = new Map();

wss.on('connection', async (ws, req) => {
  try {
    const urlParams = new URLSearchParams(req.url.split('?')[1]);
    const room = urlParams.get('room') || 'main-stage';
    const role = urlParams.get('role') || 'audience';
    const lang = urlParams.get('lang') || 'en';
    const token = urlParams.get('token');

    // 1. Authenticate Token for Presenters
    if (role === 'presenter') {
      if (!token) {
        ws.close(4003, 'Unauthorized: Missing session token');
        return;
      }

      const { data: { user }, error } = await supabase.auth.getUser(token);
      if (error || !user) {
        ws.close(4003, 'Unauthorized: Invalid auth session');
        return;
      }

      // 2. Open Deepgram Live Transcription Connection (SDK v3)
      const dgConnection = deepgram.listen.live({
        model: 'nova-2',
        language: lang,
        smart_format: true,
        interim_results: true,
        encoding: 'webm'
      });

      dgConnection.on(LiveTranscriptionEvents.Open, () => {
        console.log(`✅ Deepgram connected for room [${room}]`);
      });

      dgConnection.on(LiveTranscriptionEvents.Transcript, (data) => {
        const transcript = data.channel?.alternatives[0]?.transcript;
        if (transcript) {
          // Broadcast transcript to audience/viewers in this room
          broadcastToRoom(room, { type: 'caption', text: transcript });
        }
      });

      dgConnection.on(LiveTranscriptionEvents.Error, (err) => {
        console.error('❌ Deepgram Error:', err);
      });

      // Pass incoming audio chunks from browser to Deepgram
      ws.on('message', (chunk) => {
        if (Buffer.isBuffer(chunk) && dgConnection.getReadyState() === 1) {
          dgConnection.send(chunk);
        }
      });

      ws.on('close', () => {
        console.log(`🔌 Presenter disconnected from room [${room}]`);
        if (dgConnection) dgConnection.finish();
      });

    } else {
      // Audience / Viewer Connection Handling
      addClientToRoom(room, ws);
      ws.on('close', () => removeClientFromRoom(room, ws));
    }

  } catch (err) {
    console.error('WebSocket connection error:', err);
    ws.close(1011, 'Internal Server Error');
  }
});

function addClientToRoom(room, ws) {
  if (!activeRooms.has(room)) activeRooms.set(room, new Set());
  activeRooms.get(room).add(ws);
}

function removeClientFromRoom(room, ws) {
  if (activeRooms.has(room)) {
    activeRooms.get(room).delete(ws);
    if (activeRooms.get(room).size === 0) activeRooms.delete(room);
  }
}

function broadcastToRoom(room, payload) {
  const clients = activeRooms.get(room);
  if (clients) {
    const msg = JSON.stringify(payload);
    clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(msg);
      }
    });
  }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 Server listening on port ${PORT}`);
});
