import 'dotenv/config';

import express from 'express';
import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { WebSocket, WebSocketServer } from 'ws';
import Stripe from 'stripe';
import {
  createClient as createDeepgramClient,
  LiveTranscriptionEvents,
} from '@deepgram/sdk';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';

const PORT = Number(process.env.PORT || 10000);
const DEFAULT_ROOM_ID = 'main-stage';

const MAX_WEBSOCKET_PAYLOAD_BYTES = 1024 * 1024;
const MAX_AUDIO_QUEUE_BYTES = 2 * 1024 * 1024;
const MAX_WEBSOCKET_BUFFERED_BYTES = 1024 * 1024;
const USAGE_PULSE_MS = 10_000;
const DEEPGRAM_CONNECT_TIMEOUT_MS = 15_000;
const MAX_VIEWERS_PER_ROOM = Number(process.env.MAX_VIEWERS_PER_ROOM || 500);

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({
  server,
  maxPayload: MAX_WEBSOCKET_PAYLOAD_BYTES,
});

app.use(express.json({ limit: '16kb' }));
app.use(express.static('public'));

const stripe = process.env.STRIPE_SECRET_KEY
  ? new Stripe(process.env.STRIPE_SECRET_KEY)
  : null;

const deepgramApiKey = process.env.DEEPGRAM_API_KEY;
const deepgram = deepgramApiKey
  ? createDeepgramClient(deepgramApiKey)
  : null;

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

// This key must stay on the server. Do not use it in browser code.
const supabase = supabaseUrl && supabaseServiceRoleKey
  ? createSupabaseClient(supabaseUrl, supabaseServiceRoleKey, {
      auth: {
        autoRefreshToken: false,
        persistSession: false,
        detectSessionInUrl: false,
      },
    })
  : null;

const allowGuestPresenters =
  process.env.ALLOW_GUEST_PRESENTERS === 'true';

const oneTimePriceIds = new Set(
  [
    process.env.EVENT_PASS_PRICE_ID,
    process.env.PRO_EVENT_PASS_PRICE_ID,
    ...(process.env.ONE_TIME_PRICE_IDS || '').split(','),
  ]
    .map((value) => value.trim())
    .filter(Boolean),
);

const allowedPriceIds = new Set(
  [
    ...oneTimePriceIds,
    ...(process.env.ALLOWED_PRICE_IDS || '').split(','),
  ]
    .map((value) => value.trim())
    .filter(Boolean),
);

// In-memory state is suitable only for a single server process.
const rooms = new Map();
const activeRoomsByUser = new Map();

if (!deepgram) {
  console.warn('DEEPGRAM_API_KEY is missing; presenter connections will be rejected.');
}

if (!supabase) {
  console.warn(
    'SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is missing; authenticated features are unavailable.',
  );
}

function getOrCreateRoom(roomId) {
  let room = rooms.get(roomId);

  if (!room) {
    room = {
      presenterWs: null,
      presenterUserId: null,
      viewers: new Set(),
    };
    rooms.set(roomId, room);
  }

  return room;
}

function deleteRoomIfEmpty(roomId, room) {
  if (
    room.presenterWs === null &&
    room.viewers.size === 0 &&
    rooms.get(roomId) === room
  ) {
    rooms.delete(roomId);
  }
}

function addUserRoom(userId, roomId) {
  let userRooms = activeRoomsByUser.get(userId);

  if (!userRooms) {
    userRooms = new Set();
    activeRoomsByUser.set(userId, userRooms);
  }

  userRooms.add(roomId);
}

function removeUserRoom(userId, roomId) {
  const userRooms = activeRoomsByUser.get(userId);
  if (!userRooms) return;

  userRooms.delete(roomId);

  if (userRooms.size === 0) {
    activeRoomsByUser.delete(userId);
  }
}

function safeClose(ws, code, reason) {
  if (
    ws.readyState === WebSocket.OPEN ||
    ws.readyState === WebSocket.CONNECTING
  ) {
    ws.close(code, reason);
  }
}

function safeSend(ws, payload) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;

  if (ws.bufferedAmount > MAX_WEBSOCKET_BUFFERED_BYTES) {
    safeClose(ws, 1013, 'Connection is too slow');
    return false;
  }

  try {
    ws.send(payload);
    return true;
  } catch (error) {
    console.error('WebSocket send failed:', error);
    safeClose(ws, 1011, 'Send failed');
    return false;
  }
}

function sendJson(ws, payload) {
  safeSend(ws, JSON.stringify(payload));
}

function getBearerToken(req) {
  const authorization = req.headers.authorization;

  if (!authorization?.startsWith('Bearer ')) {
    return null;
  }

  return authorization.slice('Bearer '.length).trim() || null;
}

async function getAuthenticatedUser(token) {
