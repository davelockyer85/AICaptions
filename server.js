import express from 'express';
import { createServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import Stripe from 'stripe';
import { createClient } from '@deepgram/sdk';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 10000;
const stripe = process.env.STRIPE_SECRET_KEY ? new Stripe(process.env.STRIPE_SECRET_KEY) : null;
const deepgram = process.env.DEEPGRAM_API_KEY ? createClient(process.env.DEEPGRAM_API_KEY) : null;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Store active rooms and connected overlay clients
const rooms = new Map();

// Stripe Checkout Endpoint
app.post('/api/stripe/checkout', async (req, res) => {
  if (!stripe) {
    return res.status(500).json({ error: 'Stripe API key not configured.' });
  }
  try {
    const { priceId } = req.body;
    const session = await stripe.checkout.sessions.create({
      payment_method_types: ['card'],
      line_items: [{ price: priceId, quantity: 1 }],
      mode: 'subscription',
      success_url: `${req.headers.origin}/?success=true`,
      cancel_url: `${req.headers.origin}/pricing.html`,
    });
    res.json({ url: session.url });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// WebSocket Connection Handler
wss.on('connection', (ws, req) => {
  const urlParams = new URLSearchParams(req.url.replace(/^.*\?/, ''));
  const roomId = urlParams.get('roomId') || 'default';
  const role = urlParams.get('role') || 'viewer';

  if (!rooms.has(roomId)) {
    rooms.set(roomId, new Set());
  }
  const roomClients = rooms.get(roomId);
  roomClients.add(ws);

  let dgLive = null;

  if (role === 'presenter' && deepgram) {
    dgLive = deepgram.listen.live({
      model: 'nova-2',
      language: 'en-US',
      smart_format: true,
      interim_results: true,
    });

    dgLive.on('open', () => {
      console.log(`Deepgram live connection open for room: ${roomId}`);
    });

    dgLive.on('transcript', (data) => {
      const transcript = data.channel.alternatives[0]?.transcript || '';
      if (transcript) {
        const payload = JSON.stringify({
          type: 'caption',
          text: transcript,
          isFinal: data.is_final,
        });

        // Broadcast to stream overlay viewers in the same room
        roomClients.forEach((client) => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(payload);
          }
        });
      }
    });

    dgLive.on('error', (err) => console.error('Deepgram Error:', err));
  }

  ws.on('message', (message) => {
    if (role === 'presenter' && dgLive && dgLive.getReadyState() === 1) {
      dgLive.send(message);
    }
  });

  ws.on('close', () => {
    roomClients.delete(ws);
    if (roomClients.size === 0) rooms.delete(roomId);
    if (dgLive) dgLive.finish();
  });
});

server.listen(PORT, () => {
  console.log(`AICaptions server listening on port ${PORT}`);
});
