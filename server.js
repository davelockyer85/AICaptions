import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import { createClient, LiveTranscriptionEvents } from "@deepgram/sdk";
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";
import Stripe from "stripe";
import { createClient as createSupabaseClient } from "@supabase/supabase-js";

dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server });

// --- Client & API Initialization ---------------------------------------
const DEEPGRAM_KEY = process.env.DEEPGRAM_API_KEY;
if (!DEEPGRAM_KEY) {
  console.error("[WARNING] DEEPGRAM_API_KEY is missing in environment variables!");
}
const deepgram = createClient(DEEPGRAM_KEY);
const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);
const supabase = createSupabaseClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY
);

// --- Stripe Webhook Endpoint (Must be defined BEFORE global express.json()) ---
app.post("/webhook", express.raw({ type: "application/json" }), async (req, res) => {
  const sig = req.headers["stripe-signature"];
  let event;

  try {
    event = stripe.webhooks.constructEvent(
      req.body,
      sig,
      process.env.STRIPE_WEBHOOK_SECRET
    );
  } catch (err) {
    console.error(`Webhook signature verification failed:`, err.message);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  // Handle successful checkouts and subscription renewals
  if (event.type === "checkout.session.completed" || event.type === "invoice.paid") {
    const sessionOrInvoice = event.data.object;
    
    // Retrieve client_reference_id or metadata user ID
    const userId = sessionOrInvoice.client_reference_id || sessionOrInvoice.metadata?.supabase_user_id;
    
    if (!userId) {
      console.error("Webhook error: Missing user ID in session/invoice data.");
      return res.status(400).json({ error: "Missing user ID" });
    }

    let priceId;
    let subscriptionId = null;
    let maxStreamingSeconds = 0;
    let eventPassExpiresAt = null;

    if (event.type === "checkout.session.completed") {
      const lineItems = await stripe.checkout.sessions.listLineItems(sessionOrInvoice.id);
      priceId = lineItems.data[0]?.price?.id;
      subscriptionId = sessionOrInvoice.subscription || null;
    } else if (event.type === "invoice.paid") {
      priceId = sessionOrInvoice.lines?.data[0]?.price?.id;
      subscriptionId = sessionOrInvoice.subscription;
    }

    // Map Price IDs to Supabase columns
    // 1. Event Pass ($49 - One Time 2 Hours / 7200 seconds)
    if (priceId === "price_1UJtjiJv4dyhvuKyHFiWUnuI") {
      maxStreamingSeconds = 7200;
      eventPassExpiresAt = new Date(Date.now() + 2 * 60 * 60 * 1000).toISOString();
      subscriptionId = "one_time";
    } 
    // 2. Starter Plan ($179/mo - 25 Hours / 90000 seconds)
    else if (priceId === "price_1ULl5AJv4dyhvuKyGW8Ffyq2") {
      maxStreamingSeconds = 90000;
      eventPassExpiresAt = null;
    } 
    // 3. Pro Multi-Stage Plan ($299/mo - 60 Hours / 216000 seconds)
    else if (priceId === "price_1ULl2yJv4dyhvuKyhF9Gu7Wq") {
      maxStreamingSeconds = 216000;
      eventPassExpiresAt = null;
    }

    if (maxStreamingSeconds > 0) {
      const { error: updateError } = await supabase
        .from("users")
        .update({
          max_streaming_seconds: maxStreamingSeconds,
          event_pass_expires_at: eventPassExpiresAt,
          stripe_subscription_id: subscriptionId,
          streaming_seconds_used: 0 // Reset usage on new pass/billing cycle
        })
        .eq("user_id", userId);

      if (updateError) {
        console.error("Failed to update user in Supabase:", updateError);
        return res.status(500).json({ error: "Database update failed" });
      }

      console.log(`Successfully updated user ${userId} with plan details.`);
    }
  }

  res.json({ received: true });
});

// --- General Middleware (Applied AFTER webhook route) ------------------
app.use(express.json());

// --- Stripe Checkout Session Endpoint ----------------------------------
app.post("/create-checkout-session", async (req, res) => {
  try {
    const { priceId, mode, userId, email } = req.body;

    const session = await stripe.checkout.sessions.create({
      payment_method_types: ["card"],
      mode: mode, // 'payment' for one-time Event Pass, 'subscription' for Starter/Pro
      customer_email: email,
      client_reference_id: userId,
      line_items: [{ price: priceId, quantity: 1 }],
      success_url: `${req.headers.origin}/dashboard.html?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${req.headers.origin}/pricing.html`,
    });

    res.json({ url: session.url });
  } catch (error) {
    console.error("Error creating checkout session:", error);
    res.status(500).json({ error: error.message });
  }
});

// --- Google Translate Helper ------------------------------------------------
async function translateText(text, sourceLang, targetLang) {
  if (!text || !targetLang || targetLang === sourceLang) return text;
  const sl = (sourceLang || "en").split("-")[0];
  const tl = (targetLang || "en").split("-")[0];
  return text;
}
