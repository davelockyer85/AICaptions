// Endpoint to create a Stripe Checkout Session
app.post("/create-checkout-session", async (req, res) => {
  try {
    const { priceId, userId } = req.body;

    if (!priceId) {
      return res.status(400).json({ error: "Missing priceId" });
    }

    // Determine if this is a one-time purchase (like an Event Pass) 
    // or a recurring subscription (Starter/Pro)
    const isOneTime = priceId === process.env.EVENT_PASS_PRICE_ID;

    const session = await stripe.checkout.sessions.create({
      payment_method_types: ["card"],
      line_items: [
        {
          price: priceId,
          quantity: 1,
        },
      ],
      // Use 'payment' for one-time Event Passes, 'subscription' for recurring plans
      mode: isOneTime ? "payment" : "subscription",
      client_reference_id: userId || null,
      success_url: `${process.env.CLIENT_URL || req.headers.origin}/dashboard.html?success=true`,
      cancel_url: `${process.env.CLIENT_URL || req.headers.origin}/pricing.html?canceled=true`,
    });

    res.json({ url: session.url });
  } catch (error) {
    console.error("Stripe session error:", error);
    res.status(500).json({ error: error.message });
  }
});
