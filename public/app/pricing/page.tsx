'use client';

import { useState } from 'react';

const PRICING_TIERS = [
  {
    id: 'starter',
    name: 'Starter',
    description: 'For individual streamers and simple presentations.',
    priceMonthly: '$15',
    priceAnnual: '$12',
    priceIdMonthly: 'price_starter_monthly', // Replace with your Stripe Price ID
    priceIdAnnual: 'price_starter_annual',   // Replace with your Stripe Price ID
    features: ['1 Live Streamer Session', 'OBS Transparent Overlay', 'Standard Latency'],
  },
  {
    id: 'pro',
    name: 'Pro Presenter',
    description: 'For professional operators and event productions.',
    popular: true,
    priceMonthly: '$39',
    priceAnnual: '$29',
    priceIdMonthly: 'price_pro_monthly',     // Replace with your Stripe Price ID
    priceIdAnnual: 'price_pro_annual',       // Replace with your Stripe Price ID
    features: ['Unlimited Streams', 'Low-latency Realtime Broadcast', 'Custom Overlay CSS', 'Stage Presenter View'],
  },
];

export default function PricingPage() {
  const [isAnnual, setIsAnnual] = useState(true);
  const [loadingPriceId, setLoadingPriceId] = useState<string | null>(null);

  const handleCheckout = async (priceId: string) => {
    setLoadingPriceId(priceId);
    try {
      const res = await fetch('/api/stripe/checkout', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ priceId }),
      });

      const { url, error } = await res.json();
      if (error) throw new Error(error);

      if (url) window.location.href = url;
    } catch (err: any) {
      alert(err.message || 'Checkout failed');
    } finally {
      setLoadingPriceId(null);
    }
  };

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 py-16 px-4">
      <div className="max-w-4xl mx-auto text-center">
        <h1 className="text-4xl font-extrabold tracking-tight text-white">Choose Your Plan</h1>
        <div className="mt-8 flex justify-center items-center gap-4">
          <span className="text-sm">Monthly</span>
          <button
            onClick={() => setIsAnnual(!isAnnual)}
            className="w-14 h-8 bg-indigo-600 rounded-full p-1 relative"
          >
            <div className={`w-6 h-6 bg-white rounded-full transition-transform ${isAnnual ? 'translate-x-6' : 'translate-x-0'}`} />
          </button>
          <span className="text-sm font-semibold">Annual (Save 20%)</span>
        </div>

        <div className="mt-12 grid grid-cols-1 md:grid-cols-2 gap-8 text-left">
          {PRICING_TIERS.map((tier) => {
            const priceId = isAnnual ? tier.priceIdAnnual : tier.priceIdMonthly;
            const price = isAnnual ? tier.priceAnnual : tier.priceMonthly;

            return (
              <div key={tier.id} className={`rounded-2xl bg-slate-900 border p-8 flex flex-col justify-between ${tier.popular ? 'border-indigo-500' : 'border-slate-800'}`}>
                <div>
                  <h3 className="text-2xl font-bold">{tier.name}</h3>
                  <p className="mt-2 text-sm text-slate-400">{tier.description}</p>
                  <p className="mt-6 text-4xl font-bold">{price} <span className="text-sm text-slate-400">/ mo</span></p>
                  <ul className="mt-6 space-y-2 text-sm text-slate-300">
                    {tier.features.map((f, i) => <li key={i}>✓ {f}</li>)}
                  </ul>
                </div>
                <button
                  onClick={() => handleCheckout(priceId)}
                  disabled={loadingPriceId === priceId}
                  className="mt-8 w-full bg-indigo-600 py-3 rounded-xl font-semibold hover:bg-indigo-500 transition-colors"
                >
                  {loadingPriceId === priceId ? 'Loading...' : 'Subscribe'}
                </button>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
