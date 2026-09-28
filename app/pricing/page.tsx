'use client';

import { useState, useEffect } from 'react';
import { createClientComponentClient } from '@supabase/auth-helpers-nextjs';

const PRICING_TIERS = [
  {
    id: 'starter',
    name: 'Starter Streamer',
    description: 'Perfect for solo streamers and small presentation rooms.',
    priceMonthly: '$15',
    priceAnnual: '$12',
    priceIdMonthly: 'price_starter_monthly_id', // Put real Stripe Price ID here
    priceIdAnnual: 'price_starter_annual_id',   // Put real Stripe Price ID here
    features: [
      '1 Concurrent Stream Session',
      'OBS Transparent Web Overlay',
      'Standard Latency (~500ms)',
      'Basic Custom Styles',
    ],
  },
  {
    id: 'pro',
    name: 'Pro Presenter',
    description: 'Designed for high-end events, broadcast, and stage operators.',
    popular: true,
    priceMonthly: '$39',
    priceAnnual: '$29',
    priceIdMonthly: 'price_pro_monthly_id',     // Put real Stripe Price ID here
    priceIdAnnual: 'price_pro_annual_id',       // Put real Stripe Price ID here
    features: [
      'Unlimited Concurrent Streams',
      'Low Latency Realtime Engine (<100ms)',
      'Stage Presenter / Confidence View',
      'OBS Custom CSS Overlay Engine',
      'Multi-Language Translation Feed',
      'Priority Support',
    ],
  },
];

export default function PricingPage() {
  const [isAnnual, setIsAnnual] = useState(true);
  const [loadingPriceId, setLoadingPriceId] = useState<string | null>(null);
  const [portalLoading, setPortalLoading] = useState(false);
  const [userSub, setUserSub] = useState<{ status: string; plan_tier: string } | null>(null);

  const supabase = createClientComponentClient();

  useEffect(() => {
    async function loadSubscription() {
      const { data: { user } } = await supabase.auth.getUser();
      if (user) {
        const { data } = await supabase
          .from('subscriptions')
          .select('status, plan_tier')
          .eq('user_id', user.id)
          .maybeSingle();

        if (data) setUserSub(data);
      }
    }
    loadSubscription();
  }, [supabase]);

  const handleCheckout = async (priceId: string) => {
    setLoadingPriceId(priceId);
    try {
      const res = await fetch('/api/stripe/checkout', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ priceId }),
      });

      const data = await res.json();
      if (data.error) throw new Error(data.error);

      if (data.url) window.location.href = data.url;
    } catch (err: any) {
      alert(err.message || 'Checkout error');
    } finally {
      setLoadingPriceId(null);
    }
  };

  const handleOpenPortal = async () => {
    setPortalLoading(true);
    try {
      const res = await fetch('/api/stripe/portal', { method: 'POST' });
      const data = await res.json();
      if (data.error) throw new Error(data.error);

      if (data.url) window.location.href = data.url;
    } catch (err: any) {
      alert(err.message || 'Could not open billing portal');
    } finally {
      setPortalLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 py-16 px-4">
      <div className="max-w-4xl mx-auto text-center">
        <h1 className="text-4xl font-extrabold tracking-tight text-white">AICaptions Pricing</h1>
        <p className="mt-3 text-slate-400">Low-latency live captions for operators, streamers, and presenters.</p>

        {/* Subscribed Banner */}
        {userSub?.status === 'active' && (
          <div className="mt-8 p-4 bg-indigo-950/80 border border-indigo-500 rounded-xl flex items-center justify-between max-w-xl mx-auto">
            <div className="text-left">
              <p className="font-semibold text-indigo-200">Active Subscription: <span className="uppercase font-bold text-white">{userSub.plan_tier}</span></p>
              <p className="text-xs text-indigo-300">You can manage your payment methods or plan via Stripe Customer Portal.</p>
            </div>
            <button
              onClick={handleOpenPortal}
              disabled={portalLoading}
              className="bg-indigo-600 hover:bg-indigo-500 text-white px-4 py-2 rounded-lg text-sm font-semibold transition-all shrink-0 ml-4"
            >
              {portalLoading ? 'Loading...' : 'Manage Billing'}
            </button>
          </div>
        )}

        {/* Toggle */}
        <div className="mt-8 flex justify-center items-center gap-4">
          <span className={`text-sm ${!isAnnual ? 'text-white font-semibold' : 'text-slate-400'}`}>Monthly</span>
          <button
            onClick={() => setIsAnnual(!isAnnual)}
            className="w-14 h-8 bg-indigo-600 rounded-full p-1 relative transition-colors"
          >
            <div className={`w-6 h-6 bg-white rounded-full transition-transform ${isAnnual ? 'translate-x-6' : 'translate-x-0'}`} />
          </button>
          <span className={`text-sm ${isAnnual ? 'text-white font-semibold' : 'text-slate-400'}`}>Annual (Save 20%)</span>
        </div>

        {/* Cards */}
        <div className="mt-12 grid grid-cols-1 md:grid-cols-2 gap-8 text-left">
          {PRICING_TIERS.map((tier) => {
            const priceId = isAnnual ? tier.priceIdAnnual : tier.priceIdMonthly;
            const price = isAnnual ? tier.priceAnnual : tier.priceMonthly;
            const isCurrentTier = userSub?.status === 'active' && userSub.plan_tier === tier.id;

            return (
              <div key={tier.id} className={`rounded-2xl bg-slate-900 border p-8 flex flex-col justify-between ${tier.popular ? 'border-indigo-500' : 'border-slate-800'}`}>
                <div>
                  <h3 className="text-2xl font-bold text-white">{tier.name}</h3>
                  <p className="mt-2 text-sm text-slate-400">{tier.description}</p>
                  <p className="mt-6 text-4xl font-extrabold text-white">{price} <span className="text-sm text-slate-400 font-normal">/ month</span></p>
                  <ul className="mt-6 space-y-3 text-sm text-slate-300">
                    {tier.features.map((f, i) => (
                      <li key={i} className="flex items-center gap-2">
                        <span className="text-emerald-400 font-bold">✓</span> {f}
                      </li>
                    ))}
                  </ul>
                </div>

                {isCurrentTier ? (
                  <button
                    onClick={handleOpenPortal}
                    className="mt-8 w-full bg-slate-800 text-slate-300 py-3 rounded-xl font-semibold hover:bg-slate-700 transition-colors"
                  >
                    Current Plan (Manage)
                  </button>
                ) : (
                  <button
                    onClick={() => handleCheckout(priceId)}
                    disabled={loadingPriceId === priceId}
                    className="mt-8 w-full bg-indigo-600 text-white py-3 rounded-xl font-semibold hover:bg-indigo-500 transition-colors disabled:opacity-50"
                  >
                    {loadingPriceId === priceId ? 'Connecting to Stripe...' : 'Upgrade Now'}
                  </button>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
