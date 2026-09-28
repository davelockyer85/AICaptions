import { NextRequest, NextResponse } from 'next/server';
import { createClient as createDeepgramClient } from '@deepgram/sdk';
import { createRouteHandlerClient } from '@supabase/auth-helpers-nextjs';
import { cookies } from 'next/headers';
import { createClient as createSupabaseAdmin } from '@supabase/supabase-js';

const deepgram = createDeepgramClient(process.env.DEEPGRAM_API_KEY!);

const supabaseAdmin = createSupabaseAdmin(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

export async function GET(req: NextRequest) {
  try {
    // 1. Authenticate user session
    const supabase = createRouteHandlerClient({ cookies });
    const { data: { user } } = await supabase.auth.getUser();

    if (!user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    // 2. Fetch user active subscription from Supabase
    const { data: sub } = await supabaseAdmin
      .from('subscriptions')
      .select('status, plan_tier')
      .eq('user_id', user.id)
      .maybeSingle();

    if (!sub || sub.status !== 'active') {
      return NextResponse.json(
        { error: 'Active subscription required to access live captioning.' },
        { status: 403 }
      );
    }

    // 3. Define feature tier configs
    const tierConfig = {
      model: sub.plan_tier === 'pro' ? 'nova-2' : 'nova-2-general',
      smart_format: true,
      interim_results: true,
      utterance_end_ms: 1000,
      language: 'en-US',
      // Pro-only features
      filler_words: sub.plan_tier === 'pro',
      diarize: sub.plan_tier === 'pro',
    };

    // 4. Create a temporary, short-lived Deepgram API key
    const { result, error } = await deepgram.manage.createProjectKey(
      process.env.DEEPGRAM_PROJECT_ID!,
      {
        comment: `Temp key for user ${user.id}`,
        scopes: ['usage:write'],
        time_to_live_in_seconds: 60, // Key expires quickly after connection establishment
      }
    );

    if (error || !result?.key) {
      throw new Error(error?.message || 'Failed to generate temporary Deepgram key');
    }

    return NextResponse.json({
      key: result.key,
      config: tierConfig,
    });
  } catch (err: any) {
    console.error('Deepgram Key Generation Error:', err);
    return NextResponse.json({ error: err.message || 'Internal Server Error' }, { status: 500 });
  }
}
