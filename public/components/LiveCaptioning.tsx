'use client';

import { useState, useRef, useEffect } from 'react';

export default function LiveCaptioning() {
  const [isStreaming, setIsStreaming] = useState(false);
  const [transcript, setTranscript] = useState('');
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  const socketRef = useRef<WebSocket | null>(null);
  const mediaRecorderRef = useRef<MediaRecorder | null>(null);

  const startStreaming = async () => {
    setErrorMsg(null);
    try {
      // 1. Fetch short-lived token & tier config from API
      const res = await fetch('/api/deepgram/token');
      const data = await res.json();

      if (!res.ok) throw new Error(data.error || 'Failed to authorize streaming session');

      const { key, config } = data;

      // 2. Request access to user's microphone
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });

      // 3. Construct WebSocket parameters
      const params = new URLSearchParams({
        model: config.model,
        smart_format: String(config.smart_format),
        interim_results: String(config.interim_results),
        language: config.language,
        utterance_end_ms: String(config.utterance_end_ms),
      });

      // 4. Open direct WebSocket connection to Deepgram
      const socket = new WebSocket(`wss://api.deepgram.com/v1/listen?${params.toString()}`, [
        'token',
        key,
      ]);

      socket.onopen = () => {
        setIsStreaming(true);

        // Send audio chunks every 250ms
        const mediaRecorder = new MediaRecorder(stream, { mimeType: 'audio/webm' });
        mediaRecorder.addEventListener('dataavailable', (event) => {
          if (event.data.size > 0 && socket.readyState === WebSocket.OPEN) {
            socket.send(event.data);
          }
        });
        mediaRecorder.start(250);
        mediaRecorderRef.current = mediaRecorder;
      };

      socket.onmessage = (message) => {
        const received = JSON.parse(message.data);
        const sentence = received.channel?.alternatives[0]?.transcript;

        if (sentence && sentence.trim().length > 0) {
          if (received.is_final) {
            setTranscript((prev) => `${prev} ${sentence}`);
          }
        }
      };

      socket.onerror = (err) => {
        console.error('Deepgram WebSocket Error:', err);
        setErrorMsg('WebSocket connection error.');
      };

      socket.onclose = () => {
        setIsStreaming(false);
      };

      socketRef.current = socket;
    } catch (err: any) {
      setErrorMsg(err.message || 'Could not start live captions.');
    }
  };

  const stopStreaming = () => {
    if (mediaRecorderRef.current && mediaRecorderRef.current.state !== 'inactive') {
      mediaRecorderRef.current.stop();
      mediaRecorderRef.current.stream.getTracks().forEach((track) => track.stop());
    }

    if (socketRef.current && socketRef.current.readyState === WebSocket.OPEN) {
      socketRef.current.close();
    }

    setIsStreaming(false);
  };

  useEffect(() => {
    return () => {
      stopStreaming();
    };
  }, []);

  return (
    <div className="max-w-2xl mx-auto p-6 bg-slate-900 border border-slate-800 rounded-xl text-white">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-xl font-bold">Live Caption Engine</h2>
        <div className="flex items-center gap-2">
          <span className={`w-3 h-3 rounded-full ${isStreaming ? 'bg-emerald-500 animate-pulse' : 'bg-slate-600'}`} />
          <span className="text-xs text-slate-400">{isStreaming ? 'STREAMING' : 'OFFLINE'}</span>
        </div>
      </div>

      {errorMsg && (
        <div className="mb-4 p-3 bg-red-950/80 border border-red-500 text-red-200 text-sm rounded-lg">
          {errorMsg}
        </div>
      )}

      {/* Realtime Output Box */}
      <div className="h-48 overflow-y-auto bg-slate-950 p-4 rounded-lg border border-slate-800 text-slate-200 font-mono text-sm leading-relaxed">
        {transcript || <span className="text-slate-600 italic">Captions will appear here once you begin speaking...</span>}
      </div>

      {/* Controls */}
      <div className="mt-4 flex gap-3">
        {!isStreaming ? (
          <button
            onClick={startStreaming}
            className="w-full bg-indigo-600 hover:bg-indigo-500 text-white font-semibold py-2.5 rounded-lg transition-all"
          >
            Start Captioning
          </button>
        ) : (
          <button
            onClick={stopStreaming}
            className="w-full bg-red-600 hover:bg-red-500 text-white font-semibold py-2.5 rounded-lg transition-all"
          >
            Stop Stream
          </button>
        )}
      </div>
    </div>
  );
}
