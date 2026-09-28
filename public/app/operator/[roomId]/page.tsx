'use client';

import { useState } from 'react';
import { useCaptionStream } from '@/hooks/useCaptionStream';
import { useParams } from 'next/navigation';

export default function OperatorPage() {
  const params = useParams();
  const roomId = (params?.roomId as string) || 'default';
  const { captionData, sendCaptionUpdate } = useCaptionStream(roomId, true);
  const [inputText, setInputText] = useState('');

  const handleTextChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const text = e.target.value;
    setInputText(text);
    sendCaptionUpdate({ text, isFinal: false });
  };

  const handleStyleChange = (key: string, value: any) => {
    sendCaptionUpdate({ [key]: value });
  };

  return (
    <main className="min-h-screen bg-slate-950 text-slate-100 p-8">
      <div className="max-w-4xl mx-auto space-y-6">
        <div className="flex justify-between items-center border-b border-slate-800 pb-4">
          <h1 className="text-2xl font-bold">Operator Console — Room: <span className="text-indigo-400">{roomId}</span></h1>
        </div>

        {/* Caption Text Input */}
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-6">
          <label className="block text-sm font-medium text-slate-300 mb-2">Live Caption Text</label>
          <textarea
            value={inputText}
            onChange={handleTextChange}
            rows={4}
            placeholder="Type or stream captions here..."
            className="w-full bg-slate-950 border border-slate-800 rounded-lg p-4 text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
          />
        </div>

        {/* Styling & Position Controls */}
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 grid grid-cols-1 md:grid-cols-3 gap-6">
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-2">Overlay Position</label>
            <select
              value={captionData.position || 'bottom'}
              onChange={(e) => handleStyleChange('position', e.target.value)}
              className="w-full bg-slate-950 border border-slate-800 rounded-lg p-3 text-white"
            >
              <option value="top">Top</option>
              <option value="middle">Middle</option>
              <option value="bottom">Bottom</option>
            </select>
          </div>

          <div>
            <label className="block text-sm font-medium text-slate-300 mb-2">Font Size ({captionData.fontSize}px)</label>
            <input
              type="range"
              min={20}
              max={60}
              value={captionData.fontSize || 32}
              onChange={(e) => handleStyleChange('fontSize', Number(e.target.value))}
              className="w-full"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-slate-300 mb-2">Text Color</label>
            <input
              type="color"
              value={captionData.textColor || '#ffffff'}
              onChange={(e) => handleStyleChange('textColor', e.target.value)}
              className="w-full h-10 bg-slate-950 rounded-lg cursor-pointer"
            />
          </div>
        </div>
      </div>
    </main>
  );
}
