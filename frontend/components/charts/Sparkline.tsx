"use client";

import React from "react";

function clamp(n: number, a: number, b: number) {
  return Math.max(a, Math.min(b, n));
}

export default function Sparkline({
  values,
  width = 220,
  height = 48,
  strokeClassName = "stroke-blue-700",
  fillClassName = "fill-blue-200/40",
}: {
  values: number[];
  width?: number;
  height?: number;
  strokeClassName?: string;
  fillClassName?: string;
}) {
  const v = (values || []).filter((x) => Number.isFinite(x));
  if (v.length < 2) {
    return (
      <div className="flex h-[48px] items-center justify-center text-xs text-slate-400">
        нет данных
      </div>
    );
  }

  const min = Math.min(...v);
  const max = Math.max(...v);
  const span = max - min || 1;

  const pad = 2;
  const innerW = width - pad * 2;
  const innerH = height - pad * 2;
  const step = innerW / (v.length - 1);

  const pts = v.map((x, i) => {
    const nx = pad + i * step;
    const ny = pad + innerH - ((x - min) / span) * innerH;
    return [nx, ny];
  });

  const line = "M " + pts.map((p) => `${p[0].toFixed(2)} ${p[1].toFixed(2)}`).join(" L ");

  const area =
    line +
    ` L ${(pad + innerW).toFixed(2)} ${(pad + innerH).toFixed(2)}` +
    ` L ${pad.toFixed(2)} ${(pad + innerH).toFixed(2)} Z`;

  const last = v[v.length - 1];
  const first = v[0];
  const delta = ((last - first) / (Math.abs(first) || 1)) * 100;
  const deltaTxt = `${delta >= 0 ? "+" : ""}${clamp(delta, -999, 999).toFixed(0)}%`;

  return (
    <div className="flex items-center justify-between gap-3">
      <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`}>
        <path d={area} className={fillClassName} />
        <path d={line} className={`${strokeClassName} fill-none`} strokeWidth={2} />
      </svg>
      <div className="w-12 text-right text-xs font-medium text-slate-600">{deltaTxt}</div>
    </div>
  );
}
