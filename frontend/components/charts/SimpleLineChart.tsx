"use client";

import * as React from "react";

export type LinePoint = {
  label: string;
  value: number;
};

function clamp(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, n));
}

export default function SimpleLineChart({
  title,
  subtitle,
  data,
  height = 180,
  valueFormatter,
}: {
  title?: string;
  subtitle?: string;
  data: LinePoint[];
  height?: number;
  valueFormatter?: (v: number) => string;
}) {
  const w = 600;
  const h = Math.max(120, height);
  const padX = 36;
  const padY = 22;

  const values = data.map((d) => (Number.isFinite(d.value) ? d.value : 0));
  const minV = values.length ? Math.min(...values) : 0;
  const maxV = values.length ? Math.max(...values) : 0;
  const span = maxV - minV;
  const safeSpan = span === 0 ? 1 : span;
  const yMin = minV - safeSpan * 0.08;
  const yMax = maxV + safeSpan * 0.08;
  const ySpan = yMax - yMin || 1;

  const pts = data.map((d, i) => {
    const x = padX + (data.length <= 1 ? 0 : (i / (data.length - 1)) * (w - padX * 2));
    const yRaw = (d.value - yMin) / ySpan;
    const y = h - padY - clamp(yRaw, 0, 1) * (h - padY * 2);
    return { x, y, label: d.label, value: d.value };
  });

  const path = pts
    .map((p, i) => `${i === 0 ? "M" : "L"}${p.x.toFixed(1)},${p.y.toFixed(1)}`)
    .join(" ");

  const fmt = (v: number) => (valueFormatter ? valueFormatter(v) : String(Math.round(v)));

  return (
    <div className="overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm">
      <div className="h-1 w-full bg-blue-900" />
      {title || subtitle ? (
        <div className="border-b border-slate-200 px-4 py-3">
          {title ? <div className="text-sm font-semibold text-slate-900">{title}</div> : null}
          {subtitle ? <div className="mt-0.5 text-xs text-slate-500">{subtitle}</div> : null}
        </div>
      ) : null}

      <div className="p-4">
        <svg
          viewBox={`0 0 ${w} ${h}`}
          className="h-[180px] w-full"
          role="img"
          aria-label={title || "chart"}
          preserveAspectRatio="none"
        >
          {[0.25, 0.5, 0.75].map((t) => {
            const y = padY + t * (h - padY * 2);
            return (
              <line
                key={t}
                x1={padX}
                x2={w - padX}
                y1={y}
                y2={y}
                stroke="rgb(226,232,240)"
                strokeWidth={1}
              />
            );
          })}

          <line x1={padX} x2={padX} y1={padY} y2={h - padY} stroke="rgb(148,163,184)" strokeWidth={1} />
          <line x1={padX} x2={w - padX} y1={h - padY} y2={h - padY} stroke="rgb(148,163,184)" strokeWidth={1} />

          <path d={path} fill="none" stroke="rgb(30,64,175)" strokeWidth={2.2} />
          {pts.map((p, idx) => (
            <circle key={idx} cx={p.x} cy={p.y} r={3} fill="rgb(30,64,175)" />
          ))}

          <text x={4} y={padY + 6} fontSize={11} fill="rgb(100,116,139)">
            {fmt(yMax)}
          </text>
          <text x={4} y={h - padY} fontSize={11} fill="rgb(100,116,139)">
            {fmt(yMin)}
          </text>

          {pts.length ? (
            <>
              <text x={padX} y={h - 4} fontSize={11} fill="rgb(100,116,139)">
                {pts[0].label}
              </text>
              <text x={w - padX} y={h - 4} fontSize={11} textAnchor="end" fill="rgb(100,116,139)">
                {pts[pts.length - 1].label}
              </text>
            </>
          ) : null}
        </svg>

        {!data.length ? <div className="pt-2 text-sm text-slate-500">Нет данных для графика</div> : null}
      </div>
    </div>
  );
}
