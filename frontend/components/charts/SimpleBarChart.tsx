"use client";

import * as React from "react";

export type BarPoint = {
  label: string;
  value: number;
};

export default function SimpleBarChart({
  title,
  subtitle,
  data,
  height = 180,
  valueFormatter,
}: {
  title?: string;
  subtitle?: string;
  data: BarPoint[];
  height?: number;
  valueFormatter?: (v: number) => string;
}) {
  const w = 600;
  const h = Math.max(120, height);
  const padX = 36;
  const padY = 22;

  const values = data.map((d) => (Number.isFinite(d.value) ? d.value : 0));
  const maxV = values.length ? Math.max(...values) : 0;
  const safeMax = maxV === 0 ? 1 : maxV;

  const innerW = w - padX * 2;
  const innerH = h - padY * 2;
  const bw = data.length ? innerW / data.length : innerW;

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
          <line x1={padX} x2={padX} y1={padY} y2={h - padY} stroke="rgb(148,163,184)" strokeWidth={1} />
          <line x1={padX} x2={w - padX} y1={h - padY} y2={h - padY} stroke="rgb(148,163,184)" strokeWidth={1} />

          {data.map((d, i) => {
            const v = Math.max(0, d.value || 0);
            const barH = (v / safeMax) * innerH;
            const x = padX + i * bw + bw * 0.15;
            const y = padY + (innerH - barH);
            const width = bw * 0.7;
            return (
              <rect
                key={i}
                x={x}
                y={y}
                width={width}
                height={barH}
                rx={4}
                fill="rgb(30,64,175)"
                opacity={0.85}
              />
            );
          })}

          {data.length ? (
            <>
              <text x={padX} y={h - 4} fontSize={11} fill="rgb(100,116,139)">
                {data[0].label}
              </text>
              <text x={w - padX} y={h - 4} fontSize={11} textAnchor="end" fill="rgb(100,116,139)">
                {data[data.length - 1].label}
              </text>
            </>
          ) : null}

          <text x={4} y={padY + 6} fontSize={11} fill="rgb(100,116,139)">
            {fmt(maxV)}
          </text>
        </svg>

        {!data.length ? <div className="pt-2 text-sm text-slate-500">Нет данных для графика</div> : null}
      </div>
    </div>
  );
}
