"use client";

import React from "react";

export default function BarMini({
  values,
  height = 40,
  barClassName = "bg-emerald-500/70",
}: {
  values: number[];
  height?: number;
  barClassName?: string;
}) {
  const v = (values || []).filter((x) => Number.isFinite(x));
  if (v.length === 0) {
    return (
      <div className="flex h-[40px] items-center justify-center text-xs text-slate-400">
        нет данных
      </div>
    );
  }
  const max = Math.max(...v) || 1;
  return (
    <div className="flex items-end gap-1" style={{ height }}>
      {v.map((x, i) => (
        <div
          key={i}
          className={`w-2 rounded-sm ${barClassName}`}
          style={{ height: `${(x / max) * 100}%` }}
          title={String(x)}
        />
      ))}
    </div>
  );
}
