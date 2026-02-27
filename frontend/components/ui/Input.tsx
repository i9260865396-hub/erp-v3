"use client";

import type { KeyboardEvent } from "react";

export default function Input({
  label,
  value,
  onChange,
  placeholder,
  type = "text",
  onKeyDown,
}: {
  label?: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  type?: "text" | "number" | "date";
  onKeyDown?: (e: KeyboardEvent<HTMLInputElement>) => void;
}) {
  return (
    <label className="block">
      {label ? <div className="mb-1 text-xs font-medium text-slate-700">{label}</div> : null}
      <input
        type={type}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={onKeyDown}
        placeholder={placeholder}
        className="h-10 w-full rounded-md border border-slate-200 bg-slate-50 px-3 text-sm outline-none focus:border-blue-400"
      />
    </label>
  );
}
