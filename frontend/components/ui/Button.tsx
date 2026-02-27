"use client";

import * as React from "react";

type Variant = "primary" | "secondary" | "ghost" | "danger";

export default function Button({
  children,
  onClick,
  disabled,
  variant = "secondary",
  type = "button",
  className = "",
}: {
  children: React.ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  variant?: Variant;
  type?: "button" | "submit";
  className?: string;
}) {
  const base =
    "inline-flex h-9 items-center justify-center rounded-md px-3 text-sm font-medium transition focus:outline-none focus:ring-2 focus:ring-blue-300 disabled:opacity-50 disabled:pointer-events-none";

  const map: Record<Variant, string> = {
    primary: "bg-blue-900 text-white hover:bg-blue-800",
    secondary: "bg-white text-slate-900 border border-slate-200 hover:bg-slate-50",
    ghost: "bg-transparent text-slate-700 hover:bg-slate-100",
    danger: "bg-rose-600 text-white hover:bg-rose-500",
  };

  return (
    <button
      type={type}
      onClick={onClick}
      disabled={disabled}
      className={`${base} ${map[variant]} ${className}`}
    >
      {children}
    </button>
  );
}
