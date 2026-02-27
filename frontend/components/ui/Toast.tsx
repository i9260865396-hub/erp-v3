"use client";

import * as React from "react";

type ToastItem = { id: string; text: string; kind: "ok" | "err" };

const ToastCtx = React.createContext<{
  push: (text: string, kind?: "ok" | "err") => void;
} | null>(null);

export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [items, setItems] = React.useState<ToastItem[]>([]);

  const push = React.useCallback((text: string, kind: "ok" | "err" = "ok") => {
    const id = Math.random().toString(36).slice(2);
    setItems((prev) => [...prev, { id, text, kind }]);
    window.setTimeout(() => {
      setItems((prev) => prev.filter((x) => x.id !== id));
    }, 2500);
  }, []);

  return (
    <ToastCtx.Provider value={{ push }}>
      {children}
      <div className="fixed bottom-4 right-4 z-[80] space-y-2">
        {items.map((t) => (
          <div
            key={t.id}
            className={[
              "min-w-[260px] rounded-lg border px-3 py-2 text-sm shadow-lg",
              t.kind === "ok"
                ? "border-emerald-200 bg-emerald-50 text-emerald-800"
                : "border-rose-200 bg-rose-50 text-rose-800",
            ].join(" ")}
          >
            {t.text}
          </div>
        ))}
      </div>
    </ToastCtx.Provider>
  );
}

export function useToast() {
  const ctx = React.useContext(ToastCtx);
  if (!ctx) throw new Error("ToastProvider missing");
  return ctx;
}
