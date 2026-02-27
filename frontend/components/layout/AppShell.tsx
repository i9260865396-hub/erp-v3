"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const NAV = [
  { href: "/dashboard", label: "Дашборд" },
  { href: "/warehouse", label: "Склад FIFO" },
  { href: "/money", label: "Деньги" },
  { href: "/production", label: "Производство" },
  { href: "/sales", label: "Продажи" },
  { href: "/marketplaces", label: "Маркетплейсы" },
  { href: "/finance", label: "Финансы" },
  { href: "/analytics", label: "Аналитика" },
];

export default function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();

  return (
    <div className="min-h-dvh bg-slate-50 text-slate-900">
      {/* Синий header строго */}
      <header className="sticky top-0 z-50 border-b border-blue-800 bg-blue-900">
        <div className="mx-auto flex h-14 max-w-[1400px] items-center gap-4 px-4">
          <div className="flex items-center gap-3">
            <div className="h-8 w-8 rounded-lg bg-white/10" />
            <div className="leading-tight">
              <div className="text-sm font-semibold text-white">ERP</div>
              <div className="text-[11px] text-white/70">SaaS • tenant</div>
            </div>
          </div>

          <nav className="ml-6 hidden items-center gap-2 lg:flex">
            {NAV.map((item) => {
              const isActive = pathname === item.href;

              return (
                <Link
                  key={item.href}
                  href={item.href}
                  className={[
                    "rounded-md px-3 py-1.5 text-sm transition",
                    isActive
                      ? "bg-white/15 text-white"
                      : "text-white/75 hover:bg-white/10 hover:text-white",
                  ].join(" ")}
                >
                  {item.label}
                </Link>
              );
            })}
          </nav>

          <div className="ml-auto flex items-center gap-2">
            <Link
              href="/imports"
              className="rounded-md bg-white/10 px-3 py-1.5 text-sm text-white/90 hover:bg-white/15"
            >
              Импорт
            </Link>
            <Link
              href="/sync"
              className="rounded-md bg-white/10 px-3 py-1.5 text-sm text-white/90 hover:bg-white/15"
            >
              Sync
            </Link>
            <button className="rounded-md bg-white px-3 py-1.5 text-sm font-medium text-blue-900 hover:bg-white/90">
              Профиль
            </button>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-[1400px] px-4 py-6">{children}</main>
    </div>
  );
}
