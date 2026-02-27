"use client";

import { useMemo, useState } from "react";
import Button from "@/components/ui/Button";

export type Column<T> = {
  key: keyof T;
  title: string;
  className?: string;
  render?: (row: T) => React.ReactNode;
};

export default function DataTable<T extends Record<string, any>>({
  title,
  columns,
  rows,
  searchPlaceholder = "Поиск…",
  pageSize = 10,
  actions,
  onAdd,
  onRefresh,
  onExport,
  rowActions,
}: {
  title: string;
  columns: Column<T>[];
  rows: T[];
  searchPlaceholder?: string;
  pageSize?: number;
  actions?: React.ReactNode;
  onAdd?: () => void;
  onRefresh?: () => void;
  onExport?: () => void;
  rowActions?: (row: T) => React.ReactNode;
}) {
  const [q, setQ] = useState("");
  const [page, setPage] = useState(1);

  const filtered = useMemo(() => {
    const query = q.trim().toLowerCase();
    if (!query) return rows;
    return rows.filter((r) =>
      Object.values(r).some((v) => String(v ?? "").toLowerCase().includes(query)),
    );
  }, [q, rows]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize));
  const safePage = Math.min(page, totalPages);
  const start = (safePage - 1) * pageSize;
  const pageRows = filtered.slice(start, start + pageSize);

  return (
    <section className="overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm">
      {/* синий акцент как в хедере */}
      <div className="h-1 w-full bg-blue-900" />
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-200 p-4">
        <div className="text-sm font-semibold text-slate-900">{title}</div>

        <div className="flex flex-wrap items-center gap-2">
          <input
            value={q}
            onChange={(e) => {
              setQ(e.target.value);
              setPage(1);
            }}
            placeholder={searchPlaceholder}
            className="h-9 w-64 rounded-md border border-slate-200 bg-slate-50 px-3 text-sm outline-none focus:border-blue-400"
          />

          {actions ? <div className="flex items-center gap-2">{actions}</div> : null}

          {onRefresh ? (
            <Button onClick={onRefresh} variant="secondary">
              Обновить
            </Button>
          ) : null}

          {onExport ? (
            <Button onClick={onExport} variant="secondary">
              Экспорт
            </Button>
          ) : null}

          {onAdd ? (
            <Button onClick={onAdd} variant="primary">
              Добавить
            </Button>
          ) : null}
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="min-w-full border-separate border-spacing-0">
          <thead>
            <tr className="bg-slate-50">
              {columns.map((c) => (
                <th
                  key={String(c.key)}
                  className="whitespace-nowrap border-b border-slate-200 px-4 py-3 text-left text-xs font-semibold text-slate-600"
                >
                  {c.title}
                </th>
              ))}
              {rowActions ? (
                <th className="border-b border-slate-200 px-4 py-3 text-right text-xs font-semibold text-slate-600">
                  Действия
                </th>
              ) : null}
            </tr>
          </thead>

          <tbody>
            {pageRows.map((row, idx) => (
              <tr key={idx} className="hover:bg-slate-50">
                {columns.map((c) => (
                  <td
                    key={String(c.key)}
                    className={[
                      "border-b border-slate-100 px-4 py-3 text-sm text-slate-900",
                      c.className ?? "",
                    ].join(" ")}
                  >
                    {c.render ? c.render(row) : String(row[c.key] ?? "")}
                  </td>
                ))}

                {rowActions ? (
                  <td className="border-b border-slate-100 px-4 py-3">
                    <div className="flex justify-end gap-2">{rowActions(row)}</div>
                  </td>
                ) : null}
              </tr>
            ))}

            {pageRows.length === 0 ? (
              <tr>
                <td
                  colSpan={columns.length + (rowActions ? 1 : 0)}
                  className="px-4 py-10 text-center text-sm text-slate-500"
                >
                  Нет данных
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>

      <div className="flex items-center justify-between gap-2 p-4">
        <div className="text-xs text-slate-500">
          Строк: {filtered.length} • Стр {safePage} / {totalPages}
        </div>
        <div className="flex gap-2">
          <Button
            variant="secondary"
            disabled={safePage <= 1}
            onClick={() => setPage((p) => Math.max(1, p - 1))}
          >
            Назад
          </Button>
          <Button
            variant="secondary"
            disabled={safePage >= totalPages}
            onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
          >
            Вперёд
          </Button>
        </div>
      </div>
    </section>
  );
}
