"use client";

import { useEffect, useMemo, useState } from "react";
import KpiCard from "@/components/kpi/KpiCard";
import DataTable, { Column } from "@/components/table/DataTable";
import Button from "@/components/ui/Button";
import { useToast } from "@/components/ui/Toast";
import { api } from "@/lib/api";
import type { LotRow, Material, Control } from "@/types/api";

export default function DashboardPage() {
  const toast = useToast();
  const [materials, setMaterials] = useState<Material[]>([]);
  const [lots, setLots] = useState<LotRow[]>([]);
  const [control, setControl] = useState<Control | null>(null);
  const [loading, setLoading] = useState(false);

  async function load() {
    setLoading(true);
    try {
      const [m, l, c] = await Promise.all([
        api<Material[]>("/materials"),
        api<LotRow[]>("/stock/lots"),
        api<Control>("/control"),
      ]);
      setControl(c);
      setMaterials(m);
      setLots(l);
    } catch (e: any) {
      toast.push(e.message || "Ошибка загрузки", "err");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const kpi = useMemo(() => {
    const lotsRemaining = lots.reduce((s, x) => s + (x.qty_remaining || 0), 0);
    const stockValue = lots.reduce((s, x) => s + (x.qty_remaining || 0) * (x.unit_cost || 0), 0);
    return {
      materials: materials.length,
      lotsRemaining,
      stockValue,
      draftPurchases: control?.draft_purchases ?? 0,
      openOrders: control?.open_orders ?? 0,
      lowStock: control?.low_stock ?? 0,
    };
  }, [materials, lots, control]);

  const columns: Column<LotRow>[] = [
    { key: "lot_id", title: "Партия" },
    { key: "material_name", title: "Материал" },
    { key: "qty_remaining", title: "Остаток" },
    { key: "unit_cost", title: "Себестоимость" },
    { key: "created_at", title: "Дата" },
  ];

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">Дашборд</div>
          <div className="mt-1 text-sm text-slate-500">KPI и состояние склада (FIFO партии)</div>
        </div>
        <Button variant="secondary" onClick={load} disabled={loading}>
          {loading ? "Загрузка…" : "Обновить"}
        </Button>
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
        <KpiCard title="Материалов" value={String(kpi.materials)} hint="Справочник" />
        <KpiCard title="Остаток (сумма)" value={kpi.lotsRemaining.toFixed(3)} hint="По партиям" />
        <KpiCard title="Стоимость склада" value={kpi.stockValue.toFixed(2)} hint="qty_remaining × unit_cost" />
      </div>

      <DataTable
        title="Партии на складе"
        columns={columns}
        rows={lots}
        onRefresh={load}
        onExport={() => toast.push("Экспорт: скоро", "ok")}
        rowActions={(row) => (
          <>
            <Button variant="secondary" onClick={() => toast.push(`Партия #${row.lot_id}`, "ok")}>
              Просмотр
            </Button>
            <Button variant="ghost" onClick={() => toast.push("EDIT/VOID: в MVP нет", "err")}>
              ...
            </Button>
          </>
        )}
      />
    </div>
  );
}
