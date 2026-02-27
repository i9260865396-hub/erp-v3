"use client";

import { useEffect, useMemo, useState } from "react";

import KpiCard from "@/components/kpi/KpiCard";
import Button from "@/components/ui/Button";
import Input from "@/components/ui/Input";
import DataTable, { Column } from "@/components/table/DataTable";
import { api } from "@/lib/api";
import { useToast } from "@/components/ui/Toast";
import type { CashflowRow, ProfitCashRow } from "@/types/api";

function todayISO() {
  const d = new Date();
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

function addDaysISO(iso: string, days: number) {
  const d = new Date(`${iso}T00:00:00`);
  d.setDate(d.getDate() + days);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

type BalanceRow = { account_id: string; account_name: string; balance: number };

export default function AnalyticsPage() {
  const toast = useToast();

  const [dateFrom, setDateFrom] = useState<string>(addDaysISO(todayISO(), -30));
  const [dateTo, setDateTo] = useState<string>(todayISO());
  const [loading, setLoading] = useState(false);

  const [cashflow, setCashflow] = useState<CashflowRow[]>([]);
  const [profit, setProfit] = useState<ProfitCashRow[]>([]);
  const [balances, setBalances] = useState<BalanceRow[]>([]);
  const [unallocated, setUnallocated] = useState<any[]>([]);

  async function load() {
    setLoading(true);
    try {
      const [cf, pr, bal, unal] = await Promise.all([
        api<CashflowRow[]>(`/reports/cashflow?date_from=${dateFrom}&date_to=${dateTo}`),
        api<ProfitCashRow[]>(`/reports/profit-cash?date_from=${dateFrom}&date_to=${dateTo}`),
        api<BalanceRow[]>("/reports/cash-balance"),
        api<any[]>("/reports/unallocated"),
      ]);
      setCashflow(cf || []);
      setProfit(pr || []);
      setBalances(bal || []);
      setUnallocated(unal || []);
    } catch (e: any) {
      toast.push(e.message || "Ошибка загрузки отчётов", "err");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const kpis = useMemo(() => {
    const inflow = (cashflow || []).reduce((s, r: any) => s + (Number(r.inflow) || 0), 0);
    const outflow = (cashflow || []).reduce((s, r: any) => s + (Number(r.outflow) || 0), 0);
    const income = (profit || []).reduce((s, r: any) => s + (Number(r.income) || 0), 0);
    const expense = (profit || []).reduce((s, r: any) => s + (Number(r.expense) || 0), 0);
    const profitSum = income - expense;

    const cashBalance = (balances || []).reduce((s, r) => s + (Number(r.balance) || 0), 0);
    const unallocatedCount = (unallocated || []).length;
    const unallocatedSum = (unallocated || []).reduce((s, r: any) => s + (Number(r.unallocated) || 0), 0);

    return { inflow, outflow, profitSum, cashBalance, unallocatedCount, unallocatedSum };
  }, [cashflow, profit, balances, unallocated]);

  const cashflowColumns: Column<any>[] = [
    { key: "date", header: "Дата" },
    { key: "inflow", header: "Приход", align: "right" },
    { key: "outflow", header: "Расход", align: "right" },
  ];

  const profitColumns: Column<any>[] = [
    { key: "date", header: "Дата" },
    { key: "income", header: "Доход (по разнесению)", align: "right" },
    { key: "expense", header: "Расход (по разнесению)", align: "right" },
    { key: "profit", header: "Прибыль", align: "right" },
  ];

  const cashflowRows = (cashflow || []).map((r: any) => ({
    date: String(r.date),
    inflow: Number(r.inflow).toFixed(2),
    outflow: Number(r.outflow).toFixed(2),
  }));

  const profitRows = (profit || []).map((r: any) => ({
    date: String(r.date),
    income: Number(r.income).toFixed(2),
    expense: Number(r.expense).toFixed(2),
    profit: Number(r.profit).toFixed(2),
  }));

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">Аналитика</div>
          <div className="mt-1 text-sm text-slate-500">
            Реальные деньги (банк/касса) + прибыль только по <b>подтверждённым</b> распределениям. Никаких “примерно”.
          </div>
        </div>

        <div className="flex flex-wrap items-end gap-2">
          <Input label="С" type="date" value={dateFrom} onChange={setDateFrom} />
          <Input label="По" type="date" value={dateTo} onChange={setDateTo} />
          <Button variant="secondary" disabled={loading} onClick={load}>
            {loading ? "Загрузка..." : "Обновить"}
          </Button>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
        <KpiCard title="Остаток денег (все счета)" value={`${kpis.cashBalance.toFixed(2)} ₽`} />
        <KpiCard title="Денежный поток" value={`${(kpis.inflow - kpis.outflow).toFixed(2)} ₽`} delta={`+${kpis.inflow.toFixed(0)} / -${kpis.outflow.toFixed(0)}`} />
        <KpiCard title="Прибыль (по разнесению)" value={`${kpis.profitSum.toFixed(2)} ₽`} />
        <KpiCard title="Неразнесено" value={`${kpis.unallocatedCount}`} delta={`${kpis.unallocatedSum.toFixed(0)} ₽`} />
      </div>

      <div className="rounded-xl border border-slate-200 bg-white p-4">
        <div className="text-sm font-semibold">Что важно помнить</div>
        <div className="mt-2 text-sm text-slate-700">
          <ul className="list-disc space-y-1 pl-5">
            <li>Кэшфлоу берётся из Money Ledger (факты: банк/касса/выплаты).</li>
            <li>Прибыль берётся только из подтверждённых распределений (Allocation.confirmed=true).</li>
            <li>Если “Неразнесено” &gt; 0 — прибыль будет неполной, пока не разнесёшь операции.</li>
          </ul>
        </div>
      </div>

      <DataTable
        title="Кэшфлоу по дням (факты Money Ledger)"
        rows={cashflowRows as any}
        columns={cashflowColumns as any}
        pageSize={12}
        onRefresh={load}
      />

      <DataTable
        title="Прибыль по дням (только подтверждённые распределения)"
        rows={profitRows as any}
        columns={profitColumns as any}
        pageSize={12}
        onRefresh={load}
      />

      <div className="rounded-xl border border-blue-100 bg-blue-50 p-4 text-sm text-slate-700">
        Следующий шаг для “реальной эффективности по каналам”: добавить слой <b>начислений</b> (Ozon/WB/ЯМ) и связать выплаты с банком через Allocation.
      </div>
    </div>
  );
}
