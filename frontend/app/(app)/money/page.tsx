"use client";

import { useEffect, useMemo, useState } from "react";
import DataTable, { Column } from "@/components/table/DataTable";
import KpiCard from "@/components/kpi/KpiCard";
import Modal from "@/components/ui/Modal";
import Input from "@/components/ui/Input";
import Select from "@/components/ui/Select";
import Button from "@/components/ui/Button";
import { useToast } from "@/components/ui/Toast";
import { api } from "@/lib/api";
import type { MoneyAccount, MoneyOperation, Category, MoneyAllocation, BankImportResult, MoneyAutoAllocateResult, MoneyConfirmBatchResult, MoneyRule, CashPlanItem, CashForecastRow } from "@/types/api";

type BalanceRow = { account_id: string; account_name: string; balance: number };

type UnallocatedRow = { id: string; posted_at: string; amount: number; account_id: string; required: number; confirmed: number; unallocated: number };

function todayISO() {
  const d = new Date();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${d.getFullYear()}-${mm}-${dd}`;
}

function parseISODate(iso: string) {
  return new Date(iso + "T00:00:00");
}

function jsDowMon0(d: Date) {
  return (d.getDay() + 6) % 7;
}

function lastDayOfMonth(year: number, month1to12: number) {
  return new Date(year, month1to12, 0).getDate();
}

function planOccursOn(item: CashPlanItem, dateIso: string) {
  if (!item.is_active) return false;

  // Lexicographic compare works for YYYY-MM-DD
  if (item.start_date && dateIso < item.start_date) return false;
  if (item.end_date && dateIso > item.end_date) return false;

  if (item.schedule === "once") {
    return !!item.due_date && item.due_date === dateIso;
  }

  const d = parseISODate(dateIso);

  if (item.schedule === "weekly") {
    const target = Number(item.weekday ?? 0);
    return jsDowMon0(d) === target;
  }

  if (item.schedule === "monthly") {
    const dom = Number(item.day_of_month ?? 1);
    const last = lastDayOfMonth(d.getFullYear(), d.getMonth() + 1);
    const eff = Math.min(dom, last);
    return d.getDate() === eff;
  }

  return false;
}

export default function MoneyPage() {
  const toast = useToast();
  const [accounts, setAccounts] = useState<MoneyAccount[]>([]);
  const [categories, setCategories] = useState<Category[]>([]);
  const [balances, setBalances] = useState<BalanceRow[]>([]);
  const [operations, setOperations] = useState<MoneyOperation[]>([]);
  const [unallocated, setUnallocated] = useState<UnallocatedRow[]>([]);
  const [loading, setLoading] = useState(false);

  const [onlyUnallocated, setOnlyUnallocated] = useState(false);
  const [rules, setRules] = useState<MoneyRule[]>([]);

  // Treasury plan + forecast
  const [planItems, setPlanItems] = useState<CashPlanItem[]>([]);
  const [forecast, setForecast] = useState<CashForecastRow[]>([]);
  const [forecastDays, setForecastDays] = useState<string>("30");
  const [loadingForecast, setLoadingForecast] = useState(false);

  // Forecast day explain modal
  const [openForecastDay, setOpenForecastDay] = useState(false);
  const [forecastDay, setForecastDay] = useState<string>(todayISO());

  // Plan item modal
  const [openPlan, setOpenPlan] = useState(false);
  const [savingPlan, setSavingPlan] = useState(false);
  const [planName, setPlanName] = useState("");
  const [planDirection, setPlanDirection] = useState<string>("out");
  const [planAmount, setPlanAmount] = useState("");
  const [planSchedule, setPlanSchedule] = useState<string>("monthly");
  const [planDueDate, setPlanDueDate] = useState(todayISO());
  const [planDayOfMonth, setPlanDayOfMonth] = useState("10");
  const [planWeekday, setPlanWeekday] = useState("0");
  const [planCategory, setPlanCategory] = useState<string>("");
  const [planAccount, setPlanAccount] = useState<string>("");

  // Auto allocation controls
  const [autoAllocating, setAutoAllocating] = useState(false);
  const [confirmingBatch, setConfirmingBatch] = useState(false);
  const [batchMinConfidence, setBatchMinConfidence] = useState<string>("0.95");

  // Create account modal
  const [openAcc, setOpenAcc] = useState(false);
  const [savingAcc, setSavingAcc] = useState(false);
  const [accType, setAccType] = useState<string>("bank");
  const [accName, setAccName] = useState<string>("");
  const [accCurrency, setAccCurrency] = useState<string>("RUB");
  const [accExternalRef, setAccExternalRef] = useState<string>("");

  // Create operation modal
  const [openOp, setOpenOp] = useState(false);
  const [savingOp, setSavingOp] = useState(false);
  const [opDate, setOpDate] = useState(todayISO());
  const [opAccount, setOpAccount] = useState<string>("");
  const [opAmount, setOpAmount] = useState<string>("");
  const [opCounterparty, setOpCounterparty] = useState<string>("");
  const [opDesc, setOpDesc] = useState<string>("");
  const [opSource, setOpSource] = useState<string>("cash_manual");
  const [opType, setOpType] = useState<string>("payment");

  // Allocation modal
  const [openAlloc, setOpenAlloc] = useState(false);
  const [savingAlloc, setSavingAlloc] = useState(false);
  const [allocOpId, setAllocOpId] = useState<string>("");
  const [allocCatId, setAllocCatId] = useState<string>("");
  const [allocAmount, setAllocAmount] = useState<string>("");
  const [allocConfirmed, setAllocConfirmed] = useState<boolean>(true);

  // Rules modal
  const [openRule, setOpenRule] = useState(false);
  const [savingRule, setSavingRule] = useState(false);
  const [ruleEditingId, setRuleEditingId] = useState<string | null>(null);
  const [ruleName, setRuleName] = useState<string>("");
  const [ruleMatchField, setRuleMatchField] = useState<string>("text");
  const [ruleDirection, setRuleDirection] = useState<string>("any");
  const [rulePattern, setRulePattern] = useState<string>("");
  const [ruleCategory, setRuleCategory] = useState<string>("");
  const [ruleConfidence, setRuleConfidence] = useState<string>("0.90");
  const [rulePriority, setRulePriority] = useState<string>("100");


  // Bank import modal
  const [openImport, setOpenImport] = useState(false);
  const [importing, setImporting] = useState(false);
  const [importAccount, setImportAccount] = useState<string>("");
  const [importFile, setImportFile] = useState<File | null>(null);
  const [importResult, setImportResult] = useState<BankImportResult | null>(null);

  async function loadAll() {
    setLoading(true);
    try {
      // Ensure base refs exist (helps if user has old docker volume)
      try {
        await api<{ status: string }>("/money/bootstrap", { method: "POST", body: {} });
      } catch {
        // ignore bootstrap errors; main fetch will show real error if any
      }

      const opsPath = onlyUnallocated ? "/money/operations?unallocated=true" : "/money/operations";

      const [acc, cats, bals, ops, rulesRes, planRes, forecastRes, unallocRes] = await Promise.all([
        api<MoneyAccount[]>("/money/accounts"),
        api<Category[]>("/money/categories"),
        api<BalanceRow[]>("/reports/cash-balance"),
        api<MoneyOperation[]>(opsPath),
        api<MoneyRule[]>("/money/rules?active_only=false"),
        api<CashPlanItem[]>("/treasury/plan-items?active_only=false"),
        api<CashForecastRow[]>(`/treasury/forecast?days=${Number(forecastDays) || 30}`),
        api<UnallocatedRow[]>("/reports/unallocated"),
      ]);
      setAccounts(acc);
      setCategories(cats);
      setBalances(bals);
      setOperations(ops);
      setUnallocated(unallocRes || []);
      setRules(rulesRes);
      setPlanItems(planRes);
      setForecast(forecastRes);
      if (!opAccount && acc.length) setOpAccount(acc[0].id);
      if (!importAccount && acc.length) {
        const bank = acc.find((a) => a.type === 'bank') || acc[0];
        if (bank) setImportAccount(bank.id);
      }
      if (!planAccount && acc.length) setPlanAccount(acc[0].id);
      if (!planCategory) {
        const exp = cats.find((c) => c.type === "expense") || cats[0];
        if (exp) setPlanCategory(exp.id);
      }
      if (!allocCatId) {
        const firstExpense = cats.find((c) => c.type === "expense") || cats[0];
        if (firstExpense) setAllocCatId(firstExpense.id);
      }
    } catch (e: any) {
      toast.push(e.message || "Ошибка загрузки", "err");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadAll();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [onlyUnallocated]);

  const totalBalance = useMemo(
    () => balances.reduce((s, r) => s + (Number(r.balance) || 0), 0),
    [balances]
  );

  const unallocatedStats = useMemo(() => {
    const count = unallocated.length;
    const sum = unallocated.reduce((s, r) => s + (Number((r as any).unallocated) || 0), 0);
    return { count, sum };
  }, [unallocated]);

  const next7Stats = useMemo(() => {
    const rows = (forecast || []).slice(0, 7);
    const inflow = rows.reduce((s, r) => s + (Number((r as any).planned_in) || 0), 0);
    const outflow = rows.reduce((s, r) => s + (Number((r as any).planned_out) || 0), 0);
    return { inflow, outflow, net: inflow - outflow };
  }, [forecast]);

  const riskInfo = useMemo(() => {
    const row = (forecast || []).find((r) => Number((r as any).balance) < 0);
    if (!row) return null;
    const d0 = new Date(todayISO() + "T00:00:00");
    const dr = new Date(String((row as any).date) + "T00:00:00");
    const days = Math.round((dr.getTime() - d0.getTime()) / 86400000);
    return { date: String((row as any).date), days };
  }, [forecast]);

  const forecastDayItems = useMemo(() => {
    return (planItems || []).filter((it) => planOccursOn(it, forecastDay));
  }, [planItems, forecastDay]);

  const accountOptions = useMemo(
    () => accounts.map((a) => ({ value: a.id, label: `${a.name} (${a.type})` })),
    [accounts]
  );

  const categoryOptions = useMemo(
    () => categories.map((c) => ({ value: c.id, label: `${c.type}: ${c.name}` })),
    [categories]
  );

  const opColumns: Column<MoneyOperation>[] = useMemo(
    () => [
      { key: "posted_at", title: "Дата" },
      { key: "amount", title: "Сумма" },
      { key: "counterparty", title: "Контрагент" },
      { key: "description", title: "Назначение" },
      { key: "source", title: "Источник" },
      { key: "operation_type", title: "Тип" },
    ],
    []
  );

  const ruleColumns: Column<MoneyRule>[] = useMemo(
    () => [
      {
        key: "is_active",
        title: "Активно",
        render: (r) => (
          <input
            type="checkbox"
            checked={Boolean(r.is_active)}
            onChange={(e) => toggleRule(r.id, e.target.checked)}
          />
        ),
      },
      { key: "priority", title: "Приоритет" },
      {
        key: "direction",
        title: "Направление",
        render: (r) => (r.direction === "in" ? "Приход" : r.direction === "out" ? "Расход" : "Любое"),
      },
      {
        key: "match_field",
        title: "Поле",
        render: (r) =>
          r.match_field === "counterparty"
            ? "Контрагент"
            : r.match_field === "description"
              ? "Назначение"
              : r.match_field === "source"
                ? "Источник"
                : "Текст",
      },
      { key: "pattern", title: "Шаблон" },
      {
        key: "category_id",
        title: "Категория",
        render: (r) => categories.find((c) => c.id === r.category_id)?.name ?? r.category_id,
      },
      {
        key: "confidence",
        title: "Увер.",
        render: (r) => Number(r.confidence).toFixed(2),
      },
    ],
    [categories]
  );
  const planColumns: Column<CashPlanItem>[] = useMemo(
    () => [
      {
        key: "is_active",
        title: "Активно",
        render: (r) => (
          <input
            type="checkbox"
            checked={Boolean(r.is_active)}
            onChange={(e) => togglePlan(r.id, e.target.checked)}
          />
        ),
      },
      { key: "name", title: "Название" },
      {
        key: "schedule",
        title: "График",
        render: (r) =>
          r.schedule === "once"
            ? `Разово: ${String(r.due_date || "")}`
            : r.schedule === "weekly"
              ? `Еженед.: ${["Пн","Вт","Ср","Чт","Пт","Сб","Вс"][Number(r.weekday ?? 0)]}`
              : `Ежемес.: ${String(r.day_of_month ?? 10)}`,
      },
      {
        key: "direction",
        title: "Напр.",
        render: (r) => (r.direction === "in" ? "Приход" : "Расход"),
      },
      {
        key: "amount",
        title: "Сумма",
        render: (r) => `${Number(r.amount).toFixed(2)} ₽`,
      },
      {
        key: "account_id",
        title: "Счёт",
        render: (r) => accounts.find((a) => a.id === r.account_id)?.name ?? (r.account_id ? String(r.account_id) : "—"),
      },
      {
        key: "category_id",
        title: "Категория",
        render: (r) => categories.find((c) => c.id === r.category_id)?.name ?? (r.category_id ? String(r.category_id) : "—"),
      },
      {
        key: "note",
        title: "Комментарий",
        render: (r) => String(r.note || ""),
      },
    ],
    [accounts, categories]
  );



  const opsPretty = useMemo(() => {
    const accMap = new Map(accounts.map((a) => [a.id, a]));
    return operations.map((o) => ({
      ...o,
      posted_at: new Date(o.posted_at).toLocaleString(),
      amount: Number(o.amount).toFixed(2),
      source: o.source,
      operation_type: o.operation_type,
      counterparty: o.counterparty || "",
      description: o.description || "",
      // @ts-ignore
      _accountName: accMap.get(o.account_id)?.name || o.account_id,
    }));
  }, [operations, accounts]);

  async function createOperation() {
    const a = Number(opAmount);
    if (!opAccount) return toast.push("Выбери счёт", "err");
    if (Number.isNaN(a) || a === 0) return toast.push("Сумма не может быть 0", "err");
    setSavingOp(true);
    try {
      await api("/money/operations", {
        method: "POST",
        body: {
          account_id: opAccount,
          posted_at: new Date(`${opDate}T12:00:00`).toISOString(),
          amount: a,
          counterparty: opCounterparty.trim() || null,
          description: opDesc.trim() || null,
          source: opSource,
          operation_type: opType,
        },
      });
      toast.push("Операция добавлена", "ok");
      setOpenOp(false);
      setOpAmount("");
      setOpCounterparty("");
      setOpDesc("");
      await loadAll();
    } catch (e: any) {
      toast.push(e.message || "Ошибка создания", "err");
    } finally {
      setSavingOp(false);
    }
  }

  function openAllocateFor(opId: string, amount: number) {
    setAllocOpId(opId);
    setAllocAmount(String(amount));
    setOpenAlloc(true);
  }

  function openRuleModal(prefill?: Partial<{
    id: string;
    name: string;
    match_field: string;
    direction: string;
    pattern: string;
    category_id: string;
    confidence: string;
    priority: string;
  }>) {
    setRuleEditingId(prefill?.id ?? null);
    setRuleName(prefill?.name ?? "");
    setRuleMatchField(prefill?.match_field ?? "text");
    setRuleDirection(prefill?.direction ?? "any");
    setRulePattern(prefill?.pattern ?? "");
    setRuleCategory(prefill?.category_id ?? (categories[0]?.id ?? ""));
    setRuleConfidence(prefill?.confidence ?? "0.90");
    setRulePriority(prefill?.priority ?? "100");
    setOpenRule(true);
  }

  function openRuleFromOp(op: MoneyOperation) {
    const pat = (op.counterparty || "").trim() || (op.description || "").trim();
    const dir = Number(op.amount) > 0 ? "in" : Number(op.amount) < 0 ? "out" : "any";
    openRuleModal({
      name: "",
      match_field: "text",
      direction: dir,
      pattern: pat,
      category_id: categories[0]?.id ?? "",
      confidence: dir === "in" ? "0.70" : "0.90",
      priority: "200",
    });
  }

  async function saveRule() {
    if (!rulePattern.trim()) return toast.push("Шаблон обязателен", "err");
    if (!ruleCategory) return toast.push("Выбери категорию", "err");
    const conf = Number(ruleConfidence);
    if (Number.isNaN(conf) || conf <= 0 || conf > 1) return toast.push("Уверенность 0..1", "err");
    const pr = Number(rulePriority);
    if (Number.isNaN(pr)) return toast.push("Приоритет — число", "err");

    setSavingRule(true);
    try {
      const body: any = {
        name: ruleName.trim() || null,
        match_field: ruleMatchField,
        direction: ruleDirection,
        pattern: rulePattern.trim(),
        category_id: ruleCategory,
        confidence: conf,
        priority: pr,
        is_active: true,
      };
      if (ruleEditingId) {
        await api<MoneyRule>(`/money/rules/${ruleEditingId}`, { method: "PATCH", body });
      } else {
        await api<MoneyRule>("/money/rules", { method: "POST", body });
      }
      toast.push("Правило сохранено", "ok");
      setOpenRule(false);
      await loadAll();
    } catch (e: any) {
      toast.push(e.message || "Ошибка правила", "err");
    } finally {
      setSavingRule(false);
    }
  }

  async function toggleRule(ruleId: string, isActive: boolean) {
    try {
      await api<MoneyRule>(`/money/rules/${ruleId}`, {
        method: "PATCH",
        body: { is_active: isActive },
      });
      await loadAll();
    } catch (e: any) {
      toast.push(e.message || "Ошибка", "err");
    }
  }

  async function deactivateRule(ruleId: string) {
    try {
      await api<any>(`/money/rules/${ruleId}`, { method: "DELETE" });
      await loadAll();
    } catch (e: any) {
      toast.push(e.message || "Ошибка", "err");
    }
  }


  async function importBankCsv() {
    if (!importAccount) return toast.push('Выбери счёт (банк)', 'err');
    if (!importFile) return toast.push('Выбери файл выписки (CSV)', 'err');
    setImporting(true);
    setImportResult(null);
    try {
      const fd = new FormData();
      fd.append('file', importFile);
      const res = await api<BankImportResult>(`${importFile?.name?.toLowerCase().endsWith('.xlsx') ? '/imports/bank/xlsx' : '/imports/bank/csv'}?account_id=${encodeURIComponent(importAccount)}`, {
        method: 'POST',
        body: fd,
      });
      setImportResult(res);
      toast.push(`Импорт: +${res.imported}, дублей пропущено: ${res.skipped_duplicates}`, res.errors?.length ? 'err' : 'ok');
      await loadAll();
    } catch (e: any) {
      toast.push(e.message || 'Ошибка импорта', 'err');
    } finally {
      setImporting(false);
    }
  }

  async function createAllocation() {
    const a = Number(allocAmount);
    if (!allocOpId) return toast.push("Выбери операцию", "err");
    if (!allocCatId) return toast.push("Выбери категорию", "err");
    if (Number.isNaN(a) || a === 0) return toast.push("Сумма не может быть 0", "err");
    setSavingAlloc(true);
    try {
      await api<MoneyAllocation>("/money/allocations", {
        method: "POST",
        body: {
          money_operation_id: allocOpId,
          category_id: allocCatId,
          amount_part: a,
          confirmed: allocConfirmed,
          method: "manual",
        },
      });
      toast.push("Распределение добавлено", "ok");
      setOpenAlloc(false);
      await loadAll();
    } catch (e: any) {
      toast.push(e.message || "Ошибка распределения", "err");
    } finally {
      setSavingAlloc(false);
    }
  }

  async function createAccount() {
    const name = accName.trim();
    if (!name) return toast.push("Название счёта обязательно", "err");
    setSavingAcc(true);
    try {
      await api<MoneyAccount>("/money/accounts", {
        method: "POST",
        body: {
          type: accType,
          name,
          currency: (accCurrency || "RUB").trim().toUpperCase(),
          external_ref: accExternalRef.trim() || null,
        },
      });
      toast.push("Счёт добавлен", "ok");
      setOpenAcc(false);
      setAccName("");
      setAccExternalRef("");
      await loadAll();
    } catch (e: any) {
      toast.push(e.message || "Ошибка создания счёта", "err");
    } finally {
      setSavingAcc(false);
    }
  }

  async function runAutoAllocate() {
    setAutoAllocating(true);
    try {
      const res = await api<MoneyAutoAllocateResult>("/money/auto-allocate", {
        method: "POST",
        body: {
          include_already_allocated: false,
        },
      });
      toast.push(
        `Авторазнесение: подсказок ${res.suggested}, пропуск ${res.skipped}${res.errors?.length ? `, ошибок ${res.errors.length}` : ""}`,
        res.errors?.length ? "err" : "ok"
      );
      await loadAll();
    } catch (e: any) {
      toast.push(e.message || "Ошибка авторазнесения", "err");
    } finally {
      setAutoAllocating(false);
    }
  }

  async function confirmBatch() {
    const minc = Number(batchMinConfidence);
    if (Number.isNaN(minc) || minc <= 0 || minc > 1) return toast.push("min_confidence должен быть от 0 до 1", "err");
    setConfirmingBatch(true);
    try {
      const res = await api<MoneyConfirmBatchResult>("/money/allocations/confirm-batch", {
        method: "POST",
        body: { min_confidence: minc },
      });
      toast.push(
        `Подтверждено: ${res.confirmed} (пропущено ${res.skipped})${res.errors?.length ? `, ошибок ${res.errors.length}` : ""}`,
        res.errors?.length ? "err" : "ok"
      );
      await loadAll();
    } catch (e: any) {
      toast.push(e.message || "Ошибка подтверждения", "err");
    } finally {
      setConfirmingBatch(false);
    }
  }

  function openPlanModal() {
    setPlanName("");
    setPlanDirection("out");
    setPlanAmount("");
    setPlanSchedule("monthly");
    setPlanDueDate(todayISO());
    setPlanDayOfMonth("10");
    setPlanWeekday("0");
    setOpenPlan(true);
  }

  async function savePlanItem() {
    const name = planName.trim();
    if (!name) return toast.push("Название обязательно", "err");
    const amt = Number(planAmount);
    if (Number.isNaN(amt) || amt <= 0) return toast.push("Сумма должна быть > 0", "err");

    const body: any = {
      name,
      direction: planDirection,
      amount: amt,
      currency: "RUB",
      account_id: planAccount || null,
      category_id: planCategory || null,
      schedule: planSchedule,
      is_active: true,
    };
    if (planSchedule === "once") body.due_date = planDueDate;
    if (planSchedule === "monthly") body.day_of_month = Number(planDayOfMonth) || 10;
    if (planSchedule === "weekly") body.weekday = Number(planWeekday) || 0;

    setSavingPlan(true);
    try {
      await api<CashPlanItem>("/treasury/plan-items", { method: "POST", body });
      toast.push("Плановый платёж добавлен", "ok");
      setOpenPlan(false);
      await loadAll();
    } catch (e: any) {
      toast.push(e.message || "Ошибка", "err");
    } finally {
      setSavingPlan(false);
    }
  }

  async function togglePlan(itemId: string, isActive: boolean) {
    try {
      await api<CashPlanItem>(`/treasury/plan-items/${itemId}`, {
        method: "PATCH",
        body: { is_active: isActive },
      });
      await loadAll();
    } catch (e: any) {
      toast.push(e.message || "Ошибка", "err");
    }
  }

  async function deletePlan(itemId: string) {
    try {
      await api<any>(`/treasury/plan-items/${itemId}`, { method: "DELETE" });
      await loadAll();
    } catch (e: any) {
      toast.push(e.message || "Ошибка", "err");
    }
  }

  async function refreshForecast() {
    setLoadingForecast(true);
    try {
      const res = await api<CashForecastRow[]>(`/treasury/forecast?days=${Number(forecastDays) || 30}`);
      setForecast(res);
    } catch (e: any) {
      toast.push(e.message || "Ошибка прогноза", "err");
    } finally {
      setLoadingForecast(false);
    }
  }

  function openForecastDetails(dateIso: string) {
    setForecastDay(dateIso);
    setOpenForecastDay(true);
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">Деньги</div>
          <div className="mt-1 text-sm text-slate-500">
            Неприкосновенный реестр операций денег (факты) + распределение (интерпретация).
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="secondary" disabled={autoAllocating} onClick={runAutoAllocate}>
            {autoAllocating ? "Авторазнесение..." : "Авторазнести"}
          </Button>
          <div className="hidden items-center gap-2 md:flex">
            <div className="text-xs text-slate-500">min_conf</div>
            <select
              value={batchMinConfidence}
              onChange={(e) => setBatchMinConfidence(e.target.value)}
              className="h-9 rounded-md border border-slate-200 bg-slate-50 px-2 text-sm"
              title="Пачкой подтверждаем только высокую уверенность (например 0.95)."
            >
              <option value="0.99">0.99</option>
              <option value="0.97">0.97</option>
              <option value="0.95">0.95</option>
              <option value="0.90">0.90</option>
            </select>
          </div>
          <Button variant="secondary" disabled={confirmingBatch} onClick={confirmBatch}>
            {confirmingBatch ? "Подтверждение..." : "Подтвердить пачкой"}
          </Button>
          <Button variant="secondary" onClick={() => setOpenAlloc(true)}>
            Распределить
          </Button>
          <Button variant="secondary" onClick={() => { setOpenImport(true); setImportResult(null); }}>Импорт выписки</Button>
          <Button variant="secondary" onClick={() => setOpenAcc(true)}>Добавить счёт</Button>
          <Button onClick={() => setOpenOp(true)}>Добавить операцию</Button>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
        <KpiCard title="Остаток денег (все счета)" value={`${totalBalance.toFixed(2)} ₽`} />
        <KpiCard title="Неразнесено" value={`${unallocatedStats.count}`} delta={`${unallocatedStats.sum.toFixed(0)} ₽`} />
        <KpiCard title="План на 7 дней" value={`${next7Stats.net.toFixed(0)} ₽`} delta={`+${next7Stats.inflow.toFixed(0)} / -${next7Stats.outflow.toFixed(0)}`} />
        <KpiCard
          title="Риск кассового разрыва"
          value={riskInfo ? riskInfo.date : "Нет"}
          delta={riskInfo ? `${riskInfo.days} дн.` : undefined}
        />
      </div>

      <div className="rounded-xl border border-slate-200 bg-white p-4">
        <div className="text-sm font-semibold">Быстрые действия</div>
        <div className="mt-3 flex flex-wrap gap-2">
          <Button variant="secondary" onClick={() => setOnlyUnallocated(true)} title="Показывает только операции, которые ещё не распределены полностью.">
            Показать неразнесённые ({unallocatedStats.count})
          </Button>
          <Button variant="secondary" disabled={autoAllocating} onClick={runAutoAllocate}>
            {autoAllocating ? "Авторазнесение..." : "Авторазнести"}
          </Button>
          <Button variant="secondary" disabled={confirmingBatch} onClick={confirmBatch}>
            {confirmingBatch ? "Подтверждение..." : "Подтвердить пачкой"}
          </Button>
          <Button variant="secondary" onClick={() => setOpenPlan(true)}>Добавить план-платёж</Button>
        </div>
        <div className="mt-2 text-xs text-slate-500">
          Прогноз и прибыль становятся точными, когда <b>неразнесённых</b> операций = 0 и распределения подтверждены.
        </div>
      </div>

      <div className="rounded-xl border border-blue-100 bg-blue-50 p-4 text-sm text-slate-700">
        <div className="font-semibold">Как это работает (коротко)</div>
        <div className="mt-1">
          <b>Авторазнести</b> — создаёт <u>неподтверждённые</u> подсказки (allocation, confirmed=false) по очевидным правилам (налоги/аренда/комиссии).
          <br />
          <b>Подтвердить пачкой</b> — подтверждает только подсказки с высокой уверенностью (min_conf), и только если они полностью закрывают сумму операции.
        </div>
      </div>

      <div className="rounded-xl border border-slate-200 bg-white p-4">
        <div className="mb-3 text-sm font-semibold">Остатки по счетам</div>
        <div className="grid grid-cols-1 gap-2 md:grid-cols-3">
          {balances.map((b) => (
            <div key={b.account_id} className="rounded-lg border border-slate-200 p-3">
              <div className="text-sm font-medium">{b.account_name}</div>
              <div className="mt-1 text-lg font-semibold">{Number(b.balance).toFixed(2)} ₽</div>
            </div>
          ))}
          {!balances.length && (
            <div className="text-sm text-slate-500">Нет данных (добавь счета/операции)</div>
          )}
        </div>
      </div>

      <DataTable
        title="Операции (Money Ledger)"
        loading={loading}
        rows={opsPretty as any}
        columns={opColumns as any}
        rowKey={(r: any) => String(r.id)}
        pageSize={12}
        onRefresh={loadAll}
        actions={
          <>
            <label className="flex items-center gap-2 rounded-md border border-slate-200 bg-slate-50 px-2 py-1 text-xs text-slate-700">
              <input
                type="checkbox"
                checked={onlyUnallocated}
                onChange={(e) => setOnlyUnallocated(e.target.checked)}
              />
              Только неразнесённые
            </label>
            <Button variant="secondary" onClick={() => openRuleModal({})}>
              + Правило
            </Button>
          </>
        }
        rowActions={(row: any) => (
          <>
            <Button
              variant="secondary"
              onClick={() => {
                const amt = Math.abs(Number(row.amount) || 0);
                openAllocateFor(String(row.id), amt);
              }}
            >
              Распределить
            </Button>
            <Button variant="secondary" onClick={() => openRuleFromOp(row as any)}>
              В правило
            </Button>
          </>
        )}
      />

      <div className="h-6" />

      <DataTable
        title="Правила авторазнесения"
        loading={loading}
        rows={rules as any}
        columns={ruleColumns as any}
        rowKey={(r: any) => String(r.id)}
        pageSize={10}
        onRefresh={loadAll}
        onAdd={() => openRuleModal({})}
        rowActions={(r: any) => (
          <>
            <Button
              variant="secondary"
              onClick={() =>
                openRuleModal({
                  id: String(r.id),
                  name: String(r.name ?? ""),
                  match_field: String(r.match_field),
                  direction: String(r.direction),
                  pattern: String(r.pattern),
                  category_id: String(r.category_id),
                  confidence: String(r.confidence),
                  priority: String(r.priority),
                })
              }
            >
              Изменить
            </Button>
            <Button variant="danger" onClick={() => deactivateRule(String(r.id))}>
              Отключить
            </Button>
          </>
        )}
      />


      <div className="h-6" />

      <DataTable
        title="План платежей (управленческое)"
        loading={loading}
        rows={planItems as any}
        columns={planColumns as any}
        rowKey={(r: any) => String(r.id)}
        pageSize={10}
        onRefresh={loadAll}
        onAdd={openPlanModal}
        rowActions={(r: any) => (
          <>
            <Button variant="secondary" onClick={() => togglePlan(String(r.id), !Boolean(r.is_active))}>
              {Boolean(r.is_active) ? "Выключить" : "Включить"}
            </Button>
            <Button variant="danger" onClick={() => deletePlan(String(r.id))}>Удалить</Button>
          </>
        )}
      />

      <div className="h-6" />

      <section className="overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm">
        <div className="h-1 w-full bg-blue-900" />
        <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-200 p-4">
          <div>
            <div className="text-sm font-semibold text-slate-900">Прогноз денег (план)</div>
            <div className="mt-1 text-xs text-slate-500">Старт от остатков на начало дня + плановые платежи (это не факты банка).</div>
          </div>
          <div className="flex items-center gap-2">
            <div className="text-xs text-slate-500">Дней</div>
            <input
              value={forecastDays}
              onChange={(e) => setForecastDays(e.target.value)}
              className="h-9 w-20 rounded-md border border-slate-200 bg-slate-50 px-2 text-sm"
            />
            <Button variant="secondary" disabled={loadingForecast} onClick={refreshForecast}>
              {loadingForecast ? "Считаю..." : "Обновить"}
            </Button>
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="min-w-full border-separate border-spacing-0">
            <thead>
              <tr className="bg-slate-50">
                <th className="border-b border-slate-200 px-4 py-3 text-left text-xs font-semibold text-slate-600">Дата</th>
                <th className="border-b border-slate-200 px-4 py-3 text-right text-xs font-semibold text-slate-600">Приход</th>
                <th className="border-b border-slate-200 px-4 py-3 text-right text-xs font-semibold text-slate-600">Расход</th>
                <th className="border-b border-slate-200 px-4 py-3 text-right text-xs font-semibold text-slate-600">Итог</th>
                <th className="border-b border-slate-200 px-4 py-3 text-right text-xs font-semibold text-slate-600">Баланс</th>
              </tr>
            </thead>
            <tbody>
              {forecast.slice(0, 60).map((r, idx) => (
                <tr key={idx} className="hover:bg-slate-50">
                  <td className="border-b border-slate-100 px-4 py-3 text-sm">
                    <button
                      className="text-blue-900 underline underline-offset-2 hover:text-blue-700"
                      onClick={() => openForecastDetails(String(r.date))}
                      title="Показать, какие плановые события влияют на этот день"
                    >
                      {String(r.date)}
                    </button>
                  </td>
                  <td className="border-b border-slate-100 px-4 py-3 text-right text-sm">{Number(r.planned_in).toFixed(2)}</td>
                  <td className="border-b border-slate-100 px-4 py-3 text-right text-sm">{Number(r.planned_out).toFixed(2)}</td>
                  <td className="border-b border-slate-100 px-4 py-3 text-right text-sm">{Number(r.net).toFixed(2)}</td>
                  <td className={["border-b border-slate-100 px-4 py-3 text-right text-sm font-semibold", Number(r.balance) < 0 ? "text-rose-600" : ""].join(" ")}>{Number(r.balance).toFixed(2)}</td>
                </tr>
              ))}
              {!forecast.length && (
                <tr>
                  <td colSpan={5} className="px-4 py-10 text-center text-sm text-slate-500">Нет данных (добавь плановые платежи и нажми “Обновить”).</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      <Modal
        open={openForecastDay}
        onClose={() => setOpenForecastDay(false)}
        title={`Почему ${forecastDay}`}
      >
        <div className="space-y-3">
          <div className="text-sm text-slate-600">
            Это объяснение строится <b>только</b> из плановых платежей (не из банка). Факты денег — в таблице операций.
          </div>

          <div className="rounded-xl border border-slate-200 bg-white">
            <div className="border-b border-slate-200 px-4 py-3 text-sm font-semibold">События на дату</div>
            <div className="divide-y divide-slate-100">
              {forecastDayItems.map((it) => (
                <div key={it.id} className="flex items-start justify-between gap-3 px-4 py-3">
                  <div>
                    <div className="text-sm font-medium">{it.name}</div>
                    <div className="mt-1 text-xs text-slate-500">
                      {it.schedule === "once" ? "Разово" : it.schedule === "weekly" ? "Еженед." : "Ежемесячно"}
                      {it.note ? ` • ${it.note}` : ""}
                    </div>
                  </div>
                  <div className={["text-sm font-semibold", it.direction === "out" ? "text-rose-600" : "text-emerald-700"].join(" ")}>
                    {it.direction === "out" ? "-" : "+"}{Number(it.amount).toFixed(2)} ₽
                  </div>
                </div>
              ))}
              {!forecastDayItems.length && (
                <div className="px-4 py-8 text-center text-sm text-slate-500">На эту дату плановых событий нет.</div>
              )}
            </div>
          </div>

          <div className="flex justify-end">
            <Button variant="secondary" onClick={() => setOpenPlan(true)}>Добавить/изменить план</Button>
          </div>
        </div>
      </Modal>

      <Modal open={openPlan} onClose={() => setOpenPlan(false)} title="Плановый платёж">
        <div className="space-y-3">
          <Input label="Название" placeholder="Напр. Аренда" value={planName} onChange={setPlanName} />

          <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
            <Select
              label="Направление"
              value={planDirection}
              onChange={setPlanDirection}
              options={[
                { value: "out", label: "Расход" },
                { value: "in", label: "Приход" },
              ]}
            />
            <Input label="Сумма" placeholder="0" value={planAmount} onChange={setPlanAmount} />
          </div>

          <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
            <Select
              label="Счёт (куда/откуда)"
              value={planAccount}
              onChange={setPlanAccount}
              options={accountOptions}
            />
            <Select
              label="Категория (для аналитики)"
              value={planCategory}
              onChange={setPlanCategory}
              options={categoryOptions}
            />
          </div>

          <Select
            label="График"
            value={planSchedule}
            onChange={setPlanSchedule}
            options={[
              { value: "monthly", label: "Ежемесячно" },
              { value: "weekly", label: "Еженедельно" },
              { value: "once", label: "Разово" },
            ]}
          />

          {planSchedule === "once" ? (
            <Input label="Дата" placeholder="YYYY-MM-DD" value={planDueDate} onChange={setPlanDueDate} />
          ) : null}

          {planSchedule === "monthly" ? (
            <Input label="День месяца" placeholder="10" value={planDayOfMonth} onChange={setPlanDayOfMonth} />
          ) : null}

          {planSchedule === "weekly" ? (
            <Select
              label="День недели"
              value={planWeekday}
              onChange={setPlanWeekday}
              options={[
                { value: "0", label: "Понедельник" },
                { value: "1", label: "Вторник" },
                { value: "2", label: "Среда" },
                { value: "3", label: "Четверг" },
                { value: "4", label: "Пятница" },
                { value: "5", label: "Суббота" },
                { value: "6", label: "Воскресенье" },
              ]}
            />
          ) : null}

          <div className="flex items-center justify-end gap-2 pt-2">
            <Button variant="secondary" onClick={() => setOpenPlan(false)}>
              Отмена
            </Button>
            <Button disabled={savingPlan} onClick={savePlanItem}>
              {savingPlan ? "Сохранение..." : "Сохранить"}
            </Button>
          </div>
        </div>
      </Modal>

      <Modal open={openRule} onClose={() => setOpenRule(false)} title={ruleEditingId ? "Правило: изменить" : "Правило: добавить"}>
        <div className="space-y-3">
          <Input label="Название (необязательно)" placeholder="Напр. Реклама (Яндекс)" value={ruleName} onChange={setRuleName} />

          <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
            <Select
              label="Поле поиска"
              value={ruleMatchField}
              onChange={setRuleMatchField}
              options={[
                { value: "text", label: "Текст (контрагент + назначение)" },
                { value: "counterparty", label: "Контрагент" },
                { value: "description", label: "Назначение" },
                { value: "source", label: "Источник" },
              ]}
            />
            <Select
              label="Направление"
              value={ruleDirection}
              onChange={setRuleDirection}
              options={[
                { value: "any", label: "Любое" },
                { value: "in", label: "Приход" },
                { value: "out", label: "Расход" },
              ]}
            />
          </div>

          <Input label="Шаблон (можно через |)" placeholder="yandex|директ|vk" value={rulePattern} onChange={setRulePattern} />

          <Select
            label="Категория"
            value={ruleCategory}
            onChange={setRuleCategory}
            options={categories.map((c) => ({ value: c.id, label: `${c.type}: ${c.name}` }))}
          />

          <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
            <Input label="Уверенность (0..1)" placeholder="0.90" value={ruleConfidence} onChange={setRuleConfidence} />
            <Input label="Приоритет" placeholder="100" value={rulePriority} onChange={setRulePriority} />
          </div>

          <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-xs text-slate-600">
            Правила создают только <b>подсказки</b> (Allocation с confirmed=false). Подтверждение — вручную или пачкой.
          </div>

          <div className="flex items-center justify-end gap-2 pt-2">
            <Button variant="secondary" onClick={() => setOpenRule(false)}>
              Отмена
            </Button>
            <Button disabled={savingRule} onClick={saveRule}>
              {savingRule ? "Сохранение..." : "Сохранить"}
            </Button>
          </div>
        </div>
      </Modal>

      <Modal open={openImport} onClose={() => setOpenImport(false)} title="Импорт выписки банка (CSV/XLSX)">
        <div className="space-y-3">
          <div className="text-sm text-slate-600">
            Загрузи CSV из интернет-банка. Мы попробуем автоматически распознать колонки (дата/сумма/контрагент/назначение).
            Разделитель <span className="font-mono">;</span> или <span className="font-mono">,</span>, кодировка UTF-8/Windows-1251 — ок.
          </div>
          {!accounts.length ? (
            <div className="rounded-lg border border-amber-200 bg-amber-50 p-3 text-sm">
              <div className="font-semibold">Сначала добавь счёт</div>
              <div className="mt-1 text-slate-700">
                Для импорта выписки нужен счёт (банк). Можно завести несколько счетов — по каждому банку/валюте/юридлицу.
              </div>
              <div className="mt-3 flex justify-end">
                <Button onClick={() => { setOpenImport(false); setOpenAcc(true); }}>Добавить счёт</Button>
              </div>
            </div>
          ) : (
            <Select
              label="Счёт (куда загрузить)"
              value={importAccount}
              onChange={(v) => setImportAccount(v)}
              options={accountOptions}
            />
          )}
          <div>
            <div className="mb-1 text-xs font-medium text-slate-600">Файл выписки (CSV или XLSX)</div>
            <input
              type="file"
              accept=".csv,.xlsx,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,text/csv"
              onChange={(e) => setImportFile(e.target.files?.[0] || null)}
              className="block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm"
            />
          </div>

          {importResult && (
            <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-sm">
              <div><b>Импортировано:</b> {importResult.imported}</div>
              <div><b>Дубликаты пропущены:</b> {importResult.skipped_duplicates}</div>
              {!!importResult.errors?.length && (
                <div className="mt-2">
                  <div className="font-semibold text-rose-600">Ошибки:</div>
                  <ul className="list-disc pl-5">
                    {importResult.errors.slice(0, 10).map((x, i) => (
                      <li key={i}>{x}</li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          )}

          <div className="flex items-center justify-end gap-2 pt-2">
            <Button variant="secondary" onClick={() => setOpenImport(false)}>Закрыть</Button>
            <Button disabled={importing || !accounts.length} onClick={importBankCsv}>{importing ? 'Импорт...' : 'Загрузить'}</Button>
          </div>
        </div>
      </Modal>

      <Modal open={openAcc} onClose={() => setOpenAcc(false)} title="Новый счёт">
        <div className="space-y-3">
          <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
            <Select
              label="Тип"
              value={accType}
              onChange={setAccType}
              options={[
                { value: "bank", label: "Банк" },
                { value: "cash", label: "Касса" },
                { value: "marketplace", label: "Маркетплейс" },
                { value: "acquiring", label: "Эквайринг" },
                { value: "other", label: "Другое" },
              ]}
            />
            <Input label="Валюта" placeholder="RUB" value={accCurrency} onChange={setAccCurrency} />
          </div>
          <Input label="Название" placeholder="Сбербанк р/с" value={accName} onChange={setAccName} />
          <Input label="Идентификатор (необязательно)" placeholder="Номер счёта / последние 4 цифры" value={accExternalRef} onChange={setAccExternalRef} />
          <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-xs text-slate-600">
            <div>• Если у тебя <b>2 счёта</b> в одном банке — заводим 2 отдельных счёта.</div>
            <div>• Если <b>2 банка</b> — тоже 2 счёта. Потом при импорте выбираешь куда загрузить выписку.</div>
            <div>• Маркетплейсы (WB/Ozon/ЯМ) — это тоже “счета”: там деньги копятся до выплаты на банк.</div>
          </div>
          <div className="flex items-center justify-end gap-2 pt-2">
            <Button variant="secondary" onClick={() => setOpenAcc(false)}>Отмена</Button>
            <Button disabled={savingAcc} onClick={createAccount}>{savingAcc ? "Сохранение..." : "Создать"}</Button>
          </div>
        </div>
      </Modal>

      <Modal open={openOp} onClose={() => setOpenOp(false)} title="Новая операция денег">
        <div className="space-y-3">
          <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
            <Input label="Дата" type="date" value={opDate} onChange={(v) => setOpDate(v)} />
            <Select label="Счёт" value={opAccount} onChange={(v) => setOpAccount(v)} options={accountOptions} />
          </div>
          <Input label="Сумма (signed)" placeholder="+10000 или -5000" value={opAmount} onChange={(v) => setOpAmount(v)} />
          <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
            <Select
              label="Источник"
              value={opSource}
              onChange={(v) => setOpSource(v)}
              options={[
                { value: "bank_import", label: "Банк (импорт)" },
                { value: "cash_manual", label: "Касса (ручной ввод)" },
                { value: "marketplace_import", label: "Маркетплейс (импорт)" },
                { value: "acquiring_import", label: "Эквайринг (импорт)" },
                { value: "manual_other", label: "Другое (ручной ввод)" },
              ]}
            />
            <Select
              label="Тип"
              value={opType}
              onChange={(v) => setOpType(v)}
              options={[
                { value: "payment", label: "Платёж" },
                { value: "transfer", label: "Перевод" },
                { value: "refund", label: "Возврат" },
                { value: "fee", label: "Комиссия" },
                { value: "payout", label: "Выплата" },
                { value: "other", label: "Другое" },
              ]}
            />
          </div>
          <Input label="Контрагент (как есть)" placeholder="ООО Ромашка" value={opCounterparty} onChange={(v) => setOpCounterparty(v)} />
          <Input label="Назначение (как есть)" placeholder="Оплата по счету..." value={opDesc} onChange={(v) => setOpDesc(v)} />

          <div className="flex items-center justify-end gap-2 pt-2">
            <Button variant="secondary" onClick={() => setOpenOp(false)}>Отмена</Button>
            <Button disabled={savingOp} onClick={createOperation}>{savingOp ? "Сохранение..." : "Создать"}</Button>
          </div>
        </div>
      </Modal>

      <Modal open={openAlloc} onClose={() => setOpenAlloc(false)} title="Распределение (Allocation)">
        <div className="space-y-3">
          <Select
            label="Операция"
            value={allocOpId}
            onChange={(id) => {
              setAllocOpId(id);
              const op = operations.find((x) => x.id === id);
              if (op) setAllocAmount(String(Math.abs(Number(op.amount) || 0)));
            }}
            options={[{ value: "", label: "Выбери операцию" }, ...operations.slice(0, 50).map((o) => ({ value: o.id, label: `${new Date(o.posted_at).toLocaleDateString()} | ${o.amount} | ${o.counterparty || ""}` }))]}
          />
          <Select label="Категория" value={allocCatId} onChange={(v) => setAllocCatId(v)} options={categoryOptions} />
          <Input label="Сумма части (положительная)" placeholder="Например 5000" value={allocAmount} onChange={(v) => setAllocAmount(v)} />
          <Select
            label="Подтверждено"
            value={allocConfirmed ? "1" : "0"}
            onChange={(v) => setAllocConfirmed(v === "1")}
            options={[
              { value: "1", label: "Да (confirmed)" },
              { value: "0", label: "Нет" },
            ]}
          />

          <div className="flex items-center justify-end gap-2 pt-2">
            <Button variant="secondary" onClick={() => setOpenAlloc(false)}>Отмена</Button>
            <Button disabled={savingAlloc} onClick={createAllocation}>{savingAlloc ? "Сохранение..." : "Добавить"}</Button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
