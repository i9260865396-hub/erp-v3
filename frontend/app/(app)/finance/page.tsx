
"use client";

import { useEffect, useMemo, useState } from "react";
import DataTable, { Column } from "@/components/table/DataTable";
import Modal from "@/components/ui/Modal";
import Input from "@/components/ui/Input";
import Select from "@/components/ui/Select";
import Button from "@/components/ui/Button";
import { useToast } from "@/components/ui/Toast";
import { api } from "@/lib/api";
import type { Expense } from "@/types/api";

const CATS = [
  { value: "rent", label: "Аренда" },
  { value: "ads", label: "Реклама" },
  { value: "service", label: "Сервис/ремонт" },
  { value: "delivery", label: "Доставка/логистика" },
  { value: "tax", label: "Налоги/комиссии" },
  { value: "other", label: "Прочее" },
];

const CHANNELS = ["", "WB", "Ozon", "Сайт", "Онлайн", "Авито", "Офлайн", "Опт"];

function todayISO() {
  const d = new Date();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${d.getFullYear()}-${mm}-${dd}`;
}

export default function FinancePage() {
  const toast = useToast();
  const [rows, setRows] = useState<Expense[]>([]);
  const [loading, setLoading] = useState(false);

  const [open, setOpen] = useState(false);
  const [saving, setSaving] = useState(false);

  const [expDate, setExpDate] = useState(todayISO());
  const [category, setCategory] = useState(CATS[0].value);
  const [amount, setAmount] = useState("");
  const [channel, setChannel] = useState("");
  const [comment, setComment] = useState("");

  async function load() {
    setLoading(true);
    try {
      const data = await api<Expense[]>("/expenses");
      setRows(data);
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

  const columns: Column<Expense>[] = useMemo(() => [
    { key: "id", title: "№" },
    { key: "exp_date", title: "Дата" },
    { key: "category", title: "Категория" },
    { key: "amount", title: "Сумма" },
    { key: "channel", title: "Канал (опц.)" },
    { key: "comment", title: "Комментарий" },
  ], []);

  async function create() {
    const a = Number(amount);
    if (Number.isNaN(a) || a <= 0) return toast.push("Сумма должна быть > 0", "err");
    setSaving(true);
    try {
      await api("/expenses", {
        method: "POST",
        body: {
          exp_date: expDate,
          category,
          amount: a,
          channel: channel || null,
          comment: comment.trim() || null,
        },
      });
      setOpen(false);
      setAmount("");
      setComment("");
      toast.push("Расход добавлен", "ok");
      await load();
    } catch (e: any) {
      toast.push(e.message || "Ошибка создания", "err");
    } finally {
      setSaving(false);
    }
  }

  const catLabel = (v: string) => CATS.find((x) => x.value === v)?.label || v;

  const rowsPretty = useMemo(() => rows.map((r) => ({ ...r, category: catLabel(r.category) })), [rows]);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">Финансы</div>
          <div className="mt-1 text-sm text-slate-500">Операционные расходы (аренда, реклама, сервис...).</div>
        </div>
        <Button onClick={() => setOpen(true)}>Добавить расход</Button>
      </div>

      <DataTable
        loading={loading}
        rows={rowsPretty}
        columns={columns}
        rowKey={(r) => String(r.id)}
      />

      <Modal open={open} onClose={() => setOpen(false)} title="Новый расход">
        <div className="space-y-3">
          <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
            <Input label="Дата" type="date" value={expDate} onChange={(e) => setExpDate(e.target.value)} />
            <Select label="Категория" value={category} onChange={(e) => setCategory(e.target.value)} options={CATS} />
          </div>
          <Input label="Сумма (₽)" placeholder="Например 50000" value={amount} onChange={(e) => setAmount(e.target.value)} />
          <Select
            label="Канал (опц.)"
            value={channel}
            onChange={(e) => setChannel(e.target.value)}
            options={CHANNELS.map((x) => ({ value: x, label: x ? x : "Общий расход" }))}
          />
          <Input label="Комментарий (опц.)" placeholder="Аренда за февраль" value={comment} onChange={(e) => setComment(e.target.value)} />

          <div className="flex items-center justify-end gap-2 pt-2">
            <Button variant="secondary" onClick={() => setOpen(false)}>Отмена</Button>
            <Button disabled={saving} onClick={create}>{saving ? "Сохранение..." : "Сохранить"}</Button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
