
"use client";

import { useEffect, useMemo, useState } from "react";
import DataTable, { Column } from "@/components/table/DataTable";
import Modal from "@/components/ui/Modal";
import Input from "@/components/ui/Input";
import Select from "@/components/ui/Select";
import Button from "@/components/ui/Button";
import { useToast } from "@/components/ui/Toast";
import { api } from "@/lib/api";
import type { BizOrder } from "@/types/api";

const CHANNELS = [
  "WB",
  "Ozon",
  "Сайт",
  "Онлайн",
  "Авито",
  "Офлайн",
  "Опт",
];

const STATUSES = [
  { value: "OPEN", label: "Открыт" },
  { value: "CLOSED", label: "Закрыт" },
  { value: "VOID", label: "Отменён" },
];

function todayISO() {
  const d = new Date();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${d.getFullYear()}-${mm}-${dd}`;
}

export default function SalesPage() {
  const toast = useToast();
  const [rows, setRows] = useState<BizOrder[]>([]);
  const [loading, setLoading] = useState(false);

  const [open, setOpen] = useState(false);
  const [saving, setSaving] = useState(false);

  const [orderDate, setOrderDate] = useState(todayISO());
  const [channel, setChannel] = useState(CHANNELS[2]);
  const [subchannel, setSubchannel] = useState("");
  const [revenue, setRevenue] = useState("");
  const [comment, setComment] = useState("");

  async function load() {
    setLoading(true);
    try {
      const data = await api<BizOrder[]>("/biz_orders");
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

  const columns: Column<BizOrder>[] = useMemo(() => [
    { key: "id", title: "№" },
    { key: "order_date", title: "Дата" },
    { key: "channel", title: "Канал" },
    { key: "subchannel", title: "Источник" },
    { key: "revenue", title: "Выручка" },
    { key: "status", title: "Статус" },
  ], []);

  async function create() {
    if (!channel) return toast.push("Канал обязателен", "err");
    const rev = revenue ? Number(revenue) : 0;
    if (Number.isNaN(rev) || rev < 0) return toast.push("Выручка должна быть числом", "err");
    setSaving(true);
    try {
      await api("/biz_orders", {
        method: "POST",
        body: {
          order_date: orderDate,
          channel,
          subchannel: subchannel.trim() || null,
          revenue: rev,
          comment: comment.trim() || null,
        },
      });
      setOpen(false);
      setRevenue("");
      setComment("");
      setSubchannel("");
      toast.push("Заказ добавлен", "ok");
      await load();
    } catch (e: any) {
      toast.push(e.message || "Ошибка создания", "err");
    } finally {
      setSaving(false);
    }
  }

  async function setStatus(row: BizOrder, status: string) {
    try {
      await api(`/biz_orders/${row.id}`, { method: "PATCH", body: { status } });
      toast.push("Обновлено", "ok");
      await load();
    } catch (e: any) {
      toast.push(e.message || "Ошибка обновления", "err");
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">Продажи</div>
          <div className="mt-1 text-sm text-slate-500">Ручной ввод заказов по каналам. Себестоимость материалов подтянем на следующем шаге (через производство).</div>
        </div>
        <Button onClick={() => setOpen(true)}>Создать</Button>
      </div>

      <DataTable
        loading={loading}
        rows={rows}
        columns={columns}
        rowKey={(r) => String(r.id)}
        extraRow={(r) => (
          <div className="flex flex-wrap items-center gap-2 py-2">
            <div className="text-sm text-slate-600">Статус:</div>
            {STATUSES.map((s) => (
              <Button
                key={s.value}
                variant={r.status === s.value ? "primary" : "secondary"}
                onClick={() => setStatus(r, s.value)}
              >
                {s.label}
              </Button>
            ))}
            {r.comment ? <div className="ml-2 text-sm text-slate-500">Комментарий: {r.comment}</div> : null}
          </div>
        )}
      />

      <Modal open={open} onClose={() => setOpen(false)} title="Новый заказ (продажа)">
        <div className="space-y-3">
          <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
            <Input label="Дата" type="date" value={orderDate} onChange={(e) => setOrderDate(e.target.value)} />
            <Select label="Канал" value={channel} onChange={(e) => setChannel(e.target.value)} options={CHANNELS.map((x) => ({ value: x, label: x }))} />
          </div>
          <Input label="Источник (опц.)" placeholder="Telegram / WhatsApp / VK / Email ..." value={subchannel} onChange={(e) => setSubchannel(e.target.value)} />
          <Input label="Выручка (₽)" placeholder="Например 4500" value={revenue} onChange={(e) => setRevenue(e.target.value)} />
          <Input label="Комментарий (опц.)" placeholder="Что это за заказ" value={comment} onChange={(e) => setComment(e.target.value)} />

          <div className="flex items-center justify-end gap-2 pt-2">
            <Button variant="secondary" onClick={() => setOpen(false)}>Отмена</Button>
            <Button disabled={saving} onClick={create}>{saving ? "Сохранение..." : "Сохранить"}</Button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
