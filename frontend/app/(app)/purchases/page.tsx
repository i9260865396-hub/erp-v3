"use client";

import { useEffect, useMemo, useState } from "react";
import DataTable, { Column } from "@/components/table/DataTable";
import Modal from "@/components/ui/Modal";
import Input from "@/components/ui/Input";
import Select from "@/components/ui/Select";
import Button from "@/components/ui/Button";
import { useToast } from "@/components/ui/Toast";
import { api } from "@/lib/api";
import type { Material, PurchaseDoc } from "@/types/api";

const PAY_TYPES = [
  { value: "cash", label: "Наличные" },
  { value: "card", label: "Карта" },
  { value: "bank", label: "Б/н" },
];

const VAT_MODES = [
  { value: "no_vat", label: "Без НДС" },
  { value: "vat_included", label: "НДС включён" },
  { value: "vat_on_top", label: "НДС сверху" },
];

function isoToday() {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

type LineDraft = {
  material_id: number | "";
  qty: string;
  uom: string;
  unit_price: string;
  vat_rate: string;
};

export default function PurchasesPage() {
  const toast = useToast();
  const [rows, setRows] = useState<PurchaseDoc[]>([]);
  const [materials, setMaterials] = useState<Material[]>([]);
  const [loading, setLoading] = useState(false);

  // modal state
  const [open, setOpen] = useState(false);
  const [docDate, setDocDate] = useState(isoToday());
  const [supplier, setSupplier] = useState("");
  const [docNo, setDocNo] = useState("");
  const [payType, setPayType] = useState(PAY_TYPES[2].value);
  const [vatMode, setVatMode] = useState(VAT_MODES[0].value);
  const [comment, setComment] = useState("");
  const [lines, setLines] = useState<LineDraft[]>([
    { material_id: "", qty: "", uom: "m2", unit_price: "", vat_rate: "0" },
  ]);

  async function load() {
    setLoading(true);
    try {
      const [p, m] = await Promise.all([
        api<PurchaseDoc[]>("/purchases"),
        api<Material[]>("/materials"),
      ]);
      setRows(p);
      setMaterials(m);
    } catch (e: any) {
      toast.push(e.message || "Ошибка", "err");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function resetForm() {
    setDocDate(isoToday());
    setSupplier("");
    setDocNo("");
    setPayType(PAY_TYPES[2].value);
    setVatMode(VAT_MODES[0].value);
    setComment("");
    setLines([{ material_id: "", qty: "", uom: "m2", unit_price: "", vat_rate: "0" }]);
  }

  const materialOptions = useMemo(
    () => materials.map((m) => ({ value: String(m.id), label: `${m.name} (#${m.id})` })),
    [materials],
  );

  const columns: Column<PurchaseDoc>[] = useMemo(
    () => [
      { key: "id", title: "ID" },
      { key: "doc_date", title: "Дата" },
      { key: "supplier", title: "Поставщик" },
      { key: "doc_no", title: "Док №" },
      { key: "pay_type", title: "Оплата" },
      { key: "vat_mode", title: "НДС" },
      { key: "status", title: "Статус" },
    ],
    [],
  );

  async function create() {
    // минимальная валидация, чтобы кнопка "Создать" не стреляла 400 от API
    const sup = supplier.trim();
    const dn = docNo.trim();
    if (!sup) return toast.push("Поставщик обязателен", "err");
    if (!dn) return toast.push("Номер документа обязателен", "err");

    const cleanLines = lines
      .map((l) => ({
        material_id: l.material_id === "" ? null : Number(l.material_id),
        qty: Number(String(l.qty).replace(",", ".")),
        uom: (l.uom || "m2").trim(),
        unit_price: Number(String(l.unit_price).replace(",", ".")),
        vat_rate: Number(String(l.vat_rate || "0").replace(",", ".")),
      }))
      .filter((l) => l.material_id && isFinite(l.qty) && isFinite(l.unit_price));

    if (cleanLines.length === 0) {
      return toast.push("Добавь хотя бы 1 строку (материал, кол-во, цена)", "err");
    }

    try {
      await api<PurchaseDoc>("/purchases", {
        method: "POST",
        body: JSON.stringify({
          doc_date: docDate,
          supplier: sup,
          doc_no: dn,
          pay_type: payType,
          vat_mode: vatMode,
          comment: comment.trim() || null,
          lines: cleanLines,
        }),
      });
      toast.push("Закупка создана", "ok");
      setOpen(false);
      resetForm();
      load();
    } catch (e: any) {
      toast.push(e.message || "Ошибка создания", "err");
    }
  }

  async function postDoc(doc: PurchaseDoc) {
    try {
      await api(`/purchases/${doc.id}/post`, { method: "POST" });
      toast.push(`Документ #${doc.id} проведён`, "ok");
      load();
    } catch (e: any) {
      toast.push(e.message || "Ошибка проведения", "err");
    }
  }

  async function voidDoc(doc: PurchaseDoc) {
    try {
      await api(`/purchases/${doc.id}/void`, {
        method: "POST",
        body: JSON.stringify({ reason: "manual" }),
      });
      toast.push(`Документ #${doc.id} VOID`, "ok");
      load();
    } catch (e: any) {
      toast.push(e.message || "Ошибка VOID", "err");
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <div className="text-lg font-semibold">Закупки</div>
          <div className="mt-1 text-sm text-slate-500">Документы закупки + проведение в склад FIFO</div>
        </div>
        <Button
          variant="primary"
          onClick={() => {
            resetForm();
            setOpen(true);
          }}
        >
          Создать
        </Button>
      </div>

      <DataTable
        title="Документы закупок"
        columns={columns}
        rows={rows}
        onRefresh={load}
        onExport={() => toast.push("Экспорт: скоро", "ok")}
        rowActions={(r) => (
          <>
            <Button variant="secondary" onClick={() => postDoc(r)} disabled={r.status !== "DRAFT"}>
              Провести
            </Button>
            <Button variant="danger" onClick={() => voidDoc(r)} disabled={r.status !== "DRAFT"}>
              VOID
            </Button>
          </>
        )}
      />

      <Modal
        open={open}
        title="Создать закупку"
        onClose={() => setOpen(false)}
        footer={
          <div className="flex justify-end gap-2">
            <Button variant="secondary" onClick={() => setOpen(false)}>
              Отмена
            </Button>
            <Button variant="primary" onClick={create} disabled={loading}>
              Создать
            </Button>
          </div>
        }
      >
        <div className="space-y-4">
          <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
            <Input label="Дата" value={docDate} onChange={setDocDate} placeholder="YYYY-MM-DD" />
            <Input label="Док №" value={docNo} onChange={setDocNo} placeholder="Напр: 000123" />
            <Input label="Поставщик" value={supplier} onChange={setSupplier} placeholder="ООО Ромашка" />
            <Input label="Комментарий" value={comment} onChange={setComment} placeholder="(необязательно)" />
            <Select label="Оплата" value={payType} onChange={setPayType} options={PAY_TYPES} />
            <Select label="НДС" value={vatMode} onChange={setVatMode} options={VAT_MODES} />
          </div>

          <div className="rounded-xl border border-slate-200">
            <div className="flex items-center justify-between border-b border-slate-200 bg-slate-50 px-4 py-3">
              <div className="text-sm font-semibold">Строки</div>
              <Button
                variant="secondary"
                onClick={() =>
                  setLines((ls) => [...ls, { material_id: "", qty: "", uom: "m2", unit_price: "", vat_rate: "0" }])
                }
              >
                + Строка
              </Button>
            </div>

            <div className="space-y-3 p-4">
              {lines.map((l, idx) => (
                <div key={idx} className="grid grid-cols-1 gap-3 md:grid-cols-6">
                  <Select
                    label={idx === 0 ? "Материал" : undefined}
                    value={l.material_id === "" ? "" : String(l.material_id)}
                    onChange={(v) =>
                      setLines((ls) =>
                        ls.map((x, i) => (i === idx ? { ...x, material_id: v ? Number(v) : "" } : x)),
                      )
                    }
                    options={[{ value: "", label: "— выбрать —" }, ...materialOptions]}
                  />

                  <Input
                    label={idx === 0 ? "Кол-во" : undefined}
                    value={l.qty}
                    onChange={(v) => setLines((ls) => ls.map((x, i) => (i === idx ? { ...x, qty: v } : x)))}
                    placeholder="0"
                  />

                  <Input
                    label={idx === 0 ? "Ед." : undefined}
                    value={l.uom}
                    onChange={(v) => setLines((ls) => ls.map((x, i) => (i === idx ? { ...x, uom: v } : x)))}
                    placeholder="m2"
                  />

                  <Input
                    label={idx === 0 ? "Цена" : undefined}
                    value={l.unit_price}
                    onChange={(v) =>
                      setLines((ls) => ls.map((x, i) => (i === idx ? { ...x, unit_price: v } : x)))
                    }
                    placeholder="0"
                  />

                  <Input
                    label={idx === 0 ? "НДС %" : undefined}
                    value={l.vat_rate}
                    onChange={(v) => setLines((ls) => ls.map((x, i) => (i === idx ? { ...x, vat_rate: v } : x)))}
                    placeholder="0"
                  />

                  <div className="flex items-end justify-end">
                    <Button
                      variant="ghost"
                      onClick={() => setLines((ls) => ls.filter((_, i) => i !== idx))}
                      disabled={lines.length === 1}
                    >
                      Удалить
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </Modal>
    </div>
  );
}
