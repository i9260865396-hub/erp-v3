"use client";

import { useEffect, useMemo, useState } from "react";
import DataTable, { Column } from "@/components/table/DataTable";
import Modal from "@/components/ui/Modal";
import Input from "@/components/ui/Input";
import Select from "@/components/ui/Select";
import Button from "@/components/ui/Button";
import { useToast } from "@/components/ui/Toast";
import { api } from "@/lib/api";
import type { LotRow, Material, PurchaseDoc, MovementRow, WriteoffCreate } from "@/types/api";

type TabKey = "lots" | "materials" | "purchases" | "movements";

const TABS: { key: TabKey; label: string }[] = [
  { key: "lots", label: "Партии (FIFO)" },
  { key: "materials", label: "Материалы" },
  { key: "purchases", label: "Закупки" },
  { key: "movements", label: "Движения" },
];

// --- Purchases helpers ---
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

const CATEGORIES = [
  { value: "film", label: "Плёнки/самоклейка" },
  { value: "banner", label: "Баннер/ткань" },
  { value: "paper", label: "Бумага/картон" },
  { value: "ink", label: "Чернила" },
  { value: "chem", label: "Клей/химия" },
  { value: "packaging", label: "Упаковка" },
  { value: "accessories", label: "Аксессуары" },
  { value: "hardware", label: "Крепёж/фурнитура" },
  { value: "frame", label: "Рамки" },
  { value: "stretcher", label: "Подрамники" },
  { value: "service", label: "Услуга" },
  { value: "other", label: "Прочее" },
];

const BASE_UOMS = [
  { value: "m2", label: "м²" },
  { value: "sheet", label: "лист" },
  { value: "pcs", label: "штука" },
  { value: "ml", label: "мл" },
  { value: "g", label: "г" },
];

const PURCHASE_UOMS = [
  { value: "roll", label: "рулон" },
  { value: "m2", label: "м²" },
  { value: "mp", label: "м.п." },
  { value: "sheet", label: "лист" },
  { value: "pack", label: "пачка" },
  { value: "box", label: "коробка" },
  { value: "pcs", label: "штука" },
  { value: "ml", label: "мл" },
  { value: "l", label: "литр" },
  { value: "g", label: "г" },
  { value: "kg", label: "кг" },
];

function uomLabel(uom: string){
  return PURCHASE_UOMS.find(x=>x.value===uom)?.label || uom;
}


function isoToday() {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

type PurchaseLineDraft = {
  material_id: number | "";
  material_query: string;
  qty: string;
  uom: string;
  unit_price: string;
  vat_rate: string;
};

function asNum(v: string) {
  const n = Number(String(v ?? "").replace(",", ".").trim());
  return Number.isFinite(n) ? n : 0;
}

export default function WarehouseUnifiedPage() {
  const toast = useToast();
  const [tab, setTab] = useState<TabKey>("lots");

  // Lots
  const [lotRows, setLotRows] = useState<LotRow[]>([]);

  // Movements
  const [movementRows, setMovementRows] = useState<MovementRow[]>([]);

  // Materials
  const [materialsRows, setMaterialsRows] = useState<Material[]>([]);
  const [matOpen, setMatOpen] = useState(false);
  const [matName, setMatName] = useState("");
  const [matCategory, setMatCategory] = useState(CATEGORIES[0].value);
  const [matBaseUom, setMatBaseUom] = useState(BASE_UOMS[0].value);
  const [matLotTracked, setMatLotTracked] = useState(true);
  const [matSaving, setMatSaving] = useState(false);

  // Purchases
  const [purchaseRows, setPurchaseRows] = useState<PurchaseDoc[]>([]);
  const [purchaseLoading, setPurchaseLoading] = useState(false);
  const [purOpen, setPurOpen] = useState(false);
  const [purSaving, setPurSaving] = useState(false);
  const [docDate, setDocDate] = useState(isoToday());
  const [supplier, setSupplier] = useState("");
  const [docNo, setDocNo] = useState("");
  const [payType, setPayType] = useState(PAY_TYPES[2].value);
  const [vatMode, setVatMode] = useState(VAT_MODES[0].value);
  const [comment, setComment] = useState("");
  const [postImmediately, setPostImmediately] = useState(true);
  const [lines, setLines] = useState<PurchaseLineDraft[]>([
    { material_id: "", material_query: "", qty: "", uom: "m2", unit_price: "", vat_rate: "0" },
  ]);

  // Material search dropdown (per-line)
  const [matPickerOpenIdx, setMatPickerOpenIdx] = useState<number | null>(null);

  // Inline material create (from Quick Receive)
  const [inlineMatOpen, setInlineMatOpen] = useState(false);
  const [inlineMatSaving, setInlineMatSaving] = useState(false);
  const [inlineMatLineIdx, setInlineMatLineIdx] = useState<number | null>(null);
  const [inlineMatName, setInlineMatName] = useState("");
  const [inlineMatBaseUom, setInlineMatBaseUom] = useState(BASE_UOMS[0].value);
  const [inlineMatCategory, setInlineMatCategory] = useState(CATEGORIES[0].value);
  const [inlineMatLotTracked, setInlineMatLotTracked] = useState(true);

  // Writeoff (списание)
  const [woOpen, setWoOpen] = useState(false);
  const [woReason, setWoReason] = useState<WriteoffCreate["reason"]>("production");
  const [woComment, setWoComment] = useState("");
  const [woSaving, setWoSaving] = useState(false);
  const [woLines, setWoLines] = useState<{ material_id: number | ""; material_query: string; qty: string; uom: string }[]>([
    { material_id: "", material_query: "", qty: "", uom: "pcs" },
  ]);

  async function loadLots() {
    try {
      const data = await api<LotRow[]>("/stock/lots");
      setLotRows(data);
    } catch (e: any) {
      toast.push(e.message || "Ошибка", "err");
    }
  }

  async function loadMaterials() {
    try {
      const data = await api<Material[]>("/materials");
      setMaterialsRows(data);
    } catch (e: any) {
      toast.push(e.message || "Ошибка", "err");
    }
  }

  async function loadPurchases() {
    setPurchaseLoading(true);
    try {
      const data = await api<PurchaseDoc[]>("/purchases");
      setPurchaseRows(data);
    } catch (e: any) {
      toast.push(e.message || "Ошибка", "err");
    } finally {
      setPurchaseLoading(false);
    }
  }

  async function loadMovements() {
    try {
      const data = await api<MovementRow[]>("/stock/movements");
      setMovementRows(data);
    } catch (e: any) {
      toast.push(e.message || "Ошибка", "err");
    }
  }

  async function loadAll() {
    await Promise.all([loadLots(), loadMaterials(), loadPurchases(), loadMovements()]);
  }

  useEffect(() => {
    loadAll();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // --- columns ---
  const lotColumns: Column<LotRow>[] = useMemo(
    () => [
      { key: "lot_id", title: "Партия" },
      { key: "material_name", title: "Материал" },
      { key: "qty_in", title: "IN" },
      { key: "qty_out", title: "OUT" },
      { key: "qty_remaining", title: "Остаток" },
      { key: "unit_cost", title: "Себестоимость" },
      { key: "created_at", title: "Дата" },
    ],
    [],
  );

  const materialsColumns: Column<Material>[] = useMemo(
    () => [
      { key: "id", title: "ID" },
      { key: "name", title: "Название" },
      { key: "category", title: "Категория" },
      { key: "base_uom", title: "Ед." },
      {
        key: "is_lot_tracked",
        title: "FIFO",
        render: (r) => (r.is_lot_tracked ? "YES" : "NO"),
      },
    ],
    [],
  );

  const purchasesColumns: Column<PurchaseDoc>[] = useMemo(
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

  const movementColumns: Column<MovementRow>[] = useMemo(
    () => [
      { key: "id", title: "ID" },
      { key: "mv_date", title: "Дата" },
      { key: "mv_type", title: "Тип" },
      { key: "material_name", title: "Материал" },
      { key: "qty", title: "Кол-во" },
      { key: "lot_id", title: "Партия" },
      { key: "ref_type", title: "Док" },
    ],
    [],
  );

  // --- actions ---
  async function createMaterial() {
    if (!matName.trim()) {
      toast.push("Название обязательно", "err");
      return;
    }
    setMatSaving(true);
    try {
      await api<Material>("/materials", {
        method: "POST",
        body: JSON.stringify({
          name: matName.trim(),
          category: matCategory,
          base_uom: matBaseUom,
          is_lot_tracked: matLotTracked,
        }),
      });
      toast.push("Материал создан", "ok");
      setMatOpen(false);
      setMatName("");
      await loadMaterials();
    } catch (e: any) {
      toast.push(e.message || "Ошибка создания", "err");
    } finally {
      setMatSaving(false);
    }
  }

  async function createMaterialInline() {
    if (!inlineMatName.trim()) {
      toast.push("Название обязательно", "err");
      return;
    }
    setInlineMatSaving(true);
    try {
      const created = await api<Material>("/materials", {
        method: "POST",
        body: JSON.stringify({
          name: inlineMatName.trim(),
          category: inlineMatCategory,
          base_uom: inlineMatBaseUom,
          is_lot_tracked: inlineMatLotTracked,
        }),
      });
      toast.push("Материал создан", "ok");
      setInlineMatOpen(false);
      await loadMaterials();

      if (inlineMatLineIdx !== null) {
        setLines((ls) =>
          ls.map((x, i) =>
            i === inlineMatLineIdx
              ? { ...x, material_id: created.id, material_query: created.name }
              : x,
          ),
        );
      }
    } catch (e: any) {
      toast.push(e.message || "Ошибка создания", "err");
    } finally {
      setInlineMatSaving(false);
    }
  }

  function resetPurchaseForm() {
    setDocDate(isoToday());
    setSupplier("");
    setDocNo("");
    setPayType(PAY_TYPES[2].value);
    setVatMode(VAT_MODES[0].value);
    setComment("");
    setPostImmediately(true);
    setLines([
      { material_id: "", material_query: "", qty: "", uom: "m2", unit_price: "", vat_rate: "0" },
    ]);
  }

  const materialsSorted = useMemo(
    () => materialsRows.slice().sort((a, b) => a.name.localeCompare(b.name)),
    [materialsRows],
  );

  const purchaseTotals = useMemo(() => {
    let net = 0;
    let vat = 0;
    for (const l of lines) {
      const lineNet = asNum(l.qty) * asNum(l.unit_price);
      const lineVat = lineNet * (asNum(l.vat_rate) / 100);
      net += lineNet;
      vat += lineVat;
    }
    return { net, vat, gross: net + vat };
  }, [lines]);

	const categorySuggestions = useMemo(() => {
	  return Array.from(new Set(materialsRows.map((m) => m.category).filter(Boolean))).sort();
	}, [materialsRows]);


  async function createPurchase() {
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

    setPurSaving(true);
    try {
      const created = await api<PurchaseDoc>("/purchases", {
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

      if (postImmediately) {
        try {
          await api(`/purchases/${created.id}/post`, { method: "POST" });
          toast.push(`Принято и проведено (#${created.id})`, "ok");
        } catch (e: any) {
          toast.push(e?.message || `Создано (#${created.id}), но не проведено`, "err");
        }
      } else {
        toast.push(`Создано (#${created.id})`, "ok");
      }

      setPurOpen(false);
      resetPurchaseForm();
      await Promise.all([loadPurchases(), loadLots()]);
      setTab("purchases");
    } catch (e: any) {
      toast.push(e.message || "Ошибка создания", "err");
    } finally {
      setPurSaving(false);
    }
  }

  async function postDoc(doc: PurchaseDoc) {
    try {
      await api(`/purchases/${doc.id}/post`, { method: "POST" });
      toast.push(`Документ #${doc.id} проведён`, "ok");
      await Promise.all([loadPurchases(), loadLots()]);
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
      await Promise.all([loadPurchases(), loadLots()]);
    } catch (e: any) {
      toast.push(e.message || "Ошибка VOID", "err");
    }
  }

  async function createWriteoff() {
    const clean = woLines
      .map((l) => ({
        material_id: l.material_id === "" ? null : Number(l.material_id),
        qty: asNum(l.qty),
        uom: (l.uom || "pcs").trim(),
      }))
      .filter((l) => l.material_id && l.qty > 0);
    if (clean.length === 0) {
      return toast.push("Добавь хотя бы 1 строку", "err");
    }

    setWoSaving(true);
    try {
      await api("/stock/writeoffs", {
        method: "POST",
        body: JSON.stringify({
          reason: woReason,
          comment: woComment.trim() || null,
          lines: clean,
        }),
      });
      toast.push("Списание выполнено", "ok");
      setWoOpen(false);
      setWoComment("");
      setWoLines([{ material_id: "", material_query: "", qty: "", uom: "pcs" }]);
      await Promise.all([loadLots(), loadMovements()]);
      setTab("movements");
    } catch (e: any) {
      toast.push(e.message || "Ошибка списания", "err");
    } finally {
      setWoSaving(false);
    }
  }

  return (
    <div className="space-y-6">
      <datalist id="cat_suggest">
        {categorySuggestions.map((c) => (
          <option key={c} value={c} />
        ))}
      </datalist>
      {/* Page header */}
      <div className="flex items-center justify-between">
        <div>
          <div className="text-lg font-semibold">Склад FIFO</div>
          <div className="mt-1 text-sm text-slate-500">
            Партии, материалы и закупки — в одном месте
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="secondary" onClick={() => setWoOpen(true)}>Списание</Button>
          <Button variant="secondary" onClick={loadAll}>Обновить</Button>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex flex-wrap items-center gap-2">
        {TABS.map((t) => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            className={[
              "rounded-md px-3 py-2 text-sm transition",
              tab === t.key
                ? "bg-blue-900 text-white"
                : "border border-slate-200 bg-white text-slate-700 hover:border-blue-300 hover:text-slate-900",
            ].join(" ")}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Content */}
      {tab === "lots" ? (
        <DataTable
          title="Партии"
          columns={lotColumns}
          rows={lotRows}
          onRefresh={loadLots}
          onExport={() => toast.push("Экспорт: скоро", "ok")}
          rowActions={(row) => (
            <>
              <Button variant="secondary" onClick={() => toast.push(`Партия #${row.lot_id}`, "ok")}>
                Просмотр
              </Button>
              <Button variant="ghost" onClick={() => toast.push("ADJUST/SCRAP: скоро", "ok")}>
                Движение
              </Button>
            </>
          )}
        />
      ) : null}

      {tab === "materials" ? (
        <>
          <DataTable
            title="Материалы"
            columns={materialsColumns}
            rows={materialsRows}
            onRefresh={loadMaterials}
            onExport={() => toast.push("Экспорт: скоро", "ok")}
            actions={
              <Button
                variant="primary"
                onClick={() => {
                  setMatOpen(true);
                }}
              >
                Добавить
              </Button>
            }
            rowActions={(row) => (
              <>
                <Button variant="secondary" onClick={() => toast.push(`Материал #${row.id}`, "ok")}>
                  Просмотр
                </Button>
                <Button variant="ghost" onClick={() => toast.push("Редактирование: API пока нет", "err")}>
                  Изменить
                </Button>
              </>
            )}
          />

          <Modal
            open={matOpen}
            title="Добавить материал"
            onClose={() => setMatOpen(false)}
            footer={
              <>
                <Button variant="secondary" onClick={() => setMatOpen(false)}>
                  Отмена
                </Button>
                <Button variant="primary" onClick={createMaterial} disabled={matSaving}>
                  {matSaving ? "Сохранение…" : "Создать"}
                </Button>
              </>
            }
          >
            <div className="grid grid-cols-1 gap-4">
              <Input label="Название" value={matName} onChange={setMatName} placeholder="Напр. Orafol 3551" />
              <label className="block">
  <div className="mb-1 text-xs font-medium text-slate-700">Категория</div>
  <input
    value={matCategory}
    onChange={(e) => setMatCategory(e.target.value)}
    list="cat_suggest"
    placeholder="Напр: аксессуары / подрамники / рамки ..."
    className="h-10 w-full rounded-md border border-slate-200 bg-slate-50 px-3 text-sm outline-none focus:border-blue-400"
  />
</label>

              <Select label="Базовая единица" value={matBaseUom} onChange={setMatBaseUom} options={BASE_UOMS} />

              <label className="flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={matLotTracked}
                  onChange={(e) => setMatLotTracked(e.target.checked)}
                />
                <span className="text-sm text-slate-700">Учет партий (FIFO)</span>
              </label>
            </div>
          </Modal>
        </>
      ) : null}

      {tab === "purchases" ? (
        <>
          <DataTable
            title="Закупки"
            columns={purchasesColumns}
            rows={purchaseRows}
            onRefresh={loadPurchases}
            onExport={() => toast.push("Экспорт: скоро", "ok")}
            actions={
              <>
                <Button
                  variant="primary"
                  onClick={() => {
                    resetPurchaseForm();
                    setPurOpen(true);
                  }}
                >
                  Быстро принять
                </Button>
              </>
            }
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
            open={purOpen}
            title="Быстрое принятие закупки"
            onClose={() => setPurOpen(false)}
            footer={
              <div className="flex justify-end gap-2">
                <Button variant="secondary" onClick={() => setPurOpen(false)}>
                  Отмена
                </Button>
                <Button variant="primary" onClick={createPurchase} disabled={purSaving}>
                  {purSaving ? "Принятие…" : "Принять"}
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

              <label className="flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={postImmediately}
                  onChange={(e) => setPostImmediately(e.target.checked)}
                />
                <span className="text-sm text-slate-700">Провести сразу (создать партии FIFO)</span>
              </label>

              <div className="rounded-xl border border-slate-200">
                <div className="flex items-center justify-between border-b border-slate-200 bg-slate-50 px-4 py-3">
                  <div className="text-sm font-semibold">Позиции</div>
                  <Button
                    variant="secondary"
                    onClick={() =>
                      setLines((ls) => [
                        ...ls,
                        {
                          material_id: "",
                          material_query: "",
                          qty: "",
                          uom: "m2",
                          unit_price: "",
                          vat_rate: "0",
                        },
                      ])
                    }
                  >
                    + Позиция
                  </Button>
                </div>

                <div className="space-y-3 p-4">
                  {lines.map((l, idx) => (
                    <div key={idx} className="grid grid-cols-1 gap-3 md:grid-cols-1">
                      <div className="relative">
                        {idx === 0 ? (
                          <div className="mb-1 text-xs font-medium text-slate-700">Материал</div>
                        ) : null}
                        <div className="flex gap-2">
                          <input
                            value={l.material_query}
                            onChange={(e) => {
                              const v = e.target.value;
                              setLines((ls) =>
                                ls.map((x, i) =>
                                  i === idx ? { ...x, material_query: v, material_id: "" } : x,
                                ),
                              );
                              setMatPickerOpenIdx(idx);
                            }}
                            onFocus={() => setMatPickerOpenIdx(idx)}
                            onBlur={() => {
                              // allow click on dropdown
                              setTimeout(() => {
                                setMatPickerOpenIdx((cur) => (cur === idx ? null : cur));
                              }, 150);
                            }}
                            placeholder="Начни вводить…"
                            className="h-10 w-full rounded-md border border-slate-200 bg-slate-50 px-3 text-sm outline-none focus:border-blue-400"
                          />
                          <Button
                            variant="secondary"
                            onClick={() => {
                              setInlineMatLineIdx(idx);
                              setInlineMatName(l.material_query || "");
                              setInlineMatBaseUom(BASE_UOMS[0].value);
                              setInlineMatCategory(CATEGORIES[0].value);
                              setInlineMatLotTracked(true);
                              setInlineMatOpen(true);
                            }}
                          >
                            +
                          </Button>
                        </div>

                        {matPickerOpenIdx === idx ? (
                          <div className="absolute z-20 mt-1 w-[520px] max-w-[90vw] overflow-hidden rounded-md border border-slate-200 bg-white shadow">
                            <div className="max-h-56 overflow-auto">
                              {materialsSorted
                                .filter((m) =>
                                  (l.material_query || "")
                                    .toLowerCase()
                                    .split(/\s+/)
                                    .filter(Boolean)
                                    .every((t) => m.name.toLowerCase().includes(t)),
                                )
                                .slice(0, 25)
                                .map((m) => (
                                  <button
                                    key={m.id}
                                    type="button"
                                    className="block w-full px-3 py-2 text-left text-sm hover:bg-slate-50"
                                    onMouseDown={(e) => e.preventDefault()}
                                    onClick={() => {
                                      setLines((ls) =>
                                        ls.map((x, i) =>
                                          i === idx
                                            ? { ...x, material_id: m.id, material_query: m.name }
                                            : x,
                                        ),
                                      );
                                      setMatPickerOpenIdx(null);
                                    }}
                                  >
                                    <div className="flex items-center justify-between">
                                      <span className="truncate">{m.name}</span>
                                      <span className="ml-3 text-xs text-slate-500">#{m.id}</span>
                                    </div>
                                  </button>
                                ))}

                              <div className="border-t border-slate-200 p-2">
                                <Button
                                  variant="ghost"
                                  onMouseDown={(e) => e.preventDefault()}
                                  onClick={() => {
                                    setInlineMatLineIdx(idx);
                                    setInlineMatName(l.material_query || "");
                                    setInlineMatBaseUom(BASE_UOMS[0].value);
                                    setInlineMatCategory(CATEGORIES[0].value);
                                    setInlineMatLotTracked(true);
                                    setInlineMatOpen(true);
                                  }}
                                >
                                  + Создать материал
                                </Button>
                              </div>
                            </div>
                          </div>
                        ) : null}
                      </div>

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
                        label={idx === 0 ? `Цена (за 1 ${uomLabel(l.uom)})` : undefined}
                        value={l.unit_price}
                        onChange={(v) =>
                          setLines((ls) => ls.map((x, i) => (i === idx ? { ...x, unit_price: v } : x)))
                        }
                        placeholder="0"
                      />

                      <Input
                        label={idx === 0 ? "НДС %" : undefined}
                        value={l.vat_rate}
                        onChange={(v) =>
                          setLines((ls) => ls.map((x, i) => (i === idx ? { ...x, vat_rate: v } : x)))
                        }
                        onKeyDown={(e) => {
                          if (e.key === "Enter") {
                            e.preventDefault();
                            if (idx === lines.length - 1) {
                              setLines((ls) => [
                                ...ls,
                                {
                                  material_id: "",
                                  material_query: "",
                                  qty: "",
                                  uom: "m2",
                                  unit_price: "",
                                  vat_rate: "0",
                                },
                              ]);
                              setMatPickerOpenIdx(null);
                            }
                          }
                        }}
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

              <div className="flex items-center justify-end">
                <div className="rounded-xl border border-slate-200 bg-white px-4 py-3 text-sm">
                  <div className="flex items-center gap-6">
                    <div>
                      <div className="text-xs text-slate-500">Итого без НДС</div>
                      <div className="font-semibold">{purchaseTotals.net.toFixed(2)}</div>
                    </div>
                    <div>
                      <div className="text-xs text-slate-500">НДС</div>
                      <div className="font-semibold">{purchaseTotals.vat.toFixed(2)}</div>
                    </div>
                    <div>
                      <div className="text-xs text-slate-500">Итого с НДС</div>
                      <div className="font-semibold">{purchaseTotals.gross.toFixed(2)}</div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </Modal>

          <Modal
            open={inlineMatOpen}
            title="Создать материал"
            onClose={() => setInlineMatOpen(false)}
            footer={
              <div className="flex justify-end gap-2">
                <Button variant="secondary" onClick={() => setInlineMatOpen(false)}>
                  Отмена
                </Button>
                <Button variant="primary" onClick={createMaterialInline} disabled={inlineMatSaving}>
                  {inlineMatSaving ? "Сохранение…" : "Создать"}
                </Button>
              </div>
            }
          >
            <div className="grid grid-cols-1 gap-4">
              <Input
                label="Название"
                value={inlineMatName}
                onChange={setInlineMatName}
                placeholder="Напр. Orafol 3551"
              />
              <label className="block">
  <div className="mb-1 text-xs font-medium text-slate-700">Категория</div>
  <input
    value={inlineMatCategory}
    onChange={(e) => setInlineMatCategory(e.target.value)}
    list="cat_suggest"
    placeholder="Напр: аксессуары / подрамники / рамки ..."
    className="h-10 w-full rounded-md border border-slate-200 bg-slate-50 px-3 text-sm outline-none focus:border-blue-400"
  />
</label>

              <Select
                label="Базовая единица"
                value={inlineMatBaseUom}
                onChange={setInlineMatBaseUom}
                options={BASE_UOMS}
              />
              <label className="flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={inlineMatLotTracked}
                  onChange={(e) => setInlineMatLotTracked(e.target.checked)}
                />
                <span className="text-sm text-slate-700">Учет партий (FIFO)</span>
              </label>
            </div>
          </Modal>
        </>
      ) : null}

      {tab === "movements" ? (
        <DataTable
          title="Движения"
          columns={movementColumns}
          rows={movementRows}
          onRefresh={loadMovements}
          onExport={() => toast.push("Экспорт: скоро", "ok")}
        />
      ) : null}

      <Modal
        open={woOpen}
        title="Списание материалов"
        onClose={() => setWoOpen(false)}
        footer={
          <div className="flex justify-end gap-2">
            <Button variant="secondary" onClick={() => setWoOpen(false)}>
              Отмена
            </Button>
            <Button variant="primary" onClick={createWriteoff} disabled={woSaving}>
              {woSaving ? "Списание…" : "Списать"}
            </Button>
          </div>
        }
      >
        <div className="space-y-4">
          <Select
            label="Причина"
            value={woReason}
            onChange={(v) => setWoReason(v as any)}
            options={[
              { value: "production", label: "Производство" },
              { value: "scrap", label: "Брак/утилизация" },
              { value: "other", label: "Другое" },
            ]}
          />
          <Input label="Комментарий" value={woComment} onChange={setWoComment} placeholder="необязательно" />

          <div className="space-y-2">
            <div className="grid grid-cols-3 gap-2 text-xs text-slate-500">
              <div>Материал</div>
              <div>Кол-во</div>
              <div>Ед.</div>
            </div>
            {woLines.map((l, idx) => (
              <div key={idx} className="grid grid-cols-3 gap-2">
                <Select
                  label={undefined}
                  value={l.material_id === "" ? "" : String(l.material_id)}
                  onChange={(v) => setWoLines((ls) => ls.map((x, i) => (i === idx ? { ...x, material_id: v ? Number(v) : "" } : x)))}
                  options={[{ value: "", label: "Выбери материал" }, ...materialsRows.map((m) => ({ value: String(m.id), label: m.name }))]}
                />
                <Input
                  label={undefined}
                  value={l.qty}
                  onChange={(v) => setWoLines((ls) => ls.map((x, i) => (i === idx ? { ...x, qty: v } : x)))}
                  placeholder="0"
                />
                <Select
                  label={undefined}
                  value={l.uom}
                  onChange={(v) => setWoLines((ls) => ls.map((x, i) => (i === idx ? { ...x, uom: v } : x)))}
                  options={PURCHASE_UOMS}
                />
              </div>
            ))}
            <div className="flex items-center gap-2">
              <Button
                variant="secondary"
                onClick={() => setWoLines((ls) => [...ls, { material_id: "", material_query: "", qty: "", uom: "pcs" }])}
              >
                + Строка
              </Button>
              <Button
                variant="ghost"
                onClick={() => setWoLines((ls) => (ls.length > 1 ? ls.slice(0, -1) : ls))}
              >
                - Удалить
              </Button>
            </div>
            <div className="text-xs text-slate-500">
              Подсказка: для рулонов можно списывать в м.п. (погонных метрах) — будет пересчёт в м² по ширине.
            </div>
          </div>
        </div>
      </Modal>
    </div>
  );
}
