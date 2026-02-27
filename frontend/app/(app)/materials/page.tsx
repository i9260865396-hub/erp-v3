"use client";

import { useEffect, useMemo, useState } from "react";
import DataTable, { Column } from "@/components/table/DataTable";
import Modal from "@/components/ui/Modal";
import Input from "@/components/ui/Input";
import Select from "@/components/ui/Select";
import Button from "@/components/ui/Button";
import { useToast } from "@/components/ui/Toast";
import { api } from "@/lib/api";
import type { Material } from "@/types/api";

const CATEGORIES = [
  { value: "film", label: "film" },
  { value: "banner", label: "banner" },
  { value: "ink", label: "ink" },
  { value: "packaging", label: "packaging" },
  { value: "service", label: "service" },
];

const UOMS = [
  { value: "m2", label: "m2" },
  { value: "ml", label: "ml" },
  { value: "pcs", label: "pcs" },
  { value: "min", label: "min" },
];

export default function MaterialsPage() {
  const toast = useToast();
  const [rows, setRows] = useState<Material[]>([]);
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [category, setCategory] = useState(CATEGORIES[0].value);
  const [baseUom, setBaseUom] = useState(UOMS[0].value);
  const [isLotTracked, setIsLotTracked] = useState(true);
  const [loading, setLoading] = useState(false);

  async function load() {
    try {
      const data = await api<Material[]>("/materials");
      setRows(data);
    } catch (e: any) {
      toast.push(e.message || "Ошибка", "err");
    }
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const columns: Column<Material>[] = useMemo(
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

  async function create() {
    if (!name.trim()) {
      toast.push("Название обязательно", "err");
      return;
    }
    setLoading(true);
    try {
      await api<Material>("/materials", {
        method: "POST",
        body: JSON.stringify({
          name: name.trim(),
          category,
          base_uom: baseUom,
          is_lot_tracked: isLotTracked,
        }),
      });
      toast.push("Материал создан", "ok");
      setOpen(false);
      setName("");
      await load();
    } catch (e: any) {
      toast.push(e.message || "Ошибка создания", "err");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <div className="text-lg font-semibold">Материалы</div>
        <div className="mt-1 text-sm text-slate-500">Справочник материалов (учет партий/FIFO)</div>
      </div>

      <DataTable
        title="Список материалов"
        columns={columns}
        rows={rows}
        onAdd={() => setOpen(true)}
        onRefresh={load}
        onExport={() => toast.push("Экспорт: скоро", "ok")}
        rowActions={(row) => (
          <>
            <Button variant="secondary" onClick={() => toast.push(`Материал #${row.id}`, "ok")}>
              Просмотр
            </Button>
            <Button variant="ghost" onClick={() => toast.push("Редактирование: API пока нет", "err")}>
              Изменить
            </Button>
            <Button variant="danger" onClick={() => toast.push("VOID: API пока нет", "err")}>
              VOID
            </Button>
          </>
        )}
      />

      <Modal
        open={open}
        title="Добавить материал"
        onClose={() => setOpen(false)}
        footer={
          <>
            <Button variant="secondary" onClick={() => setOpen(false)}>
              Отмена
            </Button>
            <Button variant="primary" onClick={create} disabled={loading}>
              {loading ? "Сохранение…" : "Создать"}
            </Button>
          </>
        }
      >
        <div className="grid grid-cols-1 gap-4">
          <Input label="Название" value={name} onChange={setName} placeholder="Напр. Orafol 3551" />
          <Select label="Категория" value={category} onChange={setCategory} options={CATEGORIES} />
          <Select label="Базовая единица" value={baseUom} onChange={setBaseUom} options={UOMS} />

          <label className="flex items-center gap-2">
            <input
              type="checkbox"
              checked={isLotTracked}
              onChange={(e) => setIsLotTracked(e.target.checked)}
            />
            <span className="text-sm text-slate-700">Учет партий (FIFO)</span>
          </label>
        </div>
      </Modal>
    </div>
  );
}
