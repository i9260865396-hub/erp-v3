"use client";

import { useEffect, useMemo, useState } from "react";

const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

type Material = {
  id: number;
  name: string;
  category: string;
  base_uom: string;
  is_lot_tracked: boolean;
  is_void?: boolean;
  voided_at?: string | null;
  void_reason?: string | null;
};

type EditState = {
  open: boolean;
  id: number | null;
  name: string;
  category: string;
  base_uom: string;
  is_lot_tracked: boolean;
};

export default function MaterialsPage() {
  const [items, setItems] = useState<Material[]>([]);
  const [includeVoid, setIncludeVoid] = useState(false);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  // create
  const [name, setName] = useState("");
  const [category, setCategory] = useState("film");
  const [baseUom, setBaseUom] = useState("m2");
  const [lotTracked, setLotTracked] = useState(true);

  // edit modal
  const [edit, setEdit] = useState<EditState>({
    open: false,
    id: null,
    name: "",
    category: "film",
    base_uom: "m2",
    is_lot_tracked: true,
  });

  // void modal
  const [voidOpen, setVoidOpen] = useState(false);
  const [voidId, setVoidId] = useState<number | null>(null);
  const [voidReason, setVoidReason] = useState("");

  const kpi = useMemo(() => {
    const active = items.filter((x) => !x.is_void).length;
    const voided = items.filter((x) => x.is_void).length;
    const lot = items.filter((x) => x.is_lot_tracked && !x.is_void).length;
    return { active, voided, lot, total: items.length };
  }, [items]);

  async function load() {
    setErr(null);
    setLoading(true);
    try {
      const r = await fetch(`${API}/materials?include_void=${includeVoid ? "true" : "false"}`, { cache: "no-store" });
      if (!r.ok) throw new Error(await r.text());
      const data = await r.json();
      setItems(data);
    } catch (e: any) {
      setErr(e?.message ?? "Ошибка загрузки");
    } finally {
      setLoading(false);
    }
  }

  async function create() {
    setErr(null);
    if (!name.trim()) {
      setErr("Название обязательно");
      return;
    }
    setLoading(true);
    try {
      const r = await fetch(`${API}/materials`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: name.trim(),
          category,
          base_uom: baseUom,
          is_lot_tracked: lotTracked,
        }),
      });
      if (!r.ok) throw new Error(await r.text());

      setName("");
      await load();
    } catch (e: any) {
      setErr(e?.message ?? "Ошибка создания");
    } finally {
      setLoading(false);
    }
  }

  function openEdit(m: Material) {
    setEdit({
      open: true,
      id: m.id,
      name: m.name,
      category: m.category,
      base_uom: m.base_uom,
      is_lot_tracked: m.is_lot_tracked,
    });
  }

  async function saveEdit() {
    if (!edit.id) return;
    setErr(null);
    setLoading(true);
    try {
      const r = await fetch(`${API}/materials/${edit.id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: edit.name.trim(),
          category: edit.category,
          base_uom: edit.base_uom,
          is_lot_tracked: edit.is_lot_tracked,
        }),
      });
      if (!r.ok) throw new Error(await r.text());
      setEdit((s) => ({ ...s, open: false }));
      await load();
    } catch (e: any) {
      setErr(e?.message ?? "Ошибка сохранения");
    } finally {
      setLoading(false);
    }
  }

  function openVoid(m: Material) {
    setVoidId(m.id);
    setVoidReason("");
    setVoidOpen(true);
  }

  async function doVoid() {
    if (!voidId) return;
    setErr(null);
    setLoading(true);
    try {
      const r = await fetch(`${API}/materials/${voidId}/void`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ reason: voidReason.trim() || null }),
      });
      if (!r.ok) throw new Error(await r.text());
      setVoidOpen(false);
      setVoidId(null);
      setVoidReason("");
      await load();
    } catch (e: any) {
      setErr(e?.message ?? "Ошибка VOID");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [includeVoid]);

  return (
    <div>
      <div className="toolbar">
        <div>
          <div style={{ fontSize: 22, fontWeight: 900, color: "#0f172a" }}>Материалы</div>
          <div className="small">Справочник материалов. Политика: не удаляем — только VOID.</div>
        </div>

        <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
          <label className="small" style={{ display: "flex", gap: 8, alignItems: "center" }}>
            <input type="checkbox" checked={includeVoid} onChange={(e) => setIncludeVoid(e.target.checked)} />
            показывать VOID
          </label>
          <button className="btn" onClick={load} disabled={loading}>Обновить</button>
          <button className="btn btnPrimary" onClick={create} disabled={loading}>+ Добавить</button>
        </div>
      </div>

      <div className="cardRow">
        <div className="kpiCard">
          <div className="kpiTitle">Активные</div>
          <div className="kpiValue">{kpi.active}</div>
        </div>
        <div className="kpiCard">
          <div className="kpiTitle">VOID</div>
          <div className="kpiValue">{kpi.voided}</div>
        </div>
        <div className="kpiCard">
          <div className="kpiTitle">С учётом партий</div>
          <div className="kpiValue">{kpi.lot}</div>
        </div>
        <div className="kpiCard">
          <div className="kpiTitle">Всего (в выборке)</div>
          <div className="kpiValue">{kpi.total}</div>
        </div>
      </div>

      {err && <div style={{ marginBottom: 12, color: "#be123c", fontWeight: 800 }}>{err}</div>}

      <div className="panel">
        <div className="small">Быстрое добавление</div>
        <div className="fieldRow">
          <input className="input" value={name} onChange={(e) => setName(e.target.value)} placeholder="Название (например: Плёнка Orafol 80мкм)" />
          <select className="select" value={category} onChange={(e) => setCategory(e.target.value)}>
            <option value="film">film</option>
            <option value="banner">banner</option>
            <option value="ink">ink</option>
            <option value="packaging">packaging</option>
            <option value="service">service</option>
          </select>
          <select className="select" value={baseUom} onChange={(e) => setBaseUom(e.target.value)}>
            <option value="m2">m2</option>
            <option value="ml">ml</option>
            <option value="pcs">pcs</option>
            <option value="min">min</option>
          </select>
          <label className="small" style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <input type="checkbox" checked={lotTracked} onChange={(e) => setLotTracked(e.target.checked)} />
            партии
          </label>
        </div>
      </div>

      <div style={{ height: 12 }} />

      <div className="tableWrap">
        <table className="table">
          <thead>
            <tr>
              <th className="th" style={{ width: 70 }}>ID</th>
              <th className="th">Название</th>
              <th className="th" style={{ width: 120 }}>Категория</th>
              <th className="th" style={{ width: 90 }}>Ед.</th>
              <th className="th" style={{ width: 90 }}>Партии</th>
              <th className="th" style={{ width: 220 }}>Действия</th>
            </tr>
          </thead>
          <tbody>
            {items.map((m) => (
              <tr key={m.id}>
                <td className="td">{m.id}</td>
                <td className="td">
                  {m.name} {m.is_void ? <span className="badgeVoid">VOID</span> : null}
                  {m.is_void && (m.void_reason || m.voided_at) ? (
                    <div className="small" style={{ marginTop: 4 }}>
                      {m.void_reason ? `Причина: ${m.void_reason}` : null}
                      {m.voided_at ? ` • ${m.voided_at}` : null}
                    </div>
                  ) : null}
                </td>
                <td className="td">{m.category}</td>
                <td className="td">{m.base_uom}</td>
                <td className="td">{m.is_lot_tracked ? "да" : "нет"}</td>
                <td className="td">
                  <div style={{ display: "flex", gap: 8 }}>
                    <button className="btn" onClick={() => openEdit(m)} disabled={loading || !!m.is_void}>Редактировать</button>
                    <button className="btn btnDanger" onClick={() => openVoid(m)} disabled={loading || !!m.is_void}>VOID</button>
                  </div>
                </td>
              </tr>
            ))}
            {!items.length && !loading && (
              <tr>
                <td className="td" colSpan={6} style={{ color: "#64748b" }}>Пусто</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {loading && <div style={{ marginTop: 10 }} className="small">Загрузка…</div>}

      {/* Edit modal */}
      {edit.open && (
        <div className="modalBack" onClick={() => setEdit((s) => ({ ...s, open: false }))}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <div style={{ fontSize: 18, fontWeight: 900 }}>Редактирование материала</div>
              <button className="btn" onClick={() => setEdit((s) => ({ ...s, open: false }))}>Закрыть</button>
            </div>

            <div className="fieldRow">
              <input className="input" value={edit.name} onChange={(e) => setEdit((s) => ({ ...s, name: e.target.value }))} placeholder="Название" />
              <select className="select" value={edit.category} onChange={(e) => setEdit((s) => ({ ...s, category: e.target.value }))}>
                <option value="film">film</option>
                <option value="banner">banner</option>
                <option value="ink">ink</option>
                <option value="packaging">packaging</option>
                <option value="service">service</option>
              </select>
              <select className="select" value={edit.base_uom} onChange={(e) => setEdit((s) => ({ ...s, base_uom: e.target.value }))}>
                <option value="m2">m2</option>
                <option value="ml">ml</option>
                <option value="pcs">pcs</option>
                <option value="min">min</option>
              </select>
              <label className="small" style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <input type="checkbox" checked={edit.is_lot_tracked} onChange={(e) => setEdit((s) => ({ ...s, is_lot_tracked: e.target.checked }))} />
                партии
              </label>
            </div>

            <div style={{ display: "flex", justifyContent: "flex-end", gap: 10, marginTop: 12 }}>
              <button className="btn" onClick={() => setEdit((s) => ({ ...s, open: false }))}>Отмена</button>
              <button className="btn btnPrimary" onClick={saveEdit} disabled={loading}>Сохранить</button>
            </div>
          </div>
        </div>
      )}

      {/* VOID modal */}
      {voidOpen && (
        <div className="modalBack" onClick={() => setVoidOpen(false)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <div style={{ fontSize: 18, fontWeight: 900, color: "#be123c" }}>VOID материала</div>
              <button className="btn" onClick={() => setVoidOpen(false)}>Закрыть</button>
            </div>

            <div className="small" style={{ marginTop: 10 }}>
              Материал не удаляется. Он помечается как VOID и скрывается из стандартных списков.
            </div>

            <div style={{ marginTop: 10 }}>
              <div className="small">Причина (опционально)</div>
              <input className="input" value={voidReason} onChange={(e) => setVoidReason(e.target.value)} placeholder="Например: ошибка / заменён / не используем" />
            </div>

            <div style={{ display: "flex", justifyContent: "flex-end", gap: 10, marginTop: 12 }}>
              <button className="btn" onClick={() => setVoidOpen(false)}>Отмена</button>
              <button className="btn btnDanger" onClick={doVoid} disabled={loading}>Подтвердить VOID</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
