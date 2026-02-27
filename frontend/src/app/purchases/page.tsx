"use client";

import { useEffect, useMemo, useState } from "react";

const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

type Material = {
  id: number;
  name: string;
  category?: string;
  base_uom: string;
  is_lot_tracked?: boolean;
};

type Line = {
  material_id: number;      // 0 если не выбран
  qty: number;
  uom: string;
  unit_price: number;
  vat_rate: number;
};

export default function PurchasesPage() {
  const [materials, setMaterials] = useState<Material[]>([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [ok, setOk] = useState<string | null>(null);

  // шапка документа
  const [docDate, setDocDate] = useState(() => new Date().toISOString().slice(0, 10));
  const [supplier, setSupplier] = useState("");
  const [docNo, setDocNo] = useState("");
  const [payType, setPayType] = useState("bank");      // cash/card/bank
  const [vatMode, setVatMode] = useState("with_vat");  // with_vat/no_vat
  const [comment, setComment] = useState("");

  // строки
  const [lines, setLines] = useState<Line[]>([
  { material_id: 0, material_name: "", qty: 1, uom: "m2", unit_price: 0, vat_rate: 20 },
]);

  const materialMap = useMemo(() => {
    const m = new Map<number, Material>();
    materials.forEach(x => m.set(x.id, x));
    return m;
  }, [materials]);

  async function loadMaterials() {
    const r = await fetch(`${API}/materials`, { cache: "no-store" });
    if (!r.ok) throw new Error(`materials load failed: HTTP ${r.status}`);
    const data = await r.json();
    setMaterials(Array.isArray(data) ? data : []);
  }

  useEffect(() => {
    loadMaterials().catch(() => {});
  }, []);

  function addLine() {
   setLines(prev => [
    ...prev,
    { material_id: 0, material_name: "", qty: 1, uom: "m2", unit_price: 0, vat_rate: 20 },
  ]);
}

  function removeLine(idx: number) {
    setLines(prev => prev.filter((_, i) => i !== idx));
  }

  function updateLine(idx: number, patch: Partial<Line>) {
    setLines(prev => prev.map((l, i) => (i === idx ? { ...l, ...patch } : l)));
  }

  async function submit() {
    setErr(null);
    setOk(null);

    if (!supplier.trim()) return setErr("Поставщик обязателен");
    if (!docNo.trim()) return setErr("Номер документа обязателен");

    // требуем хотя бы одну строку с выбранным материалом
    const clean = lines.filter(l => l.material_id > 0 && l.qty > 0);
    if (!clean.length) return setErr("Добавь хотя бы одну строку: выбери материал и количество");

    // нормализуем строки: подставим base_uom если пусто
    const normalized = clean.map(l => {
      const mat = materialMap.get(l.material_id);
      return {
        material_id: l.material_id,
        material_name: mat?.name ?? "",            // ВАЖНО: бэк ждёт material_name
        qty: Number(l.qty),
        uom: (l.uom || mat?.base_uom || "m2").trim(),
        unit_price: Number(l.unit_price),
        vat_rate: Number(l.vat_rate),
      };
    });

    setLoading(true);
    try {
      let r: Response;
      try {
        r = await fetch(`${API}/purchases`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            doc_date: docDate,
            supplier: supplier.trim(),
            doc_no: docNo.trim(),
            pay_type: payType,
            vat_mode: vatMode,
            comment: comment.trim() || null,
            lines: normalized,
          }),
        });
      } catch (e: any) {
        throw new Error(e?.message || "Не удалось соединиться с API");
      }

      if (!r.ok) {
        const t = await r.text().catch(() => "");
        throw new Error(t || `HTTP ${r.status}`);
      }

      const data: any = await r.json().catch(() => ({}));
      const id = data?.id ?? data?.purchase_doc_id ?? "???";
      setOk(`Создан документ закупки ID=${id}`);

      // reset
      setSupplier("");
      setDocNo("");
      setComment("");
      setLines([{ material_id: 0, qty: 1, uom: "m2", unit_price: 0, vat_rate: 20 }]);
    } catch (e: any) {
      setErr(e?.message ?? "Ошибка создания закупки");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div style={{ padding: 24, maxWidth: 1100, margin: "0 auto" }}>
      <h1 style={{ fontSize: 28, fontWeight: 700 }}>Закупки</h1>

      <div style={{ marginTop: 12, padding: 16, border: "1px solid #ddd", borderRadius: 12 }}>
        <h2 style={{ fontSize: 18, fontWeight: 600 }}>Документ закупки</h2>

        <div style={{ display: "grid", gridTemplateColumns: "160px 2fr 1.5fr 1fr 1fr", gap: 12, marginTop: 12 }}>
          <div>
            <div style={{ fontSize: 12, color: "#666" }}>Дата</div>
            <input type="date" value={docDate} onChange={(e) => setDocDate(e.target.value)}
              style={{ width: "100%", padding: 10, border: "1px solid #ccc", borderRadius: 10 }} />
          </div>

          <div>
            <div style={{ fontSize: 12, color: "#666" }}>Поставщик</div>
            <input value={supplier} onChange={(e) => setSupplier(e.target.value)} placeholder="ООО Поставщик"
              style={{ width: "100%", padding: 10, border: "1px solid #ccc", borderRadius: 10 }} />
          </div>

          <div>
            <div style={{ fontSize: 12, color: "#666" }}>Документ №</div>
            <input value={docNo} onChange={(e) => setDocNo(e.target.value)} placeholder="УПД/счет/чек"
              style={{ width: "100%", padding: 10, border: "1px solid #ccc", borderRadius: 10 }} />
          </div>

          <div>
            <div style={{ fontSize: 12, color: "#666" }}>Оплата</div>
            <select value={payType} onChange={(e) => setPayType(e.target.value)}
              style={{ width: "100%", padding: 10, border: "1px solid #ccc", borderRadius: 10 }}>
              <option value="bank">безнал</option>
              <option value="cash">нал</option>
              <option value="card">карта</option>
            </select>
          </div>

          <div>
            <div style={{ fontSize: 12, color: "#666" }}>НДС режим</div>
            <select value={vatMode} onChange={(e) => setVatMode(e.target.value)}
              style={{ width: "100%", padding: 10, border: "1px solid #ccc", borderRadius: 10 }}>
              <option value="with_vat">с НДС</option>
              <option value="no_vat">без НДС</option>
            </select>
          </div>
        </div>

        <div style={{ marginTop: 12 }}>
          <div style={{ fontSize: 12, color: "#666" }}>Комментарий</div>
          <input value={comment} onChange={(e) => setComment(e.target.value)} placeholder="по желанию"
            style={{ width: "100%", padding: 10, border: "1px solid #ccc", borderRadius: 10 }} />
        </div>
      </div>

      <div style={{ marginTop: 14, padding: 16, border: "1px solid #ddd", borderRadius: 12 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <h2 style={{ fontSize: 18, fontWeight: 600 }}>Строки</h2>
          <button onClick={addLine} style={{ padding: "8px 12px", borderRadius: 10, border: "1px solid #ccc" }}>
            + строка
          </button>
        </div>

        <div style={{ marginTop: 10, border: "1px solid #eee", borderRadius: 12, overflow: "hidden" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ background: "#f5f5f5" }}>
                <th style={{ textAlign: "left", padding: 10, borderBottom: "1px solid #ddd" }}>Материал</th>
                <th style={{ textAlign: "left", padding: 10, borderBottom: "1px solid #ddd" }}>Кол-во</th>
                <th style={{ textAlign: "left", padding: 10, borderBottom: "1px solid #ddd" }}>Ед.</th>
                <th style={{ textAlign: "left", padding: 10, borderBottom: "1px solid #ddd" }}>Цена/ед</th>
                <th style={{ textAlign: "left", padding: 10, borderBottom: "1px solid #ddd" }}>НДС %</th>
                <th style={{ textAlign: "left", padding: 10, borderBottom: "1px solid #ddd" }}></th>
              </tr>
            </thead>

            <tbody>
              {lines.map((l, idx) => (
                <tr key={idx}>
                  <td style={{ padding: 10, borderBottom: "1px solid #eee" }}>
                    <select
                      value={l.material_id}
                      onChange={(e) => updateLine(idx, { material_id: Number(e.target.value) })}
                      style={{ width: "100%", padding: 8, border: "1px solid #ccc", borderRadius: 10 }}
                    >
                      <option value={0}>— выбрать —</option>
                      {materials.map(m => (
                        <option key={m.id} value={m.id}>
                          {m.id} — {m.name} ({m.base_uom})
                        </option>
                      ))}
                    </select>
                  </td>

                  <td style={{ padding: 10, borderBottom: "1px solid #eee" }}>
                    <input type="number" value={l.qty}
                      onChange={(e) => updateLine(idx, { qty: Number(e.target.value) })}
                      style={{ width: "100%", padding: 8, border: "1px solid #ccc", borderRadius: 10 }} />
                  </td>

                  <td style={{ padding: 10, borderBottom: "1px solid #eee" }}>
                    <input value={l.uom}
                      onChange={(e) => updateLine(idx, { uom: e.target.value })}
                      style={{ width: "100%", padding: 8, border: "1px solid #ccc", borderRadius: 10 }} />
                  </td>

                  <td style={{ padding: 10, borderBottom: "1px solid #eee" }}>
                    <input type="number" value={l.unit_price}
                      onChange={(e) => updateLine(idx, { unit_price: Number(e.target.value) })}
                      style={{ width: "100%", padding: 8, border: "1px solid #ccc", borderRadius: 10 }} />
                  </td>

                  <td style={{ padding: 10, borderBottom: "1px solid #eee" }}>
                    <input type="number" value={l.vat_rate}
                      onChange={(e) => updateLine(idx, { vat_rate: Number(e.target.value) })}
                      style={{ width: "100%", padding: 8, border: "1px solid #ccc", borderRadius: 10 }} />
                  </td>

                  <td style={{ padding: 10, borderBottom: "1px solid #eee" }}>
                    <button onClick={() => removeLine(idx)} style={{ padding: "8px 10px", borderRadius: 10, border: "1px solid #ccc" }}>
                      Удалить
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <button
          onClick={submit}
          disabled={loading}
          style={{
            marginTop: 12,
            padding: "10px 14px",
            borderRadius: 10,
            border: "1px solid #111",
            background: "#111",
            color: "white",
            cursor: loading ? "not-allowed" : "pointer",
            opacity: loading ? 0.7 : 1,
          }}
        >
          {loading ? "Создание..." : "Создать закупку"}
        </button>

        {err && <div style={{ marginTop: 10, color: "crimson", whiteSpace: "pre-wrap" }}>{err}</div>}
        {ok && <div style={{ marginTop: 10, color: "green", whiteSpace: "pre-wrap" }}>{ok}</div>}
      </div>
    </div>
  );
}
