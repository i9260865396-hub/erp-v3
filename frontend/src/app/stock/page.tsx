"use client";

import { useEffect, useState } from "react";

const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

type LotRow = {
  lot_id: number;
  material_id: number;
  material_name: string;
  category: string;
  qty_in: number;
  qty_out: number;
  qty_remaining: number;
  unit_cost: number;
  created_at: string | null;
};

export default function StockPage() {
  const [rows, setRows] = useState<LotRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  async function load() {
    setErr(null);
    setLoading(true);
    try {
      const r = await fetch(`${API}/stock/lots`, { cache: "no-store" });
      if (!r.ok) throw new Error(await r.text());
      const data = await r.json();
      setRows(data);
    } catch (e: any) {
      setErr(e?.message ?? "РћС€РёР±РєР° Р·Р°РіСЂСѓР·РєРё СЃРєР»Р°РґР°");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, []);

  return (
    <div style={{ padding: 24, maxWidth: 1200, margin: "0 auto" }}>
      <h1 style={{ fontSize: 28, fontWeight: 700 }}>РЎРєР»Р°Рґ (РїР°СЂС‚РёРё / FIFO)</h1>

      <div style={{ marginTop: 12, display: "flex", gap: 10 }}>
        <button onClick={load} disabled={loading} style={{ padding: "8px 12px", borderRadius: 10, border: "1px solid #ccc" }}>
          РћР±РЅРѕРІРёС‚СЊ
        </button>
        {loading && <div style={{ alignSelf: "center" }}>Р—Р°РіСЂСѓР·РєР°вЂ¦</div>}
      </div>

      {err && <div style={{ marginTop: 12, color: "crimson" }}>{err}</div>}

      <div style={{ marginTop: 12, border: "1px solid #ddd", borderRadius: 12, overflow: "hidden" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ background: "#f5f5f5" }}>
              <th style={{ textAlign: "left", padding: 10, borderBottom: "1px solid #ddd" }}>Lot</th>
              <th style={{ textAlign: "left", padding: 10, borderBottom: "1px solid #ddd" }}>РњР°С‚РµСЂРёР°Р»</th>
              <th style={{ textAlign: "left", padding: 10, borderBottom: "1px solid #ddd" }}>РљР°С‚РµРіРѕСЂРёСЏ</th>
              <th style={{ textAlign: "right", padding: 10, borderBottom: "1px solid #ddd" }}>РџСЂРёС…РѕРґ</th>
              <th style={{ textAlign: "right", padding: 10, borderBottom: "1px solid #ddd" }}>РЎРїРёСЃР°РЅРѕ</th>
              <th style={{ textAlign: "right", padding: 10, borderBottom: "1px solid #ddd" }}>РћСЃС‚Р°С‚РѕРє</th>
              <th style={{ textAlign: "right", padding: 10, borderBottom: "1px solid #ddd" }}>Р¦РµРЅР°/РµРґ</th>
              <th style={{ textAlign: "left", padding: 10, borderBottom: "1px solid #ddd" }}>РЎРѕР·РґР°РЅ</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.lot_id}>
                <td style={{ padding: 10, borderBottom: "1px solid #eee" }}>{r.lot_id}</td>
                <td style={{ padding: 10, borderBottom: "1px solid #eee" }}>
                  {r.material_id} вЂ” {r.material_name}
                </td>
                <td style={{ padding: 10, borderBottom: "1px solid #eee" }}>{r.category}</td>
                <td style={{ padding: 10, borderBottom: "1px solid #eee", textAlign: "right" }}>{r.qty_in}</td>
                <td style={{ padding: 10, borderBottom: "1px solid #eee", textAlign: "right" }}>{r.qty_out}</td>
                <td style={{ padding: 10, borderBottom: "1px solid #eee", textAlign: "right", fontWeight: 700 }}>{r.qty_remaining}</td>
                <td style={{ padding: 10, borderBottom: "1px solid #eee", textAlign: "right" }}>{r.unit_cost}</td>
                <td style={{ padding: 10, borderBottom: "1px solid #eee" }}>{r.created_at ? r.created_at.slice(0, 19).replace("T", " ") : ""}</td>
              </tr>
            ))}
            {!rows.length && !loading && (
              <tr>
                <td colSpan={8} style={{ padding: 14, color: "#666" }}>
                  РџРѕРєР° РЅРµС‚ РїР°СЂС‚РёР№. РЎРѕР·РґР°Р№ Р·Р°РєСѓРїРєСѓ.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

