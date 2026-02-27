"use client";

import { useEffect, useState } from "react";

const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

type Material = {
  id: number;
  name: string;
  category: string;
  base_uom: string;
  is_lot_tracked: boolean;
};

type PostLine = {
  material_id: number;
  qty: number;
  uom: string;
};

export default function OrdersPage() {
  const [materials, setMaterials] = useState<Material[]>([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [ok, setOk] = useState<string | null>(null);

  const [orderDate, setOrderDate] = useState(() => new Date().toISOString().slice(0, 10));
  const [comment, setComment] = useState("");

  // РёР·РґРµР»РёРµ (РґР»СЏ MVP)
  const [productName, setProductName] = useState("РќР°РєР»РµР№РєР°");
  const [qty, setQty] = useState(1);
  const [widthM, setWidthM] = useState(0.3);
  const [heightM, setHeightM] = useState(0.59);

  const [createdOrderId, setCreatedOrderId] = useState<number | null>(null);

  // РїСЂРѕРІРµРґРµРЅРёРµ
  const [postOrderId, setPostOrderId] = useState<number>(3); // РјРѕР¶РЅРѕ РјРµРЅСЏС‚СЊ РІСЂСѓС‡РЅСѓСЋ
  const [consumption, setConsumption] = useState<PostLine[]>([
    { material_id: 0, qty: 0.177, uom: "m2" }, // РїСЂРёРјРµСЂ РїРѕРґ 30x59СЃРј = 0.177РјВІ (Р±РµР· РѕС‚С…РѕРґРѕРІ)
  ]);

  const [minutes, setMinutes] = useState(15);
  const [rateHour, setRateHour] = useState(500);

  const [cMl, setCMl] = useState(0);
  const [mMl, setMMl] = useState(0);
  const [yMl, setYMl] = useState(0);
  const [kMl, setKMl] = useState(0);
  const [inkPricePerMl, setInkPricePerMl] = useState(0); // РµСЃР»Рё РЅРµ Р·РЅР°РµС€СЊ вЂ” СЃС‚Р°РІСЊ 0, РїРѕС‚РѕРј Р·Р°РґР°РґРёРј

  const [postResult, setPostResult] = useState<any | null>(null);

  async function loadMaterials() {
    const r = await fetch(`${API}/materials`, { cache: "no-store" });
    const data = await r.json();
    setMaterials(data);
  }

  useEffect(() => {
    loadMaterials().catch(() => {});
  }, []);

  async function createOrder() {
    setErr(null);
    setOk(null);
    setLoading(true);
    try {
      const r = await fetch(`${API}/orders`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          order_date: orderDate,
          comment: comment.trim() || null,
          items: [{ product_name: productName.trim(), qty, width_m: widthM, height_m: heightM }],
        }),
      });

      if (!r.ok) throw new Error(await r.text());
      const data = await r.json();
      setCreatedOrderId(data.order_id);
      setPostOrderId(data.order_id);
      setOk(`РЎРѕР·РґР°РЅ Р·Р°РєР°Р· ID=${data.order_id}`);
    } catch (e: any) {
      setErr(e?.message ?? "РћС€РёР±РєР° СЃРѕР·РґР°РЅРёСЏ Р·Р°РєР°Р·Р°");
    } finally {
      setLoading(false);
    }
  }

  function addConsumption() {
    setConsumption(prev => [...prev, { material_id: 0, qty: 0, uom: "m2" }]);
  }

  function removeConsumption(idx: number) {
    setConsumption(prev => prev.filter((_, i) => i !== idx));
  }

  function updateConsumption(idx: number, patch: Partial<PostLine>) {
    setConsumption(prev => prev.map((l, i) => (i === idx ? { ...l, ...patch } : l)));
  }

  async function postOrder() {
    setErr(null);
    setOk(null);
    setPostResult(null);

    if (!postOrderId) return setErr("РЈРєР°Р¶Рё order_id");
    const clean = consumption.filter(x => x.material_id > 0 && x.qty > 0);
    if (!clean.length) return setErr("Р”РѕР±Р°РІСЊ СЃРїРёСЃР°РЅРёРµ РјР°С‚РµСЂРёР°Р»Р° (material + qty)");

    setLoading(true);
    try {
      const r = await fetch(`${API}/orders/${postOrderId}/post`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          consumption: clean,
          minutes,
          rate_rub_per_hour: rateHour,
          c_ml: cMl,
          m_ml: mMl,
          y_ml: yMl,
          k_ml: kMl,
          ink_price_per_ml: inkPricePerMl,
        }),
      });

      if (!r.ok) throw new Error(await r.text());
      const data = await r.json();
      setPostResult(data);
      setOk(`Р—Р°РєР°Р· РїСЂРѕРІРµРґС‘РЅ. РЎРµР±РµСЃС‚РѕРёРјРѕСЃС‚СЊ: ${data.total_cost} в‚Ѕ`);
    } catch (e: any) {
      setErr(e?.message ?? "РћС€РёР±РєР° РїСЂРѕРІРµРґРµРЅРёСЏ");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div style={{ padding: 24, maxWidth: 1100, margin: "0 auto" }}>
      <h1 style={{ fontSize: 28, fontWeight: 700 }}>Р—Р°РєР°Р·С‹</h1>

      <div style={{ marginTop: 12, padding: 16, border: "1px solid #ddd", borderRadius: 12 }}>
        <h2 style={{ fontSize: 18, fontWeight: 600 }}>РЎРѕР·РґР°С‚СЊ Р·Р°РєР°Р· (С‡РµСЂРЅРѕРІРёРє)</h2>

        <div style={{ display: "grid", gridTemplateColumns: "180px 2fr 1fr 1fr", gap: 12, marginTop: 12 }}>
          <div>
            <div style={{ fontSize: 12, color: "#666" }}>Р”Р°С‚Р°</div>
            <input type="date" value={orderDate} onChange={(e) => setOrderDate(e.target.value)} style={{ width: "100%", padding: 10, border: "1px solid #ccc", borderRadius: 10 }} />
          </div>

          <div>
            <div style={{ fontSize: 12, color: "#666" }}>РљРѕРјРјРµРЅС‚Р°СЂРёР№</div>
            <input value={comment} onChange={(e) => setComment(e.target.value)} placeholder="РїРѕ Р¶РµР»Р°РЅРёСЋ" style={{ width: "100%", padding: 10, border: "1px solid #ccc", borderRadius: 10 }} />
          </div>

          <div>
            <div style={{ fontSize: 12, color: "#666" }}>РЁРёСЂРёРЅР°, Рј</div>
            <input type="number" value={widthM} onChange={(e) => setWidthM(Number(e.target.value))} style={{ width: "100%", padding: 10, border: "1px solid #ccc", borderRadius: 10 }} />
          </div>

          <div>
            <div style={{ fontSize: 12, color: "#666" }}>Р’С‹СЃРѕС‚Р°, Рј</div>
            <input type="number" value={heightM} onChange={(e) => setHeightM(Number(e.target.value))} style={{ width: "100%", padding: 10, border: "1px solid #ccc", borderRadius: 10 }} />
          </div>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr 1fr", gap: 12, marginTop: 12 }}>
          <div>
            <div style={{ fontSize: 12, color: "#666" }}>РР·РґРµР»РёРµ</div>
            <input value={productName} onChange={(e) => setProductName(e.target.value)} style={{ width: "100%", padding: 10, border: "1px solid #ccc", borderRadius: 10 }} />
          </div>

          <div>
            <div style={{ fontSize: 12, color: "#666" }}>РљРѕР»-РІРѕ</div>
            <input type="number" value={qty} onChange={(e) => setQty(Number(e.target.value))} style={{ width: "100%", padding: 10, border: "1px solid #ccc", borderRadius: 10 }} />
          </div>

          <div style={{ alignSelf: "end", color: "#666", fontSize: 12 }}>
            РџР»РѕС‰Р°РґСЊ 1 С€С‚: {(widthM * heightM).toFixed(3)} РјВІ
          </div>
        </div>

        <button
          onClick={createOrder}
          disabled={loading}
          style={{
            marginTop: 12,
            padding: "10px 14px",
            borderRadius: 10,
            border: "1px solid #111",
            background: "#111",
            color: "white",
            cursor: "pointer",
          }}
        >
          РЎРѕР·РґР°С‚СЊ Р·Р°РєР°Р·
        </button>

        {createdOrderId && (
          <div style={{ marginTop: 10, color: "#111" }}>
            РЎРѕР·РґР°РЅ Р·Р°РєР°Р·: <b>ID {createdOrderId}</b>
          </div>
        )}
      </div>

      <div style={{ marginTop: 14, padding: 16, border: "1px solid #ddd", borderRadius: 12 }}>
        <h2 style={{ fontSize: 18, fontWeight: 600 }}>РџСЂРѕРІРµСЃС‚Рё Р·Р°РєР°Р· (FIFO + С‚СЂСѓРґ + С‡РµСЂРЅРёР»Р°)</h2>

        <div style={{ display: "grid", gridTemplateColumns: "220px 1fr 1fr", gap: 12, marginTop: 12 }}>
          <div>
            <div style={{ fontSize: 12, color: "#666" }}>Order ID</div>
            <input type="number" value={postOrderId} onChange={(e) => setPostOrderId(Number(e.target.value))} style={{ width: "100%", padding: 10, border: "1px solid #ccc", borderRadius: 10 }} />
          </div>

          <div>
            <div style={{ fontSize: 12, color: "#666" }}>РњРёРЅСѓС‚ (СЂР°Р±РѕС‚Р°)</div>
            <input type="number" value={minutes} onChange={(e) => setMinutes(Number(e.target.value))} style={{ width: "100%", padding: 10, border: "1px solid #ccc", borderRadius: 10 }} />
          </div>

          <div>
            <div style={{ fontSize: 12, color: "#666" }}>РЎС‚Р°РІРєР° в‚Ѕ/С‡Р°СЃ</div>
            <input type="number" value={rateHour} onChange={(e) => setRateHour(Number(e.target.value))} style={{ width: "100%", padding: 10, border: "1px solid #ccc", borderRadius: 10 }} />
          </div>
        </div>

        <div style={{ marginTop: 12 }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <h3 style={{ margin: 0 }}>РЎРїРёСЃР°РЅРёРµ РјР°С‚РµСЂРёР°Р»РѕРІ</h3>
            <button onClick={addConsumption} style={{ padding: "6px 10px", borderRadius: 10, border: "1px solid #ccc" }}>+ СЃС‚СЂРѕРєР°</button>
          </div>

          <div style={{ marginTop: 10, border: "1px solid #eee", borderRadius: 12, overflow: "hidden" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ background: "#f5f5f5" }}>
                  <th style={{ textAlign: "left", padding: 10, borderBottom: "1px solid #ddd" }}>РњР°С‚РµСЂРёР°Р»</th>
                  <th style={{ textAlign: "left", padding: 10, borderBottom: "1px solid #ddd" }}>РљРѕР»-РІРѕ</th>
                  <th style={{ textAlign: "left", padding: 10, borderBottom: "1px solid #ddd" }}>Р•Рґ.</th>
                  <th style={{ textAlign: "left", padding: 10, borderBottom: "1px solid #ddd" }}></th>
                </tr>
              </thead>
              <tbody>
                {consumption.map((l, idx) => (
                  <tr key={idx}>
                    <td style={{ padding: 10, borderBottom: "1px solid #eee" }}>
                      <select
                        value={l.material_id}
                        onChange={(e) => updateConsumption(idx, { material_id: Number(e.target.value) })}
                        style={{ width: "100%", padding: 8, border: "1px solid #ccc", borderRadius: 10 }}
                      >
                        <option value={0}>вЂ” РІС‹Р±СЂР°С‚СЊ вЂ”</option>
                        {materials.map(m => (
                          <option key={m.id} value={m.id}>
                            {m.id} вЂ” {m.name} ({m.base_uom})
                          </option>
                        ))}
                      </select>
                    </td>

                    <td style={{ padding: 10, borderBottom: "1px solid #eee" }}>
                      <input
                        type="number"
                        value={l.qty}
                        onChange={(e) => updateConsumption(idx, { qty: Number(e.target.value) })}
                        style={{ width: "100%", padding: 8, border: "1px solid #ccc", borderRadius: 10 }}
                      />
                    </td>

                    <td style={{ padding: 10, borderBottom: "1px solid #eee" }}>
                      <input
                        value={l.uom}
                        onChange={(e) => updateConsumption(idx, { uom: e.target.value })}
                        style={{ width: "100%", padding: 8, border: "1px solid #ccc", borderRadius: 10 }}
                      />
                    </td>

                    <td style={{ padding: 10, borderBottom: "1px solid #eee" }}>
                      <button onClick={() => removeConsumption(idx)} style={{ padding: "8px 10px", borderRadius: 10, border: "1px solid #ccc" }}>
                        РЈРґР°Р»РёС‚СЊ
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div style={{ marginTop: 12, display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr 1fr", gap: 12 }}>
          <div>
            <div style={{ fontSize: 12, color: "#666" }}>C РјР»</div>
            <input type="number" value={cMl} onChange={(e) => setCMl(Number(e.target.value))} style={{ width: "100%", padding: 10, border: "1px solid #ccc", borderRadius: 10 }} />
          </div>
          <div>
            <div style={{ fontSize: 12, color: "#666" }}>M РјР»</div>
            <input type="number" value={mMl} onChange={(e) => setMMl(Number(e.target.value))} style={{ width: "100%", padding: 10, border: "1px solid #ccc", borderRadius: 10 }} />
          </div>
          <div>
            <div style={{ fontSize: 12, color: "#666" }}>Y РјР»</div>
            <input type="number" value={yMl} onChange={(e) => setYMl(Number(e.target.value))} style={{ width: "100%", padding: 10, border: "1px solid #ccc", borderRadius: 10 }} />
          </div>
          <div>
            <div style={{ fontSize: 12, color: "#666" }}>K РјР»</div>
            <input type="number" value={kMl} onChange={(e) => setKMl(Number(e.target.value))} style={{ width: "100%", padding: 10, border: "1px solid #ccc", borderRadius: 10 }} />
          </div>
          <div>
            <div style={{ fontSize: 12, color: "#666" }}>Р¦РµРЅР° в‚Ѕ/РјР»</div>
            <input type="number" value={inkPricePerMl} onChange={(e) => setInkPricePerMl(Number(e.target.value))} style={{ width: "100%", padding: 10, border: "1px solid #ccc", borderRadius: 10 }} />
          </div>
        </div>

        <button
          onClick={postOrder}
          disabled={loading}
          style={{
            marginTop: 12,
            padding: "10px 14px",
            borderRadius: 10,
            border: "1px solid #111",
            background: "#111",
            color: "white",
            cursor: "pointer",
          }}
        >
          РџСЂРѕРІРµСЃС‚Рё Р·Р°РєР°Р·
        </button>

        {err && <div style={{ marginTop: 10, color: "crimson" }}>{err}</div>}
        {ok && <div style={{ marginTop: 10, color: "green" }}>{ok}</div>}

        {postResult && (
          <pre style={{ marginTop: 12, padding: 12, background: "#f7f7f7", borderRadius: 12, overflow: "auto" }}>
            {JSON.stringify(postResult, null, 2)}
          </pre>
        )}
      </div>
    </div>
  );
}

