"use client";

import { useState } from "react";

const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export default function SalesPage() {
  const [orderId, setOrderId] = useState(3);
  const [price, setPrice] = useState(1000);
  const [marketplace, setMarketplace] = useState("WB");
  const [commission, setCommission] = useState(600);

  const [result, setResult] = useState<any>(null);
  const [err, setErr] = useState("");

  async function createSale() {
    setErr("");
    const r = await fetch(`${API}/sales`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        sale_date: new Date().toISOString().slice(0,10),
        order_id: orderId,
        marketplace: marketplace,
        gross_price: price,
        charges: [
          { charge_type: "COMMISSION", amount: commission }
        ]
      })
    });

    if (!r.ok) {
      setErr(await r.text());
      return;
    }

    await r.json();

    // РїРѕР»СѓС‡РёС‚СЊ СЋРЅРёС‚ СЌРєРѕРЅРѕРјРёРєСѓ
    const r2 = await fetch(`${API}/orders/${orderId}/unit_economics`);
    const data = await r2.json();
    setResult(data);
  }

  return (
    <div style={{ padding:40 }}>
      <h1>РџСЂРѕРґР°Р¶Р°</h1>

      <div>Order ID</div>
      <input value={orderId} onChange={e=>setOrderId(Number(e.target.value))}/>

      <div>Р¦РµРЅР° РїСЂРѕРґР°Р¶Рё</div>
      <input value={price} onChange={e=>setPrice(Number(e.target.value))}/>

      <div>РљРѕРјРёСЃСЃРёСЏ РњРџ</div>
      <input value={commission} onChange={e=>setCommission(Number(e.target.value))}/>

      <button onClick={createSale}>РЎРѕР·РґР°С‚СЊ РїСЂРѕРґР°Р¶Сѓ</button>

      {err && <div style={{color:"red"}}>{err}</div>}

      {result && (
        <pre>
{JSON.stringify(result,null,2)}
        </pre>
      )}
    </div>
  );
}

