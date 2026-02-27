"use client";

import { useEffect, useState } from "react";

export default function Home() {
  const [materials, setMaterials] = useState<any[]>([]);

  useEffect(() => {
    fetch("http://localhost:8000/materials")
      .then(r => r.json())
      .then(data => setMaterials(data));
  }, []);

  return (
    <div style={{ padding: 40 }}>
      <h1>ERP СЃРёСЃС‚РµРјР°</h1>

      <h2>РњР°С‚РµСЂРёР°Р»С‹:</h2>

      {materials.map(m => (
        <div key={m.id}>
          {m.id} вЂ” {m.name}
        </div>
      ))}
    </div>
  );
}

