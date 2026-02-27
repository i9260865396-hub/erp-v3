"use client";

import * as React from "react";
import { api, API_URL } from "@/lib/api";
import { useToast } from "@/components/ui/Toast";

type Connection = {
  id: string;
  marketplace: string;
  name: string;
  client_id: string | null;
  api_key: string | null;
  created_at?: string;
};

type YMarketCampaign = {
  id: number;
  domain?: string | null;
  business_id?: number | null;
  business_name?: string | null;
  placement_type?: string | null;
  api_availability?: string | null;
};

type YMarketOrderItem = {
  offer_id?: string | null;
  shop_sku?: string | null;
  market_sku?: string | null;
  name?: string | null;
  quantity?: number | null;
  price?: number | null;
  line_total?: number | null;
};

type YMarketOrder = {
  id: string;
  connection_id: string;
  order_id: number;
  status?: string | null;
  substatus?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
  shipment_date?: string | null;
  buyer_total?: number | null;
  items_total?: number | null;
  currency?: string | null;
  imported_at?: string | null;
  items: YMarketOrderItem[];
};

type YMarketReport = {
  id: string;
  connection_id: string;
  report_id: string;
  report_type: string;
  status?: string | null;
  file_url?: string | null;
  date_from?: string | null;
  date_to?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
};

function fmtMoney(v: any) {
  if (v == null || v === "") return "";
  const n = Number(v);
  if (Number.isNaN(n)) return String(v);
  return n.toFixed(2);
}

export default function YandexMarketPage() {
  const { push } = useToast();


const [apiOk, setApiOk] = React.useState<boolean | null>(null);
const [apiUtc, setApiUtc] = React.useState<string>("");
const [apiErr, setApiErr] = React.useState<string>("");

  const [connections, setConnections] = React.useState<Connection[]>([]);
  const [selectedId, setSelectedId] = React.useState<string>("");

  const [name, setName] = React.useState("YMarket");
  const [campaignId, setCampaignId] = React.useState("");
  const [apiKey, setApiKey] = React.useState("");

  const [tab, setTab] = React.useState<"orders" | "reports" | "export">("orders");

  const [dateFrom, setDateFrom] = React.useState<string>(() => {
    const d = new Date();
    d.setDate(d.getDate() - 14);
    return d.toISOString().slice(0, 10);
  });
  const [dateTo, setDateTo] = React.useState<string>(() => new Date().toISOString().slice(0, 10));

  const [orders, setOrders] = React.useState<YMarketOrder[]>([]);
  const [loadingOrders, setLoadingOrders] = React.useState(false);

  const [campaigns, setCampaigns] = React.useState<YMarketCampaign[]>([]);
  const [loadingCampaigns, setLoadingCampaigns] = React.useState(false);

  const [reports, setReports] = React.useState<YMarketReport[]>([]);
  const [loadingReports, setLoadingReports] = React.useState(false);
  const [placementPrograms, setPlacementPrograms] = React.useState("FBS");
  // Partner API expects `format`: FILE (XLSX), CSV (ZIP), JSON (ZIP)
  const [reportFormat, setReportFormat] = React.useState<"FILE" | "CSV" | "JSON">("FILE");

  const selected = React.useMemo(
    () => connections.find((c) => c.id === selectedId) || null,
    [connections, selectedId]
  );

  async function refreshConnections() {
    try {
      const rows = await api<Connection[]>(`/integrations/marketplaces/connections?marketplace=ymarket`);
      setConnections(rows || []);
      if (!selectedId && rows?.[0]?.id) setSelectedId(rows[0].id);
    } catch (e: any) {
      push(e?.message || "Не удалось загрузить подключения", "err");
    }
  }

  React.useEffect(() => {
    refreshConnections();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
React.useEffect(() => {
    (async () => {
      try {
        const res = await api<{ status: string; utc?: string }>(`/health`);
        setApiOk(true);
        setApiUtc((res as any)?.utc || "");
        setApiErr("");
      } catch (e: any) {
        setApiOk(false);
        setApiUtc("");
        setApiErr(e?.message || String(e));
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);


  async function createConnection() {
    try {
      if (!campaignId.trim()) throw new Error("Укажи Campaign ID (client_id)");
      if (!apiKey.trim()) throw new Error("Укажи Api-Key");

      await api(`/integrations/marketplaces/connections`, {
        method: "POST",
        body: {
          marketplace: "ymarket",
          name: name.trim() || "YMarket",
          client_id: campaignId.trim(),
          api_key: apiKey.trim(),
        },
      });
      push("Подключение Yandex Market сохранено", "ok");
      setApiKey("");
      await refreshConnections();
    } catch (e: any) {
      push(e?.message || "Не удалось сохранить подключение", "err");
    }
  }

  async function loadOrders() {
    if (!selected) return;
    try {
      const rows = await api<YMarketOrder[]>(
        `/integrations/ymarket/orders?connection_id=${encodeURIComponent(selected.id)}&date_from=${dateFrom}&date_to=${dateTo}&limit=500`
      );
      setOrders(rows || []);
    } catch (e: any) {
      push(e?.message || "Не удалось загрузить заказы", "err");
    }
  }

  async function fetchOrders() {
    if (!selected) return push("Выбери подключение", "err");
    setLoadingOrders(true);
    try {
      await api(`/integrations/ymarket/orders/fetch`, {
        method: "POST",
        body: {
          connection_id: selected.id,
          date_from: dateFrom,
          date_to: dateTo,
          limit: 50,
          fake: false,
        },
      });
      push("Синхронизация заказов завершена", "ok");
      await loadOrders();
    } catch (e: any) {
      push(e?.message || "Ошибка синхронизации", "err");
    } finally {
      setLoadingOrders(false);
    }
  }

  async function loadCampaigns() {
    if (!selected) return push("Выбери подключение", "err");
    setLoadingCampaigns(true);
    try {
      const rows = await api<YMarketCampaign[]>(
        `/integrations/ymarket/campaigns?connection_id=${encodeURIComponent(selected.id)}`
      );
      setCampaigns(rows || []);
      push("Кампании загружены", "ok");
    } catch (e: any) {
      push(e?.message || "Не удалось загрузить кампании", "err");
    } finally {
      setLoadingCampaigns(false);
    }
  }

  async function refreshReports() {
    if (!selected) return;
    setLoadingReports(true);
    try {
      const rows = await api<YMarketReport[]>(
        `/integrations/ymarket/reports?connection_id=${encodeURIComponent(selected.id)}`
      );
      setReports(rows || []);
    } catch (e: any) {
      push(e?.message || "Не удалось загрузить отчёты", "err");
    } finally {
      setLoadingReports(false);
    }
  }

  async function generateUnitedNetting() {
    if (!selected) return push("Выбери подключение", "err");
    try {
      await api(`/integrations/ymarket/reports/united-netting/generate`, {
        method: "POST",
        body: {
          connection_id: selected.id,
          date_from: dateFrom,
          date_to: dateTo,
          placement_programs: placementPrograms
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean),
          format: reportFormat,
          language: "RU",
        },
      });
      push("Отчёт запрошен. Обнови статус через 1–2 минуты.", "ok");
      await refreshReports();
    } catch (e: any) {
      push(e?.message || "Не удалось запросить отчёт", "err");
    }
  }

  async function updateReportInfo(reportId: string) {
    if (!selected) return;
    try {
      await api(
        `/integrations/ymarket/reports/info?connection_id=${encodeURIComponent(selected.id)}&report_id=${encodeURIComponent(reportId)}`
      );
      await refreshReports();
      push("Статус отчёта обновлён", "ok");
    } catch (e: any) {
      push(e?.message || "Не удалось обновить статус", "err");
    }
  }

  function downloadReport(reportId: string) {
    if (!selected) return push("Выбери подключение", "err");
    const url = `${API_URL}/integrations/ymarket/reports/download?connection_id=${encodeURIComponent(selected.id)}&report_id=${encodeURIComponent(reportId)}`;
    window.open(url, "_blank");
  }

  function downloadUTPackage(reportId?: string) {
    if (!selected) return push("Выбери подключение", "err");
    const base = `${API_URL}/integrations/ymarket/ut_package?connection_id=${encodeURIComponent(selected.id)}&date_from=${dateFrom}&date_to=${dateTo}`;
    const url = reportId ? `${base}&report_id=${encodeURIComponent(reportId)}` : base;
    window.open(url, "_blank");
  }

  React.useEffect(() => {
    if (tab === "orders") loadOrders();
    if (tab === "reports") refreshReports();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedId, tab]);

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-bold">Маркетплейсы</h1>
          <p className="text-sm text-zinc-500">Интеграции → Яндекс Маркет (FBS)</p>
        </div>
        <div className="flex items-center gap-2">
          <span
            title={apiOk ? (apiUtc ? `UTC ${apiUtc}` : "OK") : apiErr || "API error"}
            className={[
              "text-xs rounded-md border px-2 py-1",
              apiOk === null
                ? "border-zinc-200 bg-zinc-50 text-zinc-600"
                : apiOk
                ? "border-emerald-200 bg-emerald-50 text-emerald-700"
                : "border-rose-200 bg-rose-50 text-rose-700",
            ].join(" ")}
          >
            API {apiOk === null ? "…" : apiOk ? "OK" : "ERR"} • {API_URL}
          </span>
          <a href="/marketplaces" className="px-3 py-2 rounded-md border text-sm hover:bg-zinc-50" title="Открыть Ozon">
            Ozon
          </a>
          <a href="/marketplaces/yandex" className="px-3 py-2 rounded-md border bg-zinc-900 text-white text-sm hover:bg-zinc-800" title="Ты сейчас здесь">
            Яндекс Маркет
          </a>
        </div>
      </div>

      <div className="rounded-xl border bg-white p-4 space-y-4">
        <div className="flex items-center justify-between gap-3">
          <div>
            <div className="font-semibold">Подключение (Api-Key)</div>
            <div className="text-sm text-zinc-500">
              client_id для Яндекс Маркета = Campaign ID (ID магазина). Api-Key берёшь в кабинете продавца.
            </div>
          </div>
          <button onClick={refreshConnections} className="px-3 py-2 rounded-md border text-sm hover:bg-zinc-50">
            Обновить
          </button>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          <div>
            <label className="text-xs text-zinc-500">Активное подключение</label>
            <select className="w-full border rounded-md p-2" value={selectedId} onChange={(e) => setSelectedId(e.target.value)}>
              <option value="">— выбери —</option>
              {connections.map((c) => (
                <option key={c.id} value={c.id}>
                  {c.name} (campaign {c.client_id})
                </option>
              ))}
            </select>
          </div>

          <div className="flex items-end gap-2">
            <button onClick={loadCampaigns} disabled={!selected || loadingCampaigns} className="px-3 py-2 rounded-md border text-sm hover:bg-zinc-50 disabled:opacity-50">
              {loadingCampaigns ? "Загрузка..." : "Показать кампании"}
            </button>
          </div>

          <div className="flex items-end justify-end">
            <div className="text-xs text-zinc-500">
              Если «кампании» не грузятся — токен неверный или нет доступа к магазину.
            </div>
          </div>
        </div>

        {campaigns.length > 0 && (
          <div className="rounded-lg bg-zinc-50 p-3">
            <div className="text-sm font-semibold mb-2">Кампании, доступные по токену</div>
            <div className="overflow-auto">
              <table className="min-w-[720px] w-full text-sm">
                <thead className="text-zinc-500">
                  <tr>
                    <th className="text-left py-1 pr-2">Campaign ID</th>
                    <th className="text-left py-1 pr-2">Домен</th>
                    <th className="text-left py-1 pr-2">Business ID</th>
                    <th className="text-left py-1 pr-2">Кабинет</th>
                    <th className="text-left py-1 pr-2">Размещение</th>
                  </tr>
                </thead>
                <tbody>
                  {campaigns.map((c) => (
                    <tr key={c.id} className="border-t">
                      <td className="py-1 pr-2">{c.id}</td>
                      <td className="py-1 pr-2">{c.domain || ""}</td>
                      <td className="py-1 pr-2">{c.business_id ?? ""}</td>
                      <td className="py-1 pr-2">{c.business_name || ""}</td>
                      <td className="py-1 pr-2">{c.placement_type || ""}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        <div className="border-t pt-4">
          <div className="font-semibold mb-2">Добавить новое подключение</div>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
            <div>
              <label className="text-xs text-zinc-500">Название</label>
              <input className="w-full border rounded-md p-2" value={name} onChange={(e) => setName(e.target.value)} />
            </div>
            <div>
              <label className="text-xs text-zinc-500">Campaign ID (client_id)</label>
              <input className="w-full border rounded-md p-2" value={campaignId} onChange={(e) => setCampaignId(e.target.value)} placeholder="например: 12345678" />
            </div>
            <div className="md:col-span-2">
              
<label className="text-xs text-zinc-500">Api-Key</label>
              <input className="w-full border rounded-md p-2" value={apiKey} onChange={(e) => setApiKey(e.target.value)} placeholder="вставь Api-Key" />
              <div className="mt-1 text-xs text-zinc-500">
                Совет: делай ключ с <span className="font-medium">минимальными</span> правами (заказы/отчёты/финансы).
                «Полное управление кабинетом» и «Все методы» лучше не включать, если это не нужно.
              </div>
            </div>
          </div>
          <div className="mt-3 flex justify-end">
            <button onClick={createConnection} className="px-4 py-2 rounded-md bg-zinc-900 text-white text-sm hover:bg-zinc-800">
              Сохранить подключение
            </button>
          </div>
        </div>
      </div>

      <div className="rounded-xl border bg-white p-4 space-y-3">
        <div className="font-semibold">Период</div>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-3 items-end">
          <div>
            <label className="text-xs text-zinc-500">Дата от</label>
            <input className="w-full border rounded-md p-2" type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} />
          </div>
          <div>
            <label className="text-xs text-zinc-500">Дата до</label>
            <input className="w-full border rounded-md p-2" type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} />
          </div>

          <div className="md:col-span-2 flex gap-2">
            <button onClick={() => setTab("orders")} className={`px-3 py-2 rounded-md border text-sm ${tab === "orders" ? "bg-zinc-900 text-white" : "hover:bg-zinc-50"}`}>
              Заказы
            </button>
            <button onClick={() => setTab("reports")} className={`px-3 py-2 rounded-md border text-sm ${tab === "reports" ? "bg-zinc-900 text-white" : "hover:bg-zinc-50"}`}>
              Отчёты
            </button>
            <button onClick={() => setTab("export")} className={`px-3 py-2 rounded-md border text-sm ${tab === "export" ? "bg-zinc-900 text-white" : "hover:bg-zinc-50"}`}>
              Экспорт в 1С (UT)
            </button>
          </div>
        </div>
      </div>

      {tab === "orders" && (
        <div className="rounded-xl border bg-white p-4 space-y-4">
          <div className="flex items-center justify-between">
            <div>
              <div className="font-semibold">Заказы (GET v2/campaigns/{selected?.client_id ?? "<campaignId>"}/orders)</div>
              <div className="text-sm text-zinc-500">Ограничение API: максимум 30 дней за запрос; мы режем период на окна автоматически.</div>
            </div>
            <div className="flex gap-2">
              <button onClick={fetchOrders} disabled={!selected || loadingOrders} className="px-4 py-2 rounded-md bg-zinc-900 text-white text-sm hover:bg-zinc-800 disabled:opacity-50">
                {loadingOrders ? "Синхронизация..." : "Синхронизировать"}
              </button>
              <button onClick={loadOrders} disabled={!selected} className="px-3 py-2 rounded-md border text-sm hover:bg-zinc-50 disabled:opacity-50">
                Обновить список
              </button>
            </div>
          </div>

          <div className="overflow-auto">
            <table className="min-w-[980px] w-full text-sm">
              <thead className="text-zinc-500">
                <tr>
                  <th className="text-left py-2 pr-2">Order ID</th>
                  <th className="text-left py-2 pr-2">Статус</th>
                  <th className="text-left py-2 pr-2">Создан</th>
                  <th className="text-left py-2 pr-2">Отгрузка</th>
                  <th className="text-left py-2 pr-2">Сумма покупателя</th>
                  <th className="text-left py-2 pr-2">Сумма товаров</th>
                  <th className="text-left py-2 pr-2">Позиций</th>
                </tr>
              </thead>
              <tbody>
                {orders.map((o) => (
                  <tr key={o.id} className="border-t">
                    <td className="py-2 pr-2">{o.order_id}</td>
                    <td className="py-2 pr-2">
                      {o.status || ""} {o.substatus ? <span className="text-zinc-500">/{o.substatus}</span> : null}
                    </td>
                    <td className="py-2 pr-2">{o.created_at ? String(o.created_at).slice(0, 19).replace("T", " ") : ""}</td>
                    <td className="py-2 pr-2">{o.shipment_date || ""}</td>
                    <td className="py-2 pr-2">{fmtMoney(o.buyer_total)}</td>
                    <td className="py-2 pr-2">{fmtMoney(o.items_total)}</td>
                    <td className="py-2 pr-2">{o.items?.length || 0}</td>
                  </tr>
                ))}
                {orders.length === 0 && (
                  <tr>
                    <td className="py-6 text-center text-zinc-500" colSpan={7}>
                      Нет данных. Нажми «Синхронизировать».
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {tab === "reports" && (
        <div className="rounded-xl border bg-white p-4 space-y-4">
          <div className="flex items-center justify-between gap-3">
            <div>
              <div className="font-semibold">Отчёт United Netting (выплаты/взаиморасчёты)</div>
              <div className="text-sm text-zinc-500">Запрашиваем отчёт по периоду. Когда готов — появится file_url и можно скачать.</div>
            </div>
            <button onClick={refreshReports} disabled={!selected || loadingReports} className="px-3 py-2 rounded-md border text-sm hover:bg-zinc-50 disabled:opacity-50">
              {loadingReports ? "Загрузка..." : "Обновить список"}
            </button>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
            <div>
              <label className="text-xs text-zinc-500">placementPrograms</label>
              <input className="w-full border rounded-md p-2" value={placementPrograms} onChange={(e) => setPlacementPrograms(e.target.value)} placeholder="FBS" />
              <div className="text-xs text-zinc-500 mt-1">Можно через запятую: FBS,FBY,FBO</div>
            </div>
            <div>
              <label className="text-xs text-zinc-500">Формат</label>
              <select className="w-full border rounded-md p-2" value={reportFormat} onChange={(e) => setReportFormat(e.target.value as any)}>
                <option value="FILE">XLSX</option>
                <option value="CSV">CSV (ZIP)</option>
                <option value="JSON">JSON (ZIP)</option>
              </select>
            </div>
            <div className="md:col-span-2 flex items-end justify-end">
              <button onClick={generateUnitedNetting} disabled={!selected} className="px-4 py-2 rounded-md bg-zinc-900 text-white text-sm hover:bg-zinc-800 disabled:opacity-50">
                Сформировать отчёт
              </button>
            </div>
          </div>

          <div className="overflow-auto">
            <table className="min-w-[980px] w-full text-sm">
              <thead className="text-zinc-500">
                <tr>
                  <th className="text-left py-2 pr-2">Report ID</th>
                  <th className="text-left py-2 pr-2">Период</th>
                  <th className="text-left py-2 pr-2">Статус</th>
                  <th className="text-left py-2 pr-2">Файл</th>
                  <th className="text-left py-2 pr-2">Действия</th>
                </tr>
              </thead>
              <tbody>
                {reports.map((r) => (
                  <tr key={r.id} className="border-t">
                    <td className="py-2 pr-2 font-mono text-xs">{r.report_id}</td>
                    <td className="py-2 pr-2">
                      {r.date_from || ""} — {r.date_to || ""}
                    </td>
                    <td className="py-2 pr-2">{r.status || ""}</td>
                    <td className="py-2 pr-2">{r.file_url ? <span className="text-green-700">готов</span> : <span className="text-zinc-500">нет</span>}</td>
                    <td className="py-2 pr-2">
                      <div className="flex gap-2">
                        <button onClick={() => updateReportInfo(r.report_id)} className="px-3 py-1 rounded-md border text-xs hover:bg-zinc-50">
                          Обновить статус
                        </button>
                        <button onClick={() => downloadReport(r.report_id)} disabled={!r.file_url} className="px-3 py-1 rounded-md border text-xs hover:bg-zinc-50 disabled:opacity-50">
                          Скачать
                        </button>
                        <button onClick={() => downloadUTPackage(r.report_id)} className="px-3 py-1 rounded-md border text-xs hover:bg-zinc-50" title="Скачать UT пакет и вложить отчёт, если он готов">
                          UT+отчёт
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
                {reports.length === 0 && (
                  <tr>
                    <td className="py-6 text-center text-zinc-500" colSpan={5}>
                      Отчётов пока нет. Нажми «Сформировать отчёт».
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {tab === "export" && (
        <div className="rounded-xl border bg-white p-4 space-y-3">
          <div className="font-semibold">Экспорт для 1С:УТ (ZIP)</div>
          <div className="text-sm text-zinc-500">Скачает ZIP с CSV: заказы, товары, метаданные отчётов. Если хочешь — выбери Report ID и он тоже вложится (если готов).</div>

          <div className="flex flex-wrap gap-2">
            <button onClick={() => downloadUTPackage()} disabled={!selected} className="px-4 py-2 rounded-md bg-zinc-900 text-white text-sm hover:bg-zinc-800 disabled:opacity-50">
              Скачать UT пакет
            </button>
          </div>

          {reports.length > 0 && (
            <div className="mt-2">
              <div className="text-xs text-zinc-500 mb-1">Вложить отчёт:</div>
              <div className="flex flex-wrap gap-2">
                {reports.slice(0, 6).map((r) => (
                  <button key={r.id} onClick={() => downloadUTPackage(r.report_id)} className="px-3 py-2 rounded-md border text-sm hover:bg-zinc-50">
                    {r.report_id.slice(0, 12)}…
                  </button>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
