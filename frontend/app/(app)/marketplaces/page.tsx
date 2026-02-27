"use client";

import { useEffect, useMemo, useState, type ReactNode } from "react";

import KpiCard from "@/components/kpi/KpiCard";
import DataTable from "@/components/table/DataTable";
import Button from "@/components/ui/Button";
import Input from "@/components/ui/Input";
import Select from "@/components/ui/Select";
import Modal from "@/components/ui/Modal";
import { useToast } from "@/components/ui/Toast";
import Sparkline from "@/components/charts/Sparkline";
import BarMini from "@/components/charts/BarMini";

import { api, API_URL } from "@/lib/api";

import type {
  MarketplaceConnection,
  MoneyAccount,
  OzonFbsFetchResult,
  OzonFetchResult,
  OzonPayoutAutoConfirmResult,
  OzonPayoutReconRow,
  OzonPeriodStatus,
  OzonPosting,
  OzonPostingsPage,
  OzonSummary,
  OzonSyncResult,
  OzonTransaction,
  YMarketCampaign,
  YMarketOrder,
  YMarketReport,
  WbFetchResult,
  WbOrderLine,
  WbPing,
  WbSaleLine,
  FbsBuildSummary,
  FbsBuildDetail,
  FbsBuildCreate,
  FbsBuildPatch,
} from "@/types/api";

const API = API_URL;

type Mp = "ozon" | "ymarket" | "wb";

type Section =
  | "dashboard"
  | "connections"
  | "fbs_builds"
  | "fbs_build_view"
  | "ozon_finance"
  | "ozon_orders"
  | "ozon_reconcile"
  | "ozon_period"
  | "ozon_export"
  | "ym_orders"
  | "ym_reports"
  | "ym_campaigns"
  | "ym_export"
  | "wb_orders"
  | "wb_sales";

function todayISO() {
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

function minusDaysISO(days: number) {
  const d = new Date();
  d.setDate(d.getDate() - days);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

function fmtRub(n: number | null | undefined) {
  const v = Number(n || 0);
  return v.toLocaleString("ru-RU", { style: "currency", currency: "RUB" });
}

function fmtNum(n: number | null | undefined) {
  const v = Number(n || 0);
  return v.toLocaleString("ru-RU");
}

function Badge({
  children,
  tone = "slate",
}: {
  children: ReactNode;
  tone?: "slate" | "emerald" | "amber" | "rose" | "blue" | "violet";
}) {
  const map: Record<string, string> = {
    slate: "bg-slate-100 text-slate-700",
    emerald: "bg-emerald-50 text-emerald-700",
    amber: "bg-amber-50 text-amber-700",
    rose: "bg-rose-50 text-rose-700",
    blue: "bg-blue-50 text-blue-700",
    violet: "bg-violet-50 text-violet-700",
  };
  return (
    <span className={`inline-flex items-center rounded-md px-2 py-1 text-xs font-medium ${map[tone]}`}>
      {children}
    </span>
  );
}

function mpMeta(mp: Mp) {
  if (mp === "ozon")
    return {
      title: "Ozon",
      pill: "bg-violet-600 text-white hover:bg-violet-500",
      pillOff: "bg-violet-50 text-violet-700 hover:bg-violet-100",
      badge: "violet" as const,
    };
  if (mp === "ymarket")
    return {
      title: "Яндекс Маркет",
      pill: "bg-amber-500 text-white hover:bg-amber-400",
      pillOff: "bg-amber-50 text-amber-700 hover:bg-amber-100",
      badge: "amber" as const,
    };
  return {
    title: "Wildberries",
    pill: "bg-rose-600 text-white hover:bg-rose-500",
    pillOff: "bg-rose-50 text-rose-700 hover:bg-rose-100",
    badge: "rose" as const,
  };
}

function defaultSection(mp: Mp): Section {
  if (mp === "ozon") return "ozon_finance";
  if (mp === "ymarket") return "ym_orders";
  return "wb_orders";
}

export default function MarketplacesPage() {
  const toastCtx = useToast();
  const toast = (text: string, kind: "success" | "error" = "success") =>
    toastCtx.push(text, kind === "success" ? "ok" : "err");

  const [apiOk, setApiOk] = useState<boolean | null>(null);
  const [apiUtc, setApiUtc] = useState<string>("");
  const [apiErr, setApiErr] = useState<string>("");

  useEffect(() => {
    (async () => {
      try {
        const res = await api<{ status: string; utc?: string }>("/health");
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

  const [mp, setMp] = useState<Mp>("ozon");
  const [section, setSection] = useState<Section>("ozon_finance");

  const [connections, setConnections] = useState<MarketplaceConnection[]>([]);
  const [connId, setConnId] = useState<string>("");

  const [dateFrom, setDateFrom] = useState(minusDaysISO(7));
  const [dateTo, setDateTo] = useState(todayISO());

  // ------------------------------------------------------------------
  // FBS Builds (internal batches)
  // ------------------------------------------------------------------

  const [selectedOrderIds, setSelectedOrderIds] = useState<string[]>([]);
  const [builds, setBuilds] = useState<FbsBuildSummary[]>([]);
  const [buildDetail, setBuildDetail] = useState<FbsBuildDetail | null>(null);
  const [buildCreateOpen, setBuildCreateOpen] = useState(false);
  const [buildTitle, setBuildTitle] = useState("");
  const [buildNote, setBuildNote] = useState("");

  // ------------------------------------------------------------------
  // Connections CRUD
  // ------------------------------------------------------------------

  const [connModalOpen, setConnModalOpen] = useState(false);
  const [editingConn, setEditingConn] = useState<MarketplaceConnection | null>(null);
  const [connName, setConnName] = useState("");
  const [connClientId, setConnClientId] = useState("");
  const [connApiKey, setConnApiKey] = useState("");
  const [connNote, setConnNote] = useState("");
  const [connActive, setConnActive] = useState(true);

  async function loadConnections(nextMp?: Mp) {
    const m = nextMp || mp;
    const rows = await api<MarketplaceConnection[]>(
      `/integrations/marketplaces/connections?marketplace=${encodeURIComponent(m)}`,
    );
    setConnections(rows || []);
    const first = (rows || [])[0]?.id || "";
    setConnId((cur) => (cur && (rows || []).some((x) => x.id === cur) ? cur : first));
  }

  useEffect(() => {
    (async () => {
      try {
        setSection(defaultSection(mp));
        setSelectedOrderIds([]);
        setBuildDetail(null);
        await loadConnections(mp);
      } catch (e: any) {
        toast(e?.message || String(e), "error");
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mp]);

  function openCreateConn() {
    setEditingConn(null);
    setConnName("");
    setConnClientId("");
    setConnApiKey("");
    setConnNote("");
    setConnActive(true);
    setConnModalOpen(true);
  }

  function openEditConn(c: MarketplaceConnection) {
    setEditingConn(c);
    setConnName(c.name || "");
    setConnClientId(c.client_id || "");
    setConnApiKey(""); // never show existing key
    setConnNote(c.note || "");
    setConnActive(Boolean(c.is_active));
    setConnModalOpen(true);
  }

  async function saveConn() {
    try {
      const payload: any = {
        marketplace: mp,
        name: connName.trim(),
        client_id: connClientId.trim(),
        note: connNote.trim() || null,
        is_active: connActive,
      };
      if (!editingConn) {
        payload.api_key = connApiKey;
        await api<MarketplaceConnection>("/integrations/marketplaces/connections", {
          method: "POST",
          body: JSON.stringify(payload),
        });
        toast("Подключение создано");
      } else {
        if (connApiKey.trim()) payload.api_key = connApiKey.trim();
        await api<MarketplaceConnection>(`/integrations/marketplaces/connections/${editingConn.id}`, {
          method: "PATCH",
          body: JSON.stringify(payload),
        });
        toast("Подключение обновлено");
      }
      setConnModalOpen(false);
      await loadConnections(mp);
    } catch (e: any) {
      toast(e?.message || String(e), "error");
    }
  }

  async function deleteConn(id: string) {
    if (!confirm("Удалить подключение?")) return;
    try {
      await api<{ ok: boolean }>(`/integrations/marketplaces/connections/${id}`, { method: "DELETE" });
      toast("Удалено");
      await loadConnections(mp);
    } catch (e: any) {
      toast(e?.message || String(e), "error");
    }
  }

  const conn = useMemo(() => connections.find((c) => c.id === connId) || null, [connections, connId]);

  useEffect(() => {
    setSelectedOrderIds([]);
    setBuildDetail(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connId]);

  function toggleSelectedOrder(id: string) {
    const s = String(id || "").trim();
    if (!s) return;
    setSelectedOrderIds((prev) => (prev.includes(s) ? prev.filter((x) => x !== s) : [...prev, s]));
  }

  function clearSelection() {
    setSelectedOrderIds([]);
  }

  async function loadBuilds() {
    if (!connId) return toast("Выбери подключение", "error");
    try {
      const qs = new URLSearchParams({ marketplace: mp, connection_id: connId, limit: "200" });
      const rows = await api<FbsBuildSummary[]>(`/integrations/fbs/builds?${qs.toString()}`);
      setBuilds(rows || []);
    } catch (e: any) {
      toast(e?.message || String(e), "error");
    }
  }

  async function openBuild(buildId: string) {
    try {
      const b = await api<FbsBuildDetail>(`/integrations/fbs/builds/${buildId}`);
      setBuildDetail(b);
      setSection("fbs_build_view");
    } catch (e: any) {
      toast(e?.message || String(e), "error");
    }
  }

  function openCreateBuildModal() {
    if (!connId) return toast("Выбери подключение", "error");
    if (selectedOrderIds.length === 0) return toast("Сначала выбери заказы", "error");
    setBuildTitle("");
    setBuildNote("");
    setBuildCreateOpen(true);
  }

  async function createBuild() {
    if (!connId) return toast("Выбери подключение", "error");
    if (selectedOrderIds.length === 0) return toast("Сначала выбери заказы", "error");
    try {
      const payload: FbsBuildCreate = {
        marketplace: mp,
        connection_id: connId,
        order_ids: selectedOrderIds,
        title: buildTitle.trim() || null,
        note: buildNote.trim() || null,
      };
      const b = await api<FbsBuildDetail>("/integrations/fbs/builds", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      setBuildCreateOpen(false);
      clearSelection();
      setBuildDetail(b);
      setSection("fbs_build_view");
      toast("Сборка создана");
      await loadBuilds();
    } catch (e: any) {
      toast(e?.message || String(e), "error");
    }
  }

  async function patchBuild(p: FbsBuildPatch) {
    if (!buildDetail) return;
    try {
      const b = await api<FbsBuildDetail>(`/integrations/fbs/builds/${buildDetail.id}`, {
        method: "PATCH",
        body: JSON.stringify(p),
      });
      setBuildDetail(b);
      await loadBuilds();
    } catch (e: any) {
      toast(e?.message || String(e), "error");
    }
  }

  function statusTone(status?: string | null): "slate" | "emerald" | "amber" | "rose" | "blue" | "violet" {
    const s = String(status || "").toLowerCase();
    if (s === "draft") return "slate";
    if (s === "picking") return "blue";
    if (s === "packed") return "violet";
    if (s === "shipped") return "emerald";
    if (s === "closed") return "emerald";
    if (s === "cancelled") return "rose";
    return "slate";
  }

  function printBuild(build: FbsBuildDetail) {
    try {
      const w = window.open("", "_blank");
      if (!w) return toast("Блокировщик всплывающих окон", "error");
      const esc = (x: any) => String(x ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
      const rows = (build.items || [])
        .map(
          (it, idx) => `
          <tr>
            <td class="c">${idx + 1}</td>
            <td>${esc(it.sku || it.offer_id || "")}</td>
            <td>${esc(it.name || "")}</td>
            <td class="c"><b>${esc(it.qty_total)}</b></td>
            <td class="c">${esc(it.orders_count)}</td>
          </tr>`,
        )
        .join("");

      const orders = (build.orders || [])
        .map(
          (o) => `
          <tr>
            <td>${esc(o.external_order_id)}</td>
            <td>${esc(o.status || "")}</td>
            <td class="c">${esc(o.qty_total)}</td>
          </tr>`,
        )
        .join("");

      w.document.write(`<!doctype html>
        <html>
        <head>
          <meta charset="utf-8" />
          <title>${esc(build.title)}</title>
          <style>
            body{font-family:Arial,Helvetica,sans-serif;padding:24px;color:#0f172a}
            h1{margin:0 0 6px 0;font-size:20px}
            .meta{color:#475569;font-size:12px;margin-bottom:14px}
            .grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
            .card{border:1px solid #e2e8f0;border-radius:12px;padding:12px}
            table{border-collapse:collapse;width:100%}
            th,td{border-bottom:1px solid #e2e8f0;padding:8px 10px;font-size:12px;vertical-align:top}
            th{background:#f8fafc;text-align:left}
            .c{text-align:center}
            .small{font-size:11px;color:#475569}
            @media print{.noprint{display:none} body{padding:0}}
          </style>
        </head>
        <body>
          <div class="noprint" style="margin-bottom:10px;">
            <button onclick="window.print()">Печать</button>
          </div>
          <h1>${esc(build.title)}</h1>
          <div class="meta">${esc(build.marketplace.toUpperCase())} • статус: ${esc(build.status)} • заказов: ${esc(build.orders_count)} • товаров: ${esc(build.qty_total)}</div>
          <div class="grid">
            <div class="card">
              <div style="font-weight:700;margin-bottom:8px;">Лист подбора</div>
              <table>
                <thead>
                  <tr>
                    <th class="c" style="width:40px;">#</th>
                    <th style="width:140px;">SKU</th>
                    <th>Название</th>
                    <th class="c" style="width:70px;">Кол-во</th>
                    <th class="c" style="width:70px;">Заказов</th>
                  </tr>
                </thead>
                <tbody>${rows}</tbody>
              </table>
              <div class="small" style="margin-top:8px;">Собирай сверху вниз, отмечай маркером.</div>
            </div>
            <div class="card">
              <div style="font-weight:700;margin-bottom:8px;">Заказы в сборке</div>
              <table>
                <thead>
                  <tr>
                    <th>Заказ</th>
                    <th>Статус</th>
                    <th class="c" style="width:70px;">Шт</th>
                  </tr>
                </thead>
                <tbody>${orders}</tbody>
              </table>
              ${build.note ? `<div class="small" style="margin-top:8px;"><b>Комментарий:</b> ${esc(build.note)}</div>` : ""}
            </div>
          </div>
        </body>
        </html>`);
      w.document.close();
      w.focus();
    } catch (e: any) {
      toast(e?.message || String(e), "error");
    }
  }

  // ------------------------------------------------------------------
  // Ozon states
  // ------------------------------------------------------------------

  const [ozonSummary, setOzonSummary] = useState<OzonSummary | null>(null);
  const [ozonTx, setOzonTx] = useState<OzonTransaction[]>([]);
  const [ozonSyncRes, setOzonSyncRes] = useState<OzonSyncResult | null>(null);

  const [postings, setPostings] = useState<OzonPosting[]>([]);
  const [postingsOffset, setPostingsOffset] = useState<number>(0);
  const [postingsHasNext, setPostingsHasNext] = useState<boolean>(false);
  const [postingsStatus, setPostingsStatus] = useState<string>("");

  const [moneyAccounts, setMoneyAccounts] = useState<MoneyAccount[]>([]);
  const [bankAccountId, setBankAccountId] = useState<string>("");
  const [payoutRows, setPayoutRows] = useState<OzonPayoutReconRow[]>([]);
  const [payoutAutoRes, setPayoutAutoRes] = useState<OzonPayoutAutoConfirmResult | null>(null);

  const [periodStatus, setPeriodStatus] = useState<OzonPeriodStatus | null>(null);
  const [periodLoading, setPeriodLoading] = useState(false);

  async function ozonFetchFinance() {
    if (!connId) return toast("Выбери подключение", "error");
    try {
      const res = await api<OzonFetchResult>("/integrations/ozon/fetch", {
        method: "POST",
        body: JSON.stringify({ connection_id: connId, date_from: dateFrom, date_to: dateTo }),
      });
      toast(`Ozon: загружено ${res.fetched}, вставлено ${res.inserted}`);
      await ozonLoadFinance();
    } catch (e: any) {
      toast(e?.message || String(e), "error");
    }
  }

  async function ozonLoadFinance() {
    if (!connId) return;
    const qs = new URLSearchParams({ connection_id: connId, date_from: dateFrom, date_to: dateTo });
    const [sum, rows] = await Promise.all([
      api<OzonSummary>(`/integrations/ozon/summary?${qs.toString()}`),
      api<OzonTransaction[]>(`/integrations/ozon/transactions?${qs.toString()}`),
    ]);
    setOzonSummary(sum);
    setOzonTx(rows || []);
  }

  async function ozonSyncAll() {
    if (!connId) return toast("Выбери подключение", "error");
    try {
      const res = await api<OzonSyncResult>("/integrations/ozon/sync_all", {
        method: "POST",
        body: JSON.stringify({ connection_id: connId, date_from: dateFrom, date_to: dateTo }),
      });
      setOzonSyncRes(res);
      toast("Ozon: sync выполнен");
      await ozonLoadFinance();
      await ozonLoadPostings(0);
    } catch (e: any) {
      toast(e?.message || String(e), "error");
    }
  }

  async function ozonFetchPostings() {
    if (!connId) return toast("Выбери подключение", "error");
    try {
      const res = await api<OzonFbsFetchResult>("/integrations/ozon/fbs/postings/fetch", {
        method: "POST",
        body: JSON.stringify({
          connection_id: connId,
          date_from: dateFrom,
          date_to: dateTo,
          status: postingsStatus || null,
        }),
      });
      toast(`Ozon FBS: fetched ${res.fetched}, created ${res.created}, updated ${res.updated}`);
      await ozonLoadPostings(0);
    } catch (e: any) {
      toast(e?.message || String(e), "error");
    }
  }

  async function ozonLoadPostings(offset: number) {
    if (!connId) return;
    const qs = new URLSearchParams({
      connection_id: connId,
      date_from: dateFrom,
      date_to: dateTo,
      limit: "50",
      offset: String(offset),
    });
    if (postingsStatus) qs.set("status", postingsStatus);

    const page = await api<OzonPostingsPage>(`/integrations/ozon/fbs/postings?${qs.toString()}`);
    setPostings(page.postings || []);
    setPostingsOffset(page.next_offset ?? offset);
    setPostingsHasNext(Boolean(page.has_next));
  }

  async function ozonLoadPayoutRecon() {
    if (!connId) return;
    const qs = new URLSearchParams({ connection_id: connId, date_from: dateFrom, date_to: dateTo });
    const rows = await api<OzonPayoutReconRow[]>(`/integrations/ozon/payouts/reconciliation?${qs.toString()}`);
    setPayoutRows(rows || []);
  }

  async function ozonLoadMoneyAccounts() {
    const rows = await api<MoneyAccount[]>("/money/accounts");
    setMoneyAccounts(rows || []);
    if (!bankAccountId && (rows || [])[0]?.id) setBankAccountId((rows || [])[0].id);
  }

  async function ozonAutoConfirmPayouts() {
    if (!connId) return toast("Выбери подключение", "error");
    try {
      const res = await api<OzonPayoutAutoConfirmResult>(
        "/integrations/ozon/payouts/reconciliation/auto_confirm",
        {
          method: "POST",
          body: JSON.stringify({
            connection_id: connId,
            date_from: dateFrom,
            date_to: dateTo,
            bank_account_id: bankAccountId || null,
            threshold: 0.86,
          }),
        },
      );
      setPayoutAutoRes(res);
      toast(`Автосверка: confirmed ${res.confirmed} / scanned ${res.scanned}`);
      await ozonLoadPayoutRecon();
    } catch (e: any) {
      toast(e?.message || String(e), "error");
    }
  }

  async function ozonLoadPeriodStatus() {
    if (!connId) return;
    try {
      setPeriodLoading(true);
      const qs = new URLSearchParams({ connection_id: connId, date_from: dateFrom, date_to: dateTo });
      const res = await api<OzonPeriodStatus>(`/integrations/ozon/period_status?${qs.toString()}`);
      setPeriodStatus(res);
    } finally {
      setPeriodLoading(false);
    }
  }

  async function ozonDownloadUtPackage() {
    if (!connId) return toast("Выбери подключение", "error");
    try {
      const qs = new URLSearchParams({ connection_id: connId, date_from: dateFrom, date_to: dateTo });
      const res = await fetch(`${API}/integrations/ozon/ut_package?${qs.toString()}`);
      if (!res.ok) throw new Error(await res.text());
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `ozon_ut_${dateFrom}_${dateTo}.zip`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (e: any) {
      toast(e?.message || String(e), "error");
    }
  }

  // ------------------------------------------------------------------
  // YMarket states
  // ------------------------------------------------------------------

  const [ymOrders, setYmOrders] = useState<YMarketOrder[]>([]);
  const [ymReports, setYmReports] = useState<YMarketReport[]>([]);
  const [ymCampaigns, setYmCampaigns] = useState<YMarketCampaign[]>([]);
  const [ymReportId, setYmReportId] = useState<string>("");

  async function ymFetchOrders() {
    if (!connId) return toast("Выбери подключение", "error");
    try {
      await api<any>("/integrations/ymarket/orders/fetch", {
        method: "POST",
        body: JSON.stringify({
          connection_id: connId,
          date_from: dateFrom,
          date_to: dateTo,
          limit: 50,
          fake: false,
          statuses: null,
        }),
      });
      toast("Я.Маркет: заказы загружены");
      await ymLoadOrders();
    } catch (e: any) {
      toast(e?.message || String(e), "error");
    }
  }

  async function ymLoadOrders() {
    if (!connId) return;
    const qs = new URLSearchParams({ connection_id: connId, date_from: dateFrom, date_to: dateTo, limit: "2000" });
    const rows = await api<YMarketOrder[]>(`/integrations/ymarket/orders?${qs.toString()}`);
    setYmOrders(rows || []);
  }

  async function ymLoadReports() {
    if (!connId) return;
    const qs = new URLSearchParams({ connection_id: connId });
    const rows = await api<YMarketReport[]>(`/integrations/ymarket/reports?${qs.toString()}`);
    setYmReports(rows || []);
  }

  async function ymGenerateUnitedNetting() {
    if (!connId) return toast("Выбери подключение", "error");
    try {
      const res = await api<YMarketReport>("/integrations/ymarket/reports/united-netting/generate", {
        method: "POST",
        body: JSON.stringify({
          connection_id: connId,
          date_from: dateFrom,
          date_to: dateTo,
          placement_programs: ["FBS"],
          format: "FILE",
          language: "RU",
        }),
      });
      toast(`Отчёт сформирован: ${res.report_id}`);
      await ymLoadReports();
    } catch (e: any) {
      toast(e?.message || String(e), "error");
    }
  }

  async function ymDownloadReport(reportId: string) {
    if (!connId) return toast("Выбери подключение", "error");
    try {
      const qs = new URLSearchParams({ connection_id: connId, report_id: reportId });
      const res = await fetch(`${API}/integrations/ymarket/reports/download?${qs.toString()}`);
      if (!res.ok) throw new Error(await res.text());
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `ymarket_${reportId}`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (e: any) {
      toast(e?.message || String(e), "error");
    }
  }

  async function ymLoadCampaigns() {
    if (!connId) return;
    const qs = new URLSearchParams({ connection_id: connId });
    const rows = await api<YMarketCampaign[]>(`/integrations/ymarket/campaigns?${qs.toString()}`);
    setYmCampaigns(rows || []);
  }

  async function ymDownloadUtPackage() {
    if (!connId) return toast("Выбери подключение", "error");
    try {
      const qs = new URLSearchParams({ connection_id: connId, date_from: dateFrom, date_to: dateTo });
      if (ymReportId.trim()) qs.set("report_id", ymReportId.trim());
      const res = await fetch(`${API}/integrations/ymarket/ut_package?${qs.toString()}`);
      if (!res.ok) throw new Error(await res.text());
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `ymarket_ut_${dateFrom}_${dateTo}.zip`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (e: any) {
      toast(e?.message || String(e), "error");
    }
  }

  // ------------------------------------------------------------------
  // WB states
  // ------------------------------------------------------------------

  const [wbPing, setWbPing] = useState<WbPing | null>(null);
  const [wbOrders, setWbOrders] = useState<WbOrderLine[]>([]);
  const [wbSales, setWbSales] = useState<WbSaleLine[]>([]);
  const [wbLastFetch, setWbLastFetch] = useState<{ orders?: WbFetchResult; sales?: WbFetchResult } | null>(null);

  async function wbDoPing() {
    if (!connId) return toast("Выбери подключение", "error");
    try {
      const qs = new URLSearchParams({ connection_id: connId });
      const res = await api<WbPing>(`/integrations/wb/ping?${qs.toString()}`);
      setWbPing(res);
      toast(res.ok ? "WB: токен валиден" : "WB: ошибка" , res.ok ? "success" : "error");
    } catch (e: any) {
      toast(e?.message || String(e), "error");
    }
  }

  async function wbFetchOrders() {
    if (!connId) return toast("Выбери подключение", "error");
    try {
      const res = await api<WbFetchResult>("/integrations/wb/orders/fetch", {
        method: "POST",
        body: JSON.stringify({ connection_id: connId, date_from: dateFrom }),
      });
      setWbLastFetch((x) => ({ ...(x || {}), orders: res }));
      toast(`WB orders: fetched ${res.fetched}, inserted ${res.inserted}, updated ${res.updated}`);
      await wbLoadOrders();
    } catch (e: any) {
      toast(e?.message || String(e), "error");
    }
  }

  async function wbFetchSales() {
    if (!connId) return toast("Выбери подключение", "error");
    try {
      const res = await api<WbFetchResult>("/integrations/wb/sales/fetch", {
        method: "POST",
        body: JSON.stringify({ connection_id: connId, date_from: dateFrom }),
      });
      setWbLastFetch((x) => ({ ...(x || {}), sales: res }));
      toast(`WB sales: fetched ${res.fetched}, inserted ${res.inserted}, updated ${res.updated}`);
      await wbLoadSales();
    } catch (e: any) {
      toast(e?.message || String(e), "error");
    }
  }

  async function wbLoadOrders() {
    if (!connId) return;
    const qs = new URLSearchParams({ connection_id: connId, date_from: dateFrom, date_to: dateTo, limit: "2000" });
    const rows = await api<WbOrderLine[]>(`/integrations/wb/orders?${qs.toString()}`);
    setWbOrders(rows || []);
  }

  async function wbLoadSales() {
    if (!connId) return;
    const qs = new URLSearchParams({ connection_id: connId, date_from: dateFrom, date_to: dateTo, limit: "2000" });
    const rows = await api<WbSaleLine[]>(`/integrations/wb/sales?${qs.toString()}`);
    setWbSales(rows || []);
  }

  // ------------------------------------------------------------------
  // Auto-load per section
  // ------------------------------------------------------------------

  useEffect(() => {
    (async () => {
      if (!connId) return;
      try {
        if (section === "fbs_builds") await loadBuilds();
        if (section === "ozon_finance") await ozonLoadFinance();
        if (section === "ozon_orders") await ozonLoadPostings(0);
        if (section === "ozon_reconcile") {
          await ozonLoadMoneyAccounts();
          await ozonLoadPayoutRecon();
        }
        if (section === "ozon_period") await ozonLoadPeriodStatus();

        if (section === "ym_orders") await ymLoadOrders();
        if (section === "ym_reports") await ymLoadReports();
        if (section === "ym_campaigns") await ymLoadCampaigns();

        if (section === "wb_orders") await wbLoadOrders();
        if (section === "wb_sales") await wbLoadSales();
      } catch (e: any) {
        toast(e?.message || String(e), "error");
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [section, connId]);

  // ------------------------------------------------------------------
  // Derived metrics for dashboard
  // ------------------------------------------------------------------

  const ozonChart = useMemo(() => {
    const byDay = new Map<string, number>();
    for (const r of ozonTx || []) {
      const d = String(r.operation_date || "").slice(0, 10);
      if (!d) continue;
      const a = Number(r.amount || 0);
      byDay.set(d, (byDay.get(d) || 0) + a);
    }
    const keys = Array.from(byDay.keys()).sort();
    return keys.map((k) => byDay.get(k) || 0);
  }, [ozonTx]);

  const ymKpi = useMemo(() => {
    const count = ymOrders.length;
    const sum = ymOrders.reduce((acc, x) => acc + Number(x.buyer_total || 0), 0);
    const items = ymOrders.reduce((acc, x) => acc + (x.items || []).reduce((a, i) => a + Number(i.quantity || 0), 0), 0);
    return { count, sum, items };
  }, [ymOrders]);

  const wbKpi = useMemo(() => {
    const orders = wbOrders.length;
    const ordersSum = wbOrders.reduce((acc, x) => acc + Number(x.price_with_disc || x.total_price || 0), 0);
    const sales = wbSales.length;
    const salesSum = wbSales.reduce((acc, x) => acc + Number(x.for_pay || 0), 0);
    const salesBars = wbSales.slice(0, 24).map((x) => Number(x.for_pay || 0)).reverse();
    return { orders, ordersSum, sales, salesSum, salesBars };
  }, [wbOrders, wbSales]);

  const nav = useMemo(() => {
    if (mp === "ozon") {
      return [
        { id: "fbs_builds" as const, title: "Сборки" },
        { id: "ozon_finance" as const, title: "Финансы" },
        { id: "ozon_orders" as const, title: "Заказы (FBS)" },
        { id: "ozon_reconcile" as const, title: "Сверка выплат" },
        { id: "ozon_period" as const, title: "Закрытие периода" },
        { id: "ozon_export" as const, title: "Экспорт в 1С" },
        { id: "connections" as const, title: "Подключения" },
      ];
    }
    if (mp === "ymarket") {
      return [
        { id: "fbs_builds" as const, title: "Сборки" },
        { id: "ym_orders" as const, title: "Заказы (FBS)" },
        { id: "ym_reports" as const, title: "Отчёты" },
        { id: "ym_campaigns" as const, title: "Кампании" },
        { id: "ym_export" as const, title: "Экспорт в 1С" },
        { id: "connections" as const, title: "Подключения" },
      ];
    }
    return [
      { id: "fbs_builds" as const, title: "Сборки" },
      { id: "wb_orders" as const, title: "Заказы" },
      { id: "wb_sales" as const, title: "Продажи / возвраты" },
      { id: "connections" as const, title: "Подключения" },
    ];
  }, [mp]);

  const meta = mpMeta(mp);

  // ------------------------------------------------------------------
  // Render helpers
  // ------------------------------------------------------------------

  function SideItem({ id, title }: { id: Section; title: string }) {
    const active = section === id;
    return (
      <button
        onClick={() => setSection(id)}
        className={[
          "w-full rounded-lg px-3 py-2 text-left text-sm transition",
          active ? "bg-slate-900 text-white" : "text-slate-700 hover:bg-slate-100",
        ].join(" ")}
      >
        {title}
      </button>
    );
  }

  // ------------------------------------------------------------------
  // Sections
  // ------------------------------------------------------------------

  const sectionTitle = useMemo(() => {
    if (section === "fbs_build_view") return buildDetail?.title || "Сборка";
    return nav.find((x) => x.id === section)?.title || "Маркетплейсы";
  }, [nav, section, buildDetail]);

  const connectionHint = useMemo(() => {
    if (mp === "ozon") return "client_id + api_key";
    if (mp === "ymarket") return "client_id = Campaign ID (число), api_key = Api-Key";
    return "api_key = токен WB (Statistics), client_id можно использовать как заметку";
  }, [mp]);

  return (
    <div className="grid grid-cols-12 gap-4">
      {/* Left */}
      <aside className="col-span-12 lg:col-span-3">
        <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
          <div className="flex items-center justify-between">
            <div className="text-sm font-semibold text-slate-900">Маркетплейсы</div>
            {apiOk !== null ? (
              <Badge tone={apiOk ? "emerald" : "rose"}>{apiOk ? "API OK" : "API ERR"}</Badge>
            ) : (
              <Badge>API…</Badge>
            )}
          </div>
          {apiOk === false ? (
            <div className="mt-2 text-xs text-rose-700">{apiErr}</div>
          ) : apiUtc ? (
            <div className="mt-2 text-xs text-slate-500">UTC: {apiUtc}</div>
          ) : null}

          <div className="mt-4 grid grid-cols-3 gap-2">
            {(["ozon", "ymarket", "wb"] as Mp[]).map((x) => {
              const m = mpMeta(x);
              const on = mp === x;
              return (
                <button
                  key={x}
                  onClick={() => setMp(x)}
                  className={[
                    "h-9 rounded-lg px-2 text-xs font-semibold transition",
                    on ? m.pill : m.pillOff,
                  ].join(" ")}
                >
                  {m.title}
                </button>
              );
            })}
          </div>

          <div className="mt-4">
            <div className="mb-1 flex items-center justify-between">
              <div className="text-xs font-medium text-slate-700">Подключение</div>
              <button
                className="text-xs font-medium text-blue-700 hover:text-blue-800"
                onClick={() => setSection("connections")}
              >
                управлять
              </button>
            </div>
            <Select
              value={connId}
              onChange={(v) => setConnId(v)}
              options={(connections || []).map((c) => ({
                value: c.id,
                label: `${c.name}${c.is_active ? "" : " (off)"}`,
              }))}
              placeholder="Выберите…"
            />
            <div className="mt-2 text-[11px] text-slate-500">{connectionHint}</div>
          </div>

          <div className="mt-4 grid grid-cols-2 gap-2">
            <Input label="Период с" value={dateFrom} onChange={setDateFrom} type="date" />
            <Input label="по" value={dateTo} onChange={setDateTo} type="date" />
          </div>

          <div className="mt-4 space-y-2">
            {nav.map((it) => (
              <SideItem key={it.id} id={it.id} title={it.title} />
            ))}
          </div>

          <div className="mt-4 rounded-xl bg-slate-50 p-3 text-xs text-slate-600">
            <div className="font-medium text-slate-700">Подсказка</div>
            <div className="mt-1">Сначала добавь подключение → затем жми «Fetch / Sync».</div>
          </div>
        </div>
      </aside>

      {/* Right */}
      <section className="col-span-12 lg:col-span-9 space-y-4">
        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <div className="flex items-center gap-2">
                <div className="text-lg font-semibold text-slate-900">{sectionTitle}</div>
                <Badge tone={meta.badge}>{meta.title}</Badge>
                {conn ? (
                  <span className="text-xs text-slate-500">• {conn.name}</span>
                ) : (
                  <span className="text-xs text-slate-500">• подключение не выбрано</span>
                )}
              </div>
              <div className="mt-1 text-xs text-slate-500">Период: {dateFrom} → {dateTo}</div>
            </div>

            <div className="flex flex-wrap gap-2">
              {section === "fbs_builds" ? (
                <>
                  <Button variant="secondary" onClick={loadBuilds}>
                    Обновить
                  </Button>
                  <Button
                    variant="secondary"
                    onClick={() =>
                      setSection(mp === "ozon" ? "ozon_orders" : mp === "ymarket" ? "ym_orders" : "wb_orders")
                    }
                  >
                    Перейти к заказам
                  </Button>
                </>
              ) : null}

              {section === "fbs_build_view" && buildDetail ? (
                <>
                  <Button variant="secondary" onClick={() => setSection("fbs_builds")}>
                    ← Сборки
                  </Button>
                  <Button variant="secondary" onClick={() => printBuild(buildDetail)}>
                    Печать
                  </Button>
                  <Button variant="secondary" onClick={() => patchBuild({ status: "picking" })}>
                    В сборке
                  </Button>
                  <Button variant="secondary" onClick={() => patchBuild({ status: "packed" })}>
                    Упаковано
                  </Button>
                  <Button variant="secondary" onClick={() => patchBuild({ status: "shipped" })}>
                    Отгружено
                  </Button>
                  <Button variant="primary" onClick={() => patchBuild({ status: "closed" })}>
                    Закрыть
                  </Button>
                </>
              ) : null}

              {mp === "ozon" && section === "ozon_finance" ? (
                <>
                  <Button variant="secondary" onClick={ozonLoadFinance}>
                    Обновить
                  </Button>
                  <Button variant="secondary" onClick={ozonFetchFinance}>
                    Fetch
                  </Button>
                  <Button variant="primary" onClick={ozonSyncAll}>
                    Sync ALL
                  </Button>
                </>
              ) : null}

              {mp === "ozon" && section === "ozon_orders" ? (
                <>
                  <Button variant="secondary" onClick={() => ozonLoadPostings(0)}>
                    Обновить
                  </Button>
                  <Button variant="primary" onClick={ozonFetchPostings}>
                    Fetch
                  </Button>
                </>
              ) : null}

              {mp === "ozon" && section === "ozon_reconcile" ? (
                <>
                  <Button variant="secondary" onClick={ozonLoadPayoutRecon}>
                    Пересчитать
                  </Button>
                  <Button variant="primary" onClick={ozonAutoConfirmPayouts}>
                    Автосверка
                  </Button>
                </>
              ) : null}

              {mp === "ozon" && section === "ozon_period" ? (
                <Button variant="primary" onClick={ozonLoadPeriodStatus} disabled={periodLoading}>
                  {periodLoading ? "Проверка…" : "Проверить"}
                </Button>
              ) : null}

              {mp === "ozon" && section === "ozon_export" ? (
                <Button variant="primary" onClick={ozonDownloadUtPackage}>
                  Скачать пакет
                </Button>
              ) : null}

              {mp === "ymarket" && section === "ym_orders" ? (
                <>
                  <Button variant="secondary" onClick={ymLoadOrders}>
                    Обновить
                  </Button>
                  <Button variant="primary" onClick={ymFetchOrders}>
                    Fetch
                  </Button>
                </>
              ) : null}

              {mp === "ymarket" && section === "ym_reports" ? (
                <>
                  <Button variant="secondary" onClick={ymLoadReports}>
                    Обновить
                  </Button>
                  <Button variant="primary" onClick={ymGenerateUnitedNetting}>
                    United Netting
                  </Button>
                </>
              ) : null}

              {mp === "ymarket" && section === "ym_campaigns" ? (
                <Button variant="secondary" onClick={ymLoadCampaigns}>
                  Обновить
                </Button>
              ) : null}

              {mp === "ymarket" && section === "ym_export" ? (
                <Button variant="primary" onClick={ymDownloadUtPackage}>
                  Скачать пакет
                </Button>
              ) : null}

              {mp === "wb" && section === "wb_orders" ? (
                <>
                  <Button variant="secondary" onClick={wbLoadOrders}>
                    Обновить
                  </Button>
                  <Button variant="primary" onClick={wbFetchOrders}>
                    Fetch
                  </Button>
                </>
              ) : null}

              {mp === "wb" && section === "wb_sales" ? (
                <>
                  <Button variant="secondary" onClick={wbLoadSales}>
                    Обновить
                  </Button>
                  <Button variant="primary" onClick={wbFetchSales}>
                    Fetch
                  </Button>
                </>
              ) : null}

              {section === "connections" ? (
                <Button variant="primary" onClick={openCreateConn}>
                  + Подключение
                </Button>
              ) : null}
            </div>
          </div>
        </div>

        {/* Dashboard chips */}
        <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
          {mp === "ozon" ? (
            <>
              <KpiCard title="Операций" value={fmtNum(ozonSummary?.tx_count || 0)} hint="по периоду" />
              <KpiCard title="Продажи" value={fmtRub(ozonSummary?.sales_total || 0)} hint="начисления" />
              <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
                <div className="text-sm font-medium text-slate-700">Динамика (amount)</div>
                <div className="mt-2">
                  <Sparkline values={ozonChart} strokeClassName="stroke-violet-700" fillClassName="fill-violet-200/40" />
                </div>
              </div>
            </>
          ) : null}

          {mp === "ymarket" ? (
            <>
              <KpiCard title="Заказов" value={fmtNum(ymKpi.count)} hint="по БД / фильтр периода" />
              <KpiCard title="Сумма" value={fmtRub(ymKpi.sum)} hint="buyer_total" />
              <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
                <div className="text-sm font-medium text-slate-700">Товары (шт.)</div>
                <div className="mt-2 flex items-center justify-between">
                  <div className="text-2xl font-semibold tracking-tight">{fmtNum(ymKpi.items)}</div>
                  <Badge tone="amber">FBS</Badge>
                </div>
                <div className="mt-2 text-xs text-slate-500">Сумма quantity по строкам заказа</div>
              </div>
            </>
          ) : null}

          {mp === "wb" ? (
            <>
              <KpiCard title="Заказы" value={fmtNum(wbKpi.orders)} hint="строки заказов" />
              <KpiCard title="Продажи" value={fmtNum(wbKpi.sales)} hint="строки продаж/возвратов" />
              <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
                <div className="flex items-center justify-between">
                  <div className="text-sm font-medium text-slate-700">forPay (посл.)</div>
                  <Badge tone="rose">WB</Badge>
                </div>
                <div className="mt-2">
                  <BarMini values={wbKpi.salesBars} barClassName="bg-rose-500/70" />
                </div>
              </div>
            </>
          ) : null}
        </div>

        {/* CONTENT */}
        {section === "fbs_builds" ? (
          <>
            <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
              <div className="flex flex-wrap items-end justify-between gap-3">
                <div>
                  <div className="text-sm font-semibold">Сборки (FBS)</div>
                  <div className="mt-1 text-xs text-slate-500">
                    Выбирай заказы → «Создать сборку». В сборке доступен лист подбора и статусы (в сборке/упаковано/отгружено/закрыто).
                  </div>
                </div>
                <div className="flex flex-wrap gap-2">
                  <Button variant="secondary" onClick={() => setSection(mp === "ozon" ? "ozon_orders" : mp === "ymarket" ? "ym_orders" : "wb_orders")}>
                    Выбрать заказы
                  </Button>
                  <Button variant="secondary" onClick={loadBuilds}>
                    Обновить
                  </Button>
                </div>
              </div>
            </div>

            <DataTable<FbsBuildSummary>
              title="Список сборок"
              rows={builds}
              pageSize={10}
              onRefresh={loadBuilds}
              columns={[
                { key: "created_at", title: "Создана", render: (r) => String(r.created_at || "").slice(0, 19).replace("T", " ") },
                { key: "title", title: "Название" },
                {
                  key: "status",
                  title: "Статус",
                  render: (r) => <Badge tone={statusTone(r.status)}>{r.status || "—"}</Badge>,
                },
                { key: "orders_count", title: "Заказов", render: (r) => fmtNum(r.orders_count) },
                { key: "qty_total", title: "Товаров (шт)", render: (r) => fmtNum(r.qty_total) },
              ]}
              rowActions={(r) => (
                <Button variant="secondary" onClick={() => openBuild(r.id)}>
                  Открыть
                </Button>
              )}
            />
          </>
        ) : null}

        {section === "fbs_build_view" ? (
          buildDetail ? (
            <>
              <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
                <div className="flex flex-wrap items-end justify-between gap-3">
                  <div>
                    <div className="text-sm font-semibold">{buildDetail.title}</div>
                    <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-slate-500">
                      <Badge tone={statusTone(buildDetail.status)}>{buildDetail.status}</Badge>
                      <span>заказов: {fmtNum(buildDetail.orders_count)}</span>
                      <span>товаров: {fmtNum(buildDetail.qty_total)}</span>
                      <span>строк: {fmtNum(buildDetail.items_count)}</span>
                    </div>
                    {buildDetail.note ? <div className="mt-2 text-xs text-slate-600">{buildDetail.note}</div> : null}
                  </div>
                  <div className="flex flex-wrap gap-2">
                    <Button variant="secondary" onClick={() => setSection("fbs_builds")}>
                      ← Сборки
                    </Button>
                    <Button variant="secondary" onClick={() => printBuild(buildDetail)}>
                      Печать листа
                    </Button>
                    <Button variant="secondary" onClick={() => patchBuild({ status: "picking" })}>
                      В сборке
                    </Button>
                    <Button variant="secondary" onClick={() => patchBuild({ status: "packed" })}>
                      Упаковано
                    </Button>
                    <Button variant="secondary" onClick={() => patchBuild({ status: "shipped" })}>
                      Отгружено
                    </Button>
                    <Button variant="primary" onClick={() => patchBuild({ status: "closed" })}>
                      Закрыть
                    </Button>
                  </div>
                </div>
              </div>

              <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
                <DataTable<any>
                  title="Лист подбора (агрегировано)"
                  rows={buildDetail.items as any}
                  pageSize={10}
                  columns={[
                    { key: "sku", title: "SKU" },
                    { key: "name", title: "Название" },
                    { key: "qty_total", title: "Кол-во", render: (r: any) => <b>{fmtNum(r.qty_total)}</b> },
                    { key: "orders_count", title: "Заказов", render: (r: any) => fmtNum(r.orders_count) },
                  ]}
                />

                <DataTable<any>
                  title="Заказы в сборке"
                  rows={buildDetail.orders as any}
                  pageSize={10}
                  columns={[
                    { key: "external_order_id", title: "Заказ" },
                    { key: "status", title: "Статус", render: (r: any) => <Badge tone="slate">{r.status || "—"}</Badge> },
                    { key: "qty_total", title: "Шт", render: (r: any) => fmtNum(r.qty_total) },
                    { key: "items_count", title: "Строк", render: (r: any) => fmtNum(r.items_count) },
                  ]}
                />
              </div>
            </>
          ) : (
            <div className="rounded-2xl border border-slate-200 bg-white p-6 text-sm text-slate-600 shadow-sm">
              Сборка не выбрана.
            </div>
          )
        ) : null}

        {section === "connections" ? (
          <DataTable<MarketplaceConnection>
            title="Подключения"
            rows={connections}
            pageSize={10}
            onRefresh={() => loadConnections(mp)}
            onAdd={openCreateConn}
            columns={[
              { key: "name", title: "Название" },
              { key: "marketplace", title: "Маркет" },
              { key: "client_id", title: "client_id" },
              {
                key: "api_key_last4",
                title: "api_key",
                render: (r) => <span className="font-mono">****{r.api_key_last4}</span>,
              },
              {
                key: "is_active",
                title: "Статус",
                render: (r) => (
                  <Badge tone={r.is_active ? "emerald" : "slate"}>{r.is_active ? "active" : "off"}</Badge>
                ),
              },
              { key: "note", title: "Заметка", render: (r) => <span className="text-slate-600">{r.note || ""}</span> },
            ]}
            rowActions={(r) => (
              <>
                <Button variant="secondary" onClick={() => openEditConn(r)}>
                  Изменить
                </Button>
                <Button variant="danger" onClick={() => deleteConn(r.id)}>
                  Удалить
                </Button>
              </>
            )}
          />
        ) : null}

        {mp === "ozon" && section === "ozon_finance" ? (
          <>
            {ozonSyncRes ? (
              <div className="rounded-2xl border border-slate-200 bg-white p-4 text-sm shadow-sm">
                <div className="flex items-center gap-2">
                  <Badge tone="violet">Sync ALL</Badge>
                  <div className="font-medium">Последний результат</div>
                </div>
                <div className="mt-2 grid grid-cols-1 gap-2 md:grid-cols-3">
                  <div className="rounded-xl bg-slate-50 p-3">
                    <div className="text-xs text-slate-500">Finance</div>
                    <div className="mt-1 text-sm">fetched {ozonSyncRes.finance.fetched}, inserted {ozonSyncRes.finance.inserted}</div>
                  </div>
                  <div className="rounded-xl bg-slate-50 p-3">
                    <div className="text-xs text-slate-500">Orders</div>
                    <div className="mt-1 text-sm">fetched {ozonSyncRes.orders.fetched}, created {ozonSyncRes.orders.created}</div>
                  </div>
                  <div className="rounded-xl bg-slate-50 p-3">
                    <div className="text-xs text-slate-500">Errors</div>
                    <div className="mt-1 text-sm">{(ozonSyncRes.errors || []).length}</div>
                  </div>
                </div>
              </div>
            ) : null}

            <DataTable<OzonTransaction>
              title="Ozon: операции (transactions)"
              rows={ozonTx}
              pageSize={12}
              onRefresh={ozonLoadFinance}
              columns={[
                { key: "operation_date", title: "Дата", render: (r) => String(r.operation_date || "").slice(0, 19).replace("T", " ") },
                { key: "operation_type_name", title: "Тип" },
                { key: "posting_number", title: "Posting" },
                { key: "amount", title: "Amount", render: (r) => fmtRub(r.amount) },
                { key: "accruals_for_sale", title: "Sale", render: (r) => fmtRub(r.accruals_for_sale) },
                { key: "sale_commission", title: "Comm", render: (r) => fmtRub(r.sale_commission) },
                { key: "delivery_charge", title: "Del", render: (r) => fmtRub(r.delivery_charge) },
              ]}
            />
          </>
        ) : null}

        {mp === "ozon" && section === "ozon_orders" ? (
          <>
            <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
              <div className="flex flex-wrap items-end justify-between gap-3">
                <div className="w-72">
                  <Select
                    label="Статус"
                    value={postingsStatus}
                    onChange={setPostingsStatus}
                    options={[
                      { value: "", label: "Все" },
                      { value: "awaiting_packaging", label: "awaiting_packaging" },
                      { value: "awaiting_deliver", label: "awaiting_deliver" },
                      { value: "delivering", label: "delivering" },
                      { value: "delivered", label: "delivered" },
                      { value: "cancelled", label: "cancelled" },
                    ]}
                  />
                </div>
                <div className="flex gap-2">
                  <Button variant="secondary" onClick={() => ozonLoadPostings(0)}>
                    Показать
                  </Button>
                  <Button variant="primary" onClick={ozonFetchPostings}>
                    Fetch
                  </Button>
                  <Button variant="secondary" onClick={clearSelection} disabled={selectedOrderIds.length === 0}>
                    Сброс
                  </Button>
                  <Button variant="primary" onClick={openCreateBuildModal} disabled={selectedOrderIds.length === 0}>
                    Создать сборку ({selectedOrderIds.length})
                  </Button>
                </div>
              </div>
            </div>

            <DataTable<OzonPosting>
              title="Ozon: postings"
              rows={postings}
              pageSize={10}
              actions={
                <div className="flex items-center gap-2">
                  <Badge tone={postingsHasNext ? "amber" : "slate"}>{postingsHasNext ? "есть ещё" : "конец"}</Badge>
                  <Button variant="secondary" disabled={postingsOffset <= 0} onClick={() => ozonLoadPostings(Math.max(0, postingsOffset - 50))}>
                    Назад
                  </Button>
                  <Button variant="secondary" disabled={!postingsHasNext} onClick={() => ozonLoadPostings(postingsOffset)}>
                    Ещё
                  </Button>
                </div>
              }
              columns={[
                { key: "posting_number", title: "Posting" },
                { key: "status", title: "Статус" },
                { key: "created_at", title: "Создан", render: (r) => String(r.created_at || "").slice(0, 19).replace("T", " ") },
                { key: "items_count", title: "Строк" },
                { key: "qty_total", title: "Шт" },
                { key: "items_total", title: "Сумма", render: (r) => fmtRub(r.items_total) },
                {
                  key: "items",
                  title: "Товары",
                  render: (r) => (
                    <div className="max-w-[520px] truncate text-slate-600" title={(r.items || []).map((i) => i.name).filter(Boolean).join("; ")}
                    >
                      {(r.items || []).slice(0, 2).map((i) => i.name || i.offer_id || i.sku).filter(Boolean).join("; ")}
                      {(r.items || []).length > 2 ? "…" : ""}
                    </div>
                  ),
                },
              ]}
              rowActions={(r) => {
                const id = r.posting_number;
                const checked = selectedOrderIds.includes(id);
                return (
                  <label className="flex items-center gap-2 text-xs text-slate-700">
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => toggleSelectedOrder(id)}
                      className="h-4 w-4 rounded border-slate-300"
                    />
                    в сборку
                  </label>
                );
              }}
            />
          </>
        ) : null}

        {mp === "ozon" && section === "ozon_reconcile" ? (
          <>
            <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
              <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm md:col-span-1">
                <div className="text-sm font-semibold">Банк-аккаунт</div>
                <div className="mt-2">
                  <Select
                    value={bankAccountId}
                    onChange={setBankAccountId}
                    options={(moneyAccounts || []).map((a) => ({ value: a.id, label: a.name }))}
                    placeholder="Выберите…"
                  />
                </div>
                <div className="mt-3 text-xs text-slate-500">
                  Используется для автосверки. Выписки импортируются в разделе «Импорт».
                </div>
                {payoutAutoRes ? (
                  <div className="mt-3 rounded-xl bg-slate-50 p-3 text-xs">
                    confirmed: <b>{payoutAutoRes.confirmed}</b> • scanned: {payoutAutoRes.scanned}
                  </div>
                ) : null}
              </div>

              <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm md:col-span-2">
                <div className="flex items-center justify-between">
                  <div className="text-sm font-semibold">Сверка выплат</div>
                  <Badge tone="violet">Ozon</Badge>
                </div>
                <div className="mt-2 text-xs text-slate-500">
                  Алгоритм ищет payout по операциям маркетплейса и предлагает совпадения из Money Ledger.
                </div>
                <div className="mt-3 flex flex-wrap gap-2">
                  <Button variant="secondary" onClick={ozonLoadPayoutRecon}>
                    Пересчитать
                  </Button>
                  <Button variant="primary" onClick={ozonAutoConfirmPayouts}>
                    Автосверка
                  </Button>
                </div>
              </div>
            </div>

            <DataTable<OzonPayoutReconRow>
              title="Payouts reconciliation"
              rows={payoutRows}
              pageSize={10}
              columns={[
                { key: "payout_date", title: "Дата", render: (r) => String(r.payout_date) },
                { key: "payout_key", title: "Ключ" },
                { key: "amount_marketplace", title: "MP", render: (r) => fmtRub(r.amount_marketplace) },
                { key: "expected_bank_in", title: "Банк(ожид)", render: (r) => fmtRub(r.expected_bank_in) },
                {
                  key: "match_status",
                  title: "Статус",
                  render: (r) => (
                    <Badge tone={r.match_status === "matched" ? "emerald" : "slate"}>
                      {r.match_status || "—"}
                    </Badge>
                  ),
                },
                {
                  key: "suggestions",
                  title: "Лучшее совпадение",
                  render: (r) => {
                    const s = (r.suggestions || [])[0];
                    if (!s) return <span className="text-slate-400">нет</span>;
                    return (
                      <div className="text-xs">
                        <div className="font-medium">{fmtRub(s.bank_op.amount)} • {Math.round((s.score || 0) * 100)}%</div>
                        <div className="text-slate-500 truncate max-w-[360px]">{s.bank_op.description || s.bank_op.counterparty || ""}</div>
                      </div>
                    );
                  },
                },
              ]}
            />
          </>
        ) : null}

        {mp === "ozon" && section === "ozon_period" ? (
          <>
            <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
              <div className="flex items-center justify-between">
                <div>
                  <div className="text-sm font-semibold">Статус периода</div>
                  <div className="mt-1 text-xs text-slate-500">Проверки на целостность и готовность к закрытию.</div>
                </div>
                <Button variant="primary" onClick={ozonLoadPeriodStatus} disabled={periodLoading}>
                  {periodLoading ? "Проверка…" : "Проверить"}
                </Button>
              </div>
            </div>

            {periodStatus ? (
              <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
                <KpiCard title="Операций" value={fmtNum(periodStatus.totals.tx_count)} />
                <KpiCard title="Постингов" value={fmtNum(periodStatus.totals.postings_count)} />
                <KpiCard title="Выплат matched" value={fmtNum(periodStatus.totals.payouts_matched)} />
              </div>
            ) : null}

            <DataTable<any>
              title="Checks"
              rows={(periodStatus?.checks || []) as any}
              pageSize={12}
              columns={[
                { key: "title", title: "Проверка" },
                {
                  key: "ok",
                  title: "OK",
                  render: (r) => <Badge tone={r.ok ? "emerald" : "rose"}>{r.ok ? "OK" : "FAIL"}</Badge>,
                },
                { key: "value", title: "Значение", render: (r) => <span className="text-slate-700">{r.value || ""}</span> },
                { key: "hint", title: "Подсказка", render: (r) => <span className="text-slate-600">{r.hint || ""}</span> },
              ]}
            />
          </>
        ) : null}

        {mp === "ozon" && section === "ozon_export" ? (
          <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <div className="text-sm font-semibold">Пакет под 1С УТ</div>
                <div className="mt-1 text-xs text-slate-500">ZIP: transactions + postings + payouts + README.</div>
              </div>
              <Button variant="primary" onClick={ozonDownloadUtPackage}>
                Скачать
              </Button>
            </div>
          </div>
        ) : null}

        {mp === "ymarket" && section === "ym_orders" ? (
          <DataTable<YMarketOrder>
            title="Яндекс Маркет: заказы"
            rows={ymOrders}
            pageSize={10}
            onRefresh={ymLoadOrders}
            actions={
              <div className="flex flex-wrap items-center gap-2">
                <Button variant="secondary" onClick={ymFetchOrders}>
                  Fetch
                </Button>
                <Button variant="secondary" onClick={clearSelection} disabled={selectedOrderIds.length === 0}>
                  Сброс
                </Button>
                <Button variant="primary" onClick={openCreateBuildModal} disabled={selectedOrderIds.length === 0}>
                  Создать сборку ({selectedOrderIds.length})
                </Button>
              </div>
            }
            columns={[
              { key: "order_id", title: "Order" },
              {
                key: "status",
                title: "Статус",
                render: (r) => <Badge tone={r.status === "DELIVERED" ? "emerald" : "amber"}>{r.status || "—"}</Badge>,
              },
              { key: "created_at", title: "Создан", render: (r) => String(r.created_at || "").slice(0, 19).replace("T", " ") },
              { key: "shipment_date", title: "Отгрузка" },
              { key: "buyer_total", title: "Сумма", render: (r) => fmtRub(r.buyer_total) },
              { key: "items_total", title: "Товары", render: (r) => fmtRub(r.items_total) },
              {
                key: "items",
                title: "Позиции",
                render: (r) => (
                  <div className="max-w-[520px] truncate text-slate-600" title={(r.items || []).map((i) => i.name).filter(Boolean).join("; ")}
                  >
                    {(r.items || []).slice(0, 2).map((i) => i.name || i.offer_id || i.shop_sku).filter(Boolean).join("; ")}
                    {(r.items || []).length > 2 ? "…" : ""}
                  </div>
                ),
              },
            ]}
            rowActions={(r) => {
              const id = String(r.order_id);
              const checked = selectedOrderIds.includes(id);
              return (
                <label className="flex items-center gap-2 text-xs text-slate-700">
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={() => toggleSelectedOrder(id)}
                    className="h-4 w-4 rounded border-slate-300"
                  />
                  в сборку
                </label>
              );
            }}
          />
        ) : null}

        {mp === "ymarket" && section === "ym_reports" ? (
          <>
            <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
              <div className="flex flex-wrap items-end justify-between gap-3">
                <div>
                  <div className="text-sm font-semibold">United Netting</div>
                  <div className="mt-1 text-xs text-slate-500">Сформируй отчёт → затем скачай или вложи в UT-пакет.</div>
                </div>
                <Button variant="primary" onClick={ymGenerateUnitedNetting}>
                  Сформировать
                </Button>
              </div>
            </div>

            <DataTable<YMarketReport>
              title="Отчёты"
              rows={ymReports}
              pageSize={10}
              onRefresh={ymLoadReports}
              columns={[
                { key: "report_id", title: "report_id" },
                { key: "report_type", title: "type" },
                {
                  key: "status",
                  title: "status",
                  render: (r) => <Badge tone={r.status === "DONE" ? "emerald" : "amber"}>{r.status || "—"}</Badge>,
                },
                { key: "date_from", title: "с" },
                { key: "date_to", title: "по" },
              ]}
              rowActions={(r) => (
                <Button variant="secondary" onClick={() => ymDownloadReport(r.report_id)}>
                  Скачать
                </Button>
              )}
            />
          </>
        ) : null}

        {mp === "ymarket" && section === "ym_campaigns" ? (
          <DataTable<YMarketCampaign>
            title="Campaigns"
            rows={ymCampaigns}
            pageSize={10}
            onRefresh={ymLoadCampaigns}
            columns={[
              { key: "id", title: "Campaign ID" },
              { key: "domain", title: "Domain" },
              { key: "business_name", title: "Business" },
              { key: "placement_type", title: "Placement" },
              { key: "api_availability", title: "API" },
            ]}
          />
        ) : null}

        {mp === "ymarket" && section === "ym_export" ? (
          <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <div className="text-sm font-semibold">Пакет под 1С УТ</div>
                <div className="mt-1 text-xs text-slate-500">orders + items + reports (+ опционально файл отчёта)</div>
              </div>
              <div className="flex flex-wrap items-end gap-3">
                <div className="w-64">
                  <Input
                    label="report_id (опц.)"
                    value={ymReportId}
                    onChange={setYmReportId}
                    placeholder="например 1234567"
                  />
                </div>
                <Button variant="primary" onClick={ymDownloadUtPackage}>
                  Скачать
                </Button>
              </div>
            </div>
          </div>
        ) : null}

        {mp === "wb" && section === "wb_orders" ? (
          <>
            <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
              <div className="flex flex-wrap items-end justify-between gap-3">
                <div>
                  <div className="text-sm font-semibold">WB Statistics: Orders</div>
                  <div className="mt-1 text-xs text-slate-500">Fetch берёт данные с dateFrom (UTC+3 по документации WB).</div>
                </div>
                <div className="flex gap-2">
                  <Button variant="secondary" onClick={wbDoPing}>
                    Ping
                  </Button>
                  <Button variant="primary" onClick={wbFetchOrders}>
                    Fetch
                  </Button>
                  <Button variant="secondary" onClick={clearSelection} disabled={selectedOrderIds.length === 0}>
                    Сброс
                  </Button>
                  <Button variant="primary" onClick={openCreateBuildModal} disabled={selectedOrderIds.length === 0}>
                    Создать сборку ({selectedOrderIds.length})
                  </Button>
                </div>
              </div>
              {wbPing ? (
                <div className="mt-2 text-xs">
                  <Badge tone={wbPing.ok ? "emerald" : "rose"}>{wbPing.ok ? "ok" : "err"}</Badge>
                  <span className="ml-2 text-slate-500">HTTP {wbPing.status_code}</span>
                </div>
              ) : null}
              {wbLastFetch?.orders ? (
                <div className="mt-2 text-xs text-slate-600">
                  fetched {wbLastFetch.orders.fetched}, inserted {wbLastFetch.orders.inserted}, updated {wbLastFetch.orders.updated}
                </div>
              ) : null}
            </div>

            <DataTable<WbOrderLine>
              title="WB: orders"
              rows={wbOrders}
              pageSize={10}
              onRefresh={wbLoadOrders}
              columns={[
                { key: "date", title: "Date", render: (r) => String(r.date || "").slice(0, 19).replace("T", " ") },
                { key: "last_change_date", title: "Changed", render: (r) => String(r.last_change_date || "").slice(0, 19).replace("T", " ") },
                { key: "srid", title: "srid" },
                { key: "supplier_article", title: "Article" },
                { key: "nm_id", title: "nmId" },
                { key: "barcode", title: "Barcode" },
                { key: "quantity", title: "Qty" },
                { key: "price_with_disc", title: "Price", render: (r) => fmtRub(r.price_with_disc || r.total_price) },
                {
                  key: "is_cancel",
                  title: "Cancel",
                  render: (r) => (r.is_cancel ? <Badge tone="rose">cancel</Badge> : <Badge tone="emerald">ok</Badge>),
                },
              ]}
              rowActions={(r) => {
                const id = String(r.srid);
                const checked = selectedOrderIds.includes(id);
                return (
                  <label className="flex items-center gap-2 text-xs text-slate-700">
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => toggleSelectedOrder(id)}
                      className="h-4 w-4 rounded border-slate-300"
                    />
                    в сборку
                  </label>
                );
              }}
            />
          </>
        ) : null}

        {mp === "wb" && section === "wb_sales" ? (
          <>
            <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
              <div className="flex flex-wrap items-end justify-between gap-3">
                <div>
                  <div className="text-sm font-semibold">WB Statistics: Sales / Returns</div>
                  <div className="mt-1 text-xs text-slate-500">В WB sales есть saleID — используем как уникальный ключ.</div>
                </div>
                <Button variant="primary" onClick={wbFetchSales}>
                  Fetch
                </Button>
              </div>
              {wbLastFetch?.sales ? (
                <div className="mt-2 text-xs text-slate-600">
                  fetched {wbLastFetch.sales.fetched}, inserted {wbLastFetch.sales.inserted}, updated {wbLastFetch.sales.updated}
                </div>
              ) : null}
            </div>

            <DataTable<WbSaleLine>
              title="WB: sales"
              rows={wbSales}
              pageSize={10}
              onRefresh={wbLoadSales}
              columns={[
                { key: "date", title: "Date", render: (r) => String(r.date || "").slice(0, 19).replace("T", " ") },
                { key: "sale_id", title: "saleID" },
                { key: "srid", title: "srid" },
                { key: "supplier_article", title: "Article" },
                { key: "nm_id", title: "nmId" },
                { key: "quantity", title: "Qty" },
                { key: "for_pay", title: "forPay", render: (r) => fmtRub(r.for_pay) },
                { key: "finished_price", title: "Finished", render: (r) => fmtRub(r.finished_price) },
              ]}
            />
          </>
        ) : null}

        {/* modal */}
        <Modal open={buildCreateOpen} title="Новая сборка" onClose={() => setBuildCreateOpen(false)}>
          <div className="grid grid-cols-1 gap-3">
            <div className="rounded-xl border border-slate-200 bg-slate-50 p-3 text-xs text-slate-600">
              Выбрано заказов: <b>{selectedOrderIds.length}</b>
              <div className="mt-1 break-words">{selectedOrderIds.slice(0, 8).join(", ")}{selectedOrderIds.length > 8 ? "…" : ""}</div>
            </div>
            <Input label="Название (опц.)" value={buildTitle} onChange={setBuildTitle} placeholder="например Утро 1 / склад" />
            <Input label="Комментарий (опц.)" value={buildNote} onChange={setBuildNote} placeholder="например срочно / хрупкое" />
            <div className="mt-2 flex justify-end gap-2">
              <Button variant="secondary" onClick={() => setBuildCreateOpen(false)}>
                Отмена
              </Button>
              <Button variant="primary" onClick={createBuild}>
                Создать
              </Button>
            </div>
          </div>
        </Modal>

        <Modal open={connModalOpen} title={editingConn ? "Изменить подключение" : "Новое подключение"} onClose={() => setConnModalOpen(false)}>
          <div className="grid grid-cols-1 gap-3">
            <Input label="Название" value={connName} onChange={setConnName} placeholder="например Основной кабинет" />

            <Input
              label={mp === "ymarket" ? "client_id (Campaign ID)" : "client_id"}
              value={connClientId}
              onChange={setConnClientId}
              placeholder={mp === "ymarket" ? "например 123456" : mp === "wb" ? "можно 0" : ""}
            />

            <Input
              label={mp === "wb" ? "WB token (Authorization)" : mp === "ymarket" ? "Api-Key" : "api_key"}
              value={connApiKey}
              onChange={setConnApiKey}
              placeholder={editingConn ? "оставь пустым, чтобы не менять" : "вставь ключ"}
            />

            <Input label="Заметка" value={connNote} onChange={setConnNote} placeholder="по желанию" />

            <Select
              label="Активно"
              value={connActive ? "1" : "0"}
              onChange={(v) => setConnActive(v === "1")}
              options={[
                { value: "1", label: "Да" },
                { value: "0", label: "Нет" },
              ]}
            />

            <div className="mt-2 flex justify-end gap-2">
              <Button variant="secondary" onClick={() => setConnModalOpen(false)}>
                Отмена
              </Button>
              <Button variant="primary" onClick={saveConn}>
                Сохранить
              </Button>
            </div>
          </div>
        </Modal>
      </section>
    </div>
  );
}
