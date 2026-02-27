// Shared API types used across pages/components.

export type Material = {
  id: number;
  name: string;
  category: string;
  base_uom: string;
  is_lot_tracked: boolean;
  props?: Record<string, string>;
  is_void?: boolean;
  voided_at?: string | null;
  void_reason?: string | null;
};

export type MovementRow = {
  id: number;
  lot_id: number;
  mv_date: string;
  mv_type: string;
  qty: number;
  ref_type?: string | null;
  ref_id?: number | null;
  material_id: number;
  material_name: string;
  lot_unit_cost: number;
};

export type WriteoffLineIn = {
  material_id: number;
  qty: number;
  uom: string;
  uom_factor?: number | null;
};

export type WriteoffCreate = {
  reason: "production" | "scrap" | "other";
  comment?: string | null;
  lines: WriteoffLineIn[];
};

export type PurchaseLine = {
  id?: number;
  material_id: number;
  qty: number;
  uom: string;
  unit_price: number;
  vat_rate: number;
};

export type PurchaseDoc = {
  id: number;
  doc_date: string; // YYYY-MM-DD
  supplier: string;
  doc_no: string;
  pay_type: string;
  vat_mode: string;
  comment?: string | null;
  status: string;
  posted_at?: string | null;
  is_void?: boolean;
  voided_at?: string | null;
  void_reason?: string | null;
  lines?: PurchaseLine[];
};

export type LotRow = {
  lot_id: number;
  material_id: number;
  material_name: string;
  qty_in: number;
  qty_out: number;
  qty_remaining: number;
  unit_cost: number;
  created_at: string;
};


export type BizOrder = {
  id: number;
  order_date: string; // YYYY-MM-DD
  channel: string;
  subchannel?: string | null;
  status: string;
  revenue: number;
  comment?: string | null;
  created_at: string;
};

export type Expense = {
  id: number;
  exp_date: string; // YYYY-MM-DD
  category: string;
  amount: number;
  channel?: string | null;
  comment?: string | null;
  created_at: string;
};

export type Control = {
  draft_purchases: number;
  open_orders: number;
  low_stock: number;
};


export type MoneyAccount = {
  id: string;
  type: string;
  name: string;
  currency: string;
  external_ref?: string | null;
  is_active: boolean;
  created_at: string;
};

export type Category = {
  id: string;
  name: string;
  type: string;
  parent_id?: string | null;
  is_tax_related: boolean;
  is_payroll_related: boolean;
  is_system: boolean;
  is_active: boolean;
};

export type MoneyOperation = {
  id: string;
  account_id: string;
  transfer_group_id?: string | null;
  posted_at: string;
  amount: number;
  currency: string;
  counterparty?: string | null;
  description?: string | null;
  operation_type: string;
  external_id?: string | null;
  source: string;
  is_void: boolean;
  created_at: string;
};

export type MoneyAllocation = {
  id: string;
  money_operation_id: string;
  category_id: string;
  amount_part: number;
  linked_entity_type?: string | null;
  linked_entity_id?: string | null;
  method: string;
  confidence?: number | null;
  confirmed: boolean;
  note?: string | null;
  created_at: string;
};


export type PeriodLock = {
  period: string; // YYYY-MM
  locked_at: string;
  locked_by?: string | null;
  note?: string | null;
};

export type ReconciliationMatch = {
  id: string;
  money_operation_id: string;
  right_type: string;
  right_id: string;
  method: string;
  score?: number | null;
  status: string;
  note?: string | null;
  created_at: string;
  confirmed_at?: string | null;
};



export type BankImportResult = {
  imported: number;
  skipped_duplicates: number;
  errors: string[];
};

export type MoneyAutoAllocateResult = {
  scanned: number;
  suggested: number;
  updated: number;
  skipped: number;
  errors: string[];
};

export type MoneyConfirmBatchResult = {
  confirmed: number;
  skipped: number;
  errors: string[];
};

export type MoneyRule = {
  id: string;
  name?: string | null;
  match_field: string;
  pattern: string;
  direction: string;
  account_id?: string | null;
  category_id: string;
  confidence: number;
  priority: number;
  is_active: boolean;
  created_at: string;
};

export type CashPlanItem = {
  id: string;
  name: string;
  direction: "in" | "out";
  amount: number;
  currency: string;
  account_id?: string | null;
  category_id?: string | null;
  schedule: "once" | "weekly" | "monthly";
  due_date?: string | null;
  day_of_month?: number | null;
  weekday?: number | null;
  start_date?: string | null;
  end_date?: string | null;
  note?: string | null;
  is_active: boolean;
  created_at: string;
  updated_at: string;
};

export type CashForecastRow = {
  date: string;
  planned_in: number;
  planned_out: number;
  net: number;
  balance: number;
};

export type CashflowRow = {
  date: string;
  inflow: number;
  outflow: number;
};

export type ProfitCashRow = {
  date: string;
  income: number;
  expense: number;
  profit: number;
};


// -----------------------------
// Marketplaces: Ozon (finance transactions)
// -----------------------------

export type MarketplaceConnection = {
  id: string;
  marketplace: string;
  name: string;
  client_id: string;
  api_key_last4: string;
  note?: string | null;
  is_active: boolean;
  created_at: string;
  updated_at: string;
};

export type OzonFetchResult = {
  fetched: number;
  inserted: number;
  duplicates: number;
  errors: string[];
};

export type OzonSummary = {
  tx_count: number;
  amount_total: number;
  sales_total: number;
  commission_total: number;
  delivery_total: number;
};

export type OzonFbsFetchResult = {
  fetched: number;
  created: number;
  updated: number;
  errors: string[];
};

export type OzonSyncResult = {
  finance: OzonFetchResult;
  orders: OzonFbsFetchResult;
  errors: string[];
};

export type OzonPostingItem = {
  id: string;
  product_id?: string | null;
  offer_id?: string | null;
  name?: string | null;
  sku?: string | null;
  quantity?: number | null;
  price?: number | null;
};

export type OzonPosting = {
  id: string;
  posting_number: string;
  order_id?: string | null;
  status?: string | null;
  substatus?: string | null;
  created_at?: string | null;
  in_process_at?: string | null;
  shipment_date?: string | null;
  items: OzonPostingItem[];
  items_count: number;
  qty_total: number;
  items_total: number;
};

export type OzonPostingsPage = {
  postings: OzonPosting[];
  has_next: boolean;
  next_offset?: number | null;
};

export type OzonTransaction = {
  id: string;
  operation_id: string;
  operation_date: string;
  operation_type?: string | null;
  operation_type_name?: string | null;
  posting_number?: string | null;
  type?: string | null;
  amount?: number | null;
  accruals_for_sale?: number | null;
  sale_commission?: number | null;
  delivery_charge?: number | null;
  return_delivery_charge?: number | null;
};


export type OzonToLedgerResult = {
  scanned: number;
  inserted: number;
  duplicates: number;
  errors: string[];
};

export type BankOpMini = {
  id: string;
  posted_at: string;
  amount: number;
  counterparty?: string | null;
  description?: string | null;
};

export type OzonPayoutSuggestion = {
  bank_op: BankOpMini;
  score: number;
};

export type OzonPayoutReconRow = {
  payout_key: string;
  payout_date: string; // YYYY-MM-DD
  amount_marketplace: number;
  expected_bank_in: number;
  operation_ids: string[];
  suggestions: OzonPayoutSuggestion[];
  matched_bank_op_id?: string | null;
  match_status?: string | null;
};


export type OzonPayoutAutoConfirmResult = {
  scanned: number;
  confirmed: number;
  skipped_existing: number;
  skipped_locked: number;
  errors: string[];
};


export type OzonPeriodCheck = {
  key: string;
  title: string;
  ok: boolean;
  value?: string | null;
  hint?: string | null;
};

export type OzonPeriodTotals = {
  tx_count: number;
  amount_total: number;
  sales_total: number;
  commission_total: number;
  delivery_total: number;

  postings_count: number;
  items_count: number;
  items_total: number;

  ledger_ops_count: number;
  bank_ops_count: number;

  payouts_detected: number;
  payouts_matched: number;
  payout_marketplace_total: number;
  bank_matched_total: number;
};

export type OzonPeriodStatus = {
  connection_id: string;
  date_from: string; // YYYY-MM-DD
  date_to: string; // YYYY-MM-DD
  checks: OzonPeriodCheck[];
  totals: OzonPeriodTotals;
};


// -----------------------------
// Yandex Market (ymarket)
// -----------------------------

export type YMarketCampaign = {
  id: number;
  domain?: string | null;
  business_id?: number | null;
  business_name?: string | null;
  placement_type?: string | null;
  api_availability?: string | null;
};

export type YMarketOrderItem = {
  offer_id?: string | null;
  shop_sku?: string | null;
  market_sku?: string | null;
  name?: string | null;
  quantity?: number | null;
  price?: number | null;
  line_total?: number | null;
};

export type YMarketOrder = {
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

export type YMarketReport = {
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

export type YMarketReportInfo = {
  report_id: string;
  status?: string | null;
  file_url?: string | null;
  raw?: any;
};


// -----------------------------
// Wildberries (wb)
// -----------------------------

export type WbPing = {
  ok: boolean;
  status_code: number;
  body?: string | null;
};

export type WbFetchResult = {
  fetched: number;
  inserted: number;
  updated: number;
  errors: string[];
};

export type WbOrderLine = {
  id: string;
  connection_id: string;
  srid: string;
  nm_id?: number | null;
  barcode?: string | null;
  supplier_article?: string | null;
  warehouse_name?: string | null;
  date?: string | null;
  last_change_date?: string | null;
  quantity?: number | null;
  total_price?: number | null;
  finished_price?: number | null;
  price_with_disc?: number | null;
  is_cancel?: boolean | null;
  cancel_date?: string | null;
};

export type WbSaleLine = {
  id: string;
  connection_id: string;
  sale_id: string;
  srid?: string | null;
  nm_id?: number | null;
  barcode?: string | null;
  supplier_article?: string | null;
  warehouse_name?: string | null;
  date?: string | null;
  last_change_date?: string | null;
  quantity?: number | null;
  for_pay?: number | null;
  finished_price?: number | null;
  price_with_disc?: number | null;
};


// -----------------------------
// FBS Builds (internal batches)
// -----------------------------

export type FbsBuildSummary = {
  id: string;
  marketplace: string;
  connection_id: string;
  title: string;
  status: string;
  note?: string | null;
  created_at: string;
  updated_at: string;
  orders_count: number;
  items_count: number;
  qty_total: number;
};

export type FbsBuildOrder = {
  id: string;
  external_order_id: string;
  status?: string | null;
  items_count: number;
  qty_total: number;
  items: Array<{
    sku?: string | null;
    offer_id?: string | null;
    name?: string | null;
    qty?: number | null;
    price?: number | null;
  }>;
};

export type FbsBuildItemAgg = {
  sku?: string | null;
  offer_id?: string | null;
  name?: string | null;
  qty_total: number;
  orders_count: number;
};

export type FbsBuildDetail = FbsBuildSummary & {
  orders: FbsBuildOrder[];
  items: FbsBuildItemAgg[];
};

export type FbsBuildCreate = {
  marketplace: string;
  connection_id: string;
  order_ids: string[];
  title?: string | null;
  note?: string | null;
};

export type FbsBuildPatch = {
  title?: string | null;
  status?: string | null;
  note?: string | null;
};

export type FbsBuildAddOrders = {
  order_ids: string[];
};
