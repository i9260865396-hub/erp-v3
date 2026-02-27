from datetime import date, datetime
from uuid import UUID
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any

class MaterialCreate(BaseModel):
    name: str
    category: str
    base_uom: str
    is_lot_tracked: bool = True
    # Доп. параметры материала (ширина рулона, упаковка, дефолтные длины и т.д.)
    props: Optional[Dict[str, Any]] = None

class MaterialUpdate(BaseModel):
    name: Optional[str] = None
    category: Optional[str] = None
    base_uom: Optional[str] = None
    is_lot_tracked: Optional[bool] = None
    props: Optional[Dict[str, Any]] = None

class MaterialVoid(BaseModel):
    reason: Optional[str] = None

class MaterialOut(BaseModel):
    id: int
    name: str
    category: str
    base_uom: str
    is_lot_tracked: bool
    props: Dict[str, str] = Field(default_factory=dict, alias="props_dict")
    is_void: bool = False
    voided_at: Optional[datetime] = None
    void_reason: Optional[str] = None

    class Config:
        from_attributes = True
        populate_by_name = True


# --- Purchases (INPUT) ---

class PurchaseLineCreate(BaseModel):
    material_id: Optional[int] = None
    material_name: Optional[str] = None
    qty: float
    uom: str
    unit_price: float
    vat_rate: float = 0.0
    # Опционально: переопределить коэффициент пересчёта (сколько базовых единиц в 1 uom)
    uom_factor: Optional[float] = None
    # Опционально: длина рулона (м.п.) для приёмки в "рулонах"
    roll_length_m: Optional[float] = None


class PurchaseDocCreate(BaseModel):
    doc_date: date
    supplier: str
    doc_no: str
    pay_type: str
    vat_mode: str
    comment: Optional[str] = None
    lines: List[PurchaseLineCreate]


# --- Purchases (OUTPUT) ---

class PurchaseLineOut(BaseModel):
    id: int
    material_id: int
    qty: float
    uom: str
    unit_price: float
    vat_rate: float

    class Config:
        from_attributes = True


class PurchaseDocOut(BaseModel):
    id: int
    doc_date: date
    supplier: str
    doc_no: str
    pay_type: str
    vat_mode: str
    comment: Optional[str] = None
    status: str = "DRAFT"
    posted_at: Optional[datetime] = None
    is_void: bool = False
    voided_at: Optional[datetime] = None
    void_reason: Optional[str] = None
    lines: List[PurchaseLineOut] = []

    class Config:
        from_attributes = True


class PurchasePostResponse(BaseModel):
    purchase_doc_id: int
    status: str
    lots_created: int


class PurchaseVoidRequest(BaseModel):
    reason: Optional[str] = None


# --- Stock movements / writeoffs ---

class MovementOut(BaseModel):
    id: int
    lot_id: int
    mv_date: datetime
    mv_type: str
    qty: float
    ref_type: Optional[str] = None
    ref_id: Optional[int] = None
    material_id: int
    material_name: str
    lot_unit_cost: float

class WriteoffLineIn(BaseModel):
    material_id: int
    qty: float
    uom: str
    uom_factor: Optional[float] = None

class WriteoffCreate(BaseModel):
    reason: str = Field(default="production")  # production/scrap/other
    comment: Optional[str] = None
    lines: List[WriteoffLineIn]

class WriteoffOutLine(BaseModel):
    material_id: int
    qty_input: float
    uom_input: str
    qty_base: float
    base_uom: str

class WriteoffOut(BaseModel):
    id: int
    doc_date: datetime
    reason: str
    comment: Optional[str] = None
    lines: List[WriteoffOutLine] = []

    class Config:
        from_attributes = True
class OrderItemIn(BaseModel):
    product_name: str
    qty: int = 1
    width_m: float
    height_m: float

class OrderCreate(BaseModel):
    order_date: date
    comment: Optional[str] = None
    items: List[OrderItemIn]

class ConsumptionRequestLine(BaseModel):
    material_id: int
    qty: float
    uom: str

class OrderPostRequest(BaseModel):
    consumption: List[ConsumptionRequestLine] = Field(default_factory=list)
    employee_id: Optional[int] = None
    minutes: int = 0
    rate_rub_per_hour: float = 0
    c_ml: float = 0
    m_ml: float = 0
    y_ml: float = 0
    k_ml: float = 0
    ink_price_per_ml: float = 0

class SaleChargeIn(BaseModel):
    charge_type: str
    amount: float
    comment: Optional[str] = None

class SaleCreate(BaseModel):
    sale_date: date
    order_id: int
    marketplace: str
    gross_price: float
    vat_rate: float = 0
    charges: List[SaleChargeIn] = Field(default_factory=list)

class UnitEconomicsOut(BaseModel):
    order_id: int
    material_cost: float
    ink_cost: float
    labor_cost: float
    total_cost: float
    gross_price: float
    charges_total: float
    net_revenue: float
    profit: float


# --- Biz Orders (Sales) ---
class BizOrderCreate(BaseModel):
    order_date: date
    channel: str
    subchannel: Optional[str] = None
    revenue: float = 0
    comment: Optional[str] = None

class BizOrderUpdate(BaseModel):
    order_date: Optional[date] = None
    channel: Optional[str] = None
    subchannel: Optional[str] = None
    revenue: Optional[float] = None
    status: Optional[str] = None
    comment: Optional[str] = None

class BizOrderOut(BaseModel):
    id: int
    order_date: date
    channel: str
    subchannel: Optional[str] = None
    status: str
    revenue: float
    comment: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True

# --- Expenses (Finance) ---
class ExpenseCreate(BaseModel):
    exp_date: date
    category: str
    amount: float
    channel: Optional[str] = None
    comment: Optional[str] = None

class ExpenseOut(BaseModel):
    id: int
    exp_date: date
    category: str
    amount: float
    channel: Optional[str] = None
    comment: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True

class ControlOut(BaseModel):
    draft_purchases: int
    open_orders: int
    low_stock: int


# -----------------------------
# Money Ledger
# -----------------------------

class MoneyAccountCreate(BaseModel):
    type: str  # bank/cash/marketplace/acquiring/other
    name: str
    currency: str = "RUB"
    external_ref: Optional[str] = None


class MoneyAccountOut(BaseModel):
    id: UUID
    type: str
    name: str
    currency: str
    external_ref: Optional[str] = None
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


class CategoryCreate(BaseModel):
    name: str
    type: str  # income/expense/transfer/balance_adjustment
    parent_id: Optional[UUID] = None
    is_tax_related: bool = False
    is_payroll_related: bool = False


class CategoryOut(BaseModel):
    id: UUID
    name: str
    type: str
    parent_id: Optional[UUID] = None
    is_tax_related: bool
    is_payroll_related: bool
    is_system: bool
    is_active: bool

    class Config:
        from_attributes = True


class MoneyOperationVoid(BaseModel):
    reason: Optional[str] = None


class MoneyOperationCreate(BaseModel):
    account_id: UUID
    transfer_group_id: Optional[UUID] = None
    posted_at: datetime
    amount: float  # signed
    currency: str = "RUB"
    counterparty: Optional[str] = None
    description: Optional[str] = None
    operation_type: str = "other"
    external_id: Optional[str] = None
    source: str = "manual_other"  # bank_import/cash_manual/marketplace_import/acquiring_import/manual_other
    raw_payload: Optional[dict] = None

class MoneyTransferCreate(BaseModel):
    from_account_id: UUID
    to_account_id: UUID
    posted_at: datetime
    amount: float  # positive
    currency: str = "RUB"
    counterparty: Optional[str] = None
    description: Optional[str] = None
    source: str = "manual_other"
    note: Optional[str] = None



class MoneyOperationOut(BaseModel):
    id: UUID
    account_id: UUID
    transfer_group_id: Optional[UUID] = None
    posted_at: datetime
    amount: float
    currency: str
    counterparty: Optional[str] = None
    description: Optional[str] = None
    operation_type: str
    external_id: Optional[str] = None
    source: str
    is_void: bool
    created_at: datetime

    class Config:
        from_attributes = True


class MoneyAllocationCreate(BaseModel):
    money_operation_id: UUID
    category_id: UUID
    amount_part: float
    linked_entity_type: Optional[str] = None
    linked_entity_id: Optional[str] = None
    method: str = "manual"
    confidence: Optional[float] = None
    confirmed: bool = False
    note: Optional[str] = None


class MoneyAllocationPatch(BaseModel):
    category_id: Optional[UUID] = None
    amount_part: Optional[float] = None
    linked_entity_type: Optional[str] = None
    linked_entity_id: Optional[str] = None
    confirmed: Optional[bool] = None
    note: Optional[str] = None


class MoneyAutoAllocateParams(BaseModel):
    # Optional filter (ISO date strings). If not set — processes recent 1000 ops.
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    # By default we only touch operations that have no confirmed/manual allocations.
    include_already_allocated: bool = False


class MoneyAutoAllocateResult(BaseModel):
    scanned: int
    suggested: int
    updated: int
    skipped: int
    errors: list[str] = []


class MoneyConfirmBatchParams(BaseModel):
    min_confidence: float = 0.95


class MoneyConfirmBatchResult(BaseModel):
    confirmed: int
    skipped: int
    errors: list[str] = []


class MoneyRuleCreate(BaseModel):
    name: Optional[str] = None
    match_field: str = "text"  # text/counterparty/description/source
    pattern: str
    direction: str = "any"  # any/in/out
    account_id: Optional[UUID] = None
    category_id: UUID
    confidence: float = 0.95
    priority: int = 100
    is_active: bool = True


class MoneyRulePatch(BaseModel):
    name: Optional[str] = None
    match_field: Optional[str] = None
    pattern: Optional[str] = None
    direction: Optional[str] = None
    account_id: Optional[UUID] = None
    category_id: Optional[UUID] = None
    confidence: Optional[float] = None
    priority: Optional[int] = None
    is_active: Optional[bool] = None


class MoneyRuleOut(BaseModel):
    id: UUID
    name: Optional[str] = None
    match_field: str
    pattern: str
    direction: str
    account_id: Optional[UUID] = None
    category_id: UUID
    confidence: float
    priority: int
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


class BankImportResult(BaseModel):
    imported: int
    skipped_duplicates: int
    errors: list[str] = []


# -----------------------------
# Marketplaces (Ozon)
# -----------------------------

class MarketplaceConnectionCreate(BaseModel):
    marketplace: str = Field(default="ozon", pattern="^(ozon|wb|ymarket)$")
    name: str
    client_id: str
    api_key: str
    note: Optional[str] = None
    is_active: bool = True


class MarketplaceConnectionOut(BaseModel):
    id: UUID
    marketplace: str
    name: str
    client_id: str
    api_key_last4: str
    note: Optional[str] = None
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class MarketplaceConnectionPatch(BaseModel):
    name: Optional[str] = None
    client_id: Optional[str] = None
    api_key: Optional[str] = None
    note: Optional[str] = None
    is_active: Optional[bool] = None


class OzonFetchParams(BaseModel):
    connection_id: UUID
    date_from: date
    date_to: date


class OzonFetchResult(BaseModel):
    fetched: int
    inserted: int
    duplicates: int
    errors: list[str] = []


class OzonTransactionOut(BaseModel):
    id: UUID
    operation_id: str
    operation_date: datetime
    operation_type: Optional[str] = None
    operation_type_name: Optional[str] = None
    posting_number: Optional[str] = None
    type: Optional[str] = None
    amount: Optional[float] = None
    accruals_for_sale: Optional[float] = None
    sale_commission: Optional[float] = None
    delivery_charge: Optional[float] = None
    return_delivery_charge: Optional[float] = None

    class Config:
        from_attributes = True


class OzonSummary(BaseModel):
    tx_count: int
    amount_total: float
    sales_total: float
    commission_total: float
    delivery_total: float


class OzonFbsFetchParams(BaseModel):
    connection_id: UUID
    date_from: date
    date_to: date
    status: Optional[str] = None
    fetch_details: bool = False


class OzonFbsFetchResult(BaseModel):
    fetched: int
    created: int
    updated: int
    errors: list[str] = []


class OzonSyncParams(BaseModel):
    connection_id: UUID
    date_from: date
    date_to: date
    status: Optional[str] = None
    fetch_details: bool = False


class OzonSyncResult(BaseModel):
    finance: OzonFetchResult
    orders: OzonFbsFetchResult
    errors: list[str] = []



class OzonToLedgerParams(BaseModel):
    connection_id: UUID
    date_from: date
    date_to: date
    dry_run: bool = False


class OzonToLedgerResult(BaseModel):
    scanned: int
    inserted: int
    duplicates: int
    errors: list[str] = []


class BankOpMini(BaseModel):
    id: UUID
    posted_at: datetime
    amount: float
    counterparty: Optional[str] = None
    description: Optional[str] = None

    class Config:
        from_attributes = True


class OzonPayoutSuggestion(BaseModel):
    bank_op: BankOpMini
    score: float


class OzonPayoutReconRow(BaseModel):
    payout_key: str
    payout_date: date
    amount_marketplace: float
    expected_bank_in: float
    operation_ids: list[str] = []
    suggestions: list[OzonPayoutSuggestion] = []
    matched_bank_op_id: Optional[UUID] = None
    match_status: Optional[str] = None


class OzonPayoutAutoConfirmParams(BaseModel):
    connection_id: UUID
    date_from: date
    date_to: date
    bank_account_id: Optional[UUID] = None
    threshold: float = 0.85


class OzonPayoutAutoConfirmResult(BaseModel):
    scanned: int
    confirmed: int
    skipped_existing: int
    skipped_locked: int
    errors: list[str] = []




class OzonPeriodCheck(BaseModel):
    key: str
    title: str
    ok: bool
    value: Optional[str] = None
    hint: Optional[str] = None


class OzonPeriodTotals(BaseModel):
    tx_count: int
    amount_total: float
    sales_total: float
    commission_total: float
    delivery_total: float

    postings_count: int
    items_count: int
    items_total: float

    ledger_ops_count: int
    bank_ops_count: int

    payouts_detected: int
    payouts_matched: int
    payout_marketplace_total: float
    bank_matched_total: float


class OzonPeriodStatus(BaseModel):
    connection_id: UUID
    date_from: date
    date_to: date
    checks: list[OzonPeriodCheck]
    totals: OzonPeriodTotals

class OzonPostingItemOut(BaseModel):
    id: UUID
    product_id: Optional[str] = None
    offer_id: Optional[str] = None
    name: Optional[str] = None
    sku: Optional[str] = None
    quantity: Optional[int] = None
    price: Optional[float] = None

    class Config:
        from_attributes = True


class OzonPostingOut(BaseModel):
    id: UUID
    posting_number: str
    order_id: Optional[str] = None
    status: Optional[str] = None
    substatus: Optional[str] = None
    created_at: Optional[datetime] = None
    in_process_at: Optional[datetime] = None
    shipment_date: Optional[datetime] = None

    items: list[OzonPostingItemOut] = []
    items_count: int = 0
    qty_total: int = 0
    items_total: float = 0

    class Config:
        from_attributes = True


class OzonPostingsPage(BaseModel):
    postings: list[OzonPostingOut]
    has_next: bool
    next_offset: Optional[int] = None


class MoneyAllocationOut(BaseModel):
    id: UUID
    money_operation_id: UUID
    category_id: UUID
    amount_part: float
    linked_entity_type: Optional[str] = None
    linked_entity_id: Optional[str] = None
    method: str
    confidence: Optional[float] = None
    confirmed: bool
    note: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


class PeriodLockCreate(BaseModel):
    period: str  # YYYY-MM
    note: Optional[str] = None
    locked_by: Optional[str] = None


class PeriodLockOut(BaseModel):
    period: str
    locked_at: datetime
    locked_by: Optional[str] = None
    note: Optional[str] = None

    class Config:
        from_attributes = True


class ReconciliationMatchCreate(BaseModel):
    money_operation_id: UUID
    right_type: str
    right_id: str
    method: str = "manual"  # exact/rule/manual/ai
    score: Optional[float] = None
    status: str = "confirmed"  # suggested/confirmed/rejected
    note: Optional[str] = None


class ReconciliationMatchOut(BaseModel):
    id: UUID
    money_operation_id: UUID
    right_type: str
    right_id: str
    method: str
    score: Optional[float] = None
    status: str
    note: Optional[str] = None
    created_at: datetime
    confirmed_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class CashflowRow(BaseModel):
    date: date
    inflow: float
    outflow: float


class ProfitCashRow(BaseModel):
    date: date
    income: float
    expense: float
    profit: float


# -----------------------------
# Treasury / Cash plan
# -----------------------------

class CashPlanItemCreate(BaseModel):
    name: str
    direction: str = Field(default="out", pattern="^(in|out)$")
    amount: float
    currency: str = "RUB"
    account_id: Optional[UUID] = None
    category_id: Optional[UUID] = None

    schedule: str = Field(default="monthly", pattern="^(once|weekly|monthly)$")
    due_date: Optional[date] = None
    day_of_month: Optional[int] = None
    weekday: Optional[int] = None

    start_date: Optional[date] = None
    end_date: Optional[date] = None
    note: Optional[str] = None
    is_active: bool = True


class CashPlanItemPatch(BaseModel):
    name: Optional[str] = None
    direction: Optional[str] = Field(default=None, pattern="^(in|out)$")
    amount: Optional[float] = None
    currency: Optional[str] = None
    account_id: Optional[UUID] = None
    category_id: Optional[UUID] = None

    schedule: Optional[str] = Field(default=None, pattern="^(once|weekly|monthly)$")
    due_date: Optional[date] = None
    day_of_month: Optional[int] = None
    weekday: Optional[int] = None

    start_date: Optional[date] = None
    end_date: Optional[date] = None
    note: Optional[str] = None
    is_active: Optional[bool] = None


class CashPlanItemOut(BaseModel):
    id: UUID
    name: str
    direction: str
    amount: float
    currency: str
    account_id: Optional[UUID] = None
    category_id: Optional[UUID] = None
    schedule: str
    due_date: Optional[date] = None
    day_of_month: Optional[int] = None
    weekday: Optional[int] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    note: Optional[str] = None
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class CashForecastParams(BaseModel):
    date_from: Optional[date] = None
    days: int = 30
    account_id: Optional[UUID] = None


class CashForecastRow(BaseModel):
    date: date
    planned_in: float
    planned_out: float
    net: float
    balance: float


# ---------------------------
# Yandex Market (ymarket)
# ---------------------------

class YMarketCampaignOut(BaseModel):
    id: int
    domain: str | None = None
    business_id: int | None = None
    business_name: str | None = None
    placement_type: str | None = None
    api_availability: str | None = None

class YMarketOrdersFetchParams(BaseModel):
    connection_id: UUID
    date_from: date
    date_to: date
    limit: int = 50
    fake: bool = False
    statuses: list[str] | None = None

class YMarketOrderItemOut(BaseModel):
    offer_id: str | None = None
    shop_sku: str | None = None
    market_sku: str | None = None
    name: str | None = None
    quantity: int | None = None
    price: float | None = None
    line_total: float | None = None

class YMarketOrderOut(BaseModel):
    id: UUID
    connection_id: UUID
    order_id: int
    status: str | None = None
    substatus: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    shipment_date: date | None = None
    buyer_total: float | None = None
    items_total: float | None = None
    currency: str | None = None
    imported_at: datetime | None = None
    items: list[YMarketOrderItemOut] = Field(default_factory=list)

    class Config:
        from_attributes = True

class YMarketReportGenerateParams(BaseModel):
    connection_id: UUID
    date_from: date
    date_to: date
    placement_programs: list[str] = Field(default_factory=lambda: ["FBS"])
    # Partner API format enum: FILE (XLSX), CSV (ZIP), JSON (ZIP)
    # We also accept legacy "XLSX" in backend and map it to FILE.
    format: str = "FILE"  # FILE | CSV | JSON
    language: str = "RU"

class YMarketReportOut(BaseModel):
    id: UUID
    connection_id: UUID
    report_id: str
    report_type: str
    status: str | None = None
    file_url: str | None = None
    date_from: date | None = None
    date_to: date | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    class Config:
        from_attributes = True

class YMarketReportInfoOut(BaseModel):
    report_id: str
    status: str | None = None
    file_url: str | None = None
    raw: dict | None = None


# ---------------------------
# Wildberries (wb)
# ---------------------------


class WbPingOut(BaseModel):
    ok: bool
    status_code: int
    body: str | None = None


class WbFetchParams(BaseModel):
    connection_id: UUID
    date_from: date


class WbFetchResult(BaseModel):
    fetched: int
    inserted: int
    updated: int
    errors: list[str] = []


class WbOrderLineOut(BaseModel):
    id: UUID
    connection_id: UUID
    srid: str
    nm_id: int | None = None
    barcode: str | None = None
    supplier_article: str | None = None
    warehouse_name: str | None = None
    date: datetime | None = None
    last_change_date: datetime | None = None
    quantity: int | None = None
    total_price: float | None = None
    finished_price: float | None = None
    price_with_disc: float | None = None
    is_cancel: bool | None = None
    cancel_date: datetime | None = None

    class Config:
        from_attributes = True


class WbSaleLineOut(BaseModel):
    id: UUID
    connection_id: UUID
    sale_id: str
    srid: str | None = None
    nm_id: int | None = None
    barcode: str | None = None
    supplier_article: str | None = None
    warehouse_name: str | None = None
    date: datetime | None = None
    last_change_date: datetime | None = None
    quantity: int | None = None
    for_pay: float | None = None
    finished_price: float | None = None
    price_with_disc: float | None = None

    class Config:
        from_attributes = True


# ---------------------------
# FBS Builds
# ---------------------------


class FbsBuildCreateParams(BaseModel):
    marketplace: str
    connection_id: UUID
    order_ids: List[str] = Field(default_factory=list)
    title: Optional[str] = None
    note: Optional[str] = None


class FbsBuildAddOrdersParams(BaseModel):
    order_ids: List[str] = Field(default_factory=list)


class FbsBuildPatchParams(BaseModel):
    title: Optional[str] = None
    status: Optional[str] = None
    note: Optional[str] = None


class FbsBuildOrderOut(BaseModel):
    id: UUID
    external_order_id: str
    status: Optional[str] = None
    items_count: int = 0
    qty_total: int = 0
    items: List[Dict[str, Any]] = Field(default_factory=list)

    class Config:
        from_attributes = True


class FbsBuildItemAggOut(BaseModel):
    sku: Optional[str] = None
    offer_id: Optional[str] = None
    name: Optional[str] = None
    qty_total: int = 0
    orders_count: int = 0


class FbsBuildOut(BaseModel):
    id: UUID
    marketplace: str
    connection_id: UUID
    title: str
    status: str
    note: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    orders_count: int = 0
    items_count: int = 0
    qty_total: int = 0

    class Config:
        from_attributes = True


class FbsBuildDetailOut(FbsBuildOut):
    orders: List[FbsBuildOrderOut] = Field(default_factory=list)
    items: List[FbsBuildItemAggOut] = Field(default_factory=list)
