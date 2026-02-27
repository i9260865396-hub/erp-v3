from datetime import datetime, date
from sqlalchemy import String, Integer, BigInteger, Numeric, Boolean, Date, DateTime, ForeignKey, Text, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .db import Base

class Material(Base):
    __tablename__ = "materials"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, index=True)
    category: Mapped[str] = mapped_column(String(50))  # film/banner/ink/packaging/service
    base_uom: Mapped[str] = mapped_column(String(20))  # m2/ml/pcs/min
    is_lot_tracked: Mapped[bool] = mapped_column(Boolean, default=True)
    is_void: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    voided_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    void_reason: Mapped[str | None] = mapped_column(String(300), nullable=True)


    props = relationship("MaterialProp", back_populates="material", cascade="all, delete-orphan")

    @property
    def props_dict(self) -> dict:
        return {p.key: p.value for p in (self.props or [])}

class MaterialProp(Base):
    __tablename__ = "material_props"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    material_id: Mapped[int] = mapped_column(ForeignKey("materials.id", ondelete="CASCADE"))
    key: Mapped[str] = mapped_column(String(100), index=True)
    value: Mapped[str] = mapped_column(String(200))
    value_type: Mapped[str] = mapped_column(String(20), default="str")  # str/num/bool

    material = relationship("Material", back_populates="props")

class PurchaseDoc(Base):
    __tablename__ = "purchase_docs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    doc_date: Mapped[date] = mapped_column(Date)
    supplier: Mapped[str] = mapped_column(String(200))
    doc_no: Mapped[str] = mapped_column(String(100))
    pay_type: Mapped[str] = mapped_column(String(30))  # cash/card/bank
    vat_mode: Mapped[str] = mapped_column(String(30))  # with_vat/no_vat
    comment: Mapped[str | None] = mapped_column(String(500), nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="DRAFT", index=True)  # DRAFT/POSTED/VOID
    posted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    is_void: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    voided_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    void_reason: Mapped[str | None] = mapped_column(String(300), nullable=True)
    
    lines = relationship("PurchaseLine", back_populates="doc", cascade="all, delete-orphan")

class PurchaseLine(Base):
    __tablename__ = "purchase_lines"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    purchase_doc_id: Mapped[int] = mapped_column(ForeignKey("purchase_docs.id", ondelete="CASCADE"))
    material_id: Mapped[int] = mapped_column(ForeignKey("materials.id"))
    qty: Mapped[float] = mapped_column(Numeric(14, 4))
    uom: Mapped[str] = mapped_column(String(20))
    unit_price: Mapped[float] = mapped_column(Numeric(14, 4))
    vat_rate: Mapped[float] = mapped_column(Numeric(6, 3), default=0)

    doc = relationship("PurchaseDoc", back_populates="lines")
    material = relationship("Material")
    lot = relationship("Lot", back_populates="purchase_line", uselist=False, cascade="all, delete-orphan")

class Lot(Base):
    __tablename__ = "lots"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    material_id: Mapped[int] = mapped_column(ForeignKey("materials.id"))
    purchase_line_id: Mapped[int] = mapped_column(ForeignKey("purchase_lines.id", ondelete="CASCADE"), unique=True)
    qty_in: Mapped[float] = mapped_column(Numeric(14, 4))
    qty_out: Mapped[float] = mapped_column(Numeric(14, 4), default=0)
    unit_cost: Mapped[float] = mapped_column(Numeric(14, 4))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    material = relationship("Material")
    purchase_line = relationship("PurchaseLine", back_populates="lot")
    movements = relationship("LotMovement", back_populates="lot", cascade="all, delete-orphan")

class LotMovement(Base):
    __tablename__ = "lot_movements"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    lot_id: Mapped[int] = mapped_column(ForeignKey("lots.id", ondelete="CASCADE"))
    mv_date: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    mv_type: Mapped[str] = mapped_column(String(10))  # IN/OUT/SCRAP/ADJUST
    qty: Mapped[float] = mapped_column(Numeric(14, 4))
    ref_type: Mapped[str | None] = mapped_column(String(20), nullable=True)
    ref_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    lot = relationship("Lot", back_populates="movements")


class WriteoffDoc(Base):
    __tablename__ = "writeoff_docs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    doc_date: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    reason: Mapped[str] = mapped_column(String(30))  # production/scrap/other
    comment: Mapped[str | None] = mapped_column(String(500), nullable=True)

    lines = relationship("WriteoffLine", back_populates="doc", cascade="all, delete-orphan")


class WriteoffLine(Base):
    __tablename__ = "writeoff_lines"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    writeoff_doc_id: Mapped[int] = mapped_column(ForeignKey("writeoff_docs.id", ondelete="CASCADE"))
    material_id: Mapped[int] = mapped_column(ForeignKey("materials.id"))
    qty_base: Mapped[float] = mapped_column(Numeric(14, 4))
    base_uom: Mapped[str] = mapped_column(String(20))
    qty_input: Mapped[float] = mapped_column(Numeric(14, 4))
    uom_input: Mapped[str] = mapped_column(String(20))

    doc = relationship("WriteoffDoc", back_populates="lines")
    material = relationship("Material")

class Employee(Base):
    __tablename__ = "employees"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), unique=True)
    hourly_rate: Mapped[float] = mapped_column(Numeric(14, 4), default=0)

class Order(Base):
    __tablename__ = "orders"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_date: Mapped[date] = mapped_column(Date)
    status: Mapped[str] = mapped_column(String(20), default="DRAFT")
    comment: Mapped[str | None] = mapped_column(String(500), nullable=True)

    items = relationship("OrderItem", back_populates="order", cascade="all, delete-orphan")
    consumption = relationship("OrderConsumption", back_populates="order", cascade="all, delete-orphan")
    labor = relationship("OrderLabor", back_populates="order", cascade="all, delete-orphan")
    ink = relationship("OrderInkUsage", back_populates="order", uselist=False, cascade="all, delete-orphan")

class OrderItem(Base):
    __tablename__ = "order_items"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"))
    product_name: Mapped[str] = mapped_column(String(200))
    qty: Mapped[int] = mapped_column(Integer, default=1)
    width_m: Mapped[float] = mapped_column(Numeric(14, 4))
    height_m: Mapped[float] = mapped_column(Numeric(14, 4))

    order = relationship("Order", back_populates="items")

class OrderConsumption(Base):
    __tablename__ = "order_consumption"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"))
    material_id: Mapped[int] = mapped_column(ForeignKey("materials.id"))
    lot_id: Mapped[int] = mapped_column(ForeignKey("lots.id"))
    qty: Mapped[float] = mapped_column(Numeric(14, 4))
    uom: Mapped[str] = mapped_column(String(20))
    fifo_cost: Mapped[float] = mapped_column(Numeric(14, 4))

    order = relationship("Order", back_populates="consumption")
    material = relationship("Material")
    lot = relationship("Lot")

class OrderLabor(Base):
    __tablename__ = "order_labor"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"))
    employee_id: Mapped[int] = mapped_column(ForeignKey("employees.id"))
    minutes: Mapped[int] = mapped_column(Integer)
    rate_rub_per_hour: Mapped[float] = mapped_column(Numeric(14, 4))
    labor_cost: Mapped[float] = mapped_column(Numeric(14, 4))

    order = relationship("Order", back_populates="labor")
    employee = relationship("Employee")

class OrderInkUsage(Base):
    __tablename__ = "order_ink_usage"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"), unique=True)
    c_ml: Mapped[float] = mapped_column(Numeric(14, 4), default=0)
    m_ml: Mapped[float] = mapped_column(Numeric(14, 4), default=0)
    y_ml: Mapped[float] = mapped_column(Numeric(14, 4), default=0)
    k_ml: Mapped[float] = mapped_column(Numeric(14, 4), default=0)
    ink_cost: Mapped[float] = mapped_column(Numeric(14, 4), default=0)

    order = relationship("Order", back_populates="ink")

class Sale(Base):
    __tablename__ = "sales"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sale_date: Mapped[date] = mapped_column(Date)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"))
    marketplace: Mapped[str] = mapped_column(String(30))
    gross_price: Mapped[float] = mapped_column(Numeric(14, 4))
    vat_rate: Mapped[float] = mapped_column(Numeric(6, 3), default=0)

    order = relationship("Order")
    charges = relationship("SaleCharge", back_populates="sale", cascade="all, delete-orphan")

class SaleCharge(Base):
    __tablename__ = "sale_charges"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sale_id: Mapped[int] = mapped_column(ForeignKey("sales.id", ondelete="CASCADE"))
    charge_type: Mapped[str] = mapped_column(String(30))
    amount: Mapped[float] = mapped_column(Numeric(14, 4))
    comment: Mapped[str | None] = mapped_column(String(200), nullable=True)

    sale = relationship("Sale", back_populates="charges")



class BizOrder(Base):
    __tablename__ = "biz_orders"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_date: Mapped[date] = mapped_column(Date, index=True)
    channel: Mapped[str] = mapped_column(String(30), index=True)  # WB/Ozon/Сайт/Онлайн/Авито/Офлайн/Опт
    subchannel: Mapped[str | None] = mapped_column(String(60), nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="OPEN", index=True)  # OPEN/CLOSED/VOID
    revenue: Mapped[float] = mapped_column(Numeric(14, 4), default=0)
    comment: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

class Expense(Base):
    __tablename__ = "expenses"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    exp_date: Mapped[date] = mapped_column(Date, index=True)
    category: Mapped[str] = mapped_column(String(40), index=True)  # rent/ads/service/delivery/other
    amount: Mapped[float] = mapped_column(Numeric(14, 4))
    channel: Mapped[str | None] = mapped_column(String(30), nullable=True, index=True)
    comment: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


# -----------------------------
# Money Ledger (immutable facts + editable interpretation)
# -----------------------------

class MoneyAccount(Base):
    __tablename__ = "money_accounts"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    type: Mapped[str] = mapped_column(String(20), index=True)  # bank/cash/marketplace/acquiring/other
    name: Mapped[str] = mapped_column(String(200), unique=True, index=True)
    currency: Mapped[str] = mapped_column(String(3), default="RUB")
    external_ref: Mapped[str | None] = mapped_column(String(200), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    opened_at: Mapped[date | None] = mapped_column(Date, nullable=True)
    closed_at: Mapped[date | None] = mapped_column(Date, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class MoneyOperation(Base):
    __tablename__ = "money_operations"
    __table_args__ = (
        # антидубль по внешнему id, если он есть
        UniqueConstraint("source", "account_id", "external_id", name="uq_moneyop_source_account_external"),
        Index("ix_moneyop_posted_at", "posted_at"),
        Index("ix_moneyop_account_posted", "account_id", "posted_at"),
        Index("ix_moneyop_transfer_group", "transfer_group_id"),
        Index("ix_moneyop_fingerprint", "hash_fingerprint"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("money_accounts.id"))
    transfer_group_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    posted_at: Mapped[datetime] = mapped_column(DateTime, index=True)
    amount: Mapped[float] = mapped_column(Numeric(18, 2))  # signed: +in / -out
    currency: Mapped[str] = mapped_column(String(3), default="RUB")
    counterparty: Mapped[str | None] = mapped_column(String(250), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    operation_type: Mapped[str] = mapped_column(String(20), default="other")  # payment/transfer/refund/fee/payout/...
    external_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    source: Mapped[str] = mapped_column(String(40), index=True)  # bank_import/cash_manual/marketplace_import/acquiring_import/manual_other
    raw_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    hash_fingerprint: Mapped[str | None] = mapped_column(String(128), nullable=True)

    is_void: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    void_reason: Mapped[str | None] = mapped_column(String(300), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    account = relationship("MoneyAccount")
    allocations = relationship("MoneyAllocation", back_populates="operation", cascade="all, delete-orphan")


class Category(Base):
    __tablename__ = "categories"
    __table_args__ = (
        Index("ix_category_parent", "parent_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(200), index=True)
    type: Mapped[str] = mapped_column(String(20), index=True)  # income/expense/transfer/balance_adjustment
    parent_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("categories.id"), nullable=True)
    is_tax_related: Mapped[bool] = mapped_column(Boolean, default=False)
    is_payroll_related: Mapped[bool] = mapped_column(Boolean, default=False)
    is_system: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    parent = relationship("Category", remote_side=[id], uselist=False)


class MoneyAllocation(Base):
    __tablename__ = "money_allocations"
    __table_args__ = (
        Index("ix_alloc_op", "money_operation_id"),
        Index("ix_alloc_category", "category_id"),
        Index("ix_alloc_link", "linked_entity_type", "linked_entity_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    money_operation_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("money_operations.id", ondelete="CASCADE"))
    category_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("categories.id"))
    amount_part: Mapped[float] = mapped_column(Numeric(18, 2))
    linked_entity_type: Mapped[str | None] = mapped_column(String(30), nullable=True)  # purchase/sale/payroll/tax/other
    linked_entity_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    method: Mapped[str] = mapped_column(String(10), default="manual")  # manual/rule/ai
    confidence: Mapped[float | None] = mapped_column(Numeric(4, 3), nullable=True)
    confirmed: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    note: Mapped[str | None] = mapped_column(String(300), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    operation = relationship("MoneyOperation", back_populates="allocations")
    category = relationship("Category")


class MoneyRule(Base):
    """User-editable rules for auto allocation (suggestions).

    Important: rules NEVER directly confirm anything. They only create suggestions
    (MoneyAllocation with confirmed=False). User confirms manually or via batch.
    """

    __tablename__ = "money_rules"
    __table_args__ = (
        Index("ix_money_rules_active", "is_active"),
        Index("ix_money_rules_priority", "priority"),
        Index("ix_money_rules_category", "category_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    match_field: Mapped[str] = mapped_column(String(20), default="text")  # text/counterparty/description/source
    pattern: Mapped[str] = mapped_column(String(500))  # supports '|' separated keywords (case-insensitive)
    direction: Mapped[str] = mapped_column(String(10), default="any")  # any/in/out
    account_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("money_accounts.id"), nullable=True)
    category_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("categories.id"))
    confidence: Mapped[float] = mapped_column(Numeric(4, 3), default=0.95)
    priority: Mapped[int] = mapped_column(Integer, default=100)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    category = relationship("Category")
    account = relationship("MoneyAccount")


class AuditLog(Base):
    __tablename__ = "audit_log"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_type: Mapped[str] = mapped_column(String(60), index=True)
    entity_id: Mapped[str] = mapped_column(String(64), index=True)
    action: Mapped[str] = mapped_column(String(20))  # create/update/confirm/void
    changed_fields: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    actor: Mapped[str | None] = mapped_column(String(80), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class PeriodLock(Base):
    __tablename__ = "period_locks"
    period: Mapped[str] = mapped_column(String(7), primary_key=True)  # YYYY-MM
    locked_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    locked_by: Mapped[str | None] = mapped_column(String(80), nullable=True)
    note: Mapped[str | None] = mapped_column(String(300), nullable=True)


class ReconciliationMatch(Base):
    __tablename__ = "reconciliation_matches"
    __table_args__ = (
        UniqueConstraint("money_operation_id", "right_type", "right_id", name="uq_recon_unique"),
        Index("ix_recon_op", "money_operation_id"),
        Index("ix_recon_right", "right_type", "right_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    money_operation_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("money_operations.id", ondelete="CASCADE"))
    right_type: Mapped[str] = mapped_column(String(30))  # purchase/sale/order/expense/other
    right_id: Mapped[str] = mapped_column(String(64))
    method: Mapped[str] = mapped_column(String(12), default="manual")  # exact/rule/manual/ai
    score: Mapped[float | None] = mapped_column(Numeric(6, 3), nullable=True)
    status: Mapped[str] = mapped_column(String(12), default="suggested", index=True)  # suggested/confirmed/rejected
    note: Mapped[str | None] = mapped_column(String(300), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    operation = relationship("MoneyOperation")

# -----------------------------
# Treasury / Cash plan (planned items for forecast)
# -----------------------------

class CashPlanItem(Base):
    """Плановый денежный поток (для прогноза).

    Это НЕ факт денег. Это управленческий план, который можно менять.
    """

    __tablename__ = "cash_plan_items"
    __table_args__ = (
        Index("ix_cashplan_active", "is_active"),
        Index("ix_cashplan_schedule", "schedule"),
        Index("ix_cashplan_direction", "direction"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(200), index=True)
    direction: Mapped[str] = mapped_column(String(3), default="out", index=True)  # in/out
    amount: Mapped[float] = mapped_column(Numeric(18, 2))  # positive
    currency: Mapped[str] = mapped_column(String(3), default="RUB")

    # Optional links
    account_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("money_accounts.id"), nullable=True)
    category_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("categories.id"), nullable=True)

    # Schedule
    schedule: Mapped[str] = mapped_column(String(10), default="monthly", index=True)  # once/weekly/monthly
    due_date: Mapped[date | None] = mapped_column(Date, nullable=True)  # for once
    day_of_month: Mapped[int | None] = mapped_column(Integer, nullable=True)  # for monthly
    weekday: Mapped[int | None] = mapped_column(Integer, nullable=True)  # for weekly: 0=Mon..6=Sun

    start_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    end_date: Mapped[date | None] = mapped_column(Date, nullable=True)

    note: Mapped[str | None] = mapped_column(String(300), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    account = relationship("MoneyAccount")
    category = relationship("Category")


# -----------------------------
# Marketplaces (Accrual/Economics layer)
# -----------------------------

class MarketplaceConnection(Base):
    """Credentials/config for marketplace API.

    В MVP храним ключи в БД (plaintext). Позже можно заменить на KMS/ENV.
    """

    __tablename__ = "marketplace_connections"
    __table_args__ = (
        UniqueConstraint("marketplace", "name", name="uq_mp_conn_marketplace_name"),
        Index("ix_mp_conn_marketplace", "marketplace"),
        Index("ix_mp_conn_active", "is_active"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    marketplace: Mapped[str] = mapped_column(String(20))  # ozon/wb/ymarket/...
    name: Mapped[str] = mapped_column(String(120))
    client_id: Mapped[str] = mapped_column(String(120))
    api_key: Mapped[str] = mapped_column(String(300))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    note: Mapped[str | None] = mapped_column(String(300), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class OzonTransaction(Base):
    """Raw finance transaction row from Ozon Seller API.

    Это НЕ деньги. Это начисления/удержания (accrual).
    """

    __tablename__ = "ozon_transactions"
    __table_args__ = (
        UniqueConstraint("connection_id", "operation_id", name="uq_ozon_conn_operation"),
        Index("ix_ozon_op_date", "operation_date"),
        Index("ix_ozon_type", "operation_type"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    connection_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("marketplace_connections.id", ondelete="CASCADE"))

    operation_id: Mapped[str] = mapped_column(String(64))
    operation_date: Mapped[datetime] = mapped_column(DateTime, index=True)
    operation_type: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    operation_type_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    posting_number: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    type: Mapped[str | None] = mapped_column(String(40), nullable=True, index=True)

    # key financial fields (best-effort)
    amount: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    accruals_for_sale: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    sale_commission: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    delivery_charge: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    return_delivery_charge: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)

    raw_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    imported_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    connection = relationship("MarketplaceConnection")


# ---------------------------
# Yandex Market integration
# ---------------------------


# -----------------------------
# Ozon (FBS postings)
# -----------------------------

class OzonPosting(Base):
    __tablename__ = "ozon_postings"
    __table_args__ = (
        UniqueConstraint("connection_id", "posting_number", name="uq_ozon_conn_posting"),
        Index("ix_ozon_postings_conn", "connection_id"),
        Index("ix_ozon_postings_status", "status"),
        Index("ix_ozon_postings_created", "created_at"),
        Index("ix_ozon_postings_imported", "imported_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    connection_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("marketplace_connections.id", ondelete="CASCADE"), index=True
    )

    posting_number: Mapped[str] = mapped_column(String(80))
    order_id: Mapped[str | None] = mapped_column(String(80), nullable=True)

    status: Mapped[str | None] = mapped_column(String(40), nullable=True)
    substatus: Mapped[str | None] = mapped_column(String(80), nullable=True)

    created_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    in_process_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    shipment_date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    raw_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    imported_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    connection = relationship("MarketplaceConnection")
    items = relationship("OzonPostingItem", back_populates="posting", cascade="all, delete-orphan")


class OzonPostingItem(Base):
    __tablename__ = "ozon_posting_items"
    __table_args__ = (
        Index("ix_ozon_posting_items_posting", "posting_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    posting_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("ozon_postings.id", ondelete="CASCADE"), index=True
    )

    product_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    offer_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    name: Mapped[str | None] = mapped_column(String(300), nullable=True)
    sku: Mapped[str | None] = mapped_column(String(80), nullable=True)

    quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    price: Mapped[float | None] = mapped_column(Numeric(14, 4), nullable=True)

    raw_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    posting = relationship("OzonPosting", back_populates="items")


class YMarketOrder(Base):
    """Order snapshot from Yandex Market Partner API (GET v2/campaigns/{campaignId}/orders).

    Примечание: API имеет ограничения (не возвращает доставленные/отменённые > 30 дней назад).
    Для истории можно использовать POST v1/businesses/{businessId}/orders (позже добавим).
    """

    __tablename__ = "ymarket_orders"
    __table_args__ = (
        UniqueConstraint("connection_id", "order_id", name="uq_ymarket_conn_order"),
        Index("ix_ymarket_order_created", "created_at"),
        Index("ix_ymarket_order_status", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    connection_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("marketplace_connections.id", ondelete="CASCADE"), index=True)

    order_id: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[str | None] = mapped_column(String(40), nullable=True)
    substatus: Mapped[str | None] = mapped_column(String(60), nullable=True)

    created_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    shipment_date: Mapped[date | None] = mapped_column(Date, nullable=True)

    buyer_total: Mapped[float | None] = mapped_column(Numeric(14, 4), nullable=True)
    items_total: Mapped[float | None] = mapped_column(Numeric(14, 4), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(10), nullable=True)

    raw_payload: Mapped[dict] = mapped_column(JSONB, default=dict)
    imported_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    items = relationship("YMarketOrderItem", back_populates="order", cascade="all, delete-orphan")


class YMarketOrderItem(Base):
    __tablename__ = "ymarket_order_items"
    __table_args__ = (
        Index("ix_ymarket_item_order", "ymarket_order_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    ymarket_order_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("ymarket_orders.id", ondelete="CASCADE"), index=True)

    offer_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    shop_sku: Mapped[str | None] = mapped_column(String(80), nullable=True)
    market_sku: Mapped[str | None] = mapped_column(String(80), nullable=True)
    name: Mapped[str | None] = mapped_column(String(400), nullable=True)

    quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    price: Mapped[float | None] = mapped_column(Numeric(14, 4), nullable=True)
    line_total: Mapped[float | None] = mapped_column(Numeric(14, 4), nullable=True)

    raw_payload: Mapped[dict] = mapped_column(JSONB, default=dict)

    order = relationship("YMarketOrder", back_populates="items")


class YMarketReport(Base):
    """Generated report metadata (e.g. United netting).

    File itself is downloaded via link returned by GET v2/reports/info/{reportId}.
    """

    __tablename__ = "ymarket_reports"
    __table_args__ = (
        UniqueConstraint("connection_id", "report_id", name="uq_ymarket_conn_report"),
        Index("ix_ymarket_report_created", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    connection_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("marketplace_connections.id", ondelete="CASCADE"), index=True)

    report_id: Mapped[str] = mapped_column(String(120))
    report_type: Mapped[str] = mapped_column(String(50), default="united_netting")  # future: sales, returns, etc.
    status: Mapped[str | None] = mapped_column(String(40), nullable=True)
    file_url: Mapped[str | None] = mapped_column(Text, nullable=True)

    date_from: Mapped[date | None] = mapped_column(Date, nullable=True)
    date_to: Mapped[date | None] = mapped_column(Date, nullable=True)

    raw_payload: Mapped[dict] = mapped_column(JSONB, default=dict)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


# ---------------------------
# Wildberries (wb) integration
# ---------------------------


class WbOrderLine(Base):
    """Order lines from WB Statistics API (supplier/orders).

    В WB статистике 1 строка = 1 товарная позиция в заказе.
    Для идентификации заказа используется srid.
    """

    __tablename__ = "wb_order_lines"
    __table_args__ = (
        UniqueConstraint("connection_id", "srid", "nm_id", "barcode", name="uq_wb_conn_srid_nm_barcode"),
        Index("ix_wb_orders_date", "date"),
        Index("ix_wb_orders_last_change", "last_change_date"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    connection_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("marketplace_connections.id", ondelete="CASCADE"), index=True
    )

    srid: Mapped[str] = mapped_column(String(120))
    nm_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    barcode: Mapped[str | None] = mapped_column(String(40), nullable=True)
    supplier_article: Mapped[str | None] = mapped_column(String(120), nullable=True)
    warehouse_name: Mapped[str | None] = mapped_column(String(120), nullable=True)

    date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_change_date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_price: Mapped[float | None] = mapped_column(Numeric(14, 4), nullable=True)
    finished_price: Mapped[float | None] = mapped_column(Numeric(14, 4), nullable=True)
    price_with_disc: Mapped[float | None] = mapped_column(Numeric(14, 4), nullable=True)

    is_cancel: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    cancel_date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    raw_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    imported_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    connection = relationship("MarketplaceConnection")


class WbSaleLine(Base):
    """Sales/returns lines from WB Statistics API (supplier/sales)."""

    __tablename__ = "wb_sale_lines"
    __table_args__ = (
        UniqueConstraint("connection_id", "sale_id", name="uq_wb_conn_sale_id"),
        Index("ix_wb_sales_date", "date"),
        Index("ix_wb_sales_last_change", "last_change_date"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    connection_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("marketplace_connections.id", ondelete="CASCADE"), index=True
    )

    sale_id: Mapped[str] = mapped_column(String(40))
    srid: Mapped[str | None] = mapped_column(String(120), nullable=True)
    nm_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    barcode: Mapped[str | None] = mapped_column(String(40), nullable=True)
    supplier_article: Mapped[str | None] = mapped_column(String(120), nullable=True)
    warehouse_name: Mapped[str | None] = mapped_column(String(120), nullable=True)

    date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_change_date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    for_pay: Mapped[float | None] = mapped_column(Numeric(14, 4), nullable=True)
    finished_price: Mapped[float | None] = mapped_column(Numeric(14, 4), nullable=True)
    price_with_disc: Mapped[float | None] = mapped_column(Numeric(14, 4), nullable=True)

    raw_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    imported_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    connection = relationship("MarketplaceConnection")


# ---------------------------
# FBS Builds (internal picking/packing batches)
# ---------------------------


class FbsBuild(Base):
    """Internal "Сборка" entity.

    Это внутренняя сущность ERP, которой может не быть в API маркетплейса.
    Сборка группирует набор FBS-заказов и позволяет вести стадии:
    draft -> picking -> packed -> shipped -> closed/cancelled.
    """

    __tablename__ = "fbs_builds"
    __table_args__ = (
        Index("ix_fbs_builds_created", "created_at"),
        Index("ix_fbs_builds_status", "status"),
        Index("ix_fbs_builds_conn", "connection_id"),
        Index("ix_fbs_builds_marketplace", "marketplace"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    marketplace: Mapped[str] = mapped_column(String(20))  # ozon/ymarket/wb
    connection_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("marketplace_connections.id", ondelete="CASCADE"), index=True
    )

    title: Mapped[str] = mapped_column(String(160), default="Сборка")
    status: Mapped[str] = mapped_column(String(20), default="draft")
    note: Mapped[str | None] = mapped_column(String(300), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    connection = relationship("MarketplaceConnection")
    orders = relationship("FbsBuildOrder", back_populates="build", cascade="all, delete-orphan")


class FbsBuildOrder(Base):
    """Order included in an internal build."""

    __tablename__ = "fbs_build_orders"
    __table_args__ = (
        UniqueConstraint("build_id", "external_order_id", name="uq_fbs_build_order"),
        Index("ix_fbs_build_orders_build", "build_id"),
        Index("ix_fbs_build_orders_conn", "connection_id"),
        Index("ix_fbs_build_orders_marketplace", "marketplace"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    build_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("fbs_builds.id", ondelete="CASCADE"), index=True
    )

    marketplace: Mapped[str] = mapped_column(String(20))
    connection_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("marketplace_connections.id", ondelete="CASCADE"), index=True
    )

    # posting_number (Ozon) / order_id (YMarket) / srid (WB)
    external_order_id: Mapped[str] = mapped_column(String(120))
    status: Mapped[str | None] = mapped_column(String(40), nullable=True)

    order_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    items_payload: Mapped[list | None] = mapped_column(JSONB, nullable=True)  # list of dicts {sku, offer_id, name, qty, price}

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    build = relationship("FbsBuild", back_populates="orders")
    connection = relationship("MarketplaceConnection")
