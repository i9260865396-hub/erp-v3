from datetime import datetime
from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import PurchaseDoc, PurchaseLine, Lot, LotMovement, Material, MaterialProp


def _material_props(db: Session, material_id: int) -> dict[str, str]:
    rows = db.execute(select(MaterialProp).where(MaterialProp.material_id == material_id)).scalars().all()
    return {r.key: r.value for r in rows}


def _convert_to_base_qty(
    *,
    base_uom: str,
    purchase_uom: str,
    qty: float,
    props: dict[str, str],
    uom_factor: float | None = None,
    roll_length_m: float | None = None,
) -> float:
    """Конвертирует количество из единицы закупки в базовую единицу материала.

    base_uom: m2/sheet/ml/g/pcs
    purchase_uom: roll/mp/m2, box/pack/sheet, l/ml, kg/g, pcs
    """
    if uom_factor and uom_factor > 0:
        return float(qty) * float(uom_factor)

    pu = (purchase_uom or "").strip().lower()
    bu = (base_uom or "").strip().lower()

    # same
    if pu == bu:
        return float(qty)

    # рулоны -> м2
    if bu == "m2":
        width_m = float(props.get("width_m", "0") or 0)
        default_len = float(props.get("default_length_m", "0") or 0)
        if pu in ("roll", "рулон"):
            length = float(roll_length_m or default_len)
            if width_m <= 0 or length <= 0:
                raise ValueError("Для рулонного материала нужны ширина и длина рулона")
            return float(qty) * width_m * length
        if pu in ("mp", "m", "м.п.", "пог.м", "pog"):
            if width_m <= 0:
                raise ValueError("Для списания/учёта в м.п. нужна ширина рулона")
            return float(qty) * width_m
        if pu in ("m2",):
            return float(qty)

    # листовые: коробка/пачка -> лист
    if bu in ("sheet", "лист"):
        sheets_per_pack = float(props.get("sheets_per_pack", "0") or 0)
        packs_per_box = float(props.get("packs_per_box", "0") or 0)
        if pu in ("pack", "пачка"):
            if sheets_per_pack <= 0:
                raise ValueError("Не задано: листов в пачке")
            return float(qty) * sheets_per_pack
        if pu in ("box", "коробка"):
            if sheets_per_pack <= 0 or packs_per_box <= 0:
                raise ValueError("Не задано: пачек в коробке и/или листов в пачке")
            return float(qty) * packs_per_box * sheets_per_pack
        if pu in ("sheet", "лист"):
            return float(qty)

    # жидкости: л -> мл
    if bu == "ml":
        if pu in ("l", "л"):
            return float(qty) * 1000.0
        if pu == "ml":
            return float(qty)

    # сыпучие: кг -> г
    if bu == "g":
        if pu in ("kg", "кг"):
            return float(qty) * 1000.0
        if pu == "g":
            return float(qty)

    # штучные
    if bu in ("pcs", "шт"):
        if pu in ("pcs", "шт"):
            return float(qty)

    raise ValueError(f"Не знаю как пересчитать {purchase_uom} -> {base_uom}")


def create_purchase(db: Session, payload) -> PurchaseDoc:
    doc = PurchaseDoc(
        doc_date=payload.doc_date,
        supplier=payload.supplier,
        doc_no=payload.doc_no,
        pay_type=payload.pay_type,
        vat_mode=payload.vat_mode,
        comment=payload.comment,
        status="DRAFT",
        is_void=False,
        posted_at=None,
        voided_at=None,
        void_reason=None,
    )
    db.add(doc)
    db.flush()

    for ln in payload.lines:
        material_id = ln.material_id

        # Р°РІС‚Рѕ-СЃРѕР·РґР°РЅРёРµ РјР°С‚РµСЂРёР°Р»Р° РїРѕ РёРјРµРЅРё (РµСЃР»Рё РЅРµ РїРµСЂРµРґР°Р»Рё id)
        if (not material_id) and getattr(ln, "material_name", None):
            name = ln.material_name.strip()
            existing = db.execute(select(Material).where(Material.name == name)).scalars().first()
            if existing:
                material_id = existing.id
            else:
                m = Material(
                    name=name,
                    category="film",
                    base_uom=ln.uom or "m2",
                    is_lot_tracked=True,
                )
                db.add(m)
                db.flush()
                material_id = m.id

        material = db.get(Material, material_id)
        if not material:
            raise ValueError(f"Material not found: {material_id}")

        props = _material_props(db, material_id)
        qty_base = _convert_to_base_qty(
            base_uom=material.base_uom,
            purchase_uom=ln.uom,
            qty=float(ln.qty),
            props=props,
            uom_factor=getattr(ln, "uom_factor", None),
            roll_length_m=getattr(ln, "roll_length_m", None),
        )
        if qty_base <= 0:
            raise ValueError("Количество должно быть > 0")

        total_cost = float(ln.qty) * float(ln.unit_price)
        unit_cost_base = total_cost / qty_base

        line = PurchaseLine(
            purchase_doc_id=doc.id,
            material_id=material_id,
            qty=qty_base,
            uom=material.base_uom,
            unit_price=unit_cost_base,
            vat_rate=ln.vat_rate,
        )
        db.add(line)
    return doc

def post_purchase(db: Session, doc_id: int) -> int:
    doc = db.get(PurchaseDoc, doc_id)
    if not doc:
        raise ValueError("Purchase doc not found")
    if doc.is_void or doc.status == "VOID":
        raise ValueError("Purchase doc is VOID")
    if doc.status == "POSTED":
        return 0

    lots_created = 0

    # СЃРѕР·РґР°С‘Рј РїР°СЂС‚РёРё РїРѕ СЃС‚СЂРѕРєР°Рј; Р°РЅС‚РёРґСѓР±Р»СЊ: Сѓ Lot purchase_line_id UNIQUE
    for line in doc.lines:
        existing_lot = db.execute(
            select(Lot).where(Lot.purchase_line_id == line.id)
        ).scalars().first()
        if existing_lot:
            continue

        lot = Lot(
            material_id=line.material_id,
            purchase_line_id=line.id,
            qty_in=line.qty,
            qty_out=0,
            unit_cost=line.unit_price,
        )
        db.add(lot)
        db.flush()

        mv = LotMovement(
            lot_id=lot.id,
            mv_date=datetime.utcnow(),
            mv_type="IN",
            qty=line.qty,
            ref_type="PURCHASE",
            ref_id=doc.id,
        )
        db.add(mv)
        lots_created += 1

    doc.status = "POSTED"
    doc.posted_at = datetime.utcnow()
    return lots_created


def void_purchase(db: Session, doc_id: int, reason: str | None = None) -> None:
    doc = db.get(PurchaseDoc, doc_id)
    if not doc:
        raise ValueError("Purchase doc not found")

    # РїРѕРєР° СѓРїСЂРѕС‰РµРЅРёРµ MVP: VOID СЂР°Р·СЂРµС€Р°РµРј С‚РѕР»СЊРєРѕ РґР»СЏ DRAFT
    if doc.status == "POSTED":
        raise ValueError("Cannot VOID POSTED doc in MVP (need storno).")

    doc.is_void = True
    doc.status = "VOID"
    doc.voided_at = datetime.utcnow()
    doc.void_reason = (reason or "").strip() or None



# --- Biz Orders / Expenses ---
from datetime import date
from sqlalchemy import func, and_
from .models import BizOrder, Expense

def create_biz_order(db: Session, *, order_date: date, channel: str, subchannel: str | None, revenue: float, comment: str | None) -> BizOrder:
    obj = BizOrder(order_date=order_date, channel=channel, subchannel=subchannel, revenue=revenue, comment=comment, status="OPEN")
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj

def list_biz_orders(db: Session, *, date_from: date | None = None, date_to: date | None = None):
    q = select(BizOrder).order_by(BizOrder.order_date.desc(), BizOrder.id.desc())
    if date_from:
        q = q.where(BizOrder.order_date >= date_from)
    if date_to:
        q = q.where(BizOrder.order_date <= date_to)
    return db.execute(q).scalars().all()

def update_biz_order(db: Session, order_id: int, patch: dict) -> BizOrder:
    obj = db.get(BizOrder, order_id)
    if not obj:
        raise ValueError("order not found")
    for k, v in patch.items():
        if hasattr(obj, k) and v is not None:
            setattr(obj, k, v)
    db.commit()
    db.refresh(obj)
    return obj

def create_expense(db: Session, *, exp_date: date, category: str, amount: float, channel: str | None, comment: str | None) -> Expense:
    obj = Expense(exp_date=exp_date, category=category, amount=amount, channel=channel, comment=comment)
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj

def list_expenses(db: Session, *, date_from: date | None = None, date_to: date | None = None):
    q = select(Expense).order_by(Expense.exp_date.desc(), Expense.id.desc())
    if date_from:
        q = q.where(Expense.exp_date >= date_from)
    if date_to:
        q = q.where(Expense.exp_date <= date_to)
    return db.execute(q).scalars().all()

def get_control(db: Session) -> dict:
    draft_purchases = db.execute(select(func.count()).select_from(PurchaseDoc).where(PurchaseDoc.status == "DRAFT")).scalar_one()
    open_orders = db.execute(select(func.count()).select_from(BizOrder).where(BizOrder.status == "OPEN")).scalar_one()
    # low stock: materials with prop min_stock set and remaining < min
    low = 0
    mats = db.execute(select(Material)).scalars().all()
    for m in mats:
        props = _material_props(db, m.id)
        ms = props.get("min_stock_base")
        if not ms:
            continue
        try:
            min_stock = float(ms)
        except Exception:
            continue
        # remaining in base uom across lots
        rem = db.execute(select(func.coalesce(func.sum(Lot.qty_in - Lot.qty_out), 0)).where(Lot.material_id == m.id)).scalar_one()
        if float(rem) < min_stock:
            low += 1
    return {"draft_purchases": int(draft_purchases or 0), "open_orders": int(open_orders or 0), "low_stock": int(low)}
