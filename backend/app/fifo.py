from sqlalchemy.orm import Session
from sqlalchemy import select
from .models import Lot

def fifo_allocate(db: Session, material_id: int, need_qty: float):
    lots = db.execute(
        select(Lot).where(Lot.material_id == material_id).order_by(Lot.created_at.asc())
    ).scalars().all()

    allocations = []
    remaining = float(need_qty)

    for lot in lots:
        available = float(lot.qty_in) - float(lot.qty_out)
        if available <= 0:
            continue
        take = min(available, remaining)
        if take > 0:
            allocations.append((lot, take))
            remaining -= take
        if remaining <= 1e-9:
            break

    if remaining > 1e-9:
        raise ValueError(f"Недостаточно остатка по материалу {material_id}. Не хватает: {remaining:.4f}")

    return allocations
