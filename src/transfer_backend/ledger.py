"""台账：双式分录过账与守恒校验。

每条跨桶移动在同一 txn_group 下写两条分录（来源桶负、目标桶正），
因此任意 txn_group 的 qty_delta 合计恒为 0 —— 这是「跨仓批次守恒」
的可验证形式。补偿分录（拒收/退回/释放）通过 compensates_txn 指向原事务组。
"""
from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .enums import Bucket, EntryType
from .models import InventoryBatch, LedgerEntry, ShipmentLine


def new_txn_group() -> str:
    return uuid.uuid4().hex


def post_movement(
    session: Session,
    *,
    txn_group: str,
    entry_type: EntryType,
    part_id: int,
    qty: int,
    from_bucket: Bucket,
    to_bucket: Bucket,
    from_warehouse_id: int | None = None,
    to_warehouse_id: int | None = None,
    from_batch_id: int | None = None,
    to_batch_id: int | None = None,
    shipment_id: int | None = None,
    ref_type: str = "",
    ref_id: int | None = None,
    compensates_txn: str | None = None,
    remark: str = "",
    now: datetime | None = None,
) -> None:
    """写一对平衡分录：from_bucket -qty，to_bucket +qty。"""
    if qty <= 0:
        raise ValueError("过账数量必须为正")
    now = now or datetime.utcnow()
    base = dict(
        txn_group=txn_group,
        entry_type=entry_type.value,
        part_id=part_id,
        shipment_id=shipment_id,
        ref_type=ref_type,
        ref_id=ref_id,
        compensates_txn=compensates_txn,
        remark=remark,
        created_at=now,
    )
    session.add(
        LedgerEntry(
            **base,
            bucket=from_bucket.value,
            qty_delta=-qty,
            warehouse_id=from_warehouse_id,
            batch_id=from_batch_id,
        )
    )
    session.add(
        LedgerEntry(
            **base,
            bucket=to_bucket.value,
            qty_delta=qty,
            warehouse_id=to_warehouse_id,
            batch_id=to_batch_id,
        )
    )


def verify_txn_groups(session: Session) -> list[str]:
    """返回借贷不平衡的事务组列表（应为空）。"""
    rows = session.execute(
        select(LedgerEntry.txn_group, func.sum(LedgerEntry.qty_delta)).group_by(LedgerEntry.txn_group)
    ).all()
    return [txn for txn, total in rows if total != 0]


def ledger_positions(session: Session, part_id: int) -> dict[tuple, int]:
    """从台账推导当前位置：(bucket, warehouse_id, batch_id, shipment_id) -> qty。

    位置标识约定：shipment_id 只在 IN_TRANSIT 桶中参与标识；
    其他桶的分录即使携带 shipment_id（作为引用信息）也归一到 None。
    """
    rows = session.execute(
        select(
            LedgerEntry.bucket,
            LedgerEntry.warehouse_id,
            LedgerEntry.batch_id,
            LedgerEntry.shipment_id,
            func.sum(LedgerEntry.qty_delta),
        )
        .where(LedgerEntry.part_id == part_id)
        .group_by(
            LedgerEntry.bucket,
            LedgerEntry.warehouse_id,
            LedgerEntry.batch_id,
            LedgerEntry.shipment_id,
        )
    ).all()
    positions: dict[tuple, int] = defaultdict(int)
    for bucket, wh, batch, shipment, total in rows:
        if bucket != Bucket.IN_TRANSIT.value:
            shipment = None
        if total:
            positions[(bucket, wh, batch, shipment)] += total
    return dict(positions)


def stored_positions(session: Session, part_id: int) -> dict[tuple, int]:
    """从计数器表推导当前位置（批次表 + 在途明细），用于与台账对账。"""
    positions: dict[tuple, int] = defaultdict(int)
    batches = session.execute(
        select(InventoryBatch).where(InventoryBatch.part_id == part_id)
    ).scalars()
    for b in batches:
        if b.qty_available:
            positions[(Bucket.AVAILABLE.value, b.warehouse_id, b.id, None)] += b.qty_available
        if b.qty_reserved:
            positions[(Bucket.RESERVED.value, b.warehouse_id, b.id, None)] += b.qty_reserved
    lines = session.execute(
        select(ShipmentLine)
        .join_from(ShipmentLine, InventoryBatch, ShipmentLine.batch_id == InventoryBatch.id)
        .where(ShipmentLine.part_id == part_id)
    ).scalars()
    for ln in lines:
        if ln.qty_in_transit:
            positions[(Bucket.IN_TRANSIT.value, None, ln.batch_id, ln.shipment_id)] += ln.qty_in_transit
    return dict(positions)


def reconcile_part(session: Session, part_id: int) -> dict:
    """守恒校验：台账推导位置必须等于计数器位置，且所有事务组平衡。"""
    derived = ledger_positions(session, part_id)
    stored = stored_positions(session, part_id)
    # EXTERNAL 桶是系统边界（期初入库的来源），不参与物理位置对账
    derived_physical = {k: v for k, v in derived.items() if k[0] != Bucket.EXTERNAL.value}
    discrepancies = []
    for key in sorted(set(derived_physical) | set(stored), key=str):
        d = derived_physical.get(key, 0)
        s = stored.get(key, 0)
        if d != s:
            discrepancies.append({"position": key, "ledger": d, "stored": s})
    unbalanced = verify_txn_groups(session)
    return {
        "part_id": part_id,
        "balanced": not discrepancies and not unbalanced,
        "discrepancies": discrepancies,
        "unbalanced_txn_groups": unbalanced,
        "positions": [
            {"bucket": k[0], "warehouse_id": k[1], "batch_id": k[2], "shipment_id": k[3], "qty": v}
            for k, v in sorted(stored.items(), key=str)
        ],
    }
