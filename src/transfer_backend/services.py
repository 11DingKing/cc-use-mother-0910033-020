"""业务服务层。

并发与原子性设计（本模块的核心约定）：

1. 每个命令先对聚合根行做「守卫更新」：
   `UPDATE ... SET ... WHERE id = :id AND status IN (...)`。
   影响行数为 0 说明状态已被并发改变 —— 重新读取后报 INVALID_STATE。
   该更新同时拿走行锁，此后的读写在本次事务内无并发干扰。
2. 所有库存/计数器修改使用「条件更新 + 影响行数校验」的原子 SQL
   （compare-and-swap），不会超卖、不会丢失更新；
   并统一 `synchronize_session=False`，避免 ORM 用过期内存值回写。
3. 「装运确认」把扣减预留/在库、核销预留明细、写在途记录、写台账
   放在同一个数据库事务：任何一步失败整体回滚 —— 不存在
   「先扣出库再补在途」的中间态，重试不会丢库存或重复发运。
4. 拒收、超时退回、取消释放都写补偿分录（compensates_txn 指向原事务组）。
5. FAULT_HOOKS 是测试注入故障的钩子（验证崩溃原子性），生产保持为空。
"""
from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from datetime import datetime

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from . import ledger
from .enums import (
    REQUEST_COMMAND_SOURCES,
    SHIPMENT_COMMAND_SOURCES,
    Bucket,
    EntryType,
    RequestStatus,
    ShipmentStatus,
)
from .errors import DomainError, insufficient_stock, invalid_state, not_found
from .models import (
    Approval,
    EventLog,
    InventoryBatch,
    Part,
    Receipt,
    ReceiptLine,
    ReservationLine,
    Shipment,
    ShipmentLine,
    TransferRequest,
    Warehouse,
)
from .schemas import (
    ReceiptOut,
    ShipmentLineOut,
    ShipmentOut,
    TracePosition,
    TransferRequestOut,
)

# 测试故障注入钩子：在事务关键路径上抛出异常以模拟崩溃。生产环境保持为空列表。
FAULT_HOOKS: list[Callable[[str], None]] = []

# 批量更新一律关闭 ORM 会话同步：内存中的对象可能是过期值，绝不能用其回写
_SYNC_OFF = {"synchronize_session": False}


def _fault(point: str) -> None:
    for hook in FAULT_HOOKS:
        hook(point)


def _now(now: datetime | None) -> datetime:
    return now or datetime.utcnow()


def _no(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12].upper()}"


# ---------------------------------------------------------------- 基础查询


def _get(session: Session, model, ident: int, label: str):
    obj = session.get(model, ident)
    if obj is None:
        raise not_found(label, ident)
    return obj


def get_request(session: Session, request_id: int) -> TransferRequest:
    return _get(session, TransferRequest, request_id, "调拨申请")


def get_shipment(session: Session, shipment_id: int) -> Shipment:
    return _get(session, Shipment, shipment_id, "装运单")


def _guard(
    session: Session,
    model,
    ident: int,
    label: str,
    command: str,
    allowed: tuple,
    **values,
):
    """守卫更新：校验来源状态并拿走行锁。返回更新后的当前行。

    并发下只有一个事务能通过守卫；其余事务 rowcount=0，重新读取实际状态后报错。
    """
    result = session.execute(
        update(model)
        .where(model.id == ident, model.status.in_([s.value for s in allowed]))
        .values(version=model.__table__.c.version + 1, **values)
        .execution_options(**_SYNC_OFF)
    )
    if result.rowcount != 1:
        current = session.get(model, ident)
        if current is None:
            raise not_found(label, ident)
        session.refresh(current)  # 读取最新状态，给出准确错误
        raise invalid_state(label, ident, current.status, command)
    obj = session.get(model, ident)
    session.refresh(obj)
    return obj


def log_event(
    session: Session,
    entity_type: str,
    entity_id: int,
    event_type: str,
    from_status: str | None,
    to_status: str | None,
    actor: str = "",
    detail: dict | None = None,
    now: datetime | None = None,
) -> None:
    session.add(
        EventLog(
            entity_type=entity_type,
            entity_id=entity_id,
            event_type=event_type,
            from_status=from_status,
            to_status=to_status,
            actor=actor,
            detail=json.dumps(detail or {}, ensure_ascii=False),
            created_at=_now(now),
        )
    )


# ---------------------------------------------------------------- DTO 构建


def request_out(session: Session, req: TransferRequest) -> TransferRequestOut:
    reserved_remaining = session.execute(
        select(
            func.coalesce(
                func.sum(ReservationLine.qty_reserved - ReservationLine.qty_shipped - ReservationLine.qty_released),
                0,
            )
        ).where(ReservationLine.request_id == req.id)
    ).scalar_one()
    in_transit = req.qty_shipped - req.qty_received - req.qty_rejected - req.qty_returned
    return TransferRequestOut(
        id=req.id,
        request_no=req.request_no,
        status=req.status,
        source_warehouse_id=req.source_warehouse_id,
        dest_warehouse_id=req.dest_warehouse_id,
        part_id=req.part_id,
        qty_requested=req.qty_requested,
        qty_approved=req.qty_approved,
        qty_reserved=req.qty_reserved,
        qty_reserved_remaining=int(reserved_remaining),
        qty_shipped=req.qty_shipped,
        qty_in_transit=in_transit,
        qty_received=req.qty_received,
        qty_rejected=req.qty_rejected,
        qty_returned=req.qty_returned,
        qty_cancelled=req.qty_cancelled,
        reason=req.reason,
        created_by=req.created_by,
        version=req.version,
        created_at=req.created_at,
    )


def shipment_out(sh: Shipment) -> ShipmentOut:
    return ShipmentOut(
        id=sh.id,
        shipment_no=sh.shipment_no,
        request_id=sh.request_id,
        source_warehouse_id=sh.source_warehouse_id,
        dest_warehouse_id=sh.dest_warehouse_id,
        status=sh.status,
        carrier=sh.carrier,
        tracking_no=sh.tracking_no,
        eta=sh.eta,
        shipped_at=sh.shipped_at,
        version=sh.version,
        lines=[
            ShipmentLineOut(
                id=ln.id,
                batch_id=ln.batch_id,
                part_id=ln.part_id,
                qty_shipped=ln.qty_shipped,
                qty_received=ln.qty_received,
                qty_rejected=ln.qty_rejected,
                qty_returned=ln.qty_returned,
                qty_in_transit=ln.qty_in_transit,
            )
            for ln in sorted(sh.lines, key=lambda x: x.id)
        ],
    )


def receipt_out(rc: Receipt) -> ReceiptOut:
    return ReceiptOut.model_validate(rc, from_attributes=True)


# ---------------------------------------------------------------- 状态机 refresh


def _set_status(session: Session, entity, entity_type: str, new_status: str, event_type: str, actor: str, now: datetime | None) -> None:
    old = entity.status
    if old == new_status:
        return
    entity.status = new_status
    entity.version += 1
    log_event(session, entity_type, entity.id, event_type, old, new_status, actor, now=now)


def refresh_request_status(session: Session, req: TransferRequest, actor: str = "", now: datetime | None = None) -> None:
    """履约状态由计数器推导；草拟/待审批/已拒绝等命令态不受影响。

    调用前必须已持有申请行锁（守卫更新或计数器原子更新），
    且 req 已刷新到最新计数器。
    """
    if req.status in (
        RequestStatus.DRAFT.value,
        RequestStatus.SUBMITTED.value,
        RequestStatus.REJECTED.value,
        RequestStatus.CANCELLED.value,
    ):
        return
    in_transit = req.qty_shipped - req.qty_received - req.qty_rejected - req.qty_returned
    unshipped = req.qty_reserved - req.qty_shipped - req.qty_cancelled
    resolved = req.qty_received + req.qty_rejected + req.qty_returned
    if in_transit == 0 and unshipped == 0:
        if req.qty_shipped == 0:
            new = RequestStatus.CANCELLED
        elif req.qty_received == req.qty_reserved:
            new = RequestStatus.RECEIVED
        else:
            new = RequestStatus.CLOSED
    elif resolved > 0:
        new = RequestStatus.PARTIALLY_RECEIVED
    elif req.qty_shipped > 0:
        to_ship = req.qty_reserved - req.qty_cancelled
        new = RequestStatus.SHIPPED if req.qty_shipped >= to_ship else RequestStatus.PARTIALLY_SHIPPED
    else:
        new = RequestStatus.APPROVED
    _set_status(session, req, "request", new.value, "STATUS_CHANGE", actor, now)


def refresh_shipment_status(session: Session, sh: Shipment, actor: str = "", now: datetime | None = None) -> None:
    shipped = sum(ln.qty_shipped for ln in sh.lines)
    received = sum(ln.qty_received for ln in sh.lines)
    rejected = sum(ln.qty_rejected for ln in sh.lines)
    returned = sum(ln.qty_returned for ln in sh.lines)
    resolved = received + rejected + returned
    if resolved == shipped:
        if received == shipped:
            new = ShipmentStatus.RECEIVED
        elif returned > 0 and received == 0 and rejected == 0:
            new = ShipmentStatus.RETURNED
        elif rejected > 0 and received == 0 and returned == 0:
            new = ShipmentStatus.REJECTED
        else:
            new = ShipmentStatus.RESOLVED_WITH_REJECTION
    elif sh.status == ShipmentStatus.TIMED_OUT.value:
        return  # 未了结前保持超时态，由 extend/receive/return 命令解除
    elif resolved > 0:
        new = ShipmentStatus.PARTIALLY_RECEIVED
    else:
        new = ShipmentStatus.IN_TRANSIT
    _set_status(session, sh, "shipment", new.value, "STATUS_CHANGE", actor, now)


def _finish(session: Session) -> None:
    """命令收尾：flush 后过期全部缓存对象，保证后续 DTO/refresh 读到最新值。"""
    session.flush()
    session.expire_all()


# ---------------------------------------------------------------- 基础档案与入库


def create_warehouse(session: Session, code: str, name: str) -> Warehouse:
    wh = Warehouse(code=code, name=name)
    session.add(wh)
    session.flush()
    return wh


def create_part(session: Session, sku: str, name: str) -> Part:
    part = Part(sku=sku, name=name)
    session.add(part)
    session.flush()
    return part


def inbound_batch(session: Session, warehouse_id: int, part_id: int, batch_no: str, qty: int, now: datetime | None = None) -> InventoryBatch:
    """期初/采购入库：不存在则建档，已存在则原子累加（台账记 INBOUND）。"""
    _get(session, Warehouse, warehouse_id, "仓库")
    _get(session, Part, part_id, "零件")
    batch = session.execute(
        select(InventoryBatch).where(
            InventoryBatch.warehouse_id == warehouse_id,
            InventoryBatch.part_id == part_id,
            InventoryBatch.batch_no == batch_no,
        )
    ).scalar_one_or_none()
    if batch is None:
        batch = InventoryBatch(
            warehouse_id=warehouse_id,
            part_id=part_id,
            batch_no=batch_no,
            qty_on_hand=qty,
            qty_available=qty,
            qty_reserved=0,
        )
        session.add(batch)
        session.flush()
    else:
        session.execute(
            update(InventoryBatch)
            .where(InventoryBatch.id == batch.id)
            .values(
                qty_on_hand=InventoryBatch.qty_on_hand + qty,
                qty_available=InventoryBatch.qty_available + qty,
                version=InventoryBatch.version + 1,
            )
            .execution_options(**_SYNC_OFF)
        )
        _finish(session)
        batch = session.get(InventoryBatch, batch.id)
    ledger.post_movement(
        session,
        txn_group=ledger.new_txn_group(),
        entry_type=EntryType.INBOUND,
        part_id=part_id,
        qty=qty,
        from_bucket=Bucket.EXTERNAL,
        to_bucket=Bucket.AVAILABLE,
        to_warehouse_id=warehouse_id,
        to_batch_id=batch.id,
        ref_type="batch",
        ref_id=batch.id,
        remark=f"入库 {batch_no}",
        now=now,
    )
    session.flush()
    return batch


# ---------------------------------------------------------------- 调拨申请


def create_transfer_request(
    session: Session,
    *,
    source_warehouse_id: int,
    dest_warehouse_id: int,
    part_id: int,
    qty: int,
    reason: str = "",
    created_by: str = "",
    idem_key: str | None = None,
    now: datetime | None = None,
) -> TransferRequest:
    if source_warehouse_id == dest_warehouse_id:
        raise DomainError("VALIDATION", "来源仓与目的仓不能相同", http_status=422)
    _get(session, Warehouse, source_warehouse_id, "仓库")
    _get(session, Warehouse, dest_warehouse_id, "仓库")
    _get(session, Part, part_id, "零件")
    req = TransferRequest(
        request_no=_no("TR"),
        source_warehouse_id=source_warehouse_id,
        dest_warehouse_id=dest_warehouse_id,
        part_id=part_id,
        qty_requested=qty,
        status=RequestStatus.DRAFT.value,
        reason=reason,
        created_by=created_by,
        idempotency_key=idem_key,
    )
    session.add(req)
    session.flush()
    log_event(session, "request", req.id, "CREATED", None, req.status, created_by, {"qty": qty}, now)
    return req


def submit_request(session: Session, request_id: int, actor: str = "", now: datetime | None = None) -> TransferRequest:
    req = _guard(session, TransferRequest, request_id, "调拨申请", "submit", REQUEST_COMMAND_SOURCES["submit"])
    _set_status(session, req, "request", RequestStatus.SUBMITTED.value, "SUBMITTED", actor, now)
    session.flush()
    return req


def approve_request(
    session: Session,
    request_id: int,
    *,
    decision: str,
    qty: int | None = None,
    approver: str = "",
    comment: str = "",
    now: datetime | None = None,
) -> Approval:
    """审批。APPROVE 时按批次 FIFO 原子预留；数量不足则整体回滚，
    不产生半个预留（调用方可改用更小数量重试）。"""
    if decision == "REJECT":
        req = _guard(session, TransferRequest, request_id, "调拨申请", "reject", REQUEST_COMMAND_SOURCES["reject"])
        approval = Approval(request_id=req.id, decision="REJECT", qty_approved=0, approver=approver, comment=comment)
        session.add(approval)
        _set_status(session, req, "request", RequestStatus.REJECTED.value, "REJECTED", approver, now)
        session.flush()
        return approval

    req = _guard(session, TransferRequest, request_id, "调拨申请", "approve", REQUEST_COMMAND_SOURCES["approve"])
    qty = qty if qty is not None else req.qty_requested
    if qty <= 0 or qty > req.qty_requested:
        raise DomainError("VALIDATION", f"批准数量必须在 1..{req.qty_requested} 之间", http_status=422)

    txn = ledger.new_txn_group()
    remaining = qty
    batches = session.execute(
        select(InventoryBatch)
        .where(
            InventoryBatch.warehouse_id == req.source_warehouse_id,
            InventoryBatch.part_id == req.part_id,
            InventoryBatch.qty_available > 0,
        )
        .order_by(InventoryBatch.id)  # FIFO：先入库的批次先预留
        .with_for_update()
    ).scalars()
    for batch in batches:
        available_now = batch.qty_available  # 本地快照，仅用于规划；绝不回写 ORM 对象
        for _attempt in range(3):
            if remaining == 0 or available_now <= 0:
                break
            take = min(available_now, remaining)
            # 条件更新：并发下若该批次可用量已被抢走则影响行数为 0，刷新后重试
            result = session.execute(
                update(InventoryBatch)
                .where(InventoryBatch.id == batch.id, InventoryBatch.qty_available >= take)
                .values(
                    qty_available=InventoryBatch.qty_available - take,
                    qty_reserved=InventoryBatch.qty_reserved + take,
                    version=InventoryBatch.version + 1,
                )
                .execution_options(**_SYNC_OFF)
            )
            if result.rowcount != 1:
                session.refresh(batch)
                available_now = batch.qty_available
                continue
            session.add(
                ReservationLine(
                    request_id=req.id,
                    batch_id=batch.id,
                    part_id=req.part_id,
                    qty_reserved=take,
                )
            )
            ledger.post_movement(
                session,
                txn_group=txn,
                entry_type=EntryType.RESERVE,
                part_id=req.part_id,
                qty=take,
                from_bucket=Bucket.AVAILABLE,
                to_bucket=Bucket.RESERVED,
                from_warehouse_id=req.source_warehouse_id,
                to_warehouse_id=req.source_warehouse_id,
                from_batch_id=batch.id,
                to_batch_id=batch.id,
                ref_type="request",
                ref_id=req.id,
                remark=f"审批预留 {req.request_no}",
                now=now,
            )
            remaining -= take
            available_now -= take
            break
        if remaining == 0:
            break
    if remaining > 0:
        # 抛错后整个事务回滚：已写的预留分录与条件更新一并撤销。
        # 报给调用方的可用量 = 本次事务已预留量（将回滚）+ 当前剩余可用量。
        left = session.execute(
            select(func.coalesce(func.sum(InventoryBatch.qty_available), 0)).where(
                InventoryBatch.warehouse_id == req.source_warehouse_id,
                InventoryBatch.part_id == req.part_id,
            )
        ).scalar_one()
        raise insufficient_stock(req.part_id, req.source_warehouse_id, qty, (qty - remaining) + int(left))

    req.qty_approved = qty
    req.qty_reserved = qty
    approval = Approval(request_id=req.id, decision="APPROVE", qty_approved=qty, approver=approver, comment=comment)
    session.add(approval)
    _set_status(session, req, "request", RequestStatus.APPROVED.value, "APPROVED", approver, now)
    session.flush()
    _fault("approve.after_reserve")
    return approval


def cancel_request(
    session: Session,
    request_id: int,
    *,
    qty: int | None = None,
    actor: str = "",
    reason: str = "",
    now: datetime | None = None,
) -> TransferRequest:
    """取消。未审批的直接终止；已预留的释放剩余预留（补偿分录 RELEASE）。

    在途部分不受取消影响，继续走签收/退回流程，全部了结后申请自动关闭。
    """
    req = _guard(session, TransferRequest, request_id, "调拨申请", "cancel", REQUEST_COMMAND_SOURCES["cancel"])

    if req.status in (RequestStatus.DRAFT.value, RequestStatus.SUBMITTED.value):
        _set_status(session, req, "request", RequestStatus.CANCELLED.value, "CANCELLED", actor, now)
        session.flush()
        return req

    releasable = req.qty_reserved - req.qty_shipped - req.qty_cancelled
    to_release = releasable if qty is None else qty
    if to_release <= 0 or to_release > releasable:
        raise DomainError(
            "VALIDATION",
            f"可取消数量范围为 1..{releasable}",
            http_status=422,
            details={"releasable": releasable},
        )
    txn = ledger.new_txn_group()
    remaining = to_release
    lines = session.execute(
        select(ReservationLine)
        .where(ReservationLine.request_id == req.id)
        .order_by(ReservationLine.id)
        .with_for_update()
    ).scalars()
    for rl in lines:
        if remaining == 0:
            break
        give = min(rl.remaining, remaining)
        if give <= 0:
            continue
        result = session.execute(
            update(InventoryBatch)
            .where(InventoryBatch.id == rl.batch_id, InventoryBatch.qty_reserved >= give)
            .values(
                qty_reserved=InventoryBatch.qty_reserved - give,
                qty_available=InventoryBatch.qty_available + give,
                version=InventoryBatch.version + 1,
            )
            .execution_options(**_SYNC_OFF)
        )
        if result.rowcount != 1:
            raise DomainError("STOCK_CONFLICT", "释放预留时库存状态冲突，请重试", http_status=409)
        result = session.execute(
            update(ReservationLine)
            .where(
                ReservationLine.id == rl.id,
                ReservationLine.qty_reserved - ReservationLine.qty_shipped - ReservationLine.qty_released >= give,
            )
            .values(qty_released=ReservationLine.qty_released + give)
            .execution_options(**_SYNC_OFF)
        )
        if result.rowcount != 1:
            raise DomainError("STOCK_CONFLICT", "释放预留时预留明细冲突，请重试", http_status=409)
        ledger.post_movement(
            session,
            txn_group=txn,
            entry_type=EntryType.RELEASE,
            part_id=req.part_id,
            qty=give,
            from_bucket=Bucket.RESERVED,
            to_bucket=Bucket.AVAILABLE,
            from_warehouse_id=req.source_warehouse_id,
            to_warehouse_id=req.source_warehouse_id,
            from_batch_id=rl.batch_id,
            to_batch_id=rl.batch_id,
            ref_type="request",
            ref_id=req.id,
            remark=f"取消释放 {req.request_no}: {reason}",
            now=now,
        )
        remaining -= give
    session.execute(
        update(TransferRequest)
        .where(TransferRequest.id == req.id)
        .values(qty_cancelled=TransferRequest.qty_cancelled + to_release, version=TransferRequest.version + 1)
        .execution_options(**_SYNC_OFF)
    )
    _finish(session)
    req = get_request(session, request_id)
    refresh_request_status(session, req, actor, now)
    log_event(
        session,
        "request",
        req.id,
        "CANCELLED" if req.status == RequestStatus.CANCELLED.value else "CANCELLED_PARTIAL",
        None,
        req.status,
        actor,
        {"released": to_release, "reason": reason},
        now,
    )
    session.flush()
    return req


# ---------------------------------------------------------------- 装运


def create_shipment(
    session: Session,
    request_id: int,
    *,
    qty: int | None = None,
    lines: list[dict] | None = None,
    carrier: str = "",
    tracking_no: str = "",
    eta: datetime | None = None,
    idem_key: str | None = None,
    now: datetime | None = None,
) -> Shipment:
    """建装运单（PENDING）。此时不扣库存，确认时才原子扣减。

    建单前先对申请行做守卫更新拿行锁：同一申请的装运计划串行，
    不会超分预留；确认时仍会以条件更新复核。
    """
    req = _guard(session, TransferRequest, request_id, "调拨申请", "create_shipment", REQUEST_COMMAND_SOURCES["create_shipment"])

    reservations = {
        rl.batch_id: rl
        for rl in session.execute(
            select(ReservationLine).where(ReservationLine.request_id == req.id).order_by(ReservationLine.id)
        ).scalars()
        if rl.remaining > 0
    }
    sh = Shipment(
        shipment_no=_no("SHP"),
        request_id=req.id,
        source_warehouse_id=req.source_warehouse_id,
        dest_warehouse_id=req.dest_warehouse_id,
        status=ShipmentStatus.PENDING.value,
        carrier=carrier,
        tracking_no=tracking_no,
        eta=eta,
        idempotency_key=idem_key,
    )
    session.add(sh)
    session.flush()

    plan: list[tuple[ReservationLine, int]] = []
    if lines:
        for item in lines:
            rl = reservations.get(item["batch_id"])
            if rl is None:
                raise DomainError(
                    "VALIDATION",
                    f"批次 {item['batch_id']} 在该申请下没有可用预留",
                    http_status=422,
                )
            if item["qty"] > rl.remaining:
                raise DomainError(
                    "QTY_EXCEEDS_REMAINING",
                    f"批次 {item['batch_id']} 预留剩余 {rl.remaining}，小于装运 {item['qty']}",
                    http_status=422,
                )
            plan.append((rl, item["qty"]))
    else:
        total = qty if qty is not None else sum(rl.remaining for rl in reservations.values())
        if total <= 0:
            raise DomainError("VALIDATION", "没有可装运的预留数量", http_status=422)
        remaining = total
        for rl in reservations.values():  # FIFO
            if remaining == 0:
                break
            take = min(rl.remaining, remaining)
            plan.append((rl, take))
            remaining -= take
        if remaining > 0:
            raise DomainError(
                "QTY_EXCEEDS_REMAINING",
                f"申请装运 {total}，预留剩余仅 {total - remaining}",
                http_status=422,
            )

    for rl, take in plan:
        session.add(
            ShipmentLine(
                shipment_id=sh.id,
                reservation_line_id=rl.id,
                batch_id=rl.batch_id,
                part_id=rl.part_id,
                qty_shipped=take,
            )
        )
    session.flush()
    log_event(
        session,
        "shipment",
        sh.id,
        "CREATED",
        None,
        sh.status,
        detail={"request": req.request_no, "lines": [(rl.batch_id, t) for rl, t in plan]},
        now=now,
    )
    return sh


def confirm_shipment(session: Session, shipment_id: int, actor: str = "", now: datetime | None = None) -> Shipment:
    """装运确认 —— 本系统最关键的原子操作。

    单个数据库事务内完成：
      0. 守卫更新把装运单 PENDING -> IN_TRANSIT（并发/重试只放行一次）；
      1. 条件更新扣减来源批次的预留量与在库量；
      2. 核销对应预留明细；
      3. 写在途记录（ShipmentLine.ship_txn_group）与 SHIP 台账分录；
      4. 推进申请/装运状态机。

    任何一步失败整体回滚；重复确认直接返回当前状态（天然幂等），
    配合接口层 Idempotency-Key，失败重试不会重复扣减或重复发运。
    """
    sh = get_shipment(session, shipment_id)
    if sh.status == ShipmentStatus.IN_TRANSIT.value:
        return sh  # 天然幂等：重复确认不产生二次扣减
    now = _now(now)
    sh = _guard(
        session,
        Shipment,
        shipment_id,
        "装运单",
        "confirm",
        SHIPMENT_COMMAND_SOURCES["confirm"],
        status=ShipmentStatus.IN_TRANSIT.value,
        shipped_at=now,
    )
    req = get_request(session, sh.request_id)
    txn = ledger.new_txn_group()
    total = 0
    for line in sh.lines:
        q = line.qty_shipped
        # 1) 原子扣减来源批次：预留 -> 在途（在库随之减少）
        result = session.execute(
            update(InventoryBatch)
            .where(
                InventoryBatch.id == line.batch_id,
                InventoryBatch.qty_reserved >= q,
                InventoryBatch.qty_on_hand >= q,
            )
            .values(
                qty_reserved=InventoryBatch.qty_reserved - q,
                qty_on_hand=InventoryBatch.qty_on_hand - q,
                version=InventoryBatch.version + 1,
            )
            .execution_options(**_SYNC_OFF)
        )
        if result.rowcount != 1:
            raise DomainError(
                "STOCK_CONFLICT",
                f"批次 {line.batch_id} 预留/在库不足，确认失败（可能已被并发操作占用）",
                http_status=409,
                details={"batch_id": line.batch_id, "qty": q},
            )
        # 2) 核销预留明细（同样带条件，防止超核销）
        result = session.execute(
            update(ReservationLine)
            .where(
                ReservationLine.id == line.reservation_line_id,
                ReservationLine.qty_reserved - ReservationLine.qty_shipped - ReservationLine.qty_released >= q,
            )
            .values(qty_shipped=ReservationLine.qty_shipped + q)
            .execution_options(**_SYNC_OFF)
        )
        if result.rowcount != 1:
            raise DomainError("STOCK_CONFLICT", "预留明细核销冲突，请重试", http_status=409)
        # 3) 在途台账：RESERVED@来源仓 -> IN_TRANSIT@装运单，记录来源批次
        line.ship_txn_group = txn
        ledger.post_movement(
            session,
            txn_group=txn,
            entry_type=EntryType.SHIP,
            part_id=line.part_id,
            qty=q,
            from_bucket=Bucket.RESERVED,
            to_bucket=Bucket.IN_TRANSIT,
            from_warehouse_id=sh.source_warehouse_id,
            from_batch_id=line.batch_id,
            to_batch_id=line.batch_id,
            shipment_id=sh.id,
            ref_type="shipment",
            ref_id=sh.id,
            remark=f"装运确认 {sh.shipment_no}",
            now=now,
        )
        total += q
    # 4) 申请计数器原子累加并推进状态机
    session.execute(
        update(TransferRequest)
        .where(TransferRequest.id == req.id)
        .values(qty_shipped=TransferRequest.qty_shipped + total, version=TransferRequest.version + 1)
        .execution_options(**_SYNC_OFF)
    )
    log_event(session, "shipment", sh.id, "CONFIRMED", ShipmentStatus.PENDING.value, ShipmentStatus.IN_TRANSIT.value, actor, {"qty": total}, now)
    _finish(session)
    req = get_request(session, sh.request_id)
    refresh_request_status(session, req, actor, now)
    session.flush()
    _fault("confirm_shipment.after_updates")  # 测试钩子：模拟提交前崩溃
    sh = get_shipment(session, shipment_id)
    return sh


def cancel_shipment(session: Session, shipment_id: int, actor: str = "", now: datetime | None = None) -> Shipment:
    """取消未确认的装运单。库存尚未扣减，无需补偿。"""
    sh = _guard(session, Shipment, shipment_id, "装运单", "cancel", SHIPMENT_COMMAND_SOURCES["cancel"])
    _set_status(session, sh, "shipment", ShipmentStatus.CANCELLED.value, "CANCELLED", actor, now)
    session.flush()
    return sh


# ---------------------------------------------------------------- 签收 / 拒收


def _dest_batch_for_receipt(session: Session, sh: Shipment, src_batch_id: int, part_id: int) -> InventoryBatch:
    """签收落批：目的仓中找同批次号的批次，没有则以来源批次为谱系新建。"""
    src = _get(session, InventoryBatch, src_batch_id, "批次")
    dest = session.execute(
        select(InventoryBatch).where(
            InventoryBatch.warehouse_id == sh.dest_warehouse_id,
            InventoryBatch.part_id == part_id,
            InventoryBatch.batch_no == src.batch_no,
        )
    ).scalar_one_or_none()
    if dest is None:
        dest = InventoryBatch(
            warehouse_id=sh.dest_warehouse_id,
            part_id=part_id,
            batch_no=src.batch_no,
            origin_batch_id=src.id,
            qty_on_hand=0,
            qty_available=0,
            qty_reserved=0,
        )
        session.add(dest)
        session.flush()
    return dest


def receive_shipment(
    session: Session,
    shipment_id: int,
    *,
    lines: list[dict],
    receiver: str = "",
    note: str = "",
    idem_key: str | None = None,
    now: datetime | None = None,
) -> Receipt:
    """签收（支持部分签收与逐行拒收）。

    - 接受数量：在途 -> 目的仓可用（落目的批次，记录批次谱系）；
    - 拒收数量：在途 -> 来源仓可用，写 REJECT_RETURN 补偿分录，
      compensates_txn 指向原 SHIP 事务组。
    """
    sh = _guard(session, Shipment, shipment_id, "装运单", "receive", SHIPMENT_COMMAND_SOURCES["receive"])
    req = get_request(session, sh.request_id)
    now = _now(now)

    line_map = {ln.id: ln for ln in sh.lines}
    receipt = Receipt(shipment_id=sh.id, receiver=receiver, note=note, idempotency_key=idem_key)
    session.add(receipt)
    session.flush()

    accepted_total = 0
    rejected_total = 0
    for item in lines:
        line = line_map.get(item["shipment_line_id"])
        if line is None:
            raise DomainError("VALIDATION", f"装运明细 {item['shipment_line_id']} 不属于装运单 {shipment_id}", http_status=422)
        accepted = int(item.get("qty_accepted", 0))
        rejected = int(item.get("qty_rejected", 0))
        if accepted < 0 or rejected < 0 or accepted + rejected == 0:
            raise DomainError("VALIDATION", "签收/拒收数量必须为非负且合计大于 0", http_status=422)
        # 条件更新核销在途：并发签收不会超收
        result = session.execute(
            update(ShipmentLine)
            .where(
                ShipmentLine.id == line.id,
                ShipmentLine.qty_shipped - ShipmentLine.qty_received - ShipmentLine.qty_rejected - ShipmentLine.qty_returned
                >= accepted + rejected,
            )
            .values(
                qty_received=ShipmentLine.qty_received + accepted,
                qty_rejected=ShipmentLine.qty_rejected + rejected,
            )
            .execution_options(**_SYNC_OFF)
        )
        if result.rowcount != 1:
            raise DomainError(
                "QTY_EXCEEDS_REMAINING",
                f"明细 {line.id} 在途剩余不足，无法签收 {accepted} / 拒收 {rejected}",
                http_status=422,
            )
        dest_batch_id = None
        if accepted:
            dest = _dest_batch_for_receipt(session, sh, line.batch_id, line.part_id)
            session.execute(
                update(InventoryBatch)
                .where(InventoryBatch.id == dest.id)
                .values(
                    qty_on_hand=InventoryBatch.qty_on_hand + accepted,
                    qty_available=InventoryBatch.qty_available + accepted,
                    version=InventoryBatch.version + 1,
                )
                .execution_options(**_SYNC_OFF)
            )
            ledger.post_movement(
                session,
                txn_group=ledger.new_txn_group(),
                entry_type=EntryType.RECEIVE,
                part_id=line.part_id,
                qty=accepted,
                from_bucket=Bucket.IN_TRANSIT,
                to_bucket=Bucket.AVAILABLE,
                to_warehouse_id=sh.dest_warehouse_id,
                from_batch_id=line.batch_id,
                to_batch_id=dest.id,
                shipment_id=sh.id,
                ref_type="receipt",
                ref_id=receipt.id,
                remark=f"签收入库 {sh.shipment_no}",
                now=now,
            )
            accepted_total += accepted
            dest_batch_id = dest.id
        if rejected:
            session.execute(
                update(InventoryBatch)
                .where(InventoryBatch.id == line.batch_id)
                .values(
                    qty_on_hand=InventoryBatch.qty_on_hand + rejected,
                    qty_available=InventoryBatch.qty_available + rejected,
                    version=InventoryBatch.version + 1,
                )
                .execution_options(**_SYNC_OFF)
            )
            ledger.post_movement(
                session,
                txn_group=ledger.new_txn_group(),
                entry_type=EntryType.REJECT_RETURN,
                part_id=line.part_id,
                qty=rejected,
                from_bucket=Bucket.IN_TRANSIT,
                to_bucket=Bucket.AVAILABLE,
                to_warehouse_id=sh.source_warehouse_id,
                from_batch_id=line.batch_id,
                to_batch_id=line.batch_id,
                shipment_id=sh.id,
                ref_type="receipt",
                ref_id=receipt.id,
                compensates_txn=line.ship_txn_group,
                remark=f"拒收退回来源仓 {sh.shipment_no}: {note}",
                now=now,
            )
            rejected_total += rejected
        session.add(
            ReceiptLine(
                receipt_id=receipt.id,
                shipment_line_id=line.id,
                qty_accepted=accepted,
                qty_rejected=rejected,
                dest_batch_id=dest_batch_id,
            )
        )

    session.execute(
        update(TransferRequest)
        .where(TransferRequest.id == req.id)
        .values(
            qty_received=TransferRequest.qty_received + accepted_total,
            qty_rejected=TransferRequest.qty_rejected + rejected_total,
            version=TransferRequest.version + 1,
        )
        .execution_options(**_SYNC_OFF)
    )
    _finish(session)
    sh = get_shipment(session, shipment_id)
    req = get_request(session, sh.request_id)
    refresh_shipment_status(session, sh, receiver, now)
    refresh_request_status(session, req, receiver, now)
    log_event(
        session,
        "shipment",
        sh.id,
        "RECEIPT",
        None,
        sh.status,
        receiver,
        {"receipt_id": receipt.id, "accepted": accepted_total, "rejected": rejected_total},
        now,
    )
    session.flush()
    _fault("receive.after_updates")
    return receipt


# ---------------------------------------------------------------- 改道 / 超时 / 退回


def reroute_shipment(
    session: Session,
    shipment_id: int,
    *,
    new_dest_warehouse_id: int,
    new_eta: datetime | None = None,
    actor: str = "",
    reason: str = "",
    now: datetime | None = None,
) -> Shipment:
    """在途改道：仅改变目的仓（数量不动，无需台账分录），事件留痕。"""
    sh = _guard(session, Shipment, shipment_id, "装运单", "reroute", SHIPMENT_COMMAND_SOURCES["reroute"])
    _get(session, Warehouse, new_dest_warehouse_id, "仓库")
    if new_dest_warehouse_id == sh.dest_warehouse_id:
        raise DomainError("VALIDATION", "新目的仓与当前目的仓相同", http_status=422)
    old_dest = sh.dest_warehouse_id
    sh.dest_warehouse_id = new_dest_warehouse_id
    if new_eta is not None:
        sh.eta = new_eta
    sh.version += 1
    log_event(
        session,
        "shipment",
        sh.id,
        "REROUTED",
        None,
        sh.status,
        actor,
        {"from_warehouse_id": old_dest, "to_warehouse_id": new_dest_warehouse_id, "reason": reason},
        now,
    )
    session.flush()
    return sh


def extend_shipment_eta(session: Session, shipment_id: int, *, new_eta: datetime, actor: str = "", now: datetime | None = None) -> Shipment:
    sh = _guard(session, Shipment, shipment_id, "装运单", "extend", SHIPMENT_COMMAND_SOURCES["extend"])
    sh.eta = new_eta
    sh.version += 1
    if sh.status == ShipmentStatus.TIMED_OUT.value:
        _set_status(session, sh, "shipment", ShipmentStatus.IN_TRANSIT.value, "ETA_EXTENDED", actor, now)
    else:
        log_event(session, "shipment", sh.id, "ETA_EXTENDED", None, sh.status, actor, {"new_eta": new_eta.isoformat()}, now)
    session.flush()
    return sh


def return_shipment(session: Session, shipment_id: int, *, actor: str = "", reason: str = "", now: datetime | None = None) -> Shipment:
    """把剩余在途全部退回发货仓（超时处置或主动召回）。

    每行写 TIMEOUT_RETURN 补偿分录，compensates_txn 指向原 SHIP 事务组。
    """
    sh = _guard(session, Shipment, shipment_id, "装运单", "return", SHIPMENT_COMMAND_SOURCES["return"])
    req = get_request(session, sh.request_id)
    now = _now(now)
    total = 0
    for line in sh.lines:
        q = line.qty_shipped - line.qty_received - line.qty_rejected - line.qty_returned
        if q <= 0:
            continue
        # 条件更新：并发退回/签收不会重复回库
        result = session.execute(
            update(ShipmentLine)
            .where(
                ShipmentLine.id == line.id,
                ShipmentLine.qty_shipped - ShipmentLine.qty_received - ShipmentLine.qty_rejected - ShipmentLine.qty_returned >= q,
            )
            .values(qty_returned=ShipmentLine.qty_returned + q)
            .execution_options(**_SYNC_OFF)
        )
        if result.rowcount != 1:
            raise DomainError("STOCK_CONFLICT", "退回时在途数量冲突，请重试", http_status=409)
        session.execute(
            update(InventoryBatch)
            .where(InventoryBatch.id == line.batch_id)
            .values(
                qty_on_hand=InventoryBatch.qty_on_hand + q,
                qty_available=InventoryBatch.qty_available + q,
                version=InventoryBatch.version + 1,
            )
            .execution_options(**_SYNC_OFF)
        )
        ledger.post_movement(
            session,
            txn_group=ledger.new_txn_group(),
            entry_type=EntryType.TIMEOUT_RETURN,
            part_id=line.part_id,
            qty=q,
            from_bucket=Bucket.IN_TRANSIT,
            to_bucket=Bucket.AVAILABLE,
            to_warehouse_id=sh.source_warehouse_id,
            from_batch_id=line.batch_id,
            to_batch_id=line.batch_id,
            shipment_id=sh.id,
            ref_type="shipment",
            ref_id=sh.id,
            compensates_txn=line.ship_txn_group,
            remark=f"退回发货仓 {sh.shipment_no}: {reason}",
            now=now,
        )
        total += q
    if total == 0:
        raise DomainError("VALIDATION", "该装运单没有可退回的在途数量", http_status=422)
    session.execute(
        update(TransferRequest)
        .where(TransferRequest.id == req.id)
        .values(qty_returned=TransferRequest.qty_returned + total, version=TransferRequest.version + 1)
        .execution_options(**_SYNC_OFF)
    )
    _finish(session)
    sh = get_shipment(session, shipment_id)
    req = get_request(session, sh.request_id)
    refresh_shipment_status(session, sh, actor, now)
    refresh_request_status(session, req, actor, now)
    log_event(session, "shipment", sh.id, "RETURNED", None, sh.status, actor, {"qty": total, "reason": reason}, now)
    session.flush()
    return sh


def timeout_sweep(session: Session, *, now: datetime | None = None) -> list[int]:
    """扫描超过 ETA 仍未了结的装运单，标记 TIMED_OUT。返回受影响的装运单 id。"""
    now = _now(now)
    candidates = session.execute(
        select(Shipment.id, Shipment.status)
        .where(
            Shipment.status.in_([s.value for s in SHIPMENT_COMMAND_SOURCES["timeout"]]),
            Shipment.eta.is_not(None),
            Shipment.eta < now,
        )
    ).all()
    marked: list[int] = []
    for shipment_id, from_status in candidates:
        result = session.execute(
            update(Shipment)
            .where(
                Shipment.id == shipment_id,
                Shipment.status.in_([s.value for s in SHIPMENT_COMMAND_SOURCES["timeout"]]),
            )
            .values(status=ShipmentStatus.TIMED_OUT.value, version=Shipment.version + 1)
            .execution_options(**_SYNC_OFF)
        )
        if result.rowcount == 1:
            log_event(session, "shipment", shipment_id, "TIMEOUT", from_status, ShipmentStatus.TIMED_OUT.value, "system", now=now)
            marked.append(shipment_id)
    session.flush()
    return marked


# ---------------------------------------------------------------- 追踪与对账


def trace_request(session: Session, request_id: int) -> dict:
    """逐数量追踪：申请下每一件货当前处于 预留/在途/已签收/已退回/已取消 哪个位置。"""
    req = get_request(session, request_id)
    positions: list[TracePosition] = []

    batch_nos = {b.id: b.batch_no for b in session.execute(select(InventoryBatch)).scalars()}

    # 1) 仍预留在来源仓的数量
    for rl in session.execute(select(ReservationLine).where(ReservationLine.request_id == req.id)).scalars():
        if rl.remaining > 0:
            positions.append(
                TracePosition(
                    bucket=Bucket.RESERVED.value,
                    qty=rl.remaining,
                    warehouse_id=req.source_warehouse_id,
                    batch_id=rl.batch_id,
                    batch_no=batch_nos.get(rl.batch_id),
                )
            )

    # 2) 在途数量（按装运单与来源批次），并汇总签收落批与退回
    shipments = session.execute(select(Shipment).where(Shipment.request_id == req.id)).scalars()
    received_by_dest_batch: dict[int, int] = {}
    returned_by_batch: dict[int, int] = {}
    for sh in shipments:
        for ln in sh.lines:
            if ln.qty_in_transit > 0:
                positions.append(
                    TracePosition(
                        bucket=Bucket.IN_TRANSIT.value,
                        qty=ln.qty_in_transit,
                        batch_id=ln.batch_id,
                        batch_no=batch_nos.get(ln.batch_id),
                        shipment_id=sh.id,
                        shipment_no=sh.shipment_no,
                        shipment_status=sh.status,
                    )
                )
            if ln.qty_returned + ln.qty_rejected > 0:
                returned_by_batch[ln.batch_id] = returned_by_batch.get(ln.batch_id, 0) + ln.qty_returned + ln.qty_rejected
        receipt_rows = session.execute(
            select(ReceiptLine).join(Receipt, ReceiptLine.receipt_id == Receipt.id).where(Receipt.shipment_id == sh.id)
        ).scalars()
        for rln in receipt_rows:
            if rln.qty_accepted > 0 and rln.dest_batch_id is not None:
                received_by_dest_batch[rln.dest_batch_id] = received_by_dest_batch.get(rln.dest_batch_id, 0) + rln.qty_accepted

    # 3) 已签收落入目的仓的数量
    for dest_batch_id, qty in received_by_dest_batch.items():
        dest = session.get(InventoryBatch, dest_batch_id)
        positions.append(
            TracePosition(
                bucket="RECEIVED",
                qty=qty,
                warehouse_id=dest.warehouse_id if dest else None,
                batch_id=dest_batch_id,
                batch_no=dest.batch_no if dest else None,
            )
        )

    # 4) 拒收/超时退回来源仓的数量
    for batch_id, qty in returned_by_batch.items():
        positions.append(
            TracePosition(
                bucket="RETURNED_TO_SOURCE",
                qty=qty,
                warehouse_id=req.source_warehouse_id,
                batch_id=batch_id,
                batch_no=batch_nos.get(batch_id),
            )
        )

    # 5) 已取消（无物理位置）
    if req.qty_cancelled > 0:
        positions.append(TracePosition(bucket="CANCELLED", qty=req.qty_cancelled))

    accounted = sum(p.qty for p in positions)
    out = request_out(session, req)
    return {
        "request": out,
        "positions": positions,
        "conservation": {
            "qty_approved": req.qty_approved,
            "accounted": accounted,
            "balanced": accounted == req.qty_reserved,
            "in_transit": out.qty_in_transit,
            "reserved_remaining": out.qty_reserved_remaining,
        },
    }


def list_events(session: Session, entity_type: str, entity_id: int) -> list[EventLog]:
    return list(
        session.execute(
            select(EventLog)
            .where(EventLog.entity_type == entity_type, EventLog.entity_id == entity_id)
            .order_by(EventLog.id)
        ).scalars()
    )
