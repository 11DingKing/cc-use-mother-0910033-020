"""ORM 模型。

数量守恒依赖两条硬约束：

1. InventoryBatch 上 qty_on_hand = qty_available + qty_reserved（数据库 CheckConstraint）；
2. 所有跨桶移动都写 LedgerEntry 双式分录，同事务组借贷平衡（见 ledger.py）。

ShipmentLine 即在途记录：确认装运时写入，携带来源批次 batch_id 与
原事务组 ship_txn_group，后续的签收/拒收/退回都挂在它上面。
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .database import Base


def utcnow() -> datetime:
    """统一使用 naive UTC，避免 SQLite 时区比较歧义。"""
    return datetime.utcnow()


class Warehouse(Base):
    __tablename__ = "warehouses"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class Part(Base):
    __tablename__ = "parts"

    id: Mapped[int] = mapped_column(primary_key=True)
    sku: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class InventoryBatch(Base):
    """仓库批次。计数器是台账的缓存，全部通过条件更新修改。"""

    __tablename__ = "inventory_batches"
    __table_args__ = (
        UniqueConstraint("warehouse_id", "part_id", "batch_no", name="uq_batch_wh_part_no"),
        CheckConstraint("qty_on_hand >= 0", name="ck_batch_on_hand_nonneg"),
        CheckConstraint("qty_available >= 0", name="ck_batch_available_nonneg"),
        CheckConstraint("qty_reserved >= 0", name="ck_batch_reserved_nonneg"),
        CheckConstraint("qty_on_hand = qty_available + qty_reserved", name="ck_batch_conservation"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"), index=True)
    part_id: Mapped[int] = mapped_column(ForeignKey("parts.id"), index=True)
    batch_no: Mapped[str] = mapped_column(String(64))
    # 批次谱系：由调拨签收产生的批次指向来源批次
    origin_batch_id: Mapped[int | None] = mapped_column(ForeignKey("inventory_batches.id"))
    qty_on_hand: Mapped[int] = mapped_column(Integer, default=0)
    qty_available: Mapped[int] = mapped_column(Integer, default=0)
    qty_reserved: Mapped[int] = mapped_column(Integer, default=0)
    version: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class TransferRequest(Base):
    """调拨申请。计数器语义：

    - qty_reserved：累计已预留（审批时 == qty_approved，取消不减少它，取消量计入 qty_cancelled）
    - qty_shipped / qty_received / qty_rejected / qty_returned：累计履约量
    - 在途 = qty_shipped - qty_received - qty_rejected - qty_returned
    - 未发剩余 = qty_reserved - qty_shipped - qty_cancelled
    """

    __tablename__ = "transfer_requests"
    __table_args__ = (
        CheckConstraint("qty_requested > 0", name="ck_req_qty_requested_pos"),
        CheckConstraint("qty_approved >= 0", name="ck_req_qty_approved_nonneg"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    request_no: Mapped[str] = mapped_column(String(40), unique=True, index=True)
    source_warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"))
    dest_warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"))
    part_id: Mapped[int] = mapped_column(ForeignKey("parts.id"))
    qty_requested: Mapped[int] = mapped_column(Integer)
    qty_approved: Mapped[int] = mapped_column(Integer, default=0)
    qty_reserved: Mapped[int] = mapped_column(Integer, default=0)
    qty_shipped: Mapped[int] = mapped_column(Integer, default=0)
    qty_received: Mapped[int] = mapped_column(Integer, default=0)
    qty_rejected: Mapped[int] = mapped_column(Integer, default=0)
    qty_returned: Mapped[int] = mapped_column(Integer, default=0)
    qty_cancelled: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(32), index=True)
    reason: Mapped[str] = mapped_column(String(256), default="")
    created_by: Mapped[str] = mapped_column(String(64), default="")
    idempotency_key: Mapped[str | None] = mapped_column(String(80), unique=True)
    version: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)

    reservation_lines: Mapped[list["ReservationLine"]] = relationship(back_populates="request")


class ReservationLine(Base):
    """审批预留明细：申请 -> 来源批次的预留量。装运确认时逐行核销。"""

    __tablename__ = "reservation_lines"
    __table_args__ = (
        CheckConstraint("qty_reserved > 0", name="ck_rl_reserved_pos"),
        CheckConstraint("qty_shipped >= 0 AND qty_released >= 0", name="ck_rl_nonneg"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    request_id: Mapped[int] = mapped_column(ForeignKey("transfer_requests.id"), index=True)
    batch_id: Mapped[int] = mapped_column(ForeignKey("inventory_batches.id"))
    part_id: Mapped[int] = mapped_column(ForeignKey("parts.id"))
    qty_reserved: Mapped[int] = mapped_column(Integer)
    qty_shipped: Mapped[int] = mapped_column(Integer, default=0)
    qty_released: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    request: Mapped[TransferRequest] = relationship(back_populates="reservation_lines")

    @property
    def remaining(self) -> int:
        return self.qty_reserved - self.qty_shipped - self.qty_released


class Approval(Base):
    """审批记录（同意或拒绝）。"""

    __tablename__ = "approvals"

    id: Mapped[int] = mapped_column(primary_key=True)
    request_id: Mapped[int] = mapped_column(ForeignKey("transfer_requests.id"), index=True)
    decision: Mapped[str] = mapped_column(String(16))  # APPROVE / REJECT
    qty_approved: Mapped[int] = mapped_column(Integer, default=0)
    approver: Mapped[str] = mapped_column(String(64), default="")
    comment: Mapped[str] = mapped_column(String(256), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class Shipment(Base):
    __tablename__ = "shipments"

    id: Mapped[int] = mapped_column(primary_key=True)
    shipment_no: Mapped[str] = mapped_column(String(40), unique=True, index=True)
    request_id: Mapped[int] = mapped_column(ForeignKey("transfer_requests.id"), index=True)
    source_warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"))
    dest_warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"))
    status: Mapped[str] = mapped_column(String(32), index=True)
    carrier: Mapped[str] = mapped_column(String(64), default="")
    tracking_no: Mapped[str] = mapped_column(String(64), default="")
    eta: Mapped[datetime | None] = mapped_column(DateTime)
    idempotency_key: Mapped[str | None] = mapped_column(String(80), unique=True)
    version: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    shipped_at: Mapped[datetime | None] = mapped_column(DateTime)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)

    lines: Mapped[list["ShipmentLine"]] = relationship(back_populates="shipment", cascade="all, delete-orphan")


class ShipmentLine(Base):
    """装运明细 = 在途记录。batch_id 即来源批次，ship_txn_group 供补偿分录引用。"""

    __tablename__ = "shipment_lines"
    __table_args__ = (
        CheckConstraint("qty_shipped > 0", name="ck_sl_shipped_pos"),
        CheckConstraint(
            "qty_received >= 0 AND qty_rejected >= 0 AND qty_returned >= 0",
            name="ck_sl_nonneg",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    shipment_id: Mapped[int] = mapped_column(ForeignKey("shipments.id"), index=True)
    reservation_line_id: Mapped[int] = mapped_column(ForeignKey("reservation_lines.id"))
    batch_id: Mapped[int] = mapped_column(ForeignKey("inventory_batches.id"))
    part_id: Mapped[int] = mapped_column(ForeignKey("parts.id"))
    qty_shipped: Mapped[int] = mapped_column(Integer)
    qty_received: Mapped[int] = mapped_column(Integer, default=0)
    qty_rejected: Mapped[int] = mapped_column(Integer, default=0)
    qty_returned: Mapped[int] = mapped_column(Integer, default=0)
    ship_txn_group: Mapped[str | None] = mapped_column(String(40))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    shipment: Mapped[Shipment] = relationship(back_populates="lines")

    @property
    def qty_in_transit(self) -> int:
        return self.qty_shipped - self.qty_received - self.qty_rejected - self.qty_returned


class Receipt(Base):
    """签收单。一张装运单可多次签收（部分签收）。"""

    __tablename__ = "receipts"

    id: Mapped[int] = mapped_column(primary_key=True)
    shipment_id: Mapped[int] = mapped_column(ForeignKey("shipments.id"), index=True)
    receiver: Mapped[str] = mapped_column(String(64), default="")
    note: Mapped[str] = mapped_column(String(256), default="")
    idempotency_key: Mapped[str | None] = mapped_column(String(80), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    lines: Mapped[list["ReceiptLine"]] = relationship(back_populates="receipt", cascade="all, delete-orphan")


class ReceiptLine(Base):
    __tablename__ = "receipt_lines"

    id: Mapped[int] = mapped_column(primary_key=True)
    receipt_id: Mapped[int] = mapped_column(ForeignKey("receipts.id"), index=True)
    shipment_line_id: Mapped[int] = mapped_column(ForeignKey("shipment_lines.id"))
    qty_accepted: Mapped[int] = mapped_column(Integer, default=0)
    qty_rejected: Mapped[int] = mapped_column(Integer, default=0)
    # 签收数量落入的目的仓批次（批次谱系的下一环）
    dest_batch_id: Mapped[int | None] = mapped_column(ForeignKey("inventory_batches.id"))

    receipt: Mapped[Receipt] = relationship(back_populates="lines")


class LedgerEntry(Base):
    """库存台账分录（双式）。qty_delta 带符号，同一 txn_group 合计为 0。"""

    __tablename__ = "ledger_entries"
    __table_args__ = (
        CheckConstraint("qty_delta <> 0", name="ck_ledger_nonzero"),
        Index("ix_ledger_part_bucket", "part_id", "bucket"),
        Index("ix_ledger_txn", "txn_group"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    txn_group: Mapped[str] = mapped_column(String(40))
    entry_type: Mapped[str] = mapped_column(String(24))
    part_id: Mapped[int] = mapped_column(ForeignKey("parts.id"))
    batch_id: Mapped[int | None] = mapped_column(ForeignKey("inventory_batches.id"))
    warehouse_id: Mapped[int | None] = mapped_column(ForeignKey("warehouses.id"))
    shipment_id: Mapped[int | None] = mapped_column(ForeignKey("shipments.id"))
    bucket: Mapped[str] = mapped_column(String(16))
    qty_delta: Mapped[int] = mapped_column(Integer)
    ref_type: Mapped[str] = mapped_column(String(24), default="")
    ref_id: Mapped[int | None] = mapped_column(Integer)
    # 补偿分录指向被补偿的原事务组（拒收/退回/释放）
    compensates_txn: Mapped[str | None] = mapped_column(String(40))
    remark: Mapped[str] = mapped_column(String(256), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class EventLog(Base):
    """状态机事件审计：每次状态迁移与命令都留痕。"""

    __tablename__ = "event_log"
    __table_args__ = (Index("ix_event_entity", "entity_type", "entity_id"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    entity_type: Mapped[str] = mapped_column(String(24))  # request / shipment
    entity_id: Mapped[int] = mapped_column(Integer)
    event_type: Mapped[str] = mapped_column(String(32))
    from_status: Mapped[str | None] = mapped_column(String(32))
    to_status: Mapped[str | None] = mapped_column(String(32))
    actor: Mapped[str] = mapped_column(String(64), default="")
    detail: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class IdempotencyRecord(Base):
    """幂等记录。与业务写入在同一事务提交：崩溃即整体回滚，重试安全。"""

    __tablename__ = "idempotency_records"

    key: Mapped[str] = mapped_column(String(80), primary_key=True)
    endpoint: Mapped[str] = mapped_column(String(160))
    fingerprint: Mapped[str] = mapped_column(String(64))
    response_body: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
