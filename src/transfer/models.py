"""领域对象视图（只读 dataclass，由服务层从行记录构造）。"""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Batch:
    """仓库批次库存。

    ``available`` 可分配；``in_transit`` 已被确认的装运单占用、尚未签收；
    守恒关系见 ``ledger.audit_conservation``。
    """

    id: int
    warehouse: str
    part_no: str
    batch_no: str
    available: int
    in_transit: int

    @property
    def total(self) -> int:
        return self.available + self.in_transit


@dataclass(frozen=True)
class TransferRequestView:
    id: int
    part_no: str
    quantity: int
    from_warehouse: str
    to_warehouse: str
    status: str
    created_at: str
    shipped_qty: int = 0
    received_qty: int = 0
    timeout_at: str | None = None
    note: str = ""

    @property
    def open_qty(self) -> int:
        """仍需发运的数量 = 申请量 - 已发运量（拒收退回会冲减已发运）。"""
        return self.quantity - self.shipped_qty


@dataclass(frozen=True)
class ShipmentView:
    id: int
    request_id: int
    part_no: str
    source_warehouse: str
    destination_warehouse: str
    status: str
    created_at: str
    shipped_at: str | None = None
    deadline_at: str | None = None
    received_at: str | None = None
    original_destination: str | None = None
    quantity: int = 0          # 确认出库的总数量
    received_qty: int = 0
    rejected_qty: int = 0
    lines: tuple["ShipmentLineView", ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class ShipmentLineView:
    """装运明细：每件数量来自哪个来源批次，签收/拒收多少。"""

    id: int
    source_batch_id: int
    source_batch_no: str
    quantity: int
    received_qty: int
    rejected_qty: int

    @property
    def in_transit_qty(self) -> int:
        return self.quantity - self.received_qty - self.rejected_qty


@dataclass(frozen=True)
class ReceiptLineView:
    """签收记录：某次签收中，某来源批次的数量落到了哪个入库批次。"""

    id: int
    shipment_id: int
    request_id: int
    source_batch_id: int
    source_batch_no: str
    dest_batch_id: int
    dest_batch_no: str
    quantity: int
    rejected: bool
    created_at: str


@dataclass(frozen=True)
class LedgerEntryView:
    id: int
    entry_type: str
    ref_table: str
    ref_id: int
    request_id: int | None
    created_at: str
    note: str
    lines: tuple["LedgerLineView", ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class LedgerLineView:
    batch_id: int
    warehouse: str
    part_no: str
    batch_no: str
    bucket: str          # available / in_transit
    debit: int
    credit: int


@dataclass(frozen=True)
class QuantityTrace:
    """单件数量（按来源批次）在出库 / 在途 / 入库三个位置的分布。

    在途库存锚定在装运单的**当前目的地仓**（改道后随之划转）。
    对一个已确认装运行，恒有::

        shipped_qty == received_qty + rejected_qty + in_transit_qty
    """

    source_warehouse: str
    source_batch_no: str
    part_no: str
    shipped_qty: int
    received_qty: int          # 已签收入库（无论落在哪个入库批次）
    rejected_qty: int          # 拒收/超时已退回来源仓可用量
    in_transit_qty: int        # 仍在途
    in_transit_locations: tuple[str, ...] = ()  # 仍在途数量锚定的目的地仓
    receipts: tuple[ReceiptLineView, ...] = field(default_factory=tuple)


def asdict(obj: object) -> dict:
    """dataclass.asdict 的轻量替代（视图是 frozen 的，浅转换即可）。"""
    return dataclasses.asdict(obj)
