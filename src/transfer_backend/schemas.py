"""接口出入参（Pydantic v2）。"""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

# ---------- 基础档案 ----------


class WarehouseCreate(BaseModel):
    code: str = Field(min_length=1, max_length=32)
    name: str = Field(min_length=1, max_length=128)


class WarehouseOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    code: str
    name: str


class PartCreate(BaseModel):
    sku: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=128)


class PartOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    sku: str
    name: str


class BatchCreate(BaseModel):
    """期初/采购入库建档。"""

    warehouse_id: int
    part_id: int
    batch_no: str = Field(min_length=1, max_length=64)
    qty: int = Field(gt=0)


class BatchOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    warehouse_id: int
    part_id: int
    batch_no: str
    origin_batch_id: int | None
    qty_on_hand: int
    qty_available: int
    qty_reserved: int


# ---------- 调拨申请 ----------


class TransferRequestCreate(BaseModel):
    source_warehouse_id: int
    dest_warehouse_id: int
    part_id: int
    qty: int = Field(gt=0)
    reason: str = ""
    created_by: str = ""


class TransferRequestOut(BaseModel):
    id: int
    request_no: str
    status: str
    source_warehouse_id: int
    dest_warehouse_id: int
    part_id: int
    qty_requested: int
    qty_approved: int
    qty_reserved: int
    qty_reserved_remaining: int
    qty_shipped: int
    qty_in_transit: int
    qty_received: int
    qty_rejected: int
    qty_returned: int
    qty_cancelled: int
    reason: str
    created_by: str
    version: int
    created_at: datetime


class ApprovalIn(BaseModel):
    decision: str = Field(pattern="^(APPROVE|REJECT)$")
    qty: int | None = Field(default=None, gt=0, description="批准数量，缺省为申请数量")
    approver: str = ""
    comment: str = ""


class ApprovalOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    request_id: int
    decision: str
    qty_approved: int
    approver: str
    comment: str
    created_at: datetime


class CancelIn(BaseModel):
    qty: int | None = Field(default=None, gt=0, description="取消数量，缺省取消全部未发剩余")
    actor: str = ""
    reason: str = ""


# ---------- 装运与签收 ----------


class ShipmentLineIn(BaseModel):
    batch_id: int
    qty: int = Field(gt=0)


class ShipmentCreate(BaseModel):
    """建装运单。lines 缺省时按预留批次 FIFO 自动分配 qty。"""

    qty: int | None = Field(default=None, gt=0)
    lines: list[ShipmentLineIn] | None = None
    carrier: str = ""
    tracking_no: str = ""
    eta: datetime | None = None


class ShipmentLineOut(BaseModel):
    id: int
    batch_id: int
    part_id: int
    qty_shipped: int
    qty_received: int
    qty_rejected: int
    qty_returned: int
    qty_in_transit: int


class ShipmentOut(BaseModel):
    id: int
    shipment_no: str
    request_id: int
    source_warehouse_id: int
    dest_warehouse_id: int
    status: str
    carrier: str
    tracking_no: str
    eta: datetime | None
    shipped_at: datetime | None
    version: int
    lines: list[ShipmentLineOut]


class ReceiptLineIn(BaseModel):
    shipment_line_id: int
    qty_accepted: int = Field(default=0, ge=0)
    qty_rejected: int = Field(default=0, ge=0)


class ReceiptCreate(BaseModel):
    lines: list[ReceiptLineIn] = Field(min_length=1)
    receiver: str = ""
    note: str = ""


class ReceiptLineOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    shipment_line_id: int
    qty_accepted: int
    qty_rejected: int
    dest_batch_id: int | None


class ReceiptOut(BaseModel):
    id: int
    shipment_id: int
    receiver: str
    note: str
    created_at: datetime
    lines: list[ReceiptLineOut]


class RerouteIn(BaseModel):
    new_dest_warehouse_id: int
    new_eta: datetime | None = None
    actor: str = ""
    reason: str = ""


class ExtendIn(BaseModel):
    new_eta: datetime
    actor: str = ""


class ReturnIn(BaseModel):
    actor: str = ""
    reason: str = ""


class TimeoutSweepIn(BaseModel):
    now: datetime | None = None


# ---------- 追踪与对账 ----------


class TracePosition(BaseModel):
    bucket: str
    qty: int
    warehouse_id: int | None = None
    batch_id: int | None = None
    batch_no: str | None = None
    shipment_id: int | None = None
    shipment_no: str | None = None
    shipment_status: str | None = None


class TraceOut(BaseModel):
    request: TransferRequestOut
    positions: list[TracePosition]
    conservation: dict


class LedgerEntryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    txn_group: str
    entry_type: str
    part_id: int
    batch_id: int | None
    warehouse_id: int | None
    shipment_id: int | None
    bucket: str
    qty_delta: int
    ref_type: str
    ref_id: int | None
    compensates_txn: str | None
    remark: str
    created_at: datetime


class EventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    entity_type: str
    entity_id: int
    event_type: str
    from_status: str | None
    to_status: str | None
    actor: str
    detail: str
    created_at: datetime


class ReconciliationOut(BaseModel):
    part_id: int
    balanced: bool
    discrepancies: list
    unbalanced_txn_groups: list[str]
    positions: list[dict]
