"""多仓库存调拨后端。

模块划分：

- ``enums``：申请单与装运单状态机（状态、合法流转、分录类型）。
- ``errors``：领域错误。
- ``models``：领域对象视图。
- ``store``：SQLite 存储（单连接 + ``BEGIN IMMEDIATE`` 串行写事务）。
- ``ledger``：平衡分录与守恒审计。
- ``service``：``TransferService`` 门面，覆盖批次、申请、审批、装运、
  签收、拒收、改道、超时补偿与追踪查询。
- ``http_api``：基于标准库 ``http.server`` 的 JSON 接口。
"""
from __future__ import annotations

from .errors import (
    BusinessRuleError,
    DuplicateShipmentError,
    InsufficientStockError,
    InvalidTransitionError,
    NotFoundError,
    OverReceiptError,
    TransferError,
    ValidationError,
)
from .service import TransferService

__all__ = [
    "TransferService",
    "TransferError",
    "ValidationError",
    "NotFoundError",
    "InvalidTransitionError",
    "InsufficientStockError",
    "OverReceiptError",
    "BusinessRuleError",
    "DuplicateShipmentError",
]
