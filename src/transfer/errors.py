"""领域错误。"""
from __future__ import annotations


class TransferError(Exception):
    """所有调拨领域错误的基类。"""


class ValidationError(TransferError):
    """入参不合法（数量、仓库、批次等）。"""


class NotFoundError(TransferError):
    """申请单、装运单或批次不存在。"""


class InvalidTransitionError(TransferError):
    """状态机不允许当前流转。"""


class InsufficientStockError(TransferError):
    """确认出库时来源批次可用量不足（并发竞争的失败方）。"""

    def __init__(self, warehouse: str, part_no: str, batch_no: str, required: int, available: int):
        super().__init__(
            f"批次可用量不足：仓库={warehouse} 零件={part_no} 批次={batch_no} "
            f"需要 {required}，可用 {available}"
        )
        self.warehouse = warehouse
        self.part_no = part_no
        self.batch_no = batch_no
        self.required = required
        self.available = available


class OverReceiptError(TransferError):
    """签收数量超过在途余量或申请数量。"""


class BusinessRuleError(TransferError):
    """其他业务规则冲突，如目的地非法、存在未结清装运单等。"""


class DuplicateShipmentError(InvalidTransitionError):
    """装运单已确认，重复确认被拒绝，防止失败重试造成重复发运。"""
