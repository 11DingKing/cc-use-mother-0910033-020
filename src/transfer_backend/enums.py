"""状态枚举与状态机定义。

申请（TransferRequest）与装运单（Shipment）各自维护一张状态机：

- 命令型迁移（提交/审批/取消/确认/签收/改道/退回/超时）显式校验来源状态；
- 履约型状态（部分装运/已装运/部分签收/已签收/已关闭）由计数器推导，
  每次数量变动后调用 refresh 函数重算，保证状态与台账一致。
"""
from __future__ import annotations

import enum


class StrEnum(str, enum.Enum):
    """Python 3.11 兼容的 str 枚举。"""

    def __str__(self) -> str:  # pragma: no cover - 便于日志阅读
        return self.value


class RequestStatus(StrEnum):
    DRAFT = "DRAFT"  # 草拟
    SUBMITTED = "SUBMITTED"  # 已提交，待审批
    APPROVED = "APPROVED"  # 已审批并完成预留，尚未装运
    REJECTED = "REJECTED"  # 审批拒绝（终态）
    PARTIALLY_SHIPPED = "PARTIALLY_SHIPPED"  # 部分装运
    SHIPPED = "SHIPPED"  # 预留量全部装运
    PARTIALLY_RECEIVED = "PARTIALLY_RECEIVED"  # 部分签收
    RECEIVED = "RECEIVED"  # 全部签收（终态）
    CLOSED = "CLOSED"  # 已关闭：含拒收/退回/取消的混合了结（终态）
    CANCELLED = "CANCELLED"  # 已取消（终态）


class ShipmentStatus(StrEnum):
    PENDING = "PENDING"  # 待确认（已建单未扣减库存）
    IN_TRANSIT = "IN_TRANSIT"  # 在途（已原子扣减并记录在途）
    PARTIALLY_RECEIVED = "PARTIALLY_RECEIVED"  # 部分签收
    TIMED_OUT = "TIMED_OUT"  # 超过 ETA 未了结
    RECEIVED = "RECEIVED"  # 全部签收（终态）
    REJECTED = "REJECTED"  # 全部拒收退回（终态）
    RESOLVED_WITH_REJECTION = "RESOLVED_WITH_REJECTION"  # 签收与拒收/退回混合了结（终态）
    RETURNED = "RETURNED"  # 剩余在途全部退回发货仓（终态）
    CANCELLED = "CANCELLED"  # 确认前取消（终态）


class Bucket(StrEnum):
    """台账桶：数量在出库、在途、入库之间的位置。"""

    EXTERNAL = "EXTERNAL"  # 系统外部（期初入库的来源）
    AVAILABLE = "AVAILABLE"  # 某仓可用
    RESERVED = "RESERVED"  # 某仓已预留（待出库）
    IN_TRANSIT = "IN_TRANSIT"  # 在途（挂在装运单上）


class EntryType(StrEnum):
    """台账分录类型；每个事务组（txn_group）内的分录借贷平衡。"""

    INBOUND = "INBOUND"  # 期初/采购入库：EXTERNAL -> AVAILABLE
    RESERVE = "RESERVE"  # 审批预留：AVAILABLE -> RESERVED
    RELEASE = "RELEASE"  # 取消释放：RESERVED -> AVAILABLE
    SHIP = "SHIP"  # 装运确认：RESERVED -> IN_TRANSIT
    RECEIVE = "RECEIVE"  # 签收：IN_TRANSIT -> AVAILABLE(目的仓)
    REJECT_RETURN = "REJECT_RETURN"  # 拒收退回：IN_TRANSIT -> AVAILABLE(来源仓)，补偿分录
    TIMEOUT_RETURN = "TIMEOUT_RETURN"  # 超时/主动退回：IN_TRANSIT -> AVAILABLE(来源仓)，补偿分录


# 命令型迁移白名单：{命令: (允许的来源状态, ...)}
REQUEST_COMMAND_SOURCES: dict[str, tuple[RequestStatus, ...]] = {
    "submit": (RequestStatus.DRAFT,),
    "approve": (RequestStatus.SUBMITTED,),
    "reject": (RequestStatus.SUBMITTED,),
    "cancel": (
        RequestStatus.DRAFT,
        RequestStatus.SUBMITTED,
        RequestStatus.APPROVED,
        RequestStatus.PARTIALLY_SHIPPED,
        RequestStatus.SHIPPED,
        RequestStatus.PARTIALLY_RECEIVED,
    ),
    "create_shipment": (RequestStatus.APPROVED, RequestStatus.PARTIALLY_SHIPPED),
}

SHIPMENT_COMMAND_SOURCES: dict[str, tuple[ShipmentStatus, ...]] = {
    "confirm": (ShipmentStatus.PENDING,),
    "receive": (
        ShipmentStatus.IN_TRANSIT,
        ShipmentStatus.PARTIALLY_RECEIVED,
        ShipmentStatus.TIMED_OUT,
    ),
    "reroute": (ShipmentStatus.IN_TRANSIT, ShipmentStatus.TIMED_OUT),
    "extend": (ShipmentStatus.IN_TRANSIT, ShipmentStatus.TIMED_OUT),
    "return": (
        ShipmentStatus.IN_TRANSIT,
        ShipmentStatus.PARTIALLY_RECEIVED,
        ShipmentStatus.TIMED_OUT,
    ),
    "cancel": (ShipmentStatus.PENDING,),
    "timeout": (ShipmentStatus.IN_TRANSIT, ShipmentStatus.PARTIALLY_RECEIVED),
}

# 终态：不再接受任何命令
REQUEST_TERMINAL = {
    RequestStatus.REJECTED,
    RequestStatus.RECEIVED,
    RequestStatus.CLOSED,
    RequestStatus.CANCELLED,
}

SHIPMENT_TERMINAL = {
    ShipmentStatus.RECEIVED,
    ShipmentStatus.REJECTED,
    ShipmentStatus.RESOLVED_WITH_REJECTION,
    ShipmentStatus.RETURNED,
    ShipmentStatus.CANCELLED,
}
