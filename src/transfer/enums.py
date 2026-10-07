"""申请单 / 装运单状态机与分录类型。

设计要点
========

* 申请单生命周期::

    草拟 ──提交──▶ 待审批 ──审批通过──▶ 已批准
                     │                    │
                     └──驳回──▶ 已驳回     ├──部分装运──▶ 部分装运
                     ▲                    ├──全部发运(数量付清)──▶ 已发运
                     │                    ├──拒收/取消──▶ 已取消(补偿)
                  (修改后重提)            └──超时──▶ 已超时(在途回滚)
    已批准/部分装运/已发运 ──全部签收入库──▶ 已签收
    已批准/部分装运 ──剩余不再发运──▶ 已关闭

* 装运单生命周期（确认=原子地把来源批次可用量转为在途）::

    已计划 ──确认──▶ 在途 ──┬─全部签收─▶ 已签收
                            ├─部分签收─▶ 部分签收 ──余下次签收─▶ 已签收
                            ├─拒收────▶ 已拒收(在途原子退回来源批次, 补偿分录)
                            └─改道────▶ 在途(仅目的地变更, 数量/来源批次不变)

  已计划装运单可取消（无库存影响，只留审计记录）。

* 每次库存移动都写一张**平衡的复式分录**（同批 ``entries`` 借贷之和为 0），
  任何失败路径都写反向补偿分录，因此账本恒守恒、可审计。
"""
from __future__ import annotations

import enum


class RequestStatus(str, enum.Enum):
    DRAFT = "草拟"
    PENDING = "待审批"
    APPROVED = "已批准"
    PARTIALLY_SHIPPED = "部分装运"
    SHIPPED = "已发运"
    PARTIALLY_RECEIVED = "部分签收"
    RECEIVED = "已签收"
    REJECTED = "已驳回"
    CANCELLED = "已取消"
    TIMEOUT = "已超时"
    CLOSED = "已关闭"


class ShipmentStatus(str, enum.Enum):
    PLANNED = "已计划"
    IN_TRANSIT = "在途"
    PARTIALLY_RECEIVED = "部分签收"
    RECEIVED = "已签收"
    REJECTED = "已拒收"
    TIMEOUT = "已超时"
    CANCELLED = "已取消"


class EntryType(str, enum.Enum):
    """分录类型。正向移动与补偿一一对应。

    出库（``SHIP_OUT``）的补偿分录是拒收/超时退回
    （``REJECT_RETURN`` / ``TIMEOUT_RETURN``，即其反向平衡凭证）；
    签收（``RECEIVE``）在同一事务内完成，失败整体回滚，无需事后补偿。
    """

    SHIP_OUT = "出库转在途"          # DR 在途 / CR 可用（按来源批次行）
    RECEIVE = "签收入库"             # DR 入库可用 / CR 在途
    REJECT_RETURN = "拒收退回"        # DR 来源批次可用 / CR 在途（SHIP_OUT 的逆）
    REROUTE = "改道"                 # 在途在目标仓间划转（借贷各一，净 0）
    TIMEOUT_RETURN = "超时退回"       # 同 REJECT_RETURN，标注超时来源
    PLAN_CANCEL = "取消计划装运"      # 占位分录，无库存影响
    APPROVAL = "审批"
    REQUEST_CANCEL = "取消申请"


# 状态机：当前状态 -> 允许迁移到的状态
REQUEST_TRANSITIONS: dict[RequestStatus, frozenset[RequestStatus]] = {
    RequestStatus.DRAFT: frozenset({RequestStatus.PENDING, RequestStatus.CANCELLED}),
    RequestStatus.PENDING: frozenset({
        RequestStatus.APPROVED,
        RequestStatus.REJECTED,
        RequestStatus.CANCELLED,
    }),
    RequestStatus.APPROVED: frozenset({
        RequestStatus.PARTIALLY_SHIPPED,
        RequestStatus.SHIPPED,
        RequestStatus.RECEIVED,  # 零数量直发（理论保留，正常不会走）
        RequestStatus.CANCELLED,
        RequestStatus.CLOSED,
    }),
    RequestStatus.PARTIALLY_SHIPPED: frozenset({
        RequestStatus.PARTIALLY_SHIPPED,  # 再次部分装运/部分签收，自循环
        RequestStatus.SHIPPED,
        RequestStatus.PARTIALLY_RECEIVED,
        RequestStatus.RECEIVED,
        RequestStatus.APPROVED,           # 全部在途被拒收/退回，可重新发运
        RequestStatus.TIMEOUT,
        RequestStatus.CANCELLED,          # 含在途货物时取消，在途由超时流程退回
        RequestStatus.CLOSED,
    }),
    RequestStatus.SHIPPED: frozenset({
        RequestStatus.SHIPPED,
        RequestStatus.PARTIALLY_RECEIVED,  # 部分签收
        RequestStatus.PARTIALLY_SHIPPED,  # 拒收部分数量后退回，可再补发
        RequestStatus.APPROVED,           # 全部拒收退回
        RequestStatus.RECEIVED,
        RequestStatus.TIMEOUT,
        RequestStatus.CANCELLED,
    }),
    RequestStatus.PARTIALLY_RECEIVED: frozenset({
        RequestStatus.PARTIALLY_RECEIVED,  # 继续部分签收
        RequestStatus.RECEIVED,
        RequestStatus.PARTIALLY_SHIPPED,   # 拒收部分后退回，可补发
        RequestStatus.TIMEOUT,
        RequestStatus.CANCELLED,
    }),
    RequestStatus.RECEIVED: frozenset({RequestStatus.CLOSED}),
    RequestStatus.REJECTED: frozenset({RequestStatus.PENDING}),  # 修改后重新提交
    RequestStatus.CANCELLED: frozenset(),
    RequestStatus.TIMEOUT: frozenset({
        RequestStatus.PARTIALLY_SHIPPED,  # 重新发运
        RequestStatus.SHIPPED,
        RequestStatus.RECEIVED,
        RequestStatus.CLOSED,
    }),
    RequestStatus.CLOSED: frozenset(),
}

SHIPMENT_TRANSITIONS: dict[ShipmentStatus, frozenset[ShipmentStatus]] = {
    ShipmentStatus.PLANNED: frozenset({
        ShipmentStatus.IN_TRANSIT,
        ShipmentStatus.CANCELLED,
    }),
    ShipmentStatus.IN_TRANSIT: frozenset({
        ShipmentStatus.PARTIALLY_RECEIVED,
        ShipmentStatus.RECEIVED,
        ShipmentStatus.REJECTED,
        ShipmentStatus.TIMEOUT,
        # 改道后仍为在途（自循环，仅 destination_warehouse 改变）
        ShipmentStatus.IN_TRANSIT,
    }),
    ShipmentStatus.PARTIALLY_RECEIVED: frozenset({
        ShipmentStatus.PARTIALLY_RECEIVED,  # 继续部分签收
        ShipmentStatus.RECEIVED,
        ShipmentStatus.REJECTED,
        ShipmentStatus.TIMEOUT,
        ShipmentStatus.IN_TRANSIT,          # 剩余在途货物可改道
    }),
    ShipmentStatus.RECEIVED: frozenset(),
    ShipmentStatus.REJECTED: frozenset(),
    ShipmentStatus.TIMEOUT: frozenset(),
    ShipmentStatus.CANCELLED: frozenset(),
}

# 哪些状态表示货物确已离开来源仓、在途或已部分签收（占用在途量）
LIVE_SHIPMENT_STATUSES = frozenset({
    ShipmentStatus.IN_TRANSIT,
    ShipmentStatus.PARTIALLY_RECEIVED,
})


def assert_request_transition(current: RequestStatus, target: RequestStatus) -> None:
    if target not in REQUEST_TRANSITIONS.get(current, frozenset()):
        raise _transition_error("申请单", current, target)


def assert_shipment_transition(current: ShipmentStatus, target: ShipmentStatus) -> None:
    if target not in SHIPMENT_TRANSITIONS.get(current, frozenset()):
        raise _transition_error("装运单", current, target)


def _transition_error(kind: str, current: enum.Enum, target: enum.Enum) -> Exception:
    from .errors import InvalidTransitionError

    return InvalidTransitionError(f"{kind}状态不允许从 {current.value} 迁移到 {target.value}")
