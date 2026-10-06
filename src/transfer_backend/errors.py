"""领域错误与 HTTP 映射。"""
from __future__ import annotations

from typing import Any


class DomainError(Exception):
    """业务规则违例。code 供客户端程序化判断，http_status 用于接口映射。"""

    def __init__(self, code: str, message: str, http_status: int = 409, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
        self.details = details or {}


def not_found(entity: str, ident: Any) -> DomainError:
    return DomainError("NOT_FOUND", f"{entity} 不存在：{ident}", http_status=404, details={"entity": entity, "id": str(ident)})


def invalid_state(entity: str, ident: Any, status: str, command: str) -> DomainError:
    return DomainError(
        "INVALID_STATE",
        f"{entity} {ident} 当前状态为 {status}，不允许执行 {command}",
        http_status=409,
        details={"entity": entity, "id": str(ident), "status": status, "command": command},
    )


def insufficient_stock(part_id: int, warehouse_id: int, requested: int, available: int) -> DomainError:
    return DomainError(
        "INSUFFICIENT_STOCK",
        f"仓库 {warehouse_id} 零件 {part_id} 可用量不足：申请 {requested}，可用 {available}",
        http_status=409,
        details={"part_id": part_id, "warehouse_id": warehouse_id, "requested": requested, "available": available},
    )
