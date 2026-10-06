"""幂等支持。

要点：幂等记录与业务写入在**同一个数据库事务**中提交。
若进程在「扣库存」与「记录在途」之间崩溃，整个事务回滚，
客户端携带同一 Idempotency-Key 重试会重新执行而非产生重复效果；
若提交成功后客户端未收到响应，重试直接回放已存响应。
"""
from __future__ import annotations

import hashlib
from typing import Callable, TypeVar

from fastapi import Header, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session

from .errors import DomainError
from .models import IdempotencyRecord

T = TypeVar("T", bound=BaseModel)


class IdemContext:
    """一次请求的幂等上下文：键 + 请求指纹。"""

    def __init__(self, key: str | None, endpoint: str, fingerprint: str):
        self.key = key
        self.endpoint = endpoint
        self.fingerprint = fingerprint


async def idem_dependency(
    request: Request,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> IdemContext:
    raw = await request.body()
    fingerprint = hashlib.sha256(request.url.path.encode() + b"|" + raw).hexdigest()
    endpoint = f"{request.method} {request.url.path}"
    return IdemContext(idempotency_key, endpoint, fingerprint)


def run_idempotent(
    session: Session,
    idem: IdemContext,
    *,
    response_model: type[T],
    fn: Callable[[], T],
) -> tuple[T, bool]:
    """执行 fn 并按 Idempotency-Key 去重。返回 (响应体, 是否回放)。"""
    if not idem.key:
        return fn(), False
    record = session.get(IdempotencyRecord, idem.key)
    if record is not None:
        if record.endpoint != idem.endpoint or record.fingerprint != idem.fingerprint:
            raise DomainError(
                "IDEMPOTENCY_CONFLICT",
                "同一 Idempotency-Key 提交了不同的请求内容",
                http_status=409,
                details={"key": idem.key},
            )
        return response_model.model_validate_json(record.response_body), True
    result = fn()
    session.add(
        IdempotencyRecord(
            key=idem.key,
            endpoint=idem.endpoint,
            fingerprint=idem.fingerprint,
            response_body=result.model_dump_json(),
        )
    )
    return result, False
