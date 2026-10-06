"""数据库引擎与会话管理。

默认使用 SQLite（便于本地与测试运行），通过环境变量 TRANSFER_DATABASE_URL
可切换到 PostgreSQL 等。所有并发安全依赖「条件更新 + 行数校验」的
compare-and-swap 语义，在两类数据库上都成立。
"""
from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

DEFAULT_URL = "sqlite:///./transfer.sqlite3"


class Base(DeclarativeBase):
    pass


def make_engine(url: str | None = None) -> Engine:
    url = url or os.environ.get("TRANSFER_DATABASE_URL", DEFAULT_URL)
    connect_args = {}
    if url.startswith("sqlite"):
        # check_same_thread=False 允许测试中的多线程并发；timeout 让写锁等待而非立即报错
        connect_args = {"check_same_thread": False, "timeout": 30}
    engine = create_engine(url, connect_args=connect_args, pool_pre_ping=True)

    if url.startswith("sqlite"):

        @event.listens_for(engine, "connect")
        def _sqlite_pragmas(dbapi_conn, _record):  # noqa: ANN001
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA busy_timeout = 30000")
            cur.execute("PRAGMA journal_mode = WAL")
            cur.execute("PRAGMA foreign_keys = ON")
            cur.close()

    return engine


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def init_schema(engine: Engine) -> None:
    from . import models  # noqa: F401  确保表已注册

    Base.metadata.create_all(engine)


@contextmanager
def session_scope(factory: sessionmaker[Session]) -> Iterator[Session]:
    """服务层/测试使用的事务上下文：正常结束提交，异常回滚。"""
    session = factory()
    try:
        yield session
        session.commit()
    except BaseException:
        session.rollback()
        raise
    finally:
        session.close()
