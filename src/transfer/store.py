"""SQLite 存储层。

并发模型
========

* 单文件 SQLite，服务持有一个连接，开启 ``WAL`` 与外键约束。
* 所有写操作走 ``BEGIN IMMEDIATE``：第一个写者立即拿到 RESERVED 锁，
  其余并发申请在提交时串行等待；后到者在事务内重新读到最新批次余量，
  余量不足即抛 :class:`InsufficientStockError` 回滚。
  由此杜绝"先扣库存、再写在途"的部分失败窗口——扣减与在途行、
  平衡分录在同一事务内提交。
* 读操作使用自动提交的快照读，不加锁。
"""
from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id           INTEGER PRIMARY KEY,
    warehouse    TEXT NOT NULL,
    part_no      TEXT NOT NULL,
    batch_no     TEXT NOT NULL,
    available    INTEGER NOT NULL CHECK (available >= 0),
    in_transit   INTEGER NOT NULL CHECK (in_transit >= 0),
    opening      INTEGER NOT NULL DEFAULT 0 CHECK (opening >= 0),
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(warehouse, part_no, batch_no)
);

CREATE TABLE IF NOT EXISTS transfer_requests (
    id               INTEGER PRIMARY KEY,
    part_no          TEXT NOT NULL,
    quantity         INTEGER NOT NULL CHECK (quantity > 0),
    from_warehouse   TEXT NOT NULL,
    to_warehouse     TEXT NOT NULL,
    status           TEXT NOT NULL,
    shipped_qty      INTEGER NOT NULL DEFAULT 0 CHECK (shipped_qty >= 0),
    received_qty     INTEGER NOT NULL DEFAULT 0 CHECK (received_qty >= 0),
    timeout_at       TEXT,
    note             TEXT NOT NULL DEFAULT '',
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    CHECK (to_warehouse <> from_warehouse),
    CHECK (shipped_qty <= quantity),
    CHECK (received_qty <= shipped_qty)
);

CREATE TABLE IF NOT EXISTS shipments (
    id                     INTEGER PRIMARY KEY,
    request_id             INTEGER NOT NULL REFERENCES transfer_requests(id),
    part_no                TEXT NOT NULL,
    source_warehouse       TEXT NOT NULL,
    destination_warehouse  TEXT NOT NULL,
    original_destination   TEXT,
    status                 TEXT NOT NULL,
    quantity               INTEGER NOT NULL DEFAULT 0 CHECK (quantity >= 0),
    received_qty           INTEGER NOT NULL DEFAULT 0 CHECK (received_qty >= 0),
    rejected_qty           INTEGER NOT NULL DEFAULT 0 CHECK (rejected_qty >= 0),
    created_at             TEXT NOT NULL DEFAULT (datetime('now')),
    shipped_at             TEXT,
    deadline_at            TEXT,
    received_at            TEXT,
    CHECK (received_qty + rejected_qty <= quantity)
);

CREATE TABLE IF NOT EXISTS shipment_lines (
    id               INTEGER PRIMARY KEY,
    shipment_id      INTEGER NOT NULL REFERENCES shipments(id),
    source_batch_id  INTEGER NOT NULL REFERENCES batches(id),
    anchor_batch_id  INTEGER REFERENCES batches(id),
    quantity         INTEGER NOT NULL CHECK (quantity > 0),
    received_qty     INTEGER NOT NULL DEFAULT 0 CHECK (received_qty >= 0),
    rejected_qty     INTEGER NOT NULL DEFAULT 0 CHECK (rejected_qty >= 0),
    CHECK (received_qty + rejected_qty <= quantity)
);

-- 计划中的装运明细（已计划、尚未确认）；确认成功后删除并落入 shipment_lines。
CREATE TABLE IF NOT EXISTS planned_shipment_lines (
    id               INTEGER PRIMARY KEY,
    shipment_id      INTEGER NOT NULL REFERENCES shipments(id),
    source_batch_id  INTEGER NOT NULL REFERENCES batches(id),
    quantity         INTEGER NOT NULL CHECK (quantity > 0)
);

-- 每次签收/拒收落地一行：某来源批次的多少数量进入/未进入哪个入库批次。
-- 全链路追踪（来源批次 -> 装运 -> 入库批次）依赖这张表。
CREATE TABLE IF NOT EXISTS receipt_lines (
    id               INTEGER PRIMARY KEY,
    shipment_id      INTEGER NOT NULL REFERENCES shipments(id),
    request_id       INTEGER NOT NULL REFERENCES transfer_requests(id),
    source_batch_id  INTEGER NOT NULL REFERENCES batches(id),
    dest_batch_id    INTEGER REFERENCES batches(id),
    quantity         INTEGER NOT NULL CHECK (quantity > 0),
    rejected         INTEGER NOT NULL DEFAULT 0,
    reason           TEXT NOT NULL DEFAULT '',
    created_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 复式分录：每张凭证内 SUM(debit)==SUM(credit)，库存永不凭空消失。
CREATE TABLE IF NOT EXISTS ledger_entries (
    id           INTEGER PRIMARY KEY,
    entry_type   TEXT NOT NULL,
    ref_table    TEXT NOT NULL,
    ref_id       INTEGER NOT NULL,
    request_id   INTEGER,
    note         TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS ledger_lines (
    id           INTEGER PRIMARY KEY,
    entry_id     INTEGER NOT NULL REFERENCES ledger_entries(id),
    batch_id     INTEGER NOT NULL REFERENCES batches(id),
    bucket       TEXT NOT NULL CHECK (bucket IN ('available', 'in_transit')),
    debit        INTEGER NOT NULL DEFAULT 0 CHECK (debit >= 0),
    credit       INTEGER NOT NULL DEFAULT 0 CHECK (credit >= 0),
    CHECK ((debit = 0) OR (credit = 0))
);

CREATE INDEX IF NOT EXISTS idx_batches_part       ON batches(part_no);
CREATE INDEX IF NOT EXISTS idx_shipments_request  ON shipments(request_id);
CREATE INDEX IF NOT EXISTS idx_lines_shipment     ON shipment_lines(shipment_id);
CREATE INDEX IF NOT EXISTS idx_receipts_shipment  ON receipt_lines(shipment_id);
CREATE INDEX IF NOT EXISTS idx_receipts_source    ON receipt_lines(source_batch_id);
CREATE INDEX IF NOT EXISTS idx_ledger_entries_ref ON ledger_entries(ref_table, ref_id);
"""


class Store:
    """封装 SQLite 连接与事务边界。

    并发策略
    --------
    SQLite 同一时刻只允许一个写事务，因此这里使用**单一连接 + 可重入锁**
    把写事务串行化（内存库与文件库行为一致）：

    * 所有写方法经 :meth:`write_tx` 持锁并执行 ``BEGIN IMMEDIATE``；
      后到线程在锁上等待，进入临界区后读到的必是已提交的最新余量。
    * 库存扣减使用事务内条件更新
      ``UPDATE ... SET available = available - ? WHERE available >= ?``，
      余量不足即失败回滚——并发申请的失败方拿到
      :class:`InsufficientStockError`，不会超卖、不会重复发运。
    """

    def __init__(self, path: str | Path = ":memory:"):
        self._path = str(path)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            self._path,
            isolation_level=None,  # 手工管理事务
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        if self._path != ":memory:":
            self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=10000")
        with self.write_tx() as conn:
            conn.executescript(SCHEMA)

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    @contextmanager
    def write_tx(self) -> Iterator[sqlite3.Connection]:
        """串行写事务：持锁 + ``BEGIN IMMEDIATE``，提交或回滚一体。

        锁保证跨线程串行；``BEGIN IMMEDIATE`` 在文件模式下立即拿写锁，
        配合事务内条件更新实现无丢失更新的原子扣减。
        """
        conn = self._conn
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except Exception:
                conn.rollback()
                raise
            else:
                conn.commit()

    @contextmanager
    def read_tx(self) -> Iterator[sqlite3.Connection]:
        """一致读。写事务串行使得单条 SELECT 始终读到已提交快照。"""
        with self._lock:
            yield self._conn

    def close(self) -> None:
        with self._lock:
            self._conn.close()
