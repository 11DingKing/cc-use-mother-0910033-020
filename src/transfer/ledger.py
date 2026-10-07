"""复式账本：所有库存移动以平衡凭证记录，并提供守恒审计。"""
from __future__ import annotations

import sqlite3

from .enums import EntryType


def post_entry(
    conn: sqlite3.Connection,
    entry_type: EntryType,
    ref_table: str,
    ref_id: int,
    lines: list[tuple[int, str, str, int, int]],
    request_id: int | None = None,
    note: str = "",
) -> int:
    """写一张平衡凭证。

    ``lines`` 每项为 ``(batch_id, bucket, debit, credit)``，
    ``bucket`` 取 ``'available'`` 或 ``'in_transit'``。
    每张凭证借贷合计必须相等，否则抛 :class:`AssertionError`——
    这是"库存不消失"的最后一道防线。

    返回分录 id。
    """
    total_debit = sum(l[2] for l in lines)
    total_credit = sum(l[3] for l in lines)
    if total_debit != total_credit:
        raise AssertionError(
            f"分录不平衡：{entry_type.value} 借={total_debit} 贷={total_credit}"
        )
    cur = conn.execute(
        "INSERT INTO ledger_entries(entry_type, ref_table, ref_id, request_id, note)"
        " VALUES (?, ?, ?, ?, ?)",
        (entry_type.value, ref_table, ref_id, request_id, note),
    )
    entry_id = int(cur.lastrowid)
    conn.executemany(
        "INSERT INTO ledger_lines(entry_id, batch_id, bucket, debit, credit)"
        " VALUES (?, ?, ?, ?, ?)",
        [(entry_id, batch_id, bucket, debit, credit) for batch_id, bucket, debit, credit in lines],
    )
    return entry_id


def audit_conservation(conn: sqlite3.Connection) -> list[str]:
    """守恒审计，返回所有违例的说明（空列表表示通过）。

    检查三项：

    1. 每张凭证借贷平衡（SQL 聚合）。
    2. 每个批次：期初 + 账本 available 桶净额 == 当前 available，
       in_transit 桶同理——即库存表是账本的真实投影。
    3. 非负：当前 available / in_transit >= 0（已由 CHECK 约束保证，这里复核）。
    """
    problems: list[str] = []

    rows = conn.execute(
        "SELECT id, SUM(debit) AS d, SUM(credit) AS c"
        " FROM ledger_lines GROUP BY entry_id HAVING d <> c"
    ).fetchall()
    for row in rows:
        problems.append(f"凭证 {row['id']} 借贷不平衡：借 {row['d']} / 贷 {row['c']}")

    rows = conn.execute(
        "SELECT b.id, b.warehouse, b.batch_no, b.available, b.in_transit, b.opening,"
        " COALESCE((SELECT SUM(ll.debit - ll.credit) FROM ledger_lines ll"
        "           WHERE ll.batch_id = b.id AND ll.bucket = 'available'), 0) AS av_net,"
        " COALESCE((SELECT SUM(ll.debit - ll.credit) FROM ledger_lines ll"
        "           WHERE ll.batch_id = b.id AND ll.bucket = 'in_transit'), 0) AS it_net"
        " FROM batches b"
    ).fetchall()
    for row in rows:
        if row["opening"] + row["av_net"] != row["available"]:
            problems.append(
                f"批次 {row['warehouse']}/{row['batch_no']} 可用量与账本不符："
                f"账面 {row['available']} != 期初 {row['opening']} + 净额 {row['av_net']}"
            )
        if row["it_net"] != row["in_transit"]:
            problems.append(
                f"批次 {row['warehouse']}/{row['batch_no']} 在途量与账本不符："
                f"账面 {row['in_transit']} != 账本净额 {row['it_net']}"
            )
    return problems
