"""调拨领域服务：状态机编排、原子库存移动与补偿分录。

所有写方法都在单个 ``BEGIN IMMEDIATE`` 事务内完成"读状态 → 校验 →
条件更新库存 → 写在途/签收行 → 写平衡分录 → 推进状态机"，
事务提交前任一步失败则整体回滚，绝不会出现"库存已扣、在途未建"。
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict
from datetime import datetime

from . import ledger
from .enums import (
    LIVE_SHIPMENT_STATUSES,
    SHIPMENT_TRANSITIONS,
    EntryType,
    RequestStatus,
    ShipmentStatus,
    assert_request_transition,
    assert_shipment_transition,
)
from .errors import (
    BusinessRuleError,
    DuplicateShipmentError,
    InsufficientStockError,
    InvalidTransitionError,
    NotFoundError,
    OverReceiptError,
    ValidationError,
)
from .models import (
    Batch,
    LedgerEntryView,
    LedgerLineView,
    QuantityTrace,
    ReceiptLineView,
    ShipmentLineView,
    ShipmentView,
    TransferRequestView,
)
from .store import Store

# 在途锚点批次的批次号约定：每仓每零件一行
_IN_TRANSIT_BATCH_NO = "__IN_TRANSIT__"
# 未指定入库批次号时的默认落地批次
_DEFAULT_RCV_PREFIX = "RCV"


class TransferService:
    def __init__(self, store: Store | str = ":memory:"):
        self.store = store if isinstance(store, Store) else Store(store)

    # ------------------------------------------------------------------ #
    # 基础工具
    # ------------------------------------------------------------------ #
    @staticmethod
    def _now(conn: sqlite3.Connection) -> str:
        return conn.execute("SELECT datetime('now')").fetchone()[0]

    @staticmethod
    def _parse_deadline(value: str | datetime | None) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        text = value.strip().replace("T", " ")
        if text.endswith("Z"):
            text = text[:-1].strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
        raise ValidationError(f"无法解析截止时间：{value!r}")

    def _request_row(self, conn: sqlite3.Connection, request_id: int) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM transfer_requests WHERE id = ?", (request_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"申请单 {request_id} 不存在")
        return row

    def _shipment_row(self, conn: sqlite3.Connection, shipment_id: int) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM shipments WHERE id = ?", (shipment_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"装运单 {shipment_id} 不存在")
        return row

    def _batch_row(
        self, conn: sqlite3.Connection, warehouse: str, part_no: str, batch_no: str
    ) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM batches WHERE warehouse = ? AND part_no = ? AND batch_no = ?",
            (warehouse, part_no, batch_no),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"批次不存在：{warehouse}/{part_no}/{batch_no}")
        return row

    def _batch_by_id(self, conn: sqlite3.Connection, batch_id: int) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"批次 {batch_id} 不存在")
        return row

    def _get_or_create_batch(
        self, conn: sqlite3.Connection, warehouse: str, part_no: str, batch_no: str
    ) -> int:
        row = conn.execute(
            "SELECT id FROM batches WHERE warehouse = ? AND part_no = ? AND batch_no = ?",
            (warehouse, part_no, batch_no),
        ).fetchone()
        if row is not None:
            return int(row["id"])
        cur = conn.execute(
            "INSERT INTO batches(warehouse, part_no, batch_no, available, in_transit, opening)"
            " VALUES (?, ?, ?, 0, 0, 0)",
            (warehouse, part_no, batch_no),
        )
        return int(cur.lastrowid)

    def _in_transit_batch(self, conn: sqlite3.Connection, warehouse: str, part_no: str) -> int:
        """在途锚点批次：每仓每零件一行，in_transit 桶承载所有途经该仓的在途量。"""
        return self._get_or_create_batch(conn, warehouse, part_no, _IN_TRANSIT_BATCH_NO)

    # ------------------------------------------------------------------ #
    # 批次 / 库存
    # ------------------------------------------------------------------ #
    def create_batch(
        self, warehouse: str, part_no: str, batch_no: str, opening_quantity: int = 0
    ) -> int:
        """登记仓库批次并录入期初库存（外部来源，计入 opening 基线）。"""
        if not warehouse or not part_no or not batch_no:
            raise ValidationError("仓库、零件号、批次号均不能为空")
        if opening_quantity < 0:
            raise ValidationError("期初库存不能为负")
        with self.store.write_tx() as conn:
            row = conn.execute(
                "SELECT id FROM batches WHERE warehouse=? AND part_no=? AND batch_no=?",
                (warehouse, part_no, batch_no),
            ).fetchone()
            if row is not None:
                raise BusinessRuleError(f"批次已存在：{warehouse}/{part_no}/{batch_no}")
            cur = conn.execute(
                "INSERT INTO batches(warehouse, part_no, batch_no, available, in_transit, opening)"
                " VALUES (?, ?, ?, ?, 0, ?)",
                (warehouse, part_no, batch_no, opening_quantity, opening_quantity),
            )
            return int(cur.lastrowid)

    def restock(self, warehouse: str, part_no: str, batch_no: str, quantity: int) -> None:
        """外部补货入库（累计入期初基线，不参与调拨守恒环）。"""
        if quantity <= 0:
            raise ValidationError("补货数量必须为正")
        with self.store.write_tx() as conn:
            batch = self._batch_row(conn, warehouse, part_no, batch_no)
            conn.execute(
                "UPDATE batches SET available = available + ?, opening = opening + ? WHERE id = ?",
                (quantity, quantity, batch["id"]),
            )

    def list_batches(
        self, warehouse: str | None = None, part_no: str | None = None
    ) -> list[Batch]:
        sql = "SELECT * FROM batches WHERE 1=1"
        args: list[object] = []
        if warehouse:
            sql += " AND warehouse = ?"
            args.append(warehouse)
        if part_no:
            sql += " AND part_no = ?"
            args.append(part_no)
        sql += " ORDER BY warehouse, part_no, batch_no"
        with self.store.read_tx() as conn:
            rows = conn.execute(sql, args).fetchall()
        return [self._batch_view(r) for r in rows]

    @staticmethod
    def _batch_view(row: sqlite3.Row) -> Batch:
        return Batch(
            id=row["id"],
            warehouse=row["warehouse"],
            part_no=row["part_no"],
            batch_no=row["batch_no"],
            available=row["available"],
            in_transit=row["in_transit"],
        )

    # ------------------------------------------------------------------ #
    # 调拨申请：草拟 → 提交 → 审批
    # ------------------------------------------------------------------ #
    def create_request(
        self,
        part_no: str,
        quantity: int,
        from_warehouse: str,
        to_warehouse: str,
        note: str = "",
    ) -> int:
        if not part_no:
            raise ValidationError("零件号不能为空")
        if not isinstance(quantity, int) or quantity <= 0:
            raise ValidationError("申请数量必须为正整数")
        if not from_warehouse or not to_warehouse:
            raise ValidationError("来源仓与目的仓均不能为空")
        if from_warehouse == to_warehouse:
            raise ValidationError("来源仓与目的仓不能相同")
        with self.store.write_tx() as conn:
            cur = conn.execute(
                "INSERT INTO transfer_requests(part_no, quantity, from_warehouse, to_warehouse,"
                " status, note) VALUES (?, ?, ?, ?, ?, ?)",
                (part_no, quantity, from_warehouse, to_warehouse, RequestStatus.DRAFT.value, note),
            )
            return int(cur.lastrowid)

    def revise_request(
        self,
        request_id: int,
        quantity: int | None = None,
        to_warehouse: str | None = None,
        note: str | None = None,
    ) -> None:
        """被驳回的申请修改后可重新提交。"""
        with self.store.write_tx() as conn:
            row = self._request_row(conn, request_id)
            if RequestStatus(row["status"]) is not RequestStatus.REJECTED:
                raise InvalidTransitionError("仅已驳回的申请可以修改")
            if quantity is not None:
                if quantity <= 0:
                    raise ValidationError("申请数量必须为正整数")
                conn.execute(
                    "UPDATE transfer_requests SET quantity = ? WHERE id = ?",
                    (quantity, request_id),
                )
            if to_warehouse is not None:
                if to_warehouse == row["from_warehouse"]:
                    raise ValidationError("来源仓与目的仓不能相同")
                conn.execute(
                    "UPDATE transfer_requests SET to_warehouse = ? WHERE id = ?",
                    (to_warehouse, request_id),
                )
            if note is not None:
                conn.execute(
                    "UPDATE transfer_requests SET note = ? WHERE id = ?", (note, request_id)
                )

    def _set_request_status(
        self, conn: sqlite3.Connection, row: sqlite3.Row, target: RequestStatus
    ) -> None:
        current = RequestStatus(row["status"])
        assert_request_transition(current, target)
        conn.execute(
            "UPDATE transfer_requests SET status = ? WHERE id = ?", (target.value, row["id"])
        )

    def submit_request(self, request_id: int) -> None:
        with self.store.write_tx() as conn:
            row = self._request_row(conn, request_id)
            self._set_request_status(conn, row, RequestStatus.PENDING)

    def approve_request(self, request_id: int, note: str = "") -> None:
        with self.store.write_tx() as conn:
            row = self._request_row(conn, request_id)
            self._set_request_status(conn, row, RequestStatus.APPROVED)
            ledger.post_entry(
                conn, EntryType.APPROVAL, "transfer_requests", request_id, [],
                request_id=request_id, note=note or "审批通过",
            )

    def reject_request(self, request_id: int, reason: str = "") -> None:
        with self.store.write_tx() as conn:
            row = self._request_row(conn, request_id)
            self._set_request_status(conn, row, RequestStatus.REJECTED)
            ledger.post_entry(
                conn, EntryType.APPROVAL, "transfer_requests", request_id, [],
                request_id=request_id, note=f"审批驳回：{reason}",
            )

    def cancel_request(self, request_id: int, reason: str = "") -> list[int]:
        """取消申请。

        * 草拟 / 待审批 / 已批准且未发运：直接终结。
        * 已有在途货物：申请置为已取消，**计划中**装运单立即取消，
          **在途**装运单由超时巡检（或手动 timeout）把在途量退回来源仓
          （补偿分录），退回时识别到申请已取消即不再重新打开申请。
        返回被联动取消的装运单 id 列表。
        """
        cancelled_shipments: list[int] = []
        with self.store.write_tx() as conn:
            row = self._request_row(conn, request_id)
            status = RequestStatus(row["status"])
            if status in (RequestStatus.CANCELLED, RequestStatus.CLOSED, RequestStatus.RECEIVED):
                raise InvalidTransitionError(f"申请处于 {status.value}，不可取消")
            # 先取消所有已计划、尚未确认的装运单（无库存影响）
            planned = conn.execute(
                "SELECT id FROM shipments WHERE request_id = ? AND status = ?",
                (request_id, ShipmentStatus.PLANNED.value),
            ).fetchall()
            for p in planned:
                sid = int(p["id"])
                conn.execute(
                    "UPDATE shipments SET status = ? WHERE id = ?",
                    (ShipmentStatus.CANCELLED.value, sid),
                )
                conn.execute("DELETE FROM planned_shipment_lines WHERE shipment_id = ?", (sid,))
                ledger.post_entry(
                    conn, EntryType.PLAN_CANCEL, "shipments", sid, [],
                    request_id=request_id, note="申请取消，联动取消计划装运",
                )
                cancelled_shipments.append(sid)
            self._set_request_status(conn, row, RequestStatus.CANCELLED)
            ledger.post_entry(
                conn, EntryType.REQUEST_CANCEL, "transfer_requests", request_id, [],
                request_id=request_id, note=reason,
            )
        return cancelled_shipments

    def close_request(self, request_id: int, note: str = "") -> None:
        """提前关闭：剩余未发运数量不再发运（要求无在途货物）。"""
        with self.store.write_tx() as conn:
            row = self._request_row(conn, request_id)
            live = conn.execute(
                "SELECT COUNT(*) AS n FROM shipments WHERE request_id = ? AND status IN (?, ?)",
                (
                    request_id,
                    ShipmentStatus.IN_TRANSIT.value,
                    ShipmentStatus.PARTIALLY_RECEIVED.value,
                ),
            ).fetchone()["n"]
            if live:
                raise BusinessRuleError("仍有在途装运单，不能关闭；请先签收/拒收或等待超时")
            self._set_request_status(conn, row, RequestStatus.CLOSED)
            ledger.post_entry(
                conn, EntryType.REQUEST_CANCEL, "transfer_requests", request_id, [],
                request_id=request_id, note=f"关闭申请：{note}",
            )

    # ------------------------------------------------------------------ #
    # 装运：计划（不占库存）→ 确认（原子 available→在途）
    # ------------------------------------------------------------------ #
    def create_shipment(
        self,
        request_id: int,
        lines: list[tuple[str, int] | dict[str, object]],
        deadline: str | datetime | None = None,
    ) -> int:
        """创建已计划装运单。

        ``lines`` 为 ``(来源批次号, 数量)`` 列表（批次须属于申请的来源仓与
        零件）。计划阶段**不**扣减库存；库存的权威扣减发生在
        :meth:`confirm_shipment` 的同一事务内。
        """
        norm_lines = self._norm_lines(lines)
        if not norm_lines:
            raise ValidationError("装运明细不能为空")
        deadline_at = self._parse_deadline(deadline)
        with self.store.write_tx() as conn:
            req = self._request_row(conn, request_id)
            status = RequestStatus(req["status"])
            if status not in (
                RequestStatus.APPROVED,
                RequestStatus.PARTIALLY_SHIPPED,
                RequestStatus.TIMEOUT,
            ):
                raise InvalidTransitionError(f"申请状态为 {status.value}，不能安排装运")
            total = sum(q for _, q in norm_lines)
            open_qty = req["quantity"] - req["shipped_qty"]
            if total > open_qty:
                raise ValidationError(
                    f"装运量 {total} 超过申请未发运余量 {open_qty}"
                )
            agg: dict[int, int] = defaultdict(int)
            for batch_no, qty in norm_lines:
                batch = self._batch_row(
                    conn, req["from_warehouse"], req["part_no"], batch_no
                )
                if batch["batch_no"] == _IN_TRANSIT_BATCH_NO:
                    raise ValidationError("不能从在途锚点批次出库")
                agg[batch["id"]] += qty
            cur = conn.execute(
                "INSERT INTO shipments(request_id, part_no, source_warehouse,"
                " destination_warehouse, original_destination, status, deadline_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    request_id,
                    req["part_no"],
                    req["from_warehouse"],
                    req["to_warehouse"],
                    None,
                    ShipmentStatus.PLANNED.value,
                    deadline_at,
                ),
            )
            shipment_id = int(cur.lastrowid)
            conn.executemany(
                "INSERT INTO planned_shipment_lines(shipment_id, source_batch_id, quantity)"
                " VALUES (?, ?, ?)",
                [(shipment_id, bid, qty) for bid, qty in agg.items()],
            )
            return shipment_id

    @staticmethod
    def _norm_lines(lines: list[tuple[str, int] | dict[str, object]]) -> list[tuple[str, int]]:
        result: list[tuple[str, int]] = []
        for item in lines:
            if isinstance(item, dict):
                batch_no = str(item["source_batch_no"])
                qty = item["quantity"]
            else:
                batch_no, qty = item[0], item[1]
            if not isinstance(qty, int) or qty <= 0:
                raise ValidationError("装运数量必须为正整数")
            result.append((batch_no, qty))
        return result

    @staticmethod
    def _norm_items(items: list[tuple[str, int] | dict[str, object]]) -> list[tuple[str, int]]:
        return TransferService._norm_lines(items)

    def confirm_shipment(self, shipment_id: int) -> None:
        """确认出库：原子地把来源批次可用量转为在途，并记录来源批次。

        并发安全核心：扣减使用条件更新
        ``UPDATE batches SET available = available - ? WHERE id=? AND available >= ?``，
        在 ``BEGIN IMMEDIATE`` 事务内执行；任一来源批次余量不足则整体回滚，
        失败方重试时读到的是已提交后的最新余量。
        """
        with self.store.write_tx() as conn:
            ship = self._shipment_row(conn, shipment_id)
            current = ShipmentStatus(ship["status"])
            if current is ShipmentStatus.IN_TRANSIT:
                # 重复确认（失败重试场景）必须幂等拒绝，不能再次发运
                raise DuplicateShipmentError("装运单已确认，请勿重复操作")
            assert_shipment_transition(current, ShipmentStatus.IN_TRANSIT)
            req = self._request_row(conn, ship["request_id"])
            req_status = RequestStatus(req["status"])
            if req_status in (
                RequestStatus.CANCELLED,
                RequestStatus.CLOSED,
                RequestStatus.REJECTED,
            ):
                raise BusinessRuleError(f"申请处于 {req_status.value}，不能确认装运")

            planned = conn.execute(
                "SELECT pl.source_batch_id AS bid, SUM(pl.quantity) AS qty,"
                " b.warehouse AS wh, b.part_no AS part, b.batch_no AS bno,"
                " b.available AS avail"
                " FROM planned_shipment_lines pl JOIN batches b ON b.id = pl.source_batch_id"
                " WHERE pl.shipment_id = ? GROUP BY pl.source_batch_id",
                (shipment_id,),
            ).fetchall()
            total = sum(int(p["qty"]) for p in planned)
            open_qty = req["quantity"] - req["shipped_qty"]
            if total <= 0:
                raise ValidationError("装运单没有可确认的明细")
            if total > open_qty:
                raise BusinessRuleError(
                    f"装运量 {total} 超过申请未发运余量 {open_qty}（可能已被其他装运占用）"
                )

            # 1) 原子扣减各来源批次可用量（条件更新，行锁裁决并发）
            source_totals: dict[int, int] = {}
            for p in planned:
                bid, qty = int(p["bid"]), int(p["qty"])
                cur = conn.execute(
                    "UPDATE batches SET available = available - ?"
                    " WHERE id = ? AND available >= ?",
                    (qty, bid, qty),
                )
                if cur.rowcount == 0:
                    fresh = self._batch_by_id(conn, bid)
                    raise InsufficientStockError(
                        p["wh"], p["part"], p["bno"], qty, fresh["available"]
                    )
                source_totals[bid] = source_totals.get(bid, 0) + qty

            # 2) 在途锚定到目的地仓（同事务内立即可见）
            anchor_id = self._in_transit_batch(
                conn, ship["destination_warehouse"], ship["part_no"]
            )
            conn.execute(
                "UPDATE batches SET in_transit = in_transit + ? WHERE id = ?",
                (total, anchor_id),
            )

            # 3) 落来源批次明细（每件数量的来源在此固化）
            conn.executemany(
                "INSERT INTO shipment_lines(shipment_id, source_batch_id, anchor_batch_id,"
                " quantity) VALUES (?, ?, ?, ?)",
                [(shipment_id, bid, anchor_id, qty) for bid, qty in source_totals.items()],
            )
            conn.execute("DELETE FROM planned_shipment_lines WHERE shipment_id = ?", (shipment_id,))

            # 4) 平衡分录：CR 来源批次 available / DR 目的地在途
            ledger_lines = [(anchor_id, "in_transit", total, 0)]
            ledger_lines += [(bid, "available", 0, qty) for bid, qty in source_totals.items()]
            ledger.post_entry(
                conn, EntryType.SHIP_OUT, "shipments", shipment_id, ledger_lines,
                request_id=req["id"], note="确认出库，可用转在途",
            )

            # 5) 推进装运单与申请单状态机
            now = self._now(conn)
            conn.execute(
                "UPDATE shipments SET status = ?, quantity = ?, shipped_at = ? WHERE id = ?",
                (ShipmentStatus.IN_TRANSIT.value, total, now, shipment_id),
            )
            new_shipped = req["shipped_qty"] + total
            target = (
                RequestStatus.SHIPPED
                if new_shipped >= req["quantity"]
                else RequestStatus.PARTIALLY_SHIPPED
            )
            self._set_request_status_from(conn, req, target, shipped_qty=new_shipped)

    def _set_request_status_from(
        self,
        conn: sqlite3.Connection,
        req: sqlite3.Row,
        target: RequestStatus,
        shipped_qty: int | None = None,
        received_qty: int | None = None,
    ) -> None:
        current = RequestStatus(req["status"])
        assert_request_transition(current, target)
        sets = ["status = ?"]
        args: list[object] = [target.value]
        if shipped_qty is not None:
            sets.append("shipped_qty = ?")
            args.append(shipped_qty)
        if received_qty is not None:
            sets.append("received_qty = ?")
            args.append(received_qty)
        args.append(req["id"])
        conn.execute(f"UPDATE transfer_requests SET {', '.join(sets)} WHERE id = ?", args)

    def cancel_planned_shipment(self, shipment_id: int) -> None:
        """取消尚未确认的装运单（无库存影响，仅审计）。"""
        with self.store.write_tx() as conn:
            ship = self._shipment_row(conn, shipment_id)
            assert_shipment_transition(ShipmentStatus(ship["status"]), ShipmentStatus.CANCELLED)
            conn.execute(
                "UPDATE shipments SET status = ? WHERE id = ?",
                (ShipmentStatus.CANCELLED.value, shipment_id),
            )
            conn.execute("DELETE FROM planned_shipment_lines WHERE shipment_id = ?", (shipment_id,))
            ledger.post_entry(
                conn, EntryType.PLAN_CANCEL, "shipments", shipment_id, [],
                request_id=ship["request_id"], note="取消计划装运",
            )

    # ------------------------------------------------------------------ #
    # 签收 / 拒收：在途 → 入库批次 / 退回来源批次
    # ------------------------------------------------------------------ #
    def _allocate_items(
        self, conn: sqlite3.Connection, shipment_id: int, items: list[tuple[str, int]]
    ) -> list[tuple[sqlite3.Row, int]]:
        """把 (来源批次号, 数量) 逐项映射到该装运单的来源批次行并校验余量。"""
        rows = conn.execute(
            "SELECT sl.*, b.batch_no AS source_batch_no FROM shipment_lines sl"
            " JOIN batches b ON b.id = sl.source_batch_id"
            " WHERE sl.shipment_id = ?",
            (shipment_id,),
        ).fetchall()
        by_batch = {r["source_batch_no"]: r for r in rows}
        alloc: list[tuple[sqlite3.Row, int]] = []
        agg: dict[int, int] = defaultdict(int)
        for batch_no, qty in items:
            line = by_batch.get(batch_no)
            if line is None:
                raise ValidationError(f"本装运单不含来源批次 {batch_no}")
            agg[line["id"]] += qty
        for line in rows:
            qty = agg.get(line["id"], 0)
            if qty == 0:
                continue
            remaining = line["quantity"] - line["received_qty"] - line["rejected_qty"]
            if qty > remaining:
                raise OverReceiptError(
                    f"批次 {line['source_batch_no']} 在途余量 {remaining}，"
                    f"不能处理 {qty}"
                )
            alloc.append((line, qty))
        if not alloc:
            raise ValidationError("处理数量必须为正")
        return alloc

    @staticmethod
    def _debit_in_transit(conn: sqlite3.Connection, anchor_id: int, qty: int) -> None:
        cur = conn.execute(
            "UPDATE batches SET in_transit = in_transit - ?"
            " WHERE id = ? AND in_transit >= ?",
            (qty, anchor_id, qty),
        )
        if cur.rowcount == 0:
            # 理论不可达：装运行与锚点同事务维护。达到即说明账本被破坏。
            raise AssertionError(f"在途锚点批次 {anchor_id} 余量不足 {qty}，账本异常")

    def receive_shipment(
        self,
        shipment_id: int,
        items: list[tuple[str, int] | dict[str, object]],
        dest_batch_no: str | None = None,
        note: str = "",
    ) -> int:
        """（部分）签收：在途数量原子转入目的仓可用库存。

        ``items`` 为 ``(来源批次号, 数量)``，可只签收一部分，可多次调用。
        返回本次签收总量。每批来源数量落到哪个入库批次记录在 ``receipt_lines``。
        """
        norm = self._norm_items(items)
        with self.store.write_tx() as conn:
            ship = self._shipment_row(conn, shipment_id)
            status = ShipmentStatus(ship["status"])
            # 终态可能是"部分签收"或"全部签收"，先确认当前状态允许签收方向
            if (
                ShipmentStatus.PARTIALLY_RECEIVED
                not in SHIPMENT_TRANSITIONS[status]
            ):
                assert_shipment_transition(status, ShipmentStatus.RECEIVED)
            alloc = self._allocate_items(conn, shipment_id, norm)
            total = sum(q for _, q in alloc)
            req = self._request_row(conn, ship["request_id"])
            if RequestStatus(req["status"]) is RequestStatus.CANCELLED:
                raise BusinessRuleError("申请已取消，在途货物须走拒收/超时退回")
            if req["received_qty"] + total > req["shipped_qty"]:
                raise OverReceiptError("签收总量不能超过已发运量")

            dest_no = dest_batch_no or f"{_DEFAULT_RCV_PREFIX}-{ship['part_no']}"
            dest_batch_id = self._get_or_create_batch(
                conn, ship["destination_warehouse"], ship["part_no"], dest_no
            )

            ledger_lines: list[tuple[int, str, int, int]] = []
            anchor_groups: dict[int, int] = defaultdict(int)
            for line, qty in alloc:
                anchor_id = int(line["anchor_batch_id"])
                anchor_groups[anchor_id] += qty
                # 在途出账
                self._debit_in_transit(conn, anchor_id, qty)
                # 明细与来源追踪
                conn.execute(
                    "UPDATE shipment_lines SET received_qty = received_qty + ? WHERE id = ?",
                    (qty, line["id"]),
                )
                conn.execute(
                    "INSERT INTO receipt_lines(shipment_id, request_id, source_batch_id,"
                    " dest_batch_id, quantity, rejected, reason) VALUES (?, ?, ?, ?, ?, 0, ?)",
                    (shipment_id, req["id"], line["source_batch_id"], dest_batch_id, qty, note),
                )
            # 入库批次可用量一次性增加，按来源批次的追踪见 receipt_lines
            conn.execute(
                "UPDATE batches SET available = available + ? WHERE id = ?",
                (total, dest_batch_id),
            )
            for anchor_id, qty in anchor_groups.items():
                ledger_lines.append((anchor_id, "in_transit", 0, qty))
            ledger_lines.append((dest_batch_id, "available", total, 0))
            ledger.post_entry(
                conn, EntryType.RECEIVE, "shipments", shipment_id, ledger_lines,
                request_id=req["id"], note=note or f"签收入库至批次 {dest_no}",
            )

            self._finish_goods_move(
                conn, ship, req, alloc, total, is_rejection=False
            )
            return total

    def reject_shipment(
        self,
        shipment_id: int,
        items: list[tuple[str, int] | dict[str, object]],
        reason: str = "",
    ) -> int:
        """（部分）拒收：在途数量原子退回**来源批次可用量**，写补偿分录。

        可在完全在途或部分签收后对剩余数量拒收。返回本次拒收总量。
        """
        norm = self._norm_items(items)
        with self.store.write_tx() as conn:
            ship = self._shipment_row(conn, shipment_id)
            status = ShipmentStatus(ship["status"])
            if ShipmentStatus.REJECTED not in SHIPMENT_TRANSITIONS[status]:
                assert_shipment_transition(status, ShipmentStatus.REJECTED)
            alloc = self._allocate_items(conn, shipment_id, norm)
            total = sum(q for _, q in alloc)
            req = self._request_row(conn, ship["request_id"])

            ledger_lines: list[tuple[int, str, int, int]] = []
            anchor_groups: dict[int, int] = defaultdict(int)
            for line, qty in alloc:
                anchor_id = int(line["anchor_batch_id"])
                anchor_groups[anchor_id] += qty
                self._debit_in_transit(conn, anchor_id, qty)
                # 退回来源批次可用量
                conn.execute(
                    "UPDATE batches SET available = available + ? WHERE id = ?",
                    (qty, line["source_batch_id"]),
                )
                conn.execute(
                    "UPDATE shipment_lines SET rejected_qty = rejected_qty + ? WHERE id = ?",
                    (qty, line["id"]),
                )
                conn.execute(
                    "INSERT INTO receipt_lines(shipment_id, request_id, source_batch_id,"
                    " dest_batch_id, quantity, rejected, reason)"
                    " VALUES (?, ?, ?, NULL, ?, 1, ?)",
                    (shipment_id, req["id"], line["source_batch_id"], qty, reason),
                )
                ledger_lines.append((line["source_batch_id"], "available", qty, 0))
            for anchor_id, qty in anchor_groups.items():
                ledger_lines.append((anchor_id, "in_transit", 0, qty))
            ledger.post_entry(
                conn, EntryType.REJECT_RETURN, "shipments", shipment_id, ledger_lines,
                request_id=req["id"], note=f"拒收退回：{reason}",
            )

            self._finish_goods_move(conn, ship, req, alloc, total, is_rejection=True)
            return total

    def _finish_goods_move(
        self,
        conn: sqlite3.Connection,
        ship: sqlite3.Row,
        req: sqlite3.Row,
        alloc: list[tuple[sqlite3.Row, int]],
        total: int,
        *,
        is_rejection: bool,
    ) -> None:
        """签收/拒收后的装运单与申请单状态推进。"""
        new_received = ship["received_qty"] + (0 if is_rejection else total)
        new_rejected = ship["rejected_qty"] + (total if is_rejection else 0)

        # 重新汇总该装运单全部明细，判断装运单是否终结
        agg_row = conn.execute(
            "SELECT SUM(quantity) AS q, SUM(received_qty) AS r, SUM(rejected_qty) AS x"
            " FROM shipment_lines WHERE shipment_id = ?",
            (ship["id"],),
        ).fetchone()
        all_done = agg_row["r"] + agg_row["x"] >= agg_row["q"]
        if is_rejection:
            ship_target = ShipmentStatus.REJECTED if all_done else ShipmentStatus.PARTIALLY_RECEIVED
        else:
            ship_target = ShipmentStatus.RECEIVED if all_done else ShipmentStatus.PARTIALLY_RECEIVED
        now = self._now(conn)
        conn.execute(
            "UPDATE shipments SET status = ?, received_qty = ?, rejected_qty = ?,"
            " received_at = COALESCE(received_at, ?) WHERE id = ?",
            (ship_target.value, new_received, new_rejected, now, ship["id"]),
        )

        # 申请单数量与状态
        if is_rejection:
            new_shipped = req["shipped_qty"] - total
            new_req_received = req["received_qty"]
            req_target = self._request_status_after_return(conn, req, new_shipped)
            self._set_request_status_from(
                conn, req, req_target, shipped_qty=new_shipped, received_qty=new_req_received
            )
        else:
            new_req_received = req["received_qty"] + total
            new_shipped = req["shipped_qty"]
            if new_req_received >= req["quantity"] and new_shipped >= req["quantity"]:
                self._set_request_status_from(
                    conn, req, RequestStatus.RECEIVED,
                    shipped_qty=new_shipped, received_qty=new_req_received,
                )
            else:
                # 未完成：尚有未发运余量为部分装运；已全量发运但未收齐为部分签收
                target = (
                    RequestStatus.PARTIALLY_SHIPPED
                    if new_shipped < req["quantity"]
                    else RequestStatus.PARTIALLY_RECEIVED
                )
                self._set_request_status_from(
                    conn, req, target,
                    shipped_qty=new_shipped, received_qty=new_req_received,
                )

    def _request_status_after_return(
        self, conn: sqlite3.Connection, req: sqlite3.Row, new_shipped: int
    ) -> RequestStatus:
        """拒收/超时退回后申请单状态：取消态保持取消，否则按在途量重算。"""
        if RequestStatus(req["status"]) is RequestStatus.CANCELLED:
            return RequestStatus.CANCELLED
        if new_shipped <= 0:
            return RequestStatus.APPROVED
        # 仍有已签收或在途货物
        return (
            RequestStatus.SHIPPED
            if new_shipped >= req["quantity"]
            else RequestStatus.PARTIALLY_SHIPPED
        )

    # ------------------------------------------------------------------ #
    # 改道：在途在目的地仓间划转（数量与来源批次不变）
    # ------------------------------------------------------------------ #
    def reroute_shipment(
        self, shipment_id: int, new_destination: str, reason: str = ""
    ) -> None:
        if not new_destination:
            raise ValidationError("新目的地不能为空")
        with self.store.write_tx() as conn:
            ship = self._shipment_row(conn, shipment_id)
            status = ShipmentStatus(ship["status"])
            assert_shipment_transition(status, ShipmentStatus.IN_TRANSIT)
            if new_destination == ship["source_warehouse"]:
                raise ValidationError("改道目的地不能是来源仓（退回请走拒收/超时）")
            if new_destination == ship["destination_warehouse"]:
                raise ValidationError("新目的地与当前目的地相同")
            req = self._request_row(conn, ship["request_id"])

            # 各锚点上的剩余在途量（正常只有一个锚点，按锚点分组更稳）
            rows = conn.execute(
                "SELECT anchor_batch_id, SUM(quantity - received_qty - rejected_qty) AS outstanding"
                " FROM shipment_lines WHERE shipment_id = ?"
                " GROUP BY anchor_batch_id HAVING outstanding > 0",
                (shipment_id,),
            ).fetchall()
            total_outstanding = sum(int(r["outstanding"]) for r in rows)
            if total_outstanding <= 0:
                raise BusinessRuleError("装运单已无在途数量，不能改道")

            new_anchor = self._in_transit_batch(conn, new_destination, ship["part_no"])
            ledger_lines: list[tuple[int, str, int, int]] = []
            for r in rows:
                old_anchor, qty = int(r["anchor_batch_id"]), int(r["outstanding"])
                self._debit_in_transit(conn, old_anchor, qty)
                conn.execute(
                    "UPDATE batches SET in_transit = in_transit + ? WHERE id = ?",
                    (qty, new_anchor),
                )
                ledger_lines.append((old_anchor, "in_transit", 0, qty))
            ledger_lines.append((new_anchor, "in_transit", total_outstanding, 0))

            # 仍有在途余量的行改挂新锚点（已终结行保留历史锚点）
            conn.execute(
                "UPDATE shipment_lines SET anchor_batch_id = ?"
                " WHERE shipment_id = ? AND quantity - received_qty - rejected_qty > 0",
                (new_anchor, shipment_id),
            )
            conn.execute(
                "UPDATE shipments SET destination_warehouse = ?,"
                " original_destination = COALESCE(original_destination, ?) WHERE id = ?",
                (new_destination, ship["destination_warehouse"], shipment_id),
            )
            ledger.post_entry(
                conn, EntryType.REROUTE, "shipments", shipment_id, ledger_lines,
                request_id=req["id"],
                note=f"{ship['destination_warehouse']} → {new_destination}：{reason}",
            )

    # ------------------------------------------------------------------ #
    # 超时：在途量退回来源仓（补偿），支持批量巡检
    # ------------------------------------------------------------------ #
    def force_timeout_shipment(self, shipment_id: int) -> bool:
        """把一张仍有在途余量的装运单判超时并退回。无在途余量时返回 False。"""
        with self.store.write_tx() as conn:
            return self._timeout_shipment(conn, shipment_id)

    def sweep_timeouts(self, now: str | datetime | None = None) -> list[int]:
        """扫描超过 deadline 仍在途的装运单，逐单补偿退回。

        每张单据独立事务，一单失败不影响其他单。
        """
        cutoff = self._parse_deadline(now)
        with self.store.read_tx() as conn:
            if cutoff is None:
                rows = conn.execute(
                    "SELECT id FROM shipments WHERE deadline_at IS NOT NULL"
                    " AND deadline_at <= datetime('now')"
                    " AND status IN (?, ?)",
                    (
                        ShipmentStatus.IN_TRANSIT.value,
                        ShipmentStatus.PARTIALLY_RECEIVED.value,
                    ),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT id FROM shipments WHERE deadline_at <= ?"
                    " AND status IN (?, ?)",
                    (
                        cutoff,
                        ShipmentStatus.IN_TRANSIT.value,
                        ShipmentStatus.PARTIALLY_RECEIVED.value,
                    ),
                ).fetchall()
            ids = [int(r["id"]) for r in rows]
        timed_out: list[int] = []
        for sid in ids:
            try:
                if self.force_timeout_shipment(sid):
                    timed_out.append(sid)
            except InvalidTransitionError:
                # 并发下状态已被其他工作流推进，跳过
                continue
        return timed_out

    def _timeout_shipment(self, conn: sqlite3.Connection, shipment_id: int) -> bool:
        ship = self._shipment_row(conn, shipment_id)
        status = ShipmentStatus(ship["status"])
        if status not in LIVE_SHIPMENT_STATUSES:
            return False
        req = self._request_row(conn, ship["request_id"])
        cancelled = RequestStatus(req["status"]) is RequestStatus.CANCELLED

        rows = conn.execute(
            "SELECT sl.id AS line_id, sl.source_batch_id AS bid, sl.anchor_batch_id AS anchor,"
            " sl.quantity - sl.received_qty - sl.rejected_qty AS outstanding"
            " FROM shipment_lines sl WHERE sl.shipment_id = ?",
            (shipment_id,),
        ).fetchall()
        total = sum(int(r["outstanding"]) for r in rows if r["outstanding"] > 0)
        if total <= 0:
            return False

        ledger_lines: list[tuple[int, str, int, int]] = []
        per_source: dict[int, int] = defaultdict(int)
        per_anchor: dict[int, int] = defaultdict(int)
        for r in rows:
            qty = int(r["outstanding"])
            if qty <= 0:
                continue
            anchor_id = int(r["anchor"])
            bid = int(r["bid"])
            self._debit_in_transit(conn, anchor_id, qty)
            conn.execute(
                "UPDATE batches SET available = available + ? WHERE id = ?", (qty, bid)
            )
            conn.execute(
                "UPDATE shipment_lines SET rejected_qty = rejected_qty + ? WHERE id = ?",
                (qty, r["line_id"]),
            )
            conn.execute(
                "INSERT INTO receipt_lines(shipment_id, request_id, source_batch_id,"
                " dest_batch_id, quantity, rejected, reason)"
                " VALUES (?, ?, ?, NULL, ?, 1, ?)",
                (shipment_id, req["id"], bid, qty, "超时未签收，自动退回"),
            )
            per_source[bid] += qty
            per_anchor[anchor_id] += qty
        for bid, qty in per_source.items():
            ledger_lines.append((bid, "available", qty, 0))
        for anchor_id, qty in per_anchor.items():
            ledger_lines.append((anchor_id, "in_transit", 0, qty))
        ledger.post_entry(
            conn, EntryType.TIMEOUT_RETURN, "shipments", shipment_id, ledger_lines,
            request_id=req["id"], note="超时在途退回来源仓（补偿分录）",
        )

        conn.execute(
            "UPDATE shipments SET status = ?, rejected_qty = rejected_qty + ? WHERE id = ?",
            (ShipmentStatus.TIMEOUT.value, total, shipment_id),
        )

        new_shipped = req["shipped_qty"] - total
        if cancelled:
            # 申请已取消（终态）：只回滚已发运数量，状态保持已取消
            conn.execute(
                "UPDATE transfer_requests SET shipped_qty = ? WHERE id = ?",
                (new_shipped, req["id"]),
            )
        else:
            if new_shipped <= 0 and req["received_qty"] <= 0:
                target = RequestStatus.TIMEOUT
            else:
                # 已有部分签收：保留部分成果，退回量重新可发运
                target = self._request_status_after_return(conn, req, new_shipped)
            self._set_request_status_from(conn, req, target, shipped_qty=new_shipped)
        return True

    # ------------------------------------------------------------------ #
    # 查询 / 追踪
    # ------------------------------------------------------------------ #
    def get_request(self, request_id: int) -> TransferRequestView:
        with self.store.read_tx() as conn:
            row = self._request_row(conn, request_id)
            return self._request_view(conn, row)

    def list_requests(self, status: RequestStatus | str | None = None) -> list[TransferRequestView]:
        sql = "SELECT * FROM transfer_requests"
        args: list[object] = []
        if status is not None:
            value = status.value if isinstance(status, RequestStatus) else status
            sql += " WHERE status = ?"
            args.append(value)
        sql += " ORDER BY id"
        with self.store.read_tx() as conn:
            rows = conn.execute(sql, args).fetchall()
            return [self._request_view(conn, r) for r in rows]

    def _request_view(self, conn: sqlite3.Connection, row: sqlite3.Row) -> TransferRequestView:
        return TransferRequestView(
            id=row["id"],
            part_no=row["part_no"],
            quantity=row["quantity"],
            from_warehouse=row["from_warehouse"],
            to_warehouse=row["to_warehouse"],
            status=row["status"],
            created_at=row["created_at"],
            shipped_qty=row["shipped_qty"],
            received_qty=row["received_qty"],
            timeout_at=row["timeout_at"],
            note=row["note"],
        )

    def list_shipments(self, request_id: int | None = None) -> list[ShipmentView]:
        sql = "SELECT * FROM shipments"
        args: list[object] = []
        if request_id is not None:
            sql += " WHERE request_id = ?"
            args.append(request_id)
        sql += " ORDER BY id"
        with self.store.read_tx() as conn:
            rows = conn.execute(sql, args).fetchall()
            return [self._shipment_view(conn, r) for r in rows]

    def get_shipment(self, shipment_id: int) -> ShipmentView:
        with self.store.read_tx() as conn:
            return self._shipment_view(conn, self._shipment_row(conn, shipment_id))

    def _shipment_view(self, conn: sqlite3.Connection, ship: sqlite3.Row) -> ShipmentView:
        line_rows = conn.execute(
            "SELECT sl.*, b.batch_no AS source_batch_no,"
            " ab.warehouse AS anchor_warehouse, ab.batch_no AS anchor_batch_no"
            " FROM shipment_lines sl JOIN batches b ON b.id = sl.source_batch_id"
            " JOIN batches ab ON ab.id = sl.anchor_batch_id"
            " WHERE sl.shipment_id = ? ORDER BY sl.id",
            (ship["id"],),
        ).fetchall()
        lines = tuple(
            ShipmentLineView(
                id=r["id"],
                source_batch_id=r["source_batch_id"],
                source_batch_no=r["source_batch_no"],
                quantity=r["quantity"],
                received_qty=r["received_qty"],
                rejected_qty=r["rejected_qty"],
            )
            for r in line_rows
        )
        return ShipmentView(
            id=ship["id"],
            request_id=ship["request_id"],
            part_no=ship["part_no"],
            source_warehouse=ship["source_warehouse"],
            destination_warehouse=ship["destination_warehouse"],
            status=ship["status"],
            created_at=ship["created_at"],
            shipped_at=ship["shipped_at"],
            deadline_at=ship["deadline_at"],
            received_at=ship["received_at"],
            original_destination=ship["original_destination"],
            quantity=ship["quantity"],
            received_qty=ship["received_qty"],
            rejected_qty=ship["rejected_qty"],
            lines=lines,
        )

    def list_ledger_entries(
        self,
        request_id: int | None = None,
        entry_type: EntryType | str | None = None,
    ) -> list[LedgerEntryView]:
        sql = (
            "SELECT * FROM ledger_entries WHERE 1=1"
        )
        args: list[object] = []
        if request_id is not None:
            sql += " AND request_id = ?"
            args.append(request_id)
        if entry_type is not None:
            value = entry_type.value if isinstance(entry_type, EntryType) else entry_type
            sql += " AND entry_type = ?"
            args.append(value)
        sql += " ORDER BY id"
        with self.store.read_tx() as conn:
            entries = conn.execute(sql, args).fetchall()
            result = []
            for e in entries:
                line_rows = conn.execute(
                    "SELECT ll.*, b.warehouse AS warehouse, b.part_no AS part_no,"
                    " b.batch_no AS batch_no FROM ledger_lines ll"
                    " JOIN batches b ON b.id = ll.batch_id WHERE ll.entry_id = ? ORDER BY ll.id",
                    (e["id"],),
                ).fetchall()
                lines = tuple(
                    LedgerLineView(
                        batch_id=r["batch_id"],
                        warehouse=r["warehouse"],
                        part_no=r["part_no"],
                        batch_no=r["batch_no"],
                        bucket=r["bucket"],
                        debit=r["debit"],
                        credit=r["credit"],
                    )
                    for r in line_rows
                )
                result.append(
                    LedgerEntryView(
                        id=e["id"],
                        entry_type=e["entry_type"],
                        ref_table=e["ref_table"],
                        ref_id=e["ref_id"],
                        request_id=e["request_id"],
                        created_at=e["created_at"],
                        note=e["note"],
                        lines=lines,
                    )
                )
            return result

    def trace_quantity(
        self,
        request_id: int | None = None,
        shipment_id: int | None = None,
    ) -> list[QuantityTrace]:
        """追踪每件数量的位置：按来源批次汇总出库 / 在途 / 签收 / 拒收。

        必须指定申请单或装运单之一。
        """
        if request_id is None and shipment_id is None:
            raise ValidationError("需要指定 request_id 或 shipment_id")
        with self.store.read_tx() as conn:
            line_where: list[str] = []
            line_args: list[object] = []
            if shipment_id is not None:
                line_where.append("sl.shipment_id = ?")
                line_args.append(shipment_id)
            if request_id is not None:
                line_where.append("s.request_id = ?")
                line_args.append(request_id)
            rows = conn.execute(
                "SELECT sl.source_batch_id AS bid, sb.warehouse AS source_warehouse,"
                " sb.batch_no AS source_batch_no, s.part_no AS part_no,"
                " SUM(sl.quantity) AS shipped, SUM(sl.received_qty) AS received,"
                " SUM(sl.rejected_qty) AS rejected,"
                " SUM(sl.quantity - sl.received_qty - sl.rejected_qty) AS outstanding,"
                " ab.warehouse AS anchor_warehouse"
                " FROM shipment_lines sl"
                " JOIN shipments s ON s.id = sl.shipment_id"
                " JOIN batches sb ON sb.id = sl.source_batch_id"
                " JOIN batches ab ON ab.id = sl.anchor_batch_id"
                " WHERE " + " AND ".join(line_where)
                + " GROUP BY sl.source_batch_id, ab.warehouse ORDER BY sl.source_batch_id",
                line_args,
            ).fetchall()

            receipt_where: list[str] = []
            receipt_args: list[object] = []
            if shipment_id is not None:
                receipt_where.append("rl.shipment_id = ?")
                receipt_args.append(shipment_id)
            if request_id is not None:
                receipt_where.append("rl.request_id = ?")
                receipt_args.append(request_id)
            receipt_rows = conn.execute(
                "SELECT rl.*, db.batch_no AS dest_batch_no, db.warehouse AS dest_warehouse,"
                " sb.batch_no AS source_batch_no"
                " FROM receipt_lines rl"
                " JOIN batches sb ON sb.id = rl.source_batch_id"
                " LEFT JOIN batches db ON db.id = rl.dest_batch_id"
                " WHERE " + " AND ".join(receipt_where) + " ORDER BY rl.id",
                receipt_args,
            ).fetchall()

            receipts_by_source: dict[int, list[ReceiptLineView]] = defaultdict(list)
            for r in receipt_rows:
                receipts_by_source[int(r["source_batch_id"])].append(
                    ReceiptLineView(
                        id=r["id"],
                        shipment_id=r["shipment_id"],
                        request_id=r["request_id"],
                        source_batch_id=r["source_batch_id"],
                        source_batch_no=r["source_batch_no"],
                        dest_batch_id=r["dest_batch_id"],
                        dest_batch_no=r["dest_batch_no"],
                        quantity=r["quantity"],
                        rejected=bool(r["rejected"]),
                        created_at=r["created_at"],
                    )
                )

            traces: dict[int, QuantityTrace] = {}
            locations: dict[int, set[str]] = defaultdict(set)
            for r in rows:
                bid = int(r["bid"])
                outstanding = int(r["outstanding"])
                if outstanding > 0:
                    locations[bid].add(r["anchor_warehouse"])
                existing = traces.get(bid)
                if existing is None:
                    traces[bid] = QuantityTrace(
                        source_warehouse=r["source_warehouse"],
                        source_batch_no=r["source_batch_no"],
                        part_no=r["part_no"],
                        shipped_qty=int(r["shipped"]),
                        received_qty=int(r["received"]),
                        rejected_qty=int(r["rejected"]),
                        in_transit_qty=outstanding,
                        in_transit_locations=tuple(sorted(locations[bid])),
                        receipts=tuple(receipts_by_source.get(bid, [])),
                    )
                else:
                    traces[bid] = QuantityTrace(
                        source_warehouse=existing.source_warehouse,
                        source_batch_no=existing.source_batch_no,
                        part_no=existing.part_no,
                        shipped_qty=existing.shipped_qty + int(r["shipped"]),
                        received_qty=existing.received_qty + int(r["received"]),
                        rejected_qty=existing.rejected_qty + int(r["rejected"]),
                        in_transit_qty=existing.in_transit_qty + outstanding,
                        in_transit_locations=tuple(sorted(locations[bid])),
                        receipts=existing.receipts,
                    )
            return [traces[k] for k in sorted(traces)]

    def audit(self) -> list[str]:
        """运行守恒审计，返回违例列表（空列表表示守恒）。"""
        with self.store.read_tx() as conn:
            return ledger.audit_conservation(conn)
