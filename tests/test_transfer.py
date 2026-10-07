"""多仓库存调拨服务的回归测试。

覆盖：原子出库、并发竞争、部分装运、部分签收、拒收补偿、改道、
超时退回、重复确认幂等拒绝、复式分录平衡与全链路数量追踪。
"""
from __future__ import annotations

import sys
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from transfer import (  # noqa: E402
    InsufficientStockError,
    InvalidTransitionError,
    NotFoundError,
    OverReceiptError,
    TransferService,
    ValidationError,
)
from transfer.enums import EntryType, RequestStatus, ShipmentStatus  # noqa: E402
from transfer.store import Store  # noqa: E402


class TransferTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Store(":memory:")
        self.svc = TransferService(self.store)
        # 工厂 A 有两批关键零件 P1：B001=100, B002=50
        self.svc.create_batch("WH_A", "P1", "B001", 100)
        self.svc.create_batch("WH_A", "P1", "B002", 50)
        self.svc.create_batch("WH_B", "P2", "B003", 30)

    def tearDown(self) -> None:
        self.store.close()

    def approve_request(self, qty: int, frm: str = "WH_A", to: str = "WH_B") -> int:
        rid = self.svc.create_request("P1", qty, frm, to)
        self.svc.submit_request(rid)
        self.svc.approve_request(rid)
        return rid

    def batch(self, wh: str, part: str, no: str):
        rows = [b for b in self.svc.list_batches(wh, part) if b.batch_no == no]
        return rows[0]

    def assertConserved(self) -> None:
        problems = self.svc.audit()
        self.assertEqual(problems, [], "守恒审计发现问题：" + "; ".join(problems))


class LifecycleTest(TransferTestBase):
    def test_draft_submit_approve_flow(self) -> None:
        rid = self.svc.create_request("P1", 10, "WH_A", "WH_B")
        self.assertEqual(self.svc.get_request(rid).status, RequestStatus.DRAFT.value)
        self.svc.submit_request(rid)
        self.svc.approve_request(rid)
        self.assertEqual(self.svc.get_request(rid).status, RequestStatus.APPROVED.value)

    def test_reject_then_revise_resubmit(self) -> None:
        rid = self.svc.create_request("P1", 999, "WH_A", "WH_B")
        self.svc.submit_request(rid)
        self.svc.reject_request(rid, "数量过大")
        self.assertEqual(self.svc.get_request(rid).status, RequestStatus.REJECTED.value)
        self.svc.revise_request(rid, quantity=5)
        self.svc.submit_request(rid)
        self.svc.approve_request(rid)
        self.assertEqual(self.svc.get_request(rid).quantity, 5)

    def test_same_warehouse_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self.svc.create_request("P1", 1, "WH_A", "WH_A")


class ShipmentTest(TransferTestBase):
    def test_confirm_atomically_moves_available_to_in_transit(self) -> None:
        rid = self.approve_request(60)
        sid = self.svc.create_shipment(rid, [("B001", 40), ("B002", 20)])
        # 计划阶段不动库存
        self.assertEqual(self.batch("WH_A", "P1", "B001").available, 100)
        ship = self.svc.get_shipment(sid)
        self.assertEqual(ship.status, ShipmentStatus.PLANNED.value)
        self.assertEqual(ship.lines, ())  # 计划明细尚未固化到来源批次行

        self.svc.confirm_shipment(sid)

        self.assertEqual(self.batch("WH_A", "P1", "B001").available, 60)
        self.assertEqual(self.batch("WH_A", "P1", "B002").available, 30)
        # 在途锚定在目的地仓
        anchor = [b for b in self.svc.list_batches("WH_B", "P1") if b.in_transit > 0][0]
        self.assertEqual(anchor.in_transit, 60)
        req = self.svc.get_request(rid)
        self.assertEqual(req.status, RequestStatus.SHIPPED.value)
        self.assertEqual(req.shipped_qty, 60)
        self.assertConserved()

    def test_duplicate_confirm_is_idempotent_rejection(self) -> None:
        rid = self.approve_request(10)
        sid = self.svc.create_shipment(rid, [("B001", 10)])
        self.svc.confirm_shipment(sid)
        # 失败重试：再次确认必须被拒绝，不能重复发运
        with self.assertRaises(InvalidTransitionError):
            self.svc.confirm_shipment(sid)
        self.assertEqual(self.batch("WH_A", "P1", "B001").available, 90)
        self.assertConserved()

    def test_over_shipment_against_request_rejected(self) -> None:
        # 计划阶段只校验申请余量：申请 120、单批 120 可建计划，
        # 确认时来源批次可用量只有 100 → 条件更新失败回滚
        rid = self.approve_request(120)
        sid = self.svc.create_shipment(rid, [("B001", 120)])
        with self.assertRaises(InsufficientStockError):
            self.svc.confirm_shipment(sid)
        # 回滚干净：库存未动、申请未推进
        self.assertEqual(self.batch("WH_A", "P1", "B001").available, 100)
        self.assertEqual(
            self.svc.get_request(rid).status, RequestStatus.APPROVED.value
        )

    def test_insufficient_stock_rolls_back_everything(self) -> None:
        rid = self.approve_request(150)
        sid = self.svc.create_shipment(rid, [("B001", 110), ("B002", 40)])
        with self.assertRaises(InsufficientStockError):
            self.svc.confirm_shipment(sid)
        # 即使第一个批次扣减成功，第二个不足也整体回滚
        self.assertEqual(self.batch("WH_A", "P1", "B001").available, 100)
        self.assertEqual(self.batch("WH_A", "P1", "B002").available, 50)
        # 装运单仍是已计划，可修正后重试
        self.assertEqual(self.svc.get_shipment(sid).status, ShipmentStatus.PLANNED.value)
        self.svc.create_shipment  # 可再开一单
        sid2 = self.svc.create_shipment(rid, [("B001", 100), ("B002", 20)])
        self.svc.confirm_shipment(sid2)
        self.assertConserved()

    def test_partial_shipments(self) -> None:
        rid = self.approve_request(100)
        s1 = self.svc.create_shipment(rid, [("B001", 30)])
        self.svc.confirm_shipment(s1)
        self.assertEqual(
            self.svc.get_request(rid).status, RequestStatus.PARTIALLY_SHIPPED.value
        )
        s2 = self.svc.create_shipment(rid, [("B001", 70)])
        self.svc.confirm_shipment(s2)
        self.assertEqual(self.svc.get_request(rid).status, RequestStatus.SHIPPED.value)
        self.assertEqual(self.batch("WH_A", "P1", "B001").available, 0)
        self.assertConserved()

    def test_cancel_planned_shipment_touches_no_stock(self) -> None:
        rid = self.approve_request(10)
        sid = self.svc.create_shipment(rid, [("B001", 10)])
        self.svc.cancel_planned_shipment(sid)
        self.assertEqual(self.batch("WH_A", "P1", "B001").available, 100)
        with self.assertRaises(InvalidTransitionError):
            self.svc.confirm_shipment(sid)


class ReceiptTest(TransferTestBase):
    def _shipped(self, qty: int = 60):
        rid = self.approve_request(qty)
        sid = self.svc.create_shipment(rid, [("B001", 40), ("B002", 20)])
        self.svc.confirm_shipment(sid)
        return rid, sid

    def test_full_receive_creates_dest_batch_and_closes(self) -> None:
        rid, sid = self._shipped()
        self.svc.receive_shipment(sid, [("B001", 40), ("B002", 20)], "RCV-1")
        dest = self.batch("WH_B", "P1", "RCV-1")
        self.assertEqual(dest.available, 60)
        ship = self.svc.get_shipment(sid)
        self.assertEqual(ship.status, ShipmentStatus.RECEIVED.value)
        req = self.svc.get_request(rid)
        self.assertEqual(req.status, RequestStatus.RECEIVED.value)
        self.assertEqual(req.received_qty, 60)
        # 在途清零
        self.assertEqual(sum(b.in_transit for b in self.svc.list_batches()), 0)
        self.assertConserved()

    def test_partial_receive_then_remainder(self) -> None:
        rid, sid = self._shipped()
        self.svc.receive_shipment(sid, [("B001", 25)], "RCV-1")
        ship = self.svc.get_shipment(sid)
        self.assertEqual(ship.status, ShipmentStatus.PARTIALLY_RECEIVED.value)
        self.assertEqual(self.batch("WH_B", "P1", "RCV-1").available, 25)
        self.assertEqual(
            self.svc.get_request(rid).status, RequestStatus.PARTIALLY_RECEIVED.value
        )
        # 收 B001 剩余与 B002 全部
        self.svc.receive_shipment(sid, [("B001", 15), ("B002", 20)], "RCV-2")
        self.assertEqual(self.svc.get_shipment(sid).status, ShipmentStatus.RECEIVED.value)
        self.assertEqual(self.svc.get_request(rid).status, RequestStatus.RECEIVED.value)
        self.assertEqual(self.batch("WH_B", "P1", "RCV-2").available, 35)
        self.assertConserved()

    def test_over_receive_rejected(self) -> None:
        _, sid = self._shipped()
        self.svc.receive_shipment(sid, [("B001", 40)], "RCV-1")
        with self.assertRaises(OverReceiptError):
            self.svc.receive_shipment(sid, [("B001", 1)], "RCV-1")
        self.assertEqual(self.batch("WH_B", "P1", "RCV-1").available, 40)

    def test_partial_receive_then_reject_remainder(self) -> None:
        rid, sid = self._shipped()
        self.svc.receive_shipment(sid, [("B001", 40)], "RCV-1")
        # B002 的 20 件拒收，退回来源仓
        self.svc.reject_shipment(sid, [("B002", 20)], "外观破损")
        ship = self.svc.get_shipment(sid)
        self.assertEqual(ship.status, ShipmentStatus.REJECTED.value)
        self.assertEqual(ship.received_qty, 40)
        self.assertEqual(ship.rejected_qty, 20)
        # 来源批次 B002 可用量恢复
        self.assertEqual(self.batch("WH_A", "P1", "B002").available, 50)
        req = self.svc.get_request(rid)
        # 已签收 40 < 申请 60，退回后 shipped=40 → 部分装运，可补发
        self.assertEqual(req.status, RequestStatus.PARTIALLY_SHIPPED.value)
        self.assertEqual(req.shipped_qty, 40)
        self.assertEqual(req.received_qty, 40)
        self.assertConserved()

    def test_full_reject_returns_all_and_allows_reship(self) -> None:
        rid = self.approve_request(40)
        sid = self.svc.create_shipment(rid, [("B001", 40)])
        self.svc.confirm_shipment(sid)
        self.svc.reject_shipment(sid, [("B001", 40)], "地址错误")
        self.assertEqual(self.batch("WH_A", "P1", "B001").available, 100)
        self.assertEqual(self.svc.get_shipment(sid).status, ShipmentStatus.REJECTED.value)
        # 申请回到已批准，可以重新发运
        self.assertEqual(self.svc.get_request(rid).status, RequestStatus.APPROVED.value)
        sid2 = self.svc.create_shipment(rid, [("B001", 40)])
        self.svc.confirm_shipment(sid2)
        self.svc.receive_shipment(sid2, [("B001", 40)], "RCV-X")
        self.assertEqual(self.svc.get_request(rid).status, RequestStatus.RECEIVED.value)
        self.assertConserved()


class RerouteTest(TransferTestBase):
    def test_reroute_moves_in_transit_anchor(self) -> None:
        rid = self.svc.create_request("P1", 40, "WH_A", "WH_B")
        self.svc.submit_request(rid)
        self.svc.approve_request(rid)
        sid = self.svc.create_shipment(rid, [("B001", 40)])
        self.svc.confirm_shipment(sid)
        self.svc.reroute_shipment(sid, "WH_C", "工厂 B 停产，改供工厂 C")

        ship = self.svc.get_shipment(sid)
        self.assertEqual(ship.destination_warehouse, "WH_C")
        self.assertEqual(ship.original_destination, "WH_B")
        self.assertEqual(ship.status, ShipmentStatus.IN_TRANSIT.value)
        # B 仓在途清零，C 仓在途 40；来源批次与数量不变
        b_anchor = [
            b for b in self.svc.list_batches("WH_B", "P1") if b.batch_no == "__IN_TRANSIT__"
        ][0]
        self.assertEqual(b_anchor.in_transit, 0)
        c_anchor = [b for b in self.svc.list_batches("WH_C", "P1") if b.in_transit > 0][0]
        self.assertEqual(c_anchor.in_transit, 40)
        # 改道后签收入 C 仓
        self.svc.receive_shipment(sid, [("B001", 40)], "RCV-C")
        self.assertEqual(self.batch("WH_C", "P1", "RCV-C").available, 40)
        self.assertConserved()

    def test_reroute_after_partial_receive(self) -> None:
        rid = self.svc.create_request("P1", 40, "WH_A", "WH_B")
        self.svc.submit_request(rid)
        self.svc.approve_request(rid)
        sid = self.svc.create_shipment(rid, [("B001", 40)])
        self.svc.confirm_shipment(sid)
        self.svc.receive_shipment(sid, [("B001", 10)], "RCV-B")
        self.svc.reroute_shipment(sid, "WH_C")
        traces = self.svc.trace_quantity(shipment_id=sid)
        self.assertEqual(traces[0].in_transit_qty, 30)
        self.assertEqual(traces[0].in_transit_locations, ("WH_C",))
        self.svc.receive_shipment(sid, [("B001", 30)], "RCV-C")
        self.assertEqual(self.batch("WH_B", "P1", "RCV-B").available, 10)
        self.assertEqual(self.batch("WH_C", "P1", "RCV-C").available, 30)
        self.assertConserved()


class TimeoutTest(TransferTestBase):
    def test_force_timeout_returns_in_transit(self) -> None:
        rid = self.svc.create_request("P1", 40, "WH_A", "WH_B")
        self.svc.submit_request(rid)
        self.svc.approve_request(rid)
        sid = self.svc.create_shipment(rid, [("B001", 40)])
        self.svc.confirm_shipment(sid)
        self.assertTrue(self.svc.force_timeout_shipment(sid))
        self.assertEqual(self.batch("WH_A", "P1", "B001").available, 100)
        self.assertEqual(self.svc.get_shipment(sid).status, ShipmentStatus.TIMEOUT.value)
        self.assertEqual(self.svc.get_request(rid).status, RequestStatus.TIMEOUT.value)
        self.assertFalse(self.svc.force_timeout_shipment(sid))  # 幂等
        self.assertConserved()

    def test_timeout_after_partial_receive_keeps_received_part(self) -> None:
        rid = self.svc.create_request("P1", 40, "WH_A", "WH_B")
        self.svc.submit_request(rid)
        self.svc.approve_request(rid)
        sid = self.svc.create_shipment(rid, [("B001", 40)])
        self.svc.confirm_shipment(sid)
        self.svc.receive_shipment(sid, [("B001", 15)], "RCV-1")
        self.svc.force_timeout_shipment(sid)
        # 已签收 15 保留在 B 仓，25 退回 A 仓可重新发运
        self.assertEqual(self.batch("WH_B", "P1", "RCV-1").available, 15)
        self.assertEqual(self.batch("WH_A", "P1", "B001").available, 85)
        req = self.svc.get_request(rid)
        self.assertEqual(req.status, RequestStatus.PARTIALLY_SHIPPED.value)
        self.assertEqual(req.received_qty, 15)
        # 补发剩余 25 并签收
        sid2 = self.svc.create_shipment(rid, [("B001", 25)])
        self.svc.confirm_shipment(sid2)
        self.svc.receive_shipment(sid2, [("B001", 25)], "RCV-2")
        self.assertEqual(self.svc.get_request(rid).status, RequestStatus.RECEIVED.value)
        self.assertConserved()

    def test_sweep_timeouts_only_expired(self) -> None:
        rid = self.svc.create_request("P1", 30, "WH_A", "WH_B")
        self.svc.submit_request(rid)
        self.svc.approve_request(rid)
        past = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        future = (datetime.now() + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        expired = self.svc.create_shipment(rid, [("B001", 10)], deadline=past)
        fresh = self.svc.create_shipment(rid, [("B001", 20)], deadline=future)
        self.svc.confirm_shipment(expired)
        self.svc.confirm_shipment(fresh)
        timed_out = self.svc.sweep_timeouts()
        self.assertEqual(timed_out, [expired])
        self.assertEqual(self.svc.get_shipment(expired).status, ShipmentStatus.TIMEOUT.value)
        self.assertEqual(self.svc.get_shipment(fresh).status, ShipmentStatus.IN_TRANSIT.value)
        self.assertConserved()


class CancelTest(TransferTestBase):
    def test_cancel_with_planned_shipment_cascades(self) -> None:
        rid = self.approve_request(10)
        sid = self.svc.create_shipment(rid, [("B001", 10)])
        cancelled = self.svc.cancel_request(rid)
        self.assertEqual(cancelled, [sid])
        self.assertEqual(self.svc.get_shipment(sid).status, ShipmentStatus.CANCELLED.value)
        self.assertEqual(self.batch("WH_A", "P1", "B001").available, 100)
        self.assertConserved()

    def test_cancel_with_in_transit_then_timeout_keeps_cancelled(self) -> None:
        rid = self.approve_request(10)
        sid = self.svc.create_shipment(rid, [("B001", 10)])
        self.svc.confirm_shipment(sid)
        self.svc.cancel_request(rid)
        self.assertEqual(self.svc.get_request(rid).status, RequestStatus.CANCELLED.value)
        # 在途货物超时退回，申请保持取消，不重新打开
        self.svc.force_timeout_shipment(sid)
        self.assertEqual(self.svc.get_request(rid).status, RequestStatus.CANCELLED.value)
        self.assertEqual(self.batch("WH_A", "P1", "B001").available, 100)
        self.assertConserved()


class ConcurrencyTest(TransferTestBase):
    def test_concurrent_requests_never_oversell(self) -> None:
        """两个缺料工厂同时抢同一批 100 件，各要 80。

        并发确认下只有一单能成功；另一单收到 InsufficientStockError，
        库存恒守恒、不消失、不重复发运。
        """
        rid1 = self.approve_request(80, to="WH_B")
        rid2 = self.approve_request(80, to="WH_C")
        s1 = self.svc.create_shipment(rid1, [("B001", 80)])
        s2 = self.svc.create_shipment(rid2, [("B001", 80)])

        errors: list[BaseException] = []

        def confirm(sid: int) -> None:
            try:
                self.svc.confirm_shipment(sid)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        t1 = threading.Thread(target=confirm, args=(s1,))
        t2 = threading.Thread(target=confirm, args=(s2,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], InsufficientStockError)
        b = self.batch("WH_A", "P1", "B001")
        self.assertEqual(b.available, 20)
        in_transit_total = sum(x.in_transit for x in self.svc.list_batches())
        self.assertEqual(in_transit_total, 80)
        self.assertConserved()

    def test_concurrent_requests_many_workers_split_stock(self) -> None:
        """10 个工厂各抢 15 件，总库存 100：恰好 6 单全中、其余失败。"""
        requests_ = []
        for i in range(10):
            rid = self.approve_request(15, to=f"WH_D{i}")
            sid = self.svc.create_shipment(rid, [("B001", 15)])
            requests_.append(sid)

        ok: list[int] = []
        fail: list[int] = []
        lock = threading.Lock()

        def confirm(sid: int) -> None:
            try:
                self.svc.confirm_shipment(sid)
                with lock:
                    ok.append(sid)
            except InsufficientStockError:
                with lock:
                    fail.append(sid)

        with ThreadPoolExecutor(max_workers=10) as pool:
            list(pool.map(confirm, requests_))

        self.assertEqual(len(ok), 6)
        self.assertEqual(len(fail), 4)
        self.assertEqual(self.batch("WH_A", "P1", "B001").available, 10)
        self.assertEqual(sum(b.in_transit for b in self.svc.list_batches()), 90)
        self.assertConserved()


class LedgerAndTraceTest(TransferTestBase):
    def test_every_move_has_balanced_entry(self) -> None:
        rid = self.approve_request(60)
        sid = self.svc.create_shipment(rid, [("B001", 40), ("B002", 20)])
        self.svc.confirm_shipment(sid)
        self.svc.receive_shipment(sid, [("B001", 20)], "R1")
        self.svc.reject_shipment(sid, [("B001", 20), ("B002", 20)], "x")

        types = {e.entry_type for e in self.svc.list_ledger_entries()}
        self.assertIn(EntryType.SHIP_OUT.value, types)
        self.assertIn(EntryType.RECEIVE.value, types)
        self.assertIn(EntryType.REJECT_RETURN.value, types)
        for e in self.svc.list_ledger_entries():
            debit = sum(l.debit for l in e.lines)
            credit = sum(l.credit for l in e.lines)
            self.assertEqual(debit, credit, f"凭证 {e.id}({e.entry_type}) 不平衡")
        self.assertConserved()

    def test_trace_locates_every_unit(self) -> None:
        rid = self.approve_request(60)
        sid = self.svc.create_shipment(rid, [("B001", 40), ("B002", 20)])
        self.svc.confirm_shipment(sid)
        # 部分签收、部分拒收、部分仍在途（B001: 收10 退10 在途20；B002 全在途）
        self.svc.receive_shipment(sid, [("B001", 10)], "R1")
        self.svc.reject_shipment(sid, [("B001", 10)], "坏件")

        traces = {t.source_batch_no: t for t in self.svc.trace_quantity(request_id=rid)}
        t1 = traces["B001"]
        self.assertEqual((t1.shipped_qty, t1.received_qty, t1.rejected_qty, t1.in_transit_qty),
                         (40, 10, 10, 20))
        self.assertEqual(t1.in_transit_locations, ("WH_B",))
        self.assertEqual(t1.shipped_qty,
                         t1.received_qty + t1.rejected_qty + t1.in_transit_qty)
        t2 = traces["B002"]
        self.assertEqual((t2.shipped_qty, t2.in_transit_qty), (20, 20))

        # 签收追踪行：10 件 B001 落到入库批次 R1
        receipts = [r for r in t1.receipts if not r.rejected]
        self.assertEqual(len(receipts), 1)
        self.assertEqual(receipts[0].dest_batch_no, "R1")
        self.assertEqual(receipts[0].quantity, 10)

    def test_trace_across_reroute_and_second_shipment(self) -> None:
        rid = self.svc.create_request("P1", 60, "WH_A", "WH_B")
        self.svc.submit_request(rid)
        self.svc.approve_request(rid)
        s1 = self.svc.create_shipment(rid, [("B001", 40)])
        self.svc.confirm_shipment(s1)
        self.svc.reroute_shipment(s1, "WH_C")
        self.svc.receive_shipment(s1, [("B001", 40)], "RC")
        s2 = self.svc.create_shipment(rid, [("B002", 20)])
        self.svc.confirm_shipment(s2)
        traces = {t.source_batch_no: t for t in self.svc.trace_quantity(request_id=rid)}
        self.assertEqual(traces["B001"].received_qty, 40)
        self.assertEqual(traces["B001"].receipts[0].dest_batch_no, "RC")
        self.assertEqual(traces["B002"].in_transit_locations, ("WH_B",))


class ErrorPathTest(TransferTestBase):
    def test_unknown_entities(self) -> None:
        with self.assertRaises(NotFoundError):
            self.svc.get_request(999)
        with self.assertRaises(NotFoundError):
            self.svc.confirm_shipment(999)

    def test_illegal_transitions(self) -> None:
        rid = self.svc.create_request("P1", 1, "WH_A", "WH_B")
        # 草拟不能直接审批
        with self.assertRaises(InvalidTransitionError):
            self.svc.approve_request(rid)
        self.svc.submit_request(rid)
        self.svc.approve_request(rid)
        sid = self.svc.create_shipment(rid, [("B001", 1)])
        # 未确认不能签收
        with self.assertRaises(InvalidTransitionError):
            self.svc.receive_shipment(sid, [("B001", 1)])


if __name__ == "__main__":
    unittest.main()
