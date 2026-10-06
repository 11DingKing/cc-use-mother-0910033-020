"""多仓库存调拨后端测试。

运行：python3 -m unittest discover -s tests -v

覆盖四类不变量：
- 跨仓批次守恒：每个用例结束后校验台账与计数器对账平衡；
- 在途状态转换：非法迁移返回 409；
- 部分装运签收：多次装运、多次签收、逐行拒收；
- 失败补偿分录：拒收/退回/取消产生 compensates_txn 指向原事务组的分录；
以及并发（不超卖、确认只扣一次）与崩溃原子性（故障注入）。
"""
from __future__ import annotations

import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import func, select  # noqa: E402
from sqlalchemy.exc import OperationalError  # noqa: E402

from transfer_backend import services  # noqa: E402
from transfer_backend.app import create_app  # noqa: E402
from transfer_backend.database import init_schema, make_engine, make_session_factory, session_scope  # noqa: E402
from transfer_backend.errors import DomainError  # noqa: E402
from transfer_backend.models import InventoryBatch, LedgerEntry, Shipment  # noqa: E402

API = "/api/v1"


class ApiTestCase(unittest.TestCase):
    """每个用例独立 SQLite 文件库 + TestClient。"""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "test.sqlite3"
        self.app = create_app(f"sqlite:///{self.db_path}")
        self.client = TestClient(self.app)

    def tearDown(self) -> None:
        self.client.close()
        self.app.state.engine.dispose()
        self.tmp.cleanup()

    # ----------------  fixture helpers  ----------------

    def make_world(self, qty: int = 100, batch_no: str = "B-001"):
        wh_a = self.client.post(f"{API}/warehouses", json={"code": "WH-A", "name": "工厂A"}).json()
        wh_b = self.client.post(f"{API}/warehouses", json={"code": "WH-B", "name": "工厂B"}).json()
        part = self.client.post(f"{API}/parts", json={"sku": "P-1", "name": "关键轴承"}).json()
        batch = self.client.post(
            f"{API}/batches",
            json={"warehouse_id": wh_a["id"], "part_id": part["id"], "batch_no": batch_no, "qty": qty},
        ).json()
        return wh_a, wh_b, part, batch

    def make_approved_request(self, wh_a, wh_b, part, qty: int, approve_qty: int | None = None):
        req = self.client.post(
            f"{API}/transfer-requests",
            json={
                "source_warehouse_id": wh_a["id"],
                "dest_warehouse_id": wh_b["id"],
                "part_id": part["id"],
                "qty": qty,
                "reason": "缺料停线",
                "created_by": "计划员甲",
            },
        ).json()
        r = self.client.post(f"{API}/transfer-requests/{req['id']}/submit")
        assert r.status_code == 200, r.text
        body = {"decision": "APPROVE", "approver": "仓储管理员"}
        if approve_qty is not None:
            body["qty"] = approve_qty
        r = self.client.post(f"{API}/transfer-requests/{req['id']}/approve", json=body)
        assert r.status_code == 200, r.text
        return req["id"]

    def confirm(self, shipment_id, key=None):
        headers = {"Idempotency-Key": key} if key else {}
        return self.client.post(f"{API}/shipments/{shipment_id}/confirm", headers=headers)

    def get_batch(self, batch_id):
        batches = self.client.get(f"{API}/batches").json()
        return next(b for b in batches if b["id"] == batch_id)

    def assert_reconciled(self, part_id):
        recon = self.client.get(f"{API}/parts/{part_id}/reconciliation").json()
        self.assertTrue(recon["balanced"], f"对账不平衡: {recon}")

    # ----------------  用例  ----------------

    def test_full_lifecycle_and_conservation(self):
        """完整链路：入库 -> 申请 -> 审批预留 -> 装运确认 -> 签收，全程守恒。"""
        wh_a, wh_b, part, batch = self.make_world(qty=100)
        req_id = self.make_approved_request(wh_a, wh_b, part, 60)

        # 审批后来源仓：可用 40 / 预留 60 / 在库 100
        b = self.get_batch(batch["id"])
        self.assertEqual((b["qty_available"], b["qty_reserved"], b["qty_on_hand"]), (40, 60, 100))

        sh = self.client.post(f"{API}/transfer-requests/{req_id}/shipments", json={"carrier": "顺丰"}).json()
        self.assertEqual(sh["status"], "PENDING")
        # 建单不扣库存
        b = self.get_batch(batch["id"])
        self.assertEqual(b["qty_on_hand"], 100)

        r = self.confirm(sh["id"])
        self.assertEqual(r.status_code, 200, r.text)
        sh = r.json()
        self.assertEqual(sh["status"], "IN_TRANSIT")
        # 确认后来源仓：预留 0 / 在库 40（60 件转为在途）
        b = self.get_batch(batch["id"])
        self.assertEqual((b["qty_available"], b["qty_reserved"], b["qty_on_hand"]), (40, 0, 40))

        line_id = sh["lines"][0]["id"]
        r = self.client.post(
            f"{API}/shipments/{sh['id']}/receipts",
            json={"lines": [{"shipment_line_id": line_id, "qty_accepted": 60}], "receiver": "仓管乙"},
        )
        self.assertEqual(r.status_code, 201, r.text)

        req = self.client.get(f"{API}/transfer-requests/{req_id}").json()
        self.assertEqual(req["status"], "RECEIVED")
        self.assertEqual(req["qty_received"], 60)
        self.assertEqual(req["qty_in_transit"], 0)

        # 目的仓出现同批次号的新批次，谱系指向来源批次
        dest_batches = [b for b in self.client.get(f"{API}/batches?warehouse_id={wh_b['id']}").json()]
        self.assertEqual(len(dest_batches), 1)
        self.assertEqual(dest_batches[0]["qty_available"], 60)
        self.assertEqual(dest_batches[0]["origin_batch_id"], batch["id"])

        self.assert_reconciled(part["id"])

    def test_partial_shipments_and_receipts(self):
        """部分装运 + 部分签收：状态机经历 部分装运/已装运/部分签收/已签收。"""
        wh_a, wh_b, part, batch = self.make_world(qty=100)
        req_id = self.make_approved_request(wh_a, wh_b, part, 80)

        sh1 = self.client.post(f"{API}/transfer-requests/{req_id}/shipments", json={"qty": 30}).json()
        req = self.client.get(f"{API}/transfer-requests/{req_id}").json()
        self.assertEqual(req["status"], "APPROVED")  # 未确认前不进入装运态
        self.confirm(sh1["id"])
        req = self.client.get(f"{API}/transfer-requests/{req_id}").json()
        self.assertEqual(req["status"], "PARTIALLY_SHIPPED")

        sh2 = self.client.post(f"{API}/transfer-requests/{req_id}/shipments", json={"qty": 50}).json()
        self.confirm(sh2["id"])
        req = self.client.get(f"{API}/transfer-requests/{req_id}").json()
        self.assertEqual(req["status"], "SHIPPED")

        # 第一张单签收 10（部分）
        line1 = sh1["lines"][0]["id"]
        self.client.post(
            f"{API}/shipments/{sh1['id']}/receipts",
            json={"lines": [{"shipment_line_id": line1, "qty_accepted": 10}]},
        )
        sh1_now = self.client.get(f"{API}/shipments/{sh1['id']}").json()
        self.assertEqual(sh1_now["status"], "PARTIALLY_RECEIVED")
        req = self.client.get(f"{API}/transfer-requests/{req_id}").json()
        self.assertEqual(req["status"], "PARTIALLY_RECEIVED")

        # 签完剩余全部
        self.client.post(
            f"{API}/shipments/{sh1['id']}/receipts",
            json={"lines": [{"shipment_line_id": line1, "qty_accepted": 20}]},
        )
        line2 = sh2["lines"][0]["id"]
        self.client.post(
            f"{API}/shipments/{sh2['id']}/receipts",
            json={"lines": [{"shipment_line_id": line2, "qty_accepted": 50}]},
        )
        req = self.client.get(f"{API}/transfer-requests/{req_id}").json()
        self.assertEqual(req["status"], "RECEIVED")
        self.assertEqual(req["qty_received"], 80)
        self.assert_reconciled(part["id"])

    def test_rejection_writes_compensation_entries(self):
        """拒收：在途 -> 来源仓可用，补偿分录指向原 SHIP 事务组。"""
        wh_a, wh_b, part, batch = self.make_world(qty=100)
        req_id = self.make_approved_request(wh_a, wh_b, part, 50)
        sh = self.client.post(f"{API}/transfer-requests/{req_id}/shipments", json={}).json()
        self.confirm(sh["id"])
        line_id = sh["lines"][0]["id"]

        r = self.client.post(
            f"{API}/shipments/{sh['id']}/receipts",
            json={"lines": [{"shipment_line_id": line_id, "qty_accepted": 30, "qty_rejected": 20}], "note": "外观不良"},
        )
        self.assertEqual(r.status_code, 201, r.text)
        sh_now = self.client.get(f"{API}/shipments/{sh['id']}").json()
        self.assertEqual(sh_now["status"], "RESOLVED_WITH_REJECTION")

        # 拒收的 20 件回到来源仓可用量
        b = self.get_batch(batch["id"])
        self.assertEqual((b["qty_available"], b["qty_reserved"], b["qty_on_hand"]), (70, 0, 70))

        # 补偿分录校验
        entries = self.client.get(f"{API}/ledger?part_id={part['id']}").json()
        ship_txns = {e["txn_group"] for e in entries if e["entry_type"] == "SHIP"}
        compensations = [e for e in entries if e["entry_type"] == "REJECT_RETURN"]
        self.assertEqual(len(compensations), 2)  # 一对平衡分录
        self.assertTrue(all(e["compensates_txn"] in ship_txns for e in compensations))
        self.assertEqual(sum(e["qty_delta"] for e in compensations), 0)

        req = self.client.get(f"{API}/transfer-requests/{req_id}").json()
        self.assertEqual(req["status"], "CLOSED")  # 有拒收，非全签 -> 关闭而非 RECEIVED
        self.assertEqual(req["qty_rejected"], 20)
        self.assert_reconciled(part["id"])

    def test_reroute_changes_destination(self):
        """在途改道：货物改发 WH-C，签收落入新目的仓。"""
        wh_a, wh_b, part, batch = self.make_world(qty=100)
        wh_c = self.client.post(f"{API}/warehouses", json={"code": "WH-C", "name": "工厂C"}).json()
        req_id = self.make_approved_request(wh_a, wh_b, part, 40)
        sh = self.client.post(f"{API}/transfer-requests/{req_id}/shipments", json={}).json()
        self.confirm(sh["id"])

        r = self.client.post(
            f"{API}/shipments/{sh['id']}/reroute",
            json={"new_dest_warehouse_id": wh_c["id"], "reason": "B厂停产"},
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["dest_warehouse_id"], wh_c["id"])

        line_id = sh["lines"][0]["id"]
        self.client.post(
            f"{API}/shipments/{sh['id']}/receipts",
            json={"lines": [{"shipment_line_id": line_id, "qty_accepted": 40}]},
        )
        dest = self.client.get(f"{API}/batches?warehouse_id={wh_c['id']}").json()
        self.assertEqual(dest[0]["qty_available"], 40)
        self.assertEqual(self.client.get(f"{API}/batches?warehouse_id={wh_b['id']}").json(), [])

        events = self.client.get(f"{API}/shipments/{sh['id']}/events").json()
        self.assertIn("REROUTED", [e["event_type"] for e in events])
        self.assert_reconciled(part["id"])

    def test_timeout_then_return_to_source(self):
        """超时：sweep 标记 TIMED_OUT，退回后在途原子转回来源仓可用。"""
        wh_a, wh_b, part, batch = self.make_world(qty=100)
        req_id = self.make_approved_request(wh_a, wh_b, part, 60)
        sh = self.client.post(
            f"{API}/transfer-requests/{req_id}/shipments",
            json={"eta": "2026-01-01T08:00:00"},
        ).json()
        self.confirm(sh["id"])

        r = self.client.post(f"{API}/shipments/timeout-sweep", json={"now": "2026-01-02T00:00:00"})
        self.assertEqual(r.json()["timed_out_shipment_ids"], [sh["id"]])
        self.assertEqual(self.client.get(f"{API}/shipments/{sh['id']}").json()["status"], "TIMED_OUT")

        r = self.client.post(f"{API}/shipments/{sh['id']}/return", json={"reason": "超时未达，召回"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["status"], "RETURNED")

        b = self.get_batch(batch["id"])
        self.assertEqual((b["qty_available"], b["qty_reserved"], b["qty_on_hand"]), (100, 0, 100))

        entries = self.client.get(f"{API}/ledger?part_id={part['id']}").json()
        returns = [e for e in entries if e["entry_type"] == "TIMEOUT_RETURN"]
        self.assertEqual(len(returns), 2)
        self.assertTrue(all(e["compensates_txn"] for e in returns))

        req = self.client.get(f"{API}/transfer-requests/{req_id}").json()
        self.assertEqual(req["status"], "CLOSED")
        self.assertEqual(req["qty_returned"], 60)
        self.assert_reconciled(part["id"])

    def test_timeout_extend_then_late_receipt(self):
        """超时后延期：TIMED_OUT -> IN_TRANSIT，迟到签收仍可用。"""
        wh_a, wh_b, part, batch = self.make_world(qty=50)
        req_id = self.make_approved_request(wh_a, wh_b, part, 50)
        sh = self.client.post(
            f"{API}/transfer-requests/{req_id}/shipments",
            json={"eta": "2026-01-01T08:00:00"},
        ).json()
        self.confirm(sh["id"])
        self.client.post(f"{API}/shipments/timeout-sweep", json={"now": "2026-01-02T00:00:00"})

        r = self.client.post(f"{API}/shipments/{sh['id']}/extend", json={"new_eta": "2026-01-05T00:00:00"})
        self.assertEqual(r.json()["status"], "IN_TRANSIT")

        line_id = sh["lines"][0]["id"]
        r = self.client.post(
            f"{API}/shipments/{sh['id']}/receipts",
            json={"lines": [{"shipment_line_id": line_id, "qty_accepted": 50}]},
        )
        self.assertEqual(r.status_code, 201, r.text)
        self.assertEqual(self.client.get(f"{API}/shipments/{sh['id']}").json()["status"], "RECEIVED")
        self.assert_reconciled(part["id"])

    def test_cancel_releases_reservation(self):
        """取消：部分取消释放预留（RELEASE 补偿分录），全部取消后申请终止。"""
        wh_a, wh_b, part, batch = self.make_world(qty=100)
        req_id = self.make_approved_request(wh_a, wh_b, part, 80)

        r = self.client.post(f"{API}/transfer-requests/{req_id}/cancel", json={"qty": 30, "reason": "需求减少"})
        self.assertEqual(r.status_code, 200, r.text)
        b = self.get_batch(batch["id"])
        self.assertEqual((b["qty_available"], b["qty_reserved"]), (50, 50))

        r = self.client.post(f"{API}/transfer-requests/{req_id}/cancel", json={})
        self.assertEqual(r.json()["status"], "CANCELLED")
        b = self.get_batch(batch["id"])
        self.assertEqual((b["qty_available"], b["qty_reserved"], b["qty_on_hand"]), (100, 0, 100))

        entries = self.client.get(f"{API}/ledger?part_id={part['id']}").json()
        releases = [e for e in entries if e["entry_type"] == "RELEASE"]
        self.assertEqual(sum(e["qty_delta"] for e in releases), 0)
        self.assert_reconciled(part["id"])

    def test_insufficient_stock_rolls_back_entire_approval(self):
        """库存不足：审批整体失败，不产生半个预留，申请保持待审批。"""
        wh_a, wh_b, part, batch = self.make_world(qty=50)
        req = self.client.post(
            f"{API}/transfer-requests",
            json={"source_warehouse_id": wh_a["id"], "dest_warehouse_id": wh_b["id"], "part_id": part["id"], "qty": 80},
        ).json()
        self.client.post(f"{API}/transfer-requests/{req['id']}/submit")
        r = self.client.post(f"{API}/transfer-requests/{req['id']}/approve", json={"decision": "APPROVE"})
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.json()["error"]["code"], "INSUFFICIENT_STOCK")
        self.assertEqual(r.json()["error"]["details"]["available"], 50)

        b = self.get_batch(batch["id"])
        self.assertEqual((b["qty_available"], b["qty_reserved"]), (50, 0))
        req_now = self.client.get(f"{API}/transfer-requests/{req['id']}").json()
        self.assertEqual(req_now["status"], "SUBMITTED")
        entries = self.client.get(f"{API}/ledger?part_id={part['id']}").json()
        self.assertEqual([e for e in entries if e["entry_type"] == "RESERVE"], [])

        # 改用有货数量重试成功
        r = self.client.post(f"{API}/transfer-requests/{req['id']}/approve", json={"decision": "APPROVE", "qty": 50})
        self.assertEqual(r.status_code, 200, r.text)
        self.assert_reconciled(part["id"])

    def test_idempotency_key_replays_and_conflicts(self):
        """幂等键：重复建单/重复确认只生效一次；同键不同体报 409。"""
        wh_a, wh_b, part, batch = self.make_world(qty=100)

        body = {"source_warehouse_id": wh_a["id"], "dest_warehouse_id": wh_b["id"], "part_id": part["id"], "qty": 10}
        r1 = self.client.post(f"{API}/transfer-requests", json=body, headers={"Idempotency-Key": "K-1"})
        r2 = self.client.post(f"{API}/transfer-requests", json=body, headers={"Idempotency-Key": "K-1"})
        self.assertEqual(r1.json()["request_no"], r2.json()["request_no"])
        self.assertEqual(r2.headers.get("Idempotency-Replayed"), "true")
        self.assertEqual(len(self.client.get(f"{API}/transfer-requests").json()), 1)

        # 同键不同内容 -> 409
        r3 = self.client.post(
            f"{API}/transfer-requests",
            json={**body, "qty": 99},
            headers={"Idempotency-Key": "K-1"},
        )
        self.assertEqual(r3.status_code, 409)
        self.assertEqual(r3.json()["error"]["code"], "IDEMPOTENCY_CONFLICT")

        # 确认幂等：同一键重试不重复扣减
        req_id = r1.json()["id"]
        self.client.post(f"{API}/transfer-requests/{req_id}/submit")
        self.client.post(f"{API}/transfer-requests/{req_id}/approve", json={"decision": "APPROVE"})
        sh = self.client.post(f"{API}/transfer-requests/{req_id}/shipments", json={}).json()
        c1 = self.confirm(sh["id"], key="C-1")
        c2 = self.confirm(sh["id"], key="C-1")
        self.assertEqual(c2.headers.get("Idempotency-Replayed"), "true")
        b = self.get_batch(batch["id"])
        self.assertEqual((b["qty_reserved"], b["qty_on_hand"]), (0, 90))
        entries = self.client.get(f"{API}/ledger?part_id={part['id']}").json()
        self.assertEqual(len([e for e in entries if e["entry_type"] == "SHIP"]), 2)

    def test_confirm_is_naturally_idempotent_without_key(self):
        """不带幂等键时，重复确认也只扣减一次（状态机天然幂等）。"""
        wh_a, wh_b, part, batch = self.make_world(qty=100)
        req_id = self.make_approved_request(wh_a, wh_b, part, 25)
        sh = self.client.post(f"{API}/transfer-requests/{req_id}/shipments", json={}).json()
        self.assertEqual(self.confirm(sh["id"]).status_code, 200)
        again = self.confirm(sh["id"])
        self.assertEqual(again.status_code, 200)
        self.assertEqual(again.json()["status"], "IN_TRANSIT")
        b = self.get_batch(batch["id"])
        self.assertEqual((b["qty_reserved"], b["qty_on_hand"]), (0, 75))

    def test_state_machine_rejects_illegal_transitions(self):
        """非法迁移一律 409：未确认不能签收、不能重复审批、取消后不能确认。"""
        wh_a, wh_b, part, batch = self.make_world(qty=100)
        req_id = self.make_approved_request(wh_a, wh_b, part, 10)
        sh = self.client.post(f"{API}/transfer-requests/{req_id}/shipments", json={}).json()

        r = self.client.post(
            f"{API}/shipments/{sh['id']}/receipts",
            json={"lines": [{"shipment_line_id": sh["lines"][0]["id"], "qty_accepted": 1}]},
        )
        self.assertEqual(r.status_code, 409)  # PENDING 不能签收

        r = self.client.post(f"{API}/transfer-requests/{req_id}/approve", json={"decision": "APPROVE"})
        self.assertEqual(r.status_code, 409)  # 已审批不能重复审批

        self.client.post(f"{API}/shipments/{sh['id']}/cancel")
        r = self.confirm(sh["id"])
        self.assertEqual(r.status_code, 409)  # 已取消不能确认

        r = self.client.post(f"{API}/transfer-requests/{req_id}/submit")
        self.assertEqual(r.status_code, 409)  # APPROVED 不能回到提交

    def test_trace_tracks_every_unit(self):
        """追踪接口：批准量 = 预留 + 在途 + 已签收 + 已退回 + 已取消。"""
        wh_a, wh_b, part, batch = self.make_world(qty=100)
        req_id = self.make_approved_request(wh_a, wh_b, part, 70)

        sh1 = self.client.post(f"{API}/transfer-requests/{req_id}/shipments", json={"qty": 40}).json()
        self.confirm(sh1["id"])
        line1 = sh1["lines"][0]["id"]
        # 签收 15、拒收 5，剩 20 在途；另 30 未发，其中取消 10
        self.client.post(
            f"{API}/shipments/{sh1['id']}/receipts",
            json={"lines": [{"shipment_line_id": line1, "qty_accepted": 15, "qty_rejected": 5}]},
        )
        r = self.client.post(f"{API}/transfer-requests/{req_id}/cancel", json={"qty": 10})
        self.assertEqual(r.status_code, 200, r.text)

        trace = self.client.get(f"{API}/transfer-requests/{req_id}/trace").json()
        by_bucket = {}
        for p in trace["positions"]:
            by_bucket[p["bucket"]] = by_bucket.get(p["bucket"], 0) + p["qty"]
        self.assertEqual(by_bucket.get("IN_TRANSIT"), 20)
        self.assertEqual(by_bucket.get("RECEIVED"), 15)
        self.assertEqual(by_bucket.get("RETURNED_TO_SOURCE"), 5)
        self.assertEqual(by_bucket.get("RESERVED"), 20)
        self.assertEqual(by_bucket.get("CANCELLED"), 10)
        self.assertTrue(trace["conservation"]["balanced"])
        self.assertEqual(trace["conservation"]["accounted"], 70)
        # 在途位置携带来源批次与装运单号
        in_transit = [p for p in trace["positions"] if p["bucket"] == "IN_TRANSIT"]
        self.assertEqual(in_transit[0]["batch_no"], "B-001")
        self.assertEqual(in_transit[0]["shipment_no"], sh1["shipment_no"])
        self.assert_reconciled(part["id"])


class ConcurrencyTest(unittest.TestCase):
    """服务层并发：多线程共用文件型 SQLite，验证不超卖、确认只扣一次、崩溃原子性。"""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.engine = make_engine(f"sqlite:///{Path(self.tmp.name) / 'c.sqlite3'}")
        init_schema(self.engine)
        self.factory = make_session_factory(self.engine)
        with session_scope(self.factory) as s:
            self.wh_a = services.create_warehouse(s, "WH-A", "工厂A").id
            self.wh_b = services.create_warehouse(s, "WH-B", "工厂B").id
            self.part = services.create_part(s, "P-1", "关键轴承").id
            self.batch = services.inbound_batch(s, self.wh_a, self.part, "B-001", 100).id

    def tearDown(self) -> None:
        self.engine.dispose()
        self.tmp.cleanup()

    def run_parallel(self, n: int, fn) -> list:
        """并发执行 fn(i)，带瞬时锁重试（SQLite 写串行化的标准客户端行为）。"""
        results: list = [None] * n

        def worker(i: int) -> None:
            for _ in range(50):
                try:
                    with session_scope(self.factory) as s:
                        results[i] = fn(s, i)
                    return
                except OperationalError:
                    time.sleep(0.01)  # SQLite 写串行化：瞬时锁冲突重试
                except DomainError as e:
                    results[i] = e  # 领域错误是预期结果之一（如库存不足）
                    return
            raise AssertionError("重试耗尽")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return results

    def batch_state(self):
        with session_scope(self.factory) as s:
            b = s.get(InventoryBatch, self.batch)
            return b.qty_available, b.qty_reserved, b.qty_on_hand

    def make_submitted_request(self, qty: int) -> int:
        with session_scope(self.factory) as s:
            req = services.create_transfer_request(
                s,
                source_warehouse_id=self.wh_a,
                dest_warehouse_id=self.wh_b,
                part_id=self.part,
                qty=qty,
            )
            services.submit_request(s, req.id)
            return req.id

    def test_concurrent_approvals_never_oversell(self):
        """10 个工厂同时申请 30 件、库存仅 100：恰好 3 个成功，绝不超卖。"""
        req_ids = [self.make_submitted_request(30) for _ in range(10)]

        def approve(s, i):
            return services.approve_request(s, req_ids[i], decision="APPROVE")

        outcomes = self.run_parallel(10, approve)
        succeeded = [o for o in outcomes if not isinstance(o, DomainError)]
        failed = [o for o in outcomes if isinstance(o, DomainError)]
        self.assertEqual(len(succeeded), 3)
        self.assertTrue(all(e.code == "INSUFFICIENT_STOCK" for e in failed))
        self.assertEqual(self.batch_state(), (10, 90, 100))
        with session_scope(self.factory) as s:
            from transfer_backend.ledger import reconcile_part

            self.assertTrue(reconcile_part(s, self.part)["balanced"])

    def test_concurrent_confirms_deduct_exactly_once(self):
        """5 个线程同时确认同一装运单：库存只扣一次，台账只有一对 SHIP。"""
        req_id = self.make_submitted_request(50)
        with session_scope(self.factory) as s:
            services.approve_request(s, req_id, decision="APPROVE")
            sh = services.create_shipment(s, req_id)
            shipment_id = sh.id

        def confirm(s, _i):
            try:
                return services.confirm_shipment(s, shipment_id)
            except DomainError as e:
                # 并发确认冲突：重读状态，已在途即幂等成功（客户端标准重试模式）
                if e.code == "INVALID_STATE":
                    with session_scope(self.factory) as s2:
                        if services.get_shipment(s2, shipment_id).status == "IN_TRANSIT":
                            return "idempotent-ok"
                raise

        outcomes = self.run_parallel(5, confirm)
        self.assertEqual(len(outcomes), 5)  # 全部收敛到成功
        self.assertFalse(any(isinstance(o, DomainError) for o in outcomes), outcomes)
        self.assertEqual(self.batch_state(), (50, 0, 50))
        with session_scope(self.factory) as s:
            ships = s.execute(select(func.count()).select_from(LedgerEntry).where(LedgerEntry.entry_type == "SHIP")).scalar_one()
            self.assertEqual(ships, 2)  # 一对平衡分录，绝无重复发运
            req = services.get_request(s, req_id)
            self.assertEqual(req.qty_shipped, 50)

    def test_crash_between_deduct_and_record_rolls_back(self):
        """模拟「扣了库存还没写在途就崩溃」：整体回滚，重试后只生效一次。"""
        req_id = self.make_submitted_request(40)
        with session_scope(self.factory) as s:
            services.approve_request(s, req_id, decision="APPROVE")
            sh = services.create_shipment(s, req_id)
            shipment_id = sh.id
        self.assertEqual(self.batch_state(), (60, 40, 100))

        def boom(point: str) -> None:
            if point == "confirm_shipment.after_updates":
                raise RuntimeError("模拟进程崩溃")

        services.FAULT_HOOKS.append(boom)
        try:
            with self.assertRaises(RuntimeError):
                with session_scope(self.factory) as s:
                    services.confirm_shipment(s, shipment_id)
        finally:
            services.FAULT_HOOKS.clear()

        # 崩溃后无任何中间态：库存未扣、无 SHIP 分录、装运单仍待确认
        self.assertEqual(self.batch_state(), (60, 40, 100))
        with session_scope(self.factory) as s:
            ship_entries = s.execute(
                select(func.count()).select_from(LedgerEntry).where(LedgerEntry.entry_type == "SHIP")
            ).scalar_one()
            self.assertEqual(ship_entries, 0)
            self.assertEqual(s.get(Shipment, shipment_id).status, "PENDING")

        # 重试：成功且只扣一次
        with session_scope(self.factory) as s:
            services.confirm_shipment(s, shipment_id)
        self.assertEqual(self.batch_state(), (60, 0, 60))
        with session_scope(self.factory) as s:
            from transfer_backend.ledger import reconcile_part

            self.assertTrue(reconcile_part(s, self.part)["balanced"])

    def test_crash_during_receipt_rolls_back(self):
        """签收中途崩溃：目的仓不入账、在途不减少，重试幂等。"""
        req_id = self.make_submitted_request(30)
        with session_scope(self.factory) as s:
            services.approve_request(s, req_id, decision="APPROVE")
            sh = services.create_shipment(s, req_id)
            shipment_id = sh.id
            services.confirm_shipment(s, shipment_id)
            line_id = sh.lines[0].id

        def boom(point: str) -> None:
            if point == "receive.after_updates":
                raise RuntimeError("模拟进程崩溃")

        services.FAULT_HOOKS.append(boom)
        try:
            with self.assertRaises(RuntimeError):
                with session_scope(self.factory) as s:
                    services.receive_shipment(s, shipment_id, lines=[{"shipment_line_id": line_id, "qty_accepted": 30}])
        finally:
            services.FAULT_HOOKS.clear()

        with session_scope(self.factory) as s:
            sh = services.get_shipment(s, shipment_id)
            self.assertEqual(sh.status, "IN_TRANSIT")
            self.assertEqual(sh.lines[0].qty_in_transit, 30)
            dest = s.execute(select(InventoryBatch).where(InventoryBatch.warehouse_id == self.wh_b)).scalars().all()
            self.assertEqual(dest, [])

        with session_scope(self.factory) as s:
            services.receive_shipment(s, shipment_id, lines=[{"shipment_line_id": line_id, "qty_accepted": 30}])
        with session_scope(self.factory) as s:
            from transfer_backend.ledger import reconcile_part

            self.assertTrue(reconcile_part(s, self.part)["balanced"])
            req = services.get_request(s, req_id)
            self.assertEqual(req.qty_received, 30)


if __name__ == "__main__":
    unittest.main()
