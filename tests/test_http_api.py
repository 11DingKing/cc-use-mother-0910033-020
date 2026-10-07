"""HTTP JSON 接口的端到端冒烟测试（真实起线程服务，urllib 调用）。"""
from __future__ import annotations

import json
import sys
import threading
import unittest
import urllib.error
import urllib.parse
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from transfer.http_api import create_app  # noqa: E402
from transfer.service import TransferService  # noqa: E402
from transfer.store import Store  # noqa: E402


class HttpApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Store(":memory:")
        self.service = TransferService(self.store)
        handler = create_app(self.service)
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)
        self.store.close()

    def call(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
        url = f"http://127.0.0.1:{self.port}{path}"
        data = None
        headers = {}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_end_to_end_transfer_with_partial_receive_and_reroute(self) -> None:
        self.assertEqual(self.call("GET", "/healthz")[1], {"status": "ok"})

        self.call("POST", "/batches",
                  {"warehouse": "WH_A", "part_no": "P1", "batch_no": "B1", "opening_quantity": 100})
        self.call("POST", "/batches",
                  {"warehouse": "WH_A", "part_no": "P1", "batch_no": "B2", "opening_quantity": 20})

        status, created = self.call("POST", "/requests", {
            "part_no": "P1", "quantity": 60,
            "from_warehouse": "WH_A", "to_warehouse": "WH_B",
        })
        self.assertEqual(status, 201)
        rid = created["id"]
        self.call("POST", f"/requests/{rid}/submit")
        self.call("POST", f"/requests/{rid}/approve", {"note": "紧急缺料"})

        status, ship = self.call("POST", f"/requests/{rid}/shipments",
                                 {"lines": [["B1", 40], ["B2", 20]]})
        self.assertEqual(status, 201)
        sid = ship["id"]

        # 重复确认：第二次必须 422
        self.assertEqual(self.call("POST", f"/shipments/{sid}/confirm")[0], 200)
        self.assertEqual(self.call("POST", f"/shipments/{sid}/confirm")[0], 422)

        # 部分签收
        code, body = self.call("POST", f"/shipments/{sid}/receive",
                               {"items": [["B1", 25]], "dest_batch_no": "R1"})
        self.assertEqual(code, 200)
        self.assertEqual(body["received_quantity"], 25)

        # 剩余改道到 WH_C 后签收
        self.call("POST", f"/shipments/{sid}/reroute", {"new_destination": "WH_C"})
        code, body = self.call("POST", f"/shipments/{sid}/receive",
                               {"items": [["B1", 15], ["B2", 20]], "dest_batch_no": "R2"})
        self.assertEqual(code, 200)

        code, req = self.call("GET", f"/requests/{rid}")
        self.assertEqual(req["status"], "已签收")

        code, traces = self.call("GET", f"/requests/{rid}/trace")
        self.assertEqual(code, 200)
        by_batch = {t["source_batch_no"]: t for t in traces}
        self.assertEqual(by_batch["B1"]["received_qty"], 40)
        self.assertEqual(by_batch["B2"]["receipts"][0]["dest_batch_no"], "R2")

        code, audit = self.call("GET", "/audit")
        self.assertEqual(code, 200)
        self.assertTrue(audit["conserved"])

        # 分录可按申请追踪
        code, entries = self.call("GET", f"/requests/{rid}/ledger")
        kinds = {e["entry_type"] for e in entries}
        self.assertIn("出库转在途", kinds)
        self.assertIn("改道", kinds)

    def test_concurrent_confirm_one_wins(self) -> None:
        self.call("POST", "/batches",
                  {"warehouse": "WH_A", "part_no": "P1", "batch_no": "B1", "opening_quantity": 100})
        ids = []
        for dest in ("WH_B", "WH_C"):
            _, created = self.call("POST", "/requests", {
                "part_no": "P1", "quantity": 80,
                "from_warehouse": "WH_A", "to_warehouse": dest,
            })
            rid = created["id"]
            self.call("POST", f"/requests/{rid}/submit")
            self.call("POST", f"/requests/{rid}/approve")
            _, ship = self.call("POST", f"/requests/{rid}/shipments", {"lines": [["B1", 80]]})
            ids.append(ship["id"])

        results = []

        def confirm(sid: int) -> None:
            results.append(self.call("POST", f"/shipments/{sid}/confirm")[0])

        t1 = threading.Thread(target=confirm, args=(ids[0],))
        t2 = threading.Thread(target=confirm, args=(ids[1],))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        self.assertEqual(sorted(results), [200, 422])

        _, audit = self.call("GET", "/audit")
        self.assertTrue(audit["conserved"])

    def test_unknown_route_404(self) -> None:
        self.assertEqual(self.call("GET", "/nope")[0], 404)
        self.assertEqual(self.call("GET", "/requests/999")[0], 422)


if __name__ == "__main__":
    unittest.main()
