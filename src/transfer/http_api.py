"""基于标准库 ``http.server`` 的 JSON 接口。

零第三方依赖。路由::

    GET  /healthz
    POST /batches                         登记批次 {warehouse, part_no, batch_no, opening_quantity?}
    POST /batches/restock                 补货 {warehouse, part_no, batch_no, quantity}
    GET  /batches?warehouse=&part_no=

    POST /requests                        创建申请
    GET  /requests[?status=]
    GET  /requests/{id}
    POST /requests/{id}/submit
    POST /requests/{id}/approve
    POST /requests/{id}/reject
    POST /requests/{id}/revise
    POST /requests/{id}/cancel
    POST /requests/{id}/close

    POST /requests/{id}/shipments         计划装运 {lines:[[batch_no,qty],...], deadline?}
    GET  /requests/{id}/shipments
    POST /shipments/{id}/confirm          原子 可用→在途
    POST /shipments/{id}/cancel           取消计划
    POST /shipments/{id}/receive          签收 {items, dest_batch_no?}
    POST /shipments/{id}/reject           拒收 {items, reason?}
    POST /shipments/{id}/reroute          改道 {new_destination, reason?}
    POST /shipments/{id}/timeout          强制超时退回
    POST /sweep-timeouts[?now=]           超时巡检

    GET  /requests/{id}/trace             逐来源批次追踪数量位置
    GET  /shipments/{id}/trace
    GET  /requests/{id}/ledger            平衡分录
    GET  /audit                           守恒审计
"""
from __future__ import annotations

import json
import re
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from .errors import TransferError
from .models import asdict
from .service import TransferService
from .store import Store


def create_app(service: TransferService) -> type[BaseHTTPRequestHandler]:
    """生成绑定了给定服务实例的 Handler 类。"""

    class Handler(BaseHTTPRequestHandler):
        server_version = "TransferService/1.0"
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt: str, *args: object) -> None:  # noqa: A003
            return

        def handle_one_request(self) -> None:  # noqa: N802
            try:
                super().handle_one_request()
            except (BrokenPipeError, ConnectionResetError):
                # 客户端已断连，丢弃本次响应即可
                self.close_connection = True

        # ---- 基础读写 ---- #
        def _send_json(self, payload: object, status: int = 200) -> None:
            body = json.dumps(payload, ensure_ascii=False, default=asdict).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> dict:
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0:
                return {}
            raw = self.rfile.read(length)
            try:
                value = json.loads(raw.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise _HttpError(400, f"请求体不是合法 JSON：{exc}") from exc
            if not isinstance(value, dict):
                raise _HttpError(400, "请求体必须是 JSON 对象")
            return value

        # ---- 路由 ---- #
        def do_GET(self) -> None:  # noqa: N802
            self._dispatch("GET")

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch("POST")

        def _dispatch(self, method: str) -> None:
            parts = urlsplit(self.path)
            path = parts.path.rstrip("/") or "/"
            query = parse_qs(parts.query)
            try:
                handler, kwargs = self._match(method, path, query)
                handler(**kwargs)
            except _HttpError as exc:
                self._send_json({"error": exc.message}, exc.status)
            except TransferError as exc:
                self._send_json({"error": str(exc)}, 422)
            except Exception as exc:  # 防御性兜底
                self._send_json({"error": f"内部错误：{exc}"}, 500)

        def _match(
            self, method: str, path: str, query: dict[str, list[str]]
        ) -> tuple[Callable[..., None], dict]:
            body = self._read_json if method == "POST" else (lambda: {})

            def q(name: str, default: str | None = None) -> str | None:
                return query.get(name, [default])[0]

            if method == "GET" and path == "/healthz":
                return (lambda: self._send_json({"status": "ok"})), {}

            # 批次
            if method == "POST" and path == "/batches":
                return self._create_batch, {"body": body()}
            if method == "POST" and path == "/batches/restock":
                return self._restock, {"body": body()}
            if method == "GET" and path == "/batches":
                return (
                    lambda: self._send_json(
                        service.list_batches(q("warehouse"), q("part_no"))
                    )
                ), {}

            # 申请单集合 / 巡检 / 审计
            if method == "POST" and path == "/requests":
                return self._create_request, {"body": body()}
            if method == "GET" and path == "/requests":
                return (lambda: self._send_json(service.list_requests(q("status")))), {}
            if method == "POST" and path == "/sweep-timeouts":
                return (
                    lambda: self._send_json(
                        {"timed_out_shipment_ids": service.sweep_timeouts(q("now"))}
                    )
                ), {}
            if method == "GET" and path == "/audit":
                problems = service.audit()
                return (
                    lambda: self._send_json(
                        {"conserved": not problems, "problems": problems},
                        200 if not problems else 409,
                    )
                ), {}

            m = re.fullmatch(r"/requests/(\d+)(/[a-z-]+)?", path)
            if m:
                rid = int(m.group(1))
                sub = m.group(2) or ""
                if method == "GET" and sub == "":
                    return (lambda: self._send_json(service.get_request(rid))), {}
                post_map = {
                    "/submit": lambda: service.submit_request(rid),
                    "/approve": lambda: service.approve_request(rid, body().get("note", "")),
                    "/reject": lambda: service.reject_request(rid, body().get("reason", "")),
                    "/cancel": lambda: service.cancel_request(rid, body().get("reason", "")),
                    "/close": lambda: service.close_request(rid, body().get("note", "")),
                    "/shipments": lambda: self._create_shipment(rid, body()),
                }
                if method == "POST" and sub == "/revise":
                    return self._revise, {"rid": rid, "body": body()}
                if method == "POST" and sub in post_map:
                    return self._run_action, {"fn": post_map[sub]}
                if method == "GET" and sub == "/shipments":
                    return (lambda: self._send_json(service.list_shipments(rid))), {}
                if method == "GET" and sub == "/trace":
                    return (lambda: self._send_json(service.trace_quantity(request_id=rid))), {}
                if method == "GET" and sub == "/ledger":
                    return (
                        lambda: self._send_json(
                            service.list_ledger_entries(request_id=rid)
                        )
                    ), {}

            m = re.fullmatch(r"/shipments/(\d+)(/[a-z-]+)?", path)
            if m:
                sid = int(m.group(1))
                sub = m.group(2) or ""
                if method == "GET" and sub == "":
                    return (lambda: self._send_json(service.get_shipment(sid))), {}
                if method == "GET" and sub == "/trace":
                    return (lambda: self._send_json(service.trace_quantity(shipment_id=sid))), {}
                if method == "POST":
                    actions = {
                        "/confirm": lambda: service.confirm_shipment(sid),
                        "/cancel": lambda: service.cancel_planned_shipment(sid),
                        "/timeout": lambda: self._force_timeout(sid),
                        "/receive": lambda: self._receive(sid, body()),
                        "/reject": lambda: self._reject(sid, body()),
                        "/reroute": lambda: self._reroute(sid, body()),
                    }
                    if sub in actions:
                        return self._run_action, {"fn": actions[sub]}

            raise _HttpError(404, f"未找到路由：{method} {path}")

        # ---- 动作包装 ---- #
        def _run_action(self, fn: Callable[[], object]) -> None:
            result = fn()
            if result is None:
                self._send_json({"ok": True})
            else:
                self._send_json({"ok": True, "result": result})

        def _force_timeout(self, sid: int) -> None:
            changed = service.force_timeout_shipment(sid)
            self._send_json({"ok": True, "timed_out": changed})

        # ---- 批次端点 ---- #
        def _create_batch(self, body: dict) -> None:
            bid = service.create_batch(
                body["warehouse"],
                body["part_no"],
                body["batch_no"],
                int(body.get("opening_quantity", 0)),
            )
            self._send_json({"id": bid}, 201)

        def _restock(self, body: dict) -> None:
            service.restock(body["warehouse"], body["part_no"], body["batch_no"], int(body["quantity"]))
            self._send_json({"ok": True})

        # ---- 申请端点 ---- #
        def _create_request(self, body: dict) -> None:
            rid = service.create_request(
                body["part_no"],
                int(body["quantity"]),
                body["from_warehouse"],
                body["to_warehouse"],
                body.get("note", ""),
            )
            self._send_json({"id": rid}, 201)

        def _revise(self, rid: int, body: dict) -> None:
            service.revise_request(
                rid,
                int(body["quantity"]) if "quantity" in body else None,
                body.get("to_warehouse"),
                body.get("note"),
            )
            self._send_json({"ok": True})

        # ---- 装运端点 ---- #
        def _create_shipment(self, rid: int, body: dict) -> None:
            sid = service.create_shipment(
                rid,
                body.get("lines", []),
                body.get("deadline"),
            )
            self._send_json({"id": sid}, 201)

        def _receive(self, sid: int, body: dict) -> None:
            qty = service.receive_shipment(
                sid,
                body.get("items", []),
                body.get("dest_batch_no"),
                body.get("note", ""),
            )
            self._send_json({"ok": True, "received_quantity": qty})

        def _reject(self, sid: int, body: dict) -> None:
            qty = service.reject_shipment(sid, body.get("items", []), body.get("reason", ""))
            self._send_json({"ok": True, "rejected_quantity": qty})

        def _reroute(self, sid: int, body: dict) -> None:
            service.reroute_shipment(sid, body["new_destination"], body.get("reason", ""))
            self._send_json({"ok": True})

    return Handler


class _HttpError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


def serve(db_path: str = ":memory:", host: str = "127.0.0.1", port: int = 8080) -> None:
    store = Store(db_path)
    service = TransferService(store)
    handler = create_app(service)
    httpd = ThreadingHTTPServer((host, port), handler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        store.close()


if __name__ == "__main__":  # pragma: no cover
    import argparse

    parser = argparse.ArgumentParser(description="多仓库存调拨 HTTP 服务")
    parser.add_argument("--db", default=":memory:")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    serve(args.db, args.host, args.port)
