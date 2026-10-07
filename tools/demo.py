"""端到端场景演示：多工厂共享关键零件的调拨全流程。

运行：python3 tools/demo.py

场景覆盖：并发申请竞争、原子出库、部分装运、部分签收、改道、
拒收补偿、超时退回与全链路数量追踪。
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from transfer import InsufficientStockError, TransferService  # noqa: E402
from transfer.enums import RequestStatus  # noqa: E402
from transfer.store import Store  # noqa: E402


def show(svc: TransferService, title: str) -> None:
    print(f"\n=== {title} ===")
    for b in svc.list_batches():
        print(f"  批次 {b.warehouse}/{b.part_no}/{b.batch_no}: "
              f"可用={b.available} 在途={b.in_transit}")
    for r in svc.list_requests():
        print(f"  申请 #{r.id} {r.part_no} x{r.quantity} "
              f"{r.from_warehouse}->{r.to_warehouse} 状态={r.status} "
              f"已发={r.shipped_qty} 已收={r.received_qty}")
    problems = svc.audit()
    print("  守恒审计：" + ("通过" if not problems else "违例 " + str(problems)))


def main() -> None:
    svc = TransferService(Store(":memory:"))

    # 关键零件 P1 只在上海仓有两批库存
    svc.create_batch("WH_SH", "P1", "B2026-01", 100)
    svc.create_batch("WH_SH", "P1", "B2026-02", 20)

    # 两个缺料工厂同时发起调拨
    r1 = svc.create_request("P1", 80, "WH_SH", "WH_BJ", "北京工厂缺料")
    r2 = svc.create_request("P1", 60, "WH_SH", "WH_GZ", "广州工厂缺料")
    for rid in (r1, r2):
        svc.submit_request(rid)
        svc.approve_request(rid)

    s1 = svc.create_shipment(r1, [("B2026-01", 80)],
                             deadline=datetime.now() + timedelta(days=2))
    s2 = svc.create_shipment(r2, [("B2026-01", 60)])

    # 并发确认：库存只够一单
    try:
        svc.confirm_shipment(s1)
        print("装运 #1 确认成功（80 件 B2026-01 在途）")
    except InsufficientStockError as e:
        print("装运 #1 失败：", e)
    try:
        svc.confirm_shipment(s2)
        print("装运 #2 确认成功")
    except InsufficientStockError as e:
        print("装运 #2 确认失败（库存被先到者占用）：", e)
        # 失败方改从两批剩余库存重新配货，幂等安全
        s2b = svc.create_shipment(r2, [("B2026-01", 20), ("B2026-02", 20)])
        svc.confirm_shipment(s2b)
        print("装运 #2 改配 40 件（部分装运）后确认成功")

    show(svc, "并发竞争与原子出库后")

    # 北京工厂部分签收 50 件，剩余改道天津工厂
    svc.receive_shipment(s1, [("B2026-01", 50)], "RCV-BJ-1")
    svc.reroute_shipment(s1, "WH_TJ", "北京产能调整")
    svc.receive_shipment(s1, [("B2026-01", 30)], "RCV-TJ-1")

    # 广州工厂拒收其中 20 件（B2026-02 批次外观问题），退回来源仓
    svc.reject_shipment(s2b, [("B2026-02", 20)], "包装破损")
    show(svc, "签收 / 改道 / 拒收补偿后")

    # 全链路追踪
    print("\n=== 申请 #1 每件数量位置追踪 ===")
    for t in svc.trace_quantity(request_id=r1):
        locs = ",".join(t.in_transit_locations) or "-"
        print(f"  来源批次 {t.source_batch_no}: 出库={t.shipped_qty} "
              f"签收={t.received_qty} 拒收={t.rejected_qty} "
              f"在途={t.in_transit_qty}(锚定 {locs})")
        for rc in t.receipts:
            tag = "拒收退回" if rc.rejected else "签收入库"
            print(f"    └ {tag} {rc.quantity} 件 -> {rc.dest_batch_no or rc.source_batch_no}")

    print("\n=== 复式分录（申请 #1）===")
    for e in svc.list_ledger_entries(request_id=r1):
        detail = "; ".join(
            f"{l.warehouse}/{l.batch_no}.{l.bucket} 借{l.debit}/贷{l.credit}"
            for l in e.lines
        )
        print(f"  #{e.id} [{e.entry_type}] {detail} | {e.note}")

    print("\n审计最终状态：", "通过，库存守恒" if not svc.audit() else svc.audit())
    svc.store.close()


if __name__ == "__main__":
    main()
