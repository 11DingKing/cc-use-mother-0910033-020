"""HTTP 接口层（FastAPI）。

所有写接口接受 `Idempotency-Key` 请求头：幂等记录与业务写入同事务提交，
重试安全；响应头 `Idempotency-Replayed: true` 表示本次为回放。

事务边界：TransactionRoute 在路由处理完成、响应发送之前提交 ——
客户端收到响应时数据必然已落库（read-your-writes）。
（FastAPI 的 yield 依赖清理运行在响应发送之后，若在那里提交，
后续请求可能读到未提交的旧状态。）
"""
from __future__ import annotations

import os
from collections.abc import Callable

from fastapi import Depends, FastAPI, Query, Request, Response
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from . import ledger, services
from .database import init_schema, make_engine, make_session_factory
from .errors import DomainError
from .idempotency import IdemContext, idem_dependency, run_idempotent
from .models import EventLog, InventoryBatch, LedgerEntry, Shipment, TransferRequest
from .schemas import (
    ApprovalIn,
    ApprovalOut,
    BatchCreate,
    BatchOut,
    CancelIn,
    EventOut,
    ExtendIn,
    LedgerEntryOut,
    PartCreate,
    PartOut,
    ReceiptCreate,
    ReceiptOut,
    ReconciliationOut,
    RerouteIn,
    ReturnIn,
    ShipmentCreate,
    ShipmentOut,
    TimeoutSweepIn,
    TraceOut,
    TransferRequestCreate,
    TransferRequestOut,
    WarehouseCreate,
    WarehouseOut,
)

API = "/api/v1"


class TransactionRoute(APIRoute):
    """每个请求一个事务：路由处理成功后、响应发送前提交；异常则回滚。

    会话由 get_session 依赖创建并挂在 request.state 上，这里统一收尾。
    """

    def get_route_handler(self) -> Callable:
        original = super().get_route_handler()

        async def handler(request: Request) -> Response:
            try:
                response = await original(request)
            except BaseException:
                self._finish_session(request, commit=False)
                raise
            self._finish_session(request, commit=True)
            return response

        return handler

    @staticmethod
    def _finish_session(request: Request, *, commit: bool) -> None:
        session = getattr(request.state, "db_session", None)
        if session is None:
            return
        try:
            if commit:
                session.commit()
            else:
                session.rollback()
        except BaseException:
            session.rollback()
            raise
        finally:
            session.close()


def create_app(database_url: str | None = None) -> FastAPI:
    engine = make_engine(database_url or os.environ.get("TRANSFER_DATABASE_URL"))
    init_schema(engine)
    factory = make_session_factory(engine)

    app = FastAPI(title="多仓库存调拨", version="0.1.0")
    app.router.route_class = TransactionRoute
    app.state.engine = engine
    app.state.session_factory = factory

    def get_session(request: Request) -> Session:
        session = factory()
        request.state.db_session = session
        return session

    @app.exception_handler(DomainError)
    async def domain_error_handler(_req: Request, exc: DomainError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.http_status,
            content={"error": {"code": exc.code, "message": exc.message, "details": exc.details}},
        )

    @app.exception_handler(IntegrityError)
    async def integrity_error_handler(_req: Request, exc: IntegrityError) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content={"error": {"code": "UNIQUE_CONFLICT", "message": "唯一性冲突（可能是重复提交或编号已存在）", "details": {}}},
        )

    def replay_header(response: Response, replayed: bool) -> None:
        if replayed:
            response.headers["Idempotency-Replayed"] = "true"

    # ------------------------------------------------------------ 健康与档案

    @app.get(f"{API}/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.post(f"{API}/warehouses", response_model=WarehouseOut, status_code=201)
    def create_warehouse(body: WarehouseCreate, session: Session = Depends(get_session)):
        return services.create_warehouse(session, body.code, body.name)

    @app.get(f"{API}/warehouses", response_model=list[WarehouseOut])
    def list_warehouses(session: Session = Depends(get_session)):
        from .models import Warehouse

        return list(session.execute(select(Warehouse).order_by(Warehouse.id)).scalars())

    @app.post(f"{API}/parts", response_model=PartOut, status_code=201)
    def create_part(body: PartCreate, session: Session = Depends(get_session)):
        return services.create_part(session, body.sku, body.name)

    @app.post(f"{API}/batches", response_model=BatchOut, status_code=201)
    def create_batch(
        body: BatchCreate,
        response: Response,
        session: Session = Depends(get_session),
        idem: IdemContext = Depends(idem_dependency),
    ):
        out, replayed = run_idempotent(
            session,
            idem,
            response_model=BatchOut,
            fn=lambda: BatchOut.model_validate(
                services.inbound_batch(session, body.warehouse_id, body.part_id, body.batch_no, body.qty)
            ),
        )
        replay_header(response, replayed)
        return out

    @app.get(f"{API}/batches", response_model=list[BatchOut])
    def list_batches(
        warehouse_id: int | None = Query(default=None),
        part_id: int | None = Query(default=None),
        session: Session = Depends(get_session),
    ):
        stmt = select(InventoryBatch).order_by(InventoryBatch.id)
        if warehouse_id is not None:
            stmt = stmt.where(InventoryBatch.warehouse_id == warehouse_id)
        if part_id is not None:
            stmt = stmt.where(InventoryBatch.part_id == part_id)
        return list(session.execute(stmt).scalars())

    @app.get(f"{API}/batches/{{batch_id}}/ledger", response_model=list[LedgerEntryOut])
    def batch_ledger(batch_id: int, session: Session = Depends(get_session)):
        return list(
            session.execute(
                select(LedgerEntry).where(LedgerEntry.batch_id == batch_id).order_by(LedgerEntry.id)
            ).scalars()
        )

    # ------------------------------------------------------------ 调拨申请

    @app.post(f"{API}/transfer-requests", response_model=TransferRequestOut, status_code=201)
    def create_transfer_request(
        body: TransferRequestCreate,
        response: Response,
        session: Session = Depends(get_session),
        idem: IdemContext = Depends(idem_dependency),
    ):
        out, replayed = run_idempotent(
            session,
            idem,
            response_model=TransferRequestOut,
            fn=lambda: services.request_out(
                session,
                services.create_transfer_request(
                    session,
                    source_warehouse_id=body.source_warehouse_id,
                    dest_warehouse_id=body.dest_warehouse_id,
                    part_id=body.part_id,
                    qty=body.qty,
                    reason=body.reason,
                    created_by=body.created_by,
                    idem_key=idem.key,
                )
            ),
        )
        replay_header(response, replayed)
        return out

    @app.get(f"{API}/transfer-requests", response_model=list[TransferRequestOut])
    def list_transfer_requests(
        status: str | None = Query(default=None),
        part_id: int | None = Query(default=None),
        session: Session = Depends(get_session),
    ):
        stmt = select(TransferRequest).order_by(TransferRequest.id)
        if status:
            stmt = stmt.where(TransferRequest.status == status)
        if part_id is not None:
            stmt = stmt.where(TransferRequest.part_id == part_id)
        return [services.request_out(session, r) for r in session.execute(stmt).scalars()]

    @app.get(f"{API}/transfer-requests/{{request_id}}", response_model=TransferRequestOut)
    def get_transfer_request(request_id: int, session: Session = Depends(get_session)):
        return services.request_out(session, services.get_request(session, request_id))

    @app.post(f"{API}/transfer-requests/{{request_id}}/submit", response_model=TransferRequestOut)
    def submit_transfer_request(request_id: int, session: Session = Depends(get_session)):
        return services.request_out(session, services.submit_request(session, request_id))

    @app.post(f"{API}/transfer-requests/{{request_id}}/approve", response_model=ApprovalOut)
    def approve_transfer_request(
        request_id: int,
        body: ApprovalIn,
        response: Response,
        session: Session = Depends(get_session),
        idem: IdemContext = Depends(idem_dependency),
    ):
        out, replayed = run_idempotent(
            session,
            idem,
            response_model=ApprovalOut,
            fn=lambda: ApprovalOut.model_validate(
                services.approve_request(
                    session,
                    request_id,
                    decision=body.decision,
                    qty=body.qty,
                    approver=body.approver,
                    comment=body.comment,
                )
            ),
        )
        replay_header(response, replayed)
        return out

    @app.post(f"{API}/transfer-requests/{{request_id}}/cancel", response_model=TransferRequestOut)
    def cancel_transfer_request(request_id: int, body: CancelIn, session: Session = Depends(get_session)):
        return services.request_out(
            session,
            services.cancel_request(session, request_id, qty=body.qty, actor=body.actor, reason=body.reason),
        )

    @app.get(f"{API}/transfer-requests/{{request_id}}/trace", response_model=TraceOut)
    def trace_transfer_request(request_id: int, session: Session = Depends(get_session)):
        return services.trace_request(session, request_id)

    @app.get(f"{API}/transfer-requests/{{request_id}}/events", response_model=list[EventOut])
    def transfer_request_events(request_id: int, session: Session = Depends(get_session)):
        services.get_request(session, request_id)
        return services.list_events(session, "request", request_id)

    # ------------------------------------------------------------ 装运

    @app.post(f"{API}/transfer-requests/{{request_id}}/shipments", response_model=ShipmentOut, status_code=201)
    def create_shipment(
        request_id: int,
        body: ShipmentCreate,
        response: Response,
        session: Session = Depends(get_session),
        idem: IdemContext = Depends(idem_dependency),
    ):
        lines = [ln.model_dump() for ln in body.lines] if body.lines else None
        out, replayed = run_idempotent(
            session,
            idem,
            response_model=ShipmentOut,
            fn=lambda: services.shipment_out(
                services.create_shipment(
                    session,
                    request_id,
                    qty=body.qty,
                    lines=lines,
                    carrier=body.carrier,
                    tracking_no=body.tracking_no,
                    eta=body.eta,
                    idem_key=idem.key,
                )
            ),
        )
        replay_header(response, replayed)
        return out

    @app.get(f"{API}/shipments", response_model=list[ShipmentOut])
    def list_shipments(
        status: str | None = Query(default=None),
        session: Session = Depends(get_session),
    ):
        stmt = select(Shipment).order_by(Shipment.id)
        if status:
            stmt = stmt.where(Shipment.status == status)
        return [services.shipment_out(s) for s in session.execute(stmt).scalars()]

    @app.get(f"{API}/shipments/{{shipment_id}}", response_model=ShipmentOut)
    def get_shipment(shipment_id: int, session: Session = Depends(get_session)):
        return services.shipment_out(services.get_shipment(session, shipment_id))

    @app.post(f"{API}/shipments/{{shipment_id}}/confirm", response_model=ShipmentOut)
    def confirm_shipment(
        shipment_id: int,
        response: Response,
        session: Session = Depends(get_session),
        idem: IdemContext = Depends(idem_dependency),
    ):
        out, replayed = run_idempotent(
            session,
            idem,
            response_model=ShipmentOut,
            fn=lambda: services.shipment_out(services.confirm_shipment(session, shipment_id)),
        )
        replay_header(response, replayed)
        return out

    @app.post(f"{API}/shipments/{{shipment_id}}/cancel", response_model=ShipmentOut)
    def cancel_shipment(shipment_id: int, session: Session = Depends(get_session)):
        return services.shipment_out(services.cancel_shipment(session, shipment_id))

    @app.post(f"{API}/shipments/{{shipment_id}}/receipts", response_model=ReceiptOut, status_code=201)
    def receive_shipment(
        shipment_id: int,
        body: ReceiptCreate,
        response: Response,
        session: Session = Depends(get_session),
        idem: IdemContext = Depends(idem_dependency),
    ):
        lines = [ln.model_dump() for ln in body.lines]
        out, replayed = run_idempotent(
            session,
            idem,
            response_model=ReceiptOut,
            fn=lambda: services.receipt_out(
                services.receive_shipment(
                    session,
                    shipment_id,
                    lines=lines,
                    receiver=body.receiver,
                    note=body.note,
                    idem_key=idem.key,
                )
            ),
        )
        replay_header(response, replayed)
        return out

    @app.post(f"{API}/shipments/{{shipment_id}}/reroute", response_model=ShipmentOut)
    def reroute_shipment(shipment_id: int, body: RerouteIn, session: Session = Depends(get_session)):
        return services.shipment_out(
            services.reroute_shipment(
                session,
                shipment_id,
                new_dest_warehouse_id=body.new_dest_warehouse_id,
                new_eta=body.new_eta,
                actor=body.actor,
                reason=body.reason,
            )
        )

    @app.post(f"{API}/shipments/{{shipment_id}}/extend", response_model=ShipmentOut)
    def extend_shipment(shipment_id: int, body: ExtendIn, session: Session = Depends(get_session)):
        return services.shipment_out(
            services.extend_shipment_eta(session, shipment_id, new_eta=body.new_eta, actor=body.actor)
        )

    @app.post(f"{API}/shipments/{{shipment_id}}/return", response_model=ShipmentOut)
    def return_shipment(shipment_id: int, body: ReturnIn, session: Session = Depends(get_session)):
        return services.shipment_out(
            services.return_shipment(session, shipment_id, actor=body.actor, reason=body.reason)
        )

    @app.post(f"{API}/shipments/timeout-sweep", response_model=dict)
    def sweep_timeouts(body: TimeoutSweepIn, session: Session = Depends(get_session)):
        marked = services.timeout_sweep(session, now=body.now)
        return {"timed_out_shipment_ids": marked}

    @app.get(f"{API}/shipments/{{shipment_id}}/events", response_model=list[EventOut])
    def shipment_events(shipment_id: int, session: Session = Depends(get_session)):
        services.get_shipment(session, shipment_id)
        return services.list_events(session, "shipment", shipment_id)

    # ------------------------------------------------------------ 追踪与对账

    @app.get(f"{API}/ledger", response_model=list[LedgerEntryOut])
    def query_ledger(
        part_id: int | None = Query(default=None),
        warehouse_id: int | None = Query(default=None),
        shipment_id: int | None = Query(default=None),
        session: Session = Depends(get_session),
    ):
        stmt = select(LedgerEntry).order_by(LedgerEntry.id)
        if part_id is not None:
            stmt = stmt.where(LedgerEntry.part_id == part_id)
        if warehouse_id is not None:
            stmt = stmt.where(LedgerEntry.warehouse_id == warehouse_id)
        if shipment_id is not None:
            stmt = stmt.where(LedgerEntry.shipment_id == shipment_id)
        return list(session.execute(stmt).scalars())

    @app.get(f"{API}/parts/{{part_id}}/reconciliation", response_model=ReconciliationOut)
    def reconcile(part_id: int, session: Session = Depends(get_session)):
        return ledger.reconcile_part(session, part_id)

    @app.get(f"{API}/events", response_model=list[EventOut])
    def list_all_events(session: Session = Depends(get_session)):
        return list(session.execute(select(EventLog).order_by(EventLog.id)).scalars())

    return app


app = create_app()
