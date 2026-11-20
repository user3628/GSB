from __future__ import annotations

import logging
import os
import platform
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app import __version__
from app.errors import (
    E_BATCH_INCOMPLETE,
    E_INTERNAL_ERROR,
    E_INVALID_ARGUMENT,
    BusinessError,
)
from app.models import (
    BatchStatus,
    PolicySnapshot,
    RouteResolveRequest,
    SimulationSubmit,
    TransitionRequest,
)
from app.service import WorkbenchService
from app.storage import Storage


logger = logging.getLogger("mesh_simulator")
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "workbench.db"


def _default_db_path() -> Path:
    return Path(os.environ.get("SIM_WORKBENCH_DB", DEFAULT_DB_PATH)).resolve()


def create_app(db_path: str | Path | None = None) -> FastAPI:
    resolved_db_path = Path(db_path).resolve() if db_path is not None else _default_db_path()

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        storage = Storage(resolved_db_path)
        service = WorkbenchService(storage)
        application.state.storage = storage
        application.state.service = service
        application.state.startup_recovery = storage.recover_incomplete_batches()
        yield

    application = FastAPI(
        title="Service Mesh Policy Simulation Workbench",
        version=__version__,
        lifespan=lifespan,
    )

    @application.exception_handler(BusinessError)
    async def business_error_handler(_request: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content=exc.to_body())

    @application.exception_handler(RequestValidationError)
    async def validation_error_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": E_INVALID_ARGUMENT,
                    "message": "request validation failed",
                    "details": {"validation_errors": jsonable_encoder(exc.errors())},
                }
            },
        )

    @application.exception_handler(StarletteHTTPException)
    async def http_error_handler(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": E_INVALID_ARGUMENT,
                    "message": str(exc.detail),
                    "details": {},
                }
            },
        )

    @application.exception_handler(Exception)
    async def unexpected_error_handler(_request: Request, exc: Exception) -> JSONResponse:
        logger.exception("unexpected API failure", exc_info=exc)
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": E_INTERNAL_ERROR,
                    "message": "internal workbench error",
                    "details": {},
                }
            },
        )

    def service(request: Request) -> WorkbenchService:
        return request.app.state.service

    @application.get("/api/health")
    def health(request: Request) -> dict[str, Any]:
        storage: Storage = request.app.state.storage
        return {
            "status": "ok",
            "app_version": __version__,
            "python": platform.python_version(),
            "database": str(storage.db_path),
            "current_snapshot_version": storage.current_version(),
            "startup_recovery_count": len(request.app.state.startup_recovery),
        }

    @application.post("/api/policies", status_code=201)
    def submit_policy(snapshot: PolicySnapshot, request: Request) -> dict[str, Any]:
        return service(request).submit_snapshot(snapshot)

    @application.get("/api/policies/current")
    def current_policy(request: Request) -> dict[str, Any]:
        return service(request).current_snapshot()

    @application.get("/api/policies/{version}")
    def get_policy(version: int, request: Request) -> dict[str, Any]:
        return service(request).get_snapshot(version)

    @application.post("/api/simulations", status_code=201)
    def submit_simulation(payload: SimulationSubmit, request: Request) -> dict[str, Any]:
        workbench = service(request)
        if payload.execution == "prepare":
            prepared = workbench.prepare_batch(payload.snapshot_version, payload.trace)
            return {"execution": "prepare", "batch": prepared}
        result = workbench.run_batch(payload.snapshot_version, payload.trace)
        return {"execution": "commit", **result}

    @application.get("/api/simulations/compare")
    def compare_simulations(left: str, right: str, request: Request) -> dict[str, Any]:
        return service(request).compare_batches(left, right)

    @application.get("/api/simulations/{batch_id}")
    def get_batch(batch_id: str, request: Request) -> dict[str, Any]:
        return service(request).get_batch(batch_id)

    @application.get("/api/simulations/{batch_id}/decisions")
    def get_decisions(batch_id: str, request: Request) -> dict[str, Any]:
        workbench = service(request)
        summary = workbench.storage.get_batch_summary(batch_id)
        decisions = workbench.get_decisions(batch_id)
        return {
            "batch_id": batch_id,
            "snapshot_version": summary["snapshot_version"],
            "snapshot_checksum": summary["snapshot_checksum"],
            "trace_checksum": summary["trace_checksum"],
            "decision_count": len(decisions),
            "decisions": decisions,
        }

    @application.get("/api/simulations/{batch_id}/orders")
    def get_orders(batch_id: str, request: Request) -> dict[str, Any]:
        orders = service(request).get_orders(batch_id)
        return {"batch_id": batch_id, "order_count": len(orders), "orders": orders}

    @application.get("/api/simulations/{batch_id}/conflicts")
    def get_conflicts(batch_id: str, request: Request) -> dict[str, Any]:
        summary = service(request).storage.get_batch_summary(batch_id)
        if summary["status"] != BatchStatus.COMPLETED.value:
            raise BusinessError(
                E_BATCH_INCOMPLETE,
                "no complete simulation conclusion is visible for this batch",
                status_code=409,
                details={"batch_id": batch_id, "status": summary["status"]},
            )
        return {
            "batch_id": batch_id,
            "snapshot_version": summary["snapshot_version"],
            "conflicts": summary["conflicts"],
        }

    @application.post("/api/simulations/{batch_id}/transition")
    def transition_batch(batch_id: str, payload: TransitionRequest, request: Request) -> dict[str, Any]:
        summary = service(request).storage.transition_batch(batch_id, BatchStatus(payload.status))
        return {"batch": summary}

    @application.get("/api/orders/{order_id}")
    def get_order(order_id: str, request: Request) -> dict[str, Any]:
        return service(request).get_order(order_id)

    @application.post("/api/routes/resolve")
    def resolve_route(payload: RouteResolveRequest, request: Request) -> dict[str, Any]:
        return service(request).resolve_route(
            payload.snapshot_version,
            payload.path,
            payload.headers,
            strict=payload.strict,
        )

    @application.post("/api/recovery/scan")
    def recovery_scan(request: Request) -> dict[str, Any]:
        return service(request).recover_incomplete_batches()

    @application.get("/api/recovery/reports")
    def recovery_reports(request: Request) -> dict[str, Any]:
        reports = service(request).list_recovery_reports()
        return {"report_count": len(reports), "reports": reports}

    @application.get("/api/recovery/reports/{batch_id}")
    def recovery_report(batch_id: str, request: Request) -> dict[str, Any]:
        return service(request).get_recovery_report_for_batch(batch_id)

    return application


app = create_app()
