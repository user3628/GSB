from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from .errors import BusinessError, SimulatedForcedTermination
from .models import (
    AdjustmentIn,
    BatchPreviewIn,
    BatchRunIn,
    FaultInjectionIn,
    SafeShutdownIn,
    SnapshotIn,
    TransitionCheckIn,
)
from .service import TieringService


def _business_error_response(exc: BusinessError, service: TieringService) -> JSONResponse:
    try:
        epoch = service.get_state_epoch()
    except Exception:
        epoch = None
    return JSONResponse(
        status_code=exc.status_code,
        content=jsonable_encoder(exc.as_dict(epoch)),
    )


def create_app(
    *,
    db_path: str | Path | None = None,
    samples_dir: str | Path | None = None,
    auto_seed: bool = True,
    service: TieringService | None = None,
) -> FastAPI:
    if service is None:
        resolved_db = Path(db_path or os.environ.get("TIERING_DB_PATH", "data/tiering.db"))
        resolved_samples = samples_dir or os.environ.get("TIERING_SAMPLES_DIR", "samples")
        service = TieringService(
            resolved_db,
            samples_dir=resolved_samples,
            auto_seed=auto_seed,
        )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        service.startup()
        try:
            yield
        finally:
            service.safe_shutdown("lifespan_exit")

    app = FastAPI(
        title="Local Tiering Migration Service",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.service = service

    @app.exception_handler(BusinessError)
    async def handle_business_error(_: Request, exc: BusinessError) -> JSONResponse:
        return _business_error_response(exc, service)

    @app.exception_handler(SimulatedForcedTermination)
    async def handle_simulated_termination(
        _: Request, exc: SimulatedForcedTermination
    ) -> JSONResponse:
        error = BusinessError(
            "E_SIMULATED_TERMINATION",
            "the in-process crash injector terminated the request before cleanup",
            context={
                "point": exc.point,
                "batch_id": exc.batch_id,
                "process_exited": False,
                "recovery_required_on_restart": True,
            },
            status_code=503,
        )
        return _business_error_response(error, service)

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        error = BusinessError(
            "E_INVALID_REQUEST",
            "request validation failed",
            context={"errors": exc.errors()},
            status_code=422,
        )
        return _business_error_response(error, service)

    @app.exception_handler(StarletteHTTPException)
    async def handle_http_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = "E_NOT_FOUND" if exc.status_code == 404 else "E_HTTP_ERROR"
        error = BusinessError(
            code,
            str(exc.detail),
            context={"method": request.method, "path": request.url.path},
            status_code=exc.status_code,
        )
        return _business_error_response(error, service)

    @app.exception_handler(Exception)
    async def handle_unexpected_error(_: Request, exc: Exception) -> JSONResponse:
        error = BusinessError(
            "E_INTERNAL",
            "unexpected internal error",
            context={"type": type(exc).__name__, "detail": str(exc)},
            status_code=500,
        )
        return _business_error_response(error, service)

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "service": "local-tiering-migration",
            "state_epoch": service.get_state_epoch(),
            "run_id": service._run_id,
        }

    @app.get("/v1/policy/current")
    def current_policy() -> dict[str, Any]:
        return service.get_current_policy()

    @app.post("/v1/policy/snapshots")
    def submit_snapshot(payload: SnapshotIn) -> dict[str, Any]:
        return service.submit_snapshot(payload.model_dump())

    @app.post("/v1/policy/adjustments")
    def adjust_snapshot(payload: AdjustmentIn) -> dict[str, Any]:
        return service.adjust_snapshot(**payload.model_dump())

    @app.get("/v1/layers/occupancy")
    def layer_occupancy() -> dict[str, Any]:
        return service.get_occupancy()

    @app.get("/v1/layers/distribution")
    def layer_distribution() -> dict[str, Any]:
        return service.get_heat_distribution()

    @app.get("/v1/shards")
    def list_shards(
        layer_id: str | None = Query(default=None),
        limit: int = Query(default=1000, ge=1, le=100_000),
    ) -> dict[str, Any]:
        return service.list_shards(layer_id=layer_id, limit=limit)

    @app.get("/v1/shards/{shard_id}")
    def get_shard(shard_id: str) -> dict[str, Any]:
        return service.get_shard(shard_id)

    @app.post("/v1/batches/preview")
    def preview_batch(payload: BatchPreviewIn) -> dict[str, Any]:
        return service.preview_batch(payload.snapshot_version)

    @app.post("/v1/batches")
    def execute_batch(payload: BatchRunIn) -> dict[str, Any]:
        return service.execute_batch(payload.batch_id, payload.snapshot_version)

    @app.get("/v1/batches/{batch_id}")
    def get_batch(batch_id: str) -> dict[str, Any]:
        return service.get_batch(batch_id)

    @app.get("/v1/state-machine")
    def state_machine() -> dict[str, Any]:
        return service.state_machine()

    @app.post("/v1/state-machine/validate")
    def validate_transition(payload: TransitionCheckIn) -> dict[str, Any]:
        return service.check_transition(payload.from_state, payload.to_state)

    @app.post("/v1/faults/injection")
    def inject_fault(payload: FaultInjectionIn) -> dict[str, Any]:
        return service.arm_fault(
            point=payload.point,
            mode=payload.mode,
            batch_id=payload.batch_id,
        )

    @app.get("/v1/recovery/report")
    def recovery_report() -> dict[str, Any]:
        return service.get_recovery_report()

    @app.get("/v1/recovery/reports")
    def recovery_reports(limit: int = Query(default=50, ge=1, le=1000)) -> dict[str, Any]:
        return service.list_reports(limit=limit)

    @app.post("/v1/admin/safe-shutdown")
    def safe_shutdown(payload: SafeShutdownIn | None = None) -> dict[str, Any]:
        return service.safe_shutdown(payload.reason if payload else "operator_requested")

    return app
