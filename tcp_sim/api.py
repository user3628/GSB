from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from .db import Database
from .errors import ERROR_CATALOG, BusinessError
from .service import BatchCommand, SimulationService


class BatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trace_id: str = Field(min_length=1)
    run_id: str | None = Field(default=None, min_length=1)
    batch_id: str = Field(min_length=1)
    events: list[Any]
    final_batch: bool = True
    expected_base_checkpoint_id: str | None = None
    config: dict[str, Any] | None = None


class CompareRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    left_run_id: str = Field(min_length=1)
    right_run_id: str = Field(min_length=1)


def _ok(data: Any) -> dict[str, Any]:
    return {"ok": True, "data": data}


def create_app(db_path: str | Path | None = None) -> FastAPI:
    resolved_db_path = Path(
        db_path
        or os.getenv("TCP_SIM_DB_PATH", str(Path("data") / "tcp_sim.db"))
    )
    database = Database(resolved_db_path)
    database.initialize()
    service = SimulationService(database)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.recovery_report = service.recover_incomplete()
        yield

    app = FastAPI(
        title="Local TCP Congestion Simulation Adjudication Workbench",
        version="1.0.0",
        description=(
            "Deterministic, checkpointed TCP connection/congestion simulation over "
            "local loopback. All simulation arithmetic is integer fixed-point."
        ),
        lifespan=lifespan,
    )
    app.state.service = service
    app.state.db_path = str(resolved_db_path)

    @app.exception_handler(BusinessError)
    async def business_error_handler(_: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status or 400, content=exc.to_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        error = BusinessError(
            "E_REQUEST_INVALID",
            context={"errors": jsonable_encoder(exc.errors())},
        )
        return JSONResponse(status_code=error.http_status or 400, content=error.to_dict())

    @app.exception_handler(Exception)
    async def internal_error_handler(_: Request, exc: Exception) -> JSONResponse:
        error = BusinessError(
            "E_INTERNAL",
            context={
                "exception_type": type(exc).__name__,
                "message": str(exc),
            },
        )
        return JSONResponse(status_code=500, content=error.to_dict())

    @app.get("/healthz")
    def healthz() -> dict[str, Any]:
        return _ok({"status": "ok", "database": app.state.db_path})

    @app.get("/v1/error-codes")
    def error_codes() -> dict[str, Any]:
        return _ok(
            {
                "codes": [
                    {
                        "code": code,
                        "http_status": value["http_status"],
                        "message": value["message"],
                    }
                    for code, value in sorted(ERROR_CATALOG.items())
                ]
            }
        )

    @app.post("/v1/simulations/batches")
    def submit_batch(request: BatchRequest) -> dict[str, Any]:
        command = BatchCommand(
            trace_id=request.trace_id,
            run_id=request.run_id or request.trace_id,
            batch_id=request.batch_id,
            events=request.events,
            final_batch=request.final_batch,
            expected_base_checkpoint_id=request.expected_base_checkpoint_id,
            config=request.config,
        )
        return _ok(service.submit_batch(command))

    @app.get("/v1/batches/{batch_id}")
    def get_batch(batch_id: str) -> dict[str, Any]:
        return _ok(service.get_batch(batch_id))

    @app.get("/v1/runs/{run_id}")
    def get_run(run_id: str) -> dict[str, Any]:
        return _ok(service.get_run(run_id))

    @app.get("/v1/runs/{run_id}/state")
    def query_state(
        run_id: str,
        at_ms: int = Query(ge=0),
        conn_id: str | None = None,
        wait_ms: int = Query(default=0, ge=0, le=10_000),
    ) -> dict[str, Any]:
        return _ok(
            service.query_connection_state(
                run_id=run_id,
                conn_id=conn_id,
                at_ms=at_ms,
                wait_ms=wait_ms,
            )
        )

    @app.get("/v1/runs/{run_id}/window-trace")
    def window_trace(run_id: str, conn_id: str = Query(min_length=1)) -> dict[str, Any]:
        return _ok(service.window_trace(run_id, conn_id))

    @app.get("/v1/runs/{run_id}/diagnostics")
    def diagnostics(run_id: str) -> dict[str, Any]:
        return _ok(service.diagnostics(run_id))

    @app.post("/v1/runs/compare")
    def compare_runs(request: CompareRequest) -> dict[str, Any]:
        return _ok(service.compare_runs(request.left_run_id, request.right_run_id))

    @app.get("/v1/checkpoints/{checkpoint_id}")
    def get_checkpoint(checkpoint_id: str) -> dict[str, Any]:
        return _ok(service.get_checkpoint(checkpoint_id))

    @app.get("/v1/recovery/reports/latest")
    def latest_recovery_report() -> dict[str, Any]:
        return _ok(service.latest_recovery_report())

    @app.post("/v1/recovery/scan")
    def recovery_scan() -> dict[str, Any]:
        return _ok(service.recover_incomplete())

    return app


app = create_app()
