"""FastAPI HTTP surface for the local-only scheduling simulator."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import Body, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException

from .engine import SchedulingEngine
from .errors import E_BATCH_NOT_FOUND, E_INVALID_TRANSITION, E_SPEC_INVALID, SchedError
from .samples import SampleRepository


class RoundRequest(BaseModel):
    round_no: int | None = None


class AbortRequest(BaseModel):
    reason: str = "operator requested abort"


class SampleSubmitRequest(BaseModel):
    sample: str = "default"
    batch_id: str | None = None


def _default_db_path() -> str:
    return os.getenv("SCHEDSIM_DB_PATH", str(Path.cwd() / "data" / "scheduler.db"))


def _default_sample_dir() -> str:
    return os.getenv("SCHEDSIM_SAMPLE_DIR", str(Path.cwd() / "samples"))


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    return repr(value)


def create_app(
    *,
    db_path: str | None = None,
    sample_dir: str | None = None,
    auto_recover: bool = True,
) -> FastAPI:
    engine = SchedulingEngine(db_path or _default_db_path())
    samples = SampleRepository(sample_dir or _default_sample_dir())

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.startup_recovery_reports = engine.recover_pending() if auto_recover else []
        yield

    app = FastAPI(
        title="Local Kubernetes Scheduling Budget Simulator",
        version="1.0.0",
        description=(
            "Offline deterministic scheduling simulation. All specs and facts are local; "
            "no cluster or external network access is used."
        ),
        lifespan=lifespan,
    )
    app.state.engine = engine
    app.state.samples = samples

    @app.exception_handler(SchedError)
    async def sched_error_handler(_request: Request, exc: SchedError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        _request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        failures = [
            {
                "path": ".".join(str(part) for part in error.get("loc", [])),
                "reason": error.get("msg", "request validation failed"),
                "value": error.get("input"),
            }
            for error in exc.errors()
        ]
        return JSONResponse(
            status_code=422,
            content={
                "code": E_SPEC_INVALID,
                "message": "HTTP request payload is invalid; no batch was persisted",
                "context": {"failures": _json_safe(failures)},
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(
        request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        code = E_BATCH_NOT_FOUND if exc.status_code == 404 else E_INVALID_TRANSITION
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "code": code,
                "message": str(exc.detail),
                "context": {
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": exc.status_code,
                },
            },
        )

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "mode": "local-offline",
            "sample_dir": str(samples.root),
            "db_path": str(engine.storage.db_path),
        }

    @app.post("/api/v1/batches", status_code=201)
    def submit_batch(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        return engine.submit_batch(payload)

    @app.get("/api/v1/batches")
    def list_batches() -> dict[str, Any]:
        return {"batches": engine.storage.list_batches()}

    @app.post("/api/v1/batches/sample", status_code=201)
    def submit_sample(payload: SampleSubmitRequest) -> dict[str, Any]:
        sample_payload = samples.load(payload.sample, batch_id=payload.batch_id)
        return engine.submit_batch(sample_payload)

    @app.post("/api/v1/samples/{sample_name}/submit", status_code=201)
    def submit_named_sample(
        sample_name: str,
        payload: SampleSubmitRequest | None = Body(default=None),
    ) -> dict[str, Any]:
        batch_id = payload.batch_id if payload is not None else None
        sample_payload = samples.load(sample_name, batch_id=batch_id)
        return engine.submit_batch(sample_payload)

    @app.get("/api/v1/batches/{batch_id}")
    def get_batch(batch_id: str) -> dict[str, Any]:
        return engine.get_batch_summary(batch_id)

    @app.post("/api/v1/batches/{batch_id}/rounds")
    def run_round(
        batch_id: str, payload: RoundRequest | None = Body(default=None)
    ) -> dict[str, Any]:
        round_no = payload.round_no if payload is not None else None
        return engine.run_round(batch_id, round_no=round_no)

    @app.post("/api/v1/batches/{batch_id}/abort")
    def abort_batch(batch_id: str, payload: AbortRequest | None = Body(default=None)) -> dict[str, Any]:
        reason = payload.reason if payload is not None else "operator requested abort"
        return engine.abort_batch(batch_id, reason=reason)

    @app.get("/api/v1/batches/{batch_id}/results")
    def get_results(batch_id: str) -> dict[str, Any]:
        return engine.get_results(batch_id)

    @app.get("/api/v1/batches/{batch_id}/placements")
    def get_placements(batch_id: str) -> dict[str, Any]:
        return engine.get_placements(batch_id)

    @app.get("/api/v1/batches/{batch_id}/rejections")
    def get_rejections(batch_id: str) -> dict[str, Any]:
        return engine.get_rejections(batch_id)

    @app.get("/api/v1/batches/{batch_id}/pods/{pod_name}/rejections")
    def get_pod_rejections(batch_id: str, pod_name: str) -> dict[str, Any]:
        return engine.get_rejections(batch_id, pod_name=pod_name)

    @app.get("/api/v1/nodes/{node_name}")
    def get_node_usage(node_name: str, batch_id: str = Query(...)) -> dict[str, Any]:
        return engine.get_node_usage(batch_id, node_name)

    @app.get("/api/v1/batches/{batch_id}/recovery")
    def get_batch_recovery(batch_id: str) -> dict[str, Any]:
        return engine.get_recovery_reports(batch_id)

    @app.get("/api/v1/recovery")
    def get_all_recovery(request: Request) -> dict[str, Any]:
        payload = engine.get_all_recovery_reports()
        payload["startup_recovery_reports"] = getattr(
            request.app.state, "startup_recovery_reports", []
        )
        return payload

    return app

