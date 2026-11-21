from __future__ import annotations

import asyncio
from typing import Any

from fastapi import Body, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response

from . import __version__
from .api_models import PolicyIncrementRequest, ReplayRequest, SampleBatchRequest, SnapshotRequest
from .config import Settings
from .errors import (
    BusinessError,
    business_error_handler,
    unexpected_error_handler,
    validation_error_handler,
)
from .service import MirrorJudgeService
from .store import Store


def _service(request: Request) -> MirrorJudgeService:
    return request.app.state.service


def _require(value: Any) -> Any:
    if value is None:
        raise BusinessError("E_VERSION_NOT_FOUND", "No committed version exists")
    return value


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    settings.ensure_directories()
    store = Store(settings.db_path)
    recovery = store.recover()
    service = MirrorJudgeService(store, settings)

    app = FastAPI(
        title="Mirror Sampling & Replay Bias Adjudicator",
        version=__version__,
        description="Deterministic local service-mesh mirror sampling, replay batching and bias attribution.",
    )
    app.state.settings = settings
    app.state.store = store
    app.state.service = service
    app.state.startup_recovery = recovery
    app.add_exception_handler(BusinessError, business_error_handler)
    app.add_exception_handler(RequestValidationError, validation_error_handler)
    app.add_exception_handler(Exception, unexpected_error_handler)

    @app.middleware("http")
    async def loopback_only(request: Request, call_next):
        host = request.client.host if request.client else ""
        if host not in {"127.0.0.1", "::1", "localhost", "testclient"}:
            return JSONResponse(
                status_code=403,
                content={"code": "E_SNAPSHOT_INVALID", "message": "This component only serves loopback clients", "details": {}},
            )
        return await call_next(request)

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "component_version": __version__,
            "current_version": store.current_version(),
            "database_path": str(settings.db_path),
            "replay_target": settings.replay_target_url,
            "loopback_only": True,
        }

    @app.get("/api/v1/commits")
    def list_commits() -> dict[str, Any]:
        return {"current_version": store.current_version(), "commits": store.list_commits()}

    @app.get("/api/v1/state")
    def state(version: int | None = Query(default=None, ge=1)) -> dict[str, Any]:
        return _require(service.state(version))

    @app.get("/api/v1/selection")
    def selection(version: int | None = Query(default=None, ge=1)) -> dict[str, Any]:
        return _require(service.selection(version))

    @app.get("/api/v1/batches")
    def batches(version: int | None = Query(default=None, ge=1)) -> dict[str, Any]:
        return _require(service.batches(version))

    @app.get("/api/v1/bias")
    def bias(version: int | None = Query(default=None, ge=1)) -> dict[str, Any]:
        return _require(service.bias(version))

    @app.get("/api/v1/records")
    def records(version: int | None = Query(default=None, ge=1)) -> dict[str, Any]:
        return _require(service.records(version))

    @app.post("/api/v1/snapshots", status_code=201)
    def commit_snapshot(payload: SnapshotRequest) -> dict[str, Any]:
        return service.commit_snapshot(
            base_version=payload.base_version,
            snapshot_version=payload.snapshot_version,
            batch_id=payload.batch_id,
            raw_records=payload.records,
            raw_policy=payload.policy,
        )

    @app.post("/api/v1/samples/batches", status_code=201)
    def append_sample_batch(payload: SampleBatchRequest, response: Response) -> dict[str, Any]:
        result = service.append_sample_batch(
            base_version=payload.base_version,
            batch_id=payload.batch_id,
            raw_records=payload.records,
        )
        if result.get("idempotent"):
            response.status_code = 200
        return result

    @app.post("/api/v1/policies/increments", status_code=201)
    def apply_policy_increment(payload: PolicyIncrementRequest) -> dict[str, Any]:
        return service.apply_policy_increment(
            base_version=payload.base_version,
            base_policy_version=payload.base_policy_version,
            policy_version=payload.policy_version,
            patch=payload.patch,
        )

    @app.post("/api/v1/replays", status_code=201)
    def replay(payload: ReplayRequest) -> dict[str, Any]:
        return service.replay(
            source_version=payload.source_version,
            batch_id=payload.batch_id,
            timeout_ms=payload.timeout_ms,
            fault_mode=payload.fault_mode,
            fault_status=payload.fault_status,
            hang_ms=payload.hang_ms,
            raise_on_timeout=payload.raise_on_timeout,
        )

    @app.get("/api/v1/replays/{run_id}")
    def replay_run(run_id: str) -> dict[str, Any]:
        value = store.get_replay_run(run_id)
        if value is None:
            raise BusinessError("E_REPLAY_NOT_FOUND", "Replay run was not found", {"run_id": run_id})
        return value

    @app.get("/api/v1/recovery/latest")
    def latest_recovery() -> dict[str, Any]:
        report = store.latest_recovery_report()
        return {"recovery": report, "startup_recovery": recovery}

    @app.post("/api/v1/recovery/scan")
    def recovery_scan() -> dict[str, Any]:
        return {"recovery": store.recover()}

    @app.post("/_test/replay")
    async def test_replay(request: Request, payload: dict[str, Any] = Body(...)) -> Response:
        client_host = request.client.host if request.client else ""
        if client_host not in {"127.0.0.1", "::1", "localhost", "testclient"}:
            return JSONResponse(status_code=403, content={"detail": "loopback clients only"})
        fault_mode = request.headers.get("x-mirror-fault-mode", "mirror").lower()
        if fault_mode not in {"mirror", "ok", "status", "hang"}:
            return JSONResponse(status_code=400, content={"detail": "invalid fault mode"})
        try:
            fault_status = int(request.headers.get("x-mirror-fault-status", "200"))
            hang_ms = int(request.headers.get("x-mirror-hang-ms", "0"))
        except ValueError:
            return JSONResponse(status_code=400, content={"detail": "invalid fault header"})
        if fault_mode == "hang" and hang_ms > 0:
            await asyncio.sleep(hang_ms / 1000.0)
        record = payload.get("record") if isinstance(payload, dict) else None
        original_status = record.get("status_code") if isinstance(record, dict) else None
        original_bytes = record.get("response_bytes") if isinstance(record, dict) else 0
        if fault_mode == "ok":
            status_code = 200
        elif fault_mode == "status":
            status_code = fault_status
        elif fault_mode == "hang":
            status_code = 200
        else:
            status_code = int(original_status) if type(original_status) is int else 200
        if not 100 <= status_code <= 599:
            status_code = 500
        size = min(int(original_bytes) if type(original_bytes) is int else 0, 262144)
        return Response(
            content=b"x" * size,
            status_code=status_code,
            media_type="application/octet-stream",
            headers={"X-Mirror-Test-Endpoint": "local", "X-Mirror-Fault-Mode": fault_mode},
        )

    return app


app = create_app()
