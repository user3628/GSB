from __future__ import annotations

import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from .config import ROOT, demo_crash_enabled, sqlite_url
from .engine import Engine
from .errors import EngineError
from .schemas import (
    BatchRequest,
    CloseWindowsRequest,
    DemoCrashRequest,
    EmitWindowsRequest,
    LoadSnapshotRequest,
    RecoveryScanRequest,
    TransitionWindowRequest,
)

INDEX_FILE = ROOT / "static" / "index.html"


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def create_app(db_url: str | None = None, *, recover_on_startup: bool = True) -> FastAPI:
    engine = Engine(sqlite_url(db_url), recover_on_startup=recover_on_startup)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        engine.close()

    app = FastAPI(
        title="SQLite Streaming Window Aggregator",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.engine = engine

    @app.exception_handler(EngineError)
    async def engine_error_handler(_request: Request, exc: EngineError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content=exc.to_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "code": "E_INVALID_REQUEST",
                "message": "request validation failed",
                "context": {"errors": _json_safe(exc.errors())},
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = "E_NOT_FOUND" if exc.status_code == 404 else "E_INVALID_REQUEST"
        return JSONResponse(
            status_code=exc.status_code,
            content={"code": code, "message": str(exc.detail), "context": {}},
        )

    @app.exception_handler(Exception)
    async def internal_error_handler(_request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "code": "E_INTERNAL",
                "message": "unexpected engine failure",
                "context": {"type": type(exc).__name__, "error": str(exc)},
            },
        )

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(INDEX_FILE)

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        status = engine.status()
        return {
            "ok": True,
            "service": "sqlite-stream-window-aggregator",
            "state_version": status["state_version"],
            "single_writer": True,
            "sqlite_wal": True,
        }

    @app.get("/api/status")
    def status() -> dict[str, Any]:
        return engine.status()

    @app.get("/api/samples")
    def list_samples() -> dict[str, Any]:
        sample_dir = ROOT / "samples"
        return {
            "samples": [
                {"name": path.name, "bytes": path.stat().st_size}
                for path in sorted(sample_dir.glob("*.json"))
            ]
        }

    @app.get("/api/samples/{name}")
    def get_sample(name: str) -> JSONResponse:
        if "/" in name or "\\" in name or not name.endswith(".json"):
            raise EngineError("E_NOT_FOUND", "sample does not exist", {"name": name})
        path = ROOT / "samples" / name
        if not path.is_file():
            raise EngineError("E_NOT_FOUND", "sample does not exist", {"name": name})
        return JSONResponse(content=json.loads(path.read_text()))

    @app.post("/api/batches")
    def ingest_batch(request: BatchRequest) -> dict[str, Any]:
        return engine.ingest_batch(request)

    @app.get("/api/batches")
    def list_batches(limit: int = Query(100, ge=1, le=1000)) -> dict[str, Any]:
        return engine.list_batches(limit)

    @app.get("/api/events")
    def query_events(
        from_ms: int | None = None,
        to_ms: int | None = None,
        group_key: str | None = None,
        batch_id: str | None = None,
        limit: int = Query(500, ge=1, le=5000),
    ) -> dict[str, Any]:
        return engine.query_events(
            from_ms=from_ms,
            to_ms=to_ms,
            group_key=group_key,
            batch_id=batch_id,
            limit=limit,
        )

    @app.get("/api/windows")
    def query_windows(
        from_ms: int | None = None,
        to_ms: int | None = None,
        group_key: str | None = None,
        window_kind: str | None = None,
        state: str | None = None,
        quantiles: str | None = None,
        include_type_history: bool = False,
    ) -> dict[str, Any]:
        parsed_quantiles = None
        if quantiles:
            try:
                parsed_quantiles = [float(value.strip()) for value in quantiles.split(",") if value.strip()]
            except ValueError as exc:
                raise EngineError(
                    "E_INVALID_REQUEST",
                    "quantiles must be comma-separated numbers",
                    {"quantiles": quantiles},
                ) from exc
        return engine.query_windows(
            from_ms=from_ms,
            to_ms=to_ms,
            group_key=group_key,
            window_kind=window_kind,
            state=state,
            quantiles=parsed_quantiles,
            include_type_history=include_type_history,
        )

    @app.get("/api/late-events")
    def query_late_events(batch_id: str | None = None, group_key: str | None = None) -> dict[str, Any]:
        return engine.query_late_events(batch_id=batch_id, group_key=group_key)

    @app.get("/api/type-evolution")
    def type_evolution(group_key: str | None = None) -> dict[str, Any]:
        return engine.type_evolution(group_key=group_key)

    @app.post("/api/windows/close")
    def close_windows(request: CloseWindowsRequest) -> dict[str, Any]:
        return engine.close_windows(request)

    @app.post("/api/windows/emit")
    def emit_windows(request: EmitWindowsRequest) -> dict[str, Any]:
        return engine.emit_windows(request)

    @app.post("/api/windows/transition")
    def transition_window(request: TransitionWindowRequest) -> dict[str, Any]:
        return engine.transition_window(request)

    @app.post("/api/snapshots")
    def create_snapshot() -> dict[str, Any]:
        return engine.create_snapshot()

    @app.get("/api/snapshots")
    def list_snapshots() -> dict[str, Any]:
        return engine.list_snapshots()

    @app.get("/api/snapshots/{version}")
    def get_snapshot(version: int) -> dict[str, Any]:
        return engine.get_snapshot(version)

    @app.post("/api/snapshots/load")
    def load_snapshot(request: LoadSnapshotRequest) -> dict[str, Any]:
        return engine.load_snapshot(request.snapshot)

    @app.post("/api/snapshots/{version}/load")
    def load_stored_snapshot(version: int) -> dict[str, Any]:
        return engine.load_stored_snapshot(version)

    @app.get("/api/recovery/report")
    def recovery_report() -> dict[str, Any]:
        return engine.latest_recovery_report()

    @app.post("/api/recovery/scan")
    def recovery_scan(request: RecoveryScanRequest | None = None) -> dict[str, Any]:
        return engine.recover_pending_intents(reason=request.reason if request else "manual")

    if demo_crash_enabled():

        @app.post("/api/demo/crash-ingest")
        def crash_ingest(request: DemoCrashRequest) -> dict[str, Any]:
            batch = BatchRequest.model_validate(request.model_dump(exclude={"crash_point"}))
            return engine.ingest_batch(batch, crash_point=request.crash_point)

    return app


app = create_app()
