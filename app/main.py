from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from .config import load_config
from .engine import WindowEngine
from .errors import (
    BusinessError,
    E_CAPACITY_EXCEEDED,
    E_CONFLICTING_BATCH,
    E_CURSOR_EXPIRED,
    E_EVENT_INVALID,
    E_INTERNAL,
    E_INVALID_TRANSITION,
    E_LATE_EVENT,
    E_NOT_FOUND,
    E_REQUEST_INVALID,
    E_SNAPSHOT_INVALID,
)
from .engine import WindowStateMachine
from .models import BatchIn, SnapshotLoadIn, SnapshotTriggerIn, TransitionValidateIn, WindowQueryIn


LOGGER = logging.getLogger(__name__)

STATUS_BY_CODE = {
    E_EVENT_INVALID: 400,
    E_REQUEST_INVALID: 400,
    E_INVALID_TRANSITION: 409,
    E_LATE_EVENT: 409,
    E_SNAPSHOT_INVALID: 409,
    E_CONFLICTING_BATCH: 409,
    E_CAPACITY_EXCEEDED: 429,
    E_CURSOR_EXPIRED: 410,
    E_NOT_FOUND: 404,
    E_INTERNAL: 500,
}


def _business_response(exc: BusinessError) -> JSONResponse:
    status = STATUS_BY_CODE.get(exc.code, exc.status_code)
    return JSONResponse(status_code=status, content=exc.as_response())


def _validation_details(exc: RequestValidationError) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for error in exc.errors():
        item: dict[str, Any] = {
            "loc": [str(part) for part in error.get("loc", [])],
            "type": str(error.get("type", "validation_error")),
            "message": str(error.get("msg", "invalid request")),
        }
        if "input" in error and isinstance(error["input"], (str, int, float, bool, type(None))):
            item["input"] = error["input"]
        details.append(item)
    return details


def create_app(
    *,
    config_path: str | Path | None = None,
    runtime_dir: str | Path | None = None,
    engine: WindowEngine | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(application: FastAPI):
        if engine is not None:
            application.state.engine = engine
        else:
            selected_config = Path(
                config_path or os.environ.get("DWE_CONFIG", "samples/config.json")
            )
            selected_runtime = Path(
                runtime_dir or os.environ.get("DWE_RUNTIME_DIR", "runtime")
            )
            load_config(selected_config)  # Validate before allocating runtime resources.
            application.state.engine = WindowEngine.from_paths(
                config_path=selected_config,
                runtime_dir=selected_runtime,
            )
        yield

    application = FastAPI(
        title="Deterministic Window Engine",
        version="1.0.0",
        description=(
            "Local deterministic tumbling/sliding/session window aggregation service with "
            "SQLite intent journal, atomic snapshots, and crash recovery."
        ),
        lifespan=lifespan,
    )

    @application.exception_handler(BusinessError)
    async def handle_business_error(request: Request, exc: BusinessError) -> JSONResponse:
        return _business_response(exc)

    @application.exception_handler(RequestValidationError)
    async def handle_request_validation(request: Request, exc: RequestValidationError) -> JSONResponse:
        if request.url.path == "/v1/batches":
            code = E_EVENT_INVALID
            message = "Event batch failed schema validation"
        else:
            code = E_REQUEST_INVALID
            message = "Request failed schema validation"
        error = BusinessError(code, message, details={"errors": _validation_details(exc)})
        return _business_response(error)

    @application.exception_handler(StarletteHTTPException)
    async def handle_http_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = E_NOT_FOUND if exc.status_code == 404 else E_REQUEST_INVALID
        error = BusinessError(code, str(exc.detail), status_code=exc.status_code)
        return _business_response(error)

    @application.exception_handler(Exception)
    async def handle_unexpected(request: Request, exc: Exception) -> JSONResponse:
        LOGGER.exception("Unhandled HTTP exception", exc_info=exc)
        return _business_response(BusinessError(E_INTERNAL, "Unexpected internal error"))

    def current_engine(request: Request) -> WindowEngine:
        return request.app.state.engine

    @application.get("/health")
    def health(request: Request) -> dict[str, Any]:
        engine_ref = current_engine(request)
        return {"ok": True, "service": "deterministic-window-engine", "state": engine_ref.state_summary()}

    @application.get("/v1/config")
    def get_config(request: Request) -> dict[str, Any]:
        return {"ok": True, "config": current_engine(request).config.public_dict()}

    @application.get("/v1/state/summary")
    def get_state_summary(request: Request) -> dict[str, Any]:
        return {"ok": True, "state": current_engine(request).state_summary()}

    @application.get("/v1/state/transitions")
    def get_transitions(request: Request) -> dict[str, Any]:
        return {"ok": True, "transitions": current_engine(request).legal_transitions()}

    @application.post("/v1/state/transitions/validate")
    def validate_transition(payload: TransitionValidateIn) -> dict[str, Any]:
        WindowStateMachine.validate(payload.current, payload.target)
        return {"ok": True, "legal": True, "current": payload.current, "target": payload.target}

    @application.post("/v1/batches")
    def ingest_batch(payload: BatchIn, request: Request) -> dict[str, Any]:
        return current_engine(request).submit_batch(payload.batch_id, payload.events)

    @application.post("/v1/snapshots")
    def trigger_snapshot(request: Request, payload: SnapshotTriggerIn | None = None) -> dict[str, Any]:
        return current_engine(request).trigger_snapshot(payload.label if payload else None)

    @application.get("/v1/snapshots")
    def list_snapshots(request: Request) -> dict[str, Any]:
        return {"ok": True, "snapshots": current_engine(request).list_snapshots()}

    @application.post("/v1/snapshots/load")
    def load_snapshot(payload: SnapshotLoadIn, request: Request) -> dict[str, Any]:
        return current_engine(request).load_snapshot(version=payload.version, path=payload.path)

    @application.post("/v1/windows/query")
    def query_windows(payload: WindowQueryIn, request: Request) -> dict[str, Any]:
        return current_engine(request).query_windows(
            start_ms=payload.start_ms,
            end_ms=payload.end_ms,
            group=payload.group,
            window_types=payload.window_types,
            quantiles=payload.quantiles,
            limit=payload.limit,
            cursor=payload.cursor,
        )

    @application.get("/v1/late-events")
    def late_events(
        request: Request,
        limit: int = 100,
        offset: int = 0,
        group: str | None = None,
    ) -> dict[str, Any]:
        return current_engine(request).late_events(limit=limit, offset=offset, group=group)

    @application.get("/v1/recovery/report")
    def recovery_report(request: Request) -> dict[str, Any]:
        return {"ok": True, "report": current_engine(request).recovery_report()}

    return application


app = create_app()
