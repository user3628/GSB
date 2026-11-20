from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.errors import BusinessError, snapshot_invalid
from app.service import ConsistencyService
from app.snapshot import load_json_decimal


DEFAULT_DB = Path(os.environ.get("CONSISTENCY_DB", str(Path("var") / "consistency.sqlite3")))


def _json_response(payload: dict[str, Any], status_code: int = 200) -> JSONResponse:
    return JSONResponse(payload, status_code=status_code)


def _parse_base_version(raw: Any) -> int | None:
    if raw is None or raw == "":
        return None
    if isinstance(raw, bool):
        raise BusinessError("E_BAD_REQUEST", "base_version must be an integer", http_status=400)
    if isinstance(raw, int):
        value = raw
    elif isinstance(raw, str):
        try:
            value = int(raw, 10)
        except ValueError as exc:
            raise BusinessError("E_BAD_REQUEST", "base_version must be an integer", http_status=400) from exc
    else:
        raise BusinessError("E_BAD_REQUEST", "base_version must be an integer", http_status=400)
    if value < 0:
        raise BusinessError("E_BAD_REQUEST", "base_version must not be negative", http_status=400)
    return value


def create_app(db_path: str | Path | None = None, *, recover_on_start: bool = True) -> FastAPI:
    service = ConsistencyService(db_path if db_path is not None else DEFAULT_DB)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        service.initialize(recover=recover_on_start)
        yield

    app = FastAPI(
        title="Configuration Consistency Coordinator",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.service = service

    @app.exception_handler(BusinessError)
    async def business_error_handler(_: Request, exc: BusinessError) -> JSONResponse:
        return _json_response(exc.to_dict(), status_code=exc.http_status)

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        error = BusinessError(
            "E_BAD_REQUEST",
            "request validation failed",
            http_status=400,
            details={"errors": exc.errors()},
        )
        return _json_response(error.to_dict(), status_code=400)

    @app.exception_handler(Exception)
    async def unhandled_error_handler(_: Request, exc: Exception) -> JSONResponse:
        error = BusinessError(
            "E_INTERNAL",
            "internal coordinator error",
            http_status=500,
            details={"exception_type": type(exc).__name__},
        )
        return _json_response(error.to_dict(), status_code=500)

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "service": "configuration-consistency", "version": "1.0.0"}

    @app.get("/api/v1/current")
    def current() -> dict[str, Any]:
        return service.get_current()

    @app.post("/api/v1/snapshots")
    async def submit_snapshot(request: Request) -> JSONResponse:
        try:
            raw_body = await request.body()
            text = raw_body.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise snapshot_invalid("request body must be UTF-8 JSON") from exc
        parsed = load_json_decimal(text)
        query_base = request.query_params.get("base_version")
        base_version = _parse_base_version(query_base)

        if isinstance(parsed, dict) and "snapshot" in parsed:
            wrapper_base = parsed.get("base_version")
            if base_version is None:
                base_version = _parse_base_version(wrapper_base)
            snapshot_payload = parsed.get("snapshot")
            if not isinstance(snapshot_payload, dict):
                raise snapshot_invalid("wrapper field 'snapshot' must be an object")
        else:
            if not isinstance(parsed, dict):
                raise snapshot_invalid("snapshot body must be a JSON object")
            snapshot_payload = parsed

        result = service.commit_snapshot(snapshot_payload, base_version=base_version)
        return _json_response(result, status_code=200 if result.get("reused") else 201)

    @app.get("/api/v1/snapshots/{version}/metrics")
    def metrics(version: int) -> dict[str, Any]:
        return service.get_metrics(version)

    @app.get("/api/v1/snapshots/{version}/consistency")
    def consistency(version: int) -> dict[str, Any]:
        return service.get_consistency(version)

    @app.get("/api/v1/snapshots/{version}/errors")
    def errors(version: int) -> dict[str, Any]:
        return service.get_errors(version)

    @app.get("/api/v1/recovery/report")
    def recovery_report() -> dict[str, Any]:
        return service.get_recovery_report()

    @app.post("/api/v1/recovery/scan")
    def recovery_scan() -> dict[str, Any]:
        return service.recover_pending(reason="api_manual_scan")

    return app


app = create_app()
