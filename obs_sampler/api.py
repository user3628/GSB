"""FastAPI application exposing the local sampling coordinator."""

from __future__ import annotations

import logging
import os
import sqlite3
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from .errors import BusinessError, PolicyError
from .models import BatchRequest
from .service import SamplerService

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    root = logging.getLogger()
    if getattr(root, "_observability_sampler_configured", False):
        return
    log_dir = Path(os.environ.get("OBS_LOG_DIR", "logs")).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S%z"
    )
    file_handler = RotatingFileHandler(
        log_dir / "observability-sampler.log",
        maxBytes=5_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root.setLevel(logging.INFO)
    root.addHandler(file_handler)
    root.addHandler(stream_handler)
    setattr(root, "_observability_sampler_configured", True)


def _error_response(
    status_code: int,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"code": code, "message": message, "details": details or {}},
    )


def create_app(
    *,
    db_path: str | Path | None = None,
    service: SamplerService | None = None,
) -> FastAPI:
    configure_logging()
    if service is None:
        path = db_path or os.environ.get("OBS_DB_PATH", "runtime/observability.db")
        service = SamplerService(path)

    app = FastAPI(
        title="Local Observability Sampling Coordinator",
        version="1.0.0",
        description=(
            "Deterministic stratified + tail sampling with versioned derived snapshots, "
            "SQLite WAL durability and forced-termination recovery."
        ),
    )
    app.state.sampler = service

    @app.exception_handler(BusinessError)
    async def business_error_handler(_: Request, exc: BusinessError) -> JSONResponse:
        return _error_response(exc.status_code, exc.code, exc.message, exc.details)

    @app.exception_handler(PolicyError)
    async def policy_error_handler(_: Request, exc: PolicyError) -> JSONResponse:
        return _error_response(422, "E_POLICY_INVALID", str(exc))

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        code = "E_POLICY_INVALID" if "/policies" in request.url.path else "E_RECORD_INVALID"
        return _error_response(
            422,
            code,
            "request validation failed",
            {"reason": "REQUEST_SCHEMA_INVALID", "errors": jsonable_encoder(exc.errors())},
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = "E_NOT_FOUND" if exc.status_code == 404 else "E_HTTP_ERROR"
        return _error_response(exc.status_code, code, str(exc.detail))

    @app.exception_handler(sqlite3.Error)
    async def sqlite_error_handler(_: Request, exc: sqlite3.Error) -> JSONResponse:
        logger.exception("SQLite operation failed")
        return _error_response(
            500,
            "E_STORAGE_FAILURE",
            "SQLite operation failed",
            {"sqlite_error": type(exc).__name__},
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(_: Request, exc: Exception) -> JSONResponse:
        logger.exception("unhandled application error")
        return _error_response(
            500,
            "E_INTERNAL",
            "unexpected internal failure",
            {"exception_type": type(exc).__name__},
        )

    @app.get("/health")
    def health(request: Request) -> dict[str, Any]:
        sampler: SamplerService = request.app.state.sampler
        current = sampler.storage.current_policy_version()
        return {
            "status": "ok",
            "current_policy_version": current,
            "sqlite_path": str(sampler.storage.path),
            "journal_mode": "WAL",
        }

    @app.post("/api/v1/policies")
    def submit_policy(request: Request, document: dict[str, Any]) -> dict[str, Any]:
        sampler: SamplerService = request.app.state.sampler
        return sampler.submit_policy(document)

    @app.get("/api/v1/policies/current")
    def current_policy(request: Request) -> dict[str, Any]:
        sampler: SamplerService = request.app.state.sampler
        return sampler.get_policy()

    @app.get("/api/v1/policies/{version}")
    def get_policy(request: Request, version: int) -> dict[str, Any]:
        sampler: SamplerService = request.app.state.sampler
        return sampler.get_policy(version)

    @app.get("/api/v1/policies/{version}/sampling-rate")
    def sampling_rate(
        request: Request,
        version: int,
        service: str = Query(min_length=1),
        level: str = Query(min_length=1),
    ) -> dict[str, Any]:
        sampler: SamplerService = request.app.state.sampler
        return sampler.get_sampling_rate(version, service, level)

    @app.get("/api/v1/policies/{version}/bias")
    def bias_report(
        request: Request,
        version: int,
        service: str = Query(min_length=1),
        level: str = Query(min_length=1),
    ) -> dict[str, Any]:
        sampler: SamplerService = request.app.state.sampler
        return sampler.get_bias_report(version, service, level)

    @app.get("/api/v1/policies/{version}/state")
    def policy_state(request: Request, version: int) -> dict[str, Any]:
        sampler: SamplerService = request.app.state.sampler
        return sampler.get_policy_state(version)

    @app.get("/api/v1/bias")
    def bias_report_alias(
        request: Request,
        service: str = Query(min_length=1),
        level: str = Query(min_length=1),
        policy_version: int | None = Query(default=None, ge=1),
    ) -> dict[str, Any]:
        sampler: SamplerService = request.app.state.sampler
        return sampler.get_bias_report(policy_version, service, level)

    @app.get("/api/v1/sampling-rate")
    def sampling_rate_alias(
        request: Request,
        service: str = Query(min_length=1),
        level: str = Query(min_length=1),
        policy_version: int | None = Query(default=None, ge=1),
    ) -> dict[str, Any]:
        sampler: SamplerService = request.app.state.sampler
        return sampler.get_sampling_rate(policy_version, service, level)

    @app.post("/api/v1/batches")
    def submit_batch(request: Request, body: BatchRequest) -> dict[str, Any]:
        sampler: SamplerService = request.app.state.sampler
        return sampler.submit_batch(body)

    @app.get("/api/v1/batches/{batch_id}")
    def get_batch(request: Request, batch_id: str) -> dict[str, Any]:
        sampler: SamplerService = request.app.state.sampler
        return sampler.get_batch_result(batch_id)

    @app.get("/api/v1/recovery-report")
    def recovery_report(request: Request) -> dict[str, Any]:
        sampler: SamplerService = request.app.state.sampler
        return sampler.get_recovery_report()

    @app.post("/api/v1/recovery/scan")
    def recovery_scan(request: Request) -> dict[str, Any]:
        sampler: SamplerService = request.app.state.sampler
        return sampler.recover()

    return app
