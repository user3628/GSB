"""FastAPI surface for the local capacity ledger."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from .errors import CapacityError
from .service import CapacityLedger


def _validation_details(exc: RequestValidationError) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for error in exc.errors():
        details.append(
            {
                "path": ".".join(str(part) for part in error.get("loc", ())),
                "reason": error.get("msg", "invalid request"),
                "type": error.get("type", "validation_error"),
            }
        )
    return details


def create_app(
    db_path: str | Path | None = None,
    intent_dir: str | Path | None = None,
    *,
    recover_on_startup: bool | None = None,
) -> FastAPI:
    app = FastAPI(title="Capacity Cost Ledger", version="1.0.0")
    ledger = CapacityLedger(db_path=db_path, intent_dir=intent_dir)
    app.state.ledger = ledger
    app.state.startup_recovery = None

    should_recover = (
        recover_on_startup
        if recover_on_startup is not None
        else os.environ.get("CAPACITY_RECOVER_ON_STARTUP", "0") == "1"
    )
    if should_recover:
        app.state.startup_recovery = ledger.recover()

    @app.exception_handler(CapacityError)
    async def capacity_error_handler(request: Request, exc: CapacityError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_payload())

    @app.exception_handler(RequestValidationError)
    async def request_validation_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        error = CapacityError(
            "E_SNAPSHOT_INVALID",
            "The HTTP request body could not be parsed as a valid ledger payload.",
            details={"path": "$", "reason": "request validation failed", "errors": _validation_details(exc)},
            http_status=422,
        )
        return JSONResponse(status_code=error.http_status, content=error.to_payload())

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(
        request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        code = "E_ROUTE_NOT_FOUND" if exc.status_code == 404 else "E_HTTP_ERROR"
        error = CapacityError(
            code,
            str(exc.detail),
            details={"path": request.url.path, "status": exc.status_code},
            http_status=exc.status_code,
        )
        return JSONResponse(status_code=exc.status_code, content=error.to_payload())

    @app.exception_handler(Exception)
    async def unexpected_error_handler(request: Request, exc: Exception) -> JSONResponse:
        error = CapacityError(
            "E_INTERNAL",
            "An unexpected internal error occurred.",
            details={"path": request.url.path, "exception_type": type(exc).__name__},
            http_status=500,
        )
        return JSONResponse(status_code=500, content=error.to_payload())

    @app.get("/health")
    def health() -> dict[str, Any]:
        current = ledger.get_current()
        return {
            "ok": True,
            "service": "capacity-cost-ledger",
            "ledger_state": current.get("state", "CURRENT"),
            "latest_complete_version": current.get("version", 0),
            "startup_recovery_performed": app.state.startup_recovery is not None,
        }

    @app.post("/v1/snapshots")
    def commit_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
        return ledger.commit_snapshot(payload)

    @app.post("/v1/increments")
    def commit_increment(payload: dict[str, Any]) -> dict[str, Any]:
        return ledger.commit_increment(payload)

    @app.get("/v1/ledger/current")
    def current_ledger() -> dict[str, Any]:
        return ledger.get_current()

    @app.get("/v1/ledger/versions/{version}")
    def ledger_version(version: int) -> dict[str, Any]:
        return ledger.get_version(version)

    @app.get("/v1/dependencies")
    def dependencies() -> dict[str, Any]:
        return ledger.get_dependencies()

    @app.get("/v1/intents")
    def intents(limit: int = 100) -> dict[str, Any]:
        return {"ok": True, "intents": ledger.list_intents(limit)}

    @app.post("/v1/admin/recover")
    def recover() -> dict[str, Any]:
        return ledger.recover()

    @app.get("/v1/admin/verify")
    def verify() -> dict[str, Any]:
        return ledger.verify()

    return app


app = create_app()

