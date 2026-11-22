"""FastAPI surface for the local governance workbench."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from .errors import AppError, ErrorCode, error_response
from .service import DecisionExecution, GovernanceService


def _decision_http(execution: DecisionExecution) -> JSONResponse:
    decision = execution.decision
    headers = {"Idempotent-Replayed": "true"} if execution.replayed else {}
    if decision["decision"] == "ACCEPTED":
        return JSONResponse(status_code=200, content=decision, headers=headers)
    payload = {
        "error": {
            "code": decision["code"],
            "message": decision["message"],
            "details": decision["details"],
        },
        "decision": decision,
    }
    return JSONResponse(
        status_code=int(decision["http_status"]), content=payload, headers=headers
    )


def create_app(service: GovernanceService | None = None, *, run_startup: bool = True) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        active_service = service or GovernanceService(
            os.environ.get("GOVERNANCE_DB", "data/governance.db")
        )
        app.state.service = active_service
        app.state.startup_report = active_service.startup() if run_startup else None
        yield

    app = FastAPI(
        title="Kubernetes-style Multi-tenant Resource Governance Workbench",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.exception_handler(AppError)
    async def app_error_handler(request: Request, exc: AppError):
        return JSONResponse(status_code=exc.http_status, content=exc.as_response())

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(request: Request, exc: StarletteHTTPException):
        if exc.status_code == 404:
            code = ErrorCode.E_NOT_FOUND
        elif exc.status_code >= 500:
            code = ErrorCode.E_INTERNAL
        else:
            code = ErrorCode.E_REQUEST_INVALID
        return JSONResponse(
            status_code=exc.status_code,
            content=error_response(
                code,
                str(exc.detail),
                {"path": str(request.url.path), "status": exc.status_code},
            ),
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(request: Request, exc: RequestValidationError):
        return JSONResponse(
            status_code=400,
            content=error_response(
                ErrorCode.E_REQUEST_INVALID,
                "request validation failed",
                {"errors": exc.errors()},
            ),
        )

    @app.exception_handler(Exception)
    async def unhandled_error_handler(request: Request, exc: Exception):
        return JSONResponse(
            status_code=500,
            content=error_response(
                ErrorCode.E_INTERNAL,
                "unexpected internal error",
                {"exception_type": type(exc).__name__},
            ),
        )

    @app.get("/health")
    def health():
        return {"status": "ok", "service": "governance-workbench"}

    @app.get("/api/meta")
    def meta():
        return app.state.service.meta()

    @app.get("/api/snapshot")
    def current_snapshot():
        return app.state.service.current_snapshot()

    @app.put("/api/snapshot")
    def load_snapshot(body: dict[str, Any]):
        document = body
        parent_map = None
        if "document" in body:
            document = body["document"]
            parent_map = body.get("parent_map")
        if not isinstance(document, dict):
            raise AppError(ErrorCode.E_SPEC_INVALID, "document must be an object")
        if parent_map is not None and not isinstance(parent_map, dict):
            raise AppError(ErrorCode.E_SPEC_INVALID, "parent_map must be an object")
        return app.state.service.load_snapshot(document, parent_map=parent_map)

    @app.patch("/api/snapshot")
    def incremental_snapshot(body: dict[str, Any]):
        return app.state.service.submit_incremental(body)

    @app.post("/api/decisions")
    def create_decision(body: dict[str, Any]):
        return _decision_http(app.state.service.submit_decision(body))

    @app.post("/api/decisions/batch")
    def create_batch(body: dict[str, Any]):
        result = app.state.service.submit_batch(body)
        for item in result["items"]:
            decision = item["decision"]
            item["error"] = (
                None
                if decision["decision"] == "ACCEPTED"
                else {
                    "code": decision["code"],
                    "message": decision["message"],
                    "details": decision["details"],
                }
            )
        return result

    @app.get("/api/decisions")
    def list_decisions(
        page: int = Query(1, ge=1),
        page_size: int = Query(50, ge=1, le=1000),
        revision: int | None = Query(None, ge=0),
    ):
        return app.state.service.list_decisions(
            page=page, page_size=page_size, revision=revision
        )

    @app.get("/api/decisions/{request_id}")
    def get_decision(request_id: str, revision: int | None = Query(None, ge=0)):
        return app.state.service.get_decision(request_id, revision=revision)

    @app.get("/api/tenants")
    def list_tenants(revision: int | None = Query(None, ge=0)):
        return app.state.service.list_tenants(revision=revision)

    @app.post("/api/tenants/{tenant_id}/state")
    def transition_state(tenant_id: str, body: dict[str, Any]):
        return app.state.service.transition_state(
            tenant_id,
            body.get("target_state"),
            release=body.get("release"),
            reason=body.get("reason"),
        )

    @app.post("/api/migrations")
    def migrate(body: dict[str, Any]):
        report = app.state.service.migrate_legacy(body)
        if report["code"] == "OK":
            return report
        payload = {
            "error": {
                "code": report["code"],
                "message": (
                    "migration requires human review; the batch was rolled back atomically"
                    if report["code"] == ErrorCode.E_MIGRATION_REVIEW
                    else "migration was rejected; the batch was rolled back atomically"
                ),
                "details": {
                    "migration_id": report["migration_id"],
                    "summary": report["summary"],
                },
            },
            "migration": report,
        }
        status = 409 if report["code"] == ErrorCode.E_MIGRATION_REVIEW else 422
        return JSONResponse(status_code=status, content=payload)

    @app.get("/api/migrations")
    def list_migrations(limit: int = Query(50, ge=1, le=500)):
        return app.state.service.list_migrations(limit=limit)

    @app.get("/api/migrations/{migration_id}")
    def get_migration(migration_id: str):
        return app.state.service.get_migration(migration_id)

    @app.post("/api/migrations/{migration_id}/replay")
    def replay_migration(migration_id: str):
        return app.state.service.replay_migration(migration_id)

    @app.get("/api/recovery/latest")
    def latest_recovery():
        return app.state.service.get_latest_recovery()

    return app
