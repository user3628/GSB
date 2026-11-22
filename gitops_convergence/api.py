from __future__ import annotations

import os
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import Body, FastAPI, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .engine import Engine, EngineConfig
from .errors import BusinessError


def create_app(
    db_path: str | Path | None = None,
    config: EngineConfig | None = None,
    *,
    run_recovery: bool = True,
) -> FastAPI:
    path = Path(db_path or os.environ.get("GITOPS_DB_PATH", "data/gitops.db"))
    engine = Engine(path, config)
    recovery_report: dict[str, Any] | None = None

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        nonlocal recovery_report
        if run_recovery:
            recovery_report = engine.recover()
        application.state.recovery_report = recovery_report
        yield

    application = FastAPI(
        title="GitOps Rollback and Convergence Adjudicator",
        version="1.0.0",
        lifespan=lifespan,
    )
    application.state.engine = engine
    application.state.db_path = str(path)

    @application.exception_handler(BusinessError)
    async def business_error_handler(
        request: Request, exc: BusinessError
    ) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    @application.exception_handler(RequestValidationError)
    async def request_validation_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        error = BusinessError(
            "E_SPEC_INVALID",
            "request body or parameters failed validation",
            {"errors": jsonable_encoder(exc.errors())},
        )
        return JSONResponse(status_code=error.http_status, content=error.to_dict())

    @application.exception_handler(sqlite3.Error)
    async def sqlite_error_handler(
        request: Request, exc: sqlite3.Error
    ) -> JSONResponse:
        error = BusinessError(
            "E_INTERNAL", "SQLite persistence operation failed", {"detail": str(exc)}
        )
        return JSONResponse(status_code=500, content=error.to_dict())

    @application.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "component": "gitops-convergence-adjudicator",
            "db_path": str(path),
            "current_version": engine.current_version(),
            "startup_recovery": application.state.recovery_report,
        }

    @application.get("/v1/state-machine")
    def state_machine() -> dict[str, Any]:
        return engine.state_machine()

    @application.get("/v1/snapshots/current")
    def current_snapshot() -> dict[str, Any]:
        version = engine.current_version()
        if version is None:
            raise BusinessError("E_NOT_FOUND", "no committed snapshot exists")
        return engine.get_snapshot_summary(version)

    @application.post("/v1/snapshots", status_code=201)
    def submit_snapshot(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        return engine.submit_snapshot(payload)

    @application.post("/v1/snapshots/{base_version}/increments", status_code=201)
    def submit_increment(
        base_version: int, payload: dict[str, Any] = Body(...)
    ) -> dict[str, Any]:
        return engine.submit_increment(base_version, payload)

    @application.post("/v1/snapshots/{version}/convergence")
    def trigger_convergence(
        version: int, payload: dict[str, Any] | None = Body(default=None)
    ) -> dict[str, Any]:
        return engine.trigger_convergence(version, payload or {})

    @application.post("/v1/snapshots/{version}/plan-preview")
    def preview_plan(
        version: int, payload: dict[str, Any] | None = Body(default=None)
    ) -> dict[str, Any]:
        return engine.preview_plan(version, payload or {})

    @application.get("/v1/snapshots/{version}/plan")
    def active_plan(version: int) -> dict[str, Any]:
        return engine.get_active_plan(version)

    @application.get("/v1/snapshots/{version}/apps")
    def list_apps(
        version: int,
        state: str | None = Query(default=None),
        cluster_id: str | None = Query(default=None),
        limit: int | None = Query(default=None, ge=1),
        offset: int = Query(default=0, ge=0),
    ) -> dict[str, Any]:
        return engine.list_apps(
            version,
            state=state,
            cluster_id=cluster_id,
            limit=limit,
            offset=offset,
        )

    @application.get("/v1/snapshots/{version}/apps/{app_id}")
    def get_app(version: int, app_id: str) -> dict[str, Any]:
        return engine.get_app(version, app_id)

    @application.post("/v1/snapshots/{version}/apps/{app_id}/transition")
    def transition_app(
        version: int,
        app_id: str,
        payload: dict[str, Any] = Body(...),
    ) -> dict[str, Any]:
        if "target_state" not in payload:
            raise BusinessError(
                "E_SPEC_INVALID", "target_state is required", {"app_id": app_id}
            )
        return engine.transition_app(
            version, app_id, payload["target_state"], payload.get("reason")
        )

    @application.get("/v1/snapshots/{version}/cascades")
    def persisted_cascades(version: int) -> dict[str, Any]:
        return engine.get_rollback_cascades(version)

    @application.get("/v1/snapshots/{version}/cascade")
    def compute_cascade(
        version: int, app_id: list[str] = Query(..., min_length=1)
    ) -> dict[str, Any]:
        return engine.compute_cascade(version, app_id)

    @application.get("/v1/snapshots/{version}")
    def snapshot_summary(version: int) -> dict[str, Any]:
        return engine.get_snapshot_summary(version)

    @application.post("/v1/recovery")
    def recover_now() -> dict[str, Any]:
        report = engine.recover()
        application.state.recovery_report = report
        return report

    return application
