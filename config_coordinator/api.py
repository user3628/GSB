"""FastAPI adapter exposing the coordinator's business operations."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .coordinator import Coordinator
from .errors import DomainError, E_INTERNAL, E_VALIDATION
from .models import (
    DispatchRequest,
    IncrementRequest,
    SnapshotSubmitRequest,
    TransitionRequest,
)


ERROR_CATALOG = {
    "E_STALE_SNAPSHOT": "full snapshot version is not greater than current",
    "E_STALE_BASE": "increment base version is stale",
    "E_SNAPSHOT_INVALID": "snapshot checksum is invalid",
    "E_DEPENDENCY_CYCLE": "component dependency graph contains a cycle",
    "E_INVALID_TRANSITION": "component state transition is illegal",
    "E_PUSH_TIMEOUT": "cluster push exceeded its real HTTP deadline",
    "E_PUSH_REJECTED": "cluster endpoint rejected the configuration",
    "E_PUSH_CONNECTION": "cluster endpoint connection failed",
    "E_CONFLICTING_PUSH": "idempotency key conflicts with a different payload",
    "E_DISPATCH_NOT_FOUND": "dispatch run was not found",
    "E_DISPATCH_IN_PROGRESS": "another writer owns this snapshot dispatch",
}


def create_app(
    db_path: str | None = None,
    *,
    coordinator: Coordinator | None = None,
    recover_on_start: bool = True,
) -> FastAPI:
    selected_db = db_path or os.environ.get("CONFIG_COORDINATOR_DB", "coordinator.db")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.coordinator = coordinator or Coordinator(selected_db)
        app.state.recovery_report = (
            app.state.coordinator.recover() if recover_on_start else None
        )
        yield

    app = FastAPI(
        title="Multi-cluster configuration distribution coordinator",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.exception_handler(DomainError)
    async def domain_error_handler(_: Request, exc: DomainError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        _: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "code": E_VALIDATION,
                "message": "request validation failed",
                "details": {"errors": exc.errors()},
            },
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(_: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "code": E_INTERNAL,
                "message": "unexpected coordinator failure",
                "details": {"exception": type(exc).__name__},
            },
        )

    def domain(request: Request) -> Coordinator:
        return request.app.state.coordinator

    @app.get("/health")
    def health(request: Request) -> dict[str, Any]:
        return {
            "ok": True,
            "current_version": domain(request).current_version(),
        }

    @app.get("/errors")
    def errors() -> dict[str, Any]:
        return {"error_catalog": ERROR_CATALOG}

    @app.post("/snapshots")
    def submit_snapshot(body: SnapshotSubmitRequest, request: Request) -> dict[str, Any]:
        return domain(request).submit_snapshot(body.snapshot, body.components)

    @app.post("/increments")
    def submit_increment(body: IncrementRequest, request: Request) -> dict[str, Any]:
        return domain(request).submit_increment(body)

    @app.get("/state")
    def state(request: Request, version: int | None = None) -> dict[str, Any]:
        return domain(request).get_state(version=version)

    @app.get("/snapshots/{version}")
    def snapshot(version: int, request: Request) -> dict[str, Any]:
        return domain(request).get_snapshot(version)

    @app.get("/snapshots/{version}/derived")
    def derived(version: int, request: Request) -> dict[str, Any]:
        return domain(request).get_derived(version)

    @app.get("/impact/{version}")
    def impact(version: int, request: Request) -> dict[str, Any]:
        return domain(request).get_impact(version)

    @app.get("/plans/{version}")
    def plan(version: int, request: Request) -> dict[str, Any]:
        return domain(request).get_plan(version)

    @app.post("/dispatches")
    async def dispatch(body: DispatchRequest, request: Request) -> dict[str, Any]:
        return await domain(request).dispatch(
            body.snapshot_version,
            policy=body.policy,
            timeout_seconds=body.timeout_seconds,
        )

    @app.get("/dispatches/{version}")
    def dispatch_status(version: int, request: Request) -> dict[str, Any]:
        return domain(request).get_dispatch(version)

    @app.get("/components/{component_id}")
    def component_state(
        component_id: str, request: Request, version: int | None = None
    ) -> dict[str, Any]:
        return domain(request).get_component_state(component_id, version)

    @app.post("/components/{component_id}/transitions")
    def transition(
        component_id: str, body: TransitionRequest, request: Request
    ) -> dict[str, Any]:
        return domain(request).transition_component(component_id, body)

    @app.post("/recovery")
    def recovery(request: Request) -> dict[str, Any]:
        report = domain(request).recover()
        request.app.state.recovery_report = report
        return report

    return app


app = create_app()

