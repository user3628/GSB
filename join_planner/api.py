from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from .config import PlannerConfig
from .errors import PlannerError, invalid_query, snapshot_invalid
from .journal import IntentJournal
from .optimizer import PlanOptimizer
from .query import QueryCatalog
from .service import PlanService
from .snapshot import SnapshotStore


logger = logging.getLogger(__name__)


class SnapshotLoadRequest(BaseModel):
    filename: str = Field(min_length=1)


class PlanRequest(BaseModel):
    query_id: str = Field(min_length=1)
    snapshot_version: int = Field(ge=1)
    mode: Literal["fast", "exact"] = "exact"


def _error_response(error: PlannerError) -> JSONResponse:
    return JSONResponse(status_code=error.status_code, content=error.as_dict())


def create_app(
    snapshots: SnapshotStore,
    catalog: QueryCatalog,
    journal: IntentJournal,
    config: PlannerConfig,
) -> FastAPI:
    optimizer = PlanOptimizer(config)
    service = PlanService(snapshots, catalog, journal, optimizer, config)

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        report = journal.recover()
        application.state.startup_recovery_report = report
        yield

    app = FastAPI(
        title="Deterministic Local Join Planner",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.snapshots = snapshots
    app.state.catalog = catalog
    app.state.journal = journal
    app.state.plan_service = service
    app.state.config = config

    @app.exception_handler(PlannerError)
    async def planner_error_handler(_request: Request, exc: PlannerError) -> JSONResponse:
        return _error_response(exc)

    @app.exception_handler(RequestValidationError)
    async def request_validation_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = []
        for detail in exc.errors():
            errors.append(
                {
                    "location": list(detail.get("loc", [])),
                    "message": str(detail.get("msg", "invalid value")),
                    "type": str(detail.get("type", "validation_error")),
                }
            )
        return _error_response(
            invalid_query("Request body or path parameters failed validation.", errors=errors)
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = "E_NOT_FOUND" if exc.status_code == 404 else "E_HTTP_ERROR"
        return JSONResponse(
            status_code=exc.status_code,
            content={"code": code, "message": str(exc.detail), "details": {}},
        )

    @app.exception_handler(Exception)
    async def unhandled_error_handler(_request: Request, exc: Exception) -> JSONResponse:
        logger.exception("unhandled planner error", exc_info=True)
        return _error_response(
            PlannerError(
                "E_INTERNAL",
                "An unexpected internal error occurred.",
                {"error_type": type(exc).__name__},
            )
        )

    @app.get("/health")
    def health() -> dict[str, Any]:
        current = snapshots.current
        return {
            "status": "ok",
            "snapshot_version": current.version if current else None,
            "max_relations": config.max_relations,
            "fast_candidate_budget": config.fast_candidate_budget,
        }

    @app.get("/v1/snapshots/current")
    def current_snapshot() -> dict[str, Any]:
        current = snapshots.current
        if current is None:
            raise snapshot_invalid("No snapshot is active.")
        return current.public_metadata()

    @app.post("/v1/snapshots/load")
    def load_snapshot(request: SnapshotLoadRequest) -> dict[str, Any]:
        previous = snapshots.current
        current = snapshots.load_file(request.filename)
        return {
            "status": "activated",
            "previous_version": previous.version if previous else None,
            "snapshot": current.public_metadata(),
        }

    @app.get("/v1/queries")
    def list_queries() -> dict[str, Any]:
        return {"queries": catalog.list_public()}

    @app.post("/v1/plans")
    def submit_plan(request: PlanRequest, response: Response) -> dict[str, Any]:
        plan, cache_status = service.submit(request.query_id, request.snapshot_version, request.mode)
        response.headers["X-Plan-Result"] = "reused" if cache_status == "hit" else "built"
        return plan

    @app.get("/v1/plans/{query_id}")
    def get_materialized_plan(
        query_id: str,
        mode: Literal["fast", "exact"] | None = None,
        snapshot_version: int | None = None,
    ) -> dict[str, Any]:
        return service.get_materialized(query_id, mode, snapshot_version)

    @app.get("/v1/recovery/report")
    def recovery_report() -> dict[str, Any]:
        return journal.latest_recovery_report()

    return app


def create_runtime_app(
    snapshot_dir: Path,
    query_catalog_path: Path,
    runtime_dir: Path,
    initial_snapshot_filename: str | None = "snapshot_v1.json",
    config: PlannerConfig | None = None,
) -> FastAPI:
    resolved_config = config or PlannerConfig()
    snapshots = SnapshotStore(snapshot_dir)
    if initial_snapshot_filename is not None:
        snapshots.load_file(initial_snapshot_filename)
    catalog = QueryCatalog(query_catalog_path)
    journal = IntentJournal(runtime_dir)
    return create_app(snapshots, catalog, journal, resolved_config)

