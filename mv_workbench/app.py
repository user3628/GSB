from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from . import __version__
from .errors import WorkbenchError
from .service import DEFAULT_DB_PATH, DEFAULT_SAMPLES_DIR, WorkbenchService
from .samples import load_json, sample_paths
from .util import parse_json_object


class RefreshBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str = Field(min_length=1)
    mode: Literal["all_stale", "all", "views"] = "all_stale"
    views: list[str] | None = None
    include_downstream: bool = True
    delay_ms: int = Field(default=0, ge=0, le=10_000)
    fail_views: list[str] | None = None


class StateTransitionBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    view: str = Field(min_length=1)
    state: str = Field(min_length=1)
    reason: str = "admin transition"
    gaps: list[dict[str, Any]] | None = None


def _ok(data: Any) -> dict[str, Any]:
    return {"ok": True, "data": data}


def create_app(
    db_path: str | Path | None = None,
    *,
    samples_dir: str | Path | None = None,
) -> FastAPI:
    resolved_db = Path(
        db_path or os.environ.get("MV_WORKBENCH_DB", str(DEFAULT_DB_PATH))
    )
    resolved_samples = Path(
        samples_dir or os.environ.get("MV_WORKBENCH_SAMPLES", str(DEFAULT_SAMPLES_DIR))
    )

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        service = WorkbenchService(resolved_db, samples_dir=resolved_samples)
        report = service.initialize()
        application.state.service = service
        application.state.startup_recovery = report
        yield

    application = FastAPI(
        title="Incremental Materialized View Workbench",
        version=__version__,
        description=(
            "Deterministic local workbench for incremental materialized views, "
            "dependency invalidation diagnostics, and crash recovery."
        ),
        lifespan=lifespan,
    )

    @application.exception_handler(WorkbenchError)
    async def workbench_error_handler(_: Request, exc: WorkbenchError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content=exc.as_dict())

    @application.exception_handler(RequestValidationError)
    async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "error": {
                    "code": "E_INVALID_REQUEST",
                    "message": "Request validation failed.",
                    "context": {"errors": jsonable_encoder(exc.errors())},
                },
            },
        )

    @application.exception_handler(Exception)
    async def unexpected_error_handler(_: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "error": {
                    "code": "E_INTERNAL",
                    "message": "Unexpected local workbench failure.",
                    "context": {"detail": str(exc)},
                },
            },
        )

    def service(request: Request) -> WorkbenchService:
        return request.app.state.service

    @application.get("/health")
    def health(request: Request) -> dict[str, Any]:
        return _ok(
            {
                "service": "mv-workbench",
                "version": __version__,
                "db": str(resolved_db),
                "samples": str(resolved_samples),
                "startup_recovery": request.app.state.startup_recovery,
            }
        )

    @application.get("/api/samples")
    async def sample_manifest() -> dict[str, Any]:
        manifest_path = sample_paths(resolved_samples)["manifest"]
        if not manifest_path.exists():
            raise WorkbenchError(
                "E_NOT_FOUND",
                "Sample manifest does not exist.",
                context={"path": str(manifest_path)},
                status_code=404,
            )
        return _ok(load_json(manifest_path))

    @application.get("/api/base-tables")
    def base_tables(request: Request) -> dict[str, Any]:
        return _ok(service(request).list_base_tables())

    @application.get("/api/base-tables/{table_name}")
    def base_table(
        table_name: str,
        request: Request,
        version: int | None = Query(default=None, ge=1),
    ) -> dict[str, Any]:
        return _ok(service(request).get_base_table(table_name, version))

    @application.post("/api/base-tables/{table_name}/batches")
    async def submit_batch(table_name: str, request: Request) -> dict[str, Any]:
        raw = await request.body()
        body = parse_json_object(raw, source=f"batch:{table_name}")
        return _ok(service(request).submit_batch(body, table_name=table_name))

    @application.get("/api/view-definitions")
    def view_definitions(
        request: Request,
        version: int | None = Query(default=None, ge=1),
    ) -> dict[str, Any]:
        return _ok(service(request).get_view_definitions(version))

    @application.put("/api/view-definitions")
    async def replace_view_definitions(request: Request) -> dict[str, Any]:
        raw = await request.body()
        body = parse_json_object(raw, source="view-definitions")
        return _ok(service(request).replace_definitions(body))

    @application.get("/api/graph")
    def graph(
        request: Request,
        version: int | None = Query(default=None, ge=1),
    ) -> dict[str, Any]:
        return _ok(service(request).get_graph(version))

    @application.get("/api/views")
    def views(request: Request) -> dict[str, Any]:
        rows = service(request).list_views()
        return _ok(
            {
                "count": len(rows),
                "views": rows,
            }
        )

    @application.get("/api/views/{view_name}/freshness")
    def view_freshness(view_name: str, request: Request) -> dict[str, Any]:
        return _ok(service(request).get_freshness(view_name))

    @application.get("/api/views/{view_name}")
    def view_content(
        view_name: str,
        request: Request,
        version: int | None = Query(default=None, ge=1),
        delay_ms: int = Query(default=0, ge=0, le=10_000),
    ) -> dict[str, Any]:
        return _ok(
            service(request).query_view(
                view_name, version=version, delay_ms=delay_ms
            )
        )

    @application.post("/api/refreshes")
    def trigger_refresh(body: RefreshBody, request: Request) -> dict[str, Any]:
        return _ok(
            service(request).refresh(
                body.request_id,
                mode=body.mode,
                views=body.views,
                include_downstream=body.include_downstream,
                delay_ms=body.delay_ms,
                fail_views=body.fail_views,
            )
        )

    @application.get("/api/refreshes/{request_id}")
    def refresh_status(request_id: str, request: Request) -> dict[str, Any]:
        return _ok(service(request).get_refresh_request(request_id))

    @application.get("/api/diagnostics")
    def diagnostics(
        request: Request,
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict[str, Any]:
        return _ok(service(request).diagnostics(limit))

    @application.get("/api/recovery")
    def recovery(request: Request) -> dict[str, Any]:
        return _ok(service(request).recovery_report())

    @application.get("/api/recovery/startup")
    def startup_recovery(request: Request) -> dict[str, Any]:
        return _ok(request.app.state.startup_recovery)

    @application.post("/api/admin/state-transition")
    def state_transition(body: StateTransitionBody, request: Request) -> dict[str, Any]:
        return _ok(
            service(request).transition_state(
                body.view,
                body.state,
                reason=body.reason,
                gaps=body.gaps,
            )
        )

    @application.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        page = Path(__file__).with_name("static") / "index.html"
        return HTMLResponse(page.read_text(encoding="utf-8"))

    return application


app = create_app()
