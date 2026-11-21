from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.errors import DomainError, error_response
from app.models import FullSnapshotInput, IncrementalSnapshotInput
from app.store import Store


PROJECT_ROOT = Path(__file__).resolve().parent.parent
STATIC_ROOT = Path(__file__).resolve().parent / "static"
SAMPLE_ROOT = PROJECT_ROOT / "samples"
SAMPLE_FILES = {
    "snapshot_v1": "snapshot_v1.json",
    "snapshot_v2": "snapshot_v2.json",
    "increment_image": "increment_image.json",
    "cycle_increment": "cycle_increment.json",
}


def success(data: Any) -> dict[str, Any]:
    return {"ok": True, "data": data}


def create_app(db_path: str | Path | None = None) -> FastAPI:
    resolved_db = Path(
        db_path or os.environ.get("DEPLOYLENS_DB", PROJECT_ROOT / "data" / "workbench.db")
    )
    store = Store(resolved_db)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        store.initialize()
        app.state.startup_recovery = store.recover_pending_intents()
        yield

    app = FastAPI(
        title="DeployLens 多环境一致性工作台",
        version="1.0.0",
        description="Local deterministic deployment consistency workbench.",
        lifespan=lifespan,
    )
    app.state.store = store
    app.state.db_path = resolved_db
    app.mount("/static", StaticFiles(directory=STATIC_ROOT), name="static")

    @app.exception_handler(DomainError)
    async def domain_error_handler(_: Request, exc: DomainError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content=exc.as_response())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        _: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content=error_response(
                "E_SPEC_INVALID",
                "request payload failed schema validation",
                details={"validation_errors": jsonable_encoder(exc.errors())},
            ),
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = "E_NOT_FOUND" if exc.status_code == 404 else "E_HTTP_ERROR"
        return JSONResponse(
            status_code=exc.status_code,
            content=error_response(code, str(exc.detail)),
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(_: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content=error_response(
                "E_INTERNAL", "unexpected local workbench failure", details={}
            ),
        )

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(STATIC_ROOT / "index.html")

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return success(
            {
                "status": "ok",
                "database": str(resolved_db),
                "snapshot_version": store.current_version(),
            }
        )

    @app.post("/api/commits/full", status_code=201)
    def submit_full(payload: FullSnapshotInput) -> dict[str, Any]:
        return success(store.submit_full(payload))

    @app.post("/api/commits/incremental", status_code=201)
    def submit_incremental(payload: IncrementalSnapshotInput) -> dict[str, Any]:
        return success(store.submit_incremental(payload))

    @app.get("/api/commits/{commit_id}")
    def get_commit(commit_id: str) -> dict[str, Any]:
        return success(store.commit_record(commit_id))

    @app.get("/api/commits/{commit_id}/cascade")
    def get_cascade(commit_id: str) -> dict[str, Any]:
        return success(store.cascade_trace(commit_id))

    @app.get("/api/environments")
    def environments(version: int | None = Query(default=None, ge=1)) -> dict[str, Any]:
        return success(store.list_environments(version=version))

    @app.get("/api/environments/{environment}/bundle")
    def environment_bundle(
        environment: str,
        version: int | None = Query(default=None, ge=1),
    ) -> dict[str, Any]:
        return success(store.environment_bundle(environment, version=version))

    @app.get("/api/environments/{environment}/plan")
    def deployment_plan(
        environment: str,
        version: int | None = Query(default=None, ge=1),
        service: str | None = Query(default=None),
        application: str | None = Query(default=None),
    ) -> dict[str, Any]:
        if service is not None and application is not None and service != application:
            raise DomainError(
                "E_SPEC_INVALID",
                "service and application query aliases must agree",
                status_code=400,
                details={"service": service, "application": application},
            )
        return success(
            store.deployment_plan(
                environment,
                version=version,
                service=service or application,
            )
        )

    @app.get("/api/environments/{environment}/topology")
    def topology(
        environment: str,
        version: int | None = Query(default=None, ge=1),
    ) -> dict[str, Any]:
        return success(store.topology(environment, version=version))

    @app.get("/api/environments/{environment}/applications/{application}")
    def application_view(
        environment: str,
        application: str,
        version: int | None = Query(default=None, ge=1),
    ) -> dict[str, Any]:
        return success(
            store.application_view(environment, application, version=version)
        )

    @app.get("/api/environments/{environment}/references")
    def references(
        environment: str,
        version: int | None = Query(default=None, ge=1),
        subject: str | None = Query(default=None),
    ) -> dict[str, Any]:
        return success(
            store.references(environment, version=version, subject=subject)
        )

    @app.get("/api/diagnostics")
    def diagnostics(
        version: int | None = Query(default=None, ge=1),
        baseline: str | None = Query(default=None),
    ) -> dict[str, Any]:
        return success(store.diagnostics(version=version, baseline=baseline))

    @app.post("/api/diagnostics/run")
    def run_diagnostics(
        version: int | None = Query(default=None, ge=1),
        baseline: str | None = Query(default=None),
    ) -> dict[str, Any]:
        result = store.diagnostics(version=version, baseline=baseline)
        result["read_only"] = True
        return success(result)

    @app.get("/api/recovery")
    def recovery() -> dict[str, Any]:
        return success(store.recovery_report())

    @app.post("/api/recovery/scan")
    def recovery_scan() -> dict[str, Any]:
        recovered = store.recover_pending_intents()
        return success({"recovery_scan": recovered, "report": store.recovery_report()})

    @app.get("/api/recovery/pending-intents")
    def pending_intents() -> dict[str, Any]:
        return success({"intents": store.pending_intents()})

    @app.get("/api/samples")
    def samples() -> dict[str, Any]:
        return success({"samples": sorted(SAMPLE_FILES)})

    @app.get("/api/samples/{name}")
    def sample(name: str) -> Any:
        filename = SAMPLE_FILES.get(name)
        if filename is None:
            raise DomainError(
                "E_SAMPLE_NOT_FOUND",
                "local sample does not exist",
                status_code=404,
                details={"name": name, "available": sorted(SAMPLE_FILES)},
            )
        return success(json.loads((SAMPLE_ROOT / filename).read_text(encoding="utf-8")))

    return app


app = create_app()
