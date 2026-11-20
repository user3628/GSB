from __future__ import annotations

import os
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .db import Store, utc_now
from .errors import WorkbenchError, error_payload
from .models import validate_plan_payload
from .service import WorkbenchService


DEFAULT_DB_PATH = Path("data") / "workbench.db"


def _database_path(explicit: str | Path | None = None) -> Path:
    if explicit is not None:
        return Path(explicit)
    return Path(os.getenv("CAPACITY_DB", str(DEFAULT_DB_PATH)))


def create_app(database_path: str | Path | None = None) -> FastAPI:
    store = Store(_database_path(database_path))
    service = WorkbenchService(store)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        service.initialize()
        app.state.store = store
        app.state.service = service
        yield

    app = FastAPI(
        title="Capacity Workbench",
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.exception_handler(WorkbenchError)
    async def workbench_error_handler(
        request: Request, exc: WorkbenchError
    ) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.payload())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        is_plan_submission = request.url.path.rstrip("/") == "/plans"
        code = "E_PLAN_INVALID" if is_plan_submission else "E_REQUEST_INVALID"
        return JSONResponse(
            status_code=422,
            content=error_payload(
                code,
                "Request validation failed",
                jsonable_encoder(exc.errors()),
            ),
        )

    @app.exception_handler(sqlite3.IntegrityError)
    async def integrity_error_handler(
        request: Request, exc: sqlite3.IntegrityError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content=error_payload(
                "E_STORAGE_CONFLICT", "A persisted invariant rejected the write"
            ),
        )

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {
            "ok": True,
            "database": str(store.path),
            "recovery_performed": service.recovery_report is not None,
            "time": utc_now(),
        }

    @app.post("/plans", status_code=201)
    async def create_plan(payload: dict[str, Any] = Body(...)) -> JSONResponse:
        document = validate_plan_payload(payload)
        report = service.create_plan(document)
        return JSONResponse(status_code=201, content=report)

    @app.get("/plans")
    async def list_plans() -> dict[str, Any]:
        return {"plans": store.list_plans()}

    @app.get("/plans/{plan_id}")
    async def get_plan(plan_id: str) -> dict[str, Any]:
        return service.report(plan_id)

    @app.get("/plans/{plan_id}/report")
    async def get_plan_report(plan_id: str) -> dict[str, Any]:
        return service.report(plan_id)

    @app.get("/plans/{plan_id}/rounds")
    async def list_rounds(plan_id: str) -> dict[str, Any]:
        store.get_plan(plan_id)
        return {"rounds": store.list_rounds(plan_id)}

    @app.get("/plans/{plan_id}/snapshot")
    async def round_snapshot(plan_id: str) -> dict[str, Any]:
        store.get_plan(plan_id)
        return store.snapshot_round_states(plan_id)

    @app.get("/plans/{plan_id}/rounds/{stage_index}")
    async def latest_stage_round(plan_id: str, stage_index: int) -> dict[str, Any]:
        latest = store.latest_round(plan_id, stage_index)
        if latest is None:
            raise WorkbenchError(
                "E_ROUND_NOT_FOUND",
                "No round exists for this stage",
                http_status=404,
                details={"plan_id": plan_id, "stage_index": stage_index},
            )
        return service.round_detail(latest.id)

    @app.post("/plans/{plan_id}/rounds/{stage_index}/execute")
    async def execute_stage(plan_id: str, stage_index: int) -> dict[str, Any]:
        return await service.execute_stage(plan_id, stage_index)

    @app.post("/plans/{plan_id}/run")
    async def run_plan(plan_id: str) -> dict[str, Any]:
        return await service.run_plan(plan_id)

    @app.get("/rounds/{round_id}")
    async def get_round(round_id: str) -> dict[str, Any]:
        return service.round_detail(round_id)

    @app.post("/admin/recover")
    async def recover() -> dict[str, Any]:
        report = await service.recover()
        return {"report": report}

    @app.get("/recovery-reports")
    async def recovery_reports() -> dict[str, Any]:
        return {"reports": store.list_recovery_reports()}

    return app


app = create_app()

