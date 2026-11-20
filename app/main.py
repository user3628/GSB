from __future__ import annotations

import logging
import os
import re
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, model_validator
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.errors import BusinessError
from app.service import SchedulerService
from app.storage import Storage

LOGGER = logging.getLogger("scheduler")
ROOT = Path(__file__).resolve().parent.parent
STATIC_DIR = Path(__file__).resolve().parent / "static"
_BATCH_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


def _camel(value: str) -> str:
    head, *tail = value.split("_")
    return head + "".join(piece[:1].upper() + piece[1:] for piece in tail)


class CreateBatchRequest(BaseModel):
    model_config = ConfigDict(alias_generator=_camel, populate_by_name=True, extra="forbid")

    batch_id: str | None = Field(default=None, alias="batchId")
    sample: str | None = None
    spec: dict[str, Any] | None = None

    @model_validator(mode="after")
    def exactly_one_source(self) -> "CreateBatchRequest":
        if (self.sample is None) == (self.spec is None):
            raise ValueError("exactly one of sample or spec must be provided")
        return self


def _batch_envelope(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "batchId": record["batch_id"],
        "status": record["status"],
        "specName": record["spec_name"],
        "intentAt": record["intent_at"],
        "updatedAt": record["updated_at"],
        "completedAt": record["completed_at"],
        "recovered": record["recovered"],
        "result": record["result"],
        "error": record["error"],
    }


def _recovery_envelope(report: dict[str, Any]) -> dict[str, Any]:
    runs = [
        {
            "id": run["id"],
            "trigger": run["trigger"],
            "startedAt": run["started_at"],
            "completedAt": run["completed_at"],
            "scannedCount": run["scanned_count"],
            "recoveredCount": run["recovered_count"],
            "abortedCount": run["aborted_count"],
            "notes": run["notes"],
        }
        for run in report["runs"]
    ]
    events = [
        {
            "id": event["id"],
            "runId": event["run_id"],
            "batchId": event["batch_id"],
            "detectedAt": event["detected_at"],
            "action": event["action"],
            "details": event["details"],
        }
        for event in report["events"]
    ]
    return {
        "generatedAt": report["generatedAt"],
        "scope": report["scope"],
        "runs": runs,
        "events": events,
        "currentIncomplete": report["currentIncomplete"],
        "limitations": report["limitations"],
    }


def create_app(*, db_path: str | Path | None = None, samples_dir: str | Path | None = None) -> FastAPI:
    database = Path(db_path) if db_path is not None else Path(os.getenv("SCHEDULER_DB", ROOT / "data" / "scheduler.db"))
    samples = Path(samples_dir) if samples_dir is not None else Path(os.getenv("SCHEDULER_SAMPLES", ROOT / "samples"))
    storage = Storage(database)
    service = SchedulerService(storage, samples)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        service.recover_incomplete(trigger="startup")
        app.state.service = service
        yield

    app = FastAPI(
        title="Local Kubernetes Scheduling Workbench",
        version="1.0.0",
        description="Offline deterministic filter/score diagnostics, affinity propagation, and intent recovery.",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url="/api/openapi.json",
    )
    app.state.service = service
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.exception_handler(BusinessError)
    async def business_error_handler(_: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content=exc.as_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "E_INVALID_SPEC",
                    "message": "request validation failed",
                    "details": {"errors": jsonable_encoder(exc.errors())},
                }
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = "E_ENDPOINT_NOT_FOUND" if exc.status_code == 404 else "E_HTTP_ERROR"
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": code,
                    "message": str(exc.detail),
                    "details": {"path": request.url.path},
                }
            },
        )

    @app.exception_handler(Exception)
    async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
        LOGGER.error(
            "unhandled API error on %s",
            request.url.path,
            exc_info=(type(exc), exc, exc.__traceback__),
        )
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": "E_INTERNAL",
                    "message": "internal server error",
                    "details": {"path": request.url.path},
                }
            },
        )

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "mode": "offline", "dependency": "local-json+sqlite"}

    @app.get("/api/samples")
    def list_samples() -> dict[str, Any]:
        return {"samples": service.list_samples()}

    @app.get("/api/samples/{sample_name}")
    def get_sample(sample_name: str) -> dict[str, Any]:
        return {"name": sample_name, "spec": service.load_sample(sample_name)}

    @app.post("/api/batches", status_code=201)
    def create_batch(request: CreateBatchRequest) -> dict[str, Any]:
        batch_id = request.batch_id or f"batch-{uuid.uuid4().hex}"
        if not _BATCH_ID.fullmatch(batch_id):
            raise BusinessError(
                "E_INVALID_BATCH_ID",
                "batchId must match [A-Za-z0-9][A-Za-z0-9_.:-]{0,127}",
                details={"batchId": batch_id},
            )
        if request.sample is not None:
            record = service.submit_sample(batch_id=batch_id, sample_name=request.sample)
        else:
            assert request.spec is not None
            record = service.submit_raw_spec(batch_id=batch_id, raw_spec=request.spec)
        return _batch_envelope(record)

    @app.get("/api/batches/{batch_id}")
    def get_batch(batch_id: str) -> dict[str, Any]:
        return _batch_envelope(service.get_batch(batch_id))

    @app.get("/api/batches/{batch_id}/nodes/{node_name}")
    def get_node(batch_id: str, node_name: str) -> dict[str, Any]:
        return service.node_detail(batch_id=batch_id, node_name=node_name)

    @app.get("/api/recovery")
    def recovery_report() -> dict[str, Any]:
        return _recovery_envelope(service.recovery_report())

    @app.post("/api/recovery/scan")
    def recovery_scan() -> dict[str, Any]:
        return _recovery_envelope(service.recover_incomplete(trigger="api"))

    return app


app = create_app()
