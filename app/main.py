from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Callable

from fastapi import Body, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.config import STATIC_DIR, allow_demo_crash, database_path
from app.errors import BusinessError
from app.service import WorkbenchService
from app.store import Store

LOGGER = logging.getLogger("batch-workbench")


def _ok(data: Any, status_code: int = 200) -> JSONResponse:
    return JSONResponse({"ok": True, "data": data}, status_code=status_code)


def _terminate_process(checkpoint: str) -> None:
    os._exit(86)


def create_app(
    *,
    db_path: Path | str | None = None,
    allow_crash: bool | None = None,
    terminate: Callable[[str], None] | None = None,
) -> FastAPI:
    crash_enabled = allow_demo_crash() if allow_crash is None else allow_crash

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        store = Store(db_path or database_path())
        store.recover_pending_intents()
        app.state.store = store
        app.state.service = WorkbenchService(
            store,
            allow_crash=crash_enabled,
            terminate=terminate or (_terminate_process if crash_enabled else None),
        )
        yield

    app = FastAPI(
        title="Local Batch Precision Workbench",
        version="1.0.0",
        description="Deterministic batching, fixed-point latency diagnostics, and crash recovery.",
        lifespan=lifespan,
    )

    @app.exception_handler(BusinessError)
    async def business_error_handler(request: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(exc.as_dict(), status_code=exc.http_status)

    @app.exception_handler(RequestValidationError)
    async def request_validation_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        error = BusinessError(
            "request body failed schema validation",
            details={"errors": exc.errors()},
        )
        error.code = "E_SPEC_INVALID"
        return JSONResponse(error.as_dict(), status_code=422)

    @app.exception_handler(Exception)
    async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
        LOGGER.exception("unhandled workbench error")
        return JSONResponse(
            {
                "ok": False,
                "error": {
                    "code": "E_INTERNAL",
                    "message": "internal workbench error",
                    "details": {"type": type(exc).__name__},
                },
            },
            status_code=500,
        )

    def service(request: Request) -> WorkbenchService:
        return request.app.state.service

    @app.get("/api/health")
    def health(request: Request):
        return _ok(service(request).health())

    @app.get("/api/samples")
    def list_samples(request: Request):
        return _ok({"samples": service(request).list_samples()})

    @app.get("/api/samples/{sample_name}")
    def get_sample(sample_name: str, request: Request):
        return _ok(service(request).sample_payload(sample_name))

    @app.post("/api/traces")
    def submit_trace(request: Request, payload: dict[str, Any] = Body(...)):
        return _ok(service(request).submit_trace(payload), status_code=201)

    @app.get("/api/traces/{trace_id}")
    def get_trace(trace_id: str, request: Request):
        return _ok(service(request).get_trace(trace_id))

    @app.post("/api/traces/{trace_id}/organize")
    def organize_trace(
        trace_id: str,
        request: Request,
        payload: dict[str, Any] | None = Body(default=None),
    ):
        crash_at = None
        if payload and payload.get("demo_crash_at"):
            crash_at = str(payload["demo_crash_at"])
        return _ok(service(request).organize(trace_id, crash_at=crash_at), status_code=201)

    @app.post("/api/demo/run/{sample_name}")
    def run_fixed_sample(
        sample_name: str,
        request: Request,
        payload: dict[str, Any] | None = Body(default=None),
    ):
        svc = service(request)
        sample = svc.sample_payload(sample_name)
        submitted = svc.submit_trace(sample["payload"])
        crash_at = None
        if payload and payload.get("demo_crash_at"):
            crash_at = str(payload["demo_crash_at"])
        run = svc.organize(submitted["trace_id"], crash_at=crash_at)
        return _ok(
            {
                "sample_name": sample_name,
                "trace_submission": {
                    "inserted": submitted["inserted"],
                    "trace_id": submitted["trace_id"],
                    "payload_digest": submitted["payload_digest"],
                },
                "run": run,
            },
            status_code=201,
        )

    @app.get("/api/runs")
    def list_runs(request: Request, limit: int = 20):
        return _ok({"runs": service(request).list_runs(max(1, min(limit, 100)))})

    @app.get("/api/runs/{run_id}")
    def get_run(run_id: str, request: Request):
        return _ok(service(request).get_run(run_id))

    @app.get("/api/runs/{run_id}/quantiles")
    def run_quantiles(run_id: str, request: Request):
        return _ok(service(request).run_quantiles(run_id))

    @app.get("/api/runs/{run_id}/validation")
    @app.get("/api/runs/{run_id}/truth", include_in_schema=False)
    def run_validation(run_id: str, request: Request):
        return _ok(service(request).run_validation(run_id))

    @app.get("/api/batches/{batch_uid}/metrics")
    @app.get("/api/batches/{batch_uid}/quantiles", include_in_schema=False)
    def batch_metrics(batch_uid: str, request: Request):
        return _ok(service(request).batch_metrics(batch_uid))

    @app.get("/api/batches/{batch_uid}/errors")
    def batch_errors(batch_uid: str, request: Request):
        return _ok(service(request).batch_errors(batch_uid))

    @app.get("/api/batches/{batch_uid}/validation")
    def batch_validation(batch_uid: str, request: Request):
        return _ok(service(request).batch_validation(batch_uid))

    @app.get("/api/recovery/report")
    def recovery_report(request: Request):
        return _ok(service(request).recovery_report())

    @app.get("/api/recovery/intents")
    def recovery_intents(request: Request, limit: int = 100):
        return _ok({"intents": service(request).intent_log(max(1, min(limit, 500)))})

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/diagnostics", include_in_schema=False)
    def diagnostics():
        return FileResponse(STATIC_DIR / "index.html")

    return app


app = create_app()
