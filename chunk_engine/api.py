from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .engine import Engine
from .errors import BusinessError, E_INTERNAL_ERROR, E_INVALID_REQUEST


LOGGER = logging.getLogger("chunk_engine.api")


def _debug_kill_process() -> None:
    os._exit(97)


def create_app(engine: Engine, *, allow_debug_kill: bool = False) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        engine.recover()
        yield

    app = FastAPI(title="Chunk Column Point Query Engine", version="0.1.0", lifespan=lifespan)
    app.state.engine = engine
    app.state.allow_debug_kill = allow_debug_kill

    @app.exception_handler(BusinessError)
    async def business_error_handler(_: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(exc.as_dict(), status_code=exc.http_status)

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        error = BusinessError(
            E_INVALID_REQUEST,
            "request validation failed",
            {"errors": exc.errors()},
            400,
        )
        return JSONResponse(error.as_dict(), status_code=error.http_status)

    @app.exception_handler(Exception)
    async def internal_error_handler(_: Request, exc: Exception) -> JSONResponse:
        LOGGER.exception("unhandled API error", exc_info=exc)
        error = BusinessError(E_INTERNAL_ERROR, "internal engine error", {"type": type(exc).__name__}, 500)
        return JSONResponse(error.as_dict(), status_code=500)

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "ok": True,
            "service": "chunk-column-point-query",
            "snapshot_loaded": engine.snapshots.try_active() is not None,
            "debug_kill_enabled": app.state.allow_debug_kill,
        }

    @app.get("/snapshots/active")
    def active_snapshot() -> dict[str, Any]:
        return {"ok": True, "snapshot": engine.active_snapshot_info()}

    @app.post("/snapshots/load")
    def load_snapshot(body: dict[str, Any]) -> dict[str, Any]:
        path = body.get("path")
        if not isinstance(path, str) or not path:
            raise BusinessError(E_INVALID_REQUEST, "path must be a non-empty string", {"field": "path"})
        snapshot = engine.load_snapshot(path)
        return {"ok": True, "snapshot": snapshot.info()}

    @app.post("/batches")
    def submit_batch(body: dict[str, Any]) -> dict[str, Any]:
        debug_kill = body.get("debug_kill_after_committed_blocks")
        kill_callback = None
        if debug_kill is not None:
            if not app.state.allow_debug_kill:
                raise BusinessError(
                    E_INVALID_REQUEST,
                    "debug kill hook is disabled",
                    {"hint": "start the server with --allow-debug-kill"},
                )

            kill_callback = _debug_kill_process

        return engine.create_batch(
            body.get("keys"),
            body.get("columns"),
            page_size=body.get("page_size"),
            client_max_page_size=body.get("max_page_size"),
            execution_mode=body.get("execution_mode", "auto"),
            batch_id=body.get("batch_id"),
            consumer_id=body.get("consumer_id"),
            crash_after_committed_blocks=debug_kill,
            crash_callback=kill_callback,
        )

    @app.post("/compare")
    def compare(body: dict[str, Any]) -> dict[str, Any]:
        return engine.compare_paths(
            body.get("keys"),
            body.get("columns"),
            page_size=engine._effective_page_size(body.get("page_size"), body.get("max_page_size")),
        )

    @app.get("/batches")
    def list_batches(limit: int = 50) -> dict[str, Any]:
        return {"ok": True, "batches": engine.db.list_batches(limit)}

    @app.get("/batches/{identifier}")
    def batch_status(identifier: str) -> dict[str, Any]:
        return {"ok": True, "batch": engine.batch_status(identifier)}

    @app.post("/batches/{identifier}/resume")
    def resume_batch(identifier: str) -> dict[str, Any]:
        return {"ok": True, "batch": engine.resume_batch(identifier)}

    @app.get("/results/{handle}/pages/{page_no}")
    def get_page(
        handle: str,
        page_no: int,
        consumer_id: str | None = Query(default=None),
        x_consumer_id: str | None = Header(default=None, alias="X-Consumer-Id"),
    ) -> dict[str, Any]:
        consumer = consumer_id or x_consumer_id
        return {"ok": True, "page": engine.get_page(handle, page_no, consumer or "")}

    @app.get("/results/{handle}/verify-pagination")
    def verify_pagination(handle: str) -> dict[str, Any]:
        return engine.verify_pagination(handle)

    @app.get("/memory")
    def memory() -> dict[str, Any]:
        return {
            "ok": True,
            "total_budget_bytes": engine.config.total_memory_budget_bytes,
            "single_task_budget_bytes": engine.config.single_task_budget_bytes,
            "usage": engine.db.memory_usage(),
        }

    @app.get("/admin/recovery")
    def recovery_report() -> dict[str, Any]:
        return {"ok": True, "report": engine.db.latest_recovery_report()}

    @app.post("/admin/recover")
    def recover() -> dict[str, Any]:
        return {"ok": True, "report": engine.recover()}

    return app
