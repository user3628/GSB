from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Body, FastAPI, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from .errors import BusinessError
from .service import UploadIndexService

LOGGER = logging.getLogger("upload_index.api")


def _success(data: Any) -> dict[str, Any]:
    return {"ok": True, "data": data}


def create_app(
    *,
    db_path: str | None = None,
    enable_crash_drill: bool | None = None,
) -> FastAPI:
    configured_db = db_path or os.environ.get("UPLOAD_INDEX_DB", "data/upload_index.db")
    configured_crash = (
        enable_crash_drill
        if enable_crash_drill is not None
        else os.environ.get("UPLOAD_INDEX_ENABLE_CRASH_DRILL", "0") == "1"
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        service = UploadIndexService(configured_db, enable_crash_drill=configured_crash)
        service.initialize()
        app.state.service = service
        try:
            yield
        finally:
            service.shutdown()
            service.close()

    app = FastAPI(
        title="Local Object Upload Index",
        version="0.1.0",
        description="SQLite-backed, versioned metadata index for segmented local-object uploads",
        lifespan=lifespan,
    )

    @app.exception_handler(BusinessError)
    async def business_error_handler(_request: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.as_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
        error = BusinessError(
            "E_REQUEST_INVALID",
            "request validation failed",
            details={"errors": jsonable_encoder(exc.errors())},
        )
        return JSONResponse(status_code=error.http_status, content=error.as_dict())

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = "E_ROUTE_NOT_FOUND" if exc.status_code == 404 else "E_HTTP_ERROR"
        error = BusinessError(
            code,
            str(exc.detail),
            http_status=exc.status_code,
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=error.as_dict(),
            headers=exc.headers,
        )

    @app.exception_handler(Exception)
    async def unhandled_error_handler(_request: Request, exc: Exception) -> JSONResponse:
        LOGGER.exception("unhandled request error")
        error = BusinessError("E_INTERNAL", "internal server error")
        return JSONResponse(status_code=500, content=error.as_dict())

    def service(request: Request) -> UploadIndexService:
        return request.app.state.service

    @app.get("/health")
    def health(request: Request) -> dict[str, Any]:
        return _success({"status": "ok", **service(request).get_current_version()})

    @app.post("/sessions")
    def create_session(
        request: Request, payload: dict[str, Any] = Body(...)
    ) -> dict[str, Any]:
        return _success(service(request).create_session(payload))

    @app.post("/sessions/{session_id}/parts")
    def submit_part(
        session_id: str, request: Request, payload: dict[str, Any] = Body(...)
    ) -> dict[str, Any]:
        return _success(service(request).submit_part(session_id, payload))

    @app.post("/sessions/{session_id}/parts/batch")
    def submit_batch(
        session_id: str, request: Request, payload: dict[str, Any] = Body(...)
    ) -> dict[str, Any]:
        return _success(service(request).submit_batch(session_id, payload))

    @app.delete("/sessions/{session_id}/parts/{seq}")
    def delete_part(session_id: str, seq: int, request: Request) -> dict[str, Any]:
        return _success(service(request).delete_part(session_id, seq))

    @app.post("/sessions/{session_id}/merge")
    def merge_session(
        session_id: str, request: Request, payload: dict[str, Any] = Body(...)
    ) -> dict[str, Any]:
        return _success(service(request).merge_session(session_id, payload))

    @app.delete("/sessions/{session_id}")
    def cancel_session(session_id: str, request: Request) -> dict[str, Any]:
        return _success(service(request).cancel_session(session_id))

    @app.get("/sessions/{session_id}/view")
    def session_view(
        session_id: str,
        request: Request,
        version: int | None = Query(default=None),
    ) -> dict[str, Any]:
        return _success(service(request).get_session_view(session_id, version))

    @app.get("/sessions/{session_id}/segments")
    def session_segments(session_id: str, request: Request) -> dict[str, Any]:
        return _success(service(request).get_session_segments(session_id))

    @app.get("/objects/view")
    def object_view_query(
        request: Request,
        object_key: str = Query(...),
        version: int | None = Query(default=None),
    ) -> dict[str, Any]:
        return _success(service(request).get_object_view(object_key, version))

    @app.get("/objects/{object_key}")
    def object_view(
        object_key: str,
        request: Request,
        version: int | None = Query(default=None),
    ) -> dict[str, Any]:
        return _success(service(request).get_object_view(object_key, version))

    @app.get("/content/{sha256}")
    def content_view(
        sha256: str,
        request: Request,
        version: int | None = Query(default=None),
    ) -> dict[str, Any]:
        return _success(service(request).get_content_view(sha256, version))

    @app.get("/versions/current")
    def current_version(request: Request) -> dict[str, Any]:
        return _success(service(request).get_current_version())

    @app.get("/versions")
    def version_trace(
        request: Request,
        since: int = Query(default=0, ge=0),
        limit: int = Query(default=200, ge=1, le=2000),
    ) -> dict[str, Any]:
        return _success(service(request).get_version_trace(since=since, limit=limit))

    @app.post("/maintenance/gc")
    def purge_gc(request: Request) -> dict[str, Any]:
        return _success(service(request).purge_pending_content())

    @app.post("/maintenance/compact")
    def compact(request: Request, payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
        session_id = payload.get("session_id")
        if session_id is None:
            return _success(service(request).compact_all())
        if not isinstance(session_id, str) or not session_id:
            raise BusinessError("E_REQUEST_INVALID", "session_id must be a non-empty string")
        return _success(service(request).compact_session(session_id))

    @app.post("/query-leases")
    def acquire_lease(request: Request, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        version = payload.get("version")
        if not isinstance(version, int) or isinstance(version, bool):
            raise BusinessError("E_REQUEST_INVALID", "version must be an integer")
        ttl_ms = payload.get("ttl_ms", 300_000)
        if not isinstance(ttl_ms, int) or isinstance(ttl_ms, bool):
            raise BusinessError("E_REQUEST_INVALID", "ttl_ms must be an integer")
        return _success(service(request).acquire_query_lease(version, ttl_ms))

    @app.delete("/query-leases/{lease_id}")
    def release_lease(lease_id: str, request: Request) -> dict[str, Any]:
        return _success(service(request).release_query_lease(lease_id))

    @app.post("/versions/retire")
    def retire_versions(
        request: Request, payload: dict[str, Any] = Body(default={})
    ) -> dict[str, Any]:
        min_active_version = payload.get("min_active_version")
        if min_active_version is not None and (
            not isinstance(min_active_version, int) or isinstance(min_active_version, bool)
        ):
            raise BusinessError("E_REQUEST_INVALID", "min_active_version must be an integer")
        return _success(
            service(request).retire_versions(min_active_version=min_active_version)
        )

    @app.get("/recovery/report")
    def recovery_report(request: Request) -> dict[str, Any]:
        return _success(service(request).get_recovery_report())

    @app.get("/diagnostics/current")
    def current_snapshot(request: Request) -> dict[str, Any]:
        return _success(service(request).current_snapshot())

    @app.post("/admin/crash-drill")
    def crash_drill(request: Request, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        service(request).crash_drill(payload)
        return _success({"unexpected": "process survived crash drill"})

    return app


app = create_app()


def main() -> None:
    import uvicorn

    host = os.environ.get("UPLOAD_INDEX_HOST", "127.0.0.1")
    port = int(os.environ.get("UPLOAD_INDEX_PORT", "8000"))
    uvicorn.run("upload_index.api:app", host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()

