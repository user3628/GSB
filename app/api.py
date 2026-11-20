from __future__ import annotations

import base64
import binascii
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import FastAPI, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.config import Settings
from app.db import Database
from app.errors import BusinessError, error
from app.service import CowService


logger = logging.getLogger("cow_snapshot")


class WriteBlockRequest(BaseModel):
    block_no: int
    content: str | None = None
    content_b64: str | None = None
    sha256: str
    request_id: str | None = None


class SnapshotCreateRequest(BaseModel):
    snapshot_id: str | None = None
    request_id: str | None = None


class IncrementalSnapshotCreateRequest(SnapshotCreateRequest):
    parent_id: str


class MergeRequest(BaseModel):
    target_snapshot_id: str
    request_id: str | None = None
    crash_after_begin: bool = False


class DeleteRequest(BaseModel):
    request_id: str | None = None


class RollbackRequest(BaseModel):
    request_id: str | None = None
    crash_before_commit: bool = False


def _decode_write_content(body: WriteBlockRequest) -> bytes:
    if body.content is not None and body.content_b64 is not None:
        raise error(
            "E_INVALID_REQUEST",
            "Provide exactly one of content or content_b64.",
            422,
        )
    if body.content is not None:
        return body.content.encode("utf-8")
    if body.content_b64 is not None:
        try:
            return base64.b64decode(body.content_b64.encode("ascii"), validate=True)
        except (binascii.Error, UnicodeEncodeError, ValueError) as exc:
            raise error(
                "E_INVALID_REQUEST",
                "content_b64 is not valid base64.",
                422,
            ) from exc
    raise error(
        "E_INVALID_REQUEST",
        "Provide exactly one of content or content_b64.",
        422,
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved_settings = settings or Settings.from_env()
    database = Database(resolved_settings.db_path)

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        service = CowService(database, resolved_settings)
        application.state.service = service
        application.state.recovery = service.initialize()
        yield

    application = FastAPI(
        title="Local COW Snapshot Chain",
        version="1.0.0",
        lifespan=lifespan,
    )

    @application.exception_handler(BusinessError)
    async def business_error_handler(request: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.body())

    @application.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "code": "E_INVALID_REQUEST",
                "message": "Request validation failed.",
                "details": {"errors": jsonable_encoder(exc.errors())},
            },
        )

    @application.exception_handler(StarletteHTTPException)
    async def http_error_handler(
        request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        code = "E_ROUTE_NOT_FOUND" if exc.status_code == 404 else "E_INVALID_REQUEST"
        return JSONResponse(
            status_code=exc.status_code,
            content={"ok": False, "code": code, "message": str(exc.detail)},
        )

    @application.exception_handler(Exception)
    async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled API error")
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "code": "E_INTERNAL",
                "message": "An unexpected internal error occurred.",
                "details": {"error_type": type(exc).__name__},
            },
        )

    def service(request: Request) -> CowService:
        return request.app.state.service

    @application.get("/health")
    def health() -> dict[str, Any]:
        return {
            "ok": True,
            "code": "OK",
            "volume_id": resolved_settings.volume_id,
            "block_count": resolved_settings.block_count,
            "sqlite": database.path.as_posix(),
        }

    @application.post("/api/volumes/{volume_id}/blocks/write")
    def write_block(volume_id: str, body: WriteBlockRequest, request: Request) -> dict[str, Any]:
        content = _decode_write_content(body)
        return service(request).write_block(
            volume_id=volume_id,
            block_no=body.block_no,
            content=content,
            expected_digest=body.sha256,
            request_id=body.request_id,
        )

    @application.post("/api/volumes/{volume_id}/snapshots/full")
    def create_full_snapshot(
        volume_id: str, body: SnapshotCreateRequest, request: Request
    ) -> dict[str, Any]:
        return service(request).create_snapshot(
            volume_id=volume_id,
            mode="full",
            parent_id=None,
            snapshot_id=body.snapshot_id,
            request_id=body.request_id,
        )

    @application.post("/api/volumes/{volume_id}/snapshots/incremental")
    def create_incremental_snapshot(
        volume_id: str, body: IncrementalSnapshotCreateRequest, request: Request
    ) -> dict[str, Any]:
        return service(request).create_snapshot(
            volume_id=volume_id,
            mode="incremental",
            parent_id=body.parent_id,
            snapshot_id=body.snapshot_id,
            request_id=body.request_id,
        )

    @application.post("/api/volumes/{volume_id}/snapshots/{source_snapshot_id}/merge")
    def merge_snapshots(
        volume_id: str,
        source_snapshot_id: str,
        body: MergeRequest,
        request: Request,
    ) -> dict[str, Any]:
        return service(request).merge_snapshots(
            volume_id=volume_id,
            source_snapshot_id=source_snapshot_id,
            target_snapshot_id=body.target_snapshot_id,
            request_id=body.request_id,
            crash_after_begin=body.crash_after_begin,
        )

    @application.post("/api/volumes/{volume_id}/snapshots/{snapshot_id}/delete")
    def delete_snapshot(
        volume_id: str,
        snapshot_id: str,
        body: DeleteRequest,
        request: Request,
    ) -> dict[str, Any]:
        return service(request).delete_snapshot(
            volume_id=volume_id,
            snapshot_id=snapshot_id,
            request_id=body.request_id,
        )

    @application.delete("/api/volumes/{volume_id}/snapshots/{snapshot_id}")
    def delete_snapshot_http(
        volume_id: str,
        snapshot_id: str,
        request: Request,
        request_id: str | None = Query(default=None),
    ) -> dict[str, Any]:
        return service(request).delete_snapshot(
            volume_id=volume_id,
            snapshot_id=snapshot_id,
            request_id=request_id,
        )

    @application.post("/api/volumes/{volume_id}/snapshots/{snapshot_id}/rollback")
    def rollback_snapshot(
        volume_id: str,
        snapshot_id: str,
        body: RollbackRequest,
        request: Request,
    ) -> dict[str, Any]:
        return service(request).rollback_snapshot(
            volume_id=volume_id,
            snapshot_id=snapshot_id,
            request_id=body.request_id,
            crash_before_commit=body.crash_before_commit,
        )

    @application.get("/api/volumes/{volume_id}/chain")
    def snapshot_chain(volume_id: str, request: Request) -> dict[str, Any]:
        return service(request).get_chain(volume_id)

    @application.get("/api/volumes/{volume_id}/snapshots/{snapshot_id}/diff")
    def rollback_diff(volume_id: str, snapshot_id: str, request: Request) -> dict[str, Any]:
        return service(request).get_rollback_diff(volume_id, snapshot_id)

    @application.get("/api/volumes/{volume_id}/snapshots/{snapshot_id}/blocks")
    def snapshot_blocks(
        volume_id: str,
        snapshot_id: str,
        request: Request,
        include_content: bool = Query(default=False),
    ) -> dict[str, Any]:
        return service(request).get_snapshot_blocks(volume_id, snapshot_id, include_content)

    @application.get("/api/volumes/{volume_id}/blocks")
    def current_blocks(volume_id: str, request: Request) -> dict[str, Any]:
        return service(request).get_current_view(volume_id)

    @application.get("/api/volumes/{volume_id}/blocks/{block_no}/versions")
    def block_versions(volume_id: str, block_no: int, request: Request) -> dict[str, Any]:
        return service(request).get_block_versions(volume_id, block_no)

    @application.get("/api/volumes/{volume_id}/recovery-report")
    def recovery_report(volume_id: str, request: Request) -> dict[str, Any]:
        return service(request).get_recovery_report(volume_id)

    return application

