from __future__ import annotations

import os
import re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal

from fastapi import Body, FastAPI, Header, Query, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from .compat import FORMAT_COMPACT, FORMAT_JSONL
from .db import Database
from .errors import E_BAD_REQUEST, E_INTERNAL, E_NOT_FOUND, MQError
from .recovery import latest_recovery_report, recover_pending_intents
from .service import MQService


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class QueueCreateRequest(StrictModel):
    name: str
    semantics: Literal["at_least_once", "exactly_once"]
    visibility_timeout_ms: int = Field(default=30_000, ge=1, le=3_600_000)
    extensions: dict[str, Any] = Field(default_factory=dict)


class QueueMigrateRequest(StrictModel):
    semantics: Literal["at_least_once", "exactly_once"]
    visibility_timeout_ms: int | None = Field(default=None, ge=1, le=3_600_000)


class EnqueueRequest(StrictModel):
    body: str
    message_id: str | None = None
    content_type: str = "text/plain"
    ttl_ms: int = 60_000
    produced_at_ms: int | None = Field(default=None, ge=0)
    extensions: dict[str, Any] = Field(default_factory=dict)


class ClaimRequest(StrictModel):
    consumer_id: str
    visibility_timeout_ms: int | None = Field(default=None, ge=1, le=3_600_000)


class AckItem(StrictModel):
    message_id: str
    lease_token: str = ""


class AckRequest(StrictModel):
    consumer_id: str
    acks: list[AckItem] = Field(default_factory=list)


def _error_response(error: MQError) -> JSONResponse:
    return JSONResponse(status_code=error.http_status, content=error.as_dict())


def create_app(
    db_path: str | Path | None = None,
    *,
    enable_failpoints: bool | None = None,
) -> FastAPI:
    resolved_path = Path(
        db_path or os.environ.get("MSMQ_DB_PATH", "artifacts/msmq.sqlite3")
    ).resolve()
    if enable_failpoints is None:
        enable_failpoints = os.environ.get("MSMQ_ENABLE_FAILPOINTS", "0") == "1"
    database = Database(resolved_path)
    service = MQService(database, enable_failpoints=enable_failpoints)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        database.initialize()
        recover_pending_intents(database)
        yield

    app = FastAPI(
        title="MSMQ Local Delivery Semantics Middleware",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.database = database
    app.state.service = service
    app.state.db_path = resolved_path
    app.state.enable_failpoints = enable_failpoints

    @app.exception_handler(MQError)
    async def mq_error_handler(request: Request, exc: MQError) -> JSONResponse:
        return _error_response(exc)

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        error = MQError(
            E_BAD_REQUEST,
            "request validation failed",
            http_status=422,
            details={"errors": jsonable_encoder(exc.errors())},
        )
        return _error_response(error)

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = E_NOT_FOUND if exc.status_code == 404 else E_BAD_REQUEST
        return _error_response(
            MQError(code, str(exc.detail), http_status=exc.status_code)
        )

    @app.exception_handler(Exception)
    async def internal_error_handler(request: Request, exc: Exception) -> JSONResponse:
        return _error_response(
            MQError(E_INTERNAL, "internal server error", http_status=500, details={"type": type(exc).__name__})
        )

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "ok": True,
            "service": "msmq",
            "db_path": str(resolved_path),
            "failpoints_enabled": enable_failpoints,
        }

    @app.get("/v1/compatibility")
    def compatibility() -> dict[str, Any]:
        return {"ok": True, "compatibility": service.compatibility_matrix()}

    @app.post("/v1/queues", status_code=201)
    def create_queue(payload: QueueCreateRequest) -> dict[str, Any]:
        queue = service.create_queue(
            name=payload.name,
            semantics=payload.semantics,
            visibility_timeout_ms=payload.visibility_timeout_ms,
            extensions=payload.extensions,
        )
        return {"ok": True, "queue": queue}

    @app.get("/v1/queues/{queue}")
    def get_queue(queue: str) -> dict[str, Any]:
        return {"ok": True, "queue": service.get_queue(queue)}

    @app.post("/v1/queues/{queue}/migrate-semantics")
    def migrate_queue(queue: str, payload: QueueMigrateRequest) -> dict[str, Any]:
        return {
            "ok": True,
            "migration": service.migrate_queue(
                name=queue,
                semantics=payload.semantics,
                visibility_timeout_ms=payload.visibility_timeout_ms,
            ),
        }

    @app.post("/v1/queues/{queue}/messages", status_code=201)
    def enqueue(queue: str, payload: EnqueueRequest) -> dict[str, Any]:
        return {
            "ok": True,
            "message": service.enqueue(
                queue=queue,
                body=payload.body,
                message_id=payload.message_id,
                content_type=payload.content_type,
                ttl_ms=payload.ttl_ms,
                produced_at_ms=payload.produced_at_ms,
                extensions=payload.extensions,
            ),
        }

    @app.post("/v1/queues/{queue}/claim")
    def claim(
        queue: str,
        payload: ClaimRequest,
        x_msmq_failpoint: str | None = Header(default=None, alias="X-MSMQ-Failpoint"),
    ) -> dict[str, Any]:
        return {
            "ok": True,
            **service.claim(
                queue=queue,
                consumer_id=payload.consumer_id,
                visibility_timeout_ms=payload.visibility_timeout_ms,
                failpoint=x_msmq_failpoint,
            ),
        }

    @app.post("/v1/queues/{queue}/acks")
    def ack_batch(
        queue: str,
        payload: AckRequest,
        x_msmq_failpoint: str | None = Header(default=None, alias="X-MSMQ-Failpoint"),
    ) -> dict[str, Any]:
        return {
            "ok": True,
            **service.ack_batch(
                queue=queue,
                consumer_id=payload.consumer_id,
                acks=[item.model_dump() for item in payload.acks],
                failpoint=x_msmq_failpoint,
            ),
        }

    @app.post("/v1/queues/{queue}/ack")
    def ack_one(
        queue: str,
        payload: AckItem,
        consumer_id: str = Query(...),
        x_msmq_failpoint: str | None = Header(default=None, alias="X-MSMQ-Failpoint"),
    ) -> dict[str, Any]:
        response = service.ack_batch(
            queue=queue,
            consumer_id=consumer_id,
            acks=[payload.model_dump()],
            failpoint=x_msmq_failpoint,
        )
        result = response["results"][0]
        if not result["ok"]:
            error = result["error"]
            status = 404 if error["code"] in {"E_MESSAGE_NOT_FOUND", "E_QUEUE_NOT_FOUND"} else 409
            raise MQError(
                error["code"],
                error["message"],
                http_status=status,
                details=error.get("details", {}),
            )
        return {"ok": True, "ack": result}

    @app.get("/v1/queues/{queue}/snapshot")
    def snapshot(queue: str) -> dict[str, Any]:
        return {"ok": True, **service.snapshot(queue)}

    @app.get("/v1/queues/{queue}/versions")
    def versions(queue: str, limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
        return {"ok": True, "queue": queue, "versions": service.version_snapshots(queue, limit=limit)}

    @app.get("/v1/export/compact")
    def export_compact(queue: str | None = Query(default=None)) -> Response:
        data = service.export_compact(queue)
        safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", queue or "all")
        filename = f"msmq-{safe_name}.msmq"
        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "X-MSMQ-Format": FORMAT_COMPACT,
                "X-MSMQ-Format-Version": "2",
            },
        )

    @app.get("/v1/export/jsonl")
    def export_jsonl(queue: str | None = Query(default=None)) -> Response:
        data = service.export_jsonl(queue)
        safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", queue or "all")
        filename = f"msmq-{safe_name}.jsonl"
        return Response(
            content=data,
            media_type="application/x-ndjson; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "X-MSMQ-Format": FORMAT_JSONL,
                "X-MSMQ-Format-Version": "1",
            },
        )

    def _import(
        data: bytes,
        expected_format: str,
        sub_ms_policy: str,
        failpoint: str | None,
    ) -> JSONResponse:
        report = service.import_data(
            data,
            expected_format=expected_format,
            sub_ms_policy=sub_ms_policy,
            failpoint=failpoint,
        )
        return JSONResponse(
            status_code=200,
            content={"ok": True, "import_report": report},
            headers={
                "X-MSMQ-Import-Decision": report["decision"],
                "X-MSMQ-Import-Atomic": "true",
            },
        )

    @app.post("/v1/import/compact")
    def import_compact(
        body: bytes = Body(...),
        sub_ms_policy: str = Query(default="truncate"),
        x_msmq_failpoint: str | None = Header(default=None, alias="X-MSMQ-Failpoint"),
    ) -> JSONResponse:
        return _import(body, FORMAT_COMPACT, sub_ms_policy, x_msmq_failpoint)

    @app.post("/v1/import/jsonl")
    def import_jsonl(
        body: bytes = Body(...),
        sub_ms_policy: str = Query(default="truncate"),
        x_msmq_failpoint: str | None = Header(default=None, alias="X-MSMQ-Failpoint"),
    ) -> JSONResponse:
        return _import(body, FORMAT_JSONL, sub_ms_policy, x_msmq_failpoint)

    @app.get("/v1/import/report/latest")
    def latest_import_report_endpoint() -> dict[str, Any]:
        report = database.latest_import_report()
        if report is None:
            raise MQError(E_NOT_FOUND, "no import report exists", http_status=404)
        return {"ok": True, "import_report": report}

    @app.get("/v1/recovery/report")
    def recovery_report() -> dict[str, Any]:
        report = latest_recovery_report(database)
        if report is None:
            raise MQError(E_NOT_FOUND, "no recovery report exists", http_status=404)
        return {"ok": True, "recovery_report": report}

    return app


app = create_app()

