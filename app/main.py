from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from typing import Any, Awaitable, Callable

from fastapi import Body, FastAPI, Header, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.config import database_path, load_settings
from app.db import Database
from app.errors import BusinessError
from app.models import (
    CreateSessionRequest,
    FinalizeSessionRequest,
    ScheduleRoundRequest,
    SubmitPartRequest,
)
from app.scheduler import Scheduler
from app.service import IdempotencyService, SessionService


def create_app(
    *,
    db_file: str | None = None,
    settings_file: str | None = None,
) -> FastAPI:
    settings = load_settings(settings_file)
    database = Database(database_path(db_file))
    sessions = SessionService(database)
    idempotency = IdempotencyService(database)
    scheduler = Scheduler(database, settings)

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        database.initialize(settings)
        application.state.recovery_report = database.recover_after_crash()
        yield

    app = FastAPI(
        title="Local Object Gateway Upload Coordinator",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.database = database
    app.state.settings = settings
    app.state.sessions = sessions
    app.state.idempotency = idempotency
    app.state.scheduler = scheduler

    @app.exception_handler(BusinessError)
    async def business_error_handler(_: Request, error: BusinessError) -> JSONResponse:
        return JSONResponse(status_code=error.status_code, content=error.as_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_: Request, error: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "code": "E_INVALID_REQUEST",
                "message": "request validation failed",
                "context": {"errors": jsonable_encoder(error.errors())},
            },
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(_: Request, error: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "code": "E_INTERNAL",
                "message": "unexpected coordinator failure",
                "context": {"error_type": type(error).__name__},
            },
        )

    async def idempotent_call(
        *,
        header_request_id: str | None,
        body_request_id: str | None,
        operation: str,
        payload: dict[str, Any],
        action: Callable[[], Awaitable[tuple[int, dict[str, Any]]]],
    ) -> JSONResponse:
        request_id = header_request_id or body_request_id
        if request_id is None:
            status_code, response = await action()
            response = {"request_id": None, **response}
            return JSONResponse(status_code=status_code, content=response)
        status_code, response = await idempotency.execute(
            request_id,
            operation,
            payload,
            action,
        )
        response = {"request_id": request_id, **response}
        return JSONResponse(
            status_code=status_code,
            content=response,
            headers={"Idempotency-Key": request_id},
        )

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "global_concurrency": settings.global_concurrency,
            "queue_limit": settings.queue_limit,
            "backends": [backend.id for backend in settings.backends],
        }

    @app.post("/v1/sessions")
    async def create_session(
        request: CreateSessionRequest,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> JSONResponse:
        payload = request.model_dump(mode="json", exclude={"request_id"})

        async def action() -> tuple[int, dict[str, Any]]:
            return 201, sessions.create_session(request)

        return await idempotent_call(
            header_request_id=idempotency_key,
            body_request_id=request.request_id,
            operation="CREATE_SESSION",
            payload=payload,
            action=action,
        )

    @app.post("/v1/sessions/{session_id}/parts")
    async def submit_part(
        session_id: str,
        request: SubmitPartRequest,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> JSONResponse:
        payload = {
            "session_id": session_id,
            **request.model_dump(mode="json", exclude={"request_id"}),
        }

        async def action() -> tuple[int, dict[str, Any]]:
            return 200, sessions.submit_part(session_id, request)

        return await idempotent_call(
            header_request_id=idempotency_key,
            body_request_id=request.request_id,
            operation="SUBMIT_PART",
            payload=payload,
            action=action,
        )

    @app.post("/v1/sessions/{session_id}/finalize")
    async def finalize_session(
        session_id: str,
        request: FinalizeSessionRequest | None = Body(default=None),
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> JSONResponse:
        body = request or FinalizeSessionRequest()
        payload = {"session_id": session_id}

        async def action() -> tuple[int, dict[str, Any]]:
            return 200, sessions.finalize_session(session_id, body)

        return await idempotent_call(
            header_request_id=idempotency_key,
            body_request_id=body.request_id,
            operation="FINALIZE_SESSION",
            payload=payload,
            action=action,
        )

    @app.get("/v1/sessions/{session_id}")
    async def get_session(session_id: str) -> dict[str, Any]:
        return sessions.get_session(session_id)

    @app.get("/v1/sessions/{session_id}/parts")
    async def get_session_parts(session_id: str) -> dict[str, Any]:
        detail = sessions.get_session(session_id)
        return {
            "session_id": session_id,
            "version": detail["version"],
            "read_version": detail["read_version"],
            "total_parts": detail["session"]["total_parts"],
            "status": detail["session"]["status"],
            "parts": detail["parts"],
            "object_metadata": detail["object_metadata"],
        }

    @app.post("/v1/scheduler/rounds")
    async def run_scheduler_round(
        request: ScheduleRoundRequest | None = Body(default=None),
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> JSONResponse:
        body = request or ScheduleRoundRequest()
        payload = body.model_dump(mode="json", exclude={"request_id"})

        async def action() -> tuple[int, dict[str, Any]]:
            return 200, await scheduler.run_round(body)

        return await idempotent_call(
            header_request_id=idempotency_key,
            body_request_id=body.request_id,
            operation="RUN_SCHEDULER_ROUND",
            payload=payload,
            action=action,
        )

    @app.get("/v1/scheduler/rounds/{round_id}")
    async def get_scheduler_round(round_id: str) -> dict[str, Any]:
        return await scheduler.get_round(round_id)

    @app.get("/v1/backends")
    async def get_backends() -> dict[str, Any]:
        return sessions.list_backend_quotas()

    @app.get("/v1/backends/{backend_id}/quota")
    async def get_backend_quota(backend_id: str) -> dict[str, Any]:
        return sessions.backend_quota(backend_id)

    @app.get("/v1/recovery/reports")
    async def get_recovery_reports(limit: int = 20) -> dict[str, Any]:
        reports = database.list_recovery_reports(limit)
        return {
            "reports": reports,
            "latest": reports[0] if reports else None,
            "note": "Recovery reports are written only after a subsequent startup scan.",
        }

    @app.get("/v1/recovery/report")
    async def get_latest_recovery_report() -> dict[str, Any]:
        reports = database.list_recovery_reports(1)
        if not reports:
            raise BusinessError(
                "E_NOT_FOUND",
                "no startup recovery report has been persisted",
                {},
                404,
            )
        return reports[0]

    @app.get("/v1/audit/events")
    async def get_audit_events(limit: int = 100) -> dict[str, Any]:
        return {"events": database.list_audit_events(min(max(limit, 1), 500))}

    return app


app = create_app()
