from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Query, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from .config import ManagerConfig
from .errors import ErrorCode, ServiceError
from .manager import CheckpointManager


class ShardPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ordinal: int = Field(ge=0)
    name: str | None = None
    data_base64: str
    sha256: str | None = None


class CheckpointWritePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str
    round: int = Field(ge=0)
    scheduler_position: int = Field(ge=0)
    data_cursor: Any
    params: list[ShardPayload] = Field(min_length=1)
    optimizer: list[ShardPayload] = Field(min_length=1)


class PrunePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    checkpoint_ids: list[str] | None = None


class TransitionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_state: str
    reason: str


def _error_response(error: ServiceError) -> JSONResponse:
    return JSONResponse(status_code=error.http_status, content=error.to_dict())


def create_app(
    manager: CheckpointManager | None = None,
    config: ManagerConfig | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        active_manager = manager or CheckpointManager(config or ManagerConfig.from_env())
        app.state.manager = active_manager
        try:
            yield
        finally:
            active_manager.shutdown()

    app = FastAPI(
        title="Local Checkpoint Recovery API",
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.exception_handler(ServiceError)
    async def service_error_handler(_request: Request, exc: ServiceError) -> JSONResponse:
        return _error_response(exc)

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
        return _error_response(
            ServiceError(
                ErrorCode.INVALID_REQUEST,
                "request validation failed",
                {"errors": jsonable_encoder(exc.errors())},
            )
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        if exc.status_code == 404:
            return _error_response(ServiceError(ErrorCode.CHECKPOINT_NOT_FOUND, "route was not found"))
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "ok": False,
                "code": ErrorCode.INVALID_REQUEST,
                "message": str(exc.detail),
                "details": {},
            },
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(_request: Request, exc: Exception) -> JSONResponse:
        return _error_response(
            ServiceError(
                ErrorCode.INTERNAL,
                "unexpected internal error",
                {"exception_type": type(exc).__name__},
            )
        )

    @app.get("/health")
    def health(request: Request) -> dict[str, Any]:
        active: CheckpointManager = request.app.state.manager
        return {"ok": True, "service": "checkpoint-recovery", "data_dir": str(active.config.data_dir)}

    @app.post("/checkpoints", status_code=202)
    def submit_checkpoint(
        payload: CheckpointWritePayload,
        request: Request,
        response: Response,
        wait: bool = Query(default=False),
    ) -> dict[str, Any]:
        active: CheckpointManager = request.app.state.manager
        submission = active.submit_checkpoint(payload.model_dump(), wait=wait)
        if wait:
            response.status_code = 200
        checkpoint = active.get_checkpoint(submission.checkpoint_id)
        return {
            "ok": True,
            "request_id": submission.request_id,
            "checkpoint_id": submission.checkpoint_id,
            "round": submission.round,
            "status": checkpoint["status"],
            "reused": submission.reused,
            "checkpoint": checkpoint,
        }

    @app.get("/write-requests/{request_id}")
    def get_write_request(request_id: str, request: Request) -> dict[str, Any]:
        active: CheckpointManager = request.app.state.manager
        return {"ok": True, "write_request": active.get_write_request(request_id)}

    @app.get("/checkpoints")
    def list_checkpoints(
        request: Request,
        status: str | None = Query(default=None),
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> dict[str, Any]:
        active: CheckpointManager = request.app.state.manager
        items = active.list_checkpoints(status=status, limit=limit)
        return {"ok": True, "count": len(items), "items": items}

    @app.get("/checkpoints/{checkpoint_id}")
    def get_checkpoint(checkpoint_id: str, request: Request) -> dict[str, Any]:
        active: CheckpointManager = request.app.state.manager
        return {"ok": True, "checkpoint": active.get_checkpoint(checkpoint_id)}

    @app.get("/checkpoints/{checkpoint_id}/transitions")
    def get_transitions(checkpoint_id: str, request: Request) -> dict[str, Any]:
        active: CheckpointManager = request.app.state.manager
        return {"ok": True, "items": active.get_transitions(checkpoint_id)}

    @app.post("/checkpoints/{checkpoint_id}/transition")
    def transition_checkpoint(checkpoint_id: str, payload: TransitionPayload, request: Request) -> dict[str, Any]:
        active: CheckpointManager = request.app.state.manager
        checkpoint = active.transition_checkpoint(checkpoint_id, payload.target_state, payload.reason)
        return {"ok": True, "checkpoint": checkpoint}

    @app.post("/prune")
    def prune(payload: PrunePayload, request: Request) -> dict[str, Any]:
        active: CheckpointManager = request.app.state.manager
        return {"ok": True, **active.prune(payload.checkpoint_ids, wait=True)}

    @app.get("/recovery/baseline")
    def recovery_baseline(request: Request) -> dict[str, Any]:
        active: CheckpointManager = request.app.state.manager
        return {"ok": True, **active.get_recovery_baseline()}

    @app.get("/recovery/startup")
    def recovery_startup(request: Request) -> dict[str, Any]:
        active: CheckpointManager = request.app.state.manager
        return {"ok": True, "startup_recovery": active.startup_recovery}

    @app.get("/recovery/report")
    def recovery_report(
        request: Request,
        target_round: int | None = Query(default=None, ge=0),
        target_scheduler_position: int | None = Query(default=None, ge=0),
        target_data_cursor: str | None = Query(default=None),
    ) -> dict[str, Any]:
        active: CheckpointManager = request.app.state.manager
        cursor: Any = None
        if target_data_cursor is not None:
            try:
                cursor = json.loads(target_data_cursor)
            except json.JSONDecodeError:
                cursor = target_data_cursor
        report = active.get_recovery_report(target_round, target_scheduler_position, cursor)
        return {"ok": True, "report": report}

    return app

