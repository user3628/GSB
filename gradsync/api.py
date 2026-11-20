from __future__ import annotations

import math
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator, model_validator
from starlette.exceptions import HTTPException as StarletteHTTPException

from .domain import FinalizationPolicy
from .errors import ErrorCode, GradSyncError
from .scheduler import GradientScheduler
from .store import SQLiteStore


class RoundCreateModel(BaseModel):
    round_id: str = Field(min_length=1, max_length=128)
    seed: int = 0
    shard_count: int = Field(gt=0, le=1024)
    shard_dim: int = Field(gt=0, le=100_000)
    worker_ids: list[str] = Field(min_length=1)
    sample_counts: dict[str, int]
    worker_endpoints: dict[str, str] = Field(default_factory=dict)
    policy: FinalizationPolicy = FinalizationPolicy.STRICT
    collect_seconds: float = Field(gt=0, le=86_400)
    max_wait_seconds: float = Field(ge=0, le=86_400)

    @model_validator(mode="after")
    def validate_workers(self) -> "RoundCreateModel":
        if len(set(self.worker_ids)) != len(self.worker_ids):
            raise ValueError("worker_ids 不能重复")
        if set(self.sample_counts) != set(self.worker_ids):
            raise ValueError("sample_counts 的键必须与 worker_ids 完全一致")
        if any(value < 0 for value in self.sample_counts.values()):
            raise ValueError("样本数必须非负")
        return self


class SubmissionModel(BaseModel):
    node_id: str = Field(min_length=1, max_length=128)
    shard_id: int = Field(ge=0)
    sample_count: int = Field(ge=0, le=1_000_000_000)
    gradient: list[float]

    @field_validator("gradient")
    @classmethod
    def validate_finite(cls, value: list[float]) -> list[float]:
        if not value:
            raise ValueError("gradient 不能为空")
        if not all(math.isfinite(item) for item in value):
            raise ValueError("gradient 必须全部为有限浮点数")
        return value


class FinalizeModel(BaseModel):
    mode: FinalizationPolicy | None = None
    deliver: bool = False


def _validation_details(errors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sanitized = []
    for error in errors:
        sanitized.append(
            {
                "location": list(error.get("loc", [])),
                "message": str(error.get("msg", "invalid value")),
                "type": str(error.get("type", "validation_error")),
            }
        )
    return sanitized


def create_app(
    db_path: str | Path | None = None,
    *,
    scheduler: GradientScheduler | None = None,
) -> FastAPI:
    if scheduler is None:
        path = Path(db_path or os.environ.get("GRADSYNC_DB", "artifacts/scheduler.db"))
        scheduler = GradientScheduler(SQLiteStore(path))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.recovery_report = scheduler.recover_pending_intents()
        yield

    app = FastAPI(
        title="GradSync local scheduler",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.scheduler = scheduler

    @app.exception_handler(GradSyncError)
    async def gradsync_error_handler(request: Request, exc: GradSyncError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.payload())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "code": ErrorCode.E_VALIDATION_ERROR.value,
                "message": "请求参数校验失败",
                "round_id": None,
                "details": {"errors": _validation_details(exc.errors())},
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = ErrorCode.E_ROUND_NOT_FOUND if exc.status_code == 404 else ErrorCode.E_INTERNAL
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "ok": False,
                "code": code.value,
                "message": str(exc.detail),
                "round_id": None,
                "details": {"path": request.url.path},
            },
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "code": ErrorCode.E_INTERNAL.value,
                "message": f"内部错误: {type(exc).__name__}",
                "round_id": None,
                "details": {"path": request.url.path},
            },
        )

    @app.get("/health")
    def health() -> dict[str, Any]:
        return scheduler.health()

    @app.post("/rounds", status_code=201)
    def create_round(payload: RoundCreateModel) -> dict[str, Any]:
        round_data = scheduler.create_round(payload.model_dump(mode="json"))
        return {
            "ok": True,
            "round_id": round_data["round_id"],
            "state": round_data["status"],
            "collect_deadline_at": round_data["collect_deadline_at"],
            "max_wait_deadline_at": round_data["max_wait_deadline_at"],
            "policy": round_data["policy"],
        }

    @app.get("/rounds")
    def list_rounds() -> dict[str, Any]:
        rounds = scheduler.store.list_rounds()
        return {
            "ok": True,
            "rounds": [
                {
                    "round_id": item["round_id"],
                    "state": item["status"],
                    "policy": item["policy"],
                    "collect_deadline_at": item["collect_deadline_at"],
                    "max_wait_deadline_at": item["max_wait_deadline_at"],
                    "last_error_code": item["last_error_code"],
                }
                for item in rounds
            ],
        }

    @app.get("/rounds/{round_id}")
    def get_round(round_id: str) -> dict[str, Any]:
        return {"ok": True, **scheduler.get_round_status(round_id)}

    @app.get("/rounds/{round_id}/events")
    def get_events(round_id: str) -> dict[str, Any]:
        events = scheduler.get_events(round_id)
        return {"ok": True, "round_id": round_id, "events": events}

    @app.post("/rounds/{round_id}/submissions")
    def submit(round_id: str, payload: SubmissionModel) -> dict[str, Any]:
        result = scheduler.submit(
            round_id=round_id,
            node_id=payload.node_id,
            shard_id=payload.shard_id,
            sample_count=payload.sample_count,
            gradient=payload.gradient,
        )
        return {"ok": True, "code": ErrorCode.OK.value, **result}

    @app.post("/rounds/{round_id}/finalize")
    def finalize(round_id: str, payload: FinalizeModel | None = None) -> dict[str, Any]:
        request = payload or FinalizeModel()
        result = scheduler.finalize(round_id=round_id, policy=request.mode)
        if request.deliver:
            result["delivery"] = scheduler.deliver(round_id)
        return result

    @app.post("/rounds/{round_id}/deliver")
    def deliver(round_id: str) -> dict[str, Any]:
        return scheduler.deliver(round_id)

    @app.get("/rounds/{round_id}/result")
    def get_result(round_id: str) -> dict[str, Any]:
        return scheduler.get_result(round_id)

    @app.post("/rounds/{round_id}/abort")
    def abort(round_id: str) -> dict[str, Any]:
        round_data = scheduler.abort(round_id)
        return {
            "ok": True,
            "round_id": round_id,
            "state": round_data["status"],
            "events": scheduler.get_events(round_id),
        }

    @app.get("/recovery/report")
    def recovery_report() -> dict[str, Any]:
        return scheduler.get_recovery_report()

    @app.post("/recovery/scan")
    def recovery_scan() -> dict[str, Any]:
        return scheduler.recover_pending_intents()

    return app

