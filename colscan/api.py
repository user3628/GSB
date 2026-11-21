from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from .errors import BusinessError, error
from .service import ColumnarService


class BudgetRequest(BaseModel):
    max_concurrent_tasks: int = Field(default=4, ge=1)
    per_task_memory_bytes: int = Field(default=1_048_576, ge=1)
    round_time_budget_ms: float = Field(default=100.0, gt=0)
    queue_capacity: int = Field(default=128, ge=0)
    degrade_mode: Literal["split", "strict"] = "split"


class SnapshotLoadRequest(BaseModel):
    path: str


class QueryRequest(BaseModel):
    query_id: str
    columns: list[str]
    predicate: dict[str, Any] | None = None
    block_start: int | None = None
    block_end: int | None = None
    offset: int = Field(default=0, ge=0)
    limit: int | None = Field(default=None, ge=1)
    snapshot_version: int | None = Field(default=None, ge=1)
    budget: BudgetRequest = Field(default_factory=BudgetRequest)
    wait_seconds: float = Field(default=5.0, ge=0, le=60)


def success(data: Any) -> dict[str, Any]:
    return {"ok": True, "data": data}


def create_app(*, state_db: str | Path | None = None, task_delay_ms: int | None = None) -> FastAPI:
    db_path = Path(state_db or os.environ.get("COLSCAN_STATE_DB", "data/state.db"))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        service = ColumnarService(db_path, task_delay_ms=task_delay_ms)
        service.recover_on_startup()
        app.state.service = service
        yield

    app = FastAPI(title="Local Columnar Scan Service", version="1.0.0", lifespan=lifespan)

    @app.exception_handler(BusinessError)
    async def business_error_handler(_request: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
        details = [
            {
                "location": list(issue.get("loc", [])),
                "message": issue.get("msg", "invalid value"),
                "type": issue.get("type", "value_error"),
            }
            for issue in exc.errors()
        ]
        payload = error("E_REQUEST_INVALID", "request validation failed", http_status=422, details={"issues": details})
        return JSONResponse(status_code=422, content=payload.to_dict())

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        if exc.status_code == 404:
            payload = error("E_NOT_FOUND", "route not found", http_status=404)
        else:
            payload = error("E_HTTP_ERROR", str(exc.detail), http_status=exc.status_code)
        return JSONResponse(status_code=exc.status_code, content=payload.to_dict())

    @app.exception_handler(Exception)
    async def unexpected_error_handler(_request: Request, exc: Exception) -> JSONResponse:
        payload = error(
            "E_INTERNAL",
            "unexpected internal error",
            http_status=500,
            details={"type": type(exc).__name__},
        )
        return JSONResponse(status_code=500, content=payload.to_dict())

    def get_service(request: Request) -> ColumnarService:
        return request.app.state.service

    @app.get("/health")
    def health(request: Request) -> dict[str, Any]:
        return success({"status": "ok", "pid": os.getpid(), "instance": get_service(request).instance_id})

    @app.post("/v1/snapshots/load")
    def load_snapshot(body: SnapshotLoadRequest, request: Request) -> dict[str, Any]:
        return success(get_service(request).load_snapshot(body.path))

    @app.get("/v1/snapshots/current")
    def current_snapshot(request: Request) -> dict[str, Any]:
        return success(get_service(request).current_snapshot())

    @app.get("/v1/snapshots/blocks")
    def snapshot_blocks(request: Request, version: int | None = Query(default=None, ge=1)) -> dict[str, Any]:
        return success(get_service(request).snapshot_blocks(version))

    @app.post("/v1/queries", status_code=202)
    def submit_query(body: QueryRequest, request: Request) -> JSONResponse:
        payload = body.model_dump()
        wait_seconds = float(payload.pop("wait_seconds", 0.0))
        status, created = get_service(request).submit_query(payload, wait_seconds=wait_seconds)
        return JSONResponse(status_code=202 if created else 200, content=success({"created": created, "query": status}))

    @app.get("/v1/queries/{query_id}")
    def query_status(query_id: str, request: Request) -> dict[str, Any]:
        return success(get_service(request).query_status(query_id))

    @app.get("/v1/queries/{query_id}/shards")
    def query_shards(query_id: str, request: Request) -> dict[str, Any]:
        return success({"query_id": query_id, "shards": get_service(request).query_shards(query_id)})

    @app.get("/v1/queries/{query_id}/decisions")
    def query_decisions(query_id: str, request: Request) -> dict[str, Any]:
        decisions = get_service(request).query_decisions(query_id)
        actions: dict[str, int] = {}
        for decision in decisions:
            action = str(decision.get("action", "unknown"))
            actions[action] = actions.get(action, 0) + 1
        return success(
            {
                "query_id": query_id,
                "decision_count": len(decisions),
                "action_counts": actions,
                "decisions": decisions,
            }
        )

    @app.get("/v1/queries/{query_id}/results")
    def query_results(
        query_id: str,
        request: Request,
        offset: int | None = Query(default=None, ge=0),
        limit: int | None = Query(default=None, ge=1),
    ) -> dict[str, Any]:
        return success(get_service(request).query_result(query_id, offset=offset, limit=limit))

    @app.get("/v1/recovery/report")
    def recovery_report(request: Request, query_id: str | None = None) -> dict[str, Any]:
        return success(get_service(request).recovery_report(query_id))

    return app


app = create_app()

