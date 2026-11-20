"""FastAPI 应用：仅绑定本机回环，业务错误统一映射为稳定业务码。"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .database import Database
from .errors import BusinessError
from .scheduler import SchedulerService
from .spec import load_workload_file, parse_workload

DEFAULT_DB_PATH = os.environ.get("GPWB_DB_PATH", "data/gpu_workbench.sqlite3")

_SERVICE: SchedulerService | None = None


def get_service() -> SchedulerService:
    global _SERVICE
    if _SERVICE is None:
        _SERVICE = SchedulerService(Database(DEFAULT_DB_PATH))
    return _SERVICE


@asynccontextmanager
async def lifespan(app: FastAPI):
    get_service()
    yield


app = FastAPI(
    title="GPU 配额调度与推理批调度诊断工作台",
    version="1.0.0",
    lifespan=lifespan,
)


STATUS_MAP = {
    "E_SPEC_INVALID": 400,
    "E_QUOTA_EXCEEDED": 409,
    "E_INVALID_TRANSITION": 409,
    "E_BATCH_TOO_LARGE": 422,
    "E_NODE_FRAGMENTED": 409,
    "E_NODE_UNHEALTHY": 409,
    "E_NOT_FOUND": 404,
    "E_PREEMPTION_NOT_POSSIBLE": 409,
    "E_INTENT_ABORTED": 409,
    "E_DUPLICATE_REQUEST": 409,
}


def _raise_business(exc: BusinessError) -> None:
    raise HTTPException(
        status_code=STATUS_MAP.get(exc.code, 400), detail=exc.to_dict()
    )


class LoadPathBody(BaseModel):
    path: str = Field(..., description="本机确定性样本 JSON 路径")


class BatchBody(BaseModel):
    group_id: str | None = None
    strategy: str | None = None
    include_ids: list[str] | None = None


class TransitionBody(BaseModel):
    to_state: str
    reason: str


@app.exception_handler(BusinessError)
async def business_error_handler(request, exc: BusinessError):
    return JSONResponse(
        status_code=STATUS_MAP.get(exc.code, 400),
        content={"error": exc.to_dict()},
    )


@app.get("/health")
def health():
    return {"status": "ok", "service": "gpu-workbench"}


@app.post("/spec/load")
def load_spec(body: LoadPathBody):
    spec = load_workload_file(Path(body.path))
    return get_service().load_spec(spec)


@app.post("/spec/load-inline")
def load_spec_inline(payload: dict = Body(...)):
    spec = parse_workload(payload)
    return get_service().load_spec(spec)


@app.get("/quota/groups")
def quota_groups(group_id: str | None = None):
    return get_service().quota_snapshot(group_id)


@app.get("/nodes")
def nodes():
    return get_service().node_snapshot()


@app.get("/requests")
def requests_view(state: str | None = None, group_id: str | None = None):
    return get_service().repo.list_requests(state=state, group_id=group_id)


@app.get("/requests/{request_id}")
def request_detail(request_id: str):
    service = get_service()
    request = service.repo.get_request(request_id)
    if request is None:
        _raise_business(BusinessError("E_NOT_FOUND", f"请求 {request_id} 不存在"))
    return {
        "request": request,
        "events": service.repo.list_events(request_id),
    }


@app.post("/requests/{request_id}/admit")
def admit(request_id: str):
    try:
        return get_service().admit_request(request_id)
    except BusinessError as exc:
        _raise_business(exc)


@app.post("/requests/{request_id}/reject")
def reject(request_id: str):
    try:
        return get_service().reject_request(request_id)
    except BusinessError as exc:
        _raise_business(exc)


@app.post("/requests/{request_id}/preempt")
def preempt(request_id: str):
    try:
        return get_service().preempt_for(request_id)
    except BusinessError as exc:
        _raise_business(exc)


@app.post("/requests/{request_id}/transition")
def transition(request_id: str, body: TransitionBody):
    try:
        return get_service().transition(request_id, body.to_state, body.reason)
    except BusinessError as exc:
        _raise_business(exc)


@app.post("/batching/run")
def run_batching(body: BatchBody | None = None):
    body = body or BatchBody()
    try:
        return get_service().run_batching(
            body.group_id, strategy=body.strategy, include_ids=body.include_ids
        )
    except BusinessError as exc:
        _raise_business(exc)


@app.post("/batching/reorganize/{group_id}")
def reorganize(group_id: str):
    try:
        return get_service().reorganize_batches(group_id)
    except BusinessError as exc:
        _raise_business(exc)


@app.get("/batches")
def batches(state: str | None = None):
    return get_service().repo.list_batches(state=state)


@app.get("/batches/{batch_id}")
def batch_detail(batch_id: str):
    service = get_service()
    batch = service.repo.get_batch(batch_id)
    if batch is None:
        _raise_business(BusinessError("E_NOT_FOUND", f"批 {batch_id} 不存在"))
    return {"batch": batch, "items": service.repo.list_batch_items(batch_id)}


@app.post("/batches/{batch_id}/start")
def start_batch(batch_id: str):
    try:
        return get_service().start_batch(batch_id)
    except BusinessError as exc:
        _raise_business(exc)


@app.post("/batches/{batch_id}/complete")
def complete_batch(batch_id: str):
    try:
        return get_service().complete_batch(batch_id)
    except BusinessError as exc:
        _raise_business(exc)


@app.get("/events")
def events(request_id: str | None = None):
    return get_service().repo.list_events(request_id)


@app.post("/recovery/run")
def recovery_run():
    return get_service().recover()


@app.get("/recovery/events")
def recovery_events():
    return get_service().repo.list_recovery_events()


@app.get("/intents")
def intents(status: str | None = None):
    rows = get_service().repo.db.connect().execute(
        "SELECT intent_id, intent_type, status, created_at, committed_at FROM intents"
        + (" WHERE status=?" if status else "")
        + " ORDER BY created_at",
        (status,) if status else (),
    ).fetchall()
    return [dict(row) for row in rows]
