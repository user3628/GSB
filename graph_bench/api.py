"""本地回环 HTTP 接口（FastAPI）。

服务只绑定 127.0.0.1，不依赖任何线上服务或外部账号。
HTTP 层负责把领域异常翻译成稳定的业务码 JSON，业务逻辑全部在服务层。
"""

from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import BenchError, DependencyCycle
from .service import BenchService
from .storage import Storage


class SubmitBody(BaseModel):
    request_id: str = Field(min_length=1)
    payload: dict[str, Any]


class AppState:
    def __init__(self, db_path: str | None = None) -> None:
        self.storage = Storage(db_path)
        self.service = BenchService(self.storage)
        self.startup_recovery = self.service.recover()


def create_app(db_path: str | None = None, *, run_recovery: bool = True) -> FastAPI:
    app = FastAPI(title="资源编排依赖图诊断工作台", version="1.0.0")
    state = AppState(db_path)
    if run_recovery:
        state.startup_recovery = state.service.recover()
    app.state.graph_bench = state

    @app.exception_handler(BenchError)
    async def handle_bench_error(_: Request, exc: BenchError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    @app.get("/health")
    async def health() -> dict[str, Any]:
        current = state.storage.get_current_version()
        return {"ok": True, "service": "graph-bench", "current_version": current}

    @app.get("/recovery")
    async def recovery() -> dict[str, Any]:
        return {"reports": state.storage.list_recovery_reports(), "startup": state.startup_recovery}

    @app.post("/snapshots")
    async def submit_snapshot(body: SubmitBody) -> dict[str, Any]:
        return state.service.submit_snapshot(body.payload, body.request_id)

    @app.post("/increments")
    async def submit_increment(body: SubmitBody) -> dict[str, Any]:
        return state.service.submit_increment(body.payload, body.request_id)

    @app.get("/snapshots/current")
    async def current_snapshot() -> dict[str, Any]:
        return state.service.get_snapshot()

    @app.get("/snapshots/{version}")
    async def snapshot_at(version: int) -> dict[str, Any]:
        return state.service.get_snapshot(version)

    @app.get("/views/layers")
    async def layers(version: int | None = None) -> dict[str, Any]:
        result = state.service.get_layers(version)
        return _with_cycle_contract(result)

    @app.get("/views/batches")
    async def batches(version: int | None = None, budget_ms: float | None = None) -> dict[str, Any]:
        result = state.service.get_batches(version, budget_ms)
        return _with_cycle_contract(result)

    @app.get("/resources/{resource_id}/impact")
    async def impact(
        resource_id: str,
        version: int | None = None,
        budget_ms: float | None = None,
        delay_per_item_ms: float = 0.0,
    ) -> dict[str, Any]:
        return state.service.get_impact(resource_id, version, budget_ms, delay_per_item_ms)

    @app.get("/resources/{resource_id}")
    async def resource_detail(resource_id: str, version: int | None = None) -> dict[str, Any]:
        return state.service.get_resource(resource_id, version)

    @app.get("/intents")
    async def list_intents() -> dict[str, Any]:
        return {"intents": state.service.list_intents()}

    @app.get("/intents/{request_id}")
    async def intent_detail(request_id: str) -> dict[str, Any]:
        return {"intent": state.service.get_intent(request_id)}

    return app


def _with_cycle_contract(result: dict[str, Any]) -> dict[str, Any]:
    """环场景的 HTTP 契约：HTTP 200 + E_DEPENDENCY_CYCLE 业务码 + 部分结果。"""

    if result.get("is_acyclic"):
        return result
    raise DependencyCycle(
        "依赖图存在环，已返回未被污染资源的部分结果与受阻清单",
        {
            "cycles": result["cycles"],
            "blocked_resources": result["blocked_resources"],
            "partial_result": result,
        },
    )


def create_configured_app() -> FastAPI:
    return create_app(os.environ.get("GRAPH_BENCH_DB"))


app = create_configured_app()
