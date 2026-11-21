from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException

from .engine import CrashController, RepairEngine
from .errors import BusinessError, fail
from .store import Store


APP_DIR = Path(__file__).resolve().parent
ROOT_DIR = APP_DIR.parent
DATA_DIR = ROOT_DIR / "data"
STATIC_DIR = APP_DIR / "static"
DEFAULT_DB = ROOT_DIR / "var" / "repair_engine.db"


ERROR_CODES = {
    "E_LAYOUT_INVALID": "布局解析、对象标识、k/m、大小、摘要或声明上限非法；整批不加载。",
    "E_TRACE_INVALID": "轨迹解析失败、时间戳非单调或位图非法；整批拒绝。",
    "E_UNRECOVERABLE": "可读分片少于 k，请求不可恢复，禁止非法重建。",
    "E_RECONSTRUCT_MISMATCH": "重建摘要与布局声明不一致，附分片及摘要。",
    "E_MATRIX_INVALID": "k/m 组合或 GF(256) 运算前提不满足。",
    "E_REBUILD_QUEUE_FULL": "超过并发上限的任务无法进入有界等待队列。",
    "E_REBUILD_SHED": "字节或时间预算不足，按优先级确定性降级快速失败。",
    "E_LATENCY_GUARD": "重建端到端估计延迟超过可容忍读延迟。",
    "E_CONFLICTING_REQUEST": "重复 request_id 内容不一致。",
    "E_LAYOUT_CONFLICT": "同一 layout_version 已加载不同布局内容。",
    "E_REQUEST_INVALID": "HTTP 请求体结构不符合接口契约。",
    "E_ROUND_NOT_COMMITTED": "轮次尚未提交，拒绝读取中间状态。",
    "E_LAYOUT_NOT_FOUND": "布局版本不存在。",
    "E_ROUND_NOT_FOUND": "轮次不存在。",
    "E_RECOVERY_NOT_FOUND": "恢复报告不存在。",
    "E_ROUTE_NOT_FOUND": "请求的路由不存在。",
    "E_INTERNAL": "未分类内部错误。",
}


def _json_file(name: str) -> dict[str, Any]:
    return json.loads((DATA_DIR / name).read_text(encoding="utf-8"))


def _require_local(request: Request) -> None:
    host = request.client.host if request.client else ""
    if host not in {"127.0.0.1", "::1", "localhost", "testclient"}:
        fail("E_FORBIDDEN", "该管理接口仅允许本机回环访问", status_code=403, details={"host": host})


def create_app(db_path: str | Path | None = None) -> FastAPI:
    configured_db = str(db_path or os.environ.get("REPAIR_DB_PATH", DEFAULT_DB))
    crash_point = os.environ.get("REPAIR_CRASH_POINT") or None
    crash_mode = os.environ.get("REPAIR_CRASH_MODE", "exit")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        store = Store(configured_db)
        controller = CrashController(point=crash_point, mode=crash_mode)
        engine = RepairEngine(store, controller)
        recovery_report = engine.recover()
        app.state.store = store
        app.state.engine = engine
        app.state.startup_recovery_report = recovery_report
        yield

    app = FastAPI(
        title="EC Read Repair Budget Engine",
        version="1.0.0",
        description="Deterministic replica read-repair adjudication and erasure-code rebuild budget engine.",
        lifespan=lifespan,
    )
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.exception_handler(BusinessError)
    async def business_error_handler(_: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content=exc.as_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content=jsonable_encoder({
                "error": {
                    "code": "E_REQUEST_INVALID",
                    "message": "HTTP 请求体不符合接口结构",
                    "details": {"errors": exc.errors()},
                }
            }),
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = "E_ROUTE_NOT_FOUND" if exc.status_code == 404 else "E_HTTP_ERROR"
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": code,
                    "message": str(exc.detail),
                    "details": {},
                }
            },
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(_: Request, __: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": "E_INTERNAL",
                    "message": "内部错误；详细原因仅保留在服务日志",
                    "details": {},
                }
            },
        )

    def engine_of(request: Request) -> RepairEngine:
        return request.app.state.engine

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/api/v1/health")
    def health(request: Request) -> dict[str, Any]:
        return engine_of(request).health()

    @app.get("/api/v1/error-codes")
    def error_codes() -> dict[str, Any]:
        return {"codes": ERROR_CODES}

    @app.post("/api/v1/layouts", status_code=201)
    def load_layout(request: Request, document: dict[str, Any] = Body(...)) -> dict[str, Any]:
        payload = document.get("layout", document)
        return engine_of(request).load_layout(payload)

    @app.get("/api/v1/layouts")
    def list_layouts(request: Request) -> dict[str, Any]:
        return {"layouts": engine_of(request).list_layouts()}

    @app.get("/api/v1/layouts/current")
    def current_layout(request: Request) -> dict[str, Any]:
        return engine_of(request).get_layout_summary()

    @app.get("/api/v1/layouts/{layout_version}")
    def get_layout(request: Request, layout_version: str) -> dict[str, Any]:
        return engine_of(request).get_layout_summary(layout_version)

    @app.post("/api/v1/traces", status_code=201)
    def submit_trace(request: Request, document: dict[str, Any] = Body(...)) -> dict[str, Any]:
        payload = document.get("trace", document)
        return engine_of(request).submit_trace(payload)

    @app.get("/api/v1/rounds")
    def list_rounds(request: Request, limit: int = Query(50, ge=1, le=500)) -> dict[str, Any]:
        return {"rounds": engine_of(request).list_rounds(limit)}

    @app.get("/api/v1/rounds/{round_id}")
    def get_round(request: Request, round_id: str) -> dict[str, Any]:
        return engine_of(request).get_round(round_id)

    @app.get("/api/v1/rounds/{round_id}/decisions")
    def get_round_decisions(request: Request, round_id: str) -> dict[str, Any]:
        decisions = engine_of(request).get_round_decisions(round_id)
        return {"round_id": round_id, "decisions": decisions}

    @app.get("/api/v1/rounds/{round_id}/budget")
    def get_round_budget(request: Request, round_id: str) -> dict[str, Any]:
        return engine_of(request).get_round_budget(round_id)

    @app.get("/api/v1/rounds/{round_id}/replay-signature")
    def replay_signature(request: Request, round_id: str) -> dict[str, Any]:
        return engine_of(request).replay_signature(round_id)

    @app.get("/api/v1/recovery")
    def latest_recovery(request: Request) -> dict[str, Any]:
        report = engine_of(request).latest_recovery_report()
        if report is None:
            return {
                "report_id": None,
                "scope": "startup_reconciliation",
                "clean_shutdown_report": False,
                "recovery_required": False,
                "summary": {"note": "尚未生成恢复报告"},
            }
        return report

    @app.get("/api/v1/recovery/{report_id}")
    def recovery_report(request: Request, report_id: str) -> dict[str, Any]:
        report = engine_of(request).recovery_report(report_id)
        if report is None:
            fail("E_RECOVERY_NOT_FOUND", "恢复报告不存在", status_code=404, details={"report_id": report_id})
        return report

    @app.post("/api/v1/admin/arm-crash")
    def arm_crash(request: Request, document: dict[str, Any] = Body(default={})) -> dict[str, Any]:
        _require_local(request)
        point = document.get("point", "after_task_start")
        mode = document.get("mode", "exit")
        return engine_of(request).arm_crash(point=point, mode=mode)

    @app.get("/api/v1/demo/fixtures")
    def demo_fixtures() -> dict[str, Any]:
        return {
            "layout": _json_file("sample_layout.json"),
            "scenarios": {
                "main": {
                    "name": "主轨迹：重建、副本补齐、预算降级与延迟保护",
                    "trace": _json_file("sample_trace.json"),
                },
                "queue": {
                    "name": "有界队列：并发=1、队列容量=0",
                    "trace": _json_file("queue_trace.json"),
                },
                "mismatch": {
                    "name": "重建摘要不一致",
                    "trace": _json_file("mismatch_trace.json"),
                },
                "recovery": {
                    "name": "强制终止恢复触发轨迹",
                    "trace": _json_file("recovery_trace.json"),
                },
            },
        }

    return app


app = create_app()
