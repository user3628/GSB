"""本地 HTTP API（FastAPI）。

仅监听回环地址，端点覆盖任务要求的全部能力：

* ``POST /policies``                 提交策略快照（校验摘要与单调版本号）
* ``POST /events``                   提交事件批次并裁决（幂等/冲突检测）
* ``GET  /stats/routing``            按版本查询分流占比与灰度使用
* ``GET  /contracts/errors``         误差说明与数值契约查询
* ``GET  /recovery``                 恢复报告查询
* ``POST /admin/rebuild``            派生统计重建（含逐位一致性自检）
* ``POST /admin/replay-check``       重放逐位一致性校验
* ``GET  /verdicts/{request_id}``    单请求裁决轨迹
* ``GET  /batches``                  批次列表
* ``POST /experiments/float64``      混合量级 float64 对照实验
* ``GET  /health``                   健康检查
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .errors import BusinessError
from .service import GrayService
from .storage import Storage

DEFAULT_DB_PATH = os.environ.get(
    "GRAYSWITCH_DB",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "grayswitch.db"),
)


def create_app(db_path: Optional[str] = None) -> FastAPI:
    storage = Storage(db_path or DEFAULT_DB_PATH)
    service = GrayService(storage)
    startup_report: Dict[str, Any] = {}

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        # 启动时扫描未完成意图并恢复；关闭时释放 SQLite 连接。
        startup_report["last"] = service.recover_on_startup()
        try:
            yield
        finally:
            storage.close()

    app = FastAPI(
        title="GraySwitch 灰度路由与流量镜像中间件",
        version="1.0.0",
        docs_url="/docs",
        lifespan=lifespan,
    )
    app.state.storage = storage
    app.state.service = service

    @app.exception_handler(BusinessError)
    async def business_error_handler(_: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_payload())

    @app.get("/health")
    def health() -> Dict[str, Any]:
        return {
            "status": "ok",
            "db_path": storage.db_path,
            "startup_recovery": startup_report.get("last"),
            "latest_policy_version": storage.latest_policy_version(),
        }

    @app.post("/policies")
    def submit_policy(payload: Dict[str, Any]) -> Dict[str, Any]:
        return service.submit_policy(payload)

    @app.post("/events")
    def submit_events(payload: Dict[str, Any]) -> Dict[str, Any]:
        return service.submit_batch(payload)

    @app.get("/stats/routing")
    def routing_stats(policy_version: Optional[int] = None) -> Dict[str, Any]:
        return service.get_routing_stats(policy_version)

    @app.get("/contracts/errors")
    def error_contract(policy_version: Optional[int] = None) -> Dict[str, Any]:
        return service.get_error_contract(policy_version)

    @app.get("/recovery")
    def recovery_report() -> Dict[str, Any]:
        return service.get_recovery_report()

    @app.get("/batches")
    def batches() -> Dict[str, Any]:
        return service.list_batches()

    @app.get("/verdicts/{request_id}")
    def verdict(request_id: str) -> Dict[str, Any]:
        return service.get_verdict(request_id)

    @app.post("/admin/rebuild")
    def rebuild(policy_version: Optional[int] = None) -> Dict[str, Any]:
        return service.rebuild_stats(policy_version)

    @app.post("/admin/replay-check")
    def replay_check(policy_version: Optional[int] = None) -> Dict[str, Any]:
        return service.replay_check(policy_version)

    @app.post("/experiments/float64")
    def float64_experiment(payload: Dict[str, Any]) -> Dict[str, Any]:
        values = payload.get("values")
        if not isinstance(values, list) or not values:
            raise BusinessError(
                "E_EVENT_INVALID", "values 必须是非空整数数组", {}
            )
        try:
            int_values = [int(v) for v in values]
        except (TypeError, ValueError) as exc:
            raise BusinessError(
                "E_EVENT_INVALID", "values 必须全部为整数", {"detail": str(exc)}
            )
        return service.numeric_comparison(int_values)

    return app


app = create_app()
