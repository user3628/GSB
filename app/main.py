from __future__ import annotations

import logging
import os
import uuid
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import __version__
from .errors import BusinessError
from .seed import seed_workbench
from .service import WorkbenchService


LOGGER = logging.getLogger("risklab")
BASE_DIR = Path(__file__).resolve().parent.parent


def _ok(request: Request, data: Any, status_code: int = 200) -> JSONResponse:
    return JSONResponse(
        {
            "ok": True,
            "data": data,
            "request_id": getattr(request.state, "request_id", None),
        },
        status_code=status_code,
    )


def _error_payload(request: Request, error: BusinessError) -> dict[str, Any]:
    return {
        "ok": False,
        "error": {
            "code": error.code,
            "message": error.message,
            "context": error.context,
        },
        "request_id": getattr(request.state, "request_id", None),
    }


def create_app(
    *,
    db_path: str | Path | None = None,
    fault_injection: bool | None = None,
    static_dir: str | Path | None = None,
    samples_dir: str | Path | None = None,
) -> FastAPI:
    database_path = Path(
        db_path
        or os.environ.get("RISKLAB_DB_PATH", str(BASE_DIR / "data" / "workbench.db"))
    )
    if fault_injection is None:
        fault_injection = os.environ.get("RISKLAB_FAULT_INJECTION", "0") in {
            "1",
            "true",
            "TRUE",
            "yes",
        }
    service = WorkbenchService(database_path, fault_injection=fault_injection)
    recovery_report = service.recover_pending_intents()

    app = FastAPI(
        title="ChangeGuard Local Change Risk Workbench",
        version=__version__,
        description=(
            "本地确定性微服务变更风险分级、快速/完整裁决、自动回滚与强制终止恢复工作台。"
        ),
    )
    app.state.service = service
    app.state.startup_recovery = recovery_report
    static_path = Path(static_dir or BASE_DIR / "app" / "static")
    sample_path = Path(samples_dir or BASE_DIR / "samples")

    @app.middleware("http")
    async def request_context(request: Request, call_next: Any) -> Any:
        request_id = request.headers.get("X-Request-ID") or f"req_{uuid.uuid4().hex}"
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response

    @app.exception_handler(BusinessError)
    async def business_error_handler(request: Request, error: BusinessError) -> JSONResponse:
        return JSONResponse(
            _error_payload(request, error), status_code=error.http_status
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, error: RequestValidationError
    ) -> JSONResponse:
        business = BusinessError(
            "E_VALIDATION",
            "HTTP 请求结构校验失败",
            {"errors": error.errors()},
        )
        return JSONResponse(_error_payload(request, business), status_code=422)

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(
        request: Request, error: StarletteHTTPException
    ) -> JSONResponse:
        code = {
            404: "E_NOT_FOUND",
            405: "E_METHOD_NOT_ALLOWED",
        }.get(error.status_code, "E_HTTP")
        business = BusinessError(
            code,
            str(error.detail),
            {"status_code": error.status_code, "path": request.url.path},
        )
        return JSONResponse(
            _error_payload(request, business), status_code=error.status_code
        )

    @app.exception_handler(Exception)
    async def internal_error_handler(request: Request, error: Exception) -> JSONResponse:
        LOGGER.exception("unhandled request error", exc_info=error)
        business = BusinessError(
            "E_INTERNAL",
            "服务内部错误；事务已按 SQLite 语义回滚",
            {"exception_type": type(error).__name__},
        )
        return JSONResponse(_error_payload(request, business), status_code=500)

    @app.get("/api/health", tags=["system"])
    def health(request: Request) -> JSONResponse:
        return _ok(request, service.health())

    @app.get("/api/meta", tags=["system"])
    def meta(request: Request) -> JSONResponse:
        return _ok(
            request,
            {
                "version": __version__,
                "modes": ["FAST", "FULL"],
                "statuses": [
                    "OBSERVING",
                    "PASSED",
                    "ROLLING_BACK",
                    "ROLLED_BACK",
                    "ESCALATED",
                ],
                "change_types": ["CONFIG_APPLY", "IMAGE_UPGRADE", "SCALE"],
                "startup_recovery": recovery_report,
                "fault_injection_enabled": service.faults.enabled,
            },
        )

    @app.post("/api/policies", tags=["policies"])
    def submit_policy(request: Request, payload: Any = Body(...)) -> JSONResponse:
        return _ok(request, service.submit_policy(payload), status_code=201)

    @app.get("/api/policies", tags=["policies"])
    def list_policies(request: Request) -> JSONResponse:
        return _ok(request, {"policies": service.list_policies()})

    @app.get("/api/policies/latest", tags=["policies"])
    def latest_policy(request: Request) -> JSONResponse:
        policy = service.latest_policy()
        if policy is None:
            raise BusinessError(
                "E_POLICY_NOT_FOUND", "尚未提交策略", {"policy_version": None}
            )
        return _ok(request, policy)

    @app.post("/api/batches", tags=["batches"])
    def submit_batch(request: Request, payload: Any = Body(...)) -> JSONResponse:
        return _ok(request, service.submit_batch(payload), status_code=201)

    @app.get("/api/batches", tags=["batches"])
    def list_batches(request: Request) -> JSONResponse:
        return _ok(request, {"batches": service.list_batches()})

    @app.get("/api/batches/{batch_id}", tags=["batches"])
    def get_batch(request: Request, batch_id: str) -> JSONResponse:
        return _ok(request, service.get_batch(batch_id))

    @app.post("/api/observations", tags=["observations"])
    def submit_observations(request: Request, payload: Any = Body(...)) -> JSONResponse:
        if not isinstance(payload, dict) or not isinstance(payload.get("batch_id"), str):
            raise BusinessError(
                "E_OBSERVATION_INVALID",
                "观测请求必须提供 batch_id",
                {"field": "batch_id"},
            )
        batch_id = payload["batch_id"]
        return _ok(request, service.ingest_observations(batch_id, payload), status_code=201)

    @app.post("/api/batches/{batch_id}/observations", tags=["observations"])
    def submit_batch_observations(
        request: Request, batch_id: str, payload: Any = Body(...)
    ) -> JSONResponse:
        return _ok(request, service.ingest_observations(batch_id, payload), status_code=201)

    @app.post("/api/batches/{batch_id}/windows/close", tags=["observations"])
    def close_windows(
        request: Request, batch_id: str, payload: Any = Body(...)
    ) -> JSONResponse:
        if not isinstance(payload, dict) or payload.get("watermark_ms") is None:
            raise BusinessError(
                "E_OBSERVATION_INVALID",
                "关闭窗口请求必须提供 watermark_ms",
                {"field": "watermark_ms"},
            )
        request_body = {
            "watermark_ms": payload["watermark_ms"],
            "observations": [],
        }
        return _ok(request, service.ingest_observations(batch_id, request_body))

    @app.get("/api/batches/{batch_id}/risk", tags=["risk"])
    def get_risk(
        request: Request,
        batch_id: str,
        mode: str = Query("FULL"),
        policy_version: int | None = Query(None),
        window_index: int | None = Query(None),
    ) -> JSONResponse:
        return _ok(
            request,
            service.get_risk(
                batch_id,
                mode=mode,
                policy_version=policy_version,
                window_index=window_index,
            ),
        )

    @app.get("/api/batches/{batch_id}/compare", tags=["risk"])
    def compare_modes(
        request: Request,
        batch_id: str,
        policy_version: int | None = Query(None),
        window_index: int | None = Query(None),
    ) -> JSONResponse:
        return _ok(
            request,
            service.compare_modes(
                batch_id,
                policy_version=policy_version,
                window_index=window_index,
            ),
        )

    @app.post("/api/batches/{batch_id}/decision", tags=["risk"])
    def apply_decision(
        request: Request, batch_id: str, payload: Any = Body(...)
    ) -> JSONResponse:
        return _ok(request, service.apply_decision(batch_id, payload))

    @app.get("/api/batches/{batch_id}/decisions", tags=["risk"])
    def list_decisions(
        request: Request, batch_id: str, limit: int = Query(20, ge=1, le=100)
    ) -> JSONResponse:
        return _ok(request, {"decisions": service.list_decisions(batch_id, limit)})

    @app.get("/api/batches/{batch_id}/trajectory", tags=["risk"])
    def trajectory(request: Request, batch_id: str) -> JSONResponse:
        return _ok(request, {"trajectory": service._trajectory(batch_id)})

    @app.get("/api/cooldowns/{service}", tags=["rollback"])
    def cooldown(
        request: Request, service: str, at_ms: int | None = Query(None)
    ) -> JSONResponse:
        return _ok(
            request,
            request.app.state.service.cooldown_status(service, at_ms=at_ms),
        )

    @app.get("/api/recovery/report", tags=["recovery"])
    def recovery_report(request: Request) -> JSONResponse:
        report = service.latest_recovery_report() or recovery_report
        return _ok(request, report)

    @app.get("/api/recovery/reports", tags=["recovery"])
    def recovery_reports(
        request: Request, limit: int = Query(20, ge=1, le=100)
    ) -> JSONResponse:
        return _ok(request, {"reports": service.recovery_reports(limit)})

    @app.get("/api/intents", tags=["recovery"])
    def list_intents(
        request: Request,
        status: str | None = Query(None),
        limit: int = Query(50, ge=1, le=500),
    ) -> JSONResponse:
        allowed = {"PENDING", "COMMITTED", "ABORTED"}
        if status is not None and status.upper() not in allowed:
            raise BusinessError(
                "E_VALIDATION",
                "status 必须是 PENDING、COMMITTED 或 ABORTED",
                {"field": "status", "value": status},
            )
        with service.db.read_snapshot() as connection:
            if status is None:
                rows = connection.execute(
                    "SELECT intent_id, operation, status, created_at_ms, updated_at_ms, "
                    "completed_at_ms, recovery_run_id FROM intents ORDER BY created_at_ms DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT intent_id, operation, status, created_at_ms, updated_at_ms, "
                    "completed_at_ms, recovery_run_id FROM intents WHERE status=? "
                    "ORDER BY created_at_ms DESC LIMIT ?",
                    (status.upper(), limit),
                ).fetchall()
        return _ok(request, {"intents": [dict(row) for row in rows]})

    @app.post("/api/_demo/faults/arm", tags=["demo"])
    def arm_fault(request: Request, payload: Any = Body(...)) -> JSONResponse:
        if not isinstance(payload, dict) or not isinstance(
            payload.get("operation"), str
        ):
            raise BusinessError(
                "E_VALIDATION",
                "故障注入请求必须提供 operation",
                {"field": "operation"},
            )
        armed = service.faults.arm(
            payload["operation"], payload.get("batch_id")
        )
        return _ok(request, {"armed": armed})

    @app.post("/api/_demo/faults/disarm", tags=["demo"])
    def disarm_fault(request: Request) -> JSONResponse:
        service.faults.disarm()
        return _ok(request, {"armed": None})

    @app.get("/api/_demo/faults/status", tags=["demo"])
    def fault_status(request: Request) -> JSONResponse:
        return _ok(
            request,
            {
                "enabled": service.faults.enabled,
                "armed": service.faults.status(),
            },
        )

    if static_path.exists():
        app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

        @app.get("/", include_in_schema=False)
        def index() -> FileResponse:
            return FileResponse(static_path / "index.html")

        @app.get("/favicon.svg", include_in_schema=False)
        def favicon() -> FileResponse:
            return FileResponse(static_path / "favicon.svg")

    @app.get("/api/startup", tags=["system"])
    def startup(request: Request) -> JSONResponse:
        return _ok(request, recovery_report)

    @app.post("/api/demo/seed", tags=["demo"])
    def seed(request: Request) -> JSONResponse:
        return _ok(request, seed_workbench(service, sample_path), status_code=201)

    return app
app = create_app()
