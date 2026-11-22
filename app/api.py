from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .config import ROOT, Settings
from .engine import RuntimeEngine
from .errors import BusinessError, E_SPEC_INVALID, error_payload


def _ok(data: Any, **extra: Any) -> dict[str, Any]:
    return {"ok": True, "business_code": "OK", "data": data, **extra}


def create_app(
    settings: Settings | None = None, *, engine: RuntimeEngine | None = None
) -> FastAPI:
    app = FastAPI(
        title="Serverless Cold-Start Lab",
        version="0.1.0",
        description="Deterministic local cold-start, quota and recovery verifier",
    )
    runtime = engine or RuntimeEngine(settings or Settings.from_env())
    app.state.engine = runtime

    @app.exception_handler(BusinessError)
    async def business_error_handler(_request: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=error_payload(exc))

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        _request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "business_code": E_SPEC_INVALID,
                "message": "request body or query validation failed; no rows were written",
                "detail": {"errors": exc.errors()},
            },
        )

    @app.get("/")
    def root() -> dict[str, Any]:
        return _ok(
            {
                "service": "serverless-coldstart-lab",
                "docs": "/docs",
                "health": "/health",
                "endpoint_base": runtime.settings.endpoint_base,
            }
        )

    @app.get("/health")
    def health() -> dict[str, Any]:
        return _ok(
            {
                "status": "ok",
                "db_path": str(runtime.settings.db_path),
                "endpoint_base": runtime.settings.endpoint_base,
            }
        )

    @app.post("/v1/functions/load")
    def load_functions(
        payload: Any = Body(...), source: str = Query(default="api")
    ) -> dict[str, Any]:
        return _ok(runtime.load_specs(payload, source=source))

    @app.get("/v1/functions")
    def list_functions() -> dict[str, Any]:
        return _ok(runtime.list_functions())

    @app.post("/v1/traces/load")
    def load_trace(
        payload: Any = Body(...),
        source: str = Query(default="api"),
        strict_order: bool = Query(default=True),
        batch_id: str | None = Query(default=None),
    ) -> dict[str, Any]:
        effective_strict = strict_order
        effective_source = source
        effective_batch = batch_id
        if isinstance(payload, dict):
            effective_strict = bool(payload.get("strict_order", strict_order))
            effective_source = str(payload.get("source", source))
            effective_batch = payload.get("batch_id", batch_id)
        return _ok(
            runtime.load_trace(
                payload,
                source=effective_source,
                strict_order=effective_strict,
                batch_id=effective_batch,
            )
        )

    @app.get("/v1/traces/{batch_id}")
    def get_trace(batch_id: str) -> dict[str, Any]:
        return _ok(runtime.get_trace(batch_id))

    @app.post("/v1/traces/{batch_id}/replay")
    def replay_trace(
        batch_id: str,
        time_scale: float = Query(default=1.0, ge=0.0),
        max_workers: int = Query(default=32, ge=1, le=256),
    ) -> dict[str, Any]:
        return _ok(
            runtime.replay_trace(
                batch_id, time_scale=time_scale, max_workers=max_workers
            )
        )

    @app.post("/v1/invocations")
    def invoke(payload: Any = Body(...)) -> dict[str, Any]:
        result = runtime.invoke(payload)
        return {"ok": result["ok"], "business_code": result["business_code"], "data": result}

    @app.get("/v1/calls/{call_id}")
    def get_call(call_id: str) -> dict[str, Any]:
        return _ok(runtime.get_call(call_id))

    @app.get("/v1/instances")
    def list_instances(
        function_id: str | None = Query(default=None)
    ) -> dict[str, Any]:
        return _ok(runtime.list_instances(function_id))

    @app.post("/v1/instances/{instance_id}/drain")
    def drain_instance(
        instance_id: str, payload: dict[str, Any] | None = Body(default=None)
    ) -> dict[str, Any]:
        reason = (payload or {}).get("reason", "manual")
        return _ok(runtime.drain_instance(instance_id, reason=reason))

    @app.post("/v1/instances/{instance_id}/transition")
    def transition_instance(
        instance_id: str, payload: dict[str, Any] = Body(...)
    ) -> dict[str, Any]:
        target = payload.get("to") or payload.get("state")
        if not isinstance(target, str):
            raise BusinessError(E_SPEC_INVALID, "transition target state is required")
        return _ok(
            runtime.transition_instance(
                instance_id, target, reason=str(payload.get("reason", "explicit"))
            )
        )

    @app.post("/v1/functions/{function_id}/prewarm")
    def prewarm(
        function_id: str, count: int = Query(default=1, ge=1)
    ) -> dict[str, Any]:
        return _ok(runtime.prewarm(function_id, count))

    @app.get("/v1/quota")
    def quota(function_id: str | None = Query(default=None)) -> dict[str, Any]:
        return _ok(runtime.quota(function_id))

    @app.get("/v1/snapshot")
    def snapshot(function_id: str | None = Query(default=None)) -> dict[str, Any]:
        return _ok(runtime.snapshot(function_id))

    @app.get("/v1/metrics")
    def metrics(function_id: str | None = Query(default=None)) -> dict[str, Any]:
        return _ok(runtime.metrics(function_id))

    @app.get("/v1/audit/invariants")
    def audit_invariants() -> dict[str, Any]:
        return _ok(runtime.audit_invariants())

    @app.post("/v1/injections")
    def run_injection(payload: Any = Body(...)) -> dict[str, Any]:
        result = runtime.run_injection(payload)
        return {"ok": True, "business_code": result["result_code"], "data": result}

    @app.get("/v1/injections")
    def list_injections() -> dict[str, Any]:
        return _ok(runtime.list_injections())

    @app.get("/v1/injections/{injection_id}")
    def get_injection(injection_id: int) -> dict[str, Any]:
        return _ok(runtime.get_injection(injection_id))

    @app.get("/v1/injections/{injection_id}/recheck")
    def recheck_injection(injection_id: int) -> dict[str, Any]:
        return _ok(runtime.recheck_injection(injection_id))

    @app.get("/v1/recovery/latest")
    @app.get("/v1/recovery/reports/latest")
    def recovery_latest() -> dict[str, Any]:
        return _ok(runtime.latest_recovery_report())

    @app.get("/v1/intents")
    def list_intents(status: str | None = Query(default=None)) -> dict[str, Any]:
        return _ok(runtime.list_intents(status=status))

    @app.post("/v1/samples/load")
    def load_samples(
        specs_path: str = Query(default=str(ROOT / "samples" / "functions.json")),
        trace_path: str = Query(default=str(ROOT / "samples" / "trajectory.json")),
    ) -> dict[str, Any]:
        spec_file = Path(specs_path).resolve()
        trace_file = Path(trace_path).resolve()
        specs = json.loads(spec_file.read_text(encoding="utf-8"))
        trace = json.loads(trace_file.read_text(encoding="utf-8"))
        spec_result = runtime.load_specs(specs, source=str(spec_file))
        trace_result = runtime.load_trace(trace, source=str(trace_file))
        return _ok({"specs": spec_result, "trace": trace_result})

    return app

