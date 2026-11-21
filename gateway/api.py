from __future__ import annotations

import argparse
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import Body, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from .config import GatewayConfig
from .domain import (
    BatchSubmission,
    CancelRequest,
    InferenceRequestSpec,
    ResetBackendRequest,
    RoundRequest,
    TransitionRequest,
    compute_route_checksum,
)
from .errors import GatewayError
from .service import GatewayService
from .validation import validate_snapshot


def create_app(config: GatewayConfig | None = None) -> FastAPI:
    service = GatewayService(config)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.recovery_report = service.startup()
        try:
            yield
        finally:
            service.shutdown()

    app = FastAPI(
        title="Local Inference Gateway",
        version="0.1.0",
        description="Deterministic local model routing, resource budgets, scheduling and recovery.",
        lifespan=lifespan,
    )
    app.state.gateway = service
    app.state.config = service.config

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(_request: Request, exc: GatewayError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.as_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "code": "E_REQUEST_INVALID",
                "message": "Request validation failed",
                "details": {"errors": exc.errors()},
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = "E_NOT_FOUND" if exc.status_code == 404 else "E_HTTP_ERROR"
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "ok": False,
                "code": code,
                "message": str(exc.detail),
                "details": {},
            },
        )

    @app.exception_handler(Exception)
    async def unhandled_error_handler(_request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "code": "E_INTERNAL",
                "message": "Unexpected gateway failure",
                "details": {"exception": type(exc).__name__},
            },
        )

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "ok": True,
            "service": "local-inference-gateway",
            "db": service.config.resolved_db_path,
            "global_concurrency_limit": service.config.global_concurrency_limit,
            "queue_limit": service.config.queue_limit,
        }

    @app.get("/config")
    def get_config() -> dict[str, Any]:
        cfg = service.config
        return {
            "global_concurrency_limit": cfg.global_concurrency_limit,
            "queue_limit": cfg.queue_limit,
            "round_time_budget_ms": cfg.round_time_budget_ms,
            "default_request_timeout_ms": cfg.default_request_timeout_ms,
            "recovery_requeue": cfg.recovery_requeue,
            "max_recovery_attempts": cfg.max_recovery_attempts,
        }

    @app.post("/routes/checksum")
    def route_checksum(document: dict[str, Any] = Body(...)) -> dict[str, Any]:
        return {"ok": True, "checksum": compute_route_checksum(document)}

    @app.post("/routes")
    @app.post("/route")
    def submit_route(document: dict[str, Any] = Body(...)) -> dict[str, Any]:
        committed = service.submit_route(document)
        return {"ok": True, "route": committed}

    @app.get("/routes/current")
    @app.get("/route/current")
    def current_route() -> dict[str, Any]:
        route = service.store.get_current_route()
        if route is None:
            raise GatewayError("E_ROUTE_UNAVAILABLE", "No route table has been committed", http_status=404)
        return {"ok": True, "route": route}

    @app.get("/routes")
    def list_routes() -> dict[str, Any]:
        return {"ok": True, "routes": service.store.list_routes()}

    @app.get("/routes/{version}")
    def get_route(version: int) -> dict[str, Any]:
        route = service.store.get_route(version)
        if route is None:
            raise GatewayError("E_NOT_FOUND", "Route version not found", http_status=404)
        return {"ok": True, "route": route}

    @app.post("/requests")
    def submit_request(
        spec: InferenceRequestSpec,
        strict_capacity: bool = Query(default=False),
        wait: bool = Query(default=False),
        wait_timeout_ms: int = Query(default=2_000, ge=0, le=60_000),
    ) -> dict[str, Any]:
        request = service.submit_request(
            spec,
            strict_capacity=strict_capacity,
            wait=wait,
            wait_timeout_ms=wait_timeout_ms,
        )
        return {"ok": True, "request": request}

    @app.post("/requests/batch")
    @app.post("/batch")
    def submit_batch(
        payload: Any = Body(...),
        strict_capacity: bool = Query(default=False),
        wait: bool = Query(default=False),
        wait_timeout_ms: int = Query(default=2_000, ge=0, le=60_000),
    ) -> dict[str, Any]:
        try:
            if isinstance(payload, list):
                specs = [InferenceRequestSpec.model_validate(item) for item in payload]
                effective_wait = wait
                effective_timeout = wait_timeout_ms
            elif isinstance(payload, dict):
                batch = BatchSubmission.model_validate(payload)
                specs = batch.requests
                effective_wait = wait or batch.wait
                effective_timeout = wait_timeout_ms if wait else batch.wait_timeout_ms
            else:
                raise ValueError("batch body must be an array or an object with requests")
        except ValidationError as exc:
            raise GatewayError(
                "E_REQUEST_INVALID",
                "Batch validation failed",
                http_status=422,
                details={"errors": exc.errors()},
            ) from exc
        except ValueError as exc:
            raise GatewayError("E_REQUEST_INVALID", str(exc), http_status=422) from exc
        return service.submit_batch(
            specs,
            strict_capacity=strict_capacity,
            wait=effective_wait,
            wait_timeout_ms=effective_timeout,
        )

    @app.get("/requests")
    def list_requests(status: str | None = None, limit: int = Query(default=500, ge=1, le=5000)) -> dict[str, Any]:
        return {"ok": True, "requests": service.store.list_requests(status=status, limit=limit)}

    @app.get("/requests/{request_id}")
    def get_request(request_id: str, include_transitions: bool = False) -> dict[str, Any]:
        request = service.store.get_request(request_id)
        if request is None:
            raise GatewayError("E_NOT_FOUND", "Request not found", http_status=404)
        payload: dict[str, Any] = {"ok": True, "request": request}
        if include_transitions:
            payload["transitions"] = service.store.get_request_transitions(request_id)
        return payload

    @app.post("/requests/{request_id}/cancel")
    def cancel_request(request_id: str, body: CancelRequest | None = None) -> dict[str, Any]:
        request = service.cancel_request(request_id, (body or CancelRequest()).reason)
        if request is None:
            raise GatewayError("E_NOT_FOUND", "Request not found", http_status=404)
        return {"ok": True, "request": request}

    @app.post("/requests/{request_id}/transition")
    def transition_request(request_id: str, body: TransitionRequest) -> dict[str, Any]:
        request = service.store.manual_transition(
            request_id, body.to_status, body.reason_code, body.details
        )
        return {"ok": True, "request": request}

    @app.post("/rounds")
    @app.post("/rounds/run")
    @app.post("/scheduler/rounds")
    def run_round(
        options: RoundRequest | None = None,
        time_budget_ms: int | None = Query(default=None, ge=1, le=3_600_000),
        degrade_excess: bool | None = Query(default=None),
        max_dispatches: int | None = Query(default=None, ge=0),
    ) -> dict[str, Any]:
        effective = options or RoundRequest()
        overrides = {
            "time_budget_ms": time_budget_ms,
            "degrade_excess": degrade_excess,
            "max_dispatches": max_dispatches,
        }
        overrides = {key: value for key, value in overrides.items() if value is not None}
        if overrides:
            effective = effective.model_copy(update=overrides)
        round_row = service.run_round(effective)
        return {"ok": True, "round": round_row}

    @app.get("/rounds")
    def list_rounds(limit: int = Query(default=50, ge=1, le=1000)) -> dict[str, Any]:
        return {"ok": True, "rounds": service.store.list_rounds(limit=limit)}

    @app.get("/rounds/{round_id}")
    def get_round(round_id: int) -> dict[str, Any]:
        round_row = service.store.get_round(round_id)
        if round_row is None:
            raise GatewayError("E_NOT_FOUND", "Round not found", http_status=404)
        return {"ok": True, "round": round_row}

    @app.get("/backends")
    @app.get("/backends/status")
    def backend_status(
        include_retired: bool = False,
        route_version: int | None = None,
    ) -> dict[str, Any]:
        return {
            "ok": True,
            **service.backend_status(
                include_retired=include_retired, route_version=route_version
            ),
        }

    @app.post("/admin/backends/reset")
    def reset_backend(body: ResetBackendRequest) -> dict[str, Any]:
        return {
            "ok": True,
            "backend": service.store.reset_backend(body.route_version, body.backend_id),
        }

    @app.get("/recovery")
    @app.get("/recovery/report")
    def recovery_report() -> dict[str, Any]:
        report = service.store.latest_recovery_report()
        return {
            "ok": True,
            "report": report,
            "startup_report": service.recovery_report,
            "source": "persisted_facts_only",
        }

    @app.get("/recovery/reports")
    def recovery_reports(limit: int = Query(default=20, ge=1, le=1000)) -> dict[str, Any]:
        return {"ok": True, "reports": service.store.list_recovery_reports(limit=limit)}

    @app.get("/validation/report")
    def validation_report() -> dict[str, Any]:
        with service.store._lock:  # one consistent SQLite read snapshot
            snapshot = service.store.validation_snapshot()
        return {"ok": True, "report": validate_snapshot(snapshot)}

    @app.get("/")
    def root() -> dict[str, Any]:
        return {
            "ok": True,
            "service": "local-inference-gateway",
            "docs": "/docs",
            "health": "/health",
            "states": ["PENDING", "QUEUED", "DISPATCHED", "COMPLETED", "FAILED", "SHED"],
            "recovery_only_state": "ABORTED",
        }

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local inference gateway")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19080)
    parser.add_argument("--db", default=None)
    parser.add_argument("--log-level", default="info")
    args = parser.parse_args()
    config = GatewayConfig.from_env()
    if args.db:
        config.db_path = args.db
    app = create_app(config)
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)


if __name__ == "__main__":
    main()

