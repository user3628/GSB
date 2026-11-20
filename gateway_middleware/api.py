from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .config import Settings
from .errors import E_INTERNAL, E_INVALID_INPUT, E_INVALID_TRANSITION, DomainError, failure
from .models import MAX_TIMESTAMP_MS, BatchInput
from .service import GatewayService


def create_app(
    settings: Settings | None = None,
    *,
    service: GatewayService | None = None,
    auto_recover: bool = True,
) -> FastAPI:
    runtime_settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        runtime = service or GatewayService(runtime_settings)
        if auto_recover:
            runtime.recover()
        app.state.service = runtime
        yield

    app = FastAPI(
        title="Gateway Auth + Rate Decision Middleware",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.exception_handler(DomainError)
    async def domain_error_handler(request: Request, exc: DomainError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code or 400, content=exc.as_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content=failure(
                E_INVALID_INPUT,
                "request validation failed",
                errors=jsonable_encoder(exc.errors()),
            ),
        )

    @app.exception_handler(HTTPException)
    async def http_error_handler(request: Request, exc: HTTPException) -> JSONResponse:
        code = E_INVALID_TRANSITION if exc.status_code < 500 else E_INTERNAL
        return JSONResponse(
            status_code=exc.status_code,
            content=failure(code, str(exc.detail), status=exc.status_code),
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content=failure(E_INTERNAL, "unexpected internal error", error_type=type(exc).__name__),
        )

    def svc(request: Request) -> GatewayService:
        return request.app.state.service

    @app.get("/health")
    def health(request: Request) -> dict[str, Any]:
        runtime = svc(request)
        version = runtime.snapshot_info()["version"]
        return {"ok": True, "snapshot_version": version, "service": "gateway-middleware"}

    @app.post("/v1/batches")
    def submit_batch(batch: BatchInput, request: Request) -> dict[str, Any]:
        return svc(request).submit_batch(batch).to_dict()

    @app.get("/v1/batches/{batch_id}")
    def get_batch(batch_id: str, request: Request) -> dict[str, Any]:
        return svc(request).get_batch(batch_id).to_dict()

    @app.get("/v1/requests/{request_id}")
    def get_request_decision(request_id: str, request: Request) -> dict[str, Any]:
        return {"ok": True, "decision": svc(request).get_decision(request_id).to_dict()}

    @app.get("/v1/windows")
    def get_windows(
        request: Request,
        route_key: str = Query(min_length=1),
        client_id: str = Query(min_length=1),
        version: int | None = Query(default=None, ge=0),
        at_ms: int | None = Query(default=None, ge=0, le=MAX_TIMESTAMP_MS),
    ) -> dict[str, Any]:
        return svc(request).query_windows(
            route_key=route_key,
            client_id=client_id,
            version=version,
            at_ms=at_ms,
        )

    @app.get("/v1/sampling/errors")
    def get_sampling_errors(
        request: Request,
        route_key: str = Query(min_length=1),
        client_id: str = Query(min_length=1),
        version: int | None = Query(default=None, ge=0),
        at_ms: int | None = Query(default=None, ge=0, le=MAX_TIMESTAMP_MS),
    ) -> dict[str, Any]:
        return svc(request).sampling_explanation(
            route_key=route_key,
            client_id=client_id,
            version=version,
            at_ms=at_ms,
        )

    @app.get("/v1/recovery-report")
    def get_latest_recovery_report(request: Request) -> dict[str, Any]:
        report = svc(request).latest_recovery_report()
        return {"ok": True, "report": report.to_dict()}

    @app.get("/v1/recovery-report/{report_id}")
    def get_recovery_report(report_id: str, request: Request) -> dict[str, Any]:
        return {"ok": True, "report": svc(request).store.get_recovery_report(report_id).to_dict()}

    @app.get("/v1/snapshots/{version}")
    def get_snapshot(version: int, request: Request) -> dict[str, Any]:
        return {"ok": True, "snapshot": svc(request).snapshot_info(version)}

    return app


app = create_app()
