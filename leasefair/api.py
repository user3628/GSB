"""Local HTTP API for LeaseFair."""

from __future__ import annotations

import json
import os
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import E_CONFLICTING_REQUEST, E_INTERNAL, E_INVALID_REQUEST, LeaseError
from .service import LeaseService
from .storage import Store


class AcquireBody(BaseModel):
    request_id: str = Field(min_length=1, max_length=256)
    holder_id: str = Field(min_length=1, max_length=512)
    ttl_ms: int = Field(gt=0)
    weight: Decimal


class RenewBody(BaseModel):
    request_id: str = Field(min_length=1, max_length=256)
    holder_id: str = Field(min_length=1, max_length=512)
    version: int = Field(gt=0)
    ttl_ms: int | None = Field(default=None, gt=0)


class ReleaseBody(BaseModel):
    request_id: str = Field(min_length=1, max_length=256)
    holder_id: str = Field(min_length=1, max_length=512)
    version: int = Field(gt=0)


class CandidateBody(BaseModel):
    holder_id: str = Field(min_length=1, max_length=512)
    ttl_ms: int = Field(gt=0)
    weight: Decimal


class AcquireBestBody(BaseModel):
    request_id: str = Field(min_length=1, max_length=256)
    candidates: list[CandidateBody] = Field(min_length=1)


class ArbitrateBody(BaseModel):
    candidates: list[CandidateBody] = Field(min_length=1)


class RegisterBody(BaseModel):
    lock_key: str = Field(min_length=1, max_length=512)


BUSINESS_STATUS = {
    E_CONFLICTING_REQUEST: 409,
    E_INVALID_REQUEST: 422,
    E_INTERNAL: 500,
}


def _status_for(payload: dict[str, Any]) -> int:
    return BUSINESS_STATUS.get(str(payload.get("code", "")), 200)


def _json(payload: dict[str, Any]) -> JSONResponse:
    return JSONResponse(status_code=_status_for(payload), content=payload)


def _root_dir() -> Path:
    return Path(__file__).resolve().parent.parent


def create_app(
    db_path: str | os.PathLike[str] | None = None,
    *,
    clock: Any = None,
    auto_recover: bool = True,
) -> FastAPI:
    resolved_db = Path(db_path or os.environ.get("LEASE_DB_PATH", "data/lease.db"))
    service = LeaseService(resolved_db, clock=clock, auto_recover=auto_recover)
    app = FastAPI(
        title="LeaseFair local lease-lock middleware",
        version="0.1.0",
        description="Local multiprocess lease locks with deterministic Q9 metrics.",
    )
    app.state.service = service

    @app.exception_handler(LeaseError)
    async def lease_error_handler(request: Request, exc: LeaseError) -> JSONResponse:
        payload = exc.payload()
        return _json(payload)

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "code": E_INVALID_REQUEST,
                "message": "request validation failed",
                "details": exc.errors(),
            },
        )

    @app.exception_handler(Exception)
    async def internal_error_handler(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "code": E_INTERNAL,
                "message": "internal error",
                "error_type": type(exc).__name__,
            },
        )

    @app.get("/healthz")
    def healthz() -> dict[str, Any]:
        return {"ok": True, "service": "leasefair", "now_ms": service.now_ms()}

    @app.post("/v1/locks/{lock_key:path}/acquire")
    def acquire(lock_key: str, body: AcquireBody) -> JSONResponse:
        return _json(
            service.acquire(
                lock_key,
                request_id=body.request_id,
                holder_id=body.holder_id,
                ttl_ms=body.ttl_ms,
                weight=body.weight,
            )
        )

    @app.post("/v1/locks/{lock_key:path}/renew")
    def renew(lock_key: str, body: RenewBody) -> JSONResponse:
        return _json(
            service.renew(
                lock_key,
                request_id=body.request_id,
                holder_id=body.holder_id,
                version=body.version,
                ttl_ms=body.ttl_ms,
            )
        )

    @app.post("/v1/locks/{lock_key:path}/release")
    def release(lock_key: str, body: ReleaseBody) -> JSONResponse:
        return _json(
            service.release(
                lock_key,
                request_id=body.request_id,
                holder_id=body.holder_id,
                version=body.version,
            )
        )

    @app.post("/v1/locks/{lock_key:path}/acquire-best")
    def acquire_best(lock_key: str, body: AcquireBestBody) -> JSONResponse:
        return _json(
            service.acquire_best(
                lock_key,
                request_id=body.request_id,
                candidates=[candidate.model_dump() for candidate in body.candidates],
            )
        )

    @app.post("/v1/locks/{lock_key:path}/arbitrate")
    def arbitrate(
        lock_key: str,
        body: ArbitrateBody,
        now_ms: int | None = Query(default=None),
    ) -> JSONResponse:
        return _json(
            service.arbitrate(
                lock_key,
                candidates=[candidate.model_dump() for candidate in body.candidates],
                now_ms=now_ms,
            )
        )

    @app.get("/v1/locks/{lock_key:path}")
    def get_lock(lock_key: str, now_ms: int | None = Query(default=None)) -> JSONResponse:
        return _json(service.get_lock(lock_key, now_ms=now_ms))

    @app.get("/v1/metrics")
    def metrics(now_ms: int | None = Query(default=None)) -> JSONResponse:
        return _json(service.metrics(now_ms=now_ms))

    @app.post("/v1/metrics/recompute")
    def recompute_metrics() -> JSONResponse:
        return _json(service.recompute_metrics())

    @app.get("/v1/recovery")
    def recovery() -> JSONResponse:
        report = service.recovery_report()
        return _json(
            {
                "ok": True,
                "code": "OK",
                "report": report,
                "caveat": None
                if report is None
                else report["report"].get("caveat"),
            }
        )

    @app.get("/v1/diagnostics")
    def diagnostics(now_ms: int | None = Query(default=None)) -> JSONResponse:
        return _json(service.diagnostics(now_ms=now_ms))

    @app.get("/v1/events")
    def events(
        lock_key: str | None = Query(default=None), limit: int = Query(default=200, ge=1, le=2000)
    ) -> JSONResponse:
        return _json(service.events(lock_key=lock_key, limit=limit))

    @app.get("/v1/intents")
    def intentions(
        phase: str | None = Query(default=None),
        limit: int = Query(default=200, ge=1, le=2000),
    ) -> JSONResponse:
        return _json(service.intentions(phase=phase, limit=limit))

    @app.get("/v1/samples")
    def samples() -> JSONResponse:
        path = Path(os.environ.get("LEASE_SAMPLE_PATH", _root_dir() / "data" / "fixed_lease_samples.json"))
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return JSONResponse(content={"ok": True, "code": "OK", "path": str(path), "samples": payload})

    @app.post("/v1/registry/keys")
    def register_key(body: RegisterBody) -> JSONResponse:
        return _json(service.register_key(body.lock_key))

    return app


app = create_app()

