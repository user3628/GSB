"""FastAPI transport for the local SLI/SLO engine."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, HTTPException, Path, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .config import Settings, window_label
from .engine import SLIEngine
from .errors import BusinessError, invalid_request

LOGGER = logging.getLogger("slo_budget.api")


class EventPayload(BaseModel):
    """Accepts the canonical fields plus common local-sample aliases."""

    model_config = ConfigDict(extra="forbid", strict=True)

    timestamp: Any | None = None
    ts: Any | None = None
    time: Any | None = None
    service: str
    sli_type: str | None = None
    sli: str | None = None
    good: bool | None = None
    is_good: bool | None = None
    good_event: bool | None = None
    event_id: str | None = None

    @model_validator(mode="after")
    def validate_aliases(self) -> "EventPayload":
        provided_timestamps = [
            item for item in (self.timestamp, self.ts, self.time) if item is not None
        ]
        provided_sli = [item for item in (self.sli_type, self.sli) if item is not None]
        provided_good = [
            item for item in (self.good, self.is_good, self.good_event) if item is not None
        ]
        if len(provided_timestamps) != 1:
            raise ValueError("Provide exactly one of timestamp, ts, or time")
        if len(provided_sli) != 1:
            raise ValueError("Provide exactly one of sli_type or sli")
        if len(provided_good) != 1:
            raise ValueError("Provide exactly one of good, is_good, or good_event")
        return self

    def as_engine_event(self) -> dict[str, Any]:
        timestamp = next(
            item for item in (self.timestamp, self.ts, self.time) if item is not None
        )
        sli_type = next(
            item for item in (self.sli_type, self.sli) if item is not None
        )
        good = next(
            item for item in (self.good, self.is_good, self.good_event) if item is not None
        )
        event = {
            "timestamp": timestamp,
            "service": self.service,
            "sli_type": sli_type,
            "good": good,
        }
        if self.event_id is not None:
            event["event_id"] = self.event_id
        return event


class IngestRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    events: list[EventPayload] = Field(min_length=1)
    batch_id: str | None = None
    source: str | None = None


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved_settings = settings or Settings.from_env()
    engine = SLIEngine(resolved_settings)
    startup_recovery = engine.recover(reason="startup")
    app = FastAPI(
        title="Local SLI/SLO Error Budget Service",
        version="1.0.0",
        description=(
            "Versioned, SQLite-backed SLI/SLO calculation with sliding windows, "
            "deterministic sampling, multi-window burn-rate alerts, and crash recovery."
        ),
    )
    app.state.engine = engine
    app.state.startup_recovery = startup_recovery

    @app.exception_handler(BusinessError)
    async def business_error_handler(_: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.as_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "code": "E_VALIDATION",
                "message": "Request validation failed.",
                "details": {"errors": jsonable_encoder(exc.errors())},
                "error": {
                    "code": "E_VALIDATION",
                    "message": "Request validation failed.",
                    "details": {"errors": jsonable_encoder(exc.errors())},
                }
            },
        )

    @app.exception_handler(HTTPException)
    async def http_error_handler(_: Request, exc: HTTPException) -> JSONResponse:
        code = "E_NOT_FOUND" if exc.status_code == 404 else "E_HTTP"
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "code": code,
                "message": str(exc.detail),
                "details": {},
                "error": {
                    "code": code,
                    "message": str(exc.detail),
                    "details": {},
                }
            },
        )

    @app.exception_handler(Exception)
    async def unhandled_error_handler(_: Request, exc: Exception) -> JSONResponse:
        LOGGER.exception("Unhandled API error", exc_info=exc)
        return JSONResponse(
            status_code=500,
            content={
                "code": "E_INTERNAL",
                "message": "An internal error occurred. Inspect server logs for details.",
                "details": {},
                "error": {
                    "code": "E_INTERNAL",
                    "message": "An internal error occurred. Inspect server logs for details.",
                    "details": {},
                }
            },
        )

    @app.get("/")
    def root() -> dict[str, Any]:
        return {
            "service": "slo-budget",
            "version": "1.0.0",
            "docs": "/docs",
            "health": "/healthz",
        }

    @app.get("/healthz")
    def health() -> dict[str, Any]:
        return engine.health()

    @app.get("/v1/config")
    def config() -> dict[str, Any]:
        return {
            "base_bucket_seconds": resolved_settings.bucket_seconds,
            "windows": [window_label(item) for item in resolved_settings.windows],
            "slo_target": resolved_settings.slo_target,
            "minimum_samples": resolved_settings.minimum_samples,
            "exact_event_limit": resolved_settings.exact_event_limit,
            "late_tolerance_seconds": resolved_settings.late_tolerance_seconds,
            "alert_short_window": window_label(resolved_settings.alert_short_window),
            "alert_long_window": window_label(resolved_settings.alert_long_window),
            "warning_burn_rate": resolved_settings.warning_burn_rate,
            "critical_burn_rate": resolved_settings.critical_burn_rate,
            "confidence_level": resolved_settings.confidence_level,
            "late_policy": "reject_atomically_and_audit",
            "write_model": "single_writer_atomic_version_commit",
        }

    @app.post("/v1/events")
    def ingest(payload: IngestRequest) -> dict[str, Any]:
        return engine.ingest(
            [event.as_engine_event() for event in payload.events],
            batch_id=payload.batch_id,
            source=payload.source,
        )

    @app.get("/v1/services/{service}/sli")
    def service_sli(
        service: str = Path(..., min_length=1, max_length=128),
        window: str = Query("5m"),
        sli_type: str = Query("availability"),
        as_of: str | None = Query(None),
        version: int | None = Query(None, ge=0),
    ) -> dict[str, Any]:
        result = engine.window_metrics(
            service, sli_type, window, as_of=as_of, version=version
        )
        return _require_sufficient(result)

    @app.get("/v1/services/{service}/budget")
    def service_budget(
        service: str = Path(..., min_length=1, max_length=128),
        window: str = Query("5m"),
        sli_type: str = Query("availability"),
        as_of: str | None = Query(None),
        version: int | None = Query(None, ge=0),
    ) -> dict[str, Any]:
        result = engine.window_metrics(
            service, sli_type, window, as_of=as_of, version=version
        )
        return _require_sufficient(result)

    @app.get("/v1/services/{service}/windows")
    def service_windows(
        service: str = Path(..., min_length=1, max_length=128),
        windows: str = Query("5m,30m,1h"),
        sli_type: str = Query("availability"),
        as_of: str | None = Query(None),
        version: int | None = Query(None, ge=0),
    ) -> dict[str, Any]:
        durations = [item.strip() for item in windows.split(",") if item.strip()]
        if not durations:
            raise invalid_request("At least one window is required.")
        results = engine.windows_metrics(
            service, sli_type, durations, as_of=as_of, version=version
        )
        return {
            "version": results[0]["version"] if results else 0,
            "service": service,
            "sli_type": sli_type,
            "windows": results,
            "has_insufficient_data": any(
                item["status"] == "E_INSUFFICIENT_DATA" for item in results
            ),
        }

    @app.get("/v1/services/{service}/budget-summary")
    def service_budget_summary(
        service: str = Path(..., min_length=1, max_length=128),
        sli_type: str = Query("all"),
        start: str | None = Query(None),
        end: str | None = Query(None),
        version: int | None = Query(None, ge=0),
        include_rolling: bool = Query(True),
    ) -> dict[str, Any]:
        return engine.budget_summary(
            service,
            sli_type=sli_type,
            start=start,
            end=end,
            version=version,
            include_rolling=include_rolling,
        )

    @app.get("/v1/alerts")
    def alerts(
        service: str | None = Query(None),
        version: int | None = Query(None, ge=0),
    ) -> dict[str, Any]:
        return engine.alerts(service=service, version=version)

    @app.get("/v1/rejections")
    def rejections(
        batch_id: str | None = Query(None),
        service: str | None = Query(None),
        limit: int = Query(100, ge=1, le=1000),
        version: int | None = Query(None, ge=0),
    ) -> dict[str, Any]:
        return engine.rejections(
            batch_id=batch_id,
            service=service,
            limit=limit,
            version=version,
        )

    @app.get("/v1/recovery")
    def recovery() -> dict[str, Any]:
        report = engine.recovery_report()
        if report is None:
            raise BusinessError(
                "E_RECOVERY_NOT_FOUND",
                "No startup recovery report exists.",
                http_status=404,
            )
        return report

    return app


def _require_sufficient(result: dict[str, Any]) -> dict[str, Any]:
    if result.get("status") != "E_INSUFFICIENT_DATA":
        return result
    raise BusinessError(
        "E_INSUFFICIENT_DATA",
        "The window contains fewer events than the configured minimum sample size.",
        http_status=422,
        details={
            "service": result["service"],
            "sli_type": result["sli_type"],
            "window": result["window"],
            "sample_count": result["sample_count"],
            "minimum_sample_count": result["minimum_sample_count"],
            "version": result["version"],
            "window_start": result["window_start"],
            "window_end": result["window_end"],
        },
    )
