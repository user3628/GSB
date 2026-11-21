from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException

from .errors import ERROR_CATALOG, BusinessError
from .service import EventBus, state_machine_document, utc_now
from .verifier import StateVerifier


logger = logging.getLogger("eventbus.api")


class TopicCreate(BaseModel):
    topic: str
    partitions: int
    group_id: str
    min_retained_seq: int = 1


class PublishBody(BaseModel):
    batch_id: str | None = None
    events: Any
    test_crash_after_intent: bool = False


class PollBody(BaseModel):
    topic: str
    group_id: str
    partition: int
    consumer_id: str
    lease_seconds: float = 30.0
    test_crash_after_intent: bool = False


class AckBody(BaseModel):
    topic: str
    group_id: str
    partition: int
    consumer_id: str
    event_id: str
    lease_token: str
    test_crash_after_intent: bool = False


class RewindBody(BaseModel):
    topic: str
    group_id: str
    partition: int
    target_seq: int
    reason: str = "operator_rewind"
    test_crash_after_intent: bool = False


class DeadLetterBody(BaseModel):
    topic: str
    group_id: str
    partition: int
    event_id: str
    reason: str = "operator_dead_letter"


def _ok(data: Any = None, **extra: Any) -> dict[str, Any]:
    response: dict[str, Any] = {"ok": True}
    if data is not None:
        if isinstance(data, dict):
            response.update(data)
        else:
            response["data"] = data
    response.update(extra)
    return response


def create_app(
    db_path: str | Path | None = None,
    *,
    auto_recover: bool = True,
    test_hooks: bool | None = None,
) -> FastAPI:
    resolved_db = Path(
        db_path or os.getenv("EVENTBUS_DB", "var/eventbus.db")
    ).expanduser().resolve()
    hooks_enabled = (
        test_hooks
        if test_hooks is not None
        else os.getenv("EVENTBUS_ENABLE_TEST_HOOKS", "0") == "1"
    )
    bus = EventBus(resolved_db, auto_recover=auto_recover, test_hooks=hooks_enabled)
    app = FastAPI(
        title="Deterministic Local Event Bus",
        version="0.1.0",
        description="SQLite-backed local event bus with lease delivery, replay and recovery.",
    )
    app.state.bus = bus
    app.state.db_path = resolved_db

    @app.exception_handler(BusinessError)
    async def business_error_handler(_: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.as_response())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        _: Request, exc: RequestValidationError
    ) -> JSONResponse:
        details = []
        for item in exc.errors():
            details.append(
                {
                    "type": item.get("type"),
                    "loc": list(item.get("loc", [])),
                    "msg": item.get("msg"),
                    "input": item.get("input"),
                }
            )
        error = BusinessError(
            "E_REQUEST_INVALID",
            "请求结构校验失败。",
            {"errors": details},
        )
        return JSONResponse(status_code=error.http_status or 400, content=error.as_response())

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = "E_ROUTE_NOT_FOUND" if exc.status_code == 404 else "E_REQUEST_INVALID"
        error = BusinessError(
            code,
            str(exc.detail),
            {"http_status": exc.status_code},
            http_status=exc.status_code,
        )
        return JSONResponse(status_code=exc.status_code, content=error.as_response())

    @app.exception_handler(Exception)
    async def unexpected_error_handler(_: Request, exc: Exception) -> JSONResponse:
        logger.exception("unhandled request error", exc_info=exc)
        error = BusinessError(
            "E_INTERNAL",
            "内部错误。",
            {"exception_type": type(exc).__name__, "detail": str(exc)[:300]},
        )
        return JSONResponse(status_code=500, content=error.as_response())

    @app.get("/health")
    def health() -> dict[str, Any]:
        return _ok(
            {
                "service": "deterministic-local-event-bus",
                "status": "ok",
                "db_path": str(resolved_db),
                "test_hooks": hooks_enabled,
                "time": utc_now(),
            }
        )

    @app.get("/api/v1/error-codes")
    def error_codes() -> dict[str, Any]:
        return _ok({"error_codes": ERROR_CATALOG})

    @app.get("/api/v1/meta/state-machine")
    def state_machine() -> dict[str, Any]:
        return _ok(state_machine_document())

    @app.post("/api/v1/topics")
    def create_topic(body: TopicCreate) -> dict[str, Any]:
        return _ok(
            bus.create_topic(
                body.topic,
                partitions=body.partitions,
                group_id=body.group_id,
                min_retained_seq=body.min_retained_seq,
            )
        )

    @app.get("/api/v1/topics")
    def list_topics() -> dict[str, Any]:
        return _ok({"topics": bus.list_topics()})

    @app.get("/api/v1/topics/{topic}")
    def get_topic(topic: str) -> dict[str, Any]:
        return _ok(bus.get_topic(topic))

    @app.post("/api/v1/topics/{topic}/events")
    @app.post("/api/v1/topics/{topic}/publish")
    def publish(topic: str, body: PublishBody) -> dict[str, Any]:
        return _ok(
            bus.publish(
                topic,
                body.batch_id,
                body.events,
                crash_after_intent=body.test_crash_after_intent,
            )
        )

    @app.get("/api/v1/events")
    def list_events(
        topic: str | None = None,
        partition: int | None = None,
        state: str | None = None,
        limit: int = Query(default=200, ge=1, le=5000),
    ) -> dict[str, Any]:
        return _ok(
            {
                "events": bus.list_events(
                    topic_id=topic, partition=partition, state=state, limit=limit
                )
            }
        )

    @app.get("/api/v1/events/{event_id}")
    def get_event(event_id: str) -> dict[str, Any]:
        return _ok(bus.get_event(event_id))

    @app.post("/api/v1/events/{event_id}/dead-letter")
    def dead_letter(event_id: str, body: DeadLetterBody) -> dict[str, Any]:
        if body.event_id != event_id:
            raise BusinessError(
                "E_REQUEST_INVALID",
                "路径与请求体 event_id 不一致。",
                {"path_event_id": event_id, "body_event_id": body.event_id},
            )
        return _ok(
            bus.dead_letter(
                body.topic,
                body.group_id,
                body.partition,
                event_id,
                reason=body.reason,
            )
        )

    @app.post("/api/v1/consumers/poll")
    def poll(body: PollBody) -> dict[str, Any]:
        return _ok(
            bus.poll(
                body.topic,
                body.group_id,
                body.partition,
                body.consumer_id,
                lease_seconds=body.lease_seconds,
                crash_after_intent=body.test_crash_after_intent,
            )
        )

    @app.post("/api/v1/consumers/ack")
    def ack(body: AckBody) -> dict[str, Any]:
        return _ok(
            bus.ack(
                body.topic,
                body.group_id,
                body.partition,
                body.consumer_id,
                body.event_id,
                body.lease_token,
                crash_after_intent=body.test_crash_after_intent,
            )
        )

    @app.get("/api/v1/consumers/offset")
    def get_offset(topic: str, group_id: str, partition: int) -> dict[str, Any]:
        return _ok(bus.get_offset(topic, group_id, partition))

    @app.post("/api/v1/consumers/offset/rewind")
    def rewind(body: RewindBody) -> dict[str, Any]:
        return _ok(
            bus.rewind_offset(
                body.topic,
                body.group_id,
                body.partition,
                body.target_seq,
                reason=body.reason,
                crash_after_intent=body.test_crash_after_intent,
            )
        )

    @app.get("/api/v1/aggregates")
    def aggregates(topic: str, group_id: str) -> dict[str, Any]:
        return _ok(bus.get_aggregates(topic, group_id))

    @app.get("/api/v1/snapshot")
    def snapshot(topic: str, group_id: str) -> dict[str, Any]:
        return _ok(bus.snapshot(topic, group_id))

    @app.get("/api/v1/recovery/reports/latest")
    def recovery_report() -> dict[str, Any]:
        report = bus.get_latest_recovery_report()
        return _ok({"report": report})

    @app.get("/api/v1/verify")
    def verify(topic: str | None = None) -> dict[str, Any]:
        result = StateVerifier(resolved_db).run(topic_id=topic)
        return result

    return app
