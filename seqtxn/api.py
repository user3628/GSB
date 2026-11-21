"""FastAPI surface for the middleware component."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from . import codes
from .codes import BusinessError
from .controller import MessageController


class TopicCreateRequest(BaseModel):
    name: str
    partition_count: Any = 1
    routing_mode: str = "hash"
    routing_salt: str | None = None
    config: dict | None = None


class ProduceRequest(BaseModel):
    producer_id: str
    client_sequence: Any
    body: Any = None
    partition: Any = None
    partition_key: Any = None
    txn_id: str | None = None
    transaction_id: str | None = None


class ClaimRequest(BaseModel):
    consumer_id: str
    visibility_timeout_ms: Any = 30_000


class AckRequest(BaseModel):
    lease_token: str
    sequence: Any = None
    consumer_id: str | None = None


class ReleaseRequest(BaseModel):
    lease_token: str
    reason: str = "consumer_release"


class ResolveRequest(BaseModel):
    idempotency_key: str | None = None
    timeout_ms: Any = None
    injection: str | None = None
    force: bool = False


class RecoverRequest(BaseModel):
    auto_renegotiate: bool | None = None


def _controller_from_env() -> MessageController:
    return MessageController(
        db_path=os.environ.get("SEQ_TXN_DB", "runtime/middleware.db"),
        coordinator_url=os.environ.get(
            "SEQ_TXN_COORDINATOR_URL", "http://127.0.0.1:18081"
        ),
        unreachable_url=os.environ.get(
            "SEQ_TXN_UNREACHABLE_URL", "http://127.0.0.1:1"
        ),
        query_timeout_seconds=float(os.environ.get("SEQ_TXN_QUERY_TIMEOUT", "0.5")),
        auto_renegotiate=os.environ.get("SEQ_TXN_AUTO_RENEGOTIATE", "1") != "0",
    )


def create_app(
    db_path: str | Path | None = None,
    coordinator_url: str | None = None,
    unreachable_url: str | None = None,
    query_timeout_seconds: float | None = None,
    auto_recover: bool = True,
    auto_renegotiate: bool | None = None,
) -> FastAPI:
    controller = MessageController(
        db_path=db_path or os.environ.get("SEQ_TXN_DB", "runtime/middleware.db"),
        coordinator_url=coordinator_url
        or os.environ.get("SEQ_TXN_COORDINATOR_URL", "http://127.0.0.1:18081"),
        unreachable_url=unreachable_url
        or os.environ.get("SEQ_TXN_UNREACHABLE_URL", "http://127.0.0.1:1"),
        query_timeout_seconds=query_timeout_seconds
        if query_timeout_seconds is not None
        else float(os.environ.get("SEQ_TXN_QUERY_TIMEOUT", "0.5")),
        auto_renegotiate=(
            auto_renegotiate
            if auto_renegotiate is not None
            else os.environ.get("SEQ_TXN_AUTO_RENEGOTIATE", "1") != "0"
        ),
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        controller.initialize()
        if auto_recover:
            controller.recover(auto_renegotiate=auto_renegotiate)
        yield

    app = FastAPI(
        title="seqtxn ordered + transactional message middleware",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.controller = controller

    @app.exception_handler(BusinessError)
    async def business_error_handler(request: Request, exc: BusinessError):
        return JSONResponse(
            status_code=codes.HTTP_STATUS.get(exc.code, 400),
            content=codes.fail(exc),
        )

    def invoke(fn, *args, **kwargs):
        try:
            return codes.ok(fn(*args, **kwargs))
        except BusinessError as exc:
            return JSONResponse(
                status_code=codes.HTTP_STATUS.get(exc.code, 400), content=codes.fail(exc)
            )

    @app.get("/health")
    def health():
        return codes.ok({"status": "ok", "db": controller.db_path})

    @app.post("/topics")
    def create_topic(request: TopicCreateRequest):
        return invoke(
            controller.create_topic,
            request.name,
            request.partition_count,
            request.routing_mode,
            request.routing_salt,
            request.config,
        )

    @app.get("/topics")
    def list_topics():
        return invoke(controller.list_topics)

    @app.get("/topics/{topic}")
    def get_topic(topic: str):
        return invoke(controller.topic_detail, topic)

    @app.post("/topics/{topic}/messages")
    def publish(topic: str, request: ProduceRequest):
        txn_id = request.txn_id if request.txn_id is not None else request.transaction_id
        return invoke(
            controller.publish,
            topic,
            request.producer_id,
            request.client_sequence,
            request.body,
            request.partition,
            request.partition_key,
            txn_id,
        )

    @app.get("/topics/{topic}/partitions/{partition_no}")
    def partition_detail(topic: str, partition_no: int):
        return invoke(controller.partition_detail, topic, partition_no)

    @app.get("/topics/{topic}/partitions/{partition_no}/messages")
    def list_messages(
        topic: str,
        partition_no: int,
        state: str | None = Query(default=None),
        limit: int = Query(default=1_000, ge=1, le=10_000),
    ):
        return invoke(controller.list_messages, topic, partition_no, state, limit)

    @app.post("/topics/{topic}/partitions/{partition_no}/claim")
    def claim(topic: str, partition_no: int, request: ClaimRequest):
        return invoke(
            controller.claim,
            topic,
            partition_no,
            request.consumer_id,
            request.visibility_timeout_ms,
        )

    @app.post("/acks")
    def ack(request: AckRequest):
        return invoke(controller.ack, request.lease_token, request.sequence, request.consumer_id)

    @app.post("/leases/release")
    def release(request: ReleaseRequest):
        return invoke(controller.release, request.lease_token, request.reason)

    @app.post("/transactions/{txn_id}/resolve")
    def resolve(txn_id: str, request: ResolveRequest | None = None):
        request = request or ResolveRequest()
        return invoke(
            controller.resolve_transaction,
            txn_id,
            request.idempotency_key,
            request.timeout_ms,
            request.injection,
            request.force,
            "api",
        )

    @app.get("/transactions/{txn_id}")
    def get_transaction(txn_id: str):
        return invoke(controller.transaction_detail, txn_id)

    @app.get("/audit/events")
    def audit_events(
        topic: str | None = None,
        txn_id: str | None = None,
        after_event_id: int = 0,
        limit: int = Query(default=5_000, ge=1, le=50_000),
    ):
        return invoke(controller.list_audit_events, topic, txn_id, after_event_id, limit)

    @app.get("/admin/fault-events")
    def fault_events():
        return invoke(controller.list_fault_events)

    @app.get("/admin/recovery-report")
    def recovery_report():
        return invoke(controller.recovery_report)

    @app.post("/admin/recover")
    def recover(request: RecoverRequest):
        return invoke(controller.recover, request.auto_renegotiate)

    return app


app = create_app()

