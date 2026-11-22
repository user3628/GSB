from __future__ import annotations

import binascii
from contextlib import asynccontextmanager
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, model_validator

from .errors import BusinessError, HTTP_STATUS_BY_CODE
from .listener import build_wire_result
from .models import ItemResult
from .protocol import SUPPORTED_TYPES, TYPE_BY_NAME, NAME_BY_TYPE, parse_query_packet
from .runtime import Runtime


class QueryBody(BaseModel):
    name: str | None = None
    wire_hex: str | None = None
    type: str | int = "A"
    qclass: int = 1
    query_id: str | None = None
    priority: int = 0
    hot: bool = False
    round_budget_ms: int | None = Field(default=None, ge=1, le=600000)

    @model_validator(mode="after")
    def exactly_one_query_form(self) -> "QueryBody":
        if (self.name is None) == (self.wire_hex is None):
            raise ValueError("provide exactly one of name or wire_hex")
        return self


class BatchBody(BaseModel):
    requests: list[QueryBody] | None = None
    queries: list[QueryBody] | None = None
    round_budget_ms: int | None = Field(default=None, ge=1, le=600000)
    round_id: str | None = None

    @model_validator(mode="after")
    def one_request_list(self) -> "BatchBody":
        if self.requests is not None and self.queries is not None:
            raise ValueError("provide only one of requests or queries")
        items = self.requests if self.requests is not None else self.queries
        if not items:
            raise ValueError("batch must contain at least one query")
        return self

    @property
    def items(self) -> list[QueryBody]:
        return self.requests if self.requests is not None else (self.queries or [])


class RawWireBody(BaseModel):
    wire_hex: str
    transport: Literal["udp", "tcp"] = "udp"


def _error_response(exc: BusinessError) -> JSONResponse:
    return JSONResponse(status_code=exc.http_status, content=jsonable_encoder(exc.to_dict()))


def _item_to_http_error(item: ItemResult, batch: dict[str, Any] | None = None) -> BusinessError:
    context = dict(item.context)
    context.update({"query_id": item.query_id, "cache_key": item.cache_key})
    if batch is not None:
        context["round_id"] = batch.get("round_id")
        context["trace_summary"] = {
            "dispatch": batch.get("trace", {}).get("dispatch", []),
            "waiting": batch.get("trace", {}).get("waiting", []),
            "skipped": batch.get("trace", {}).get("skipped", []),
        }
    return BusinessError(
        code=item.business_code or "E_INTERNAL",
        message=item.message or item.business_code or "query failed",
        context=context,
        http_status=HTTP_STATUS_BY_CODE.get(item.business_code or "E_INTERNAL", 500),
        dns_rcode=item.rcode,
    )


def _decode_wire_hex(wire_hex: str) -> bytes:
    try:
        return bytes.fromhex(wire_hex.strip())
    except (ValueError, binascii.Error) as exc:
        raise BusinessError(
            code="E_MESSAGE_INVALID",
            message="wire_hex is not valid hexadecimal",
            context={"wire_hex": wire_hex},
            http_status=HTTP_STATUS_BY_CODE["E_MESSAGE_INVALID"],
        ) from exc


def _parse_cache_qtype(value: str | int | None) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, str) and value.upper() in TYPE_BY_NAME:
            qtype = TYPE_BY_NAME[value.upper()]
        else:
            qtype = int(value)
    except (TypeError, ValueError) as exc:
        raise BusinessError(
            code="E_REQUEST_INVALID",
            message="cache type must be a supported DNS type name or number",
            context={"type": value},
            http_status=HTTP_STATUS_BY_CODE["E_REQUEST_INVALID"],
        ) from exc
    if qtype not in SUPPORTED_TYPES:
        raise BusinessError(
            code="E_REQUEST_INVALID",
            message="unsupported cache query type",
            context={"type": value, "supported": sorted(NAME_BY_TYPE[item] for item in SUPPORTED_TYPES)},
            http_status=HTTP_STATUS_BY_CODE["E_REQUEST_INVALID"],
        )
    return qtype


def _build_query_specs(service: Any, bodies: list[QueryBody]) -> tuple[list[Any], list[dict[str, Any]]]:
    specs = []
    parsed_wire: list[dict[str, Any]] = []
    for index, body in enumerate(bodies):
        if body.wire_hex is not None:
            packet = _decode_wire_hex(body.wire_hex)
            query = parse_query_packet(packet)
            question = query.questions[0]
            spec = service.prepare_spec(
                name=question.name.canonical,
                qtype=question.qtype,
                qclass=question.qclass,
                query_id=body.query_id,
                priority=body.priority,
                hot=body.hot,
                arrival_seq=index,
            )
            parsed_wire.append({"wire_hex": body.wire_hex, "query": query})
        else:
            spec = service.prepare_spec(
                name=body.name or "",
                qtype=body.type,
                qclass=body.qclass,
                query_id=body.query_id,
                priority=body.priority,
                hot=body.hot,
                arrival_seq=index,
            )
            parsed_wire.append({})
        specs.append(spec)
    return specs, parsed_wire


def create_app(runtime: Runtime, *, manage_lifecycle: bool = False) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if manage_lifecycle:
            await runtime.start()
        try:
            yield
        finally:
            if manage_lifecycle:
                await runtime.stop(clean=True)

    app = FastAPI(
        title="DNS Workbench",
        version="0.1.0",
        description="Loopback DNS recursive cache middleware diagnostics",
        lifespan=lifespan,
    )

    @app.exception_handler(BusinessError)
    async def business_error_handler(_: Request, exc: BusinessError) -> JSONResponse:
        return _error_response(exc)

    @app.exception_handler(RequestValidationError)
    async def validation_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "E_REQUEST_INVALID",
                    "message": "HTTP request validation failed",
                    "context": {"errors": jsonable_encoder(exc.errors())},
                }
            },
        )

    @app.exception_handler(HTTPException)
    async def http_handler(_: Request, exc: HTTPException) -> JSONResponse:
        detail = exc.detail if isinstance(exc.detail, dict) else {"message": str(exc.detail)}
        code = detail.pop("code", "E_NOT_FOUND" if exc.status_code == 404 else "E_REQUEST_INVALID")
        message = detail.pop("message", "HTTP request failed")
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": {"code": code, "message": message, "context": detail}},
        )

    @app.exception_handler(Exception)
    async def internal_handler(_: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": "E_INTERNAL",
                    "message": str(exc),
                    "context": {"exception_type": type(exc).__name__},
                }
            },
        )

    def service() -> Any:
        if runtime.service is None:
            raise HTTPException(status_code=503, detail={"code": "E_INTERNAL", "message": "runtime is not started"})
        return runtime.service

    @app.get("/health")
    @app.get("/api/v1/health")
    async def health() -> dict[str, Any]:
        return await runtime.health()

    @app.get("/api/v1/config")
    async def config_info() -> dict[str, Any]:
        return runtime.config.redacted()

    @app.post("/api/v1/queries")
    @app.post("/api/v1/query")
    async def submit_query(body: QueryBody) -> dict[str, Any]:
        svc = service()
        specs, _ = _build_query_specs(svc, [body])
        batch = await svc.submit_batch(specs, round_budget_ms=body.round_budget_ms)
        payload = batch.to_dict()
        item = batch.items[0]
        if item.business_code:
            raise _item_to_http_error(item, payload)
        return {"round_id": batch.round_id, "result": item.to_dict(), "trace": batch.trace}

    @app.post("/api/v1/queries/batch")
    @app.post("/api/v1/batch")
    async def submit_batch(body: BatchBody, fail_fast: bool = Query(default=False)) -> dict[str, Any]:
        svc = service()
        specs, _ = _build_query_specs(svc, body.items)
        batch = await svc.submit_batch(specs, round_budget_ms=body.round_budget_ms, round_id=body.round_id)
        payload = batch.to_dict()
        if fail_fast:
            failure = next((item for item in batch.items if item.business_code), None)
            if failure is not None:
                raise _item_to_http_error(failure, payload)
        return payload

    @app.post("/api/v1/wire/query")
    async def submit_wire_query(body: RawWireBody) -> dict[str, Any]:
        svc = service()
        packet = _decode_wire_hex(body.wire_hex)
        query = parse_query_packet(packet)
        question = query.questions[0]
        spec = svc.prepare_spec(
            name=question.name.canonical,
            qtype=question.qtype,
            qclass=question.qclass,
            source_transport=body.transport,
            client="http-diagnostic",
        )
        batch = await svc.submit_batch([spec])
        item = batch.items[0]
        wire = build_wire_result(query, item, body.transport, runtime.config.max_udp_response_bytes)
        return {
            "round_id": batch.round_id,
            "result": item.to_dict(),
            "wire_response_hex": wire.hex(),
            "trace": batch.trace,
        }

    @app.get("/api/v1/cache")
    async def cache_query(
        name: str | None = None,
        type: str | int | None = None,
        include_expired: bool = False,
    ) -> dict[str, Any]:
        svc = service()
        qtype = _parse_cache_qtype(type)
        if name is None:
            entries = await runtime.storage.list_cache(include_expired=include_expired)
            if not include_expired:
                await runtime.storage.cleanup_expired()
            views = [entry.to_dict() for entry in entries]
            return {"count": len(views), "entries": views}
        views = await svc.get_cache_entries(name, qtype, include_expired=include_expired)
        return {"count": len(views), "entries": views}

    @app.get("/api/v1/cache/by-name/{name:path}")
    async def cache_by_name(name: str, type: str | int | None = None) -> dict[str, Any]:
        return await cache_query(name=name, type=type, include_expired=False)

    @app.get("/api/v1/cache/key/{cache_key:path}")
    async def cache_by_key(cache_key: str) -> dict[str, Any]:
        view = await service().get_cache_key(cache_key)
        if view is None:
            raise HTTPException(
                status_code=404,
                detail={"code": "E_CACHE_MISS", "message": "cache entry is absent or expired", "cache_key": cache_key},
            )
        return view

    @app.get("/api/v1/cache/{name:path}")
    async def cache_by_name_alias(name: str, type: str | int | None = None) -> dict[str, Any]:
        return await cache_query(name=name, type=type, include_expired=False)

    @app.get("/api/v1/sessions/{query_id}")
    @app.get("/api/v1/queries/{query_id}")
    async def get_session(query_id: str) -> dict[str, Any]:
        session = await service().session(query_id)
        if session is None:
            raise HTTPException(status_code=404, detail={"code": "E_SESSION_NOT_FOUND", "message": "session not found"})
        return session

    @app.get("/api/v1/rounds/{round_id}")
    async def get_round(round_id: str) -> dict[str, Any]:
        round_data = await service().round(round_id)
        if round_data is None:
            raise HTTPException(status_code=404, detail={"code": "E_ROUND_NOT_FOUND", "message": "round not found"})
        round_data["sessions"] = await runtime.storage.list_sessions_for_round(round_id)
        return round_data

    @app.get("/api/v1/rounds/{round_id}/trace")
    async def get_round_trace(round_id: str) -> dict[str, Any]:
        round_data = await service().round(round_id)
        if round_data is None:
            raise HTTPException(status_code=404, detail={"code": "E_ROUND_NOT_FOUND", "message": "round not found"})
        return {"round_id": round_id, "trace": round_data.get("trace"), "status": round_data.get("status")}

    @app.get("/api/v1/recovery")
    @app.get("/api/v1/recovery/latest")
    @app.get("/api/v1/recovery/report")
    async def latest_recovery() -> dict[str, Any]:
        report = await service().recovery()
        if report is None:
            raise HTTPException(
                status_code=404,
                detail={"code": "E_RECOVERY_NOT_FOUND", "message": "no recovery report exists"},
            )
        return report

    @app.get("/api/v1/recovery/reports")
    async def recovery_reports(limit: int = Query(default=20, ge=1, le=200)) -> dict[str, Any]:
        reports = await runtime.storage.list_recovery_reports(limit)
        return {"count": len(reports), "reports": reports}

    return app

