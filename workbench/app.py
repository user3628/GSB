from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import urlencode

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from starlette.exceptions import HTTPException as StarletteHTTPException

from .canonical import normalize_path
from .config import WorkbenchConfig
from .engine import CacheEngine, RequestSpec
from .errors import WorkbenchError, wb_error
from .injection import (
    normalize_injection_specs,
    split_reserved_query,
)
from .origin import content_identity
from .policy import CachePolicy, canonical_json
from .sample_loader import load_origin_manifest, seed_origin_if_empty, seed_policy_if_empty
from .store import Store
from .store import ALLOWED_TRANSITIONS


def _json_response(payload: Mapping[str, Any], status_code: int = 200, headers: Mapping[str, str] | None = None) -> JSONResponse:
    return JSONResponse(content=dict(payload), status_code=status_code, headers=dict(headers or {}))


def _query_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value[1:] if value.startswith("?") else value
    if isinstance(value, Mapping):
        pairs: list[tuple[str, str]] = []
        for key, raw in value.items():
            if isinstance(raw, (list, tuple)):
                pairs.extend((str(key), str(item)) for item in raw)
            else:
                pairs.append((str(key), str(raw)))
        return urlencode(pairs, doseq=True)
    if isinstance(value, (list, tuple)):
        pairs = []
        for item in value:
            if not isinstance(item, (list, tuple)) or len(item) != 2:
                raise wb_error("E_KEY_INVALID", "Query array items must be [name, value] pairs")
            pairs.append((str(item[0]), str(item[1])))
        return urlencode(pairs, doseq=True)
    raise wb_error("E_KEY_INVALID", "Query must be a string, object, or pair array")


def _string_headers(value: Any) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise wb_error("E_KEY_INVALID", "headers must be an object")
    return {str(key): str(item) for key, item in value.items()}


def _request_id(value: Any, header_value: str | None = None) -> str:
    candidate = value or header_value
    if candidate is None:
        return f"req-{uuid.uuid4()}"
    candidate = str(candidate).strip()
    if not candidate or len(candidate) > 200:
        raise wb_error("E_INJECT_INVALID", "request_id must be a non-empty string of at most 200 characters")
    return candidate


def _fingerprint(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(dict(payload)).encode("utf-8")).hexdigest()


def _entry_public(entry: Mapping[str, Any]) -> dict[str, Any]:
    result = {key: value for key, value in entry.items() if key != "body"}
    body = bytes(entry.get("body", b""))
    result["body_length"] = len(body)
    result["body_sha256"] = hashlib.sha256(body).hexdigest()
    now = time.time()
    result["age_seconds"] = max(0.0, now - float(entry.get("stored_at", now)))
    result["remaining_ttl_seconds"] = max(0.0, float(entry.get("fresh_until", now)) - now)
    result["stale_remaining_seconds"] = max(0.0, float(entry.get("stale_until", now)) - now)
    return result


def create_app(config: WorkbenchConfig | None = None) -> FastAPI:
    config = config or WorkbenchConfig.load()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        config.ensure_paths()
        store = Store(config.sqlite_path)
        store.initialize()
        for node_id in config.edge_nodes:
            store.ensure_node(node_id)
        seeded_content = seed_origin_if_empty(store, config)
        seeded_policy = seed_policy_if_empty(store, config)
        recovery_report = store.recover_on_startup() if config.auto_recover else None
        engine = CacheEngine(store, config)
        app.state.config = config
        app.state.store = store
        app.state.engine = engine
        app.state.startup = {
            "started_at": time.time(),
            "seeded_content": seeded_content,
            "seeded_policy": seeded_policy,
            "recovery_report": recovery_report,
            "baseline": store.get_state_summary(config.edge_nodes),
            "clean_startup": bool(recovery_report and recovery_report.get("clean_startup")),
        }
        yield

    app = FastAPI(
        title="CDN Edge Cache Consistency Workbench",
        version="0.1.0",
        description="Local-only deterministic origin + edge cache simulator and diagnostics API",
        lifespan=lifespan,
    )

    @app.exception_handler(WorkbenchError)
    async def workbench_error_handler(_request: Request, exc: WorkbenchError):
        return _json_response(exc.to_dict(), exc.http_status)

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_request: Request, exc: RequestValidationError):
        return _json_response(
            {
                "ok": False,
                "business_code": "E_INJECT_INVALID",
                "message": "Request validation failed",
                "details": {"errors": exc.errors()},
            },
            422,
        )

    @app.exception_handler(HTTPException)
    async def http_error_handler(_request: Request, exc: HTTPException):
        code = {404: "E_NOT_FOUND", 405: "E_METHOD_NOT_ALLOWED"}.get(exc.status_code, "E_INTERNAL")
        return _json_response(
            {"ok": False, "business_code": code, "message": str(exc.detail)},
            exc.status_code,
        )

    @app.exception_handler(StarletteHTTPException)
    async def starlette_http_error_handler(_request: Request, exc: StarletteHTTPException):
        code = {
            404: "E_NOT_FOUND",
            405: "E_METHOD_NOT_ALLOWED",
        }.get(exc.status_code, "E_INTERNAL")
        return _json_response(
            {"ok": False, "business_code": code, "message": str(exc.detail)},
            exc.status_code,
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(_request: Request, exc: Exception):
        details = {"error_type": type(exc).__name__}
        if config.debug_errors:
            details["error"] = str(exc)
        return _json_response(
            {"ok": False, "business_code": "E_INTERNAL", "message": "Unexpected workbench error", "details": details},
            500,
        )

    @app.get("/")
    async def root(_request: Request):
        return {
            "ok": True,
            "name": "CDN Edge Cache Consistency Workbench",
            "docs": "/docs",
            "health": "/api/health",
            "scope": "local loopback deterministic samples only",
        }

    @app.get("/api/health")
    async def health(_request: Request):
        return {
            "ok": True,
            "status": "ready",
            "config": config.as_public_dict(),
            "store_writable": _path_writable(config.sqlite_path),
        }

    @app.get("/api/diagnostics/state")
    async def diagnostics_state(_request: Request):
        store: Store = app.state.store
        engine: CacheEngine = app.state.engine
        state = store.get_state_summary(config.edge_nodes)
        state["in_flight_origin_flights"] = len(engine._flights)
        state["in_flight_requests"] = len(engine._request_futures)
        state["background_revalidations"] = engine.background_status()
        state["startup"] = app.state.startup
        state["observed_at"] = time.time()
        return {"ok": True, "state": state}

    @app.get("/api/diagnostics/baseline")
    async def diagnostics_baseline(_request: Request):
        return {"ok": True, "baseline": app.state.startup.get("baseline"), "startup": app.state.startup}

    @app.get("/api/state-machine")
    async def state_machine(_request: Request):
        return {
            "ok": True,
            "states": ["MISS", "REVALIDATING", "FRESH", "STALE", "PURGED"],
            "transitions": {state: sorted(targets) for state, targets in ALLOWED_TRANSITIONS.items()},
            "terminal_states": ["PURGED"],
            "notes": [
                "MISS is represented in request traces before the first entry generation exists.",
                "PURGED is terminal for an entry generation; a later origin fetch creates a new generation.",
                "Illegal transitions return E_INVALID_TRANSITION and do not mutate the entry or indexes.",
            ],
        }

    @app.post("/api/policies")
    @app.post("/api/policies/snapshots")
    async def submit_policy(_request: Request, payload: dict[str, Any]):
        store: Store = app.state.store
        if isinstance(payload.get("snapshot"), Mapping):
            payload = dict(payload["snapshot"])
        policy = CachePolicy.from_snapshot(payload, config.edge_nodes)
        result = store.submit_policy(policy)
        return {"ok": True, "policy": policy.as_dict(), "commit": result}

    @app.get("/api/policies/current")
    async def current_policy(_request: Request):
        store: Store = app.state.store
        policy = store.get_current_policy()
        if policy is None:
            raise wb_error("E_POLICY_MISSING", "No cache policy snapshot has been committed")
        return {"ok": True, "policy": policy.as_dict()}

    @app.get("/api/policies/snapshots")
    async def policy_snapshots(_request: Request):
        return {"ok": True, "snapshots": app.state.store.list_policy_snapshots()}

    async def execute_body_request(node_id: str, payload: dict[str, Any], request: Request) -> tuple[dict[str, Any], int]:
        engine: CacheEngine = app.state.engine
        engine.validate_node(node_id)
        header_request_id = request.headers.get("x-request-id")
        try:
            public_query, reserved = split_reserved_query(request.query_params.multi_items())
        except WorkbenchError:
            raise
        method = str(payload.get("method", "GET")).upper()
        path = str(payload.get("path", "/"))
        query = _query_string(payload.get("query", ""))
        if public_query:
            query = public_query if not query else f"{query}&{public_query}"
        headers = _string_headers(payload.get("headers"))
        injections = payload.get("injections", payload.get("inject"))
        control_request_id = (reserved.get("__sim_request_id") or [None])[-1]
        request_id = _request_id(payload.get("request_id"), header_request_id or control_request_id)
        force = bool(payload.get("force_revalidate", payload.get("revalidate", False)))
        spec = RequestSpec(
            request_id=request_id,
            node_id=node_id,
            method=method,
            path=path,
            query=query,
            headers=headers,
            reserved_controls=reserved,
            injections=injections,
            force_revalidate=force,
            request_payload=payload,
        )
        semantic = {
            "kind": "edge_request",
            "node_id": node_id,
            "method": method,
            "path": path,
            "query": query,
            "headers": headers,
            "injections": injections,
            "force_revalidate": force,
            "reserved_controls": reserved,
        }
        return await engine.submit(spec, fingerprint=_fingerprint(semantic), request_payload=payload)

    @app.post("/api/edges/{node_id}/requests")
    async def edge_request(node_id: str, request: Request, payload: dict[str, Any] | None = None):
        response, status = await execute_body_request(node_id, payload or {}, request)
        return _json_response(response, status)

    @app.post("/api/edges/{node_id}/requests/batch")
    @app.post("/api/edges/{node_id}/batch")
    async def edge_batch(node_id: str, request: Request, payload: dict[str, Any]):
        requests = payload.get("requests")
        if not isinstance(requests, list):
            raise wb_error("E_INJECT_INVALID", "Batch payload requires a 'requests' array")
        tasks = [execute_body_request(node_id, item if isinstance(item, dict) else {}, request) for item in requests]
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        results: list[dict[str, Any]] = []
        for index, outcome in enumerate(outcomes):
            if isinstance(outcome, WorkbenchError):
                item = outcome.to_dict()
                item["batch_index"] = index
                item["http_status"] = outcome.http_status
                results.append(item)
            elif isinstance(outcome, BaseException):
                results.append(
                    {
                        "ok": False,
                        "business_code": "E_INTERNAL",
                        "message": "Batch item failed",
                        "error_type": type(outcome).__name__,
                        "batch_index": index,
                        "http_status": 500,
                    }
                )
            else:
                response, _status = outcome
                response = dict(response)
                response["batch_index"] = index
                results.append(response)
        return {"ok": True, "node_id": node_id, "count": len(results), "results": results}

    @app.post("/api/replay")
    async def replay(request: Request, payload: dict[str, Any]):
        requests = payload.get("requests")
        if not isinstance(requests, list):
            raise wb_error("E_INJECT_INVALID", "Replay payload requires a 'requests' array")
        tasks = []
        indexes: list[int] = []
        for index, item in enumerate(requests):
            if not isinstance(item, dict):
                continue
            node_id = item.get("node_id") or payload.get("node_id")
            if not isinstance(node_id, str) or not node_id:
                raise wb_error("E_INJECT_INVALID", "Every replay item needs a node_id or a top-level node_id")
            tasks.append(execute_body_request(node_id, item, request))
            indexes.append(index)
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        results: list[dict[str, Any]] = []
        for index, outcome in zip(indexes, outcomes):
            if isinstance(outcome, WorkbenchError):
                item = outcome.to_dict()
                item["batch_index"] = index
                item["http_status"] = outcome.http_status
                results.append(item)
            elif isinstance(outcome, BaseException):
                results.append(
                    {
                        "ok": False,
                        "business_code": "E_INTERNAL",
                        "code": "E_INTERNAL",
                        "message": "Replay item failed",
                        "error_type": type(outcome).__name__,
                        "batch_index": index,
                        "http_status": 500,
                    }
                )
            else:
                response, _status = outcome
                response = dict(response)
                response["batch_index"] = index
                results.append(response)
        return {"ok": True, "count": len(results), "results": results}

    @app.get("/edge/{node_id}/{path:path}")
    async def edge_get(node_id: str, path: str, request: Request):
        engine: CacheEngine = app.state.engine
        try:
            public_query, reserved = split_reserved_query(request.query_params.multi_items())
        except WorkbenchError as exc:
            return _json_response(exc.to_dict(), exc.http_status)
        control_request_id = (reserved.get("__sim_request_id") or [None])[-1]
        request_id = _request_id(request.headers.get("x-request-id") or control_request_id, None)
        headers = {key.lower(): value for key, value in request.headers.items()}
        spec = RequestSpec(
            request_id=request_id,
            node_id=node_id,
            method=request.method.upper(),
            path="/" + path,
            query=public_query,
            headers=headers,
            reserved_controls=reserved,
            request_payload={"method": request.method, "path": "/" + path, "query": public_query},
        )
        semantic = {
            "kind": "edge_get",
            "node_id": node_id,
            "method": request.method.upper(),
            "path": "/" + path,
            "query": public_query,
            "reserved_controls": reserved,
        }
        try:
            response, status = await engine.submit(spec, fingerprint=_fingerprint(semantic), request_payload=semantic)
        except WorkbenchError as exc:
            return _json_response(exc.to_dict(), exc.http_status)
        if not response.get("ok"):
            return _json_response(response, status)
        response_headers = {key: str(value) for key, value in response.get("headers", {}).items()}
        response_headers.update(
            {
                "X-Request-Id": request_id,
                "X-Cache-Key": str(response.get("cache_key")),
                "X-Cache-Status": str(response.get("cache_status")),
                "X-Origin-Fetches": str(response.get("origin_fetches")),
                "X-Trace-Id": str(response.get("trace_id")),
            }
        )
        body = str(response.get("body", "")).encode(response.get("body_encoding") if response.get("body_encoding") == "base64" else "utf-8")
        if response.get("body_encoding") == "base64":
            import base64

            body = base64.b64decode(body)
        return Response(
            content=body,
            status_code=int(response.get("http_status", 200)),
            media_type=str(response.get("content_type", "application/octet-stream")),
            headers=response_headers,
        )

    @app.post("/api/purges")
    @app.post("/api/purge")
    async def purge(_request: Request, payload: dict[str, Any]):
        engine: CacheEngine = app.state.engine
        try:
            _, controls = split_reserved_query(_request.query_params.multi_items())
        except WorkbenchError as exc:
            return _json_response(exc.to_dict(), exc.http_status)
        purge_control_request_id = None
        if controls.get("__sim_request_id"):
            purge_control_request_id = controls["__sim_request_id"][-1]
        request_id = _request_id(
            payload.get("request_id"), _request.headers.get("x-request-id") or purge_control_request_id
        )
        injections = payload.get("injections", payload.get("inject"))
        if injections is not None and not isinstance(injections, list):
            injections = [injections] if isinstance(injections, Mapping) else []
        if controls:
            try:
                control_specs = normalize_injection_specs(controls={
                    key.removeprefix("__sim_"): values[-1] for key, values in controls.items()
                })
            except WorkbenchError:
                control_specs = []
            if control_specs:
                injections = (injections or []) + control_specs
        semantic = {
            "kind": "purge",
            "path": payload.get("path"),
            "tag": payload.get("tag"),
            "injections": injections,
            "target_node": payload.get("target_node"),
        }
        response, status = await engine.purge(
            request_id=request_id,
            path=payload.get("path"),
            tag=payload.get("tag"),
            injections=injections,
            request_payload=payload,
            fingerprint=_fingerprint(semantic),
            target_node=payload.get("target_node"),
        )
        return _json_response(response, status)

    @app.get("/api/purges")
    async def list_purges(_request: Request, limit: int = 100):
        return {"ok": True, "purges": app.state.store.list_purges(limit=limit)}

    @app.get("/api/edges/{node_id}/entries")
    async def list_entries(
        node_id: str,
        _request: Request,
        cache_key: str | None = None,
        path: str | None = None,
        include_purged: bool = True,
    ):
        engine: CacheEngine = app.state.engine
        engine.validate_node(node_id)
        canonical = None
        if path is not None:
            canonical = normalize_path(path)
        entries = app.state.store.find_entries(
            node_id=node_id,
            cache_key=cache_key,
            path=canonical,
            include_purged=include_purged,
        )
        return {"ok": True, "node_id": node_id, "entries": [_entry_public(entry) for entry in entries]}

    @app.get("/api/edges/{node_id}/entries/{cache_key}")
    async def get_entry(node_id: str, cache_key: str, _request: Request):
        engine: CacheEngine = app.state.engine
        engine.validate_node(node_id)
        entry = app.state.store.get_entry(node_id, cache_key)
        if entry is None:
            raise wb_error("E_NOT_FOUND", "Cache entry not found", cache_key=cache_key, node_id=node_id)
        return {"ok": True, "entry": _entry_public(entry)}

    @app.get("/api/edges/{node_id}/entries/{cache_key}/events")
    async def entry_state_events(node_id: str, cache_key: str, _request: Request, limit: int = 200):
        engine: CacheEngine = app.state.engine
        engine.validate_node(node_id)
        entry = app.state.store.get_entry(node_id, cache_key)
        if entry is None:
            raise wb_error("E_NOT_FOUND", "Cache entry not found", cache_key=cache_key, node_id=node_id)
        events = app.state.store.list_entry_state_events(node_id=node_id, cache_key=cache_key, limit=limit)
        return {"ok": True, "node_id": node_id, "cache_key": cache_key, "events": events}

    @app.post("/api/edges/{node_id}/entries/{cache_key}/transition")
    async def transition_entry(node_id: str, cache_key: str, _request: Request, payload: dict[str, Any]):
        engine: CacheEngine = app.state.engine
        engine.validate_node(node_id)
        from_state = str(payload.get("from_state", ""))
        to_state = str(payload.get("to_state", ""))
        reason = str(payload.get("reason", "DIAGNOSTIC_REQUEST"))
        updated = app.state.store.transition_entry(
            node_id=node_id,
            cache_key=cache_key,
            from_state=from_state,
            to_state=to_state,
            reason=reason,
        )
        return {"ok": True, "entry": _entry_public(updated), "state_machine": {from_state: sorted(ALLOWED_TRANSITIONS.get(from_state, []))}}

    @app.get("/api/entries/{cache_key}")
    async def get_entry_all_nodes(cache_key: str, _request: Request):
        entries = app.state.store.find_entries(cache_key=cache_key)
        return {"ok": True, "cache_key": cache_key, "entries": [_entry_public(entry) for entry in entries]}

    @app.get("/api/requests/{request_id}")
    async def get_request(request_id: str, _request: Request):
        record = app.state.store.get_request(request_id)
        if record is None:
            raise wb_error("E_NOT_FOUND", "Request record not found", request_id=request_id)
        record["traces"] = app.state.store.list_traces(request_id=request_id)
        return {"ok": True, "request": record}

    @app.get("/api/traces")
    async def traces(_request: Request, request_id: str | None = None, limit: int = 100):
        return {"ok": True, "traces": app.state.store.list_traces(request_id=request_id, limit=limit)}

    @app.get("/api/injections")
    async def injections(_request: Request, request_id: str | None = None, node_id: str | None = None, limit: int = 200):
        return {
            "ok": True,
            "injections": app.state.store.list_injections(request_id=request_id, node_id=node_id, limit=limit),
        }

    @app.get("/api/origin/fetches")
    async def origin_fetches(_request: Request, request_id: str | None = None, limit: int = 200):
        return {"ok": True, "fetches": app.state.store.list_origin_fetches(request_id=request_id, limit=limit)}

    @app.get("/api/background/{task_id}")
    async def background_status(task_id: str, _request: Request, wait: bool = False, timeout: float = 5.0):
        engine: CacheEngine = app.state.engine
        status = await engine.wait_background(task_id, timeout=timeout) if wait else engine.background_status(task_id)
        if status is None:
            raise wb_error("E_NOT_FOUND", "Background task not found", task_id=task_id)
        return {"ok": True, "background": status}

    @app.get("/api/recovery/report")
    @app.get("/api/recovery/reports/latest")
    async def latest_recovery(_request: Request):
        report = app.state.store.latest_recovery_report()
        if report is None:
            raise wb_error("E_NOT_FOUND", "No startup recovery report exists")
        return {"ok": True, "report": report}

    @app.get("/api/recovery/reports")
    async def recovery_reports(_request: Request, limit: int = 50):
        return {"ok": True, "reports": app.state.store.get_recovery_reports(limit=limit)}

    @app.get("/api/origin/content")
    async def origin_content_list(_request: Request):
        return {"ok": True, "items": app.state.store.list_origin_content()}

    @app.post("/api/origin/content")
    async def origin_content_update(_request: Request, payload: dict[str, Any]):
        path = payload.get("path")
        if not isinstance(path, str) or not path.startswith("/"):
            raise wb_error("E_KEY_INVALID", "Origin content path must start with '/'")
        if "body_file" in payload:
            body_path = Path(str(payload["body_file"])).expanduser()
            if not body_path.is_absolute():
                body_path = (config.samples_dir / body_path).resolve()
            if not body_path.exists():
                raise wb_error("E_NOT_FOUND", "Origin body_file not found", body_file=str(body_path))
            body = body_path.read_bytes()
        elif "body_b64" in payload:
            import base64

            try:
                body = base64.b64decode(str(payload["body_b64"]), validate=True)
            except Exception as exc:
                raise wb_error("E_INJECT_INVALID", "body_b64 is not valid base64") from exc
        elif "body" in payload:
            body = str(payload["body"]).encode("utf-8")
        else:
            raise wb_error("E_INJECT_INVALID", "Origin update requires body, body_b64, or body_file")
        tags = payload.get("tags", [])
        if not isinstance(tags, list):
            raise wb_error("E_INJECT_INVALID", "Origin tags must be a list")
        etag, version = content_identity(body)
        result = app.state.store.upsert_origin_content(
            path=path,
            body=body,
            content_type=str(payload.get("content_type", "application/octet-stream")),
            tags=[str(tag) for tag in tags],
            etag=etag,
            version=version,
        )
        return {"ok": True, "item": result}

    @app.post("/api/origin/load-manifest")
    async def origin_load_manifest(_request: Request, payload: dict[str, Any]):
        manifest = payload.get("manifest")
        if not isinstance(manifest, str) or not manifest:
            raise wb_error("E_INJECT_INVALID", "manifest path is required")
        items = load_origin_manifest(app.state.store, config, manifest)
        return {"ok": True, "items": items}

    @app.get("/origin/content")
    async def origin_get(
        _request: Request,
        path: str,
        __delay_ms: int = 0,
        __status: int = 0,
        __pad_bytes: int = 0,
    ):
        try:
            canonical = normalize_path(path)
        except WorkbenchError as exc:
            return _json_response(exc.to_dict(), exc.http_status)
        if __delay_ms < 0 or __delay_ms > 120_000:
            return _json_response(
                {"ok": False, "business_code": "E_INJECT_INVALID", "message": "__delay_ms is out of range"}, 400
            )
        if __status and not 500 <= __status <= 599:
            return _json_response(
                {"ok": False, "business_code": "E_INJECT_INVALID", "message": "__status must be 5xx"}, 400
            )
        if __pad_bytes < 0 or __pad_bytes > config.max_body_pad_bytes:
            return _json_response(
                {"ok": False, "business_code": "E_INJECT_INVALID", "message": "__pad_bytes is out of range"}, 400
            )
        if __delay_ms:
            await asyncio.sleep(__delay_ms / 1000.0)
        if __status:
            return _json_response(
                {
                    "ok": False,
                    "business_code": "E_ORIGIN_5XX",
                    "message": "Injected origin status",
                    "injected_status": __status,
                },
                __status,
                headers={"X-Injected-Status": str(__status)},
            )
        item = app.state.store.get_origin_content(canonical)
        if item is None:
            return _json_response(
                {"ok": False, "business_code": "E_NOT_FOUND", "message": "Origin content not found", "path": canonical},
                404,
            )
        body = bytes(item["body"]) + (b"x" * __pad_bytes)
        etag, version = content_identity(body)
        headers = {
            "ETag": etag,
            "X-Content-Version": version,
            "X-Content-Tags": json.dumps(item["tags"], separators=(",", ":"), ensure_ascii=False),
            "X-Origin-Path": canonical,
            "X-Origin-Injected-Pad-Bytes": str(__pad_bytes),
            "Cache-Control": "no-store",
        }
        return Response(content=body, media_type=item["content_type"], headers=headers)

    return app


def _path_writable(path: Path) -> bool:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        probe = path.parent / f".write-probe-{uuid.uuid4().hex}"
        probe.write_bytes(b"ok")
        probe.unlink()
        return True
    except OSError:
        return False


app = create_app()

