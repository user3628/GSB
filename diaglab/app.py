"""FastAPI application and local-only HTTP surface for the workbench."""

from __future__ import annotations

import base64
import binascii
import hashlib
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from .errors import BusinessError, bad_request
from .formats import (
    JSONL_FORMAT,
    canonical_fingerprint_payload,
    compatibility_matrix,
    decode_bundle,
    encode_binary,
    encode_jsonl,
)
from .models import DiagnosticRecord, ProbeRequest, TIMESTAMP_POLICIES
from .normalize import normalize_bundle
from .probe import validate_loopback_target
from .service_metrics import validate_loopback_url
from .store import WorkbenchStore, make_batch_fingerprint


PACKAGE_DIR = Path(__file__).resolve().parent
WEB_DIR = PACKAGE_DIR / "web"
MAX_UPLOAD_BYTES = 32 * 1024 * 1024


class ContentRequest(BaseModel):
    content_base64: str = Field(min_length=1)
    filename: str | None = None
    batch_id: str | None = None
    timestamp_policy: str = "truncate"


class ProbeBody(BaseModel):
    host: str = "127.0.0.1"
    port: int = Field(ge=1, le=65535)
    path_node: str = Field(min_length=1, max_length=255)
    probe_id: str | None = None
    timeout_ms: int = Field(default=750, ge=50, le=30_000)
    payload_bytes: int = Field(default=32, ge=1, le=65_507)
    protocol: str = "tcp"
    batch_id: str | None = None


class MetricsPollBody(BaseModel):
    admin_url: str
    path_node: str = Field(min_length=1, max_length=255)
    batch_id: str | None = None


class RecomputeBody(BaseModel):
    batch_id: str | None = None


def _decode_content(value: str) -> bytes:
    try:
        data = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise bad_request("content_base64 不是合法 Base64") from exc
    if not data:
        raise bad_request("诊断包内容为空")
    if len(data) > MAX_UPLOAD_BYTES:
        raise bad_request("诊断包超过 32 MiB 上限", actual_bytes=len(data))
    return data


def _auto_batch_id(prefix: str, supplied: str | None) -> str:
    batch_id = supplied or f"{prefix}-{uuid.uuid4().hex[:20]}"
    if len(batch_id) > 128:
        raise bad_request("batch_id 不得超过 128 字符")
    return batch_id


def _import_fingerprint(data: bytes, timestamp_policy: str) -> str:
    payload = canonical_fingerprint_payload(
        data, {"kind": "import", "timestamp_policy": timestamp_policy}
    )
    return hashlib.sha256(payload).hexdigest()


def _import_pipeline(data: bytes, timestamp_policy: str):
    if timestamp_policy not in TIMESTAMP_POLICIES:
        raise bad_request(
            "未知时间戳截断规则",
            timestamp_policy=timestamp_policy,
            supported=sorted(TIMESTAMP_POLICIES),
        )
    bundle = decode_bundle(data)
    normalized = normalize_bundle(bundle, timestamp_policy)
    return bundle, normalized


def create_app(db_path: str | os.PathLike[str] | None = None) -> FastAPI:
    store = WorkbenchStore(
        db_path
        or os.environ.get("DIAGLAB_DB")
        or str(Path.cwd() / "data" / "diaglab.sqlite3")
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        store.initialize()
        app.state.store = store
        yield

    app = FastAPI(
        title="Loopback Margin Lab",
        version="1.0.0",
        description="本地回环丢包与网络故障诊断工作台",
        lifespan=lifespan,
    )
    app.state.store = store

    @app.exception_handler(BusinessError)
    async def business_error_handler(request: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.as_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "code": "E_BAD_REQUEST",
                "message": "请求参数校验失败",
                "details": {"errors": exc.errors()},
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = "E_NOT_FOUND" if exc.status_code == 404 else "E_HTTP_ERROR"
        return JSONResponse(
            status_code=exc.status_code,
            content={"ok": False, "code": code, "message": str(exc.detail), "details": {}},
        )

    @app.exception_handler(Exception)
    async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "code": "E_INTERNAL",
                "message": f"未处理异常：{type(exc).__name__}",
                "details": {},
            },
        )

    app.mount("/static", StaticFiles(directory=str(WEB_DIR)), name="static")

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(WEB_DIR / "index.html")

    @app.get("/health")
    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return store.health()

    @app.get("/api/formats/compatibility")
    def formats_compatibility() -> dict[str, Any]:
        return {"ok": True, **compatibility_matrix()}

    @app.post("/api/preflight")
    def preflight(body: ContentRequest) -> dict[str, Any]:
        data = _decode_content(body.content_base64)
        bundle, normalized = _import_pipeline(data, body.timestamp_policy)
        summary = normalized.report
        return {
            "ok": True,
            "filename": body.filename,
            "content_bytes": len(data),
            "content_sha256": hashlib.sha256(data).hexdigest(),
            "negotiation": bundle.report.as_dict(),
            "format_metadata": bundle.metadata,
            "normalization": summary,
            "preflight_only": True,
            "persisted": False,
        }

    @app.post("/api/imports")
    def import_diagnostic_package(body: ContentRequest) -> JSONResponse:
        data = _decode_content(body.content_base64)
        batch_id = _auto_batch_id("imp", body.batch_id)
        bundle, normalized = _import_pipeline(data, body.timestamp_policy)
        fingerprint = _import_fingerprint(data, body.timestamp_policy)
        result = store.import_batch(
            batch_id=batch_id,
            fingerprint=fingerprint,
            records=normalized.records,
            negotiation=bundle.report,
            normalization_report=normalized.report,
            source={
                "filename": body.filename,
                "content_bytes": len(data),
                "content_sha256": hashlib.sha256(data).hexdigest(),
                "timestamp_policy": body.timestamp_policy,
            },
        )
        return JSONResponse(status_code=result["http_status"], content=result)

    @app.get("/api/imports/{batch_id}")
    def get_import(batch_id: str) -> dict[str, Any]:
        return store.get_batch(batch_id)

    @app.get("/api/negotiations")
    def list_negotiations(limit: int = Query(default=100, ge=1, le=500)) -> dict[str, Any]:
        return store.list_negotiations(limit)

    @app.get("/api/overview")
    def overview() -> dict[str, Any]:
        return store.overview()

    @app.get("/api/paths")
    def paths() -> dict[str, Any]:
        return store.paths()

    @app.get("/api/records")
    def records(
        path_node: str | None = None,
        start_ms: int | None = Query(default=None, ge=0),
        end_ms: int | None = Query(default=None, ge=0),
        limit: int = Query(default=500, ge=1, le=10_000),
    ) -> dict[str, Any]:
        return store.records(
            path_node=path_node, start_ms=start_ms, end_ms=end_ms, limit=limit
        )

    @app.get("/api/stats")
    def stats(
        path_node: str = Query(min_length=1),
        start_ms: int | None = Query(default=None, ge=0),
        end_ms: int | None = Query(default=None, ge=0),
        min_samples: int = Query(default=5, ge=1, le=10_000),
    ) -> dict[str, Any]:
        result = store.stats(
            path_node,
            start_ms=start_ms,
            end_ms=end_ms,
            min_samples=min_samples,
        )
        return {"ok": True, **result}

    @app.get("/api/stats/snapshot")
    def materialized_stats() -> dict[str, Any]:
        return store.stats_snapshot()

    @app.post("/api/stats/recompute")
    def recompute_stats(body: RecomputeBody) -> JSONResponse:
        batch_id = _auto_batch_id("stats", body.batch_id)
        fingerprint = make_batch_fingerprint(
            "stats-recompute", {"batch_id": batch_id, "operation": "recompute"}
        )
        result = store.recompute_stats(batch_id=batch_id, fingerprint=fingerprint)
        return JSONResponse(status_code=result["http_status"], content=result)

    @app.get("/api/root-causes")
    def root_causes(
        start_ms: int | None = Query(default=None, ge=0),
        end_ms: int | None = Query(default=None, ge=0),
        min_samples: int = Query(default=5, ge=1, le=10_000),
        path_node: str | None = None,
    ) -> dict[str, Any]:
        result = store.root_causes(
            start_ms=start_ms,
            end_ms=end_ms,
            min_samples=min_samples,
            path_node=path_node,
        )
        return {"ok": True, **result}

    @app.get("/api/compare")
    def compare(
        path_a: str = Query(min_length=1),
        path_b: str = Query(min_length=1),
        start_ms: int | None = Query(default=None, ge=0),
        end_ms: int | None = Query(default=None, ge=0),
        min_samples: int = Query(default=5, ge=1, le=10_000),
        rel_tol: float = Query(default=1e-9, ge=0.0, le=1.0),
    ) -> dict[str, Any]:
        result = store.compare_paths(
            path_a,
            path_b,
            start_ms=start_ms,
            end_ms=end_ms,
            min_samples=min_samples,
            relative_tolerance=rel_tol,
        )
        return {"ok": True, **result}

    @app.post("/api/probes")
    def run_probe(body: ProbeBody) -> JSONResponse:
        batch_id = _auto_batch_id("probe", body.batch_id)
        probe_id = body.probe_id or f"probe-{batch_id}"
        request = ProbeRequest(
            host=body.host,
            port=body.port,
            path_node=body.path_node,
            probe_id=probe_id,
            timeout_ms=body.timeout_ms,
            payload_bytes=body.payload_bytes,
            protocol=body.protocol,
        )
        validate_loopback_target(request)
        fingerprint = make_batch_fingerprint(
            "probe",
            {
                "host": request.host,
                "port": request.port,
                "path_node": request.path_node,
                "probe_id": request.probe_id,
                "timeout_ms": request.timeout_ms,
                "payload_bytes": request.payload_bytes,
                "protocol": request.protocol,
            },
        )
        result = store.probe_batch(
            batch_id=batch_id, fingerprint=fingerprint, request=request
        )
        return JSONResponse(status_code=result["http_status"], content=result)

    @app.post("/api/service-metrics/poll")
    def poll_service_metrics(body: MetricsPollBody) -> JSONResponse:
        batch_id = _auto_batch_id("metrics", body.batch_id)
        safe_url = validate_loopback_url(body.admin_url)
        fingerprint = make_batch_fingerprint(
            "service-metrics",
            {"path_node": body.path_node, "admin_url": safe_url},
        )
        result = store.service_metrics_batch(
            batch_id=batch_id,
            fingerprint=fingerprint,
            path_node=body.path_node,
            admin_url=safe_url,
        )
        return JSONResponse(status_code=result["http_status"], content=result)

    @app.get("/api/service-metrics")
    def service_metrics(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
        return store.latest_service_metrics(limit)

    @app.get("/api/exports/{fmt}")
    def export_records(
        fmt: Literal["binary", "jsonl"],
        path_node: str | None = None,
        start_ms: int | None = Query(default=None, ge=0),
        end_ms: int | None = Query(default=None, ge=0),
        endian: Literal["little", "big"] = "little",
        version: int = Query(default=2, ge=1, le=2),
    ) -> Response:
        snapshot_version, records = store.export_records(
            path_node=path_node, start_ms=start_ms, end_ms=end_ms
        )
        if fmt == "binary":
            content = encode_binary(records, version=version, endian=endian)
            media_type = "application/octet-stream"
            extension = "bin"
        else:
            content = encode_jsonl(records, version=version)
            media_type = "application/x-ndjson; charset=utf-8"
            extension = "jsonl"
        headers = {
            "Content-Disposition": f'attachment; filename="diaglab-v{snapshot_version}.{extension}"',
            "X-Diaglab-Snapshot-Version": str(snapshot_version),
            "X-Diaglab-Record-Count": str(len(records)),
            "X-Diaglab-Format": fmt,
        }
        return Response(content=content, media_type=media_type, headers=headers)

    @app.get("/api/recovery/report")
    def recovery_report() -> dict[str, Any]:
        return store.recovery_report()

    return app


app = create_app()
