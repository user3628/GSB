from __future__ import annotations

import base64
import binascii
import json
import logging
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .codecs import (
    FORMAT_BINARY,
    compatibility_matrix,
    detect_and_negotiate,
)
from .config import DEFAULT_DB_PATH, SAMPLES_DIR
from .errors import (
    E_REQUEST_INVALID,
    E_SNAPSHOT_INVALID,
    BusinessError,
)
from .models import Policy, PolicySnapshot
from .store import SnapshotStore


LOGGER = logging.getLogger("qos_workbench.api")


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PolicyInput(StrictModel):
    queue_id: str
    cir_bps: int = Field(ge=0)
    pir_bps: int = Field(ge=0)
    cbs_bytes: int = Field(gt=0)
    pbs_bytes: int = Field(gt=0)
    priority: int = Field(ge=0, le=7)
    match_rule: str
    parent_queue_id: str | None = None
    timestamp_us: int = Field(ge=0)

    def to_policy(self) -> Policy:
        return Policy(**self.model_dump())


class SnapshotLoadRequest(StrictModel):
    format_version: int = Field(ge=1)
    snapshot_no: int = Field(ge=1)
    generated_at_us: int = Field(ge=0)
    policies: list[PolicyInput] = Field(min_length=1)


class ImportRequest(StrictModel):
    format: Literal["auto", "binary", "jsonl"] = "auto"
    content: str | None = None
    content_base64: str | None = None
    filename: str | None = None
    timestamp_truncation: str | None = None
    snapshot_no_override: int | None = Field(default=None, ge=1)
    generated_at_us_override: int | None = Field(default=None, ge=0)
    apply_snapshot: bool = True

    @model_validator(mode="after")
    def exactly_one_content(self) -> "ImportRequest":
        if (self.content is None) == (self.content_base64 is None):
            raise ValueError("provide exactly one of content or content_base64")
        return self


class NegotiateRequest(StrictModel):
    format: Literal["auto", "binary", "jsonl"] = "auto"
    content: str | None = None
    content_base64: str | None = None

    @model_validator(mode="after")
    def exactly_one_content(self) -> "NegotiateRequest":
        if (self.content is None) == (self.content_base64 is None):
            raise ValueError("provide exactly one of content or content_base64")
        return self


class ExportRequest(StrictModel):
    format: Literal["binary", "jsonl"]
    version: int = Field(default=3, ge=1)
    endianness: Literal["little", "big"] = "little"
    response_encoding: Literal["base64", "text"] = "base64"


class TrafficSampleInput(StrictModel):
    timestamp_ms: Decimal | None = None
    timestamp_us: int | None = None
    queue_id: str
    packet_bytes: int | None = Field(default=None, gt=0)
    length_bytes: int | None = Field(default=None, gt=0)
    packet_length: int | None = Field(default=None, gt=0)
    color: str = "green"

    @model_validator(mode="after")
    def require_time_and_length(self) -> "TrafficSampleInput":
        if self.timestamp_ms is None and self.timestamp_us is None:
            raise ValueError("timestamp_ms or timestamp_us is required")
        if not any(value is not None for value in (self.packet_bytes, self.length_bytes, self.packet_length)):
            raise ValueError("packet_bytes, length_bytes or packet_length is required")
        return self

    def to_mapping(self) -> dict[str, Any]:
        result = self.model_dump(exclude_none=True, mode="python")
        return result


class SimulationRequest(StrictModel):
    samples: list[TrafficSampleInput] | None = None
    sample_name: str | None = None
    relative_tolerance: str = "0.10"

    @model_validator(mode="after")
    def exactly_one_source(self) -> "SimulationRequest":
        if (self.samples is None) == (self.sample_name is None):
            raise ValueError("provide exactly one of samples or sample_name")
        return self


def _decode_request_content(*, content: str | None, content_base64: str | None) -> bytes:
    if content is not None:
        return content.encode("utf-8")
    try:
        return base64.b64decode(content_base64 or "", validate=True)
    except (binascii.Error, ValueError) as exc:
        raise BusinessError(
            E_REQUEST_INVALID,
            "content_base64 is not valid strict base64",
            {},
        ) from exc


def _safe_sample_path(sample_name: str) -> Path:
    candidate = (SAMPLES_DIR / sample_name).resolve()
    if candidate.parent != SAMPLES_DIR.resolve() or candidate.suffix.lower() not in {".jsonl", ".json"}:
        raise BusinessError(
            E_REQUEST_INVALID,
            "sample_name must name a .json or .jsonl file directly under samples/",
            {"sample_name": sample_name},
        )
    if not candidate.is_file():
        raise BusinessError(
            "E_SAMPLE_NOT_FOUND",
            "local deterministic sample file does not exist",
            {"sample_name": sample_name},
            http_status=404,
        )
    return candidate


def load_sample_traffic(sample_name: str) -> list[dict[str, Any]]:
    path = _safe_sample_path(sample_name)
    try:
        if path.suffix.lower() == ".json":
            value = json.loads(path.read_text(encoding="utf-8"), parse_float=Decimal)
            if isinstance(value, dict) and "samples" in value:
                value = value["samples"]
            if not isinstance(value, list):
                raise ValueError("JSON sample must be an array or an object with samples")
            return value
        records = []
        for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if line.strip():
                records.append(json.loads(line, parse_float=Decimal))
        return records
    except (json.JSONDecodeError, ValueError) as exc:
        raise BusinessError(
            "E_SAMPLE_INVALID",
            "local sample file is not valid traffic JSONL",
            {"sample_name": sample_name, "message": str(exc)},
        ) from exc


def _snapshot_to_dict(snapshot: PolicySnapshot) -> dict[str, Any]:
    payload = snapshot.canonical_payload()
    payload["checksum"] = snapshot.checksum
    return payload


def create_app(
    db_path: str | Path | None = None,
    *,
    recover_on_start: bool = True,
) -> FastAPI:
    app = FastAPI(
        title="QoS Policy Shaping and Diagnostics Workbench",
        version="1.0.0",
        description="Local-loopback-only QoS policy import/export, snapshot and simulation API.",
    )
    store = SnapshotStore(db_path or DEFAULT_DB_PATH, recover_on_start=recover_on_start)
    app.state.store = store

    @app.exception_handler(BusinessError)
    async def business_error_handler(_request: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.as_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        _request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "error": {
                    "code": E_REQUEST_INVALID,
                    "message": "request validation failed",
                    "details": {"errors": jsonable_encoder(exc.errors())},
                },
            },
        )

    @app.exception_handler(HTTPException)
    async def http_error_handler(_request: Request, exc: HTTPException) -> JSONResponse:
        code = "E_HTTP_ERROR" if exc.status_code >= 400 else "E_HTTP"
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "ok": False,
                "error": {"code": code, "message": str(exc.detail), "details": {}},
            },
        )

    @app.exception_handler(Exception)
    async def unhandled_error_handler(_request: Request, exc: Exception) -> JSONResponse:
        LOGGER.exception("unhandled API error", exc_info=exc)
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "error": {
                    "code": "E_INTERNAL",
                    "message": "internal server error",
                    "details": {"error_type": type(exc).__name__},
                },
            },
        )

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        try:
            seq, snapshot = store.current_snapshot()
            snapshot_info = {
                "snapshot_seq": seq,
                "snapshot_no": snapshot.snapshot_no,
                "checksum": snapshot.checksum,
                "policy_count": len(snapshot.policies),
            }
        except BusinessError:
            snapshot_info = None
        return {
            "ok": True,
            "service": "qos-workbench",
            "database": str(store.db_path),
            "active_snapshot": snapshot_info,
            "recovery_performed": store.recovery_report.get("recovery_performed", False),
        }

    @app.get("/api/formats/compatibility")
    def formats_compatibility() -> dict[str, Any]:
        return {"ok": True, **compatibility_matrix()}

    @app.post("/api/formats/negotiate")
    def formats_negotiate(body: NegotiateRequest) -> dict[str, Any]:
        content = _decode_request_content(
            content=body.content, content_base64=body.content_base64
        )
        return detect_and_negotiate(content, body.format)

    @app.post("/api/policies/import")
    def policies_import(body: ImportRequest) -> dict[str, Any]:
        content = _decode_request_content(
            content=body.content, content_base64=body.content_base64
        )
        return store.import_policy_content(
            content,
            format_name=body.format,
            timestamp_truncation=body.timestamp_truncation,
            snapshot_no_override=body.snapshot_no_override,
            generated_at_us_override=body.generated_at_us_override,
            commit=body.apply_snapshot,
        )

    @app.post("/api/policies/validate")
    def policies_validate(body: ImportRequest) -> dict[str, Any]:
        content = _decode_request_content(
            content=body.content, content_base64=body.content_base64
        )
        return store.import_policy_content(
            content,
            format_name=body.format,
            timestamp_truncation=body.timestamp_truncation,
            snapshot_no_override=body.snapshot_no_override,
            generated_at_us_override=body.generated_at_us_override,
            commit=False,
        )

    @app.get("/api/policies/export")
    def policies_export_get(
        format: Literal["binary", "jsonl"] = Query(...),
        version: int = Query(default=3, ge=1),
        endianness: Literal["little", "big"] = Query(default="little"),
    ) -> Response:
        seq, snapshot, content = store.export_current(
            format_name=format, version=version, endianness=endianness
        )
        media_type = "application/octet-stream" if format == FORMAT_BINARY else "application/x-ndjson"
        return Response(
            content=content,
            media_type=media_type,
            headers={
                "X-QoS-Snapshot-Seq": str(seq),
                "X-QoS-Snapshot-No": str(snapshot.snapshot_no),
                "X-QoS-Snapshot-Checksum": snapshot.checksum,
            },
        )

    @app.post("/api/policies/export")
    def policies_export_post(body: ExportRequest) -> dict[str, Any]:
        seq, snapshot, content = store.export_current(
            format_name=body.format, version=body.version, endianness=body.endianness
        )
        response: dict[str, Any] = {
            "ok": True,
            "format": body.format,
            "version": body.version,
            "snapshot_seq": seq,
            "snapshot_no": snapshot.snapshot_no,
            "checksum": snapshot.checksum,
            "size_bytes": len(content),
            "content_base64": base64.b64encode(content).decode("ascii"),
        }
        if body.response_encoding == "text":
            try:
                response["content"] = content.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise BusinessError(
                    E_REQUEST_INVALID,
                    "binary export cannot be returned as text; use base64",
                    {},
                ) from exc
        return response

    @app.get("/api/snapshots/current")
    def snapshots_current() -> dict[str, Any]:
        seq, snapshot = store.current_snapshot()
        return {
            "ok": True,
            "snapshot_seq": seq,
            "snapshot": _snapshot_to_dict(snapshot),
        }

    @app.get("/api/snapshots")
    def snapshots_list(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
        return {"ok": True, "snapshots": store.list_snapshots(limit)}

    @app.get("/api/snapshots/{seq}")
    def snapshots_get(seq: int) -> dict[str, Any]:
        snapshot = store.get_snapshot_by_seq(seq)
        return {"ok": True, "snapshot_seq": seq, "snapshot": _snapshot_to_dict(snapshot)}

    @app.post("/api/snapshots/load")
    def snapshots_load(body: SnapshotLoadRequest) -> dict[str, Any]:
        snapshot = PolicySnapshot(
            format_version=body.format_version,
            snapshot_no=body.snapshot_no,
            generated_at_us=body.generated_at_us,
            policies=tuple(policy.to_policy() for policy in body.policies),
        )
        try:
            result = store.commit_snapshot(snapshot)
        except BusinessError as exc:
            if exc.code == E_SNAPSHOT_INVALID:
                raise
            raise BusinessError(
                E_SNAPSHOT_INVALID,
                "snapshot load was rejected; the previous complete snapshot remains active",
                {"cause_code": exc.code, "cause_message": exc.message, "cause_details": exc.details},
                http_status=409,
            ) from exc
        return {"ok": True, **result.as_dict()}

    @app.get("/api/recovery/report")
    def recovery_report() -> dict[str, Any]:
        return {"ok": True, **store.recovery_report}

    @app.post("/api/simulations")
    def simulations_run(body: SimulationRequest) -> dict[str, Any]:
        if body.sample_name is not None:
            samples = load_sample_traffic(body.sample_name)
        else:
            samples = [sample.to_mapping() for sample in body.samples or []]
        return store.run_simulation(
            samples,
            relative_tolerance=body.relative_tolerance,
        )

    @app.post("/api/simulations/from-sample/{sample_name}")
    def simulations_from_sample(
        sample_name: str,
        relative_tolerance: str = Query(default="0.10"),
    ) -> dict[str, Any]:
        samples = load_sample_traffic(sample_name)
        return store.run_simulation(
            samples,
            relative_tolerance=relative_tolerance,
        )

    @app.get("/api/simulations/{batch_id}")
    def simulations_get(batch_id: str) -> dict[str, Any]:
        return {"ok": True, "batch": store.get_simulation(batch_id)}

    @app.get("/api/diagnostics/latest")
    def diagnostics_latest() -> dict[str, Any]:
        batch = store.latest_simulation()
        if batch["status"] != "committed":
            raise BusinessError(
                "E_BATCH_NOT_READY",
                "latest simulation batch has no committed diagnostics",
                {"batch_id": batch["batch_id"], "status": batch["status"]},
                http_status=409,
            )
        return {
            "ok": True,
            "batch_id": batch["batch_id"],
            "snapshot_seq": batch["snapshot_seq"],
            "diagnostics": batch["diagnostics_json"],
        }

    return app


app = create_app()

