from __future__ import annotations

import base64
import binascii
import hashlib
import os
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Annotated, Any, Literal

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .db import Database, canonical_json
from .errors import (
    E_FORMAT_CORRUPT,
    E_IMPORT_REJECTED,
    E_VALIDATION,
    BusinessError,
)
from .formats import COMPATIBILITY_MATRIX, decode_snapshot, encode_snapshot
from .importer import ImportInput, SnapshotImporter
from .models import (
    NormalizedRegistration,
    ProbeResultRecord,
    RegistrationRecord,
    Snapshot,
)
from .normalization import normalize_registrations
from .policies import TimePolicy, WeightPolicy
from .recovery import RecoveryManager
from .scheduler import ProbeScheduler
from .service import RegistryService


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RegisterBody(StrictModel):
    service_name: str = Field(min_length=1, max_length=255)
    instance_id: str = Field(min_length=1, max_length=255)
    endpoint: str
    weight: int | float = 500
    weight_encoding: Literal["permille", "ratio"] = "permille"
    weight_policy: dict[str, Any] | None = None
    lease_ttl_ms: int = Field(default=10_000, gt=0, le=86_400_000)
    probe_method: Literal["http", "tcp", "lease"] = "http"
    probe_params: dict[str, Any] | None = None
    probe_window_ms: int | None = Field(default=None, gt=0, le=600_000)


class RenewBody(StrictModel):
    expected_lease_version: int = Field(gt=0)


class EncodedInput(StrictModel):
    batch_id: str | None = None
    format: Literal["auto", "binary", "jsonl"] = "auto"
    data_base64: str | None = None
    shards_base64: list[str] | None = None
    expected_kind: Literal["registration", "probe_result"] | None = None
    weight_policy: dict[str, Any] | None = None
    time_policy: dict[str, Any] | None = None

    @model_validator(mode="after")
    def exactly_one_payload(self) -> "EncodedInput":
        if self.data_base64 is None and self.shards_base64 is None:
            raise ValueError("one of data_base64 or shards_base64 is required")
        if self.data_base64 is not None and self.shards_base64 is not None:
            raise ValueError("data_base64 and shards_base64 are mutually exclusive")
        return self


class ImportBody(EncodedInput):
    batch_id: str = Field(min_length=1, max_length=256)


class ConvertBody(EncodedInput):
    target_format: Literal["binary", "jsonl"]
    target_version: Literal[1, 2] = 2
    endianness: Literal["little", "big"] = "little"
    shards: int = Field(default=1, ge=1, le=1024)


class ProbeTarget(StrictModel):
    service_name: str
    instance_id: str


class ProbeRoundBody(StrictModel):
    instances: list[ProbeTarget] | None = None
    budget_ms: int = Field(default=5000, gt=0, le=600_000)


def _decode_b64(value: str, *, field: str) -> bytes:
    try:
        return base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise BusinessError(
            E_FORMAT_CORRUPT,
            f"{field} is not valid base64",
            details={"field": field},
        ) from exc


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _encoded_payload(body: EncodedInput) -> bytes | list[bytes]:
    if body.shards_base64 is not None:
        if body.format == "binary":
            raise BusinessError(
                E_FORMAT_CORRUPT,
                "binary input must use data_base64, not shards_base64",
                details={"format": body.format},
            )
        return [_decode_b64(value, field="shards_base64") for value in body.shards_base64]
    assert body.data_base64 is not None
    return _decode_b64(body.data_base64, field="data_base64")


def _policy(body: EncodedInput) -> tuple[WeightPolicy, TimePolicy]:
    return WeightPolicy.from_dict(body.weight_policy), TimePolicy.from_dict(body.time_policy)


def _registration_snapshot(service: RegistryService) -> Snapshot:
    records: list[RegistrationRecord] = []
    for row in service.db.list_instances():
        records.append(
            RegistrationRecord(
                service_name=row.service_name,
                instance_id=row.instance_id,
                endpoint=row.endpoint,
                weight=row.weight_permille,
                weight_encoding="permille",
                lease_ttl_ms=row.lease_ttl_ms,
                probe_method=row.probe_method,
                probe_params=row.probe_params,
                registered_at=row.last_registered_at_ms,
                time_unit="ms",
                lease_version=row.lease_version,
                probe_window_ms=row.probe_window_ms,
                last_heartbeat_at=row.last_heartbeat_at_ms,
                source_version=2,
            )
        )
    return Snapshot(kind="registration", version=2, records=tuple(records), source_format="database")


def _probe_snapshot(scheduler: ProbeScheduler) -> Snapshot:
    records: list[ProbeResultRecord] = []
    for row in reversed(scheduler.history(limit=1000)):
        records.append(
            ProbeResultRecord(
                service_name=row["service_name"],
                instance_id=row["instance_id"],
                endpoint=row["detail"].get("endpoint", "http://127.0.0.1:1"),
                weight=row["weight_after"],
                weight_encoding="permille",
                probe_method=row["probe_method"],
                probe_params=row["detail"].get("probe_params", {}),
                checked_at=row["checked_at_ms"],
                time_unit="ms",
                success=row["success"],
                status=row["status"],
                latency_ms=row["latency_ms"],
                outcome=row["outcome"],
                lease_version=row["lease_version"],
                probe_window_ms=row["detail"].get("probe_window_ms"),
                error_code=row["error_code"],
                detail=canonical_json(row["detail"]),
                source_version=2,
            )
        )
    return Snapshot(kind="probe_result", version=2, records=tuple(records), source_format="database")


def _snapshot_hash(snapshot: Snapshot) -> str:
    material = canonical_json(
        {
            "kind": snapshot.kind,
            "records": [
                record.to_dict() if hasattr(record, "to_dict") else record.__dict__
                for record in snapshot.records
            ],
        }
    ).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def _records_from_normalized_registrations(
    records: list[NormalizedRegistration],
) -> list[RegistrationRecord]:
    return [
        RegistrationRecord(
            service_name=record.service_name,
            instance_id=record.instance_id,
            endpoint=record.endpoint,
            weight=record.weight_permille,
            weight_encoding="permille",
            lease_ttl_ms=record.lease_ttl_ms,
            probe_method=record.probe_method,
            probe_params=record.probe_params,
            registered_at=record.registered_at_ms,
            time_unit="ms",
            lease_version=record.lease_version,
            probe_window_ms=record.probe_window_ms,
            last_heartbeat_at=record.last_heartbeat_at_ms,
            source_version=2,
        )
        for record in records
    ]


def _snapshot_for_version(snapshot: Snapshot, version: int) -> Snapshot:
    if version == 2:
        return snapshot
    if snapshot.kind == "registration":
        records = tuple(
            replace(
                record,
                weight=int(record.weight) / 1000,
                weight_encoding="ratio",
                source_version=1,
            )
            for record in snapshot.records
            if isinstance(record, RegistrationRecord)
        )
    else:
        records = tuple(
            replace(
                record,
                weight=int(record.weight) / 1000,
                weight_encoding="ratio",
                source_version=1,
            )
            for record in snapshot.records
            if isinstance(record, ProbeResultRecord)
        )
    return Snapshot(
        kind=snapshot.kind,
        version=version,
        records=records,
        source_format=snapshot.source_format,
    )


def create_app(
    *,
    db_path: str | None = None,
    max_concurrency: int | None = None,
    queue_size: int | None = None,
) -> FastAPI:
    database_path = db_path or os.environ.get("REGISTRY_DB_PATH", "data/registry.db")
    concurrency = max_concurrency or int(os.environ.get("REGISTRY_MAX_CONCURRENCY", "4"))
    bounded_queue = queue_size or int(os.environ.get("REGISTRY_QUEUE_SIZE", "8"))
    failure_step = int(os.environ.get("REGISTRY_FAILURE_WEIGHT_STEP", "50"))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        database = Database(database_path)
        database.initialize()
        recovery = RecoveryManager(database)
        recovery_report = recovery.run()
        service = RegistryService(database)
        importer = SnapshotImporter(database)
        scheduler = ProbeScheduler(
            database,
            max_concurrency=concurrency,
            queue_size=bounded_queue,
            failure_weight_step_permille=failure_step,
        )
        scheduler.start()
        app.state.database = database
        app.state.recovery = recovery
        app.state.recovery_report = recovery_report
        app.state.service = service
        app.state.importer = importer
        app.state.scheduler = scheduler
        try:
            yield
        finally:
            await scheduler.stop()

    app = FastAPI(
        title="Local Service Registry",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.exception_handler(BusinessError)
    async def business_error_handler(request: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        error = BusinessError(
            E_VALIDATION,
            "request validation failed",
            details={"errors": _json_safe(exc.errors())},
            http_status=422,
        )
        return JSONResponse(status_code=422, content=error.to_dict())

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {
            "ok": True,
            "service": "local-service-registry",
            "version": "1.0.0",
            "recovery_startup": app.state.recovery_report,
        }

    @app.get("/api/v1/compatibility")
    async def compatibility() -> dict[str, Any]:
        return {"ok": True, "matrix": COMPATIBILITY_MATRIX}

    @app.post("/api/v1/negotiate")
    async def negotiate(body: EncodedInput) -> dict[str, Any]:
        weight_policy, time_policy = _policy(body)
        return app.state.importer.negotiate(
            _encoded_payload(body),
            expected_format=body.format,
            expected_kind=body.expected_kind,
            weight_policy=weight_policy,
            time_policy=time_policy,
        )

    @app.post("/api/v1/import")
    async def import_snapshot(body: ImportBody) -> dict[str, Any]:
        weight_policy, time_policy = _policy(body)
        report = await app.state.importer.import_snapshot(
            ImportInput(
                batch_id=body.batch_id,
                data=_encoded_payload(body),
                expected_format=body.format,
                expected_kind=body.expected_kind,
                weight_policy=weight_policy,
                time_policy=time_policy,
            )
        )
        if report.get("status") == "rejected":
            raise BusinessError(
                E_IMPORT_REJECTED,
                "one or more import records were rejected; entire batch rolled back",
                details={"report": report},
                http_status=409,
            )
        return report

    @app.get("/api/v1/import/batches/{batch_id}")
    async def import_batch(batch_id: str) -> dict[str, Any]:
        return {"ok": True, "batch": app.state.importer.get_batch(batch_id)}

    @app.post("/api/v1/convert")
    async def convert(body: ConvertBody) -> dict[str, Any]:
        weight_policy, time_policy = _policy(body)
        snapshot, info, negotiation = decode_snapshot(
            _encoded_payload(body),
            expected_format=body.format,
            expected_kind=body.expected_kind,
        )
        if snapshot.kind == "registration":
            normalized = normalize_registrations(
                list(snapshot.records),  # type: ignore[arg-type]
                weight_policy=weight_policy,
                time_policy=time_policy,
            )
            output_snapshot = Snapshot(
                kind="registration",
                version=body.target_version,
                records=tuple(_records_from_normalized_registrations(normalized.records)),  # type: ignore[arg-type]
                source_format="conversion",
            )
        else:
            from .normalization import normalize_probe_results

            normalized = normalize_probe_results(
                list(snapshot.records),  # type: ignore[arg-type]
                weight_policy=weight_policy,
                time_policy=time_policy,
            )
            output_records: list[ProbeResultRecord] = []
            for record in normalized.records:
                output_records.append(
                    ProbeResultRecord(
                        service_name=record.service_name,
                        instance_id=record.instance_id,
                        endpoint=record.endpoint,
                        weight=record.weight_permille,
                        weight_encoding="permille",
                        probe_method=record.probe_method,
                        probe_params=record.probe_params,
                        checked_at=record.checked_at_ms,
                        time_unit="ms",
                        success=record.success,
                        status=record.status,
                        latency_ms=record.latency_ms,
                        outcome=record.outcome,
                        lease_version=record.lease_version,
                        probe_window_ms=record.probe_window_ms,
                        error_code=record.error_code,
                        detail=record.detail,
                        source_version=body.target_version,
                    )
                )
            output_snapshot = Snapshot(
                kind="probe_result",
                version=body.target_version,
                records=tuple(output_records),
                source_format="conversion",
            )
        versioned_snapshot = _snapshot_for_version(output_snapshot, body.target_version)
        encoded = encode_snapshot(
            versioned_snapshot,
            target_format=body.target_format,
            version=body.target_version,
            endianness=body.endianness,
            shards=body.shards,
        )
        shards = encoded if isinstance(encoded, list) else [encoded]
        return {
            "ok": True,
            "source": info.to_dict(),
            "negotiation": negotiation,
            "target": {
                "format": body.target_format,
                "version": body.target_version,
                "endianness": body.endianness if body.target_format == "binary" else None,
                "shards": len(shards),
            },
            "data_base64": base64.b64encode(shards[0]).decode("ascii") if len(shards) == 1 else None,
            "shards_base64": [base64.b64encode(shard).decode("ascii") for shard in shards],
            "semantic_hash": _snapshot_hash(versioned_snapshot),
            "degradation_details": normalized.details()["degradation_details"],
            "weight_rounding": normalized.details()["weight_rounding"],
            "time_truncation": normalized.details()["time_truncation"],
        }

    @app.get("/api/v1/export")
    async def export_snapshot(
        format: Annotated[Literal["binary", "jsonl"], Query()] = "binary",
        kind: Annotated[Literal["registration", "probe_result"], Query()] = "registration",
        version: Annotated[int, Query(ge=1, le=2)] = 2,
        endianness: Annotated[Literal["little", "big"], Query()] = "little",
        shards: Annotated[int, Query(ge=1, le=1024)] = 1,
    ) -> dict[str, Any]:
        snapshot = (
            _registration_snapshot(app.state.service)
            if kind == "registration"
            else _probe_snapshot(app.state.scheduler)
        )
        versioned_snapshot = _snapshot_for_version(snapshot, version)
        encoded = encode_snapshot(
            versioned_snapshot,
            target_format=format,
            version=version,
            endianness=endianness,
            shards=shards,
        )
        shard_bytes = encoded if isinstance(encoded, list) else [encoded]
        return {
            "ok": True,
            "kind": kind,
            "format": format,
            "version": version,
            "endianness": endianness if format == "binary" else None,
            "record_count": len(versioned_snapshot.records),
            "data_base64": base64.b64encode(shard_bytes[0]).decode("ascii") if len(shard_bytes) == 1 else None,
            "shards_base64": [base64.b64encode(shard).decode("ascii") for shard in shard_bytes],
            "semantic_hash": _snapshot_hash(versioned_snapshot),
        }

    @app.post("/api/v1/register")
    async def register(body: RegisterBody) -> dict[str, Any]:
        policy = WeightPolicy.from_dict(body.weight_policy)
        normalized = normalize_registrations(
            [
                RegistrationRecord(
                    service_name=body.service_name,
                    instance_id=body.instance_id,
                    endpoint=body.endpoint,
                    weight=body.weight,
                    weight_encoding=body.weight_encoding,
                    lease_ttl_ms=body.lease_ttl_ms,
                    probe_method=body.probe_method,
                    probe_params=body.probe_params,
                    registered_at=0,
                    time_unit="ms",
                    probe_window_ms=body.probe_window_ms,
                    source_version=2,
                )
            ],
            weight_policy=policy,
            time_policy=TimePolicy(declared=True),
        )
        return await app.state.service.register(normalized.records[0])

    @app.get("/api/v1/instances")
    async def instances(service_name: str | None = None) -> dict[str, Any]:
        return {"ok": True, "instances": app.state.service.list_instances(service_name)}

    @app.get("/api/v1/instances/{service_name}/{instance_id}")
    async def instance(service_name: str, instance_id: str) -> dict[str, Any]:
        return {"ok": True, "instance": app.state.service.get_instance(service_name, instance_id)}

    @app.post("/api/v1/instances/{service_name}/{instance_id}/renew")
    async def renew(service_name: str, instance_id: str, body: RenewBody) -> dict[str, Any]:
        result = await app.state.service.renew_lease(
            service_name,
            instance_id,
            body.expected_lease_version,
        )
        return {
            "ok": True,
            "reused_concurrent_result": result.reused_concurrent_result,
            "instance": result.instance,
        }

    @app.post("/api/v1/probes/run")
    async def run_probes(body: ProbeRoundBody) -> dict[str, Any]:
        keys = (
            [(target.service_name, target.instance_id) for target in body.instances]
            if body.instances is not None
            else None
        )
        return await app.state.scheduler.run_round(keys, budget_ms=body.budget_ms)

    @app.get("/api/v1/probes/history")
    async def probe_history(
        service_name: str | None = None,
        instance_id: str | None = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ) -> dict[str, Any]:
        return {
            "ok": True,
            "history": app.state.scheduler.history(
                service_name=service_name,
                instance_id=instance_id,
                limit=limit,
            ),
        }

    @app.get("/api/v1/recovery/latest")
    async def recovery_latest() -> dict[str, Any]:
        report = app.state.recovery.latest()
        return {"ok": True, "report": report}

    @app.post("/api/v1/recovery/run")
    async def recovery_run() -> dict[str, Any]:
        report = app.state.recovery.run()
        app.state.recovery_report = report
        return report

    @app.get("/api/v1/intents")
    async def intents(status: str | None = None) -> dict[str, Any]:
        return {"ok": True, "intents": app.state.service.intent_log(status=status)}

    return app


app = create_app()
