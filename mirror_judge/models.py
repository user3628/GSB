from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

from .errors import BusinessError


RECORD_FIELDS = {
    "timestamp_ms",
    "connection_id",
    "source_service",
    "destination_service",
    "method",
    "path_template",
    "status_code",
    "response_bytes",
    "duration_us",
}


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _required_string(raw: dict[str, Any], key: str, line_no: int, *, max_len: int = 512) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip() or len(value) > max_len:
        raise BusinessError(
            "E_RECORD_INVALID",
            f"Record line {line_no} field {key!r} must be a non-empty string",
            {"line_no": line_no, "field": key, "value": value},
        )
    return value.strip()


def _required_int(
    raw: dict[str, Any], key: str, line_no: int, *, minimum: int = 0, maximum: int | None = None
) -> int:
    value = raw.get(key)
    if type(value) is not int or value < minimum or (maximum is not None and value > maximum):
        bound = f" between {minimum} and {maximum}" if maximum is not None else f" >= {minimum}"
        raise BusinessError(
            "E_RECORD_INVALID",
            f"Record line {line_no} field {key!r} must be an integer{bound}",
            {"line_no": line_no, "field": key, "value": value},
        )
    return value


@dataclass(frozen=True)
class MirrorRecord:
    timestamp_ms: int
    connection_id: str
    source_service: str
    destination_service: str
    method: str
    path_template: str
    status_code: int
    response_bytes: int
    duration_us: int
    batch_id: str
    line_no: int

    @property
    def record_key(self) -> str:
        return f"{self.batch_id}:{self.line_no}"

    @property
    def sampling_sequence(self) -> str:
        # Content-derived sequence keeps a record's sampling draw stable when the
        # same record set is committed in different batch/commit groupings.
        return sha256_json(self.business_dict())

    def business_dict(self) -> dict[str, Any]:
        return {
            "timestamp_ms": self.timestamp_ms,
            "connection_id": self.connection_id,
            "source_service": self.source_service,
            "destination_service": self.destination_service,
            "method": self.method,
            "path_template": self.path_template,
            "status_code": self.status_code,
            "response_bytes": self.response_bytes,
            "duration_us": self.duration_us,
        }

    def stored_dict(self) -> dict[str, Any]:
        value = self.business_dict()
        value.update({"batch_id": self.batch_id, "line_no": self.line_no})
        return value

    def public_dict(self) -> dict[str, Any]:
        value = self.stored_dict()
        value["record_key"] = self.record_key
        return value


def parse_record(raw: Any, batch_id: str, line_no: int) -> MirrorRecord:
    if not isinstance(raw, dict):
        raise BusinessError(
            "E_RECORD_INVALID",
            f"Record line {line_no} must be a JSON object",
            {"line_no": line_no},
        )
    missing = sorted(RECORD_FIELDS - set(raw))
    extra = sorted(set(raw) - RECORD_FIELDS)
    if missing or extra:
        raise BusinessError(
            "E_RECORD_INVALID",
            f"Record line {line_no} has missing or unknown fields",
            {"line_no": line_no, "missing": missing, "unknown": extra},
        )
    timestamp_ms = _required_int(raw, "timestamp_ms", line_no)
    connection_id = _required_string(raw, "connection_id", line_no)
    source_service = _required_string(raw, "source_service", line_no)
    destination_service = _required_string(raw, "destination_service", line_no)
    method = _required_string(raw, "method", line_no, max_len=32).upper()
    if not all(ch.isalnum() or ch in "!#$%&'*+-.^_`|~" for ch in method):
        raise BusinessError(
            "E_RECORD_INVALID",
            f"Record line {line_no} has an invalid HTTP method",
            {"line_no": line_no, "method": method},
        )
    path_template = _required_string(raw, "path_template", line_no, max_len=2048)
    if not path_template.startswith("/"):
        raise BusinessError(
            "E_RECORD_INVALID",
            f"Record line {line_no} path_template must start with '/'",
            {"line_no": line_no, "path_template": path_template},
        )
    status_code = _required_int(raw, "status_code", line_no, minimum=100, maximum=599)
    response_bytes = _required_int(raw, "response_bytes", line_no)
    duration_us = _required_int(raw, "duration_us", line_no)
    return MirrorRecord(
        timestamp_ms=timestamp_ms,
        connection_id=connection_id,
        source_service=source_service,
        destination_service=destination_service,
        method=method,
        path_template=path_template,
        status_code=status_code,
        response_bytes=response_bytes,
        duration_us=duration_us,
        batch_id=batch_id,
        line_no=line_no,
    )


def parse_record_batch(batch_id: str, raw_records: list[Any]) -> list[MirrorRecord]:
    if not isinstance(batch_id, str) or not batch_id.strip():
        raise BusinessError("E_RECORD_INVALID", "batch_id must be a non-empty string")
    if not isinstance(raw_records, list) or not raw_records:
        raise BusinessError("E_RECORD_INVALID", "records must be a non-empty array")
    batch_id = batch_id.strip()
    return [parse_record(raw, batch_id, index + 1) for index, raw in enumerate(raw_records)]


def record_from_stored_dict(value: dict[str, Any]) -> MirrorRecord:
    return MirrorRecord(
        timestamp_ms=value["timestamp_ms"],
        connection_id=value["connection_id"],
        source_service=value["source_service"],
        destination_service=value["destination_service"],
        method=value["method"],
        path_template=value["path_template"],
        status_code=value["status_code"],
        response_bytes=value["response_bytes"],
        duration_us=value["duration_us"],
        batch_id=value["batch_id"],
        line_no=value["line_no"],
    )


def records_hash(records: list[MirrorRecord]) -> str:
    payload = [record.stored_dict() for record in sorted(records, key=lambda item: (item.batch_id, item.line_no))]
    return sha256_json(payload)
