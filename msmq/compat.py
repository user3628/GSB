from __future__ import annotations

from typing import Any

from .errors import E_FORMAT_INCOMPATIBLE, E_FORMAT_UNKNOWN, MQError


FORMAT_COMPACT = "compact"
FORMAT_JSONL = "jsonl"

# Explicit, machine-readable compatibility policy.  The matrix is part of the
# public contract and is returned by GET /v1/compatibility.
COMPATIBILITY_MATRIX: dict[str, Any] = {
    "internal_time_precision": "microseconds_on_millisecond_grid",
    "compact": {
        "format": FORMAT_COMPACT,
        "current_version": 2,
        "supported_versions": [1, 2],
        "decisions": {"1": "degrade_accept", "2": "accept"},
        "reject_versions": {">2": "E_FORMAT_INCOMPATIBLE"},
        "degrade_defaults": {
            "expires_at_us": "produced_at_us + 60000ms",
            "content_type": "text/plain",
            "state": "pending",
            "record_version": 1,
            "last_write_at_us": "produced_at_us",
            "delivery_count": 0,
            "extensions": {},
        },
        "capacity": {"max_body_bytes": 8192, "max_batch_records": 128, "max_record_bytes": 1048576},
        "time_precision": "microseconds",
        "field_optionality": "record bitmap",
    },
    "jsonl": {
        "format": FORMAT_JSONL,
        "current_version": 1,
        "supported_versions": [1],
        "decisions": {"1": "accept"},
        "reject_versions": {">1": "E_FORMAT_INCOMPATIBLE"},
        "degrade_defaults": {
            "content_type": "text/plain",
            "state": "pending",
            "record_version": 1,
            "last_write_at_ms": "produced_at_ms",
            "delivery_count": 0,
            "extensions": {},
        },
        "capacity": {"max_body_bytes": 4096, "max_batch_records": 64, "max_line_bytes": 65536},
        "time_precision": "milliseconds",
        "field_optionality": "absence of optional JSON keys",
    },
}

COMPACT_MAX_BODY_BYTES = 8192
COMPACT_MAX_BATCH_RECORDS = 128
JSONL_MAX_BODY_BYTES = 4096
JSONL_MAX_BATCH_RECORDS = 64


def decide_compatibility(
    format_name: str,
    version: int,
    *,
    has_unknown_required_extension: bool = False,
) -> tuple[str, dict[str, Any]]:
    """Return ``accept`` or ``degrade_accept``; raise for incompatible input."""

    spec = COMPATIBILITY_MATRIX.get(format_name)
    if spec is None:
        raise MQError(
            E_FORMAT_UNKNOWN,
            "unknown persistence format",
            http_status=415,
            details={"format": format_name},
        )
    if has_unknown_required_extension:
        raise MQError(
            E_FORMAT_INCOMPATIBLE,
            "input declares an unknown required extension",
            http_status=422,
            details={
                "format": format_name,
                "version": version,
                "decision": "reject",
                "reason": "unknown_required_extension",
                "matrix": spec["decisions"],
            },
        )
    decision = spec["decisions"].get(str(version))
    if decision is None:
        raise MQError(
            E_FORMAT_INCOMPATIBLE,
            f"{format_name} version {version} is outside the compatibility matrix",
            http_status=422,
            details={
                "format": format_name,
                "version": version,
                "decision": "reject",
                "supported_versions": spec["supported_versions"],
                "matrix": spec["decisions"],
            },
        )
    return decision, {
        "format": format_name,
        "version": version,
        "decision": decision,
        "current_version": spec["current_version"],
    }

