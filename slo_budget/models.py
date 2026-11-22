"""Internal models and deterministic JSON helpers."""

from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Iterable


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    raise TypeError(f"not JSON serializable: {type(value)!r}")


def to_jsonable(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return to_jsonable(dataclasses.asdict(value))
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    return value


def dumps_json(value: Any, *, pretty: bool = False) -> str:
    if pretty:
        return json.dumps(
            to_jsonable(value),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    return json.dumps(
        to_jsonable(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def canonical_json(value: Any) -> str:
    return dumps_json(value, pretty=False)


@dataclass(frozen=True)
class WindowRecord:
    """One normalized, time-aggregated window record."""

    service: str
    sli_type: str
    window_start_ms: int
    window_length_ms: int
    good_events: int
    total_events: int
    late_count: int
    last_write_ms: int = 0
    format_version: int = 2
    snapshot_sequence: int = 0
    source_format: str = "internal"
    source_offset: int = field(default=-1, compare=False)

    @property
    def window_end_ms(self) -> int:
        return self.window_start_ms + self.window_length_ms

    @property
    def key(self) -> tuple[str, str, int]:
        return (self.service, self.sli_type, self.window_start_ms)

    @property
    def sort_key(self) -> tuple[str, str, int, int]:
        return (
            self.service,
            self.sli_type,
            self.window_start_ms,
            self.window_length_ms,
        )

    @property
    def bad_events(self) -> int:
        return self.total_events - self.good_events

    def internal_dict(self) -> dict[str, Any]:
        return {
            "service": self.service,
            "sli_type": self.sli_type,
            "window_start_ms": self.window_start_ms,
            "window_length_ms": self.window_length_ms,
            "good_events": self.good_events,
            "total_events": self.total_events,
            "late_count": self.late_count,
        }

    def persisted_dict(self) -> dict[str, Any]:
        value = self.internal_dict()
        value.update(
            {
                "last_write_ms": self.last_write_ms,
                "format_version": self.format_version,
                "snapshot_sequence": self.snapshot_sequence,
            }
        )
        return value


@dataclass(frozen=True)
class RoundingInfo:
    key: tuple[str, str, int]
    source_field: str
    source_value: int
    source_unit: str
    normalized_ms: int
    rule: str
    remainder_us: int
    rounding_bit: int
    rounding_error_us: int
    offset: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "service": self.key[0],
            "sli_type": self.key[1],
            "window_start_ms": self.key[2],
            "source_field": self.source_field,
            "source_value": self.source_value,
            "source_unit": self.source_unit,
            "normalized_ms": self.normalized_ms,
            "rule": self.rule,
            "remainder_us": self.remainder_us,
            "rounding_bit": self.rounding_bit,
            "rounding_error_us": self.rounding_error_us,
            "offset": self.offset,
        }


@dataclass(frozen=True)
class SnapshotManifest:
    file_format: str
    format_version: int
    snapshot_sequence: int
    generated_at_ms: int
    record_count: int
    required_extensions: tuple[str, ...] = ()
    source_path: str = ""
    header_size: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": self.file_format,
            "format_version": self.format_version,
            "snapshot_sequence": self.snapshot_sequence,
            "generated_at_ms": self.generated_at_ms,
            "record_count": self.record_count,
            "required_extensions": list(self.required_extensions),
            "source_path": self.source_path,
            "header_size": self.header_size,
        }


@dataclass(frozen=True)
class NegotiationResult:
    file_format: str
    format_version: int
    verdict: str
    manifest: SnapshotManifest
    reasons: tuple[str, ...] = ()
    degradations: tuple[dict[str, Any], ...] = ()
    code: str | None = None
    unknown_required_fields: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": self.file_format,
            "format_version": self.format_version,
            "verdict": self.verdict,
            "manifest": self.manifest.to_dict(),
            "reasons": list(self.reasons),
            "degradations": list(self.degradations),
            "code": self.code,
            "unknown_required_fields": list(self.unknown_required_fields),
        }


@dataclass(frozen=True)
class ParsedBatch:
    manifest: SnapshotManifest
    negotiation: NegotiationResult
    records: tuple[WindowRecord, ...]
    rounding: tuple[RoundingInfo, ...] = ()
    cumulative_rounding_error_us: int = 0
    degradations: tuple[dict[str, Any], ...] = ()
    checksum_sha256: str = ""

    def to_dict(self, *, include_records: bool = True) -> dict[str, Any]:
        value: dict[str, Any] = {
            "manifest": self.manifest.to_dict(),
            "negotiation": self.negotiation.to_dict(),
            "rounding": {
                "rule": self.rounding[0].rule if self.rounding else "floor",
                "cumulative_error_us": self.cumulative_rounding_error_us,
                "records": [item.to_dict() for item in self.rounding],
            },
            "degradations": list(self.degradations),
            "checksum_sha256": self.checksum_sha256,
            "record_count": len(self.records),
        }
        if include_records:
            value["records"] = [record.persisted_dict() for record in self.records]
        return value


@dataclass(frozen=True)
class RequestEvent:
    timestamp_ms: int
    service: str
    sli_type: str
    good: bool
    late: bool
    source_line: int = 0


def sorted_records(records: Iterable[WindowRecord]) -> tuple[WindowRecord, ...]:
    return tuple(sorted(records, key=lambda item: item.sort_key))
