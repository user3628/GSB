"""JSON Lines snapshot codec.

The first non-empty line is a manifest.  Every subsequent non-empty line is one
window record and may be processed independently by external tools.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from .compat import CURRENT_FORMAT_VERSION, FUTURE_FORMAT_VERSION, negotiate
from .errors import SloError, corrupt
from .models import (
    NegotiationResult,
    ParsedBatch,
    SnapshotManifest,
    WindowRecord,
)
from .policy import BoundaryPolicy
from .timeutil import normalize_us_to_ms


MANIFEST_KIND = "snapshot_manifest"
RECORD_KIND = "window_record"


def _json_loads_object(raw: bytes, path: str, offset: int, what: str) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise corrupt(
            path,
            f"{what} is not valid UTF-8",
            offset + exc.start,
            details={"record_type": what},
        ) from exc
    except json.JSONDecodeError as exc:
        raise corrupt(
            path,
            f"{what} is not valid JSON",
            offset + exc.pos,
            details={"record_type": what, "line": exc.lineno, "column": exc.colno},
        ) from exc
    if not isinstance(value, dict):
        raise corrupt(
            path,
            f"{what} must be a JSON object",
            offset,
            details={"record_type": what},
        )
    return value


def _nonempty_lines(data: bytes):
    offset = 0
    for line_number, raw in enumerate(data.splitlines(keepends=True), start=1):
        stripped = raw.rstrip(b"\r\n")
        if stripped.strip():
            yield line_number, offset, stripped
        offset += len(raw)
    return


def _require_int(
    obj: dict[str, Any],
    field: str,
    *,
    path: str,
    offset: int,
    line_number: int,
    minimum: int = 0,
) -> int:
    if field not in obj:
        raise corrupt(
            path,
            f"missing required JSON field: {field}",
            offset,
            details={"line": line_number, "field": field},
        )
    value = obj[field]
    if isinstance(value, bool) or not isinstance(value, int):
        raise corrupt(
            path,
            f"JSON field {field} must be an integer",
            offset,
            details={"line": line_number, "field": field, "value": value},
        )
    if value < minimum:
        raise corrupt(
            path,
            f"JSON field {field} must be >= {minimum}",
            offset,
            details={"line": line_number, "field": field, "value": value},
        )
    return value


def _string_field(
    obj: dict[str, Any],
    field: str,
    *,
    path: str,
    offset: int,
    line_number: int,
    limit_bytes: int,
) -> str:
    value = obj.get(field)
    if not isinstance(value, str) or not value:
        raise corrupt(
            path,
            f"JSON field {field} must be a non-empty string",
            offset,
            details={"line": line_number, "field": field},
        )
    encoded = value.encode("utf-8")
    if len(encoded) > limit_bytes:
        raise corrupt(
            path,
            f"JSON field {field} byte length is out of bounds",
            offset,
            details={
                "line": line_number,
                "field": field,
                "length": len(encoded),
                "maximum": limit_bytes,
            },
        )
    return value


def inspect_jsonl_manifest(path: str | Path) -> tuple[SnapshotManifest, int, int]:
    source = str(path)
    try:
        data = Path(path).read_bytes()
    except OSError as exc:
        raise SloError("E_IO", f"cannot read {source}: {exc}", path=source) from exc
    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]
    first = next(_nonempty_lines(data), None)
    if first is None:
        raise corrupt(source, "JSONL snapshot is empty", 0)
    line_number, offset, raw = first
    manifest_obj = _json_loads_object(raw, source, offset, "snapshot manifest")
    if manifest_obj.get("kind") not in {MANIFEST_KIND, "manifest"}:
        raise SloError(
            "E_FORMAT_UNSUPPORTED",
            "first non-empty JSONL line must be a snapshot manifest",
            path=source,
            offset=offset,
            details={"kind": manifest_obj.get("kind")},
        )
    if manifest_obj.get("format", "jsonl") != "jsonl":
        raise SloError(
            "E_FORMAT_UNSUPPORTED",
            "JSONL manifest format must be jsonl",
            path=source,
            offset=offset,
            details={"format": manifest_obj.get("format")},
        )
    version = _require_int(
        manifest_obj,
        "format_version",
        path=source,
        offset=offset,
        line_number=line_number,
        minimum=1,
    )
    sequence = _require_int(
        manifest_obj,
        "snapshot_sequence",
        path=source,
        offset=offset,
        line_number=line_number,
        minimum=0,
    )
    generated_at_ms = _require_int(
        manifest_obj,
        "generated_at_ms",
        path=source,
        offset=offset,
        line_number=line_number,
        minimum=0,
    )
    record_count = _require_int(
        manifest_obj,
        "record_count",
        path=source,
        offset=offset,
        line_number=line_number,
        minimum=0,
    )
    required_raw = manifest_obj.get("required_extensions", [])
    if not isinstance(required_raw, list) or any(
        not isinstance(item, str) or not item for item in required_raw
    ):
        raise corrupt(
            source,
            "required_extensions must be a list of non-empty strings",
            offset,
            details={"line": line_number},
        )
    manifest = SnapshotManifest(
        file_format="jsonl",
        format_version=version,
        snapshot_sequence=sequence,
        generated_at_ms=generated_at_ms,
        record_count=record_count,
        required_extensions=tuple(required_raw),
        source_path=source,
        header_size=offset + len(raw) + (1 if raw.endswith(b"\n") else 0),
    )
    return manifest, offset, len(raw)


def preflight_jsonl(path: str | Path, _boundaries: BoundaryPolicy | None = None) -> NegotiationResult:
    manifest, _offset, _length = inspect_jsonl_manifest(path)
    return negotiate("jsonl", manifest)


def decode_jsonl(
    path: str | Path,
    *,
    truncation_rule: str = "floor",
    boundaries: BoundaryPolicy | None = None,
) -> ParsedBatch:
    source = str(path)
    boundaries = boundaries or BoundaryPolicy()
    boundaries.validate()
    try:
        data = Path(path).read_bytes()
    except OSError as exc:
        raise SloError("E_IO", f"cannot read {source}: {exc}", path=source) from exc
    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]
    manifest, _manifest_offset, _manifest_length = inspect_jsonl_manifest(path)
    negotiation = negotiate("jsonl", manifest)
    if negotiation.verdict == "REJECT":
        raise SloError(
            negotiation.code or "E_FORMAT_UNSUPPORTED",
            "JSONL format rejected by compatibility matrix",
            path=source,
            offset=0,
            details=negotiation.to_dict(),
        )

    records: list[WindowRecord] = []
    rounding = []
    degradations: list[dict[str, Any]] = []
    seen_keys: dict[tuple[str, str, int], int] = {}
    seen_manifest = False
    final_offset = 0

    for line_number, offset, raw in _nonempty_lines(data):
        final_offset = offset + len(raw)
        if not seen_manifest:
            seen_manifest = True
            continue
        obj = _json_loads_object(raw, source, offset, "window record")
        kind = obj.get("kind", RECORD_KIND)
        if kind not in {RECORD_KIND, "record"}:
            raise corrupt(
                source,
                "unsupported JSONL record kind",
                offset,
                details={"line": line_number, "kind": kind},
            )
        required_fields = obj.get("required_extensions")
        if required_fields:
            if not isinstance(required_fields, list) or any(
                not isinstance(item, str) or not item for item in required_fields
            ):
                raise corrupt(
                    source,
                    "record required_extensions must be a list of non-empty strings",
                    offset,
                    details={"line": line_number},
                )
            raise SloError(
                "E_FORMAT_UNSUPPORTED",
                "record declares an unknown required extension",
                path=source,
                offset=offset,
                details={
                    "line": line_number,
                    "unknown_required_fields": required_fields,
                },
            )

        service = _string_field(
            obj,
            "service",
            path=source,
            offset=offset,
            line_number=line_number,
            limit_bytes=boundaries.max_service_name_bytes,
        )
        sli_type = _string_field(
            obj,
            "sli_type",
            path=source,
            offset=offset,
            line_number=line_number,
            limit_bytes=boundaries.max_sli_type_bytes,
        )
        window_length_ms = _require_int(
            obj,
            "window_length_ms",
            path=source,
            offset=offset,
            line_number=line_number,
            minimum=1,
        )
        if window_length_ms > boundaries.max_window_length_ms:
            raise corrupt(
                source,
                "window_length_ms is outside declared bounds",
                offset,
                details={
                    "line": line_number,
                    "window_length_ms": window_length_ms,
                    "maximum": boundaries.max_window_length_ms,
                },
            )
        good_events = _require_int(
            obj,
            "good_events",
            path=source,
            offset=offset,
            line_number=line_number,
        )
        total_events = _require_int(
            obj,
            "total_events",
            path=source,
            offset=offset,
            line_number=line_number,
        )
        if good_events > total_events:
            raise SloError(
                "E_COUNT_INVALID",
                "good_events must not exceed total_events",
                path=source,
                offset=offset,
                details={
                    "line": line_number,
                    "good_events": good_events,
                    "total_events": total_events,
                },
            )
        if manifest.format_version == 1:
            if "window_start_us" in obj:
                raise corrupt(
                    source,
                    "version 1 JSONL must use window_end_ago_us, not window_start_us",
                    offset,
                    details={"line": line_number},
                )
            ago_us = _require_int(
                obj,
                "window_end_ago_us",
                path=source,
                offset=offset,
                line_number=line_number,
            )
            generated_us = manifest.generated_at_ms * 1000
            absolute_start_us = generated_us - ago_us - window_length_ms * 1000
            if absolute_start_us < 0:
                raise corrupt(
                    source,
                    "version 1 relative window resolves before the Unix epoch",
                    offset,
                    details={
                        "line": line_number,
                        "generated_at_ms": manifest.generated_at_ms,
                        "window_end_ago_us": ago_us,
                        "window_length_ms": window_length_ms,
                    },
                )
            window_start_ms, time_rounding = normalize_us_to_ms(
                absolute_start_us,
                rule=truncation_rule,
                key=(service, sli_type, 0),
                source_field="window_end_ago_us",
                offset=offset,
                path=source,
            )
            time_rounding = type(time_rounding)(
                key=(service, sli_type, window_start_ms),
                source_field=time_rounding.source_field,
                source_value=time_rounding.source_value,
                source_unit=time_rounding.source_unit,
                normalized_ms=window_start_ms,
                rule=time_rounding.rule,
                remainder_us=time_rounding.remainder_us,
                rounding_bit=time_rounding.rounding_bit,
                rounding_error_us=time_rounding.rounding_error_us,
                offset=offset,
            )
            late_count = 0
            late_actions: list[dict[str, Any]] = []
            if "late_count" in obj:
                late_count = _require_int(
                    obj,
                    "late_count",
                    path=source,
                    offset=offset,
                    line_number=line_number,
                )
            else:
                late_actions.append(
                    {
                        "action": "late_count_defaulted",
                        "from": None,
                        "to": 0,
                        "rule": "version_1_missing_late_count",
                    }
                )
            degradations.append(
                {
                    "line": line_number,
                    "offset": offset,
                    "key": [service, sli_type, window_start_ms],
                    "actions": [
                        {
                            "action": "relative_time_converted",
                            "rule": (
                                "generated_at_ms*1000 - window_end_ago_us "
                                "- window_length_ms*1000"
                            ),
                            "source_window_end_ago_us": ago_us,
                            "normalized_window_start_ms": window_start_ms,
                            "truncation_rule": truncation_rule,
                        },
                        *late_actions,
                    ],
                }
            )
        else:
            start_us = _require_int(
                obj,
                "window_start_us",
                path=source,
                offset=offset,
                line_number=line_number,
            )
            window_start_ms, time_rounding = normalize_us_to_ms(
                start_us,
                rule=truncation_rule,
                key=(service, sli_type, 0),
                source_field="window_start_us",
                offset=offset,
                path=source,
            )
            time_rounding = type(time_rounding)(
                key=(service, sli_type, window_start_ms),
                source_field=time_rounding.source_field,
                source_value=time_rounding.source_value,
                source_unit=time_rounding.source_unit,
                normalized_ms=window_start_ms,
                rule=time_rounding.rule,
                remainder_us=time_rounding.remainder_us,
                rounding_bit=time_rounding.rounding_bit,
                rounding_error_us=time_rounding.rounding_error_us,
                offset=offset,
            )
            late_count = _require_int(
                obj,
                "late_count",
                path=source,
                offset=offset,
                line_number=line_number,
            )

        if late_count > total_events:
            raise SloError(
                "E_COUNT_INVALID",
                "late_count must not exceed total_events",
                path=source,
                offset=offset,
                details={
                    "line": line_number,
                    "late_count": late_count,
                    "total_events": total_events,
                },
            )
        last_write_ms = manifest.generated_at_ms
        if "last_write_ms" in obj:
            last_write_ms = _require_int(
                obj,
                "last_write_ms",
                path=source,
                offset=offset,
                line_number=line_number,
            )

        key = (service, sli_type, window_start_ms)
        if key in seen_keys:
            raise corrupt(
                source,
                "duplicate record key in JSONL snapshot",
                offset,
                details={
                    "line": line_number,
                    "first_offset": seen_keys[key],
                    "key": list(key),
                },
            )
        seen_keys[key] = offset
        records.append(
            WindowRecord(
                service=service,
                sli_type=sli_type,
                window_start_ms=window_start_ms,
                window_length_ms=window_length_ms,
                good_events=good_events,
                total_events=total_events,
                late_count=late_count,
                last_write_ms=last_write_ms,
                format_version=manifest.format_version,
                snapshot_sequence=manifest.snapshot_sequence,
                source_format="jsonl",
                source_offset=offset,
            )
        )
        rounding.append(time_rounding)

    if not seen_manifest:
        raise corrupt(source, "JSONL snapshot has no manifest", 0)
    if len(records) != manifest.record_count:
        raise corrupt(
            source,
            "JSONL record count does not match manifest",
            final_offset,
            details={
                "declared": manifest.record_count,
                "decoded": len(records),
                "file_size": len(data),
            },
        )
    return ParsedBatch(
        manifest=manifest,
        negotiation=negotiation,
        records=tuple(records),
        rounding=tuple(rounding),
        cumulative_rounding_error_us=sum(item.rounding_error_us for item in rounding),
        degradations=tuple(degradations),
        checksum_sha256=hashlib.sha256(data).hexdigest(),
    )


def encode_jsonl_bytes(
    records: tuple[WindowRecord, ...] | list[WindowRecord],
    manifest: SnapshotManifest,
    *,
    version: int = CURRENT_FORMAT_VERSION,
    timestamp_remainders_us: dict[tuple[str, str, int], int] | None = None,
    relative_remainders_us: dict[tuple[str, str, int], int] | None = None,
    required_extension: str = "tenant_shard_v3",
) -> bytes:
    timestamp_remainders_us = timestamp_remainders_us or {}
    relative_remainders_us = relative_remainders_us or {}
    ordered = sorted(records, key=lambda item: item.sort_key)
    extensions = [required_extension] if version >= FUTURE_FORMAT_VERSION else []
    manifest_obj: dict[str, Any] = {
        "kind": MANIFEST_KIND,
        "format": "jsonl",
        "format_version": version,
        "snapshot_sequence": manifest.snapshot_sequence,
        "generated_at_ms": manifest.generated_at_ms,
        "record_count": len(ordered),
        "required_extensions": extensions,
    }
    lines = [json.dumps(manifest_obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)]
    for record in ordered:
        obj: dict[str, Any] = {
            "kind": RECORD_KIND,
            "service": record.service,
            "sli_type": record.sli_type,
            "window_length_ms": record.window_length_ms,
            "good_events": record.good_events,
            "total_events": record.total_events,
            "last_write_ms": record.last_write_ms or manifest.generated_at_ms,
        }
        if version == 1:
            ago_us = (
                manifest.generated_at_ms * 1000
                - record.window_end_ms * 1000
                - relative_remainders_us.get(record.key, 0)
            )
            if ago_us < 0:
                raise SloError(
                    "E_SNAPSHOT_INVALID",
                    "version 1 JSONL export cannot represent a window ending after generated_at_ms",
                )
            obj["window_end_ago_us"] = ago_us
        else:
            remainder = timestamp_remainders_us.get(record.key, 0)
            if not 0 <= remainder < 1000:
                raise SloError(
                    "E_CONFIG_INVALID",
                    "JSONL sub-millisecond remainders must be within 0..999",
                    details={"record": list(record.key), "remainder_us": remainder},
                )
            obj["window_start_us"] = record.window_start_ms * 1000 + remainder
            obj["late_count"] = record.late_count
        if version >= FUTURE_FORMAT_VERSION:
            obj["required_extensions"] = [required_extension]
            obj[required_extension] = "local-tenant"
        lines.append(json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
    return ("\n".join(lines) + "\n").encode("utf-8")


def write_jsonl_snapshot(
    path: str | Path,
    records: tuple[WindowRecord, ...] | list[WindowRecord],
    manifest: SnapshotManifest,
    *,
    version: int = CURRENT_FORMAT_VERSION,
    timestamp_remainders_us: dict[tuple[str, str, int], int] | None = None,
    relative_remainders_us: dict[tuple[str, str, int], int] | None = None,
) -> str:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = encode_jsonl_bytes(
        records,
        manifest,
        version=version,
        timestamp_remainders_us=timestamp_remainders_us,
        relative_remainders_us=relative_remainders_us,
    )
    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=str(target.parent))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, target)
    except Exception:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise
    return hashlib.sha256(payload).hexdigest()
