from __future__ import annotations

import json
from collections import Counter
from typing import Any, Dict, Iterable, List, Mapping, Optional

from .errors import FormatInvalid
from .model import DecodedBatch, ImportedRecord, Record, canonical_json


_KNOWN_FIELDS = {
    "key",
    "value",
    "version",
    "written_at_us",
    "written_at_ms",
    "expires_at_us",
    "expires_at_ms",
    "ttl_us",
    "ttl_ms",
    "deleted",
}


def encode_jsonl(records: Iterable[Record]) -> bytes:
    lines: List[str] = []
    for record in records:
        if record.deleted:
            continue
        item: Dict[str, Any] = {
            "key": record.key,
            "value": record.value,
            "version": record.version,
            "written_at_us": record.written_at_us,
        }
        if record.expires_at_us is not None:
            item["expires_at_us"] = record.expires_at_us
        for key, value in record.extras.items():
            if key not in item:
                item[key] = value
        lines.append(json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    if not lines:
        return b""
    return ("\n".join(lines) + "\n").encode("utf-8")


def decode_jsonl(data: bytes, *, default_written_at_us: int, source_name: str = "JSON lines input") -> DecodedBatch:
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise FormatInvalid("JSON lines input is not valid UTF-8", {"source": source_name}) from exc

    records: List[ImportedRecord] = []
    warnings: List[str] = []
    unknown_counts: Counter[str] = Counter()
    filled_counts: Counter[str] = Counter()
    timestamp_units: Counter[str] = Counter()
    expiry_semantics: Counter[str] = Counter()
    non_empty_lines = 0

    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        non_empty_lines += 1
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            raise FormatInvalid(
                "JSON line is malformed",
                {"source": source_name, "line": line_number, "reason": exc.msg, "column": exc.colno},
            ) from exc
        if not isinstance(item, dict):
            raise FormatInvalid(
                "each JSON line must contain one object",
                {"source": source_name, "line": line_number},
            )
        record = _decode_record(item, len(records), default_written_at_us, line_number, source_name)
        records.append(record)
        unknown_counts.update(record.unknown_fields)
        filled_counts.update(record.filled_fields)
        timestamp_units.update(t for t in record.transformations if "timestamp" in t)
        if record.expires_at_us is not None:
            if any("ttl" in t for t in record.transformations):
                expiry_semantics["relative_ttl"] += 1
            else:
                expiry_semantics["absolute"] += 1
        else:
            expiry_semantics["none"] += 1

    if not records:
        warnings.append("input contains no JSON records")

    expiry_without_none = set(expiry_semantics) - {"none"}
    if not expiry_without_none:
        semantics = "none"
    elif len(expiry_without_none) == 1:
        semantics = next(iter(expiry_without_none))
    elif {"absolute", "relative_ttl"}.issubset(expiry_without_none):
        semantics = "mixed"
    else:
        semantics = "unknown"

    negotiation: Dict[str, Any] = {
        "input_format": "json_lines",
        "detected_by": "first-non-whitespace-brace",
        "encoding": "utf-8",
        "record_count": len(records),
        "non_empty_line_count": non_empty_lines,
        "source_timestamp_unit": "microseconds",
        "target_timestamp_unit": "microseconds",
        "timestamp_conversions": dict(timestamp_units),
        "expiry_semantics": semantics,
        "field_mapping": {
            "written_at_us": "kept as microseconds",
            "written_at_ms": "written_at_us = written_at_ms * 1000",
            "expires_at_us": "kept as absolute microseconds",
            "expires_at_ms": "expires_at_us = expires_at_ms * 1000",
            "ttl_us": "expires_at_us = record.written_at_us + ttl_us",
            "ttl_ms": "expires_at_us = record.written_at_us + ttl_ms * 1000",
            "missing expires/ttl": "expires_at_us = null (never expires)",
        },
        "unknown_fields": dict(unknown_counts),
        "filled_fields": dict(filled_counts),
        "records_with_filled_fields": {
            str(record.source_index + 1): list(record.filled_fields)
            for record in records
            if record.filled_fields
        },
        "records_with_unknown_fields": {
            str(record.source_index + 1): list(record.unknown_fields)
            for record in records
            if record.unknown_fields
        },
    }
    return DecodedBatch(
        input_format="json_lines",
        records=tuple(records),
        negotiation=negotiation,
        warnings=tuple(warnings),
    )


def _decode_record(
    item: Mapping[str, Any],
    source_index: int,
    default_written_at_us: int,
    line_number: int,
    source_name: str,
) -> ImportedRecord:
    filled: List[str] = []
    transformations: List[str] = []
    unknown = tuple(sorted(set(item) - _KNOWN_FIELDS))
    extras = {key: item[key] for key in unknown}

    key = item.get("key")
    if not isinstance(key, str) or not key:
        raise FormatInvalid(
            "JSON record key must be a non-empty string",
            {"source": source_name, "line": line_number, "field": "key"},
        )

    if "value" in item:
        value = item["value"]
    else:
        value = None
        filled.append("value")
    try:
        canonical_json(value)
    except Exception as exc:  # InvalidArgument intentionally normalized to format error here
        raise FormatInvalid(
            "JSON record value is not serializable",
            {"source": source_name, "line": line_number, "reason": str(exc)},
        ) from exc

    if "version" in item:
        version = item["version"]
        if isinstance(version, bool) or not isinstance(version, int) or version < 0:
            raise FormatInvalid(
                "JSON record version must be a non-negative integer",
                {"source": source_name, "line": line_number, "field": "version"},
            )
    else:
        version = 0
        filled.append("version")

    written_at_us = _resolve_written_at(item, default_written_at_us, filled, transformations, line_number, source_name)
    expires_at_us = _resolve_expiry(item, written_at_us, filled, transformations, line_number, source_name)
    if expires_at_us is not None and expires_at_us < written_at_us:
        raise FormatInvalid(
            "JSON record expiry is earlier than its write timestamp",
            {
                "source": source_name,
                "line": line_number,
                "written_at_us": written_at_us,
                "expires_at_us": expires_at_us,
            },
        )

    if item.get("deleted") not in (None, False, 0):
        raise FormatInvalid(
            "external JSON imports may not contain deleted records",
            {"source": source_name, "line": line_number, "field": "deleted"},
        )

    return ImportedRecord(
        key=key,
        value=value,
        version=version,
        written_at_us=written_at_us,
        expires_at_us=expires_at_us,
        source_index=source_index,
        extras=extras,
        filled_fields=tuple(filled),
        transformations=tuple(transformations),
        unknown_fields=unknown,
    )


def _resolve_written_at(
    item: Mapping[str, Any],
    default_written_at_us: int,
    filled: List[str],
    transformations: List[str],
    line_number: int,
    source_name: str,
) -> int:
    present = [name for name in ("written_at_us", "written_at_ms") if name in item]
    if len(present) > 1:
        raise FormatInvalid(
            "record specifies more than one write timestamp representation",
            {"source": source_name, "line": line_number, "fields": present},
        )
    if not present:
        filled.append("written_at_us")
        return default_written_at_us
    field = present[0]
    value = item[field]
    if isinstance(value, bool) or not isinstance(value, int):
        raise FormatInvalid(
            f"{field} must be an integer",
            {"source": source_name, "line": line_number, "field": field},
        )
    if field == "written_at_ms":
        transformations.append("written_at_ms->written_at_us")
        return value * 1000
    return value


def _resolve_expiry(
    item: Mapping[str, Any],
    written_at_us: int,
    filled: List[str],
    transformations: List[str],
    line_number: int,
    source_name: str,
) -> Optional[int]:
    absolute_fields = [name for name in ("expires_at_us", "expires_at_ms") if name in item]
    ttl_fields = [name for name in ("ttl_us", "ttl_ms") if name in item]
    if len(absolute_fields) + len(ttl_fields) > 1:
        raise FormatInvalid(
            "record specifies more than one expiry representation",
            {
                "source": source_name,
                "line": line_number,
                "fields": absolute_fields + ttl_fields,
            },
        )
    if absolute_fields:
        field = absolute_fields[0]
        value = item[field]
        if value is None:
            filled.append("expires_at_us")
            return None
        if isinstance(value, bool) or not isinstance(value, int):
            raise FormatInvalid(
                f"{field} must be an integer or null",
                {"source": source_name, "line": line_number, "field": field},
            )
        if field == "expires_at_ms":
            transformations.append("expires_at_ms->expires_at_us")
            return value * 1000
        return value
    if ttl_fields:
        field = ttl_fields[0]
        ttl = item[field]
        if isinstance(ttl, bool) or not isinstance(ttl, int) or ttl < 0:
            raise FormatInvalid(
                f"{field} must be a non-negative integer",
                {"source": source_name, "line": line_number, "field": field},
            )
        if field == "ttl_ms":
            transformations.append("ttl_ms->absolute_expires_at_us")
            return written_at_us + ttl * 1000
        transformations.append("ttl_us->absolute_expires_at_us")
        return written_at_us + ttl
    filled.append("expires_at_us")
    return None
