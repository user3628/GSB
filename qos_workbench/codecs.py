from __future__ import annotations

import json
import struct
import zlib
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Sequence

from .errors import (
    E_CRC_MISMATCH,
    E_FORMAT_INVALID,
    E_FORMAT_UNSUPPORTED,
    BusinessError,
    fail,
)
from .models import CURRENT_FORMAT_VERSION, Policy, PolicySnapshot
from .normalization import (
    UINT64_MAX,
    as_decimal,
    canonical_timestamp_rule,
    exact_us_from_ms,
    kb_to_bytes,
    kbps_to_bps,
    normalize_timestamp,
    serialize_decimal,
)
from .validation import resolve_duplicate_queues, validate_snapshot


FORMAT_BINARY = "binary"
FORMAT_JSONL = "jsonl"

BINARY_MAGIC = b"QOSB"
BINARY_HEADER_LENGTH = 38
ENDIAN_LITTLE = 1
ENDIAN_BIG = 2

BIT_CIR = 0x0001
BIT_PIR = 0x0002
BIT_CBS = 0x0004
BIT_PBS = 0x0008
BIT_PRIORITY = 0x0010
BIT_MATCH_RULE = 0x0020
BIT_PARENT = 0x0040
BIT_TIMESTAMP = 0x0080
KNOWN_RECORD_BITS = (
    BIT_CIR
    | BIT_PIR
    | BIT_CBS
    | BIT_PBS
    | BIT_PRIORITY
    | BIT_MATCH_RULE
    | BIT_PARENT
    | BIT_TIMESTAMP
)
REQUIRED_FLAG_UNKNOWN = 0x8000


COMPATIBILITY_MATRIX: dict[str, dict[str, dict[str, Any]]] = {
    FORMAT_BINARY: {
        "1": {
            "decision": "degraded_accept",
            "reason": "v1 has no PIR/PBS fields",
            "fills": {"pir_bps": "cir_bps", "pbs_bytes": "cbs_bytes"},
        },
        "2": {
            "decision": "accept",
            "reason": "v2 supports optional PIR/PBS fields",
            "fills": {"pir_bps": "cir_bps when absent", "pbs_bytes": "cbs_bytes when absent"},
        },
        "3": {
            "decision": "accept",
            "reason": "current version",
            "fills": {"pir_bps": "cir_bps when absent", "pbs_bytes": "cbs_bytes when absent"},
        },
    },
    FORMAT_JSONL: {
        "1": {
            "decision": "degraded_accept",
            "reason": "v1 JSONL omits PIR/PBS",
            "fills": {"pir_kbps": "cir_kbps", "pbs_kb": "cbs_kb"},
        },
        "2": {
            "decision": "accept",
            "reason": "v2 JSONL allows optional PIR/PBS",
            "fills": {"pir_kbps": "cir_kbps when absent", "pbs_kb": "cbs_kb when absent"},
        },
        "3": {
            "decision": "accept",
            "reason": "current version",
            "fills": {"pir_kbps": "cir_kbps when absent", "pbs_kb": "cbs_kb when absent"},
        },
    },
}


@dataclass(frozen=True, slots=True)
class Negotiation:
    format: str
    detected_version: int
    decision: str
    accepted: bool
    reason: str
    fills: dict[str, str]
    matrix_version: int = CURRENT_FORMAT_VERSION

    def as_dict(self) -> dict[str, Any]:
        return {
            "format": self.format,
            "detected_version": self.detected_version,
            "decision": self.decision,
            "accepted": self.accepted,
            "reason": self.reason,
            "fills": self.fills,
            "matrix_version": self.matrix_version,
        }


@dataclass(frozen=True, slots=True)
class DecodedPolicySet:
    format: str
    snapshot: PolicySnapshot
    negotiation: Negotiation
    import_report: dict[str, Any]


def compatibility_matrix() -> dict[str, Any]:
    return {
        "current_version": CURRENT_FORMAT_VERSION,
        "formats": COMPATIBILITY_MATRIX,
        "decision_order": ["accept", "degraded_accept", "reject"],
        "reject_rule": "a format version above current_version rejects the complete input",
    }


def negotiate_format(format_name: str, version: int) -> Negotiation:
    if format_name not in COMPATIBILITY_MATRIX:
        raise BusinessError(
            E_FORMAT_INVALID,
            "unknown policy format",
            {"format": format_name, "supported": sorted(COMPATIBILITY_MATRIX)},
        )
    if version <= 0:
        raise BusinessError(
            E_FORMAT_INVALID,
            "format version must be positive",
            {"format": format_name, "version": version},
        )
    if version > CURRENT_FORMAT_VERSION:
        raise BusinessError(
            E_FORMAT_UNSUPPORTED,
            "format version is newer than the supported compatibility matrix",
            {
                "format": format_name,
                "detected_version": version,
                "current_version": CURRENT_FORMAT_VERSION,
                "decision": "reject",
                "partial_import": False,
            },
            http_status=422,
        )
    item = COMPATIBILITY_MATRIX[format_name][str(version)]
    return Negotiation(
        format=format_name,
        detected_version=version,
        decision=item["decision"],
        accepted=True,
        reason=item["reason"],
        fills=dict(item["fills"]),
    )


def detect_format(content: bytes) -> str:
    if content.startswith(BINARY_MAGIC):
        return FORMAT_BINARY
    stripped = content.lstrip(b"\xef\xbb\xbf \t\r\n")
    if stripped.startswith(b"{") or stripped.startswith(b"["):
        return FORMAT_JSONL
    raise BusinessError(
        E_FORMAT_INVALID,
        "unable to detect policy format from content",
        {"expected_magic": BINARY_MAGIC.decode("ascii"), "expected_jsonl_prefix": "{"},
    )


def _u64(value: int, field: str) -> int:
    if value < 0 or value > UINT64_MAX:
        raise BusinessError(
            "E_UNIT_OVERFLOW",
            f"{field} exceeds unsigned 64-bit range",
            {"field": field, "value": str(value), "max": str(UINT64_MAX)},
        )
    return value


def _pack_len_bytes(value: str, field: str) -> tuple[int, bytes]:
    encoded = value.encode("utf-8")
    if len(encoded) > 0xFFFF:
        raise BusinessError(
            E_FORMAT_INVALID,
            f"{field} exceeds the 65535-byte binary field limit",
            {"field": field, "length": len(encoded)},
        )
    return len(encoded), encoded


def encode_binary(
    snapshot: PolicySnapshot,
    *,
    version: int = CURRENT_FORMAT_VERSION,
    endianness: str = "little",
) -> bytes:
    negotiate_format(FORMAT_BINARY, version)
    if endianness.lower() in {"little", "le", "<"}:
        endian = "<"
        endian_marker = ENDIAN_LITTLE
    elif endianness.lower() in {"big", "be", ">"}:
        endian = ">"
        endian_marker = ENDIAN_BIG
    else:
        raise BusinessError(
            E_FORMAT_INVALID,
            "endianness must be little or big",
            {"endianness": endianness},
        )

    header_prefix = struct.pack(
        endian + "4sBBHHIBBBBQQ",
        BINARY_MAGIC,
        version,
        endian_marker,
        0,
        BINARY_HEADER_LENGTH,
        len(snapshot.policies),
        0,  # timestamp unit: microseconds
        0,  # rate unit: bps
        0,  # burst unit: bytes
        0,
        _u64(snapshot.snapshot_no, "snapshot_no"),
        _u64(snapshot.generated_at_us, "generated_at_us"),
    )
    header = header_prefix + struct.pack(endian + "I", zlib.crc32(header_prefix) & 0xFFFFFFFF)

    records: list[bytes] = []
    for policy in snapshot.policies:
        bitmap = 0
        body_parts: list[bytes] = []

        bitmap |= BIT_CIR
        body_parts.append(struct.pack(endian + "Q", _u64(policy.cir_bps, "cir_bps")))
        if version >= 2:
            bitmap |= BIT_PIR
            body_parts.append(struct.pack(endian + "Q", _u64(policy.pir_bps, "pir_bps")))
        bitmap |= BIT_CBS
        body_parts.append(struct.pack(endian + "Q", _u64(policy.cbs_bytes, "cbs_bytes")))
        if version >= 2:
            bitmap |= BIT_PBS
            body_parts.append(struct.pack(endian + "Q", _u64(policy.pbs_bytes, "pbs_bytes")))

        bitmap |= BIT_PRIORITY
        if not 0 <= policy.priority <= 0xFFFF:
            raise BusinessError(
                E_FORMAT_INVALID,
                "priority does not fit the binary format",
                {"priority": policy.priority},
            )
        body_parts.append(struct.pack(endian + "H", policy.priority))

        bitmap |= BIT_MATCH_RULE
        match_len, match_bytes = _pack_len_bytes(policy.match_rule, "match_rule")
        body_parts.append(struct.pack(endian + "H", match_len) + match_bytes)

        if policy.parent_queue_id is not None:
            bitmap |= BIT_PARENT
            parent_len, parent_bytes = _pack_len_bytes(policy.parent_queue_id, "parent_queue_id")
            body_parts.append(struct.pack(endian + "H", parent_len) + parent_bytes)

        bitmap |= BIT_TIMESTAMP
        body_parts.append(struct.pack(endian + "Q", _u64(policy.timestamp_us, "timestamp_us")))

        queue_len, queue_bytes = _pack_len_bytes(policy.queue_id, "queue_id")
        body = (
            struct.pack(endian + "HH", bitmap, queue_len)
            + queue_bytes
            + b"".join(body_parts)
        )
        record = (
            struct.pack(endian + "I", len(body))
            + body
            + struct.pack(endian + "I", zlib.crc32(body) & 0xFFFFFFFF)
        )
        records.append(record)
    return header + b"".join(records)


@dataclass(frozen=True, slots=True)
class _BinaryHeader:
    version: int
    endian: str
    endianness: str
    flags: int
    header_length: int
    record_count: int
    snapshot_no: int
    generated_at_us: int
    header_crc32: int


def _parse_binary_header(content: bytes) -> _BinaryHeader:
    if len(content) < 6:
        raise BusinessError(E_FORMAT_INVALID, "binary policy content is shorter than its header")
    if content[:4] != BINARY_MAGIC:
        raise BusinessError(
            E_FORMAT_INVALID,
            "binary policy magic does not match",
            {"expected": BINARY_MAGIC.decode("ascii"), "actual": content[:4].hex()},
        )
    version = content[4]
    negotiate_format(FORMAT_BINARY, version)
    endian_marker = content[5]
    if endian_marker == ENDIAN_LITTLE:
        endian, endianness = "<", "little"
    elif endian_marker == ENDIAN_BIG:
        endian, endianness = ">", "big"
    else:
        raise BusinessError(
            E_FORMAT_INVALID,
            "unknown binary endianness marker",
            {"marker": endian_marker, "expected": [ENDIAN_LITTLE, ENDIAN_BIG]},
        )
    if len(content) < BINARY_HEADER_LENGTH:
        raise BusinessError(
            E_FORMAT_INVALID,
            "binary policy header is truncated",
            {"actual_length": len(content), "required": BINARY_HEADER_LENGTH},
        )
    (
        _magic,
        parsed_version,
        _marker,
        flags,
        header_length,
        record_count,
        timestamp_unit,
        rate_unit,
        burst_unit,
        _reserved,
        snapshot_no,
        generated_at_us,
        header_crc32,
    ) = struct.unpack(endian + "4sBBHHIBBBBQQI", content[:BINARY_HEADER_LENGTH])
    if header_length < BINARY_HEADER_LENGTH or header_length > len(content):
        raise BusinessError(
            E_FORMAT_INVALID,
            "binary header_length is outside the supplied content",
            {"header_length": header_length, "content_length": len(content)},
        )
    expected_crc = zlib.crc32(content[: header_length - 4]) & 0xFFFFFFFF
    if expected_crc != header_crc32:
        raise BusinessError(
            E_CRC_MISMATCH,
            "binary header CRC32 mismatch",
            {
                "scope": "header",
                "expected": f"{expected_crc:08x}",
                "actual": f"{header_crc32:08x}",
            },
        )
    if flags & REQUIRED_FLAG_UNKNOWN:
        raise BusinessError(
            E_FORMAT_UNSUPPORTED,
            "binary header declares an unknown required feature flag",
            {"flags": flags, "unknown_required_mask": flags & REQUIRED_FLAG_UNKNOWN},
            http_status=422,
        )
    unit_labels = {0: "microseconds", 1: "milliseconds"}
    rate_labels = {0: "bps", 1: "kbps"}
    burst_labels = {0: "bytes", 1: "kb"}
    if timestamp_unit not in unit_labels or rate_unit not in rate_labels or burst_unit not in burst_labels:
        raise BusinessError(
            E_FORMAT_UNSUPPORTED,
            "binary header contains unsupported unit codes",
            {
                "timestamp_unit": timestamp_unit,
                "rate_unit": rate_unit,
                "burst_unit": burst_unit,
            },
            http_status=422,
        )
    if timestamp_unit != 0 or rate_unit != 0 or burst_unit != 0:
        raise BusinessError(
            E_FORMAT_UNSUPPORTED,
            "binary v1-v3 contract requires microseconds, bps and bytes",
            {
                "timestamp_unit": unit_labels[timestamp_unit],
                "rate_unit": rate_labels[rate_unit],
                "burst_unit": burst_labels[burst_unit],
            },
            http_status=422,
        )
    return _BinaryHeader(
        version=parsed_version,
        endian=endian,
        endianness=endianness,
        flags=flags,
        header_length=header_length,
        record_count=record_count,
        snapshot_no=snapshot_no,
        generated_at_us=generated_at_us,
        header_crc32=header_crc32,
    )


def _rounding_entry(
    *,
    source_index: int,
    queue_id: str,
    source_value: Any,
    source_unit: str,
    rounded,
) -> dict[str, Any]:
    return {
        "source_index": source_index,
        "queue_id": queue_id,
        "source_timestamp": str(source_value),
        "source_unit": source_unit,
        "normalized_timestamp_us": rounded.timestamp_us,
        "rounding_bit": 1 if rounded.rounding_applied else 0,
        "rounding_applied": rounded.rounding_applied,
        "rounding_remainder_us": rounded.rounding_remainder_us,
        "rounding_direction": rounded.rounding_direction,
    }


def _timestamp_report(entries: list[dict[str, Any]], rule: str, errors: Sequence[Decimal]) -> dict[str, Any]:
    cumulative = sum(errors, Decimal(0))
    return {
        "internal_unit": "microseconds",
        "truncation_rule": rule,
        "records": entries,
        "cumulative_rounding_error_us": serialize_decimal(cumulative),
        "rounding_semantics": (
            "remainder = normalized - exact source timestamp in microseconds; "
            "negative means the normalized value was rounded downward"
        ),
    }


def decode_binary(
    content: bytes,
    *,
    timestamp_truncation: str | None = None,
    snapshot_no_override: int | None = None,
    generated_at_us_override: int | None = None,
) -> DecodedPolicySet:
    header = _parse_binary_header(content)
    negotiation = negotiate_format(FORMAT_BINARY, header.version)
    rule = canonical_timestamp_rule(timestamp_truncation)
    generated_round = normalize_timestamp(
        Decimal(header.generated_at_us), rule, field="generated_at_us"
    )
    generated_at_us = (
        generated_at_us_override
        if generated_at_us_override is not None
        else generated_round.timestamp_us
    )
    if generated_at_us < 0 or generated_at_us > UINT64_MAX:
        fail("E_UNIT_OVERFLOW", "generated_at_us is outside unsigned 64-bit range")

    policies: list[Policy] = []
    degrade_details: list[dict[str, Any]] = []
    timestamp_entries: list[dict[str, Any]] = []
    timestamp_errors: list[Decimal] = []
    cursor = header.header_length

    for source_index in range(header.record_count):
        if cursor + 4 > len(content):
            raise BusinessError(
                E_FORMAT_INVALID,
                "binary record length prefix is truncated",
                {"source_index": source_index, "offset": cursor},
            )
        (record_length,) = struct.unpack(header.endian + "I", content[cursor : cursor + 4])
        cursor += 4
        record_end = cursor + record_length
        if record_end + 4 > len(content):
            raise BusinessError(
                E_FORMAT_INVALID,
                "binary record is truncated",
                {
                    "source_index": source_index,
                    "record_length": record_length,
                    "remaining": len(content) - cursor,
                },
            )
        body = content[cursor:record_end]
        stored_crc = struct.unpack(header.endian + "I", content[record_end : record_end + 4])[0]
        actual_crc = zlib.crc32(body) & 0xFFFFFFFF
        if stored_crc != actual_crc:
            raise BusinessError(
                E_CRC_MISMATCH,
                "binary policy record CRC32 mismatch",
                {
                    "scope": "record",
                    "source_index": source_index,
                    "expected": f"{actual_crc:08x}",
                    "actual": f"{stored_crc:08x}",
                },
            )
        cursor = record_end + 4

        offset = 0

        def read(fmt: str):
            nonlocal offset
            size = struct.calcsize(header.endian + fmt)
            if offset + size > len(body):
                raise BusinessError(
                    E_FORMAT_INVALID,
                    "binary record body is truncated",
                    {"source_index": source_index, "field_offset": offset},
                )
            value = struct.unpack(header.endian + fmt, body[offset : offset + size])[0]
            offset += size
            return value

        bitmap = read("H")
        unknown_bits = bitmap & ~KNOWN_RECORD_BITS
        if unknown_bits:
            raise BusinessError(
                E_FORMAT_UNSUPPORTED,
                "binary record contains unknown required presence bits",
                {
                    "source_index": source_index,
                    "unknown_required_bits": unknown_bits,
                    "partial_import": False,
                },
                http_status=422,
            )
        if header.version == 1 and (bitmap & (BIT_PIR | BIT_PBS)):
            raise BusinessError(
                E_FORMAT_INVALID,
                "binary v1 record cannot carry PIR/PBS fields",
                {"source_index": source_index, "presence_bitmap": bitmap},
            )

        queue_len = read("H")
        if offset + queue_len > len(body):
            raise BusinessError(
                E_FORMAT_INVALID,
                "binary queue_id is truncated",
                {"source_index": source_index},
            )
        try:
            queue_id = body[offset : offset + queue_len].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise BusinessError(
                E_FORMAT_INVALID,
                "binary queue_id is not valid UTF-8",
                {"source_index": source_index},
            ) from exc
        offset += queue_len
        if not queue_id:
            raise BusinessError(
                E_FORMAT_INVALID,
                "binary queue_id must not be empty",
                {"source_index": source_index},
            )

        record_degrade: list[dict[str, Any]] = []

        def required_u64(bit: int, field: str) -> int:
            if not bitmap & bit:
                raise BusinessError(
                    E_FORMAT_INVALID,
                    f"binary record is missing required field {field}",
                    {"source_index": source_index, "queue_id": queue_id, "field": field},
                )
            return read("Q")

        cir_bps = required_u64(BIT_CIR, "cir_bps")
        if bitmap & BIT_PIR:
            pir_bps = read("Q")
        else:
            pir_bps = cir_bps
            record_degrade.append(
                {
                    "field": "pir_bps",
                    "reason": "absent_in_source",
                    "filled_value": pir_bps,
                    "rule": "pir_bps = cir_bps",
                }
            )
        cbs_bytes = required_u64(BIT_CBS, "cbs_bytes")
        if bitmap & BIT_PBS:
            pbs_bytes = read("Q")
        else:
            pbs_bytes = cbs_bytes
            record_degrade.append(
                {
                    "field": "pbs_bytes",
                    "reason": "absent_in_source",
                    "filled_value": pbs_bytes,
                    "rule": "pbs_bytes = cbs_bytes",
                }
            )

        if bitmap & BIT_PRIORITY:
            priority = read("H")
        else:
            priority = 0
            record_degrade.append(
                {
                    "field": "priority",
                    "reason": "absent_in_source",
                    "filled_value": 0,
                    "rule": "priority = 0",
                }
            )

        def read_string(bit: int, field: str) -> str | None:
            nonlocal offset
            if not bitmap & bit:
                return None
            length = read("H")
            if offset + length > len(body):
                raise BusinessError(
                    E_FORMAT_INVALID,
                    f"binary {field} is truncated",
                    {"source_index": source_index, "queue_id": queue_id},
                )
            try:
                value = body[offset : offset + length].decode("utf-8")
            except UnicodeDecodeError as exc:
                raise BusinessError(
                    E_FORMAT_INVALID,
                    f"binary {field} is not valid UTF-8",
                    {"source_index": source_index, "queue_id": queue_id},
                ) from exc
            offset += length
            return value

        match_rule = read_string(BIT_MATCH_RULE, "match_rule")
        parent_id = read_string(BIT_PARENT, "parent_queue_id")
        if match_rule is None:
            match_rule = ""
            record_degrade.append(
                {
                    "field": "match_rule",
                    "reason": "absent_in_source",
                    "filled_value": "",
                    "rule": "match_rule = empty string",
                }
            )

        if bitmap & BIT_TIMESTAMP:
            source_timestamp_us = read("Q")
            rounded = normalize_timestamp(Decimal(source_timestamp_us), rule, field="timestamp_us")
        else:
            source_timestamp_us = generated_at_us
            rounded = normalize_timestamp(Decimal(source_timestamp_us), rule, field="timestamp_us")
            record_degrade.append(
                {
                    "field": "timestamp_us",
                    "reason": "absent_in_source",
                    "filled_value": rounded.timestamp_us,
                    "rule": "timestamp_us = generated_at_us",
                }
            )
        timestamp_entries.append(
            _rounding_entry(
                source_index=source_index,
                queue_id=queue_id,
                source_value=source_timestamp_us,
                source_unit="us",
                rounded=rounded,
            )
        )
        timestamp_errors.append(Decimal(rounded.timestamp_us) - Decimal(source_timestamp_us))

        if offset != len(body):
            raise BusinessError(
                E_FORMAT_INVALID,
                "binary record has trailing bytes outside the presence bitmap",
                {"source_index": source_index, "queue_id": queue_id, "trailing_bytes": len(body) - offset},
            )

        if record_degrade:
            degrade_details.append(
                {"source_index": source_index, "queue_id": queue_id, "changes": record_degrade}
            )

        policies.append(
            Policy(
                queue_id=queue_id,
                cir_bps=cir_bps,
                pir_bps=pir_bps,
                cbs_bytes=cbs_bytes,
                pbs_bytes=pbs_bytes,
                priority=priority,
                match_rule=match_rule,
                parent_queue_id=parent_id,
                timestamp_us=rounded.timestamp_us,
            )
        )

    if cursor != len(content):
        raise BusinessError(
            E_FORMAT_INVALID,
            "binary policy content contains trailing bytes after its declared records",
            {"trailing_bytes": len(content) - cursor, "record_count": header.record_count},
        )

    snapshot_no = snapshot_no_override if snapshot_no_override is not None else header.snapshot_no
    duplicate_resolution = resolve_duplicate_queues(policies)
    snapshot = PolicySnapshot(
        format_version=header.version,
        snapshot_no=int(snapshot_no),
        generated_at_us=int(generated_at_us),
        policies=duplicate_resolution.policies,
    )
    snapshot = validate_snapshot(snapshot)
    import_report = {
        "format": FORMAT_BINARY,
        "format_version": header.version,
        "negotiation": negotiation.as_dict(),
        "source_units": {"rate": "bps", "burst": "bytes", "timestamp": "us"},
        "normalized_units": {"rate": "bps", "burst": "bytes", "timestamp": "us"},
        "endianness": header.endianness,
        "magic": BINARY_MAGIC.decode("ascii"),
        "header_crc32": f"{header.header_crc32:08x}",
        "record_crc32": "verified_per_record",
        "record_count": len(policies),
        "duplicate_resolution": duplicate_resolution.report,
        "degrade_details": degrade_details,
        "timestamp_normalization": _timestamp_report(timestamp_entries, rule, timestamp_errors),
        "partial_import": False,
    }
    return DecodedPolicySet(FORMAT_BINARY, snapshot, negotiation, import_report)


def _parse_json_lines(content: bytes) -> list[dict[str, Any]]:
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise BusinessError(E_FORMAT_INVALID, "JSONL policy content is not valid UTF-8") from exc
    records: list[dict[str, Any]] = []
    for line_no, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            value = json.loads(line, parse_float=Decimal, parse_int=int)
        except json.JSONDecodeError as exc:
            raise BusinessError(
                E_FORMAT_INVALID,
                "JSONL policy line is not valid JSON",
                {"line": line_no, "column": exc.colno, "message": exc.msg},
            ) from exc
        if not isinstance(value, dict):
            raise BusinessError(
                E_FORMAT_INVALID,
                "each JSONL policy line must be a JSON object",
                {"line": line_no, "actual_type": type(value).__name__},
            )
        records.append(value)
    if not records:
        raise BusinessError(E_FORMAT_INVALID, "JSONL policy content contains no records")
    return records


def _json_version(records: Sequence[dict[str, Any]]) -> int:
    declared: set[int] = set()
    for record in records:
        if "format_version" in record:
            value = record["format_version"]
            if isinstance(value, bool) or not isinstance(value, int):
                raise BusinessError(
                    E_FORMAT_INVALID,
                    "format_version must be an integer",
                    {"value": str(value)},
                )
            declared.add(value)
    if len(declared) > 1:
        raise BusinessError(
            E_FORMAT_INVALID,
            "JSONL records declare mixed format versions",
            {"versions": sorted(declared)},
        )
    if declared:
        return next(iter(declared))
    has_pir = all("pir_kbps" in record or "pir" in record for record in records)
    has_pbs = all("pbs_kb" in record or "pbs" in record for record in records)
    if has_pir and has_pbs:
        return 3
    if any("pir_kbps" in record or "pir" in record for record in records):
        return 2
    return 1


def _required_json_rate(record: dict[str, Any], primary: str, alias: str, field: str) -> Any:
    if primary in record:
        return record[primary]
    if alias in record:
        return record[alias]
    raise BusinessError(
        E_FORMAT_INVALID,
        f"JSONL policy record is missing required field {primary}",
        {"queue_id": record.get("queue_id"), "field": primary},
    )


def _strict_json_int(value: Any, field: str) -> int:
    decimal_value = as_decimal(value, field)
    if decimal_value != decimal_value.to_integral_value():
        raise BusinessError(
            E_FORMAT_INVALID,
            f"{field} must be an integer",
            {"field": field, "value": str(value)},
        )
    return int(decimal_value)


def _optional_required_extension_error(record: dict[str, Any]) -> None:
    for key, value in record.items():
        if key.startswith("required_") and key not in {"required_fields"}:
            if value:
                raise BusinessError(
                    E_FORMAT_UNSUPPORTED,
                    "JSONL record declares an unknown required extension",
                    {"field": key, "value": str(value), "partial_import": False},
                    http_status=422,
                )
    required = record.get("required_fields")
    if required:
        if not isinstance(required, list):
            raise BusinessError(
                E_FORMAT_UNSUPPORTED,
                "required_fields must be a list",
                {"value": str(required)},
                http_status=422,
            )
        known = {
            "queue_id",
            "cir_kbps",
            "pir_kbps",
            "cbs_kb",
            "pbs_kb",
            "priority",
            "match_rule",
            "parent_queue_id",
            "timestamp_ms",
        }
        unknown = sorted(str(item) for item in required if str(item) not in known)
        if unknown:
            raise BusinessError(
                E_FORMAT_UNSUPPORTED,
                "JSONL record requires fields unknown to this implementation",
                {"unknown_required_fields": unknown, "partial_import": False},
                http_status=422,
            )


def decode_jsonl(
    content: bytes,
    *,
    timestamp_truncation: str | None = None,
    snapshot_no_override: int | None = None,
    generated_at_us_override: int | None = None,
) -> DecodedPolicySet:
    raw_records = _parse_json_lines(content)
    version = _json_version(raw_records)
    negotiation = negotiate_format(FORMAT_JSONL, version)
    rule = canonical_timestamp_rule(timestamp_truncation)

    snapshot_numbers: set[int] = set()
    generated_values: list[Decimal] = []
    for record in raw_records:
        _optional_required_extension_error(record)
        if "snapshot_no" in record:
            try:
                value = _strict_json_int(record["snapshot_no"], "snapshot_no")
            except (TypeError, ValueError) as exc:
                raise BusinessError(
                    E_FORMAT_INVALID,
                    "snapshot_no must be an integer",
                    {"value": str(record["snapshot_no"])},
                ) from exc
            snapshot_numbers.add(value)
        if "generated_at_ms" in record:
            generated_values.append(exact_us_from_ms(record["generated_at_ms"], "generated_at_ms"))
        elif "generated_at_us" in record:
            generated_values.append(as_decimal(record["generated_at_us"], "generated_at_us"))
    if len(snapshot_numbers) > 1:
        raise BusinessError(
            E_FORMAT_INVALID,
            "JSONL records contain inconsistent snapshot_no metadata",
            {"snapshot_numbers": sorted(snapshot_numbers)},
        )
    if len(set(generated_values)) > 1:
        raise BusinessError(
            E_FORMAT_INVALID,
            "JSONL records contain inconsistent generated_at metadata",
            {"generated_at_us_values": sorted(str(value) for value in set(generated_values))},
        )

    generated_exact = generated_values[0] if generated_values else Decimal(0)
    generated_round = normalize_timestamp(generated_exact, rule, field="generated_at_us")
    generated_at_us = (
        int(generated_at_us_override)
        if generated_at_us_override is not None
        else generated_round.timestamp_us
    )

    policies: list[Policy] = []
    degrade_details: list[dict[str, Any]] = []
    timestamp_entries: list[dict[str, Any]] = []
    timestamp_errors: list[Decimal] = []

    for index, record in enumerate(raw_records):
        queue_id = record.get("queue_id")
        if queue_id is None:
            queue_id = record.get("queue")
        if not isinstance(queue_id, str) or not queue_id:
            raise BusinessError(
                E_FORMAT_INVALID,
                "JSONL policy record requires a non-empty queue_id",
                {"source_index": index},
            )
        record_degrade: list[dict[str, Any]] = []

        cir = _required_json_rate(record, "cir_kbps", "cir", "cir_kbps")
        cir_bps = kbps_to_bps(cir, f"records[{index}].cir_kbps")
        if version == 1 and ("pir_kbps" in record or "pir" in record):
            raise BusinessError(
                E_FORMAT_INVALID,
                "JSONL v1 record cannot carry PIR",
                {"source_index": index, "queue_id": queue_id},
            )
        if "pir_kbps" in record or "pir" in record:
            pir_bps = kbps_to_bps(record.get("pir_kbps", record.get("pir")), f"records[{index}].pir_kbps")
        else:
            pir_bps = cir_bps
            record_degrade.append(
                {
                    "field": "pir_bps",
                    "reason": "absent_in_source",
                    "filled_value": pir_bps,
                    "rule": "pir_bps = cir_bps",
                }
            )

        cbs = _required_json_rate(record, "cbs_kb", "cbs", "cbs_kb")
        cbs_bytes = kb_to_bytes(cbs, f"records[{index}].cbs_kb")
        if version == 1 and ("pbs_kb" in record or "pbs" in record):
            raise BusinessError(
                E_FORMAT_INVALID,
                "JSONL v1 record cannot carry PBS",
                {"source_index": index, "queue_id": queue_id},
            )
        if "pbs_kb" in record or "pbs" in record:
            pbs_bytes = kb_to_bytes(record.get("pbs_kb", record.get("pbs")), f"records[{index}].pbs_kb")
        else:
            pbs_bytes = cbs_bytes
            record_degrade.append(
                {
                    "field": "pbs_bytes",
                    "reason": "absent_in_source",
                    "filled_value": pbs_bytes,
                    "rule": "pbs_bytes = cbs_bytes",
                }
            )

        if "priority" not in record:
            priority = 0
            record_degrade.append(
                {
                    "field": "priority",
                    "reason": "absent_in_source",
                    "filled_value": 0,
                    "rule": "priority = 0",
                }
            )
        else:
            priority = _strict_json_int(record["priority"], "priority")

        match_rule = record.get("match_rule", record.get("match", ""))
        if not isinstance(match_rule, str):
            raise BusinessError(
                E_FORMAT_INVALID,
                "match_rule must be a string",
                {"source_index": index, "queue_id": queue_id},
            )
        if "match_rule" not in record and "match" not in record:
            record_degrade.append(
                {
                    "field": "match_rule",
                    "reason": "absent_in_source",
                    "filled_value": "",
                    "rule": "match_rule = empty string",
                }
            )

        parent_id = record.get("parent_queue_id", record.get("parent"))
        if parent_id is not None and not isinstance(parent_id, str):
            raise BusinessError(
                E_FORMAT_INVALID,
                "parent_queue_id must be a string or null",
                {"source_index": index, "queue_id": queue_id},
            )

        if "timestamp_ms" in record:
            exact_us = exact_us_from_ms(record["timestamp_ms"], f"records[{index}].timestamp_ms")
            source_value = record["timestamp_ms"]
            source_unit = "ms"
            rounded = normalize_timestamp(exact_us, rule, field="timestamp_ms")
        elif "timestamp" in record:
            exact_us = exact_us_from_ms(record["timestamp"], f"records[{index}].timestamp")
            source_value = record["timestamp"]
            source_unit = "ms"
            rounded = normalize_timestamp(exact_us, rule, field="timestamp")
        elif "timestamp_us" in record:
            exact_us = as_decimal(record["timestamp_us"], f"records[{index}].timestamp_us")
            source_value = record["timestamp_us"]
            source_unit = "us"
            rounded = normalize_timestamp(exact_us, rule, field="timestamp_us")
        else:
            exact_us = Decimal(generated_at_us)
            source_value = generated_at_us
            source_unit = "us(generated_at_us fallback)"
            rounded = normalize_timestamp(exact_us, rule, field="timestamp_us")
            record_degrade.append(
                {
                    "field": "timestamp",
                    "reason": "absent_in_source",
                    "filled_value": rounded.timestamp_us,
                    "rule": "timestamp_us = generated_at_us",
                }
            )
        timestamp_entries.append(
            _rounding_entry(
                source_index=index,
                queue_id=queue_id,
                source_value=source_value,
                source_unit=source_unit,
                rounded=rounded,
            )
        )
        timestamp_errors.append(Decimal(rounded.timestamp_us) - exact_us)
        if record_degrade:
            degrade_details.append(
                {"source_index": index, "queue_id": queue_id, "changes": record_degrade}
            )

        policies.append(
            Policy(
                queue_id=queue_id,
                cir_bps=cir_bps,
                pir_bps=pir_bps,
                cbs_bytes=cbs_bytes,
                pbs_bytes=pbs_bytes,
                priority=priority,
                match_rule=match_rule,
                parent_queue_id=parent_id,
                timestamp_us=rounded.timestamp_us,
            )
        )

    snapshot_no = (
        snapshot_no_override
        if snapshot_no_override is not None
        else (next(iter(snapshot_numbers)) if snapshot_numbers else 1)
    )
    duplicate_resolution = resolve_duplicate_queues(policies)
    snapshot = PolicySnapshot(
        format_version=version,
        snapshot_no=int(snapshot_no),
        generated_at_us=generated_at_us,
        policies=duplicate_resolution.policies,
    )
    snapshot = validate_snapshot(snapshot)
    import_report = {
        "format": FORMAT_JSONL,
        "format_version": version,
        "negotiation": negotiation.as_dict(),
        "source_units": {"rate": "kbps", "burst": "KB (1024 bytes)", "timestamp": "ms"},
        "normalized_units": {"rate": "bps", "burst": "bytes", "timestamp": "us"},
        "record_count": len(policies),
        "duplicate_resolution": duplicate_resolution.report,
        "degrade_details": degrade_details,
        "timestamp_normalization": _timestamp_report(timestamp_entries, rule, timestamp_errors),
        "partial_import": False,
    }
    return DecodedPolicySet(FORMAT_JSONL, snapshot, negotiation, import_report)


def decode_policy_content(
    content: bytes,
    *,
    format_name: str = "auto",
    timestamp_truncation: str | None = None,
    snapshot_no_override: int | None = None,
    generated_at_us_override: int | None = None,
) -> DecodedPolicySet:
    selected = detect_format(content) if format_name == "auto" else format_name
    if selected == FORMAT_BINARY:
        return decode_binary(
            content,
            timestamp_truncation=timestamp_truncation,
            snapshot_no_override=snapshot_no_override,
            generated_at_us_override=generated_at_us_override,
        )
    if selected == FORMAT_JSONL:
        return decode_jsonl(
            content,
            timestamp_truncation=timestamp_truncation,
            snapshot_no_override=snapshot_no_override,
            generated_at_us_override=generated_at_us_override,
        )
    raise BusinessError(
        E_FORMAT_INVALID,
        "format must be auto, binary or jsonl",
        {"format": format_name},
    )


def _json_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def encode_jsonl(
    snapshot: PolicySnapshot,
    *,
    version: int = CURRENT_FORMAT_VERSION,
) -> bytes:
    negotiate_format(FORMAT_JSONL, version)
    lines: list[str] = []
    for policy in snapshot.policies:
        fields: list[str] = [
            f'"format_version":{version}',
            f'"snapshot_no":{snapshot.snapshot_no}',
            '"generated_at_ms":' + serialize_decimal(Decimal(snapshot.generated_at_us) / 1000),
            '"queue_id":' + _json_string(policy.queue_id),
            '"cir_kbps":' + serialize_decimal(Decimal(policy.cir_bps) / 1000),
        ]
        if version >= 2:
            fields.append('"pir_kbps":' + serialize_decimal(Decimal(policy.pir_bps) / 1000))
        fields.append('"cbs_kb":' + serialize_decimal(Decimal(policy.cbs_bytes) / 1024))
        if version >= 2:
            fields.append('"pbs_kb":' + serialize_decimal(Decimal(policy.pbs_bytes) / 1024))
        fields.extend(
            [
                f'"priority":{policy.priority}',
                '"match_rule":' + _json_string(policy.match_rule),
                '"parent_queue_id":'
                + ("null" if policy.parent_queue_id is None else _json_string(policy.parent_queue_id)),
                '"timestamp_ms":' + serialize_decimal(Decimal(policy.timestamp_us) / 1000),
            ]
        )
        lines.append("{" + ",".join(fields) + "}")
    return ("\n".join(lines) + "\n").encode("utf-8")


def encode_policy_content(
    snapshot: PolicySnapshot,
    *,
    format_name: str,
    version: int = CURRENT_FORMAT_VERSION,
    endianness: str = "little",
) -> bytes:
    if format_name == FORMAT_BINARY:
        return encode_binary(snapshot, version=version, endianness=endianness)
    if format_name == FORMAT_JSONL:
        return encode_jsonl(snapshot, version=version)
    raise BusinessError(
        E_FORMAT_INVALID,
        "format must be binary or jsonl",
        {"format": format_name},
    )


def detect_and_negotiate(content: bytes, format_name: str = "auto") -> dict[str, Any]:
    selected = detect_format(content) if format_name == "auto" else format_name
    if selected == FORMAT_BINARY:
        header = _parse_binary_header(content)
        negotiation = negotiate_format(FORMAT_BINARY, header.version)
        return {
            "ok": True,
            "format": FORMAT_BINARY,
            "detected_version": header.version,
            "endianness": header.endianness,
            "record_count": header.record_count,
            "negotiation": negotiation.as_dict(),
            "matrix": compatibility_matrix(),
            "partial_import": False,
        }
    if selected == FORMAT_JSONL:
        records = _parse_json_lines(content)
        version = _json_version(records)
        negotiation = negotiate_format(FORMAT_JSONL, version)
        return {
            "ok": True,
            "format": FORMAT_JSONL,
            "detected_version": version,
            "record_count": len(records),
            "negotiation": negotiation.as_dict(),
            "matrix": compatibility_matrix(),
            "partial_import": False,
        }
    raise BusinessError(
        E_FORMAT_INVALID,
        "format must be auto, binary or jsonl",
        {"format": format_name},
    )

