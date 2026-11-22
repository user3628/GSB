from __future__ import annotations

import json
import struct
from dataclasses import dataclass
from typing import Any

from ..errors import E_FORMAT_CORRUPT, E_FORMAT_UNSUPPORTED, BusinessError, corrupt
from ..models import Snapshot
from .binary import (
    BYTE_TO_KIND,
    ENDIAN_NAMES,
    FLAG_UNKNOWN_REQUIRED,
    HEADER_FIXED,
    MAGIC,
    decode_binary,
    encode_binary,
)
from .jsonl import decode_jsonl, encode_jsonl


COMPATIBILITY_MATRIX: dict[str, dict[str, Any]] = {
    "binary": {
        "versions": {
            "1": {
                "decision": "degraded_accept",
                "reason": "legacy binary uses ratio weights; explicit rounding policy required",
            },
            "2": {"decision": "accept", "reason": "current binary format"},
            "3+": {
                "decision": "reject",
                "code": E_FORMAT_UNSUPPORTED,
                "reason": "newer versions may contain unknown required fields",
            },
        }
    },
    "jsonl": {
        "versions": {
            "1": {
                "decision": "degraded_accept",
                "reason": "legacy JSONL omits probe params and uses float ratio weights",
            },
            "2": {"decision": "accept", "reason": "current sharded JSONL format"},
            "3+": {
                "decision": "reject",
                "code": E_FORMAT_UNSUPPORTED,
                "reason": "newer versions may contain unknown required fields",
            },
        }
    },
}


@dataclass(frozen=True)
class FormatInfo:
    format: str
    version: int
    kind: str
    endianness: str | None = None
    record_count: int | None = None
    unknown_required: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": self.format,
            "version": self.version,
            "kind": self.kind,
            "endianness": self.endianness,
            "record_count": self.record_count,
            "unknown_required": self.unknown_required,
        }


def _detect_binary(data: bytes) -> FormatInfo:
    if len(data) < HEADER_FIXED.size:
        raise corrupt(0, "binary header is truncated")
    magic, version, marker, kind_byte, flags = HEADER_FIXED.unpack_from(data, 0)
    if magic != MAGIC:
        raise corrupt(0, "binary magic does not match")
    if marker not in ENDIAN_NAMES:
        raise corrupt(0, "binary endianness marker is invalid", endianness_marker=marker)
    if kind_byte not in BYTE_TO_KIND:
        raise corrupt(0, "binary kind byte is invalid", kind_byte=kind_byte)
    endianness = ENDIAN_NAMES[marker]
    offset = HEADER_FIXED.size
    count = None
    if len(data) >= offset + 4:
        prefix = "<" if endianness == "little" else ">"
        count = struct.unpack(prefix + "I", data[offset : offset + 4])[0]
    return FormatInfo(
        format="binary",
        version=version,
        kind=BYTE_TO_KIND[kind_byte],
        endianness=endianness,
        record_count=count,
        unknown_required=bool(flags & FLAG_UNKNOWN_REQUIRED),
    )


def _first_json_object(data: bytes | str | list[bytes | str]) -> dict[str, Any]:
    if isinstance(data, list):
        texts = []
        for shard in data:
            if isinstance(shard, bytes):
                texts.append(shard.decode("utf-8", errors="replace"))
            else:
                texts.append(shard)
    elif isinstance(data, bytes):
        try:
            texts = [data.decode("utf-8")]
        except UnicodeDecodeError as exc:
            raise BusinessError(
                E_FORMAT_UNSUPPORTED,
                "input is neither binary nor UTF-8 JSONL",
                http_status=415,
            ) from exc
    else:
        texts = [data]
    for text in texts:
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise corrupt(0, f"invalid JSONL: {exc.msg}") from exc
            if not isinstance(value, dict):
                raise corrupt(0, "JSONL record must be an object")
            return value
    raise corrupt(0, "JSONL snapshot contains no records")


def detect_format(data: bytes | str | list[bytes | str]) -> FormatInfo:
    if isinstance(data, bytes) and data.startswith(MAGIC):
        return _detect_binary(data)
    item = _first_json_object(data)
    version = item.get("format_version", 1)
    if isinstance(version, bool) or not isinstance(version, int):
        raise corrupt(0, "format_version must be an integer")
    kind = item.get("kind", "registration")
    if kind not in ("registration", "probe_result"):
        raise corrupt(0, "kind is invalid", kind=kind)
    unknown_required = item.get("unknown_required_fields", [])
    return FormatInfo(
        format="jsonl",
        version=version,
        kind=kind,
        unknown_required=bool(unknown_required),
    )


def decide_compatibility(info: FormatInfo) -> dict[str, Any]:
    format_matrix = COMPATIBILITY_MATRIX.get(info.format)
    if format_matrix is None:
        raise BusinessError(
            E_FORMAT_UNSUPPORTED,
            "unknown snapshot format",
            details={"format": info.format},
            http_status=415,
        )
    entry = format_matrix["versions"].get(str(info.version))
    if entry is None and info.version >= 3:
        entry = format_matrix["versions"]["3+"]
    if entry is None:
        raise BusinessError(
            E_FORMAT_UNSUPPORTED,
            "snapshot version is not present in the compatibility matrix",
            details={"format": info.format, "version": info.version, "matrix": COMPATIBILITY_MATRIX},
            http_status=409,
        )
    decision = entry["decision"]
    if info.unknown_required:
        decision = "reject"
        entry = {
            **entry,
            "reason": "snapshot declares unknown required fields",
            "code": E_FORMAT_UNSUPPORTED,
        }
    if info.version >= 3:
        decision = "reject"
        entry = {
            **entry,
            "reason": "newer format version is outside the supported compatibility window",
            "code": E_FORMAT_UNSUPPORTED,
        }
    result = {
        "format": info.format,
        "version": info.version,
        "kind": info.kind,
        "decision": decision,
        "reason": entry["reason"],
        "matrix_entry": {**entry, "version": str(info.version)},
        "detected": info.to_dict(),
    }
    if decision == "reject":
        raise BusinessError(
            entry.get("code", E_FORMAT_UNSUPPORTED),
            entry["reason"],
            details={"negotiation": result, "matrix": COMPATIBILITY_MATRIX},
            http_status=409,
        )
    return result


def decode_snapshot(
    data: bytes | str | list[bytes | str],
    *,
    expected_format: str = "auto",
    expected_kind: str | None = None,
) -> tuple[Snapshot, FormatInfo, dict[str, Any]]:
    info = detect_format(data)
    if expected_format not in ("auto", None) and expected_format != info.format:
        raise BusinessError(
            E_FORMAT_UNSUPPORTED,
            "input format does not match the declared format",
            details={"declared": expected_format, "detected": info.format},
            http_status=415,
        )
    if expected_kind is not None and expected_kind != info.kind:
        raise BusinessError(
            E_FORMAT_UNSUPPORTED,
            "snapshot kind does not match the requested kind",
            details={"expected": expected_kind, "detected": info.kind},
            http_status=409,
        )
    negotiation = decide_compatibility(info)
    if info.format == "binary":
        snapshot = decode_binary(data)  # type: ignore[arg-type]
    else:
        snapshot = decode_jsonl(data)
    if snapshot.version != info.version or snapshot.kind != info.kind:
        raise BusinessError(
            E_FORMAT_CORRUPT,
            "snapshot metadata changed between negotiation and decoding",
            details={"detected": info.to_dict(), "decoded_version": snapshot.version, "decoded_kind": snapshot.kind},
        )
    return snapshot, info, negotiation


def encode_snapshot(
    snapshot: Snapshot,
    *,
    target_format: str,
    version: int = 2,
    endianness: str = "little",
    shards: int = 1,
) -> bytes | list[bytes]:
    if target_format == "binary":
        return encode_binary(snapshot, version=version, endianness=endianness)
    if target_format == "jsonl":
        return encode_jsonl(snapshot, version=version, shards=shards)
    raise BusinessError(
        E_FORMAT_UNSUPPORTED,
        "target format must be binary or jsonl",
        details={"target_format": target_format},
        http_status=415,
    )
