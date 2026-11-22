from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..compat import decide, ensure_supported
from ..errors import BusinessError, E_FORMAT_CORRUPT, E_FORMAT_UNSUPPORTED
from ..model import StatsSnapshot
from .binary import MAGIC as BINARY_MAGIC
from .binary import decode_binary, encode_binary
from .jsonl import decode_jsonl, encode_jsonl


@dataclass(frozen=True)
class DecodedSnapshot:
    snapshot: StatsSnapshot
    report: dict[str, Any]


def detect_format(data: bytes) -> str:
    if data.startswith(BINARY_MAGIC):
        return "binary"
    stripped = data.lstrip()
    if stripped.startswith(b"\xef\xbb\xbf"):
        stripped = stripped[3:].lstrip()
    if stripped.startswith(b"{"):
        return "jsonl"
    if BINARY_MAGIC.startswith(data[: len(BINARY_MAGIC)]):
        raise BusinessError(
            E_FORMAT_CORRUPT,
            "input is a truncated binary magic prefix",
            {"format": "binary", "bytes": len(data)},
        )
    raise BusinessError(
        E_FORMAT_UNSUPPORTED,
        "input is neither a JCSB binary snapshot nor JSON Lines statistics",
        {"prefix_hex": data[:16].hex()},
    )


def decode_snapshot(data: bytes, format_hint: str | None = None) -> DecodedSnapshot:
    detected = detect_format(data)
    if format_hint is not None and format_hint not in {detected, "auto"}:
        raise BusinessError(
            E_FORMAT_CORRUPT,
            "declared format hint disagrees with detected format",
            {"format_hint": format_hint, "detected_format": detected},
        )
    if detected == "binary":
        decoded = decode_binary(data)
    else:
        decoded = decode_jsonl(data)
    return DecodedSnapshot(snapshot=decoded.snapshot, report=decoded.report)


def encode_snapshot(
    snapshot: StatsSnapshot,
    format_name: str,
    *,
    endian: str = "little",
    format_version: int = 2,
) -> bytes:
    if format_name == "binary":
        return encode_binary(snapshot, endian=endian, format_version=format_version)
    if format_name == "jsonl":
        return encode_jsonl(snapshot, format_version=format_version)
    raise BusinessError(
        E_FORMAT_UNSUPPORTED, "unsupported output format", {"format": format_name}
    )


__all__ = [
    "DecodedSnapshot",
    "decode_binary",
    "decode_jsonl",
    "decode_snapshot",
    "detect_format",
    "encode_binary",
    "encode_jsonl",
    "encode_snapshot",
]

