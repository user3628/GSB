"""Format detection and a single decode/export entry point."""

from __future__ import annotations

from pathlib import Path

from .binary_format import (
    decode_binary,
    preflight_binary,
    write_binary_snapshot,
)
from .compat import CURRENT_FORMAT_VERSION
from .errors import SloError
from .jsonl_format import (
    decode_jsonl,
    preflight_jsonl,
    write_jsonl_snapshot,
)
from .models import NegotiationResult, ParsedBatch, SnapshotManifest, WindowRecord
from .policy import BoundaryPolicy


SUPPORTED_FORMATS = {"binary", "jsonl"}


def normalize_format(value: str | None) -> str:
    if value is None or value == "auto":
        return "auto"
    normalized = value.lower().replace("-", "").replace("_", "")
    if normalized in {"binary", "bin", "compactbinary"}:
        return "binary"
    if normalized in {"jsonl", "jsonlines", "ndjson", "json"}:
        return "jsonl"
    raise SloError(
        "E_FORMAT_UNSUPPORTED",
        "format must be auto, binary, or jsonl",
        details={"format": value},
    )


def detect_format(path: str | Path) -> str:
    source = str(path)
    try:
        with open(path, "rb") as handle:
            prefix = handle.read(64)
    except OSError as exc:
        raise SloError("E_IO", f"cannot read {source}: {exc}", path=source) from exc
    if prefix.startswith(b"SLOSNAP\x00"):
        return "binary"
    stripped = prefix.lstrip(b"\xef\xbb\xbf \t\r\n")
    if stripped.startswith(b"{"):
        return "jsonl"
    raise SloError(
        "E_FORMAT_UNSUPPORTED",
        "input format cannot be detected",
        path=source,
        offset=0,
        details={"prefix_hex": prefix[:16].hex()},
    )


def preflight(
    path: str | Path,
    *,
    expected_format: str | None = "auto",
    boundaries: BoundaryPolicy | None = None,
) -> NegotiationResult:
    expected = normalize_format(expected_format)
    detected = detect_format(path)
    if expected != "auto" and expected != detected:
        raise SloError(
            "E_FORMAT_UNSUPPORTED",
            "declared format does not match detected input format",
            path=str(path),
            offset=0,
            details={"declared": expected, "detected": detected},
        )
    if detected == "binary":
        return preflight_binary(path, boundaries)
    return preflight_jsonl(path, boundaries)


def decode(
    path: str | Path,
    *,
    expected_format: str | None = "auto",
    truncation_rule: str = "floor",
    boundaries: BoundaryPolicy | None = None,
) -> ParsedBatch:
    expected = normalize_format(expected_format)
    detected = detect_format(path)
    if expected != "auto" and expected != detected:
        raise SloError(
            "E_FORMAT_UNSUPPORTED",
            "declared format does not match detected input format",
            path=str(path),
            offset=0,
            details={"declared": expected, "detected": detected},
        )
    if detected == "binary":
        return decode_binary(
            path,
            truncation_rule=truncation_rule,
            boundaries=boundaries,
        )
    return decode_jsonl(
        path,
        truncation_rule=truncation_rule,
        boundaries=boundaries,
    )


def export_snapshot(
    path: str | Path,
    records: tuple[WindowRecord, ...] | list[WindowRecord],
    manifest: SnapshotManifest,
    *,
    file_format: str,
    endian: str = "little",
) -> str:
    normalized = normalize_format(file_format)
    if normalized == "auto":
        raise SloError("E_CONFIG_INVALID", "export format must be binary or jsonl")
    if normalized == "binary":
        return write_binary_snapshot(
            path,
            records,
            manifest,
            endian=endian,
            version=CURRENT_FORMAT_VERSION,
        )
    return write_jsonl_snapshot(
        path,
        records,
        manifest,
        version=CURRENT_FORMAT_VERSION,
    )
