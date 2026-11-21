"""Format auto-detection and codec dispatch."""
from __future__ import annotations

from app.codecs import binary, jsonl, yaml_like
from app.errors import ErrorCode, WorkbenchError
from app.models import DecodedPayload, FormatName


def detect_format(data: bytes) -> FormatName:
    if data.startswith(binary.MAGIC):
        return FormatName.BINARY
    stripped = data.lstrip()
    if stripped.startswith(b"{"):
        return FormatName.JSONL
    if stripped.startswith(b"#") or b":" in stripped.split(b"\n", 1)[0]:
        return FormatName.YAML
    raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, "unable to detect snapshot format")


def decode(data: bytes) -> DecodedPayload:
    fmt = detect_format(data)
    if fmt is FormatName.BINARY:
        return binary.decode(data)
    if fmt is FormatName.JSONL:
        return jsonl.decode(data)
    return yaml_like.decode(data)


def encode(payload: DecodedPayload) -> bytes:
    fmt = payload.header.format
    if fmt is FormatName.BINARY:
        return binary.encode(payload)
    if fmt is FormatName.JSONL:
        return jsonl.encode(payload)
    return yaml_like.encode(payload)
