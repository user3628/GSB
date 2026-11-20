from __future__ import annotations

from .codec_binary import BINARY_MAGIC, decode_binary
from .codec_jsonl import decode_jsonl
from .errors import FormatInvalid
from .model import DecodedBatch


def detect_format(data: bytes) -> str:
    if data.startswith(BINARY_MAGIC):
        return "compact_binary"
    stripped = data
    if stripped.startswith(b"\xef\xbb\xbf"):
        stripped = stripped[3:]
    stripped = stripped.lstrip()
    if stripped.startswith(b"{"):
        return "json_lines"
    raise FormatInvalid(
        "input format could not be negotiated",
        {
            "expected": ["compact_binary(KVSB magic)", "json_lines(single JSON object per line)"],
            "first_bytes_hex": data[:16].hex(),
        },
    )


def decode_external(
    data: bytes,
    *,
    expected_format: str | None = None,
    default_written_at_us: int,
    source_name: str = "input",
) -> DecodedBatch:
    stripped = data[3:] if data.startswith(b"\xef\xbb\xbf") else data
    if not stripped.strip() and expected_format == "json_lines":
        decoded = decode_jsonl(data, default_written_at_us=default_written_at_us, source_name=source_name)
        decoded.negotiation["detected_by"] = "expected-format:empty-jsonl"
        return decoded
    negotiated = detect_format(data)
    if expected_format is not None and negotiated != expected_format:
        raise FormatInvalid(
            "input format does not match the endpoint or requested format",
            {"negotiated_format": negotiated, "expected_format": expected_format, "source": source_name},
        )
    if negotiated == "compact_binary":
        return decode_binary(data, source_name=source_name)
    return decode_jsonl(data, default_written_at_us=default_written_at_us, source_name=source_name)
