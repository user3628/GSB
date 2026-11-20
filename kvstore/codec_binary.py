from __future__ import annotations

import json
import struct
import zlib
from typing import Any, Dict, Iterable, List

from .errors import FormatInvalid, InvalidArgument
from .model import DecodedBatch, ImportedRecord, Record, canonical_json


BINARY_MAGIC = b"KVSB"
BINARY_FORMAT_VERSION = 1
ENDIAN_LITTLE = 0x01
ENDIAN_BIG = 0x02
_ENDIAN_TO_PREFIX = {ENDIAN_LITTLE: "<", ENDIAN_BIG: ">"}
_PREFIX_TO_ENDIAN = {value: key for key, value in _ENDIAN_TO_PREFIX.items()}
_MAX_RECORD_BYTES = 64 * 1024 * 1024


def _endian_marker(endian: str) -> int:
    if endian == "little":
        return ENDIAN_LITTLE
    if endian == "big":
        return ENDIAN_BIG
    raise InvalidArgument("endian must be 'little' or 'big'", {"endian": endian})


def _endian_name(marker: int) -> str:
    return "little" if marker == ENDIAN_LITTLE else "big"


def encode_binary(records: Iterable[Record], endian: str = "little") -> bytes:
    marker = _endian_marker(endian)
    prefix = _ENDIAN_TO_PREFIX[marker]
    materialized = list(records)
    header = bytearray()
    header.extend(BINARY_MAGIC)
    header.extend(bytes((BINARY_FORMAT_VERSION, marker, 0, 0)))
    header.extend(struct.pack(prefix + "I", len(materialized)))

    output = bytearray(header)
    for record in materialized:
        key_bytes = record.key.encode("utf-8")
        value_bytes = canonical_json(record.value).encode("utf-8")
        if len(key_bytes) > 0xFFFFFFFF or len(value_bytes) > 0xFFFFFFFF:
            raise InvalidArgument("key or value is too large for binary format", {"key": record.key})
        written_ms = record.written_at_us // 1000
        expires_ms = -1 if record.expires_at_us is None else record.expires_at_us // 1000
        payload = b"".join(
            (
                struct.pack(prefix + "I", len(key_bytes)),
                key_bytes,
                struct.pack(prefix + "I", len(value_bytes)),
                value_bytes,
                struct.pack(prefix + "qqq", record.version, written_ms, expires_ms),
            )
        )
        crc = zlib.crc32(payload) & 0xFFFFFFFF
        output.extend(struct.pack(prefix + "I", len(payload)))
        output.extend(payload)
        output.extend(struct.pack(prefix + "I", crc))
    return bytes(output)


def decode_binary(data: bytes, *, source_name: str = "binary input") -> DecodedBatch:
    if len(data) < 12:
        raise FormatInvalid("binary input is shorter than its 12-byte header", {"source": source_name})
    if data[:4] != BINARY_MAGIC:
        raise FormatInvalid("binary magic does not match KVSB", {"source": source_name})
    version = data[4]
    if version != BINARY_FORMAT_VERSION:
        raise FormatInvalid(
            "unsupported compact binary format version",
            {"source": source_name, "format_version": version, "supported": [BINARY_FORMAT_VERSION]},
        )
    marker = data[5]
    if marker not in _ENDIAN_TO_PREFIX:
        raise FormatInvalid("unknown endian marker", {"source": source_name, "endian_marker": marker})
    if data[6:8] != b"\x00\x00":
        raise FormatInvalid("reserved binary header bytes must be zero", {"source": source_name})
    prefix = _ENDIAN_TO_PREFIX[marker]
    record_count = struct.unpack(prefix + "I", data[8:12])[0]

    offset = 12
    records: List[ImportedRecord] = []
    for record_index in range(record_count):
        if offset + 4 > len(data):
            raise FormatInvalid(
                "binary input ended before a record length prefix",
                {"source": source_name, "record_index": record_index},
            )
        payload_len = struct.unpack(prefix + "I", data[offset : offset + 4])[0]
        offset += 4
        if payload_len > _MAX_RECORD_BYTES:
            raise FormatInvalid(
                "binary record exceeds the implementation limit",
                {"source": source_name, "record_index": record_index, "payload_len": payload_len},
            )
        if offset + payload_len + 4 > len(data):
            raise FormatInvalid(
                "binary input ended inside a record",
                {"source": source_name, "record_index": record_index},
            )
        payload = data[offset : offset + payload_len]
        offset += payload_len
        expected_crc = struct.unpack(prefix + "I", data[offset : offset + 4])[0]
        offset += 4
        actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
        if actual_crc != expected_crc:
            raise FormatInvalid(
                "record CRC32 mismatch",
                {
                    "source": source_name,
                    "record_index": record_index,
                    "expected_crc32": expected_crc,
                    "actual_crc32": actual_crc,
                },
            )
        record = _decode_payload(payload, prefix, record_index, source_name)
        records.append(record)

    if offset != len(data):
        raise FormatInvalid(
            "binary input has trailing bytes after its declared record count",
            {"source": source_name, "trailing_bytes": len(data) - offset, "record_count": record_count},
        )

    negotiation: Dict[str, Any] = {
        "input_format": "compact_binary",
        "detected_by": "magic:KVSB",
        "format_version": version,
        "endianness": _endian_name(marker),
        "record_count": len(records),
        "checksum": "crc32-per-record",
        "checksums_verified": True,
        "source_timestamp_unit": "milliseconds",
        "target_timestamp_unit": "microseconds",
        "expiry_semantics": "absolute",
        "field_mapping": {
            "written_at_ms": "written_at_us = written_at_ms * 1000",
            "expires_at_ms": "expires_at_us = expires_at_ms * 1000; -1 becomes null",
        },
        "records_with_normalized_timestamps": len(records),
        "unknown_fields": {},
        "filled_fields": {},
    }
    return DecodedBatch(input_format="compact_binary", records=tuple(records), negotiation=negotiation)


def _decode_payload(payload: bytes, prefix: str, record_index: int, source_name: str) -> ImportedRecord:
    try:
        pos = 0
        if pos + 4 > len(payload):
            raise ValueError("missing key length")
        key_len = struct.unpack(prefix + "I", payload[pos : pos + 4])[0]
        pos += 4
        if pos + key_len + 4 > len(payload):
            raise ValueError("key overruns payload")
        key = payload[pos : pos + key_len].decode("utf-8")
        pos += key_len
        value_len = struct.unpack(prefix + "I", payload[pos : pos + 4])[0]
        pos += 4
        if pos + value_len + 24 != len(payload):
            raise ValueError("value length does not match payload layout")
        value_bytes = payload[pos : pos + value_len]
        pos += value_len
        version, written_ms, expires_ms = struct.unpack(prefix + "qqq", payload[pos : pos + 24])
        if not key:
            raise ValueError("empty key")
        if version < 0:
            raise ValueError("negative version")
        value = json.loads(value_bytes.decode("utf-8"))
        written_at_us = written_ms * 1000
        expires_at_us = None if expires_ms == -1 else expires_ms * 1000
        if expires_at_us is not None and expires_at_us < written_at_us:
            raise ValueError("expiry is earlier than write time")
    except (UnicodeDecodeError, json.JSONDecodeError, struct.error, ValueError) as exc:
        raise FormatInvalid(
            "binary record payload is malformed",
            {"source": source_name, "record_index": record_index, "reason": str(exc)},
        ) from exc

    return ImportedRecord(
        key=key,
        value=value,
        version=version,
        written_at_us=written_at_us,
        expires_at_us=expires_at_us,
        source_index=record_index,
        transformations=("written_at_ms->written_at_us", "expires_at_ms->expires_at_us"),
    )
