from __future__ import annotations

import json

import pytest

from kvstore.errors import StoreError
from kvstore.formats import (
    BINARY_HEADER_SIZE,
    BINARY_MAGIC,
    decode_binary,
    decode_import,
    encode_binary,
    encode_jsonl,
    negotiate_format,
)
from kvstore.model import Record


def sample_records() -> list[Record]:
    return [
        Record("alpha", b"one", 1, 1_000, 5_000),
        Record("binary", b"\x00\xff\x10", 2, 1_001, None),
        Record("deleted", b"", 3, 1_002, None, True),
    ]


@pytest.mark.parametrize("endian", ["little", "big"])
def test_binary_roundtrip_both_endians(endian: str) -> None:
    raw = encode_binary(sample_records(), endian=endian)
    assert raw[:4] == BINARY_MAGIC
    assert raw[4] == 1
    assert raw[5:6] == (b"L" if endian == "little" else b"B")
    decoded = decode_binary(raw)
    assert [item.record for item in decoded] == sample_records()
    assert decoded[0].offset == BINARY_HEADER_SIZE
    assert decoded[1].offset == decoded[0].offset + decoded[0].length


def test_binary_crc_failure_reports_record_position() -> None:
    raw = bytearray(encode_binary(sample_records()))
    raw[-2] ^= 0xFF
    with pytest.raises(StoreError) as caught:
        decode_binary(raw)
    assert caught.value.code == "E_RECORD_CORRUPT"
    assert caught.value.context["record_index"] == 2
    assert "byte_offset" in caught.value.context


def test_jsonl_roundtrip_preserves_bytes_and_reports_rounding() -> None:
    records = [
        Record("a", b"text", 2, 1_700_000_000_123, 1_700_000_100_456),
        Record("b", b"\x00\xff", 1, 1_700_000_000_999, None),
    ]
    raw = encode_jsonl(records, shard_count=2)
    parsed, negotiation = decode_import(
        raw, requested_format="auto", import_time_ms=1_700_000_000_000
    )
    assert [item.record for item in parsed] == records
    assert negotiation["detected_format"] == "jsonl"
    assert negotiation["actual_record_count"] == 2
    assert negotiation["cumulative_rounding_error_us"] == 0
    assert all(item.line_no is not None for item in parsed)


def test_jsonl_microsecond_ttl_and_defaults() -> None:
    lines = [
        {
            "key": "rounded",
            "value": "x",
            "version": 4,
            "write_timestamp_us": 1_000_001_999,
            "expires_at_us": 2_000_004_999,
        },
        {
            "key": "ttl",
            "write_timestamp_us": 3_000_009_999,
            "base_time_us": 3_000_009_999,
            "ttl_us": 5_000_777,
        },
    ]
    raw = ("\n".join(json.dumps(line) for line in lines) + "\n").encode()
    parsed, report = decode_import(raw, import_time_ms=1)
    first, second = parsed
    assert first.record.write_ts_ms == 1_000_001
    assert first.record.expires_at_ms == 2_000_004
    assert first.normalization["write"]["rounding_remainder_us"] == 999
    assert first.normalization["expires"]["rounding_remainder_us"] == 999
    assert second.record.version == 1
    assert "version=1" in second.defaults_used
    assert second.record.expires_at_ms == 3_005_010
    assert report["cumulative_rounding_error_us"] < 0


def test_jsonl_corrupt_line_is_positioned_and_atomic_input() -> None:
    good = json.loads(encode_jsonl([Record("ok", b"v", 1, 1000, None)]).decode().strip())
    bad = dict(good)
    bad["key"] = "tampered"
    raw = (
        json.dumps(good, separators=(",", ":"))
        + "\n"
        + json.dumps(bad, separators=(",", ":"))
        + "\n"
    ).encode()
    with pytest.raises(StoreError) as caught:
        decode_import(raw, import_time_ms=1)
    assert caught.value.code == "E_RECORD_CORRUPT"
    assert caught.value.context["line_no"] == 2


def test_format_negotiation_and_mismatch() -> None:
    binary = encode_binary(sample_records())
    detected, report = negotiate_format(binary, "auto")
    assert detected == "binary"
    assert report["endianness"] == "little"
    assert report["declared_record_count"] == 3
    with pytest.raises(StoreError) as caught:
        negotiate_format(binary, "jsonl")
    assert caught.value.code == "E_FORMAT_MISMATCH"

