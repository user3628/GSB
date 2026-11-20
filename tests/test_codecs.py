from __future__ import annotations

import json
import struct

import pytest

from kvstore.codec_binary import BINARY_MAGIC, decode_binary, encode_binary
from kvstore.codec_jsonl import decode_jsonl, encode_jsonl
from kvstore.errors import FormatInvalid
from kvstore.formats import decode_external, detect_format
from kvstore.model import Record


def test_binary_roundtrip_little_and_big_endian() -> None:
    records = [
        Record("a", {"n": 1}, 2, 4_100_000_000_123_000, 4_200_000_000_000_000),
        Record("b", "value", 1, 4_100_000_000_000_000, None),
    ]
    for endian in ("little", "big"):
        encoded = encode_binary(records, endian=endian)
        assert encoded.startswith(BINARY_MAGIC)
        decoded = decode_binary(encoded)
        assert decoded.negotiation["endianness"] == endian
        assert decoded.negotiation["checksums_verified"] is True
        assert [record.key for record in decoded.records] == ["a", "b"]
        assert decoded.records[0].written_at_us == 4_100_000_000_123_000
        assert decoded.records[0].expires_at_us == 4_200_000_000_000_000
        assert decoded.records[1].expires_at_us is None
        assert decoded.records[0].transformations == (
            "written_at_ms->written_at_us",
            "expires_at_ms->expires_at_us",
        )


def test_binary_bad_magic_and_crc_raise_format_invalid() -> None:
    record = Record("a", 1, 1, 4_100_000_000_000_000, None)
    encoded = bytearray(encode_binary([record]))
    broken_magic = bytes(encoded)
    broken_magic = b"BAD!" + broken_magic[4:]
    with pytest.raises(FormatInvalid) as magic_error:
        decode_binary(broken_magic)
    assert magic_error.value.code == "E_FORMAT_INVALID"

    encoded[-1] ^= 0xFF
    with pytest.raises(FormatInvalid) as crc_error:
        decode_binary(bytes(encoded))
    assert crc_error.value.code == "E_FORMAT_INVALID"
    assert crc_error.value.details["record_index"] == 0


def test_jsonl_ttl_precision_defaults_and_unknown_fields() -> None:
    payload = b"\n".join(
        [
            json.dumps(
                {
                    "key": "rel",
                    "value": 1,
                    "version": 3,
                    "written_at_ms": 4_100_000_000_000,
                    "ttl_ms": 500,
                    "vendor": "x",
                }
            ).encode(),
            json.dumps({"key": "none", "value": 2, "version": 1, "written_at_us": 5}).encode(),
        ]
    )
    decoded = decode_jsonl(payload, default_written_at_us=9_999)
    assert decoded.records[0].written_at_us == 4_100_000_000_000_000
    assert decoded.records[0].expires_at_us == 4_100_000_000_500_000
    assert decoded.records[0].unknown_fields == ("vendor",)
    assert decoded.records[0].extras == {"vendor": "x"}
    assert decoded.records[1].expires_at_us is None
    assert "expires_at_us" in decoded.records[1].filled_fields
    assert decoded.negotiation["unknown_fields"] == {"vendor": 1}
    assert decoded.negotiation["expiry_semantics"] == "relative_ttl"

    exported = encode_jsonl([decoded.records[0].to_record()])
    exported_obj = json.loads(exported)
    assert exported_obj["vendor"] == "x"


def test_jsonl_missing_timestamp_uses_default_and_is_reported() -> None:
    decoded = decode_jsonl(b'{"key":"k","value":1,"version":1}\n', default_written_at_us=1234)
    assert decoded.records[0].written_at_us == 1234
    assert set(decoded.records[0].filled_fields) == {"written_at_us", "expires_at_us"}


def test_jsonl_rejects_ambiguous_expiry_and_bad_late_line() -> None:
    bad = json.dumps({"key": "k", "value": 1, "version": 1, "expires_at_us": 10, "ttl_ms": 1}).encode()
    with pytest.raises(FormatInvalid):
        decode_jsonl(bad, default_written_at_us=1)
    two_lines = b'{"key":"ok","value":1,"version":1,"written_at_us":2}\nnot-json\n'
    with pytest.raises(FormatInvalid):
        decode_jsonl(two_lines, default_written_at_us=1)


def test_detection_and_expected_format_mismatch() -> None:
    binary = encode_binary([Record("a", 1, 1, 4_100_000_000_000_000, None)])
    jsonl = b'{"key":"a","value":1,"version":1,"written_at_us":1}\n'
    assert detect_format(binary) == "compact_binary"
    assert detect_format(jsonl) == "json_lines"
    assert detect_format(b"\xef\xbb\xbf" + jsonl) == "json_lines"
    with pytest.raises(FormatInvalid):
        decode_external(jsonl, expected_format="compact_binary", default_written_at_us=1)
    with pytest.raises(FormatInvalid):
        detect_format(b"plain text")
    empty_json = decode_external(b"", expected_format="json_lines", default_written_at_us=1)
    assert empty_json.records == ()
    assert empty_json.negotiation["detected_by"] == "expected-format:empty-jsonl"


def test_binary_header_count_must_match_trailing_data() -> None:
    encoded = bytearray(encode_binary([Record("a", 1, 1, 4_100_000_000_000_000, None)]))
    struct.pack_into("<I", encoded, 8, 0)
    with pytest.raises(FormatInvalid):
        decode_binary(bytes(encoded))
