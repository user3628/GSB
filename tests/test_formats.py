from __future__ import annotations

import json
from pathlib import Path

import pytest

from diaglab.errors import BusinessError
from diaglab.formats import (
    decode_bundle,
    encode_binary,
    encode_jsonl,
)
from diaglab.normalize import normalize_bundle


SAMPLES = Path(__file__).resolve().parents[1] / "samples"


def normalized_sample(name: str):
    bundle = decode_bundle((SAMPLES / name).read_bytes())
    return bundle, normalize_bundle(bundle, "truncate")


def test_binary_both_endian_round_trip_is_field_exact():
    bundle, normalized = normalized_sample("binary_v2.bin")
    assert bundle.report.decision == "accept"
    for endian in ("little", "big"):
        encoded = encode_binary(normalized.records, version=2, endian=endian)
        assert encoded[:4] == b"DXPK"
        assert encoded[4] == 2
        assert encoded[5] in (0x4C, 0x42)
        decoded = normalize_bundle(decode_bundle(encoded))
        assert [record.core_dict() for record in decoded.records] == [
            record.core_dict() for record in normalized.records
        ]


def test_jsonl_and_binary_round_trip_cross_format():
    _, normalized = normalized_sample("binary_v2.bin")
    json_bundle = decode_bundle(encode_jsonl(normalized.records))
    json_normalized = normalize_bundle(json_bundle)
    assert [record.core_dict() for record in json_normalized.records] == [
        record.core_dict() for record in normalized.records
    ]
    binary_again = normalize_bundle(decode_bundle(encode_binary(json_normalized.records)))
    assert [record.core_dict() for record in binary_again.records] == [
        record.core_dict() for record in normalized.records
    ]


def test_v1_binary_degrade_defaults_are_declared_and_audited():
    bundle, normalized = normalized_sample("binary_v1_degraded.bin")
    assert bundle.report.decision == "degrade_accept"
    assert bundle.report.version == 1
    degradation = normalized.report["degradation"]
    assert degradation["missing_rtt_count"] == 2
    assert degradation["missing_payload_count"] == 2
    assert degradation["defaults"] == {"rtt_us": 0, "payload_bytes": 0}
    assert normalized.records[0].rtt_us == 0
    assert normalized.records[0].rtt_missing is True
    assert normalized.records[1].payload_bytes == 0
    assert normalized.records[1].payload_missing is True


def test_v1_jsonl_degrade_and_time_rounding_audit():
    bundle, normalized = normalized_sample("jsonl_v1_degraded.jsonl")
    assert bundle.report.decision == "degrade_accept"
    assert normalized.report["timestamp_policy"] == "truncate"
    assert normalized.report["rounding"]["records_with_sub_ms"] == 3
    assert normalized.report["rounding"]["cumulative_error_us"] == -1849
    assert all(row["rounding_bit"] == 1 for row in normalized.report["records"])


def test_jsonl_units_are_converted_to_internal_integers():
    _, normalized = normalized_sample("jsonl_v2.jsonl")
    first = normalized.records[0]
    assert first.rtt_us == 2_000
    assert first.payload_bytes == 1_024
    assert normalized.report["unit_conversion"]["internal_rtt_unit"] == "us"
    assert normalized.report["unit_conversion"]["internal_payload_unit"] == "B"


def test_timestamp_policies_and_cumulative_error():
    item = {
        "format": "jsonl",
        "version": 2,
        "timestamp_us": 2_000_500,
        "probe_id": "rounding-1",
        "path_node": "rounding",
        "protocol": "tcp",
        "success": True,
        "failure_type": "none",
        "rtt_value": 1200,
        "rtt_unit": "us",
        "payload_value": 32,
        "payload_unit": "B",
    }
    data = (json.dumps(item) + "\n").encode()
    expectations = {
        "truncate": (2000, -500, 1),
        "trunc": (2000, -500, 1),
        "floor": (2000, -500, 1),
        "ceil": (2001, 500, 1),
        "half_up": (2001, 500, 1),
        "round": (2001, 500, 1),
    }
    for policy, (timestamp_ms, error_us, rounding_bit) in expectations.items():
        normalized = normalize_bundle(decode_bundle(data), policy)
        record = normalized.records[0]
        assert record.timestamp_ms == timestamp_ms
        assert record.rounding_error_us == error_us
        assert record.rounding_bit == rounding_bit
        assert normalized.report["rounding"]["cumulative_error_us"] == error_us


@pytest.mark.parametrize(
    "name,code",
    [
        ("binary_bad_crc.bin", "E_FRAME_CORRUPT"),
        ("binary_bad_length.bin", "E_FRAME_CORRUPT"),
        ("binary_v3_unsupported.bin", "E_FORMAT_UNSUPPORTED"),
        ("jsonl_v3_unsupported.jsonl", "E_FORMAT_UNSUPPORTED"),
        ("jsonl_unknown_required.jsonl", "E_FORMAT_UNSUPPORTED"),
        ("jsonl_unit_invalid.jsonl", "E_UNIT_INVALID"),
    ],
)
def test_corrupt_and_unsupported_samples_return_business_codes(name: str, code: str):
    with pytest.raises(BusinessError) as caught:
        bundle = decode_bundle((SAMPLES / name).read_bytes())
        normalize_bundle(bundle)
    assert caught.value.code == code


def test_unit_overflow_rejects_whole_batch():
    items = []
    for index in range(2):
        items.append(
            {
                "format": "jsonl",
                "version": 2,
                "timestamp_us": 2_000_000 + index,
                "probe_id": f"overflow-{index}",
                "path_node": "overflow",
                "protocol": "tcp",
                "success": True,
                "failure_type": "none",
                "rtt_value": 100,
                "rtt_unit": "us",
                "payload_value": 2**32,
                "payload_unit": "B",
            }
        )
    data = ("\n".join(json.dumps(item) for item in items) + "\n").encode()
    with pytest.raises(BusinessError) as caught:
        normalize_bundle(decode_bundle(data))
    assert caught.value.code == "E_UNIT_INVALID"
    assert caught.value.details["ordinal"] == 0
    assert caught.value.details["max_value"] == 2**32 - 1


def test_binary_header_unknown_required_flag_is_rejected():
    data = bytearray((SAMPLES / "binary_v2.bin").read_bytes())
    data[8:10] = (1).to_bytes(2, "little")
    with pytest.raises(BusinessError) as caught:
        decode_bundle(bytes(data))
    assert caught.value.code == "E_FORMAT_UNSUPPORTED"
    assert "required_flags" in caught.value.details

