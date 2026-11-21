from __future__ import annotations

import struct
import zlib

import pytest

from qos_workbench.codecs import (
    decode_binary,
    decode_jsonl,
    decode_policy_content,
    encode_binary,
    encode_jsonl,
)
from qos_workbench.errors import BusinessError


@pytest.mark.parametrize("endianness", ["little", "big"])
@pytest.mark.parametrize("version", [1, 2, 3])
def test_binary_roundtrip_versions_and_endianness(snapshot, version, endianness):
    content = encode_binary(snapshot, version=version, endianness=endianness)
    decoded = decode_binary(content)
    assert decoded.negotiation.detected_version == version
    assert decoded.import_report["endianness"] == endianness
    assert decoded.import_report["record_crc32"] == "verified_per_record"
    if version == 1:
        assert decoded.negotiation.decision == "degraded_accept"
        for policy in decoded.snapshot.policies:
            assert policy.pir_bps == policy.cir_bps
            assert policy.pbs_bytes == policy.cbs_bytes
        assert decoded.import_report["degrade_details"]
    else:
        assert decoded.snapshot.policies == snapshot.policies
        assert decoded.snapshot.snapshot_no == snapshot.snapshot_no
        assert decoded.snapshot.generated_at_us == snapshot.generated_at_us
        assert decoded.snapshot.format_version == version


def test_jsonl_roundtrip_and_cross_format_equivalence(snapshot):
    json_snapshot = decode_jsonl(encode_jsonl(snapshot)).snapshot
    little = decode_binary(encode_binary(snapshot, endianness="little")).snapshot
    big = decode_binary(encode_binary(snapshot, endianness="big")).snapshot
    assert json_snapshot == snapshot == little == big
    assert json_snapshot.checksum == little.checksum == big.checksum


def test_sample_files_normalize_identically(sample_paths):
    json_snapshot = decode_jsonl((sample_paths / "policies_v3.jsonl").read_bytes()).snapshot
    little = decode_binary((sample_paths / "policies_v3_little_endian.bin").read_bytes()).snapshot
    big = decode_binary((sample_paths / "policies_v3_big_endian.bin").read_bytes()).snapshot
    assert json_snapshot == little == big
    assert len(json_snapshot.policies) == 4

    json_v1 = decode_jsonl((sample_paths / "policies_v1.jsonl").read_bytes())
    binary_v1 = decode_binary((sample_paths / "policies_v1_little_endian.bin").read_bytes())
    assert json_v1.snapshot == binary_v1.snapshot
    assert json_v1.negotiation.decision == "degraded_accept"
    assert len(json_v1.import_report["degrade_details"]) == 4


def test_record_crc_corruption_is_rejected(snapshot):
    content = bytearray(encode_binary(snapshot))
    content[-6] ^= 0x01
    with pytest.raises(BusinessError) as caught:
        decode_binary(bytes(content))
    assert caught.value.code == "E_CRC_MISMATCH"
    assert caught.value.details["scope"] == "record"


def test_header_crc_corruption_is_rejected(snapshot):
    content = bytearray(encode_binary(snapshot))
    content[20] ^= 0x01
    with pytest.raises(BusinessError) as caught:
        decode_binary(bytes(content))
    assert caught.value.code == "E_CRC_MISMATCH"
    assert caught.value.details["scope"] == "header"


def test_high_format_version_is_rejected_before_partial_decode(snapshot):
    content = bytearray(encode_binary(snapshot))
    content[4] = 4
    with pytest.raises(BusinessError) as caught:
        decode_binary(bytes(content))
    assert caught.value.code == "E_FORMAT_UNSUPPORTED"
    assert caught.value.details["detected_version"] == 4
    assert caught.value.details["partial_import"] is False

    json_content = (
        b'{"format_version":4,"snapshot_no":9,"generated_at_ms":1,"queue_id":"x",'
        b'"cir_kbps":1,"pir_kbps":1,"cbs_kb":1,"pbs_kb":1,"priority":0,'
        b'"match_rule":"all","parent_queue_id":null,"timestamp_ms":1}\n'
    )
    with pytest.raises(BusinessError) as caught:
        decode_jsonl(json_content)
    assert caught.value.code == "E_FORMAT_UNSUPPORTED"


def test_unknown_required_record_bitmap_is_rejected(snapshot):
    content = bytearray(encode_binary(snapshot))
    record_start = 38
    record_len = struct.unpack_from("<I", content, record_start)[0]
    body_start = record_start + 4
    body_end = body_start + record_len
    bitmap = struct.unpack_from("<H", content, body_start)[0] | 0x0100
    struct.pack_into("<H", content, body_start, bitmap)
    crc = zlib.crc32(bytes(content[body_start:body_end])) & 0xFFFFFFFF
    struct.pack_into("<I", content, body_end, crc)
    with pytest.raises(BusinessError) as caught:
        decode_binary(bytes(content))
    assert caught.value.code == "E_FORMAT_UNSUPPORTED"
    assert caught.value.details["unknown_required_bits"] == 0x0100


def test_unknown_required_header_flag_is_rejected(snapshot):
    content = bytearray(encode_binary(snapshot))
    struct.pack_into("<H", content, 6, 0x8000)
    struct.pack_into("<I", content, 34, zlib.crc32(bytes(content[:34])) & 0xFFFFFFFF)
    with pytest.raises(BusinessError) as caught:
        decode_binary(bytes(content))
    assert caught.value.code == "E_FORMAT_UNSUPPORTED"
    assert caught.value.details["unknown_required_mask"] == 0x8000


def test_json_unknown_required_field_is_rejected():
    line = (
        b'{"format_version":3,"snapshot_no":1,"generated_at_ms":1,"queue_id":"a",'
        b'"cir_kbps":1,"pir_kbps":1,"cbs_kb":1,"pbs_kb":1,"priority":0,'
        b'"match_rule":"all","timestamp_ms":1,"required_fields":["future_metric"]}\n'
    )
    with pytest.raises(BusinessError) as caught:
        decode_jsonl(line)
    assert caught.value.code == "E_FORMAT_UNSUPPORTED"
    assert caught.value.details["unknown_required_fields"] == ["future_metric"]


def test_json_unit_overflow_is_rejected_before_snapshot_creation():
    line = (
        b'{"format_version":3,"snapshot_no":1,"generated_at_ms":1,"queue_id":"a",'
        b'"cir_kbps":20000000000000000,"pir_kbps":20000000000000000,"cbs_kb":1,'
        b'"pbs_kb":1,"priority":0,"match_rule":"all","timestamp_ms":1}\n'
    )
    with pytest.raises(BusinessError) as caught:
        decode_jsonl(line)
    assert caught.value.code == "E_UNIT_OVERFLOW"


def test_timestamp_truncation_reports_remainder_and_cumulative_error(snapshot):
    content = encode_jsonl(snapshot)
    decoded = decode_jsonl(content, timestamp_truncation="floor_ms")
    report = decoded.import_report["timestamp_normalization"]
    assert report["truncation_rule"] == "floor_ms"
    assert report["cumulative_rounding_error_us"] == "-1824"  # 4 records * -456 us
    assert all(item["rounding_applied"] for item in report["records"])
    assert all(item["rounding_bit"] == 1 for item in report["records"])
    assert all(item["rounding_remainder_us"] == "-456" for item in report["records"])


def test_duplicate_queue_last_occurrence_wins_and_is_reported():
    rows = []
    for priority, match_rule in ((1, "first"), (5, "last")):
        rows.append(
            (
                '{"format_version":3,"snapshot_no":1,"generated_at_ms":1,'
                f'"queue_id":"dup","cir_kbps":1000,"pir_kbps":2000,"cbs_kb":2,'
                f'"pbs_kb":4,"priority":{priority},"match_rule":"{match_rule}",'
                '"parent_queue_id":null,"timestamp_ms":1}'
            )
        )
    decoded = decode_jsonl(("\n".join(rows) + "\n").encode())
    assert len(decoded.snapshot.policies) == 1
    assert decoded.snapshot.policies[0].priority == 5
    assert decoded.snapshot.policies[0].match_rule == "last"
    duplicate_report = decoded.import_report["duplicate_resolution"]
    assert duplicate_report["duplicate_count"] == 1
    assert duplicate_report["duplicates"][0]["winner_source_index"] == 1


def test_format_detection_rejects_unknown_content():
    with pytest.raises(BusinessError) as caught:
        decode_policy_content(b"not-a-policy-format")
    assert caught.value.code == "E_FORMAT_INVALID"

