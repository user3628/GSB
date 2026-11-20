from __future__ import annotations

from pathlib import Path

import pytest

from msmq.errors import MQError
from msmq.formats import decode_auto, encode_compact, encode_jsonl

SAMPLES = Path(__file__).resolve().parents[1] / "artifacts" / "samples"


def test_binary_and_jsonl_are_semantically_equivalent() -> None:
    compact = decode_auto((SAMPLES / "current_v2.bin").read_bytes())
    jsonl = decode_auto((SAMPLES / "current_v1.jsonl").read_bytes())
    assert compact.decision == "accept"
    assert jsonl.decision == "accept"
    assert [message.semantic_dict() for message in compact.messages] == [
        message.semantic_dict() for message in jsonl.messages
    ]
    assert compact.queues[0].to_dict() == jsonl.queues[0].to_dict()


def test_binary_crc_rejects_corruption() -> None:
    data = bytearray((SAMPLES / "current_v2.bin").read_bytes())
    data[-1] ^= 0xFF
    with pytest.raises(MQError) as caught:
        decode_auto(bytes(data))
    assert caught.value.code == "E_FORMAT_INVALID"
    assert "CRC32" in caught.value.message


def test_legacy_v1_fills_defaults_and_normalizes_to_milliseconds() -> None:
    batch = decode_auto((SAMPLES / "legacy_v1.bin").read_bytes())
    message = batch.messages[0]
    assert batch.decision == "degrade_accept"
    assert message.extensions == {}
    assert message.content_type == "text/plain"
    assert message.expires_at_us - message.produced_at_us == 60_000_000
    assert message.produced_at_us % 1_000 == 0
    assert batch.decode_report["sub_ms_normalizations"][0]["truncated_us"] == 123
    assert any(
        fill["field"] == "expires_at_us" and fill["reason"] == "missing_in_v1"
        for fill in batch.decode_report["degradations"]
    )


def test_strict_time_policy_rejects_sub_millisecond_input() -> None:
    with pytest.raises(MQError) as caught:
        decode_auto((SAMPLES / "legacy_v1.bin").read_bytes(), sub_ms_policy="reject")
    assert caught.value.code == "E_TIME_PRECISION_UNSUPPORTED"


def test_future_required_version_is_incompatible() -> None:
    with pytest.raises(MQError) as caught:
        decode_auto((SAMPLES / "future_required_v3.bin").read_bytes())
    assert caught.value.code == "E_FORMAT_INCOMPATIBLE"
    assert caught.value.details["reason"] == "unknown_required_extension"


def test_capacity_errors_are_explicit() -> None:
    with pytest.raises(MQError) as batch_error:
        decode_auto((SAMPLES / "oversize_batch.jsonl").read_bytes())
    assert batch_error.value.code == "E_BATCH_SPLIT_REQUIRED"
    assert batch_error.value.details["count"] == 65
    with pytest.raises(MQError) as body_error:
        decode_auto((SAMPLES / "oversize_body.jsonl").read_bytes())
    assert body_error.value.code == "E_RECORD_TOO_LARGE"
    assert body_error.value.details["body_bytes"] == 4097


def test_unknown_optional_extensions_survive_reexport() -> None:
    batch = decode_auto((SAMPLES / "optional_v2.bin").read_bytes())
    target = next(message for message in batch.messages if message.message_id == "msg-0001")
    assert target.extensions["vendor_trace_id"] == "vendor-0001"
    reexported = encode_compact(batch.messages, batch.queues)
    decoded_again = decode_auto(reexported)
    target_again = next(message for message in decoded_again.messages if message.message_id == "msg-0001")
    assert target_again.extensions["vendor_trace_id"] == "vendor-0001"
    jsonl = encode_jsonl(batch.messages, batch.queues)
    decoded_jsonl = decode_auto(jsonl)
    target_jsonl = next(message for message in decoded_jsonl.messages if message.message_id == "msg-0001")
    assert target_jsonl.extensions["vendor_trace_id"] == "vendor-0001"


def test_format_mismatch_is_detected() -> None:
    with pytest.raises(MQError) as caught:
        decode_auto(
            (SAMPLES / "current_v2.bin").read_bytes(), expected_format="jsonl"
        )
    assert caught.value.code == "E_FORMAT_MISMATCH"


def test_jsonl_accepts_message_id_as_id_alias() -> None:
    document = (
        '{"_type":"mq_manifest","format":"jsonl","version":1,"queues":'
        '[{"name":"alias","semantics":"at_least_once","visibility_timeout_ms":1000}]}\n'
        '{"message_id":"alias-1","queue":"alias","body":"body","produced_at_ms":4102444800000,'
        '"ttl_ms":60000}\n'
    ).encode("utf-8")
    batch = decode_auto(document)
    assert batch.messages[0].message_id == "alias-1"

