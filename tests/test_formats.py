from __future__ import annotations

import json
import struct

import pytest

from svc_registry.errors import E_FORMAT_CORRUPT, E_FORMAT_UNSUPPORTED, BusinessError
from svc_registry.formats import decode_binary, decode_jsonl, decode_snapshot, encode_binary, encode_jsonl
from svc_registry.formats.binary import HEADER_FIXED, MAGIC
from svc_registry.models import ProbeResultRecord, RegistrationRecord, Snapshot
from svc_registry.policies import TimePolicy
from svc_registry.normalization import normalize_probe_results, normalize_registrations


def registration(index: int = 0, **overrides: object) -> RegistrationRecord:
    values = {
        "service_name": "orders",
        "instance_id": f"orders-{index}",
        "endpoint": "http://127.0.0.1:28765/health/ok",
        "weight": 733,
        "weight_encoding": "permille",
        "lease_ttl_ms": 5000,
        "probe_method": "http",
        "probe_params": {
            "timeout_ms": 800,
            "expected_status": 200,
            "expected_body_contains": "OK",
        },
        "registered_at": 1_700_000_000_123 + index,
        "time_unit": "ms",
        "lease_version": 2,
        "probe_window_ms": 600,
        "last_heartbeat_at": 1_700_000_000_123 + index,
        "source_version": 2,
    }
    values.update(overrides)
    return RegistrationRecord(**values)  # type: ignore[arg-type]


def test_binary_roundtrip_little_big_and_optional_bitmap() -> None:
    snapshot = Snapshot(kind="registration", version=2, records=(registration(0), registration(1)))
    for endianness in ("little", "big"):
        encoded = encode_binary(snapshot, endianness=endianness)
        assert encoded[:4] == MAGIC
        assert encoded[4] == 2
        assert encoded[5] == (1 if endianness == "little" else 2)
        decoded = decode_binary(encoded)
        assert decoded.version == 2
        assert decoded.endianness == endianness
        assert decoded.records == snapshot.records

    missing_optional = registration(2, probe_window_ms=None, last_heartbeat_at=None)
    encoded = encode_binary(Snapshot("registration", 2, (missing_optional,)))
    first_payload = encoded[HEADER_FIXED.size + 4 : HEADER_FIXED.size + 12]
    bitmap = int.from_bytes(first_payload[:2], "little")
    assert bitmap & 0b0100 == 0
    assert bitmap & 0b1000 == 0
    assert decode_binary(encoded).records[0].probe_window_ms is None


def test_binary_crc_corruption_reports_exact_record_index() -> None:
    snapshot = Snapshot("registration", 2, (registration(0), registration(1)))
    encoded = bytearray(encode_binary(snapshot))
    offset = HEADER_FIXED.size + 12
    for _ in range(2):
        length = struct.unpack("<I", encoded[offset : offset + 4])[0]
        payload_start = offset + 4
        if _ == 1:
            encoded[payload_start] ^= 0xFF
        offset = payload_start + length + 4
    with pytest.raises(BusinessError) as caught:
        decode_binary(bytes(encoded))
    assert caught.value.code == E_FORMAT_CORRUPT
    assert caught.value.details["record_index"] == 1
    assert caught.value.details["record_number"] == 2


def test_binary_truncated_record_length_is_not_silently_skipped() -> None:
    encoded = encode_binary(Snapshot("registration", 2, (registration(0), registration(1))))
    with pytest.raises(BusinessError) as caught:
        decode_binary(encoded[:-7])
    assert caught.value.code == E_FORMAT_CORRUPT
    assert caught.value.details["record_index"] == 1


def test_jsonl_shards_roundtrip_and_corruption_index() -> None:
    snapshot = Snapshot("registration", 2, (registration(0), registration(1), registration(2)))
    shards = encode_jsonl(snapshot, shards=2)
    assert all(isinstance(shard, bytes) for shard in shards)
    decoded = decode_jsonl(shards)
    assert len(decoded.records) == 3
    assert decoded.records[1].registered_at == snapshot.records[1].registered_at * 1000
    assert decoded.records[1].time_unit == "us"

    broken = [shards[0], b'{"service_name":"broken"}\n']
    with pytest.raises(BusinessError) as caught:
        decode_jsonl(broken)
    assert caught.value.code == E_FORMAT_CORRUPT
    assert caught.value.details["record_index"] == len(decode_jsonl(shards[0]).records)


def test_binary_and_jsonl_are_semantically_equivalent_after_normalization() -> None:
    def canonical_snapshot(snapshot: Snapshot) -> Snapshot:
        normalized = normalize_registrations(
            list(snapshot.records),
            time_policy=TimePolicy(declared=True, truncation_rule="floor"),
        )
        return Snapshot(
            "registration",
            2,
            tuple(
                RegistrationRecord(
                    service_name=record.service_name,
                    instance_id=record.instance_id,
                    endpoint=record.endpoint,
                    weight=record.weight_permille,
                    weight_encoding="permille",
                    lease_ttl_ms=record.lease_ttl_ms,
                    probe_method=record.probe_method,
                    probe_params=record.probe_params,
                    registered_at=record.registered_at_ms,
                    time_unit="ms",
                    lease_version=record.lease_version,
                    probe_window_ms=record.probe_window_ms,
                    last_heartbeat_at=record.last_heartbeat_at_ms,
                    source_version=2,
                )
                for record in normalized.records
            ),
        )

    source = Snapshot("registration", 2, (registration(0), registration(1)))
    binary_decoded = decode_binary(encode_binary(source, endianness="big"))
    jsonl_decoded = decode_jsonl(encode_jsonl(canonical_snapshot(binary_decoded), shards=2))
    binary_again = decode_binary(encode_binary(canonical_snapshot(jsonl_decoded), endianness="little"))
    left = normalize_registrations(list(binary_decoded.records)).records
    right = normalize_registrations(list(binary_again.records)).records
    assert [record.to_dict() for record in left] == [record.to_dict() for record in right]


def test_probe_result_snapshot_roundtrip() -> None:
    record = ProbeResultRecord(
        service_name="orders",
        instance_id="orders-0",
        endpoint="http://127.0.0.1:28765/health/ok",
        weight=733,
        weight_encoding="permille",
        probe_method="http",
        probe_params={"timeout_ms": 800, "expected_status": 200, "expected_body_contains": "OK"},
        checked_at=1_700_000_000_999,
        time_unit="ms",
        success=True,
        status="UP",
        latency_ms=4,
        outcome="http_expected_marker",
        lease_version=2,
        probe_window_ms=600,
        detail='{"http_status":200}',
    )
    snapshot = Snapshot("probe_result", 2, (record,))
    assert decode_binary(encode_binary(snapshot)).records == snapshot.records
    assert normalize_probe_results(
        list(decode_jsonl(encode_jsonl(snapshot)).records),
        time_policy=TimePolicy(declared=True, truncation_rule="floor"),
    ).records[0].status == "UP"


def test_newer_version_is_rejected_before_batch_changes() -> None:
    binary = bytearray(encode_binary(Snapshot("registration", 2, (registration(),))))
    binary[4] = 3
    with pytest.raises(BusinessError) as caught:
        decode_snapshot(bytes(binary))
    assert caught.value.code == E_FORMAT_UNSUPPORTED

    line = {
        "format_version": 3,
        "kind": "registration",
        "unknown_required_fields": ["future_hint"],
        "service_name": "orders",
        "instance_id": "future",
        "endpoint": "http://127.0.0.1:28765/health/ok",
        "weight": 900,
        "lease_ttl_ms": 5000,
        "probe_method": "http",
        "probe_params": {},
        "registered_at_us": 1_700_000_000_000_000,
    }
    with pytest.raises(BusinessError) as caught:
        decode_snapshot(json.dumps(line).encode())
    assert caught.value.code == E_FORMAT_UNSUPPORTED
