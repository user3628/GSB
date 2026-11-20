from __future__ import annotations

import json
from pathlib import Path

import pytest

from replication_guard.errors import AppError
from replication_guard.formats import (
    decode_policies,
    encode_binary,
    encode_jsonl,
    encode_jsonl_shards,
)
from replication_guard.models import Policy, normalize_policy


SAMPLES = Path(__file__).resolve().parents[1] / "samples"


def _internal(policies: list[Policy]) -> list[dict]:
    return [policy.to_internal_dict() for policy in sorted(policies, key=lambda item: item.slave_id)]


def test_binary_v2_roundtrip_is_field_equivalent(sample_policies: list[Policy]) -> None:
    encoded, truncations = encode_binary(sample_policies, version=2)
    decoded = decode_policies(encoded)
    assert truncations == []
    assert decoded.negotiation["decision"] == "ACCEPT"
    assert decoded.negotiation["inputVersion"] == 2
    assert _internal(decoded.policies) == _internal(sample_policies)


def test_jsonl_roundtrip_is_field_equivalent(sample_policies: list[Policy]) -> None:
    encoded = encode_jsonl(sample_policies)
    decoded = decode_policies(encoded.encode("utf-8"))
    assert decoded.negotiation["decision"] == "ACCEPT"
    assert _internal(decoded.policies) == _internal(sample_policies)


def test_jsonl_shards_roundtrip(sample_policies: list[Policy]) -> None:
    shards = encode_jsonl_shards(sample_policies, shard_count=3)
    assert len(shards) == 3
    combined = "\n".join(shards.values())
    decoded = decode_policies(combined, "jsonl")
    assert decoded.shard_count == 3
    assert _internal(decoded.policies) == _internal(sample_policies)


def test_binary_v1_degraded_accept_and_declared_second_truncation() -> None:
    policy = normalize_policy(
        {
            "slave_id": "legacy-time",
            "replication_group": "g",
            "delay_threshold_ms": 100,
            "tolerable_delay_ms": 300,
            "expected_version": "5.7.44",
            "degradation_action": "READ_ONLY",
            "priority": 5,
            "forward_compatible": True,
            "policy_version": 9,
            "updated_at_ms": 1763000000123,
        }
    )
    encoded, truncations = encode_binary([policy], version=1)
    assert truncations == [
        {
            "policyId": "legacy-time",
            "field": "updated_at_ms",
            "from": 1763000000123,
            "to": 1763000000000,
            "rule": "FLOOR_TO_SECOND_MULTIPLE_OF_1000",
        }
    ]
    decoded = decode_policies(encoded)
    result = decoded.policies[0]
    assert decoded.negotiation["decision"] == "DEGRADED_ACCEPT"
    assert decoded.negotiation["timePrecision"] == "seconds"
    assert result.updated_at_ms == 1763000000000
    assert result.forward_compatible is False
    assert result.policy_version == 1
    degradation_fields = {item["field"] for item in decoded.degradations}
    assert {"forward_compatible", "policy_version"} <= degradation_fields
    assert decoded.time_normalizations[0]["rule"] == "SECONDS_MULTIPLY_1000"


def test_binary_crc_corruption_is_rejected(sample_policies: list[Policy]) -> None:
    encoded, _ = encode_binary(sample_policies, version=2)
    corrupted = bytearray(encoded)
    corrupted[20] ^= 0xFF
    with pytest.raises(AppError) as captured:
        decode_policies(bytes(corrupted))
    assert captured.value.code == "E_FORMAT_MALFORMED"
    assert "CRC32" in captured.value.message


def test_high_version_unknown_required_field_is_incompatible() -> None:
    payload = (SAMPLES / "policies_future_v2.jsonl").read_bytes()
    with pytest.raises(AppError) as captured:
        decode_policies(payload)
    assert captured.value.code == "E_FORMAT_INCOMPATIBLE"
    assert captured.value.details["unknown_required_fields"] == ["tenantRoutingKey"]


def test_jsonl_missing_optional_fields_records_defaults() -> None:
    line = json.dumps(
        {
            "policyId": "old-jsonl",
            "replicationGroup": "g",
            "delayThresholdMs": 100,
            "tolerableDelayMs": 200,
            "expectedVersion": "8.0.1",
            "priority": 7,
            "updatedAtMs": 1763000000123,
        }
    )
    decoded = decode_policies(line)
    policy = decoded.policies[0]
    assert policy.forward_compatible is False
    assert policy.policy_version == 1
    assert policy.degradation_action == "NONE"
    fields = {item["field"] for item in decoded.degradations}
    assert {"forwardCompatible", "policyVersion", "degradationAction"} <= fields


def test_jsonl_declared_known_required_field_cannot_be_silently_defaulted() -> None:
    payload = "\n".join(
        [
            json.dumps(
                {
                    "_meta": {
                        "format": "policy-jsonl",
                        "version": 2,
                        "requiredFields": ["forwardCompatible"],
                    }
                }
            ),
            json.dumps(
                {
                    "policyId": "high-known-required",
                    "replicationGroup": "g",
                    "delayThresholdMs": 100,
                    "tolerableDelayMs": 200,
                    "expectedVersion": "8.0.1",
                    "degradationAction": "MONITOR",
                    "priority": 1,
                    "policyVersion": 1,
                    "updatedAtMs": 1763000000123,
                }
            ),
        ]
    )
    with pytest.raises(AppError) as captured:
        decode_policies(payload)
    assert captured.value.code == "E_FORMAT_INCOMPATIBLE"
    assert captured.value.details["missing_required_fields"] == ["forwardCompatible"]

