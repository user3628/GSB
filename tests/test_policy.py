from __future__ import annotations

import copy

import pytest

from app.errors import AppError, ErrorCode
from app.policy import (
    SCHEMA_V1,
    SCHEMA_V2,
    compute_checksum,
    parse_hierarchical_document,
    parse_legacy_document,
    parse_policy_document,
    verify_checksum,
)
from tests.conftest import T0, T1, sample


def test_parse_both_fixed_samples():
    hierarchy = parse_policy_document(sample("policies_hierarchical_v2.json"))
    assert hierarchy.version == 2
    assert hierarchy.source_schema == SCHEMA_V2
    assert hierarchy.checksum.startswith("sha256:")
    assert hierarchy.chain("alice") == ("alice", "team-alpha", "platform")
    assert hierarchy.tenants["alice"].quota.cpu_limit == 4500

    legacy = parse_policy_document(sample("policies_flat_v1.json"))
    assert legacy.version == 1
    assert legacy.source_schema == SCHEMA_V1
    assert legacy.chain("dave") == ("dave", "team-beta", "platform")
    assert legacy.tenants["team-beta"].quota.cpu_limit == 1200
    assert legacy.tenants["edge"].rate_limit.capacity == 1


def test_checksum_rejects_modified_policy():
    document = sample("policies_hierarchical_v2.json")
    document["tenants"][0]["quota"]["cpu_millicores"] += 1
    with pytest.raises(AppError) as exc:
        parse_hierarchical_document(document)
    assert exc.value.code == ErrorCode.E_SPEC_INVALID
    assert exc.value.details["field"] == "checksum"


def test_checksum_is_order_independent_and_content_sensitive():
    document = sample("policies_hierarchical_v2.json")
    reordered = copy.deepcopy(document)
    reordered["tenants"] = list(reversed(reordered["tenants"]))
    assert compute_checksum(reordered) == document["checksum"]
    verify_checksum(reordered)
    reordered["tenants"][0]["rate_limit"]["capacity"] += 1
    assert compute_checksum(reordered) != document["checksum"]


@pytest.mark.parametrize(
    "mutator, detail_key",
    [
        (
            lambda d: d["tenants"].append(copy.deepcopy(d["tenants"][0])),
            "tenant_id",
        ),
        (
            lambda d: d["tenants"][0]["quota"].__setitem__("cpu_millicores", 0),
            "field",
        ),
        (
            lambda d: d["tenants"][0].__setitem__("parent_id", "does-not-exist"),
            "missing_parent_id",
        ),
        (
            lambda d: d["tenants"][0].__setitem__("parent_id", "alice"),
            "tenant_id",
        ),
    ],
)
def test_invalid_policy_batches_are_rejected(mutator, detail_key):
    document = sample("policies_hierarchical_v2.json")
    mutator(document)
    with pytest.raises(AppError) as exc:
        parse_hierarchical_document(document)
    assert exc.value.code == ErrorCode.E_SPEC_INVALID
    assert detail_key in exc.value.details


def test_parent_depth_limit():
    tenants = []
    for index in range(6):
        tenants.append(
            {
                "tenant_id": f"t{index}",
                "parent_id": None if index == 0 else f"t{index - 1}",
                "quota": {
                    "cpu_millicores": 100,
                    "memory_bytes": 100,
                    "burst": {"cpu_millicores": 0, "memory_bytes": 0},
                },
                "rate_limit": {
                    "refill_per_second": 1,
                    "capacity": 1,
                    "initial_tokens": 1,
                },
                "lifecycle": {"start_ms": T0, "end_ms": T1},
            }
        )
    document = {
        "schema_version": SCHEMA_V2,
        "version": 9,
        "generated_at_ms": T0,
        "tenants": tenants,
    }
    document["checksum"] = compute_checksum(document)
    with pytest.raises(AppError) as exc:
        parse_hierarchical_document(document)
    assert exc.value.code == ErrorCode.E_SPEC_INVALID
    assert exc.value.details["max_depth"] == 4
    assert exc.value.details["depth"] == 5


def test_legacy_dup_and_unknown_deprecated_fields_still_parse():
    legacy = parse_legacy_document(sample("policies_flat_v1.json"))
    assert len(legacy.tenants) == 8
    assert legacy.tenants["platform"].deprecated_fields["deprecated_burst_tokens"] == 120
    # Unknown deprecated fields are retained for migration review, not invented.
    document = sample("policies_flat_v1.json")
    document["tenants"][0]["deprecated_unknown_qos"] = "platinum"
    document["checksum"] = compute_checksum(document)
    parsed = parse_legacy_document(document)
    assert parsed.tenants["platform"].deprecated_fields["deprecated_unknown_qos"] == "platinum"
