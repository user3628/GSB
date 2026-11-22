from __future__ import annotations

from pathlib import Path

import pytest

from svc_registry.errors import (
    E_TIME_POLICY_REQUIRED,
    E_WEIGHT_INVALID,
    E_WEIGHT_POLICY_REQUIRED,
    BusinessError,
)
from svc_registry.formats import decode_snapshot
from svc_registry.models import RegistrationRecord
from svc_registry.normalization import normalize_registrations
from svc_registry.policies import TimePolicy, WeightPolicy, convert_weight_to_permille


def record(**overrides: object) -> RegistrationRecord:
    values = {
        "service_name": "svc",
        "instance_id": "i",
        "endpoint": "http://127.0.0.1:28765/health/ok",
        "weight": 0.7334,
        "weight_encoding": "ratio",
        "lease_ttl_ms": 5000,
        "probe_method": "http",
        "probe_params": None,
        "registered_at": 1_700_000_000_123_456,
        "time_unit": "us",
        "source_version": 1,
    }
    values.update(overrides)
    return RegistrationRecord(**values)  # type: ignore[arg-type]


def test_legacy_sample_degrades_and_reports_each_rounding_and_truncation() -> None:
    snapshot, info, negotiation = decode_snapshot(Path("samples/legacy_v1.jsonl").read_bytes())
    assert negotiation["decision"] == "degraded_accept"
    normalized = normalize_registrations(
        list(snapshot.records),
        weight_policy=WeightPolicy(source_scale="ratio", rounding_rule="half_up", declared=True),
        time_policy=TimePolicy(truncation_rule="floor", declared=True),
    )
    assert [item.weight_permille for item in normalized.records] == [733, 500, 626]
    assert all(item.probe_params for item in normalized.records)
    assert normalized.records[0].registered_at_ms == 1_700_000_000_123
    details = normalized.details()
    assert len(details["weight_rounding"]) == 3
    assert details["weight_rounding"][:2] == [
        {
            "record_index": 0,
            "service_name": "catalog",
            "instance_id": "catalog-1",
            "source_weight": 0.7334,
            "source_encoding": "ratio",
            "weight_permille": 733,
            "rounding_rule": "half_up",
            "rounding_residue": 0.4,
        },
        {
            "record_index": 1,
            "service_name": "catalog",
            "instance_id": "catalog-2",
            "source_weight": 0.5,
            "source_encoding": "ratio",
            "weight_permille": 500,
            "rounding_rule": "half_up",
            "rounding_residue": 0.0,
        },
    ]
    assert {item["residue_us"] for item in details["time_truncation"]} == {456, 567, 678}
    assert {item["field"] for item in details["degradation_details"]} == {"probe_params", "probe_window_ms"}


def test_ratio_policy_is_mandatory() -> None:
    with pytest.raises(BusinessError) as caught:
        normalize_registrations([record()], time_policy=TimePolicy(declared=True))
    assert caught.value.code == E_WEIGHT_POLICY_REQUIRED


def test_mixed_float_and_fixed_weights_require_explicit_policy() -> None:
    fixed = record(weight=733, weight_encoding="permille", registered_at=1_700_000_000_000, time_unit="ms")
    with pytest.raises(BusinessError) as caught:
        normalize_registrations([fixed, record()], time_policy=TimePolicy(declared=True))
    assert caught.value.code == E_WEIGHT_POLICY_REQUIRED
    accepted = normalize_registrations(
        [fixed, record()],
        weight_policy=WeightPolicy(source_scale="mixed", rounding_rule="floor", declared=True),
        time_policy=TimePolicy(declared=True),
    )
    assert len(accepted.records) == 2


@pytest.mark.parametrize(
    ("value", "encoding"),
    [(-0.1, "ratio"), (1.1, "ratio"), (0.0, "ratio"), (0, "permille"), (1001, "permille"), (750.0, "permille")],
)
def test_invalid_weight_domains(value: object, encoding: str) -> None:
    policy = WeightPolicy(source_scale="ratio", rounding_rule="half_up", declared=True)
    with pytest.raises(BusinessError) as caught:
        convert_weight_to_permille(value, encoding, policy, record_index=4)  # type: ignore[arg-type]
    assert caught.value.code == E_WEIGHT_INVALID


def test_time_policy_is_mandatory_and_rules_handle_submillisecond() -> None:
    with pytest.raises(BusinessError) as caught:
        normalize_registrations(
            [record(weight=750, weight_encoding="permille")],
            time_policy=TimePolicy(),
        )
    assert caught.value.code == E_TIME_POLICY_REQUIRED
    floor = normalize_registrations(
        [record(weight=750, weight_encoding="permille")],
        time_policy=TimePolicy(truncation_rule="floor", declared=True),
    ).records[0]
    ceil = normalize_registrations(
        [record(weight=750, weight_encoding="permille")],
        time_policy=TimePolicy(truncation_rule="ceil", declared=True),
    ).records[0]
    half = normalize_registrations(
        [record(weight=750, weight_encoding="permille", registered_at=1_700_000_000_123_678)],
        time_policy=TimePolicy(truncation_rule="half_up", declared=True),
    ).records[0]
    assert floor.registered_at_ms == 1_700_000_000_123
    assert ceil.registered_at_ms == 1_700_000_000_124
    assert half.registered_at_ms == 1_700_000_000_124
