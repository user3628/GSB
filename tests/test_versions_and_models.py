from __future__ import annotations

from datetime import datetime

import pytest

from replication_guard.errors import AppError
from replication_guard.models import normalize_observation, normalize_policy
from replication_guard.versions import parse_version


def test_version_segments_preserve_raw_and_compare_numerically() -> None:
    version = parse_version("8.0.32-24")
    assert version.raw == "8.0.32-24"
    assert version.segments == (8, 0, 32, 24)
    assert parse_version("8.0.32-24").compare(parse_version("8.0.32")) > 0
    assert parse_version("8.0.32").compare(parse_version("8.0.32.0")) == 0
    assert parse_version("v5.7.44").segments == (5, 7, 44)


@pytest.mark.parametrize("bad", ["", "eight", "8..0", "8.0-", "8.0+1", None, 8])
def test_malformed_version_has_business_code(bad) -> None:
    with pytest.raises(AppError) as captured:
        parse_version(bad)
    assert captured.value.code == "E_VERSION_MALFORMED"


def test_policy_dimension_validation() -> None:
    base = {
        "slave_id": "s1",
        "replication_group": "g1",
        "delay_threshold_ms": 100,
        "tolerable_delay_ms": 100,
        "expected_version": "8.0.32-24",
        "degradation_action": "read_only",
        "priority": 10,
        "updated_at_ms": 1763000000123,
    }
    policy = normalize_policy(base)
    assert policy.degradation_action == "READ_ONLY"
    with pytest.raises(AppError) as captured:
        normalize_policy({**base, "delay_threshold_ms": -1})
    assert captured.value.code == "E_POLICY_INVALID"
    with pytest.raises(AppError) as captured:
        normalize_policy({**base, "tolerable_delay_ms": 99})
    assert captured.value.code == "E_POLICY_INVALID"
    with pytest.raises(AppError) as captured:
        normalize_policy({**base, "delay_threshold_ms": 1.5})
    assert captured.value.code == "E_POLICY_INVALID"


def test_observation_normalization_parses_iso_time() -> None:
    observation = normalize_observation(
        {
            "slaveId": "s1",
            "sampledAt": "2026-11-20T12:00:00+08:00",
            "binlogGap": "42",
            "delayMs": 321,
            "reportedVersion": "8.0.32-24",
        }
    )
    assert observation.binlog_gap == 42
    assert observation.delay_ms == 321
    assert observation.reported_version == "8.0.32-24"
    assert observation.sampled_at_ms == int(
        datetime.fromisoformat("2026-11-20T12:00:00+08:00").timestamp() * 1000
    )

