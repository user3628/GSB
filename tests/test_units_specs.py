from __future__ import annotations

import pytest

from quota_scheduler.errors import (
    E_QUOTA_GROUP_UNKNOWN,
    E_SPEC_INVALID,
    SchedulerError,
)
from quota_scheduler.specs import load_bundle, parse_memory_bytes, parse_millis
from quota_scheduler.units import MAX_INTERNAL_INT

from conftest import write_bundle


def test_dimension_parsing() -> None:
    assert parse_memory_bytes("40Gi", "f") == 40 * (1 << 30)
    assert parse_memory_bytes("512Mi", "f") == 512 * (1 << 20)
    assert parse_memory_bytes("1.5Gi", "f") == 3 * (1 << 29)
    assert parse_memory_bytes("100MB", "f") == 100_000_000
    assert parse_memory_bytes(12, "f") == 12
    assert parse_millis("250m", "f") == 250
    assert parse_millis("1s", "f") == 1000
    assert parse_millis(42, "f") == 42


@pytest.mark.parametrize(
    ("value", "field"),
    [
        ("-1Gi", "nodes[0].memory"),
        ("12Xi", "nodes[0].memory"),
        (f"{MAX_INTERNAL_INT + 1}Gi", "nodes[0].memory"),
        (1.5, "nodes[0].memory"),
        ("NaN", "nodes[0].memory"),
    ],
)
def test_memory_parse_errors_carry_field(value: object, field: str) -> None:
    with pytest.raises(SchedulerError) as caught:
        parse_memory_bytes(value, field)
    assert caught.value.code == E_SPEC_INVALID
    assert caught.value.field == field


def test_unknown_quota_group_on_node(tmp_path) -> None:
    samples = write_bundle(
        tmp_path / "s",
        groups=[{"group_id": "known", "memory_limit": "10Gi", "compute_limit": "1000m"}],
        nodes=[
            {
                "node_id": "n1",
                "total_memory": "8Gi",
                "available_compute": "1000m",
                "occupied_memory": "0",
                "quota_group": "missing",
                "healthy": True,
            }
        ],
    )
    with pytest.raises(SchedulerError) as caught:
        load_bundle(samples)
    assert caught.value.code == E_QUOTA_GROUP_UNKNOWN
    assert caught.value.field == "nodes[0].quota_group"


def test_unknown_quota_group_on_request(tmp_path) -> None:
    samples = write_bundle(
        tmp_path / "s",
        requests=[
            {
                "request_id": "r1",
                "service_name": "s",
                "model_id": "m1",
                "memory_demand": "1Gi",
                "samples_per_batch": 1,
                "expected_latency": "1s",
                "priority": 1,
                "quota_group": "missing",
            }
        ],
    )
    with pytest.raises(SchedulerError) as caught:
        load_bundle(samples)
    assert caught.value.code == E_QUOTA_GROUP_UNKNOWN
    assert caught.value.field == "requests[0].quota_group"


def test_node_occupancy_cannot_exceed_capacity(tmp_path) -> None:
    samples = write_bundle(
        tmp_path / "s",
        nodes=[
            {
                "node_id": "n1",
                "total_memory": "8Gi",
                "available_compute": "1000m",
                "occupied_memory": "9Gi",
                "quota_group": "g1",
                "healthy": True,
            }
        ],
    )
    with pytest.raises(SchedulerError) as caught:
        load_bundle(samples)
    assert caught.value.code == E_SPEC_INVALID
    assert caught.value.field == "nodes[0].occupied_memory"

