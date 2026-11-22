from __future__ import annotations

import pytest

from conftest import load_sample
from graph_bench.errors import SnapshotInvalid, SpecInvalid
from graph_bench.models import Resource, Snapshot
from graph_bench.validation import validate_resources, validate_snapshot_envelope


def res(rid: str, parents: list[str] | None = None, weight: int = 500) -> Resource:
    return Resource(id=rid, replicas=1, parents=tuple(parents or []), weight_milli=weight, desired_version="v")


def test_duplicate_ids_missing_parents_and_weight_range() -> None:
    resources = [
        res("a", weight=0),
        res("a", ["missing"], weight=1001),
        res("b", ["a"], weight=500),
    ]
    with pytest.raises(SpecInvalid) as info:
        validate_resources(resources)
    rules = {issue["rule"] for issue in info.value.details["issues"]}
    assert {"unique_id", "parent_exists", "weight_range"} <= rules


def test_self_parent_and_duplicate_parent_rejected() -> None:
    with pytest.raises(SpecInvalid) as info:
        validate_resources([res("a", ["a", "a"])])
    rules = {issue["rule"] for issue in info.value.details["issues"]}
    assert {"self_parent", "duplicate_parent"} <= rules


def test_increment_known_ids_allow_existing_parents() -> None:
    validate_resources([res("child", ["existing"])], known_ids={"existing"})


def test_snapshot_envelope_checksum_and_monotonic_version() -> None:
    good = Snapshot.from_dict(load_sample("snapshot_v1.json"))
    validate_snapshot_envelope(good, None)
    with pytest.raises(SnapshotInvalid) as info:
        validate_snapshot_envelope(good, 5)
    assert "version_not_monotonic" in info.value.details["problems"]

    bad = Snapshot.from_dict(load_sample("snapshot_bad_checksum_v2.json"))
    with pytest.raises(SnapshotInvalid) as info:
        validate_snapshot_envelope(bad, None)
    assert "checksum_mismatch" in info.value.details["problems"]
