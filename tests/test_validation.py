from __future__ import annotations

import pytest

from qos_workbench.errors import BusinessError
from qos_workbench.models import Policy, PolicySnapshot
from qos_workbench.validation import validate_snapshot


def make_policy(queue_id, parent=None, cir=1000, pir=2000, cbs=1000, pbs=2000):
    return Policy(queue_id, cir, pir, cbs, pbs, 1, "all", parent, 1)


def test_dependency_cycle_reports_cycle_identifiers():
    snapshot = PolicySnapshot(
        3,
        1,
        1,
        (
            make_policy("a", "b"),
            make_policy("b", "c"),
            make_policy("c", "a"),
        ),
    )
    with pytest.raises(BusinessError) as caught:
        validate_snapshot(snapshot)
    assert caught.value.code == "E_DEPENDENCY_CYCLE"
    assert caught.value.details["cycle"] in (["a", "b", "c", "a"], ["a", "c", "b", "a"])
    assert caught.value.details["cycle_queue_ids"] == ["a", "b", "c"]


def test_child_rate_exceeded_reports_both_values():
    snapshot = PolicySnapshot(
        3,
        1,
        1,
        (
            make_policy("parent", None, cir=1000, pir=2000),
            make_policy("child", "parent", cir=1500, pir=2500),
        ),
    )
    with pytest.raises(BusinessError) as caught:
        validate_snapshot(snapshot)
    assert caught.value.code == "E_RATE_EXCEEDED"
    assert caught.value.details["queue_id"] == "child"
    assert caught.value.details["parent_queue_id"] == "parent"
    assert caught.value.details["child_value"] == 1500
    assert caught.value.details["parent_value"] == 1000


def test_missing_parent_is_rejected():
    snapshot = PolicySnapshot(3, 1, 1, (make_policy("child", "missing"),))
    with pytest.raises(BusinessError) as caught:
        validate_snapshot(snapshot)
    assert caught.value.code == "E_PARENT_NOT_FOUND"


def test_invalid_pir_cbs_relations_are_rejected():
    with pytest.raises(BusinessError) as caught:
        validate_snapshot(
            PolicySnapshot(3, 1, 1, (make_policy("a", cir=2000, pir=1000),))
        )
    assert caught.value.code == "E_POLICY_INVALID"


def test_snapshot_rejects_newer_version_and_empty_policy_set():
    with pytest.raises(BusinessError) as caught:
        validate_snapshot(PolicySnapshot(4, 1, 1, (make_policy("a"),)))
    assert caught.value.code == "E_FORMAT_UNSUPPORTED"

    with pytest.raises(BusinessError) as caught:
        validate_snapshot(PolicySnapshot(3, 1, 1, ()))
    assert caught.value.code == "E_POLICY_INVALID"

    with pytest.raises(BusinessError) as caught:
        validate_snapshot(
            PolicySnapshot(3, 1, 1, (make_policy("a", cbs=2000, pbs=1000),))
        )
    assert caught.value.code == "E_POLICY_INVALID"

