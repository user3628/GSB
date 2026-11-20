import pytest

from config_dispatch.errors import (
    InvalidSnapshotError,
    InvalidTransitionError,
    StaleSnapshotError,
)
from config_dispatch.models import Snapshot, compute_checksum
from config_dispatch.state_machine import (
    APPLIED,
    DISPATCHED,
    FAILED,
    PENDING,
    ROLLED_BACK,
    is_legal,
)


def test_snapshot_submit_and_stale_detection(coordinator, snapshot_payload):
    coordinator.submit_snapshot(snapshot_payload(1))
    coordinator.submit_snapshot(snapshot_payload(2))
    with pytest.raises(StaleSnapshotError) as exc:
        coordinator.submit_snapshot(snapshot_payload(2))
    assert exc.value.code == "E_STALE_SNAPSHOT"
    assert coordinator.repo.latest_version() == 2


def test_snapshot_bad_checksum_rejected(coordinator, snapshot_payload):
    payload = snapshot_payload(1)
    payload["checksum"] = "deadbeef"
    with pytest.raises(InvalidSnapshotError) as exc:
        coordinator.submit_snapshot(payload)
    assert exc.value.code == "E_INVALID_SNAPSHOT"
    assert coordinator.repo.latest_version() is None


def test_snapshot_missing_field_rejected(coordinator, snapshot_payload):
    payload = snapshot_payload(1)
    del payload["generated_at"]
    with pytest.raises(InvalidSnapshotError) as exc:
        coordinator.submit_snapshot(payload)
    assert exc.value.code == "E_INVALID_SNAPSHOT"


def test_checksum_deterministic():
    items = {"b": 2, "a": [1, 2], "c": {"x": 1}}
    assert compute_checksum(items) == compute_checksum(dict(reversed(list(items.items()))))


@pytest.mark.parametrize(
    "current,target,expected",
    [
        (PENDING, DISPATCHED, True),
        (DISPATCHED, APPLIED, True),
        (DISPATCHED, FAILED, True),
        (APPLIED, ROLLED_BACK, True),
        (APPLIED, FAILED, True),
        (FAILED, ROLLED_BACK, True),
        (FAILED, PENDING, True),
        (DISPATCHED, ROLLED_BACK, True),
        (PENDING, APPLIED, False),
        (APPLIED, DISPATCHED, False),
        (DISPATCHED, PENDING, True),
        (ROLLED_BACK, PENDING, False),
        (ROLLED_BACK, APPLIED, False),
        (PENDING, FAILED, False),
    ],
)
def test_state_machine_transitions(current, target, expected):
    assert is_legal(current, target) is expected


def test_illegal_transition_persists_nothing(repo):
    repo.insert_snapshot(
        Snapshot(
            version=1,
            generated_at="2026-11-20T00:00:00+08:00",
            config_items={"k": "v"},
            checksum=compute_checksum({"k": "v"}),
        )
    )
    repo.ensure_component_state(1, "c1", "idem-1")
    repo.transition_component_state(1, "c1", DISPATCHED, reason="go")
    with pytest.raises(InvalidTransitionError) as exc:
        repo.transition_component_state(1, "c1", APPLIED, reason="go-2")
        repo.transition_component_state(1, "c1", DISPATCHED, reason="bad")
    assert exc.value.code == "E_INVALID_TRANSITION"
    row = repo.get_component_state(1, "c1")
    assert row["state"] == APPLIED
