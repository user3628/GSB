from __future__ import annotations

import json
from pathlib import Path

import pytest

from join_planner.errors import PlannerError
from join_planner.snapshot import SnapshotStore, parse_snapshot_text


def test_snapshot_loads_and_validates_checksum(snapshot_dir: Path) -> None:
    store = SnapshotStore(snapshot_dir)
    snapshot = store.load_file("snapshot_v1.json")
    assert snapshot.version == 1
    assert snapshot.relations["orders"].rows == 100000
    assert snapshot.relations["orders"].columns["amount"].min_value == 1.0


def test_invalid_checksum_does_not_replace_current(snapshot_store: SnapshotStore, snapshot_dir: Path) -> None:
    before = snapshot_store.current
    payload = json.loads((snapshot_dir / "snapshot_v2.json").read_text(encoding="utf-8"))
    payload["relations"]["orders"]["rows"] += 1
    invalid = snapshot_dir / "tampered.json"
    invalid.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(PlannerError) as exc_info:
        snapshot_store.load_file("tampered.json")
    assert exc_info.value.code == "E_SNAPSHOT_INVALID"
    assert snapshot_store.current is before


def test_snapshot_versions_are_strictly_monotonic(snapshot_store: SnapshotStore) -> None:
    v2 = snapshot_store.load_file("snapshot_v2.json")
    with pytest.raises(PlannerError) as same:
        snapshot_store.load_file("snapshot_v2.json")
    assert same.value.code == "E_SNAPSHOT_INVALID"
    assert snapshot_store.current is v2
    with pytest.raises(PlannerError) as older:
        snapshot_store.load_file("snapshot_v1.json")
    assert older.value.code == "E_SNAPSHOT_INVALID"
    assert snapshot_store.current is v2


def test_retired_version_is_rejected_for_new_submission(snapshot_store: SnapshotStore) -> None:
    snapshot_store.load_file("snapshot_v2.json")
    with pytest.raises(PlannerError) as exc_info:
        snapshot_store.for_submission(1)
    assert exc_info.value.code == "E_VERSION_RETIRED"
    assert exc_info.value.details["current_version"] == 2


def test_parse_rejects_zero_ndv(snapshot_dir: Path) -> None:
    payload = json.loads((snapshot_dir / "snapshot_v1.json").read_text(encoding="utf-8"))
    payload["relations"]["orders"]["columns"]["order_id"]["ndv"] = 0
    from join_planner.snapshot import checksum_for_snapshot

    payload["checksum"] = checksum_for_snapshot(payload)
    with pytest.raises(PlannerError) as exc_info:
        parse_snapshot_text(json.dumps(payload))
    assert exc_info.value.code == "E_SNAPSHOT_INVALID"

