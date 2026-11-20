from __future__ import annotations

import json
from pathlib import Path

import pytest

from kvstore.errors import SnapshotInvalid
from kvstore.segment import create_segment
from kvstore.store import KVStore


def test_snapshot_switch_is_atomic_and_monotonic(store: KVStore) -> None:
    store.write("a", 1, version=1)
    store.write("b", 2, version=1)
    old_segment = store.index_summary()["segment"]
    result = store.switch_snapshot()
    assert result["snapshot_seq"] == 1
    assert result["atomic_switch"] is True
    summary = store.index_summary()
    assert summary["snapshot_seq"] == 1
    assert summary["segment"] != old_segment
    assert summary["segment"].startswith("snapshots/snapshot-")
    assert store.get("a")["value"] == 1
    assert store.get("b")["value"] == 2

    reopened = KVStore(store.data_dir)
    assert reopened.index_summary()["snapshot_seq"] == 1
    assert reopened.get("a")["value"] == 1


def test_invalid_snapshot_leaves_old_state_untouched(store: KVStore, tmp_path: Path) -> None:
    store.write("old", "kept", version=1)
    store.switch_snapshot()
    before = store.index_summary()
    before_value = store.get("old")["value"]

    external = tmp_path / "invalid-snapshot.kvs"
    create_segment(
        external,
        segment_id="external-seq-2",
        kind="snapshot",
        snapshot_seq=2,
        generated_at_us=4_100_000_000_000_000,
    )
    # A valid header followed by a CRC-broken record must fail before a switch.
    from kvstore.model import Record

    good = tmp_path / "valid-source.kvs"
    create_segment(
        good,
        segment_id="external-seq-2",
        kind="snapshot",
        snapshot_seq=2,
        generated_at_us=4_100_000_000_000_000,
        records=[Record("new", "must-not-appear", 1, 4_100_000_000_000_000, None)],
    )
    data = bytearray(good.read_bytes())
    data[-1] ^= 0xFF
    external.write_bytes(data)

    with pytest.raises(SnapshotInvalid) as error:
        store.switch_snapshot(external)
    assert error.value.code == "E_SNAPSHOT_INVALID"
    after = store.index_summary()
    assert after["segment"] == before["segment"]
    assert after["snapshot_seq"] == before["snapshot_seq"]
    assert store.get("old")["value"] == before_value
    from kvstore.errors import KeyNotFound

    with pytest.raises(KeyNotFound):
        store.get("new")


def test_older_snapshot_sequence_is_rejected(store: KVStore, tmp_path: Path) -> None:
    store.write("a", 1, version=1)
    store.switch_snapshot()
    store.switch_snapshot()
    current = store.index_summary()

    stale = tmp_path / "stale.kvs"
    from kvstore.model import Record

    create_segment(
        stale,
        segment_id="stale",
        kind="snapshot",
        snapshot_seq=1,
        generated_at_us=4_100_000_000_000_000,
        records=[Record("stale", 1, 1, 4_100_000_000_000_000, None)],
    )
    with pytest.raises(SnapshotInvalid) as error:
        store.switch_snapshot(stale)
    assert error.value.code == "E_SNAPSHOT_INVALID"
    assert store.index_summary()["segment"] == current["segment"]
    assert store.get("a")["value"] == 1


def test_external_snapshot_load_switches_every_index_entry(store: KVStore, tmp_path: Path) -> None:
    store.write("old", 1, version=1)
    external = tmp_path / "external-valid.kvs"
    from kvstore.model import Record

    create_segment(
        external,
        segment_id="external-valid",
        kind="snapshot",
        snapshot_seq=2,
        generated_at_us=4_100_000_000_000_000,
        records=[
            Record("x", 10, 3, 4_100_000_000_000_000, None),
            Record("y", 20, 1, 4_100_000_000_100_000, 4_200_000_000_000_000),
        ],
    )
    result = store.switch_snapshot(external)
    assert result["snapshot_seq"] == 2
    summary = store.index_summary()
    assert summary["segment"] == str(external.resolve())
    assert summary["live_keys"] == 2
    assert store.get("x")["value"] == 10
    state = store._acquire_state()
    try:
        assert {entry.segment_path for entry in state.index.values()} == {str(external.resolve())}
    finally:
        store._release_state(state)
    meta = json.loads((store.data_dir / "meta.json").read_text(encoding="utf-8"))
    assert meta["snapshot_seq"] == 2
    assert meta["active_segment"] == str(external.resolve())
