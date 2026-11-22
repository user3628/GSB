from __future__ import annotations

import json
from pathlib import Path

import pytest

from kvstore.errors import StoreError
from kvstore.snapshot import parse_snapshot
from kvstore.store import Store


def test_same_key_update_replaces_version_and_offset_only(tmp_path) -> None:
    store = Store(tmp_path)
    try:
        store.put("a", "one", write_ts_ms=1_000, expires_at_ms=10_000)
        store.put("b", "two", write_ts_ms=1_001, expires_at_ms=20_000)
        before_a = store.read_view().ref("a")
        before_b = store.read_view().ref("b")
        assert before_a is not None and before_b is not None
        store.put("a", "updated", write_ts_ms=2_000, expires_at_ms=30_000)
        after_a = store.read_view().ref("a")
        after_b = store.read_view().ref("b")
        assert after_a is not None and after_b is not None
        assert after_a.version == 2
        assert after_a.segment_id != before_a.segment_id
        assert after_a.offset != before_a.offset or after_a.token != before_a.token
        assert after_b == before_b
        check = store.heap_check()
        assert check["ok"] is True
        assert check["stale_entries"] == []
        assert len(check["entries"]) == 2
        assert {entry["key"]: entry["expires_ms"] for entry in check["entries"]} == {
            "a": 30_000,
            "b": 20_000,
        }
    finally:
        store.close()


def test_segment_digest_failure_cascades_atomically_and_old_read_view_survives(tmp_path) -> None:
    store = Store(tmp_path)
    try:
        store.put("a", "one", write_ts_ms=1_000, expires_at_ms=9_000_000_000_000)
        store.put("b", "two", write_ts_ms=1_001, expires_at_ms=9_000_000_000_001)
        old_view = store.read_view()
        old_b = old_view.ref("b")
        summary = store.segment_summary()["segments"]
        first = summary[0]
        segment_path = Path(tmp_path) / "segments" / first["filename"]
        blob = bytearray(segment_path.read_bytes())
        blob[-3] ^= 0x7F
        segment_path.write_bytes(blob)

        result = store.verify_segments()
        assert result["status"] == "invalid_segments_rebuilt"
        assert result["invalidated_keys"] == ["a"]
        assert result["invalidated_key_count"] == 1
        assert result["state_rebuilt"] is True
        assert store.stats()["state"]["tombstone_count"] == 0
        with pytest.raises(StoreError):
            store.get("a")
        assert store.get("b")["value"] == "two"
        assert store.read_view().ref("b") == old_b
        # A reader that captured the pre-verification state keeps a consistent
        # view because its old immutable State object is still alive.
        assert old_view.public_record("a")["value"] == "one"
        assert store.heap_check()["ok"] is True
    finally:
        store.close()


def test_compaction_rebuilds_offsets_and_remains_restartable(tmp_path) -> None:
    store = Store(tmp_path)
    try:
        store.put("a", "one", write_ts_ms=1_000, expires_at_ms=9_000_000_000_000)
        store.put("b", "two", write_ts_ms=1_001)
        store.delete("b", write_ts_ms=1_100)
        assert len(store.read_view().state.segments) == 3
        result = store.compact()
        assert result["status"] == "compacted"
        assert len(store.read_view().state.segments) == 1
        assert store.get("a")["value"] == "one"
        with pytest.raises(StoreError):
            store.get("b")
        segment_id = result["new_segment_id"]
        assert all(
            ref.segment_id == segment_id
            for ref in store.read_view().state.index.values()
        )
        store.close()
        reopened = Store(tmp_path)
        try:
            assert reopened.get("a")["value"] == "one"
            with pytest.raises(StoreError):
                reopened.get("b")
            assert reopened.heap_check()["ok"] is True
        finally:
            reopened.close()
    finally:
        if not store._closed:
            store.close()


def test_snapshot_switch_validation_and_old_state_preservation(tmp_path) -> None:
    source = Store(tmp_path / "source")
    target = Store(tmp_path / "target")
    try:
        source.put("s1", "one", write_ts_ms=1_000, expires_at_ms=9_000_000_000_000)
        source.put("s2", "two", write_ts_ms=1_001)
        target.put("old", "keep-on-error", write_ts_ms=500)
        snapshot = source.export_snapshot(snapshot_seq=3)
        ok = target.load_snapshot(snapshot)
        assert ok["snapshot_seq"] == 3
        assert target.get("s1")["value"] == "one"
        with pytest.raises(StoreError):
            target.get("old")

        with pytest.raises(StoreError) as stale:
            target.load_snapshot(snapshot)
        assert stale.value.code == "E_SNAPSHOT_STALE"
        assert target.get("s1")["value"] == "one"

        invalid = json.loads(json.dumps(snapshot))
        invalid["records"][0]["value"] = "tampered"
        with pytest.raises(StoreError) as bad:
            target.load_snapshot(invalid)
        assert bad.value.code == "E_SNAPSHOT_INVALID"
        assert target.get("s1")["value"] == "one"
    finally:
        source.close()
        target.close()


def test_snapshot_version_and_stale_base_codes(tmp_path) -> None:
    source = Store(tmp_path / "source")
    target = Store(tmp_path / "target")
    try:
        source.put("x", "y", write_ts_ms=1_000)
        snapshot = source.export_snapshot(snapshot_seq=2)
        incompatible = json.loads(json.dumps(snapshot))
        incompatible["format_version"] = 99
        with pytest.raises(StoreError) as version_error:
            parse_snapshot(incompatible)
        assert version_error.value.code == "E_SNAPSHOT_INVALID"
        with pytest.raises(StoreError) as base_error:
            target.load_snapshot(snapshot, expected_base_snapshot_seq=1)
        assert base_error.value.code == "E_STALE_BASE"
        assert target.stats()["state"]["key_count"] == 0
    finally:
        source.close()
        target.close()


def test_loaded_snapshot_survives_restart_and_monotonic_sequence(tmp_path) -> None:
    store = Store(tmp_path)
    store.put("a", "one", write_ts_ms=1_000, expires_at_ms=9_000_000_000_000)
    store.put("b", "two", write_ts_ms=1_001)
    snapshot = store.export_snapshot(snapshot_seq=5)
    store.close()
    reopened = Store(tmp_path)
    try:
        assert reopened.stats()["state"]["last_snapshot_seq"] == 0
        result = reopened.load_snapshot(snapshot)
        assert result["snapshot_seq"] == 5
        assert reopened.get("a")["value"] == "one"
        reopened.close()
        second = Store(tmp_path)
        try:
            assert second.stats()["state"]["last_snapshot_seq"] == 5
            assert second.get("b")["value"] == "two"
        finally:
            second.close()
    finally:
        if not reopened._closed:
            reopened.close()

