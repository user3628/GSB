from __future__ import annotations

from pathlib import Path

import pytest

from inverted_index.storage import IndexStore, SearchClause


class SimulatedProcessDeath(BaseException):
    pass


def snapshot(store: IndexStore, version: int) -> list[str]:
    response = store.search(
        clauses=[SearchClause(kind="term", field_path="tags", term="local")],
        version=version,
        limit=100,
    )
    return [hit.doc_id for hit in response.items]


def test_merge_preserves_version_queries(loaded_store) -> None:
    store, first, second = loaded_store
    before_first = snapshot(store, first)
    before_second = snapshot(store, second)
    active_before = {row["segment_id"] for row in store.list_segments(include_merged=False)}
    result = store.merge_segments()
    active_after = {row["segment_id"] for row in store.list_segments(include_merged=False)}
    assert len(active_after) == len(active_before) - len(result.source_segment_ids) + 1
    assert snapshot(store, first) == before_first
    assert snapshot(store, second) == before_second
    assert len(before_first) > 0


def test_merge_crash_is_recovered_from_persisted_intent(tmp_path: Path, index_fields) -> None:
    death_triggered = False

    def hook(name: str) -> None:
        nonlocal death_triggered
        if name == "merge:after_copy":
            death_triggered = True
            raise SimulatedProcessDeath("hard kill during merge")

    db_path = tmp_path / "index.db"
    store = IndexStore(db_path, index_fields, failpoint_hook=hook)
    store.write_batch(inserts=[{"_id": "a", "tags": ["local"], "body": "alpha"}], batch_id="one")
    store.write_batch(inserts=[{"_id": "b", "tags": ["local"], "body": "beta"}], batch_id="two")
    before = snapshot(store, 2)
    with pytest.raises(SimulatedProcessDeath):
        store.merge_segments()
    assert death_triggered

    recovered = IndexStore(db_path, index_fields)
    report = recovered.recovery_report()
    assert report is not None
    assert report["pending_operations_found"] == 1
    assert report["last_committed_version"] == 2
    assert report["actions"][0]["decision"] == "discarded-uncommitted-merge"
    assert snapshot(recovered, 2) == before
    active = [row for row in recovered.list_segments(include_merged=False)]
    assert len(active) == 2
    assert all(row["status"] == "active" for row in active)


def test_batch_crash_leaves_old_version_intact(tmp_path: Path, index_fields) -> None:
    armed = False

    def hook(name: str) -> None:
        if name == "batch:after_changes" and armed:
            raise SimulatedProcessDeath("hard kill during batch")

    db_path = tmp_path / "index.db"
    store = IndexStore(db_path, index_fields, failpoint_hook=hook)
    store.write_batch(inserts=[{"_id": "a", "body": "stable"}], batch_id="one")
    armed = True
    with pytest.raises(SimulatedProcessDeath):
        store.write_batch(
            inserts=[{"_id": "b", "body": "uncommitted"}], batch_id="two"
        )
    recovered = IndexStore(db_path, index_fields)
    report = recovered.recovery_report()
    assert report is not None
    assert report["pending_operations_found"] == 1
    assert report["actions"][0]["decision"] == "aborted-uncommitted-batch"
    assert recovered.current_version() == 1
    assert recovered.get_document("b") is None
    assert recovered.get_postings("body", "uncommitted").entries == ()
