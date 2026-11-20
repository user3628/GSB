from collections import defaultdict

import pytest

from logindex.errors import (
    ConflictingRecordError,
    InvalidTransitionError,
    LineInvalidError,
    StaleBaseError,
)
from logindex.models import BatchState


def test_full_then_append_then_rebuild(engine, sample_lines):
    r1 = engine.submit_batch("B1", sample_lines("batch1_full.txt"))
    assert r1.state == "COMMITTED" and r1.version == 1
    assert r1.path_mode == "APPEND_ONLY"
    assert r1.rebuilt_partitions == []

    r2 = engine.submit_batch("B2", sample_lines("batch2_append.txt"), base_version=1)
    assert r2.version == 2 and r2.path_mode == "APPEND_ONLY"
    assert r2.rebuilt_partitions == []

    r3 = engine.submit_batch("B3", sample_lines("batch3_mixed.txt"), base_version=2)
    assert r3.path_mode == "REBUILD"
    assert r3.rebuilt_partitions == ["gateway"]
    assert "最大时间戳" in r3.rebuild_reason
    assert engine.current_version() == 3


def test_invalid_line_batch_persists_nothing(engine, sample_lines):
    with pytest.raises(LineInvalidError):
        engine.submit_batch("BAD", sample_lines("batch_invalid.txt"))
    assert engine.list_batches() == []
    assert engine.current_version() is None
    assert engine.index_state()["timeline_index"] == []


def test_idempotent_resubmit_returns_same_version(engine, sample_lines):
    first = engine.submit_batch("B1", sample_lines("batch1_full.txt"))
    again = engine.submit_batch("B1", sample_lines("batch1_full.txt"))
    assert again.reused is True
    assert again.version == first.version
    state = engine.index_state()
    # 重复应用不得产生重复条目
    counts = defaultdict(int)
    for row in state["classification_index"]:
        counts[(row["service"], row["level"])] += row["record_count"]
    raw = defaultdict(int)
    for record in engine.query_records():
        raw[(record["service"], record["level"])] += 1
    assert dict(counts) == dict(raw)


def test_conflicting_record_keeps_first_value(engine):
    line = "1700000000000|svc|INFO|original"
    engine.submit_batch("BX", [line])
    conflicting = "1700000000000|svc|INFO|changed-message"
    with pytest.raises(ConflictingRecordError) as exc:
        engine.submit_batch("BX", [conflicting])
    assert exc.value.code == "E_CONFLICTING_RECORD"
    records = engine.query_records()
    assert len(records) == 1 and records[0]["message"] == "original"


def test_stale_base_changes_nothing(engine, sample_lines):
    engine.submit_batch("B1", sample_lines("batch1_full.txt"))
    engine.submit_batch("B2", sample_lines("batch2_append.txt"), base_version=1)
    before = engine.current_version()
    with pytest.raises(StaleBaseError) as exc:
        engine.submit_batch("BX", ["1700000099999|svc|INFO|late"], base_version=1)
    assert exc.value.code == "E_STALE_BASE"
    assert engine.current_version() == before
    assert all(b["batch_id"] != "BX" for b in engine.list_batches())


def test_append_and_rebuild_paths_produce_identical_index(db_path, sample_lines):
    from logindex.engine import LogIndexEngine
    from logindex.store import reset_database

    reset_database(db_path)
    fast = LogIndexEngine(db_path)
    fast.submit_batch("S1", sample_lines("batch1_full.txt"))
    fast.submit_batch("S2", sample_lines("batch2_append.txt"))
    fast_state = fast.index_state()

    reset_database(db_path)
    rebuild = LogIndexEngine(db_path)
    rebuild.submit_batch(
        "S1", sample_lines("batch1_full.txt") + sample_lines("batch2_append.txt")
    )
    rebuild_state = rebuild.index_state()

    def intervals(state):
        return sorted(
            (row["service"], row["start_ms"], row["end_ms"])
            for row in state["timeline_index"]
        )

    def classification(state):
        agg = defaultdict(int)
        for row in state["classification_index"]:
            agg[(row["service"], row["level"])] += row["record_count"]
        return sorted(agg.items())

    assert intervals(fast_state) == intervals(rebuild_state)
    assert classification(fast_state) == classification(rebuild_state)


def test_illegal_manual_transition_via_double_invalidate(engine, sample_lines):
    engine.submit_batch("B1", sample_lines("batch1_full.txt"))
    engine.invalidate_batch("B1")
    with pytest.raises(InvalidTransitionError) as exc:
        engine.invalidate_batch("B1")
    assert exc.value.code == "E_INVALID_TRANSITION"
    batch = next(b for b in engine.list_batches() if b["batch_id"] == "B1")
    assert batch["state"] == BatchState.INVALIDATED.value


def test_query_filters_and_time_range(engine, sample_lines):
    engine.submit_batch("B1", sample_lines("batch1_full.txt"))
    auth_info = engine.query_records(service="auth-svc", level="INFO")
    assert all(r["service"] == "auth-svc" and r["level"] == "INFO" for r in auth_info)
    assert len(auth_info) == 2

    window = engine.query_records(start_ms=1700000002000, end_ms=1700000003000)
    assert all(1700000002000 <= r["timestamp_ms"] <= 1700000003000 for r in window)
    assert len(window) == 3
