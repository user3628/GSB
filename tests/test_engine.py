from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
import pytest

from app.engine import WindowStateMachine
from app.errors import BusinessError, E_CAPACITY_EXCEEDED, E_CONFLICTING_BATCH, E_LATE_EVENT


def query(engine, **overrides):
    values = {
        "start_ms": -10_000,
        "end_ms": 100_000,
        "group": None,
        "window_types": ["tumbling", "sliding", "session"],
        "quantiles": [0.5, 0.95],
        "limit": 1000,
        "cursor": None,
    }
    values.update(overrides)
    return engine.query_windows(**values)


def test_tumbling_epoch_alignment_and_boundary_membership(engine):
    result = engine.submit_batch(
        "boundary",
        [
            {"timestamp_ms": 999, "group": "g", "value": 1.0},
            {"timestamp_ms": 1000, "group": "g", "value": 2.0},
        ],
    )
    assert result["state_version"] == 1
    payload = query(engine, window_types=["tumbling"], quantiles=[0.5])
    windows = {(item["window_start_ms"], item["window_end_ms"]): item for item in payload["items"]}
    assert windows[(0, 1000)]["count"] == 1
    assert windows[(0, 1000)]["state"] == "EMITTED"
    assert windows[(1000, 2000)]["count"] == 1
    assert windows[(1000, 2000)]["state"] == "OPEN"


def test_sliding_assigns_event_to_all_covering_windows(engine):
    engine.submit_batch("sliding", [{"timestamp_ms": 1000, "group": "g", "value": 1.0}])
    payload = query(engine, window_types=["sliding"], quantiles=[0.5])
    starts = {item["window_start_ms"] for item in payload["items"]}
    assert starts == {-500, 0, 500, 1000}
    assert all(item["window_end_ms"] - item["window_start_ms"] == 2000 for item in payload["items"])
    assert all(item["count"] == 1 for item in payload["items"])


def test_session_split_exact_gap_and_watermark_close(engine):
    engine.submit_batch(
        "sessions",
        [
            {"timestamp_ms": 0, "group": "g1", "value": 1.0},
            {"timestamp_ms": 700, "group": "g1", "value": 2.0},
            {"timestamp_ms": 1400, "group": "g1", "value": 3.0},
            {"timestamp_ms": 200, "group": "g2", "value": None},
        ],
    )
    payload = query(engine, window_types=["session"], group="g1", quantiles=[0.5])
    sessions = [(item["window_start_ms"], item["window_end_ms"], item["count"], item["state"]) for item in payload["items"]]
    assert sessions == [(0, 1400, 3, "OPEN")]

    engine.submit_batch("advance", [{"timestamp_ms": 3000, "group": "g2", "value": 4.0}])
    payload = query(engine, window_types=["session"], group="g1", quantiles=[0.5])
    assert payload["items"][0]["state"] == "EMITTED"

    engine.submit_batch("split", [{"timestamp_ms": 2500, "group": "g1", "value": 5.0}])
    payload = query(engine, window_types=["session"], group="g1", quantiles=[0.5])
    spans = [(item["window_start_ms"], item["window_end_ms"]) for item in payload["items"]]
    assert (0, 1400) in spans
    assert (2500, 2500) in spans


def test_compensated_sum_fixed_decimals_empty_aggregate_and_quantiles(engine):
    engine.submit_batch(
        "numbers",
        [
            {"timestamp_ms": 100, "group": "g", "value": 1_000_000_000_000_000.0},
            {"timestamp_ms": 200, "group": "g", "value": 1.0},
            {"timestamp_ms": 300, "group": "g", "value": -1_000_000_000_000_000.0},
            {"timestamp_ms": 400, "group": "g", "value": 4.0},
            {"timestamp_ms": 500, "group": "empty", "value": None},
            {"timestamp_ms": 600, "group": "q", "value": 10.0},
            {"timestamp_ms": 700, "group": "q", "value": 20.0},
            {"timestamp_ms": 800, "group": "q", "value": 30.0},
            {"timestamp_ms": 900, "group": "q", "value": 40.0},
        ],
    )
    payload = query(engine, window_types=["tumbling"], group="g", quantiles=[0.5, 0.95])
    window = next(item for item in payload["items"] if item["window_start_ms"] == 0)
    assert window["count"] == 4
    assert window["non_null_count"] == 4
    assert window["sum"] == "5.000000"
    quantile_payload = query(engine, window_types=["tumbling"], group="q", quantiles=[0.5, 0.95])
    quantile_window = quantile_payload["items"][0]
    assert quantile_window["quantiles"] == [
        {"p": 0.5, "value": 20.0, "rank": 2, "sample_size": 4},
        {"p": 0.95, "value": 40.0, "rank": 4, "sample_size": 4},
    ]

    empty_payload = query(engine, window_types=["tumbling"], group="empty", quantiles=[0.5])
    empty = empty_payload["items"][0]
    assert empty["count"] == 1
    assert empty["non_null_count"] == 0
    assert empty["aggregate_status"] == "empty"
    assert empty["sum"] == "0.000000"
    assert empty["min"] is None and empty["max"] is None
    assert empty["quantiles"] == [{"p": 0.5, "value": None, "rank": None, "sample_size": 0}]


def test_late_event_queued_without_changing_emitted_window(engine):
    engine.submit_batch("initial", [{"timestamp_ms": 100, "group": "g", "value": 1.0}])
    before = query(engine, window_types=["tumbling"], group="g", quantiles=[0.5])
    engine.submit_batch("advance", [{"timestamp_ms": 2000, "group": "g", "value": 2.0}])
    closed_before = next(item for item in query(engine, window_types=["tumbling"], group="g")["items"] if item["window_start_ms"] == 0)
    assert closed_before["state"] == "EMITTED"

    response = engine.submit_batch("late", [{"timestamp_ms": 500, "group": "g", "value": 99.0}])
    assert response["late_assignments"] >= 1
    closed_after = next(item for item in query(engine, window_types=["tumbling"], group="g")["items"] if item["window_start_ms"] == 0)
    assert closed_after == closed_before
    late = engine.late_events(limit=100)
    assert late["queue_length"] == response["late_assignments"]
    assert all(entry["batch_id"] == "late" for entry in late["items"])
    assert before["as_of_version"] == 1


def test_late_policy_reject_is_atomic(make_engine):
    reject_engine = make_engine(late_policy="reject", runtime_name="reject")
    reject_engine.submit_batch("initial", [{"timestamp_ms": 100, "group": "g", "value": 1.0}])
    reject_engine.submit_batch("advance", [{"timestamp_ms": 2000, "group": "g", "value": 2.0}])
    version_before = reject_engine.state_summary()["state_version"]
    with pytest.raises(BusinessError) as caught:
        reject_engine.submit_batch("late", [{"timestamp_ms": 500, "group": "g", "value": 99.0}])
    assert caught.value.code == E_LATE_EVENT
    assert reject_engine.state_summary()["state_version"] == version_before
    assert reject_engine.late_events()["queue_length"] == 0


def test_hard_capacity_limit_rejects_entire_batch(make_engine):
    limited = make_engine(
        active_window_soft_limit=2,
        active_window_hard_limit=5,
        runtime_name="capacity",
    )
    with pytest.raises(BusinessError) as caught:
        limited.submit_batch("too-many", [{"timestamp_ms": 1000, "group": "g", "value": 1.0}])
    assert caught.value.code == E_CAPACITY_EXCEEDED
    assert limited.state_summary()["state_version"] == 0
    assert limited.state_summary()["window_count"] == 0


def test_soft_limit_evicts_oldest_emitted_windows_deterministically(make_engine):
    limited = make_engine(
        active_window_soft_limit=6,
        active_window_hard_limit=20,
        late_queue_soft_limit=100,
        late_queue_hard_limit=200,
        runtime_name="evict",
    )
    first = limited.submit_batch("first", [{"timestamp_ms": 0, "group": "g", "value": 1.0}])
    assert first["evictions"] == []
    second = limited.submit_batch("second", [{"timestamp_ms": 5000, "group": "g", "value": 2.0}])
    assert second["evictions"]
    assert len(second["evictions"]) == 6
    assert limited.state_summary()["window_count"] == 6
    ends = [entry["window_end_ms"] for entry in second["evictions"]]
    assert ends == sorted(ends)


def test_evicted_session_tombstone_preserves_late_classification(make_engine):
    limited = make_engine(
        active_window_soft_limit=1,
        active_window_hard_limit=20,
        late_queue_soft_limit=100,
        late_queue_hard_limit=200,
        runtime_name="session-tombstone",
    )
    limited.submit_batch("first", [{"timestamp_ms": 0, "group": "g", "value": 1.0}])
    second = limited.submit_batch("advance", [{"timestamp_ms": 1000, "group": "g", "value": 2.0}])
    assert any(
        entry["window_type"] == "session" and entry["window_start_ms"] == 0
        for entry in second["evictions"]
    )
    assert limited._state["session_tombstones"]

    response = limited.submit_batch("late-near-evicted-session", [{"timestamp_ms": 600, "group": "g", "value": 9.0}])
    assert response["late_assignments"] >= 1
    late = limited.late_events(limit=100)
    assert any(
        entry["window_type"] == "session"
        and entry["reason"] == "session_evicted_influence"
        and entry["detected_state"] == "EMITTED"
        for entry in late["items"]
    )


def test_invalid_transition_is_rejected_without_mutation():
    window = {
        "state": "OPEN",
        "emitted_at_version": None,
    }
    with pytest.raises(BusinessError) as caught:
        WindowStateMachine.transition(window, "EMITTED", 1)
    assert caught.value.code == "E_INVALID_TRANSITION"
    assert window == {"state": "OPEN", "emitted_at_version": None}
    WindowStateMachine.transition(window, "CLOSED")
    WindowStateMachine.transition(window, "EMITTED", 2)
    assert window == {"state": "EMITTED", "emitted_at_version": 2}
    with pytest.raises(BusinessError) as caught:
        WindowStateMachine.transition(window, "OPEN")
    assert caught.value.code == "E_INVALID_TRANSITION"
    assert window["state"] == "EMITTED"


def test_duplicate_batch_concurrency_runs_once(engine):
    batch = {
        "batch_id": "same",
        "events": [{"timestamp_ms": 1000, "group": "g", "value": 1.0}],
    }
    barrier = threading.Barrier(8)

    def submit():
        barrier.wait()
        return engine.submit_batch(batch["batch_id"], batch["events"])

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: submit(), range(8)))
    assert engine.state_summary()["state_version"] == 1
    assert {item["state_version"] for item in results} == {1}
    assert sum(1 for item in results if item["replayed"]) == 7

    with pytest.raises(BusinessError) as caught:
        engine.submit_batch("same", [{"timestamp_ms": 1000, "group": "g", "value": 2.0}])
    assert caught.value.code == E_CONFLICTING_BATCH


def test_query_never_observes_partially_applied_batch(engine):
    engine.submit_batch("committed", [{"timestamp_ms": 1000, "group": "g", "value": 1.0}])
    entered = threading.Event()
    release = threading.Event()
    original = engine._apply_flat_event

    def blocked(*args, **kwargs):
        entered.set()
        assert release.wait(timeout=10)
        return original(*args, **kwargs)

    engine._apply_flat_event = blocked
    errors: list[BaseException] = []

    def ingest():
        try:
            engine.submit_batch("in-progress", [{"timestamp_ms": 5000, "group": "g", "value": 2.0}])
        except BaseException as exc:  # pragma: no cover - assertion aid
            errors.append(exc)

    thread = threading.Thread(target=ingest)
    thread.start()
    try:
        assert entered.wait(timeout=10)
        during = query(engine, window_types=["tumbling"], quantiles=[0.5])
        assert during["as_of_version"] == 1
        assert all(item["last_event_ts_ms"] == 1000 for item in during["items"])
    finally:
        release.set()
        thread.join(timeout=10)
        engine._apply_flat_event = original
    assert not errors
    after = query(engine, window_types=["tumbling"], quantiles=[0.5])
    assert after["as_of_version"] == 2


def test_pinned_pagination_is_stable_across_new_commits(engine):
    engine.submit_batch(
        "base",
        [{"timestamp_ms": ts, "group": f"g{ts % 3}", "value": float(ts)} for ts in range(0, 4000, 50)],
    )
    first = query(engine, limit=3)
    collected = list(first["items"])
    cursor = first["next_cursor"]
    assert cursor is not None
    engine.submit_batch("late-commit", [{"timestamp_ms": 9000, "group": "new", "value": 1.0}])
    while cursor:
        page = query(engine, limit=3, cursor=cursor)
        assert page["as_of_version"] == first["as_of_version"]
        collected.extend(page["items"])
        cursor = page["next_cursor"]
    one_shot = query(engine, limit=1000)
    pinned_identities = [
        (item["window_type"], item["window_start_ms"], item["window_end_ms"], item["group"], item["session_id"])
        for item in collected
    ]
    one_shot_identities = [
        (item["window_type"], item["window_start_ms"], item["window_end_ms"], item["group"], item["session_id"])
        for item in one_shot["items"]
    ]
    assert len(pinned_identities) == len(set(pinned_identities))
    assert first["total"] == len(pinned_identities)
    assert one_shot["as_of_version"] > first["as_of_version"]
    assert len(one_shot_identities) > len(pinned_identities)
