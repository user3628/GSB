from __future__ import annotations

import pytest

from app.engine import Engine
from app.errors import EngineError
from app.schemas import BatchRequest, CloseWindowsRequest, EmitWindowsRequest, TransitionWindowRequest


SLIDING = {
    "kind": "sliding",
    "width_ms": 1000,
    "step_ms": 500,
    "boundary": "left_closed_right_open",
}


def make_batch(batch_id: str, measure_type: dict, events: list[dict], spec: dict | None = None, **kwargs):
    return BatchRequest.model_validate(
        {
            "batch_id": batch_id,
            "window_spec": spec or SLIDING,
            "measure_type": measure_type,
            "events": events,
            "timestamp_order": kwargs.pop("timestamp_order", "sort"),
            "late_policy": kwargs.pop("late_policy", "queue"),
            **kwargs,
        }
    )


def by_start(result: dict, start_ms: int) -> dict:
    matches = [window for window in result["windows"] if window["start_ms"] == start_ms]
    assert len(matches) == 1, matches
    return matches[0]


def test_exact_type_evolution_overlap_and_nulls(engine: Engine) -> None:
    first = make_batch(
        "int-1",
        {"kind": "integer"},
        [
            {"event_id": "e0", "timestamp_ms": 0, "group_key": "g", "measure": 10, "labels": {"l": 1}},
            {"event_id": "e1", "timestamp_ms": 500, "group_key": "g", "measure": None},
        ],
    )
    second = make_batch(
        "dec-2",
        {"kind": "decimal", "scale": 2},
        [{"event_id": "e2", "timestamp_ms": 900, "group_key": "g", "measure": "1.25"}],
    )
    third = make_batch(
        "int-3",
        {"kind": "integer"},
        [{"event_id": "e3", "timestamp_ms": 950, "group_key": "g", "measure": 2}],
    )
    assert engine.ingest_batch(first)["state_version"] == 1
    assert engine.ingest_batch(second)["state_version"] == 2
    assert engine.ingest_batch(third)["state_version"] == 3

    result = engine.query_windows(include_type_history=True)
    assert result["state_version"] == 3
    assert result["window_count"] == 3

    start_zero = by_start(result, 0)
    assert start_zero["boundary"] == {"start": "closed", "end": "open"}
    assert start_zero["count"] == 4
    assert start_zero["non_null_count"] == 3
    assert start_zero["sum"] == "13.25"
    assert start_zero["sum_scaled"] == "1325"
    assert start_zero["min"] == "1.25"
    assert start_zero["max"] == "10.00"
    assert start_zero["aggregate_type"] == "decimal"
    assert start_zero["scale"] == 2
    assert start_zero["rounding"] == "none"
    assert start_zero["group_current_type"] == {"kind": "integer", "scale": 0}
    assert [item["kind"] for item in start_zero["input_types"]] == ["integer", "decimal"]

    start_500 = by_start(result, 500)
    assert start_500["count"] == 3
    assert start_500["non_null_count"] == 2
    assert start_500["sum"] == "3.25"
    assert start_500["min"] == "1.25"
    assert start_500["max"] == "2.00"

    evolution = engine.type_evolution()["groups"][0]
    assert evolution["current_type"] == {"kind": "integer", "scale": 0}
    assert [item["reason"] for item in evolution["evolution"]] == [
        "initial_declaration",
        "exact_promotion",
        "exact_narrowing_of_future_inputs",
    ]
    assert [item["rounding"] for item in evolution["evolution"]] == ["none", "none", "none"]

    q = engine.query_windows(quantiles=[0.5])
    quantile_window = by_start(q, 0)
    assert quantile_window["quantiles"][0]["value"] == "2.00"
    # Quantile extension must not perturb the four primary metrics.
    assert quantile_window["count"] == start_zero["count"]
    assert quantile_window["sum"] == start_zero["sum"]
    assert quantile_window["min"] == start_zero["min"]
    assert quantile_window["max"] == start_zero["max"]


def test_all_null_group_is_deterministic(engine: Engine) -> None:
    engine.ingest_batch(
        make_batch(
            "null-batch",
            {"kind": "decimal", "scale": 3},
            [{"event_id": "n1", "timestamp_ms": 100, "group_key": "empty", "measure": None}],
        )
    )
    result = engine.query_windows(group_key="empty")
    assert result["window_count"] == 2
    for window in result["windows"]:
        assert window["count"] == 1
        assert window["non_null_count"] == 0
        assert window["sum"] is None
        assert window["sum_scaled"] is None
        assert window["min"] is None
        assert window["max"] is None
        assert window["scale"] == 3
        assert window["all_null"] is True


def test_type_mismatch_rejects_whole_batch(engine: Engine) -> None:
    engine.ingest_batch(
        make_batch(
            "valid",
            {"kind": "integer"},
            [{"event_id": "v1", "timestamp_ms": 100, "group_key": "g", "measure": 7}],
        )
    )
    before = engine.status()
    bad = make_batch(
        "bad-type",
        {"kind": "integer"},
        [
            {"event_id": "bad1", "timestamp_ms": 200, "group_key": "g", "measure": 3},
            {"event_id": "bad2", "timestamp_ms": 300, "group_key": "g", "measure": "2.5"},
        ],
    )
    with pytest.raises(EngineError) as caught:
        engine.ingest_batch(bad)
    assert caught.value.code == "E_MEASURE_TYPE_MISMATCH"
    assert caught.value.context["batch_id"] == "bad-type"
    after = engine.status()
    assert after["state_version"] == before["state_version"] == 1
    assert after["batch_count"] == 1
    assert [item["batch_id"] for item in engine.list_batches()["batches"]] == ["valid"]

    too_precise = make_batch(
        "bad-scale",
        {"kind": "decimal", "scale": 2},
        [{"event_id": "bad3", "timestamp_ms": 400, "group_key": "g", "measure": "1.234"}],
    )
    with pytest.raises(EngineError) as caught:
        engine.ingest_batch(too_precise)
    assert caught.value.code == "E_MEASURE_TYPE_MISMATCH"

    # JSON fractional numbers are accepted only when exactly representable.
    numeric_decimal = make_batch(
        "numeric-decimal",
        {"kind": "decimal", "scale": 2},
        [{"event_id": "num1", "timestamp_ms": 500, "group_key": "g", "measure": 1.5}],
    )
    response = engine.ingest_batch(numeric_decimal)
    assert response["status"] == "COMMITTED"
    assert by_start(engine.query_windows(), 0)["sum"] == "8.50"


def test_tumbling_boundaries_and_sliding_overlap(engine: Engine) -> None:
    tumbling_open = {
        "kind": "tumbling",
        "width_ms": 1000,
        "boundary": "left_closed_right_open",
    }
    engine.ingest_batch(
        make_batch(
            "tum-open",
            {"kind": "integer"},
            [{"event_id": "t1", "timestamp_ms": 1000, "group_key": "g", "measure": 1}],
            spec=tumbling_open,
        )
    )
    result = engine.query_windows(window_kind="tumbling")
    assert [(w["start_ms"], w["end_ms"]) for w in result["windows"]] == [(1000, 2000)]
    assert engine.query_windows(window_kind="tumbling", from_ms=2000, to_ms=2000)["window_count"] == 0
    assert engine.query_windows(window_kind="tumbling", from_ms=1999, to_ms=1999)["window_count"] == 1

    tumbling_closed = {
        "kind": "tumbling",
        "width_ms": 1000,
        "boundary": "left_closed_right_closed",
    }
    engine.ingest_batch(
        make_batch(
            "tum-closed",
            {"kind": "integer"},
            [{"event_id": "t2", "timestamp_ms": 0, "group_key": "h", "measure": 2}],
            spec=tumbling_closed,
        )
    )
    closed = engine.query_windows(window_kind="tumbling", group_key="h")
    assert [(w["start_ms"], w["end_ms"], w["count"]) for w in closed["windows"]] == [
        (-1000, 0, 1),
        (0, 1000, 1),
    ]
    assert all(w["boundary"] == {"start": "closed", "end": "closed"} for w in closed["windows"])


def test_session_windows_sorted_and_auto_close(engine: Engine) -> None:
    spec = {"kind": "session", "gap_ms": 100, "boundary": "left_closed_right_open"}
    response = engine.ingest_batch(
        make_batch(
            "session-1",
            {"kind": "integer"},
            [
                {"event_id": "s300", "timestamp_ms": 300, "group_key": "g", "measure": 3},
                {"event_id": "s100", "timestamp_ms": 100, "group_key": "g", "measure": 1},
                {"event_id": "s150", "timestamp_ms": 150, "group_key": "g", "measure": 2},
            ],
            spec=spec,
            timestamp_order="sort",
        )
    )
    assert response["timestamp_order_applied"] == "sorted_by_timestamp_then_event_id"
    result = engine.query_windows(window_kind="session")
    assert [(w["start_ms"], w["end_ms"], w["count"], w["state"]) for w in result["windows"]] == [
        (100, 250, 2, "CLOSED"),
        (300, 400, 1, "OPEN"),
    ]
    assert response["auto_transitions"][0]["reason"] == "session_idle_gap"

    # A later batch with an earlier timestamp still inside the idle gap extends
    # the open session start backwards rather than creating an overlapping one.
    engine.ingest_batch(
        make_batch(
            "session-backward",
            {"kind": "integer"},
            [{"event_id": "s250", "timestamp_ms": 250, "group_key": "g", "measure": 9}],
            spec=spec,
        )
    )
    extended = [
        window
        for window in engine.query_windows(window_kind="session")["windows"]
        if window["start_ms"] == 250 and window["group_key"] == "g"
    ]
    assert len(extended) == 1
    assert extended[0]["end_ms"] == 400
    assert extended[0]["count"] == 2
    assert extended[0]["sum"] == 12

    rejected = make_batch(
        "session-reject-order",
        {"kind": "integer"},
        [
            {"event_id": "r2", "timestamp_ms": 200, "group_key": "g", "measure": 1},
            {"event_id": "r1", "timestamp_ms": 100, "group_key": "g", "measure": 1},
        ],
        spec=spec,
        timestamp_order="reject",
    )
    before = engine.status()["state_version"]
    with pytest.raises(EngineError) as caught:
        engine.ingest_batch(rejected)
    assert caught.value.code == "E_OUT_OF_ORDER"
    assert engine.status()["state_version"] == before


def test_state_machine_late_queue_and_illegal_transition(engine: Engine) -> None:
    spec = {"kind": "tumbling", "width_ms": 1000, "boundary": "left_closed_right_open"}
    engine.ingest_batch(
        make_batch(
            "state-1",
            {"kind": "integer"},
            [{"event_id": "x1", "timestamp_ms": 100, "group_key": "g", "measure": 10}],
            spec=spec,
        )
    )
    window_id = engine.query_windows()["windows"][0]["window_id"]
    closed = engine.close_windows(CloseWindowsRequest(through_ms=1100))
    assert closed["transitions"][0]["to_state"] == "CLOSED"
    closed_version = engine.status()["state_version"]

    reject_late = make_batch(
        "late-reject",
        {"kind": "integer"},
        [{"event_id": "x2", "timestamp_ms": 200, "group_key": "g", "measure": 99}],
        spec=spec,
        late_policy="reject",
    )
    with pytest.raises(EngineError) as caught:
        engine.ingest_batch(reject_late)
    assert caught.value.code == "E_LATE_EVENT"
    assert caught.value.context["target_state"] == "CLOSED"
    assert engine.status()["state_version"] == closed_version
    assert all(item["batch_id"] != "late-reject" for item in engine.list_batches()["batches"])

    queue_late = make_batch(
        "late-queue",
        {"kind": "integer"},
        [{"event_id": "x3", "timestamp_ms": 200, "group_key": "g", "measure": 99}],
        spec=spec,
        late_policy="queue",
    )
    late_response = engine.ingest_batch(queue_late)
    assert late_response["late_event_count"] == 1
    unchanged = engine.query_windows(group_key="g")["windows"][0]
    assert unchanged["state"] == "CLOSED"
    assert unchanged["count"] == 1
    assert unchanged["sum"] == 10
    assert engine.query_late_events()["late_event_count"] == 1

    emitted = engine.emit_windows(EmitWindowsRequest(through_ms=1100))
    assert emitted["transition_count"] == 1
    assert engine.query_windows()["windows"][0]["state"] == "EMITTED"
    emitted_version = engine.status()["state_version"]

    with pytest.raises(EngineError) as caught:
        engine.transition_window(TransitionWindowRequest(window_id=window_id, target_state="OPEN"))
    assert caught.value.code == "E_INVALID_TRANSITION"
    assert engine.status()["state_version"] == emitted_version
    assert engine.query_windows()["windows"][0]["state"] == "EMITTED"

    engine.ingest_batch(
        make_batch(
            "state-open",
            {"kind": "integer"},
            [{"event_id": "x4", "timestamp_ms": 2100, "group_key": "g", "measure": 1}],
            spec=spec,
        )
    )
    open_window = [
        window for window in engine.query_windows(state="OPEN")["windows"] if window["start_ms"] == 2000
    ][0]
    before_invalid = engine.status()["state_version"]
    with pytest.raises(EngineError) as caught:
        engine.transition_window(
            TransitionWindowRequest(window_id=open_window["window_id"], target_state="EMITTED")
        )
    assert caught.value.code == "E_INVALID_TRANSITION"
    assert engine.status()["state_version"] == before_invalid
    assert engine.query_windows(state="OPEN")[ "windows"][0]["state"] == "OPEN"


def test_duplicate_batch_reuses_committed_result(engine: Engine) -> None:
    request = make_batch(
        "same-batch",
        {"kind": "integer"},
        [{"event_id": "d1", "timestamp_ms": 100, "group_key": "g", "measure": 1}],
    )
    first = engine.ingest_batch(request)
    version = engine.status()["state_version"]
    second = engine.ingest_batch(request)
    assert second["deduplicated"] is True
    assert second["state_version"] == first["state_version"]
    assert engine.status()["state_version"] == version
    assert engine.query_windows()["windows"][0]["count"] == 1

    conflicting = make_batch(
        "same-batch",
        {"kind": "integer"},
        [{"event_id": "d2", "timestamp_ms": 200, "group_key": "g", "measure": 2}],
    )
    with pytest.raises(EngineError) as caught:
        engine.ingest_batch(conflicting)
    assert caught.value.code == "E_CONFLICTING_BATCH"
    assert engine.status()["state_version"] == version
