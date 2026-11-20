from __future__ import annotations

import pytest

from h2workbench.engine import Workbench
from h2workbench.errors import BusinessError
from h2workbench.models import StreamState


def submit_basic(wb: Workbench, *, conn="c", sample="s", connection_window=1000, stream_window=900):
    return wb.submit_sample(
        {
            "sample_id": sample,
            "connection_id": conn,
            "config": {
                "connection_window": connection_window,
                "stream_window": stream_window,
                "round_budget": 4096,
            },
            "frames": [
                {"stream_id": 1, "frame_type": "HEADERS", "flags": []},
                {"stream_id": 1, "frame_type": "DATA", "payload_bytes": 500},
            ],
        }
    )


def test_flow_control_blocked_reports_both_windows_and_required_bytes(wb: Workbench):
    wb.submit_sample(
        {
            "sample_id": "blocked",
            "connection_id": "c",
            "config": {
                "connection_window": 1000,
                "stream_window": 500,
                "round_budget": 4096,
            },
            "frames": [
                {"stream_id": 1, "frame_type": "HEADERS"},
                {"stream_id": 1, "frame_type": "DATA", "payload_bytes": 1200},
            ],
        }
    )
    with pytest.raises(BusinessError) as exc:
        wb.schedule_sample("blocked")
    assert exc.value.code == "E_FLOW_CONTROL_BLOCKED"
    context = exc.value.context
    assert context["connection_window"] == 500
    blocked = context["streams"][0]
    assert blocked["stream_id"] == 1
    assert blocked["connection_window"] == 500
    assert blocked["stream_window"] == 0
    assert blocked["required_bytes"] > 0
    stream = wb.get_stream("c", 1)["stream"]
    assert stream["window"] == 0
    assert stream["sent_unacked"] == 500
    assert stream["pending_bytes"] == 700
    blocked_diagnosis = wb.get_sample_trace("blocked")["diagnoses"][0]
    assert blocked_diagnosis["conclusion"] == "E_FLOW_CONTROL_BLOCKED"


def test_window_update_is_capped_at_initial_limit_and_records_violation(wb: Workbench):
    submit_basic(wb)
    wb.schedule_sample("s")
    assert wb.get_connection("c")["connection_window"] == 500
    assert wb.get_stream("c", 1)["stream"]["window"] == 400

    result = wb.submit_sample(
        {
            "sample_id": "wu",
            "connection_id": "c",
            "frames": [
                {
                    "stream_id": 0,
                    "frame_type": "WINDOW_UPDATE",
                    "window_increment": 1000,
                },
                {
                    "stream_id": 1,
                    "frame_type": "WINDOW_UPDATE",
                    "window_increment": 900,
                },
            ],
        }
    )
    assert result["connection"]["connection_window"] == 1000
    stream = wb.get_stream("c", 1)["stream"]
    assert stream["window"] == 900
    assert stream["sent_unacked"] == 0
    violations = wb.db.fetch_all(
        "SELECT stream_id, code, applied_increment FROM protocol_violations "
        "WHERE connection_id = 'c' AND sample_id = 'wu' ORDER BY violation_id"
    )
    assert [row["code"] for row in violations] == [
        "WINDOW_UPDATE_OVERFLOW_TRUNCATED",
        "WINDOW_UPDATE_OVERFLOW_TRUNCATED",
    ]
    assert violations[0]["applied_increment"] == 500
    assert violations[1]["applied_increment"] == 500


def test_end_stream_transition_and_no_post_close_data(wb: Workbench):
    wb.submit_sample(
        {
            "sample_id": "end",
            "connection_id": "c",
            "frames": [
                {"stream_id": 1, "frame_type": "HEADERS"},
                {
                    "stream_id": 1,
                    "frame_type": "DATA",
                    "flags": ["END_STREAM"],
                    "payload_bytes": 100,
                },
            ],
        }
    )
    wb.schedule_sample("end")
    assert wb.get_stream("c", 1)["stream"]["state"] == StreamState.HALF_CLOSED_LOCAL.value
    with pytest.raises(BusinessError) as exc:
        wb.submit_sample(
            {
                "sample_id": "after-end",
                "connection_id": "c",
                "frames": [{"stream_id": 1, "frame_type": "DATA", "payload_bytes": 1}],
            }
        )
    assert exc.value.code == "E_INVALID_TRANSITION"
    assert wb.get_stream("c", 1)["stream"]["state"] == StreamState.HALF_CLOSED_LOCAL.value
    assert wb.db.fetch_one("SELECT 1 FROM samples WHERE sample_id = 'after-end'") is None


def test_reset_stream_is_never_scheduled(wb: Workbench):
    result = wb.submit_sample(
        {
            "sample_id": "reset",
            "connection_id": "c",
            "config": {"connection_window": 10000, "stream_window": 10000},
            "frames": [
                {"stream_id": 1, "frame_type": "HEADERS"},
                {"stream_id": 3, "frame_type": "HEADERS"},
                {"stream_id": 1, "frame_type": "DATA", "payload_bytes": 5000},
                {"stream_id": 1, "frame_type": "RST_STREAM"},
                {"stream_id": 3, "frame_type": "DATA", "payload_bytes": 1000},
            ],
        }
    )
    assert result["queued_data"]["bytes"] == 1000
    run = wb.schedule_sample("reset")
    assert {item["stream_id"] for item in run["trace"]} == {3}
    assert wb.get_stream("c", 1)["stream"]["state"] == StreamState.RESET.value
    assert wb.get_stream("c", 1)["stream"]["pending_bytes"] == 0
    diagnosis = {item["stream_id"]: item for item in run["diagnoses"]}
    assert diagnosis[1]["conclusion"] == "reset_not_scheduled"


def test_connection_close_transitions_active_streams_and_cancels_jobs(wb: Workbench):
    wb.submit_sample(
        {
            "sample_id": "close",
            "connection_id": "c",
            "frames": [
                {"stream_id": 1, "frame_type": "HEADERS"},
                {"stream_id": 1, "frame_type": "DATA", "payload_bytes": 10},
            ],
        }
    )
    snapshot = wb.close_connection("c")
    assert snapshot["closed"] is True
    assert snapshot["streams"][0]["state"] == StreamState.CLOSED.value
    assert snapshot["pending_jobs"] == 0
    run = wb.schedule_connection("c")
    assert run["status"] == "COMPLETED"
    assert run["trace"] == []
