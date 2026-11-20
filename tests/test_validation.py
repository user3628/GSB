from __future__ import annotations

import pytest

from h2workbench.engine import Workbench
from h2workbench.errors import BusinessError


def frame(stream_id: int, frame_type: str, **kwargs):
    value = {
        "connection_id": "validation-conn",
        "stream_id": stream_id,
        "frame_type": frame_type,
        "flags": [],
        "payload_bytes": 0,
        "window_increment": 0,
    }
    value.update(kwargs)
    return value


@pytest.mark.parametrize(
    "bad_frame, expected_context",
    [
        (frame(1, "BOGUS"), "frame_type"),
        (frame(-1, "HEADERS"), "stream_id"),
        (frame(3, "DATA", payload_bytes=1), "unopened"),
    ],
)
def test_invalid_frames_do_not_advance_batch(wb: Workbench, bad_frame, expected_context):
    sample = {
        "sample_id": "invalid-sample",
        "connection_id": "validation-conn",
        "frames": [frame(1, "HEADERS"), bad_frame],
    }
    with pytest.raises(BusinessError) as exc:
        wb.submit_sample(sample)
    assert exc.value.code == "E_FRAME_INVALID"
    assert exc.value.connection_id == "validation-conn"
    assert expected_context in str(exc.value.context).lower()
    assert wb.db.fetch_one(
        "SELECT 1 FROM connections WHERE connection_id = 'validation-conn'"
    ) is None
    assert wb.db.fetch_one(
        "SELECT 1 FROM samples WHERE sample_id = 'invalid-sample'"
    ) is None


def test_invalid_transition_does_not_persist_any_state(wb: Workbench):
    sample = {
        "sample_id": "transition-invalid",
        "connection_id": "validation-conn",
        "frames": [
            frame(1, "HEADERS"),
            frame(1, "RST_STREAM"),
            frame(1, "DATA", payload_bytes=10),
        ],
    }
    with pytest.raises(BusinessError) as exc:
        wb.submit_sample(sample)
    assert exc.value.code == "E_INVALID_TRANSITION"
    assert wb.db.fetch_one(
        "SELECT 1 FROM connections WHERE connection_id = 'validation-conn'"
    ) is None
    assert wb.db.fetch_one(
        "SELECT COUNT(*) AS n FROM state_transitions WHERE connection_id = 'validation-conn'"
    )["n"] == 0


def test_data_after_queued_end_stream_is_invalid(wb: Workbench):
    sample = {
        "sample_id": "double-end",
        "connection_id": "validation-conn",
        "frames": [
            frame(1, "HEADERS"),
            frame(1, "DATA", payload_bytes=10, flags=["END_STREAM"]),
            frame(1, "DATA", payload_bytes=1),
        ],
    }
    with pytest.raises(BusinessError) as exc:
        wb.submit_sample(sample)
    assert exc.value.code == "E_INVALID_TRANSITION"
    assert wb.db.fetch_one("SELECT 1 FROM samples WHERE sample_id = 'double-end'") is None


def test_headers_end_stream_cannot_overtake_queued_data(wb: Workbench):
    sample = {
        "sample_id": "headers-overtake",
        "connection_id": "validation-conn",
        "frames": [
            frame(1, "HEADERS"),
            frame(1, "DATA", payload_bytes=10),
            frame(1, "HEADERS", flags=["END_STREAM"]),
        ],
    }
    with pytest.raises(BusinessError) as exc:
        wb.submit_sample(sample)
    assert exc.value.code == "E_INVALID_TRANSITION"
    assert exc.value.context["pending_bytes"] == 10
    assert wb.db.fetch_one(
        "SELECT 1 FROM connections WHERE connection_id = 'validation-conn'"
    ) is None
