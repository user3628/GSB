from __future__ import annotations

from h2workbench.engine import Workbench


def test_thousand_stream_bounded_schedule_reaches_zero_pending(wb: Workbench):
    streams = 1000
    bytes_per_stream = 1024
    frames = []
    for index in range(streams):
        stream_id = index * 2 + 1
        frames.append(
            {
                "stream_id": stream_id,
                "frame_type": "HEADERS",
                "priority": index % 2,
                "weight": (index % 4) + 1,
            }
        )
    for index in range(streams):
        frames.append(
            {
                "stream_id": index * 2 + 1,
                "frame_type": "DATA",
                "payload_bytes": bytes_per_stream,
            }
        )
    wb.submit_sample(
        {
            "sample_id": "thousand",
            "connection_id": "thousand-conn",
            "config": {
                "connection_window": streams * bytes_per_stream + 1024,
                "stream_window": bytes_per_stream + 1024,
                "round_budget": 16_384,
                "candidate_threshold": 256,
                "candidate_limit": 64,
                "max_rounds": 100_000,
            },
            "frames": frames,
        }
    )
    run = wb.schedule_sample("thousand")
    assert run["selected_bytes"] == streams * bytes_per_stream
    assert run["connection"]["pending_bytes"] == 0
    assert run["connection"]["connection_window"] == 1024
    assert any(item["truncated_count"] > 0 for item in run["rounds"])


def test_hundred_megabyte_stream_budget_is_exact(wb: Workbench):
    size = 100_000_000
    wb.submit_sample(
        {
            "sample_id": "hundred-mb",
            "connection_id": "large",
            "config": {
                "connection_window": size + 1024,
                "stream_window": size + 1024,
                "round_budget": 1_000_000,
            },
            "frames": [
                {"stream_id": 1, "frame_type": "HEADERS", "weight": 8},
                {"stream_id": 1, "frame_type": "DATA", "payload_bytes": size},
            ],
        }
    )
    run = wb.schedule_sample("hundred-mb")
    assert run["selected_bytes"] == size
    assert run["round_count"] == 100
    assert run["connection"]["connection_window"] == 1024
    assert run["connection"]["pending_bytes"] == 0
