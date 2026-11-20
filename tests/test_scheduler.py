from __future__ import annotations

from pathlib import Path

from h2workbench.engine import Workbench


def _multiplex_sample(sample_id: str = "weighted") -> dict:
    return {
        "sample_id": sample_id,
        "connection_id": "sched",
        "config": {
            "connection_window": 100_000,
            "stream_window": 100_000,
            "round_budget": 400,
            "candidate_threshold": 100,
            "candidate_limit": 100,
            "priority_scale": 4,
        },
        "frames": [
            {
                "stream_id": 1,
                "frame_type": "HEADERS",
                "priority": 0,
                "weight": 3,
            },
            {
                "stream_id": 3,
                "frame_type": "HEADERS",
                "priority": 0,
                "weight": 1,
            },
            {"stream_id": 1, "frame_type": "DATA", "payload_bytes": 3000},
            {"stream_id": 3, "frame_type": "DATA", "payload_bytes": 3000},
        ],
    }


def test_weighted_allocation_within_same_priority(wb: Workbench):
    wb.submit_sample(_multiplex_sample())
    run = wb.schedule_sample("weighted")
    first_round = run["rounds"][0]
    by_stream = {item["stream_id"]: item["bytes"] for item in first_round["selections"]}
    assert by_stream == {1: 300, 3: 100}
    assert run["rounds"][0]["algorithm"] == "exact"
    assert "weight=3" in next(
        item["reason"] for item in first_round["selections"] if item["stream_id"] == 1
    )
    assert {item["stream_id"] for item in run["diagnoses"]} == {1, 3}
    assert all(
        item["conclusion"] == "scheduled_complete" for item in run["diagnoses"]
    )


def test_two_priority_levels_receive_weighted_round_share(wb: Workbench):
    wb.submit_sample(
        {
            "sample_id": "priorities",
            "connection_id": "p",
            "config": {
                "connection_window": 100_000,
                "stream_window": 100_000,
                "round_budget": 600,
                "priority_scale": 4,
            },
            "frames": [
                {"stream_id": 1, "frame_type": "HEADERS", "priority": 0, "weight": 1},
                {"stream_id": 2, "frame_type": "HEADERS", "priority": 1, "weight": 1},
                {"stream_id": 1, "frame_type": "DATA", "payload_bytes": 3000},
                {"stream_id": 2, "frame_type": "DATA", "payload_bytes": 3000},
            ],
        }
    )
    run = wb.schedule_sample("priorities")
    first = {item["stream_id"]: item for item in run["rounds"][0]["selections"]}
    assert first[1]["bytes"] > first[2]["bytes"]
    assert first[1]["priority"] == 0
    assert first[2]["priority"] == 1


def _many_stream_sample(threshold: int = 4, limit: int = 3) -> dict:
    frames = []
    for stream_id in range(1, 21, 2):
        frames.append(
            {
                "stream_id": stream_id,
                "frame_type": "HEADERS",
                "priority": stream_id % 2,
                "weight": (stream_id % 4) + 1,
            }
        )
    for stream_id in range(1, 21, 2):
        frames.append(
            {"stream_id": stream_id, "frame_type": "DATA", "payload_bytes": 2000}
        )
    return {
        "sample_id": "many",
        "connection_id": "many-conn",
        "config": {
            "connection_window": 1_000_000,
            "stream_window": 1_000_000,
            "round_budget": 1000,
            "candidate_threshold": threshold,
            "candidate_limit": limit,
        },
        "frames": frames,
    }


def test_bounded_approx_truncates_candidates_and_reports_error(wb: Workbench):
    wb.submit_sample(_many_stream_sample())
    run = wb.schedule_sample("many")
    approx_rounds = [item for item in run["rounds"] if item["truncated_count"] > 0]
    assert approx_rounds
    first = approx_rounds[0]
    assert first["algorithm"] == "bounded_approx"
    assert first["candidate_count"] == 10
    assert first["selected_candidate_count"] == 3
    assert first["truncated_count"] == 7
    assert first["approximation"]["error_bound_bytes"] > 0
    assert 0 < first["approximation"]["weighted_mass_omitted_ratio"] < 1
    assert len(first["selections"]) <= 3


def test_small_sample_order_is_identical_when_threshold_changes(tmp_path: Path):
    wb_exact = Workbench(tmp_path / "exact.db")
    wb_auto = Workbench(tmp_path / "auto.db")
    sample_exact = _multiplex_sample("same-small")
    sample_exact["config"]["candidate_threshold"] = 100
    sample_exact["config"]["candidate_limit"] = 100
    sample_auto = _multiplex_sample("same-small")
    sample_auto["config"]["candidate_threshold"] = 2
    sample_auto["config"]["candidate_limit"] = 2
    wb_exact.submit_sample(sample_exact)
    wb_auto.submit_sample(sample_auto)
    exact = wb_exact.schedule_sample("same-small")
    auto = wb_auto.schedule_sample("same-small")
    assert exact["trace"] == auto["trace"]
    assert exact["connection"]["connection_window"] == auto["connection"]["connection_window"]
    assert all(item["truncated_count"] == 0 for item in auto["rounds"])
