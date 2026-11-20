from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from h2workbench.engine import Workbench
from h2workbench.errors import BusinessError


def _sample(sample_id: str = "idem", *, payload: int = 100) -> dict:
    return {
        "sample_id": sample_id,
        "connection_id": "idem-conn",
        "config": {"connection_window": 10_000, "stream_window": 10_000},
        "frames": [
            {"stream_id": 1, "frame_type": "HEADERS"},
            {"stream_id": 1, "frame_type": "DATA", "payload_bytes": payload},
        ],
    }


def test_duplicate_submission_reuses_stored_result_and_advances_once(wb: Workbench):
    first = wb.submit_sample(_sample())
    second = wb.submit_sample(_sample())
    assert first["cache_replayed"] is False
    assert second["cache_replayed"] is True
    assert first["sample"] == second["sample"]
    assert wb.db.fetch_one("SELECT COUNT(*) AS n FROM samples")["n"] == 1
    assert wb.db.fetch_one("SELECT COUNT(*) AS n FROM streams")["n"] == 1
    assert wb.db.fetch_one("SELECT COUNT(*) AS n FROM data_jobs")["n"] == 1
    assert wb.get_connection("idem-conn")["pending_bytes"] == 100


def test_conflicting_sample_id_returns_conflict(wb: Workbench):
    wb.submit_sample(_sample())
    with pytest.raises(BusinessError) as exc:
        wb.submit_sample(_sample(payload=101))
    assert exc.value.code == "E_CONFLICTING_SAMPLE"
    assert exc.value.context["sample_id"] == "idem"
    assert wb.get_connection("idem-conn")["pending_bytes"] == 100
    assert wb.db.fetch_one("SELECT COUNT(*) AS n FROM data_jobs")["n"] == 1


def test_absent_config_matches_explicit_default_config_for_idempotency(wb: Workbench):
    first = _sample("default-config")
    first.pop("config")
    second = _sample("default-config")
    second["config"] = {
        "connection_window": 65_535,
        "stream_window": 65_535,
        "round_budget": 16_384,
        "candidate_threshold": 256,
        "candidate_limit": 64,
        "priority_scale": 4,
        "max_rounds": 100_000,
    }
    assert wb.submit_sample(first)["cache_replayed"] is False
    assert wb.submit_sample(second)["cache_replayed"] is True


def test_concurrent_duplicate_submissions_only_advance_once(wb: Workbench):
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: wb.submit_sample(_sample()), range(8)))
    assert sum(1 for item in results if not item["cache_replayed"]) == 1
    assert wb.db.fetch_one("SELECT COUNT(*) AS n FROM samples")["n"] == 1
    assert wb.db.fetch_one("SELECT COUNT(*) AS n FROM data_jobs")["n"] == 1
    assert wb.get_connection("idem-conn")["pending_bytes"] == 100


def test_duplicate_submission_is_idempotent_across_engine_instances(tmp_path):
    first_engine = Workbench(tmp_path / "shared.db")
    second_engine = Workbench(tmp_path / "shared.db")
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda engine: engine.submit_sample(_sample("cross-instance")),
                [first_engine, second_engine],
            )
        )
    assert sorted(item["cache_replayed"] for item in results) == [False, True]
    assert first_engine.db.fetch_one("SELECT COUNT(*) AS n FROM samples")["n"] == 1
    assert first_engine.db.fetch_one("SELECT COUNT(*) AS n FROM data_jobs")["n"] == 1
    assert first_engine.get_connection("idem-conn")["pending_bytes"] == 100


def test_readers_never_observe_torn_connection_and_stream_windows(wb: Workbench):
    frames = []
    for stream_id in range(1, 11, 2):
        frames.append({"stream_id": stream_id, "frame_type": "HEADERS", "weight": 2})
    for stream_id in range(1, 11, 2):
        frames.append(
            {"stream_id": stream_id, "frame_type": "DATA", "payload_bytes": 100_000}
        )
    wb.submit_sample(
        {
            "sample_id": "concurrent-read",
            "connection_id": "concurrent",
            "config": {
                "connection_window": 2_000_000,
                "stream_window": 2_000_000,
                "round_budget": 1024,
            },
            "frames": frames,
        }
    )
    errors: list[BaseException] = []

    def work() -> None:
        try:
            wb.schedule_sample("concurrent-read")
        except BaseException as exc:  # pragma: no cover - diagnostics
            errors.append(exc)

    thread = threading.Thread(target=work)
    thread.start()
    snapshots = []
    deadline = time.time() + 10
    while thread.is_alive() and time.time() < deadline:
        snapshots.append(wb.get_connection("concurrent"))
        time.sleep(0.001)
    thread.join(timeout=10)
    assert not thread.is_alive()
    assert not errors
    snapshots.append(wb.get_connection("concurrent"))
    assert snapshots
    for snapshot in snapshots:
        sent_total = sum(item["sent_total"] for item in snapshot["streams"])
        assert snapshot["initial_connection_window"] - snapshot["connection_window"] == sent_total
        for stream in snapshot["streams"]:
            assert stream["initial_window"] - stream["window"] == stream["sent_total"]
            assert stream["sent_unacked"] == stream["sent_total"]
    assert snapshots[-1]["pending_bytes"] == 0
