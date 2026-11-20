from __future__ import annotations

from pathlib import Path

from h2workbench.engine import Workbench

from .conftest import load_sample


def test_batch_partition_does_not_change_trace_windows_or_transitions(tmp_path: Path):
    base = load_sample("normal_multiplex.json")
    batch_one = dict(base)
    batch_one["batch_size"] = 1
    batch_many = dict(base)
    batch_many["batch_size"] = 64

    wb_one = Workbench(tmp_path / "batch-one.db")
    wb_many = Workbench(tmp_path / "batch-many.db")
    ingest_one = wb_one.submit_sample(batch_one)
    ingest_many = wb_many.submit_sample(batch_many)
    assert ingest_one["transitions"] == ingest_many["transitions"]
    assert ingest_one["connection"] == ingest_many["connection"]
    assert ingest_one["queued_data"] == ingest_many["queued_data"]
    assert len(ingest_one["batches"]) == 6
    assert len(ingest_many["batches"]) == 1

    run_one = wb_one.schedule_sample("demo-normal-multiplex")
    run_many = wb_many.schedule_sample("demo-normal-multiplex")
    assert run_one["trace"] == run_many["trace"]
    assert run_one["rounds"] == run_many["rounds"]
    assert run_one["blocked"] == run_many["blocked"]
    assert run_one["connection"] == run_many["connection"]
    assert run_one["selected_bytes"] == 12_500


def test_repeated_queries_return_the_same_committed_snapshot(tmp_path: Path):
    wb = Workbench(tmp_path / "query.db")
    wb.submit_sample(load_sample("normal_multiplex.json"))
    wb.schedule_sample("demo-normal-multiplex")
    first = wb.get_connection("demo-normal")
    second = wb.get_connection("demo-normal")
    third = wb.get_connection("demo-normal")
    assert first == second == third
    trace_one = wb.get_connection_trace("demo-normal")
    trace_two = wb.get_connection_trace("demo-normal")
    assert trace_one == trace_two


def test_historical_sample_trace_keeps_its_committed_window_snapshot(tmp_path: Path):
    wb = Workbench(tmp_path / "historical.db")
    wb.submit_sample(load_sample("normal_multiplex.json"))
    first = wb.schedule_sample("demo-normal-multiplex")
    first_connection = first["connection"]

    wb.submit_sample(
        {
            "sample_id": "later-work",
            "connection_id": "demo-normal",
            "frames": [
                {"stream_id": 99, "frame_type": "HEADERS", "weight": 1},
                {"stream_id": 99, "frame_type": "DATA", "payload_bytes": 500},
            ],
        }
    )
    wb.schedule_connection("demo-normal")
    current = wb.get_connection("demo-normal")
    assert current["connection_window"] != first_connection["connection_window"]

    historical = wb.get_sample_trace("demo-normal-multiplex")
    assert historical["trace"] == first["trace"]
    assert historical["connection"] == first_connection
