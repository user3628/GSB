import json
import sqlite3
from pathlib import Path

import pytest

from app.config import SAMPLES_DIR
from app.errors import BusinessError, InsufficientSamplesError
from app.service import WorkbenchService
from app.store import Store


def _sample_payload(service, name="normal"):
    return service.sample_payload(name)["payload"]


def test_run_metrics_validation_and_deterministic_digest(tmp_path):
    store = Store(tmp_path / "workbench.db")
    service = WorkbenchService(store)
    submitted = service.submit_trace(_sample_payload(service, "normal"))
    assert submitted["inserted"] is True

    first = service.organize(submitted["trace_id"])
    second = service.organize(submitted["trace_id"])
    assert first["deterministic_digest"] == second["deterministic_digest"]
    assert first["schedule_digest"] == second["schedule_digest"]
    assert first["state"] == "COMMITTED"
    assert len(first["batches"]) == 5

    batch_uid = first["batches"][0]["batch_uid"]
    metrics = service.batch_metrics(batch_uid)
    assert [item["label"] for item in metrics["latency"]["quantiles"]] == ["P50", "P95", "P99"]
    for item in metrics["latency"]["quantiles"]:
        assert item["sample_count"] == 4
        assert 1 <= item["rank"] <= item["sample_count"]
        assert item["method"] == "nearest_rank"
    assert metrics["rounding"]["output_unit"] == "0.01ms"
    validation = service.batch_validation(batch_uid)
    assert validation["conclusion"] == "PASS"
    assert validation["failed_count"] == 0
    run_validation = service.run_validation(first["run_id"])
    assert run_validation["conclusion"] == "PASS"
    assert run_validation["check_count"] > 0


def test_fragment_service_exposes_bitmap_and_mixed_rejection(tmp_path):
    store = Store(tmp_path / "workbench.db")
    service = WorkbenchService(store)
    submitted = service.submit_trace(_sample_payload(service, "fragment"))
    run = service.organize(submitted["trace_id"])
    events = run["memory_plan"]["fragmentation_events"]
    assert events
    assert events[0]["before_bitmap"]["text"] != events[0]["after_bitmap"]["text"]
    assert run["memory_plan"]["mixed_rejections"]
    assert run["memory_plan"]["mixed_rejections"][0]["code"] == "MIXED_BATCH_REJECTED_LATENCY_CAP"
    for batch in run["batches"]:
        assert "MEMORY_FRAGMENTATION" in batch["split_reasons"] or batch["split_reasons"]


def test_precision_drift_is_persisted_and_returns_business_code(tmp_path):
    store = Store(tmp_path / "workbench.db")
    service = WorkbenchService(store)
    submitted = service.submit_trace(_sample_payload(service, "precision-drift"))
    with pytest.raises(BusinessError) as raised:
        service.organize(submitted["trace_id"])
    assert raised.value.code == "E_PRECISION_DRIFT"
    assert raised.value.details["persisted"] is True
    run_id = raised.value.details["run_id"]
    run = service.get_run(run_id)
    assert run["state"] == "VALIDATION_FAILED"
    assert run["error_code"] == "E_PRECISION_DRIFT"
    assert run["batches"][0]["state"] == "VALIDATION_FAILED"
    validation = service.run_validation(run_id)
    assert validation["conclusion"] == "FAIL"
    assert validation["failed_count"] >= 1


def test_invalid_trace_is_not_written(tmp_path):
    database = tmp_path / "workbench.db"
    store = Store(database)
    service = WorkbenchService(store)
    payload = _sample_payload(service, "normal")
    payload["trace"]["requests"][0]["model_id"] = "unknown-model"
    with pytest.raises(BusinessError) as raised:
        service.submit_trace(payload)
    assert raised.value.code == "E_SPEC_INVALID"
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM traces").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM intents").fetchone()[0] == 0


def test_forced_termination_then_recovery_marks_partial_run_aborted(tmp_path):
    database = tmp_path / "workbench.db"
    store = Store(database)

    def terminate(_checkpoint: str):
        raise SystemExit("simulated hard termination")

    service = WorkbenchService(store, allow_crash=True, terminate=terminate)
    submitted = service.submit_trace(_sample_payload(service, "normal"))
    with pytest.raises(SystemExit):
        service.organize(submitted["trace_id"], crash_at="mid_quantiles")

    partial = store.list_runs()[0]
    assert partial["state"] == "QUANTILES_PENDING"
    pending = [item for item in store.intent_log() if item["status"] == "PENDING"]
    assert len(pending) == 1
    assert pending[0]["checkpoint"] == "BATCHES_PERSISTED_QUANTILES_PENDING"

    recovered_store = Store(database)
    report = recovered_store.recover_pending_intents()
    assert report["pending_intents_found"] == 1
    assert report["events"][0]["action"] == "RUN_AND_PENDING_BATCHES_ABORTED"
    assert report["events"][0]["batches_marked_aborted"] == 5
    run = recovered_store.get_run(partial["run_id"])
    assert run["state"] == "ABORTED"
    assert run["error_code"] == "E_PROCESS_TERMINATED"
    assert all(batch["state"] == "ABORTED" for batch in run["batches"])

    restarted_service = WorkbenchService(recovered_store)
    with pytest.raises(InsufficientSamplesError) as raised:
        restarted_service.run_quantiles(partial["run_id"])
    assert raised.value.code == "E_INSUFFICIENT_SAMPLES"
    with pytest.raises(InsufficientSamplesError) as raised_batch:
        restarted_service.batch_metrics(run["batches"][0]["batch_uid"])
    assert raised_batch.value.code == "E_INSUFFICIENT_SAMPLES"
    assert report["limitations"]
