from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from gateway.config import GatewayConfig
from gateway.domain import InferenceRequestSpec, normalize_route_document
from gateway.service import GatewayService
from gateway.store import Store
from gateway.validation import validate_database
from tests.helpers import sample_route


def _prepare_dispatched_without_worker(tmp_path: Path, backend: str, *, cancelled: bool = False):
    db_path = str(tmp_path / "recovery.db")
    config = GatewayConfig(db_path=db_path, global_concurrency_limit=2)
    store = Store(config)
    route, persisted, checksum = normalize_route_document(sample_route(backend))
    source = sample_route(backend)
    store.submit_route(route, persisted, checksum, source_document=source)
    spec = InferenceRequestSpec(
        request_id="recover-me",
        model_id="alpha",
        sample_count=4,
        latency_cap_ms=5_000,
        priority=50,
        tenant_id="tenant-recovery",
        fault_mode="hang",
    )
    store.create_request(spec, route)
    round_row = store.create_round(500, route.version)
    dispatched = store.try_dispatch(
        "recover-me",
        round_id=round_row["id"],
        global_limit=2,
        remaining_budget_ms=500,
        dispatch_seq=1,
    )
    assert dispatched["ok"] is True
    if cancelled:
        store.request_cancel("recover-me", "cancel-before-crash")
    # Simulate a crash after the dispatch transaction but before the worker
    # releases resources by closing the SQLite handle without finishing it.
    with store._write() as conn:
        Store._open_intent_locked(
            conn,
            kind="DISPATCH",
            request_id="recover-me",
            phase="INTENT_COMMITTED",
            payload={"crash_simulation": True},
        )
    store.close()
    return db_path, config


def test_startup_requeues_dispatched_request_and_rebuilds_counters(tmp_path, backend):
    db_path, config = _prepare_dispatched_without_worker(tmp_path, backend)
    service = GatewayService(config)
    report = service.startup()
    assert report["requeued_count"] == 1
    assert report["aborted_count"] == 0
    assert report["open_intent_count"] == 1
    assert report["source"] == "persisted_facts_only"
    request = service.store.get_request("recover-me")
    assert request["status"] == "QUEUED"
    assert request["recovery_count"] == 1
    assert request["vram_reserved_mb"] == 0
    runtime = service.store.backend_status(route_version=1)
    assert all(row["in_flight"] == 0 and row["vram_reserved_mb"] == 0 for row in runtime)
    service.shutdown()
    validation = validate_database(db_path, config)
    assert validation["verdict"] == "PASS", validation


def test_startup_aborts_cancelled_dispatched_request(tmp_path, backend):
    db_path, config = _prepare_dispatched_without_worker(tmp_path, backend, cancelled=True)
    service = GatewayService(config)
    report = service.startup()
    assert report["aborted_count"] == 1
    assert report["requeued_count"] == 0
    request = service.store.get_request("recover-me")
    assert request["status"] == "ABORTED"
    assert request["error_code"] == "E_RECOVERY_ABORTED"
    service.shutdown()
    assert validate_database(db_path, config)["verdict"] == "PASS"


def test_validator_detects_counter_breakage(gateway_factory, backend):
    client, config = gateway_factory()
    assert client.post("/routes", json=sample_route(backend)).status_code == 200
    with sqlite3.connect(config.resolved_db_path) as conn:
        conn.execute(
            "UPDATE backend_runtime SET in_flight=1, vram_reserved_mb=8 WHERE backend_id='backend-a'"
        )
        conn.commit()
    report = client.get("/validation/report").json()["report"]
    assert report["verdict"] == "FAIL"
    failed_ids = {item["id"] for item in report["failures"]}
    assert "reservation_closure" in failed_ids


def test_validation_snapshot_is_structured_and_reports_recovery(gateway_factory, backend):
    client, _ = gateway_factory()
    assert client.post("/routes", json=sample_route(backend)).status_code == 200
    report = client.get("/recovery/report").json()
    assert report["source"] == "persisted_facts_only"
    assert report["report"]["requeued_count"] == 0
    validation = client.get("/validation/report").json()["report"]
    assert validation["summary"]["latest_recovery_report_id"] == report["report"]["id"]

