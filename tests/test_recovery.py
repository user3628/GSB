from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import create_app
from app.store import Store


ROOT = Path(__file__).resolve().parent.parent


def test_forced_termination_mid_rebuild_recovers_to_last_complete(db_path):
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "app.chaos",
            "--db",
            str(db_path),
            "--fault",
            "mid_rebuild",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode != 0, completed.stdout + completed.stderr

    store = Store(db_path)
    store.initialize()
    assert store.current_version() == 2
    pending = store.pending_intents()
    assert len(pending) == 1
    assert pending[0]["commit_id"] == "demo-inc-prod-inventory-image"
    assert pending[0]["target_version"] == 3

    app = create_app(db_path)
    with TestClient(app) as client:
        report = client.get("/api/recovery").json()["data"]
        assert report["current_version"] == 2
        assert report["last_complete_version"] == 2
        assert report["pending_intents"] == 0
        assert report["exit_report_guaranteed"] is False
        assert report["basis"] == "sqlite_committed_rows_only"
        assert len(report["recovered_events"]) == 1
        event = report["recovered_events"][0]
        assert event["commit_id"] == "demo-inc-prod-inventory-image"
        assert event["last_complete_version"] == 2
        assert event["facts"]["recovery_action"] == "intent_aborted_no_uncommitted_rows_visible"

        environments = client.get("/api/environments").json()["data"]
        assert environments["snapshot_version"] == 2
        prod = client.get("/api/environments/prod/bundle").json()["data"]
        assert [item["service"] for item in prod["plan"]] == [
            "identity-db",
            "ledger",
            "auth",
            "inventory",
            "payments",
            "orders",
            "gateway",
        ]
        assert prod["snapshot_version"] == 2
        assert prod["topology"]["complete"] is True

    store = Store(db_path)
    store.initialize()
    second_scan = store.recover_pending_intents()
    assert second_scan["scanned_pending"] == 0
    assert store.current_version() == 2


def test_after_intent_fault_also_recovers(db_path):
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "app.chaos",
            "--db",
            str(db_path),
            "--fault",
            "after_intent",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode != 0
    store = Store(db_path)
    store.initialize()
    assert store.current_version() == 2
    assert len(store.pending_intents()) == 1
    recovered = store.recover_pending_intents()
    assert recovered["scanned_pending"] == 1
    assert recovered["current_version"] == 2
    assert not store.pending_intents()
