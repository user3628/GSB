from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from tests.conftest import ROOT, SAMPLES, state_fingerprint


def _event(time_ms: int, tenant: str = "child", payload: int = 1000) -> dict:
    return {
        "event_time_ms": time_ms,
        "tenant_id": tenant,
        "meter_mode": "BYTES",
        "io_direction": "WRITE",
        "payload_bytes": payload,
        "request_weight": 0,
    }


def test_state_machine_illegal_transitions_and_ancestor_suspension(client: TestClient) -> None:
    suspended = client.post(
        "/api/v1/tenants/child/suspend", json={"reason": "operator drill"}
    )
    assert suspended.status_code == 200
    assert suspended.json()["previous_status"] == "ACTIVE"
    assert suspended.json()["status"] == "SUSPENDED"

    illegal = client.post("/api/v1/tenants/child/suspend", json={"reason": "again"})
    assert illegal.status_code == 409
    assert illegal.json()["code"] == "E_INVALID_TRANSITION"

    before = state_fingerprint(client)
    response = client.post(
        "/api/v1/batches",
        json={"batch_id": "child-suspended", "events": [_event(1001000)]},
    )
    assert response.status_code == 201
    assert response.json()["events"][0]["code"] == "E_TENANT_SUSPENDED"
    after = state_fingerprint(client)
    assert after["child"]["used_bytes_units"] == before["child"]["used_bytes_units"]

    resume = client.post("/api/v1/tenants/child/resume", json={"at_ms": 1002000})
    assert resume.status_code == 200
    assert resume.json()["status"] == "ACTIVE"
    assert resume.json()["recovery_evidence"]["tokens_remaining"] == "3.000000"

    illegal_resume = client.post("/api/v1/tenants/child/resume", json={"at_ms": 1003000})
    assert illegal_resume.status_code == 409
    assert illegal_resume.json()["code"] == "E_INVALID_TRANSITION"

    assert client.post("/api/v1/tenants/root/suspend", json={"reason": "root drill"}).status_code == 200
    response = client.post(
        "/api/v1/batches",
        json={"batch_id": "parent-suspended", "events": [_event(1004000)]},
    )
    assert response.status_code == 201
    assert response.json()["events"][0]["code"] == "E_PARENT_SUSPENDED"

    assert client.post("/api/v1/tenants/root/resume", json={"at_ms": 1005000}).status_code == 200


def test_forced_process_termination_then_restart_recovery(tmp_path: Path) -> None:
    database = tmp_path / "crash.db"
    crash = subprocess.run(
        [
            sys.executable,
            "-m",
            "app.cli",
            "crash-batch",
            "--database",
            str(database),
            "--samples",
            str(SAMPLES),
            "--batch-file",
            str(SAMPLES / "crash_batch.json"),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert crash.returncode == 91, (crash.stdout, crash.stderr)

    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    pending = connection.execute(
        "SELECT * FROM intents WHERE status='PENDING'"
    ).fetchall()
    assert len(pending) == 1
    assert pending[0]["batch_id"] == "crash-recovery-batch-001"
    assert connection.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 0
    state = connection.execute(
        "SELECT * FROM tenant_states WHERE tenant_id='isolated'"
    ).fetchone()
    assert state["direct_used_bytes_units"] == 0
    assert state["tokens_units"] == 2_000_000
    connection.close()

    # Restarting the app opens the same SQLite file, scans pending intent facts,
    # rolls back the affected tenant view and marks the batch aborted.
    from app.config import Settings
    from app.main import create_app

    app = create_app(
        Settings(
            database_path=database,
            samples_dir=SAMPLES,
            auto_seed=True,
            auto_recover=True,
            throttle_rejection_threshold=3,
        )
    )
    with TestClient(app) as restarted:
        report = restarted.get("/api/v1/recovery/report").json()
        assert report["report_id"] is not None
        assert "crash-recovery-batch-001" in report["aborted_batches"]
        assert "crash-recovery-batch-001" in report["rolled_back_batches"]
        assert any(
            item["batch_id"] == "crash-recovery-batch-001"
            for item in report["pending_intents"]
        )
        assert restarted.get("/api/v1/intents", params={"status": "PENDING"}).json()[
            "intents"
        ] == []
        tenant = restarted.get("/api/v1/tenants/isolated").json()
        assert tenant["state"]["used_bytes_units"] == 0
        assert tenant["state"]["tokens_units"] == 2_000_000
        assert restarted.get("/api/v1/batches/crash-recovery-batch-001").status_code == 404
        assert restarted.get("/health").json()["pending_intents"] == 0


def test_throttled_state_and_successful_recovery(client: TestClient) -> None:
    patch = {
        "base_version": 1,
        "generated_at_ms": 1000500,
        "tenant_patches": [
            {"tenant_id": "isolated", "requests_per_second": 1, "burst_capacity": 1}
        ],
    }
    assert client.patch("/api/v1/policy", json=patch).status_code == 200
    events = [
        {
            "event_time_ms": 1001000,
            "tenant_id": "isolated",
            "meter_mode": "REQUESTS",
            "io_direction": "READ",
            "payload_bytes": 0,
            "request_weight": 1,
        }
        for _ in range(4)
    ]
    response = client.post(
        "/api/v1/batches", json={"batch_id": "throttle-sequence", "events": events}
    )
    assert response.status_code == 201
    body = response.json()
    assert [item["code"] for item in body["events"]] == [
        "OK",
        "E_RATE_LIMITED",
        "E_RATE_LIMITED",
        "E_RATE_LIMITED",
    ]
    assert body["events"][-1]["tenant_status"] == "THROTTLED"

    recovery_event = {
        "event_time_ms": 1002000,
        "tenant_id": "isolated",
        "meter_mode": "REQUESTS",
        "io_direction": "READ",
        "payload_bytes": 0,
        "request_weight": 1,
    }
    recovered = client.post(
        "/api/v1/batches",
        json={"batch_id": "throttle-recovery", "events": [recovery_event]},
    ).json()
    assert recovered["events"][0]["code"] == "OK"
    assert recovered["events"][0]["tenant_status"] == "ACTIVE"


def test_forced_termination_during_policy_rebuild_rolls_back(tmp_path: Path) -> None:
    database = tmp_path / "rebuild-crash.db"
    crash = subprocess.run(
        [
            sys.executable,
            "-m",
            "app.cli",
            "crash-rebuild",
            "--database",
            str(database),
            "--samples",
            str(SAMPLES),
            "--patch-file",
            str(SAMPLES / "incremental_patch.json"),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert crash.returncode == 92, (crash.stdout, crash.stderr)

    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    assert connection.execute("SELECT MAX(version) FROM policy_snapshots").fetchone()[0] == 1
    assert len(connection.execute("SELECT * FROM intents WHERE status='PENDING'").fetchall()) == 1
    connection.close()

    from app.config import Settings
    from app.main import create_app

    app = create_app(
        Settings(
            database_path=database,
            samples_dir=SAMPLES,
            auto_seed=True,
            auto_recover=True,
            throttle_rejection_threshold=3,
        )
    )
    with TestClient(app) as restarted:
        report = restarted.get("/api/v1/recovery/report").json()
        assert report["restored_policy_versions"] == [1]
        assert report["pending_intents"][0]["kind"] == "POLICY"
        assert restarted.get("/api/v1/policy").json()["version"] == 1
        assert restarted.get("/api/v1/intents", params={"status": "PENDING"}).json()[
            "intents"
        ] == []
        child = restarted.get("/api/v1/tenants/child").json()
        assert child["state"]["used_bytes_units"] == 0
        assert child["state"]["tokens_units"] == 3_000_000
