from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from schedsim.api import create_app
from schedsim.engine import SchedulingEngine

REPO_ROOT = Path(__file__).resolve().parents[1]


def _run_crash(db_path: Path, batch_id: str, *extra: str) -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        "-m",
        "schedsim.cli",
        "run-sample",
        "--db",
        str(db_path),
        "--samples",
        str(REPO_ROOT / "samples"),
        "--sample",
        "default",
        "--batch-id",
        batch_id,
        *extra,
    ]
    return subprocess.run(
        command,
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )


def test_forced_kill_after_two_persisted_placements_is_recoverable(tmp_path) -> None:
    db_path = tmp_path / "crash.db"
    crashed = _run_crash(db_path, "crash-batch", "--crash-after-placements", "2")
    assert crashed.returncode == 92, crashed.stderr

    before_recovery = SchedulingEngine(db_path)
    placements_before = before_recovery.storage.get_placements("crash-batch")
    pending = before_recovery.storage.find_pending_intents()
    assert len(placements_before) == 2
    assert len(pending) == 1
    assert pending[0]["operation"] == "RUN_ROUND"
    assert before_recovery.storage.get_batch("crash-batch")["status"] == "RUNNING"
    assert before_recovery.storage.get_recoveries("crash-batch") == []

    reports = before_recovery.recover_pending()
    assert len(reports) == 1
    report = reports[0]["report"]
    assert report["factual_only"] is True
    assert report["exit_report_available"] is False
    assert report["exit_report"] is None
    assert report["process_exit_observed"] is False
    assert len(report["persisted_facts"]["placements"]) == 2
    assert report["batch_status_after"] == "INCOMPLETE"
    assert before_recovery.storage.get_batch("crash-batch")["status"] == "INCOMPLETE"
    assert before_recovery.storage.find_pending_intents() == []

    placements_after_recovery = before_recovery.storage.get_placements("crash-batch")
    assert [item.placement_id for item in placements_after_recovery] == [
        item.placement_id for item in placements_before
    ]
    interrupted_rounds = [
        row for row in before_recovery.storage.get_rounds("crash-batch") if row["status"] == "INTERRUPTED"
    ]
    assert len(interrupted_rounds) == 1

    for _ in range(20):
        if before_recovery.get_batch_summary("crash-batch")["status"] == "COMPLETED":
            break
        before_recovery.run_round("crash-batch")
    assert before_recovery.get_batch_summary("crash-batch")["status"] == "COMPLETED"
    final_placements = before_recovery.storage.get_placements("crash-batch")
    assert len(final_placements) > 2
    results = before_recovery.get_results("crash-batch")
    assert all(value["cpu_millicores"] >= 0 for value in results["remaining"].values())
    assert all(value["memory_bytes"] >= 0 for value in results["remaining"].values())
    recovery_view = before_recovery.get_recovery_reports("crash-batch")
    assert recovery_view["recovery_report_available"] is True
    assert recovery_view["exit_report_available"] is False
    assert recovery_view["exit_report"] is None


def test_forced_kill_after_intent_has_no_fabricated_placements(tmp_path) -> None:
    db_path = tmp_path / "intent-crash.db"
    crashed = _run_crash(db_path, "intent-batch", "--crash-after-intent")
    assert crashed.returncode == 91, crashed.stderr

    engine = SchedulingEngine(db_path)
    assert engine.storage.get_placements("intent-batch") == []
    assert len(engine.storage.find_pending_intents()) == 1
    reports = engine.recover_pending()
    assert len(reports) == 1
    assert reports[0]["report"]["persisted_facts"]["placements"] == []
    assert reports[0]["report"]["exit_report"] is None
    assert engine.get_batch_summary("intent-batch")["status"] == "INCOMPLETE"

    for _ in range(20):
        if engine.get_batch_summary("intent-batch")["status"] == "COMPLETED":
            break
        engine.run_round("intent-batch")
    assert engine.get_batch_summary("intent-batch")["status"] == "COMPLETED"
    assert len(engine.storage.get_placements("intent-batch")) > 0


def test_api_startup_automatically_scans_pending_intents(tmp_path) -> None:
    db_path = tmp_path / "api-recovery.db"
    crashed = _run_crash(db_path, "api-crash", "--crash-after-placements", "1")
    assert crashed.returncode == 92, crashed.stderr

    app = create_app(
        db_path=str(db_path), sample_dir=str(REPO_ROOT / "samples"), auto_recover=True
    )
    with TestClient(app) as client:
        batch = client.get("/api/v1/batches/api-crash")
        assert batch.status_code == 200
        assert batch.json()["status"] == "INCOMPLETE"
        recovery = client.get("/api/v1/batches/api-crash/recovery").json()
        assert recovery["recovery_report_available"] is True
        assert recovery["reports"][0]["report"]["exit_report_available"] is False
        assert recovery["reports"][0]["report"]["exit_report"] is None
        startup = client.get("/api/v1/recovery").json()
        assert startup["recovery_report_count"] >= 1
        assert startup["startup_recovery_reports"]

