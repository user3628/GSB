from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from pdb_engine.service import EngineService

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_PATH = PROJECT_ROOT / "samples" / "scenario.json"


def test_forced_termination_recovery_report(tmp_path) -> None:
    db_path = tmp_path / "crash.sqlite3"
    command = [
        sys.executable,
        "-m",
        "scripts.crash_worker",
        "--db",
        str(db_path),
        "--sample",
        str(SAMPLE_PATH),
        "--node",
        "worker-a",
        "--request-id",
        "crash-drain",
        "--after-steps",
        "1",
    ]
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(PROJECT_ROOT)
    child = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert child.returncode == 97, child.stderr

    engine = EngineService(db_path)
    report = engine.initialize()
    assert report is not None
    assert report["facts_scope"] == "PERSISTED_ONLY"
    assert report["complete_exit_report"] is False
    assert report["aborted_drains"][0]["request_id"] == "crash-drain"
    assert report["aborted_drains"][0]["status_after"] == "ABORTED"
    assert report["aborted_drains"][0]["last_complete_step"]["pod"] == "web-a-alpha"

    pdb_state = engine.repo.get_pdb_state()
    assert pdb_state["web-a-budget"]["current_available"] == 2
    assert pdb_state["web-critical-budget"]["current_available"] == 3
    assert report["pdb_availability"]["web-a-budget"]["restored_available"] == 2

    detail = engine.get_drain_detail("crash-drain")
    assert detail["status"] == "ABORTED"
    assert len(detail["steps"]) == 1
    assert detail["steps"][0]["outcome"] == "ALLOWED"
    assert detail["recovery_hint"] == "GET /api/v1/recovery/report"


def test_pending_intent_is_aborted_without_pdb_change(tmp_path) -> None:
    db_path = tmp_path / "pending.sqlite3"
    engine = EngineService(db_path)
    engine.initialize()
    engine.load_sample(str(SAMPLE_PATH))
    drain = engine.repo.create_drain("pending-intent", "worker-a")
    engine.repo.update_drain_status(drain["id"], "RUNNING")
    before = engine.repo.get_pdb_state()
    engine.repo.insert_step_intent(
        drain["id"],
        1,
        "web-a-alpha",
        "worker-a",
        {name: item["current_available"] for name, item in before.items()},
    )

    restarted = EngineService(db_path)
    report = restarted.initialize()
    assert report is not None
    assert report["aborted_drains"][0]["pending_intent_count"] == 1
    assert report["aborted_drains"][0]["last_complete_step"] is None
    after = restarted.repo.get_pdb_state()
    assert after == before
    detail = restarted.get_drain_detail("pending-intent")
    assert detail["status"] == "ABORTED"
    assert detail["steps"] == []

