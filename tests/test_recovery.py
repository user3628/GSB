from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from slowquery.config import WorkbenchConfig
from slowquery.service import SlowQueryService


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"


def _seed(config: WorkbenchConfig) -> SlowQueryService:
    service = SlowQueryService(config)
    service.submit_samples(json.loads((DATA_DIR / "samples.json").read_text()))
    service.load_snapshot(json.loads((DATA_DIR / "snapshot_v1.json").read_text()))
    return service


def _crash(config: WorkbenchConfig, session_id: str, operation: str, point: str) -> None:
    command = [
        sys.executable,
        str(ROOT / "scripts" / "crash_worker.py"),
        "--database",
        str(config.database_path),
        "--sample-db",
        str(config.sample_database_path),
        "--session-id",
        session_id,
        "--operation",
        operation,
        "--crash-point",
        point,
    ]
    completed = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=20,
    )
    assert completed.returncode == 86, completed.stderr


def test_interrupted_analysis_restarts_as_created_with_facts_only_report(
    config: WorkbenchConfig,
) -> None:
    service = _seed(config)
    service.create_session(session_id="crash-analysis", sample_id="sq-missing-index")
    _crash(config, "crash-analysis", "diagnose", "ANALYZE_AFTER_ANALYZING")

    assert service.repo.require_session("crash-analysis")["status"] == "ANALYZING"
    assert [(item["kind"], item["checkpoint"]) for item in service.repo.pending_intents()] == [
        ("ANALYZE", "SESSION_ANALYZING")
    ]

    restarted = SlowQueryService(config)
    session = restarted.get_session("crash-analysis")
    assert session["status"] == "CREATED"
    assert session["snapshot_version"] is None
    assert session["suggestions"] == []
    assert session["last_error_code"] == "E_RECOVERED_INCOMPLETE_ANALYSIS"
    assert restarted.repo.pending_intents() == []

    report = restarted.get_recovery_report()
    payload = report["payload"]
    assert payload["facts_only"] is True
    assert payload["pending_intents_scanned"] == 1
    assert payload["recoveries"][0]["action"] == "SESSION_RESTORED_TO_CREATED"
    assert payload["recoveries"][0]["final_status"] == "CREATED"
    assert payload["facts"]["invariants"]["pending_intents_remaining"] == 0
    assert payload["facts"]["invariants"]["verified_without_verification_record"] == 0
    assert payload["guarantees"]["interrupted_verification_marked_verified"] is False


def test_interrupted_verification_restarts_as_failed_never_verified(
    config: WorkbenchConfig,
) -> None:
    service = _seed(config)
    service.create_session(session_id="crash-verify", sample_id="sq-missing-index")
    service.diagnose("crash-verify")
    _crash(config, "crash-verify", "verify", "VERIFY_AFTER_ANALYZING")

    assert service.repo.require_session("crash-verify")["status"] == "ANALYZING"
    assert service.repo.list_verifications("crash-verify") == []

    restarted = SlowQueryService(config)
    session = restarted.get_session("crash-verify")
    assert session["status"] == "FAILED"
    assert session["last_error_code"] == "E_RECOVERED_INCOMPLETE_VERIFICATION"
    assert session["verifications"] == []
    assert session["suggestions"][0]["verification_status"] == "PROPOSED"

    report = restarted.get_recovery_report()["payload"]
    assert report["recoveries"][0]["action"] == "SESSION_FAILED_NOT_VERIFIED"
    assert report["recoveries"][0]["previous_status"] == "ANALYZING"
    assert report["recoveries"][0]["final_status"] == "FAILED"
    assert report["guarantees"]["interrupted_verification_marked_verified"] is False
    assert report["facts"]["invariants"]["pending_intents_remaining"] == 0


def test_crash_before_analyzing_commit_leaves_session_created(
    config: WorkbenchConfig,
) -> None:
    service = _seed(config)
    service.create_session(session_id="crash-intent", sample_id="sq-missing-index")
    _crash(config, "crash-intent", "diagnose", "ANALYZE_AFTER_INTENT")
    assert service.repo.require_session("crash-intent")["status"] == "CREATED"
    restarted = SlowQueryService(config)
    assert restarted.get_session("crash-intent")["status"] == "CREATED"
    recovery = restarted.get_recovery_report()["payload"]["recoveries"][0]
    assert recovery["action"] == "INTENT_DISCARDED_SESSION_REMAINS_CREATED"
