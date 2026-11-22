from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

from app.service import GovernanceService
from tests.conftest import ROOT, T0, sample


def request(request_id: str, arrival: int) -> dict:
    return {
        "request_id": request_id,
        "tenant_id": "starter",
        "namespace": "shared-prod",
        "arrival_ms": arrival,
        "cpu_millicores": 100,
        "memory_bytes": 1024,
    }


def test_forced_termination_after_intent_recovers_last_complete_revision(tmp_path):
    db_path = tmp_path / "crash.sqlite"
    svc = GovernanceService(str(db_path))
    svc.startup()
    svc.load_snapshot(sample("policies_hierarchical_v2.json"))
    accepted = svc.submit_decision(request("before-crash", T0 + 500))
    assert accepted.decision["decision"] == "ACCEPTED"
    baseline_revision = svc.meta()["revision"]
    baseline_snapshot = svc.current_snapshot()
    baseline_tenants = {
        item["tenant_id"]: item for item in svc.list_tenants()["tenants"]
    }
    baseline_starter = baseline_tenants["starter"]

    process = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "crash_probe.py"),
            "--db",
            str(db_path),
            "--request-id",
            "crash-request",
            "--arrival-ms",
            str(T0 + 1000),
            "--cpu",
            "100",
            "--memory",
            "1024",
        ],
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert process.returncode == 91, process.stderr
    assert "CRASH_PROBE_READY" in process.stdout

    with sqlite3.connect(db_path) as conn:
        pending = conn.execute(
            "SELECT intent_id, kind, status, base_revision FROM intents WHERE status='PENDING'"
        ).fetchall()
        assert len(pending) == 1
        assert pending[0][1] == "DECISION"
        assert pending[0][3] == baseline_revision
        request_count = conn.execute(
            "SELECT COUNT(*) FROM decisions WHERE request_id='crash-request'"
        ).fetchone()[0]
        assert request_count == 0

    recovered = GovernanceService(str(db_path))
    recovery = recovered.startup()
    assert recovery["pending_found"] == 1
    assert recovery["aborted_count"] == 1
    assert recovery["last_complete_revision"] == baseline_revision
    assert recovery["conclusion"] == (
        "UNCOMMITTED_INTENTS_ABORTED_LAST_COMPLETE_REVISION_PRESERVED"
    )
    assert recovered.meta()["revision"] == baseline_revision
    assert recovered.current_snapshot()["version"] == baseline_snapshot["version"]

    after_tenants = {
        item["tenant_id"]: item for item in recovered.list_tenants()["tenants"]
    }
    after_starter = after_tenants["starter"]
    assert after_starter["usage"] == baseline_starter["usage"]
    assert after_starter["token_bucket"] == baseline_starter["token_bucket"]
    assert recovered.list_decisions(page_size=100)["total"] == 1

    latest = recovered.get_latest_recovery()
    assert latest["replayable"] is True
    assert latest["report"]["pending_found"] == 1
    facts = latest["report"]["facts"]
    assert facts[0]["action"] == "MARKED_ABORTED"
    assert facts[0]["business_state_rollback_required"] is False

    with sqlite3.connect(db_path) as conn:
        statuses = [row[0] for row in conn.execute("SELECT status FROM intents ORDER BY created_at_ms")]
        assert "PENDING" not in statuses
        assert statuses.count("ABORTED") == 1
