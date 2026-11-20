from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from confighub.service import ConfigService


ROOT = Path(__file__).resolve().parent.parent


def test_forced_process_exit_then_restart_recovers_from_committed_facts(
    tmp_path: Path, endpoint
):
    crash_db = tmp_path / "crash.sqlite"
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "crash_worker.py"),
            "--db",
            str(crash_db),
            "--endpoint",
            endpoint.push_url("crash"),
            "--crash-code",
            "37",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 37, completed.stderr
    assert "after_dispatch_records_committed" in completed.stdout

    restarted = ConfigService(crash_db)
    report = restarted.recover()
    assert report["conclusion"] == "RECOVERED_INCOMPLETE_WORK"
    assert report["aborted_batches"]
    crash_batch = next(row for row in report["aborted_batches"] if row["target_version"] == 2)
    assert crash_batch["reason"] == "RECOVERY_INCOMPLETE_BATCH"
    assert crash_batch["clients"][0]["state"] == "ABORTED"
    assert restarted.get_client("crash-client")["applied_version"] == 1
    assert restarted.storage.query_one(
        "SELECT state FROM batches WHERE target_version=2"
    )["state"] == "ABORTED"
    assert restarted.storage.query_one(
        "SELECT state FROM push_records WHERE client_id='crash-client' AND version=2"
    )["state"] == "ABORTED"
    assert restarted.storage.query_all(
        "SELECT * FROM push_records WHERE state IN ('PENDING','PUSHED','ACKED')"
    ) == []
    assert report["visibility_violations"] == []
    assert report["client_facts_consistent"] is True
    assert restarted.storage.latest_recovery_report() is not None

