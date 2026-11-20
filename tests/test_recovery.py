from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from app.db import connect, get_current_version, snapshot_is_complete, utc_now
from app.errors import BusinessError
from app.service import ConsistencyService


def test_forced_termination_recovery_preserves_last_complete_version(
    root: Path,
    tmp_path: Path,
    baseline_payload: dict,
    adjusted_payload: dict,
) -> None:
    database = tmp_path / "recovery.sqlite3"
    service = ConsistencyService(database)
    service.initialize()
    service.commit_snapshot(baseline_payload, base_version=0)

    env = dict(os.environ)
    env["CONSISTENCY_FAULT_POINT"] = "after_intent_committed"
    process = subprocess.run(
        [
            sys.executable,
            str(root / "scripts" / "crash_worker.py"),
            "--db",
            str(database),
            "--snapshot",
            str(root / "data" / "adjusted.json"),
            "--base-version",
            "1",
        ],
        cwd=str(root),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert process.returncode == 91, process.stderr

    connection = connect(database)
    try:
        assert get_current_version(connection) == 1
        assert connection.execute("SELECT COUNT(*) AS c FROM intents WHERE status = 'started'").fetchone()["c"] == 1
        assert snapshot_is_complete(connection, 1)
    finally:
        connection.close()

    with pytest.raises(BusinessError) as caught:
        service.get_metrics(2)
    assert caught.value.code == "E_VERSION_NOT_FOUND"

    report = service.recover_pending(reason="test_restart")
    assert report["started_intent_count"] == 1
    assert report["current_version_before"] == 1
    assert report["current_version_after"] == 1
    assert report["complete_versions"] == [1]
    assert report["facts_only"] is True
    assert report["actions"][0]["action"] == "marked_intent_aborted_no_snapshot_rows"
    assert any("cannot produce a complete graceful-exit report" in line for line in report["limitations"])

    stored = service.get_recovery_report()
    assert stored["report_id"] == report["report_id"]
    assert stored["current_version_after"] == 1
    assert service.get_current()["current_version"] == 1
    with pytest.raises(BusinessError):
        service.get_metrics(2)


def test_recovery_rolls_back_incomplete_current_pointer(
    tmp_path: Path, baseline_payload: dict
) -> None:
    database = tmp_path / "rollback.sqlite3"
    service = ConsistencyService(database)
    service.initialize()
    service.commit_snapshot(baseline_payload, base_version=0)

    connection = connect(database)
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            """
            INSERT INTO snapshots(
                version, checksum, generated_at, schema_version, component_count,
                payload_json, status, committed_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'committed', ?)
            """,
            (2, "sha256:" + "0" * 64, "2026-11-20T00:00:02Z", "1.0", 3, "{}", utc_now()),
        )
        connection.execute(
            "INSERT INTO intents(version, checksum, operation, status, started_at) VALUES (?, ?, ?, 'started', ?)",
            (2, "sha256:" + "0" * 64, "commit_snapshot", utc_now()),
        )
        connection.execute("UPDATE current_state SET current_version = 2, updated_at = ? WHERE id = 1", (utc_now(),))
        connection.commit()
    finally:
        connection.close()

    report = service.recover_pending(reason="rollback_test")
    assert report["current_version_before"] == 2
    assert report["current_version_after"] == 1
    assert report["aborted_versions"] == [2]
    assert report["actions"][0]["action"] == "truncated_incomplete_snapshot_and_marked_aborted"
    assert report["actions"][-1]["action"] == "rolled_back_current_pointer"
    assert service.get_current()["current_version"] == 1
    assert service.get_metrics(1)["conclusion"]["status"] == "CONSISTENT"
    with pytest.raises(BusinessError):
        service.get_metrics(2)
