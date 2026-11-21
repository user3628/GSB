from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from obs_sampler.errors import BusinessError
from obs_sampler.models import BatchRequest
from obs_sampler.service import SamplerService

from .conftest import ROOT, SAMPLES, make_log, sample_policy_object


def _seed_database(db_path: Path) -> None:
    service = SamplerService(db_path)
    policy = sample_policy_object(rates={"checkout": {"INFO": 0.5}})
    service.submit_policy(policy.document)
    service.submit_batch(
        BatchRequest(
            batch_id="recovery-base",
            logs=[make_log(index, trace_id=f"recover-{index}") for index in range(40)],
        )
    )


def _crash_policy_commit(db_path: Path) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT) + (
        os.pathsep + environment["PYTHONPATH"] if environment.get("PYTHONPATH") else ""
    )
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "obs_sampler.crash_worker",
            "--db",
            str(db_path),
            "--policy",
            str(SAMPLES / "policy_v2.json"),
            "--stage",
            "after_counts_and_reports_before_commit_marker",
            "--exit-code",
            "87",
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def test_forced_termination_mid_recompute_rolls_back_and_aborts_intent(tmp_path):
    db_path = tmp_path / "crash.sqlite3"
    _seed_database(db_path)
    before = SamplerService(db_path).get_policy_state(1)

    completed = _crash_policy_commit(db_path)
    assert completed.returncode == 87, (completed.stdout, completed.stderr)
    assert "HARD_PROCESS_EXIT" in completed.stdout

    restarted = SamplerService(db_path)
    assert restarted.storage.current_policy_version() == 1
    with pytest.raises(BusinessError) as missing:
        restarted.get_policy(2)
    assert missing.value.code == "E_POLICY_NOT_FOUND"

    after = restarted.get_policy_state(1)
    assert after["commit"]["decision_digest"] == before["commit"]["decision_digest"]
    assert after["commit"]["counts_digest"] == before["commit"]["counts_digest"]
    assert after["commit"]["report_digest"] == before["commit"]["report_digest"]
    assert after["decision_states"] == before["decision_states"]

    recovery = restarted.get_recovery_report()
    assert recovery["scope"] == "COMMITTED_SQLITE_FACTS_ONLY"
    assert recovery["forced_termination_exit_report_guaranteed"] is False
    assert recovery["current_policy_version"] == 1
    assert recovery["intents"]["running_found"] == 1
    assert len(recovery["intents"]["aborted"]) == 1
    assert recovery["intents"]["aborted"][0]["kind"] == "POLICY_COMMIT"
    assert recovery["intents"]["aborted"][0]["policy_version"] == 2
    assert [row["version"] for row in recovery["committed_policy_versions"]] == [1]
    assert recovery["integrity"]["foreign_key_check"] == []
    assert recovery["integrity"]["current_has_policy_and_derived_commit"] is True
    assert any("No complete graceful-exit report" in item for item in recovery["limitations"])

    connection = sqlite3.connect(db_path)
    try:
        assert connection.execute("SELECT COUNT(*) FROM policies WHERE version = 2").fetchone()[0] == 0
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM decisions WHERE policy_version = 2"
            ).fetchone()[0]
            == 0
        )
    finally:
        connection.close()

    # The aborted intent no longer blocks a clean retry of version 2.
    retry = restarted.submit_policy(
        __import__("json").loads((SAMPLES / "policy_v2.json").read_text(encoding="utf-8"))
    )
    assert retry["status"] == "COMMITTED"
    assert restarted.storage.current_policy_version() == 2
    second_scan = restarted.recover()
    assert second_scan["intents"]["running_found"] == 0
    assert second_scan["current_policy_version"] == 2


def test_recovery_report_does_not_claim_complete_inflight_derivations(tmp_path):
    db_path = tmp_path / "facts.sqlite3"
    _seed_database(db_path)
    completed = _crash_policy_commit(db_path)
    assert completed.returncode == 87

    restarted = SamplerService(db_path)
    recovery = restarted.get_recovery_report()
    assert recovery["crash_during_write_detected"] is True
    assert recovery["facts"]["record_count"] == 40
    assert sum(recovery["facts"]["current_decision_states"].values()) == 40
    assert recovery["current_commit"]["record_count"] == 40
    assert all("report" not in intent for intent in recovery["intents"]["aborted"])


def test_recovery_reconciles_missing_cursor_to_last_complete_commit(tmp_path):
    db_path = tmp_path / "cursor.sqlite3"
    _seed_database(db_path)
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(
            "DELETE FROM app_meta WHERE key = 'current_policy_version'"
        )
        connection.commit()
    finally:
        connection.close()

    restarted = SamplerService(db_path)
    recovery = restarted.get_recovery_report()
    assert recovery["current_policy_version"] == 1
    assert recovery["cursor_reconciled_to"] == 1
    assert recovery["integrity"]["current_has_policy_and_derived_commit"] is True
    assert restarted.get_policy_state(1)["is_current"] is True
