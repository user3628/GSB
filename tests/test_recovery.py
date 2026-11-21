from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from prep_pipeline.errors import PipelineError
from prep_pipeline.service import PipelineService

from tests.helpers import REPO_ROOT, batch_request, make_settings


def test_forced_termination_recovers_to_last_complete_stage(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    service = PipelineService(settings)
    service.bootstrap_policies(
        [settings.data_dir / "policy_v1.json", settings.data_dir / "policy_v2.json"]
    )
    env = os.environ.copy()
    env.update(
        {
            "PREP_ROOT": str(tmp_path),
            "PREP_DATA_DIR": str(settings.data_dir),
            "PREP_ARTIFACT_DIR": str(settings.artifact_dir),
            "PREP_DB_PATH": str(settings.db_path),
        }
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "prep_pipeline.cli",
            "run-batch",
            "--batch-id",
            "crash-batch",
            "--policy-version",
            "1",
            "--input",
            "records_valid.tsv",
            "--hard-crash-after-stage",
            "filter",
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 71, (completed.stdout, completed.stderr)

    before = service.db.get_batch("crash-batch")
    assert before["status"] == "running"
    attempt_root = settings.staging_dir / before["attempt_id"]
    assert attempt_root.is_dir()
    facts_before = service.db.get_stage_facts("crash-batch")
    assert [fact["stage_name"] for fact in facts_before] == ["parse_clean", "filter"]
    assert all(fact["status"] == "completed" for fact in facts_before)

    restarted = PipelineService(settings)
    reports = restarted.recover()
    assert len(reports) == 1
    report = reports[0]
    assert report["batch_id"] == "crash-batch"
    assert report["previous_status"] == "running"
    assert report["recovered_status"] == "aborted"
    assert report["last_complete_stage"]["stage"] == "filter"
    assert report["final_commit_observed"] is False
    assert report["exit_report_available"] is False
    assert report["clean_shutdown_report_available"] is False
    assert report["committed_shards"] == []
    assert report["uncommitted_artifacts_recoverable"] is False
    assert report["filesystem_facts"]["staging_removed"] is True
    assert report["filesystem_facts"]["removal_incomplete"] is False

    after = restarted.db.get_batch("crash-batch")
    assert after["status"] == "aborted"
    assert after["error_code"] == "E_RECOVERY_ABORTED"
    assert restarted.db.get_shard_rows("crash-batch") == []
    assert not attempt_root.exists()
    with restarted.db.read_transaction() as conn:
        assert conn.execute("SELECT COUNT(*) AS n FROM policy_locks").fetchone()["n"] == 0
    with_service_error = None
    try:
        restarted.get_batch("crash-batch")
    except PipelineError as exc:
        with_service_error = exc
    assert with_service_error is not None
    assert with_service_error.code == "E_BATCH_NOT_COMMITTED"

    successor = restarted.submit_batch(batch_request("after-recovery"))
    assert successor["status"] == "committed"
    stored_reports = restarted.recovery_reports("crash-batch")
    assert stored_reports[0]["report_id"] == report["report_id"]
