from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from mv_workbench.service import WorkbenchService

from .conftest import ROOT, SAMPLES


def test_forced_termination_is_recovered_as_partial(tmp_path: Path) -> None:
    db_path = tmp_path / "crash.db"
    service = WorkbenchService(db_path, samples_dir=SAMPLES)
    service.initialize()
    service.refresh("recovery-bootstrap", mode="all")
    before = service.query_view("v_region_revenue")
    assert before["state"] == "FRESH"

    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "mv_workbench.crash_worker",
            "--db",
            str(db_path),
            "--request-id",
            "forced-crash-1",
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert completed.returncode == 97, (completed.stdout, completed.stderr)

    restarted = WorkbenchService(db_path, samples_dir=SAMPLES)
    startup = restarted.initialize()
    assert startup["status"] == "RECOVERED"
    facts = startup["facts"]
    assert facts["persisted_fact_only"] is True
    assert facts["last_complete_commit_seq"] == before["commit_seq"]
    assert "forced termination" in facts["note"]
    assert len(facts["unfinished_intents"]) == 1
    assert facts["unfinished_intents"][0]["kind"] == "REFRESH"
    assert "v_region_revenue" in facts["views_marked_partial"]

    freshness = restarted.get_freshness("v_region_revenue")
    assert freshness["state"] == "PARTIAL"
    assert freshness["gaps"][0]["code"] == "INCOMPLETE_REFRESH"
    assert freshness["materialized_version"] == before["bound_version"]
    query = restarted.query_view("v_region_revenue")
    assert query["state"] == "PARTIAL"
    assert query["bound_version"] == before["bound_version"]
    request = restarted.get_refresh_request("forced-crash-1")
    assert request["status"] == "FAILED"
    assert request["response"]["recovered"] is True
    report = restarted.recovery_report()
    assert report["latest"]["status"] == "RECOVERED"
    assert report["latest"]["report_id"] == startup["report_id"]
