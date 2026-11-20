from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from h2workbench.engine import Workbench

from .conftest import ROOT, load_sample


CRASH_CODE = r"""
import json
import sys
from h2workbench.engine import Workbench

db_path, sample_path = sys.argv[1], sys.argv[2]
sample = json.loads(open(sample_path, encoding='utf-8').read())
workbench = Workbench(db_path)
workbench.submit_sample(sample)
workbench.schedule_sample(sample['sample_id'])
print('CRASH_HOOK_DID_NOT_FIRE')
"""


def _run_crash(db_path: Path, sample_path: Path) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["H2WB_CRASH_AT_ROUND"] = "2"
    env["PYTHONPATH"] = str(ROOT)
    return subprocess.run(
        [sys.executable, "-c", CRASH_CODE, str(db_path), str(sample_path)],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
    )


def test_forced_termination_rolls_back_incomplete_round_and_keeps_committed_round(tmp_path: Path):
    db_path = tmp_path / "recovery.db"
    sample_path = ROOT / "samples" / "recovery_crash.json"
    crashed = _run_crash(db_path, sample_path)
    assert crashed.returncode == 137
    assert "CRASH_HOOK_DID_NOT_FIRE" not in crashed.stdout

    normal_db = tmp_path / "normal.db"
    normal = Workbench(normal_db)
    normal.submit_sample(load_sample("recovery_crash.json"))
    normal_run = normal.schedule_sample("demo-recovery-crash")

    recovered = Workbench(db_path)
    reports = recovered.get_recovery_report()
    assert len(reports["reports"]) == 1
    report = reports["reports"][0]
    assert report["action"] == "ROLLED_BACK_TO_LAST_COMMITTED_BOUNDARY"
    assert report["facts"]["committed_round_count"] == 1
    assert report["facts"]["last_committed_round"] == 1
    assert report["facts"]["connection_window"] == normal_run["rounds"][0][
        "connection_window_after"
    ]
    assert reports["pending_intents"] == []
    assert reports["scope"] == "persisted_facts_only"

    trace = recovered.get_sample_trace("demo-recovery-crash")
    assert trace["status"] == "ABORTED"
    assert trace["round_count"] == 1
    assert trace["trace"] == normal_run["trace"][: len(trace["trace"])]
    assert trace["connection"]["connection_window"] == normal_run["rounds"][0][
        "connection_window_after"
    ]
    assert trace["connection"]["pending_bytes"] > 0

    recovered.submit_sample(
        {
            "sample_id": "post-recovery-work",
            "connection_id": "demo-recovery",
            "frames": [
                {"stream_id": 99, "frame_type": "HEADERS"},
                {"stream_id": 99, "frame_type": "DATA", "payload_bytes": 100},
            ],
        }
    )
    recovered.schedule_connection("demo-recovery")
    assert recovered.get_connection("demo-recovery")["connection_window"] != trace[
        "connection"
    ]["connection_window"]
    assert recovered.get_sample_trace("demo-recovery-crash") == trace

    second_restart = Workbench(db_path)
    assert second_restart.get_recovery_report()["reports"] == reports["reports"]
