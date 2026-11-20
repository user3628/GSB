from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from .conftest import ROOT


@pytest.mark.slow
def test_external_crash_recovery_verifier(tmp_path: Path):
    output = tmp_path / "verification_report.json"
    completed = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "verify.py"), "--output", str(output)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=180,
    )
    assert completed.returncode == 0, f"stdout={completed.stdout}\nstderr={completed.stderr}"
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["all_recovery_paths_item_by_item_equal"] is True
    assert report["uninterrupted"]["window_count"] > 0
    assert set(report["scenarios"]) == {
        "crash-after-intent",
        "crash-during-snapshot-temp",
        "crash-after-snapshot-replace",
    }
    for name, scenario in report["scenarios"].items():
        assert scenario["crash"]["exit_code"] == 137, name
        comparison = report["comparisons"][name]
        assert comparison["windows_equal_item_by_item"] is True
        assert comparison["late_queue_equal_item_by_item"] is True
        assert comparison["differences"] == []
    temp_scenario = report["scenarios"]["crash-during-snapshot-temp"]
    assert temp_scenario["temporary_snapshot_files_after_crash"]
    replace_scenario = report["scenarios"]["crash-after-snapshot-replace"]
    assert replace_scenario["retried_batch_replayed"] is True
