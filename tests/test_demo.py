from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def _run_demo(tmp_path: Path) -> dict:
    samples = tmp_path / "samples"
    runtime = tmp_path / "runtime"
    summary = tmp_path / "summary.json"
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(ROOT / "src")
    process = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "run_demo.py"),
            "--samples",
            str(samples),
            "--runtime",
            str(runtime),
            "--summary",
            str(summary),
        ],
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    assert "summary_digest=" in process.stdout
    return json.loads(summary.read_text(encoding="utf-8"))


def test_demo_is_reproducible_and_covers_acceptance_cases(tmp_path: Path) -> None:
    first = _run_demo(tmp_path / "first")
    second = _run_demo(tmp_path / "second")
    assert first["summary_digest"] == second["summary_digest"]
    assert first["manifest_digest"] == second["manifest_digest"]
    assert first["sample_count"] == 11
    assert all(run["audit"]["passed"] for run in first["sample_runs"])
    assert any(run["split_count"] > 0 for run in first["sample_runs"])
    assert any(run["audit"]["nonfinite_source_count"] > 0 for run in first["sample_runs"])
    plan = first["plan_change_demo"]
    assert plan["steps"][0]["cache_hit"] is False
    assert plan["steps"][1]["cache_hit"] is True
    assert plan["steps"][3]["cache_hit"] is False
    assert plan["a_output_stable_after_rebuild"] is True
    assert plan["a_and_b_output_differ"] is True

