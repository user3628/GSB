from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_DEMO_DIGEST = "450a1e55e9877d53be02928aa733f5b45809136dbbb70ebab1e210fd2a64de9a"


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT)
    return subprocess.run(
        [sys.executable, "-m", "panopipe.cli", *args],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=60,
    )


def test_cli_demo_status_and_verify_errors(tmp_path: Path) -> None:
    demo_dir = tmp_path / "demo"
    workspace = tmp_path / "workspace"
    completed = _run("demo", "--demo-dir", str(demo_dir), "--workspace", str(workspace))
    assert completed.returncode == 0, completed.stderr + completed.stdout
    output = json.loads(completed.stdout)
    assert output["ok"] is True
    assert output["summary_digest"] == EXPECTED_DEMO_DIGEST
    assert output["checkpoint_sequence"] == 16

    completed = _run("status", "--workspace", str(workspace), "--shard", "shard_1_indexed")
    assert completed.returncode == 0
    status = json.loads(completed.stdout)
    assert status["shards"]["shard_1_indexed"]["stages"]["validate"]["status"] == "ready"

    completed = _run("verify-errors", "--demo-dir", str(demo_dir), "--workspace", str(tmp_path / "errors"))
    assert completed.returncode == 0, completed.stderr + completed.stdout
    errors = json.loads(completed.stdout)
    assert errors["ok"] is True
    assert errors["results"]["unknown_color"]["code"] == "E_UNKNOWN_COLOR"
    assert errors["results"]["segment_overflow"]["code"] == "E_SEGMENT_ID_OVERFLOW"
    assert errors["results"]["unknown_schema"]["code"] == "E_UNSUPPORTED_SCHEMA"
