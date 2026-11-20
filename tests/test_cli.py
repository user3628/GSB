"""Requirement 8: command line interface smoke test."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _run(*args: str) -> tuple[int, dict]:
    proc = subprocess.run(
        [sys.executable, "-m", "depworkbench.cli", *args],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    return proc.returncode, json.loads(proc.stdout)


def test_cli_lifecycle(tmp_path, ensure_samples):
    workspace = tmp_path / "cli_ws"
    common = ["--workspace", str(workspace)]
    code, body = _run(*common, "import", "--shards-dir",
                      str(ensure_samples), "--manifest",
                      str(ensure_samples / "manifest.json"),
                      "--config", str(ensure_samples / "config.yaml"))
    assert code == 0 and body["shard_count"] == 3

    code, body = _run(*common, "run")
    assert code == 0 and body["status"] == "completed"

    code, body = _run(*common, "status")
    assert code == 0 and body["stage"] == "completed"

    code, body = _run(*common, "stats")
    assert code == 0 and body["stats"]["token_count"] == 198

    export_path = tmp_path / "out.json"
    code, body = _run(*common, "export", "--output", str(export_path))
    assert code == 0 and export_path.exists()


def test_cli_reports_business_error(tmp_path, ensure_samples):
    workspace = tmp_path / "cli_bad"
    code, body = _run("--workspace", str(workspace), "import",
                      "--shards-dir", str(ensure_samples),
                      "--manifest", str(ensure_samples / "manifest.json"),
                      "--config",
                      str(ensure_samples / "config_bad_weights.yaml"))
    assert code == 2 and body["error"]["code"] == "E_INVALID_CONFIG"
