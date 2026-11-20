"""Requirement 1 & 3: byte-identical outputs across independent runs."""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_two_independent_runs_are_byte_identical(ensure_samples, tmp_path):
    root = Path(__file__).resolve().parent.parent
    exports = []
    for name in ("run_a", "run_b"):
        workspace = tmp_path / name
        output = tmp_path / f"{name}.json"
        subprocess.run([
            sys.executable, "-m", "depworkbench.cli",
            "--workspace", str(workspace), "import",
            "--shards-dir", str(ensure_samples),
            "--manifest", str(ensure_samples / "manifest.json"),
            "--config", str(ensure_samples / "config.yaml"),
        ], cwd=root, check=True, capture_output=True, text=True)
        subprocess.run([
            sys.executable, "-m", "depworkbench.cli",
            "--workspace", str(workspace), "run",
        ], cwd=root, check=True, capture_output=True, text=True)
        subprocess.run([
            sys.executable, "-m", "depworkbench.cli",
            "--workspace", str(workspace), "export",
            "--output", str(output),
        ], cwd=root, check=True, capture_output=True, text=True)
        exports.append(output)

    assert _file_sha256(exports[0]) == _file_sha256(exports[1])
    payload_a = json.loads(exports[0].read_text(encoding="utf-8"))
    payload_b = json.loads(exports[1].read_text(encoding="utf-8"))
    assert payload_a == payload_b

    cp_a = sorted(p.name for p in (tmp_path / "run_a" / "checkpoints").glob(
        "shard_*.checkpoint.json"))
    cp_b = sorted(p.name for p in (tmp_path / "run_b" / "checkpoints").glob(
        "shard_*.checkpoint.json"))
    assert cp_a == cp_b
    for name in cp_a:
        assert _file_sha256(tmp_path / "run_a" / "checkpoints" / name) == \
            _file_sha256(tmp_path / "run_b" / "checkpoints" / name)
