#!/usr/bin/env python3
"""Run the complete local acceptance demonstration and print a JSON report."""

from __future__ import annotations

import json
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from conv_workbench.engine import Workbench
from conv_workbench.errors import WorkbenchError
from conv_workbench.samples import generate_demo_workspace


PYTHON = sys.executable


def cli(workspace: Path, run_dir: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    command = [
        PYTHON,
        "-m",
        "conv_workbench",
        "--workspace",
        str(workspace),
        "--run-dir",
        str(run_dir),
        *args,
    ]
    process = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=60)
    if check and process.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(command)}\n{process.stderr}")
    return process


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def port_is_available(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def compare_runs(first: Path, second: Path, shard_ids: list[str]) -> dict[str, bool]:
    comparison: dict[str, bool] = {}
    for shard_id in shard_ids:
        comparison[f"output:{shard_id}"] = (first / "outputs" / f"{shard_id}.npy").read_bytes() == (
            second / "outputs" / f"{shard_id}.npy"
        ).read_bytes()
        comparison[f"checkpoints:{shard_id}"] = (
            first / "checkpoints" / shard_id / "checkpoints.jsonl"
        ).read_bytes() == (second / "checkpoints" / shard_id / "checkpoints.jsonl").read_bytes()
    comparison["trace"] = (first / "outputs" / "trace.json").read_bytes() == (
        second / "outputs" / "trace.json"
    ).read_bytes()
    return comparison


def graceful_external_stop(workspace: Path, run_dir: Path) -> dict[str, Any]:
    if run_dir.exists():
        shutil.rmtree(run_dir)
    command = [
        PYTHON,
        "-m",
        "conv_workbench",
        "--workspace",
        str(workspace),
        "--run-dir",
        str(run_dir),
        "run",
        "--shard",
        "small_odd_pad",
        "--delay-between-stages",
        "0.4",
    ]
    process = subprocess.Popen(command, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    journal = run_dir / "checkpoints" / "small_odd_pad" / "checkpoints.jsonl"
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and not journal.is_file():
        if process.poll() is not None:
            break
        time.sleep(0.02)
    if not journal.is_file():
        stdout, stderr = process.communicate(timeout=5)
        raise RuntimeError(f"worker did not publish a checkpoint\nstdout={stdout}\nstderr={stderr}")
    stop = cli(workspace, run_dir, "stop", "--reason", "demo: graceful boundary stop")
    stdout, stderr = process.communicate(timeout=20)
    if process.returncode != 0:
        raise RuntimeError(f"stopped worker failed\nstdout={stdout}\nstderr={stderr}")
    stopped = json.loads(stdout)
    assert stopped["status"] == "stopped", stopped
    stop_report = read_json(run_dir / "reports" / "stop.json")
    assert stop_report["reason"] == "stop_request", stop_report
    assert not (run_dir / "STOP_REQUEST.json").exists()
    resumed = cli(workspace, run_dir, "resume")
    return {
        "stop_request": json.loads(stop.stdout),
        "stop_report": stop_report,
        "resume": json.loads(resumed.stdout),
    }


def expect_error(workspace: Path, run_dir: Path, expected_code: str) -> dict[str, Any]:
    try:
        Workbench(workspace, run_dir).run()
    except WorkbenchError as exc:
        assert exc.code == expected_code, exc.to_dict()
        return exc.to_dict()["error"]
    raise AssertionError(f"expected {expected_code}, but the run succeeded")


def run() -> dict[str, Any]:
    demo_workspace = ROOT / "demo_workspace"
    runs_root = ROOT / "runs" / "validation"
    if runs_root.exists():
        shutil.rmtree(runs_root)
    runs_root.mkdir(parents=True)
    generation = generate_demo_workspace(demo_workspace, force=True)
    shard_ids = [shard["id"] for shard in generation["shards"]]

    self_check = cli(demo_workspace, runs_root / "self_check", "self-check")
    clean_dir = runs_root / "clean"
    clean_process = cli(demo_workspace, clean_dir, "run")
    clean = json.loads(clean_process.stdout)

    interrupted_dir = runs_root / "graceful_stop_resume"
    interrupted = graceful_external_stop(demo_workspace, interrupted_dir)
    comparison = compare_runs(clean_dir, interrupted_dir, shard_ids)
    assert all(comparison.values()), comparison

    invalid_root = runs_root / "invalid"
    invalid_root.mkdir(parents=True)
    invalid_conv_workspace = invalid_root / "invalid_conv_workspace"
    shutil.copytree(demo_workspace, invalid_conv_workspace)
    conv_manifest_path = invalid_conv_workspace / "manifest.json"
    conv_manifest = read_json(conv_manifest_path)
    conv_manifest["shards"][0]["operators"][0] = {
        "name": "conv2d",
        "params": {"stride": [1, 1], "padding": "same", "dilation": [1, 1]},
    }
    write_json(conv_manifest_path, conv_manifest)
    invalid_conv = expect_error(invalid_conv_workspace, invalid_root / "invalid_conv_run", "E_INVALID_STAGE")

    invalid_scale_workspace = invalid_root / "invalid_scale_workspace"
    shutil.copytree(demo_workspace, invalid_scale_workspace)
    scale_manifest_path = invalid_scale_workspace / "manifest.json"
    scale_manifest = read_json(scale_manifest_path)
    scale_manifest["shards"][0]["operators"][1]["params"]["scale"] = [1.0, 129.0]
    write_json(scale_manifest_path, scale_manifest)
    invalid_scale = expect_error(invalid_scale_workspace, invalid_root / "invalid_scale_run", "E_INVALID_STAGE")

    invalidation_workspace = runs_root / "invalidation_workspace"
    shutil.copytree(demo_workspace, invalidation_workspace)
    invalidation_run = runs_root / "invalidation_run"
    baseline = Workbench(invalidation_workspace, invalidation_run).run()
    changed_manifest_path = invalidation_workspace / "manifest.json"
    changed_manifest = read_json(changed_manifest_path)
    medium = next(shard for shard in changed_manifest["shards"] if shard["id"] == "medium_odd_dilated")
    medium["operators"][1]["params"]["mode"] = "bilinear"
    medium["operators"][1]["params"]["align_corners"] = False
    write_json(changed_manifest_path, changed_manifest)
    expectations_path = invalidation_workspace / "expectations.json"
    expectations = read_json(expectations_path)
    for stage in expectations["shards"]["medium_odd_dilated"]["stages"][1:]:
        stage["range"] = [-1000.0, 1000.0]
        stage["tolerance"] = 1e-6
    write_json(expectations_path, expectations)
    changed = Workbench(invalidation_workspace, invalidation_run).run(shard_ids=["medium_odd_dilated"])
    assert changed["reused_stages"]["medium_odd_dilated"] == [0]
    invalidation = changed["invalidations"][0]

    report = {
        "ok": True,
        "python": sys.version.split()[0],
        "numpy": np.__version__,
        "port_8766_available": port_is_available(8766),
        "seed": generation["seed"],
        "demo_shards": generation["shards"],
        "self_check": json.loads(self_check.stdout),
        "clean_run": clean,
        "graceful_stop": interrupted,
        "byte_identical_clean_vs_resume": comparison,
        "invalid_conv": invalid_conv,
        "invalid_scale": invalid_scale,
        "invalidation": {
            "earliest_invalid_stage_index": invalidation["earliest_invalid_stage_index"],
            "reason": invalidation["reason"],
            "affected_stages": invalidation["affected_stages"],
            "reused_stages": changed["reused_stages"]["medium_odd_dilated"],
            "old_final_digest": baseline["final_digests"]["medium_odd_dilated"],
            "new_final_digest": changed["final_digests"]["medium_odd_dilated"],
        },
    }
    assert all(report["byte_identical_clean_vs_resume"].values())
    return report


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2, sort_keys=True))

