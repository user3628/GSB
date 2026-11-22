from __future__ import annotations

import json
import shutil
import time
from pathlib import Path

import pytest

from multiscale_fusion.engine import FusionEngine
from multiscale_fusion.samplegen import generate_demo_bundle
from multiscale_fusion.workspace import import_bundle

from .conftest import key_artifacts, popen_cli, run_cli


def _fresh_workspace(path: Path, bundle: Path) -> Path:
    import_bundle(bundle, path, force=True)
    return path


def _wait_for_sequence(workspace: Path, minimum: int, timeout: float = 10.0) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            state = FusionEngine(workspace).store("demo_fpn").scan(repair_head=False)
            if state.head_sequence >= minimum:
                return state.head_sequence
        except Exception:
            pass
        time.sleep(0.03)
    raise AssertionError("timed out waiting for checkpoint sequence")


def test_partial_run_then_resume_matches_full(workspace: Path, tmp_path: Path) -> None:
    full = _fresh_workspace(tmp_path / "full", workspace / "input")
    # The fixture input contains the copied bundle plus an import report; it is
    # also a valid bundle root for this local helper.
    report = (full / "input" / "import_report.json")
    if report.exists():
        report.unlink()
    full_engine = FusionEngine(full)
    full_result = full_engine.run("demo_fpn")
    full_hashes = key_artifacts(full)

    engine = FusionEngine(workspace)
    first = engine.run("demo_fpn", max_stages=1)
    assert first["status"] == "partial"
    assert first["completed_layers"] == ["p5"]
    resumed = engine.resume("demo_fpn", verify_full=True)
    assert resumed["status"] == "completed"
    assert resumed["reused_layers"] == ["p5"]
    assert resumed["computed_layers"] == ["p4", "p3"]
    assert resumed["verification"]["result"] == "incremental_matches_full_rebuild"
    assert key_artifacts(workspace) == full_hashes
    sequences = [record["sequence"] for record in engine.store("demo_fpn").scan().records]
    assert sequences == [1, 2, 3]


def test_graceful_stop_completes_current_layer_and_resume_is_byte_identical(
    workspace: Path, tmp_path: Path
) -> None:
    full = _fresh_workspace(tmp_path / "full", workspace / "input")
    report = full / "input" / "import_report.json"
    if report.exists():
        report.unlink()
    FusionEngine(full).run("demo_fpn")
    full_hashes = key_artifacts(full)

    process = popen_cli(
        "run",
        "--workspace",
        str(workspace),
        "--task",
        "demo_fpn",
        "--stage-delay",
        "0.35",
    )
    try:
        _wait_for_sequence(workspace, 1)
        engine = FusionEngine(workspace)
        stop_response = engine.stop("demo_fpn", reason="pytest_graceful")
        assert stop_response["stop_requested"] is True
        stdout, stderr = process.communicate(timeout=20)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()
    assert process.returncode == 0, stderr
    payload = json.loads(stdout)
    assert payload["status"] == "stopped"
    assert len(payload["completed_layers"]) >= 1
    resumed = engine.resume("demo_fpn", verify_full=True)
    assert resumed["status"] == "completed"
    assert resumed["verification"]["result"] == "incremental_matches_full_rebuild"
    assert key_artifacts(workspace) == full_hashes
    state = engine.store("demo_fpn").scan()
    assert [record["sequence"] for record in state.records] == [1, 2, 3]


def test_forced_termination_resume_uses_only_written_records(workspace: Path, tmp_path: Path) -> None:
    full = _fresh_workspace(tmp_path / "full", workspace / "input")
    report = full / "input" / "import_report.json"
    if report.exists():
        report.unlink()
    FusionEngine(full).run("demo_fpn")
    full_hashes = key_artifacts(full)

    crashed = run_cli(
        "run",
        "--workspace",
        str(workspace),
        "--task",
        "demo_fpn",
        "--crash-after",
        "p4",
    )
    assert crashed.returncode == 137
    engine = FusionEngine(workspace)
    state = engine.store("demo_fpn").scan()
    assert state.head_sequence == 2
    # Remove every derived output/report and inject a partial temp file.  The
    # resume path must reconstruct from committed checkpoints and artifacts.
    shutil.rmtree(workspace / "outputs", ignore_errors=True)
    reports = workspace / "state" / "tasks" / "demo_fpn" / "reports"
    shutil.rmtree(reports, ignore_errors=True)
    reports.mkdir(parents=True, exist_ok=True)
    (engine.store("demo_fpn").checkpoints_dir / ".tmp-half-record").write_text("{half", encoding="utf-8")
    resumed = engine.resume("demo_fpn", verify_full=True)
    assert resumed["status"] == "completed"
    assert resumed["reused_layers"] == ["p5", "p4"]
    assert resumed["computed_layers"] == ["p3"]
    assert resumed["verification"]["result"] == "incremental_matches_full_rebuild"
    assert key_artifacts(workspace) == full_hashes
    assert [record["sequence"] for record in engine.store("demo_fpn").scan().records] == [1, 2, 3]
