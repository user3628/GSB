from __future__ import annotations

import shutil
from pathlib import Path

from sonic_sep.demo import run_demo
from sonic_sep.workspace import Workspace


def test_demo_is_repeatable_and_reports_all_acceptance_checks(dataset_root: Path, tmp_path: Path) -> None:
    project = tmp_path / "project"
    shutil.copytree(dataset_root / "samples", project / "samples")
    shutil.copytree(dataset_root / "configs", project / "configs")
    workspace = Workspace(project, state_root=tmp_path / "state")
    first = run_demo(workspace, output_path=tmp_path / "first.json", reset=True)
    assert first["ok"] is True
    assert first["graceful_stop"]["stopped_status"] == "stopped"
    assert first["graceful_stop"]["resume_status"] == "complete"
    assert first["graceful_stop"]["matches_fresh_full"] is True
    assert first["incremental"]["full_equivalent"] is True
    assert first["invalidation"]["earliest_stage"] == "mask_refine"
    assert first["invalidation"]["upstream_reused"] == {
        "stft": "reused",
        "features": "reused",
        "mask_init": "reused",
    }
    assert first["checkpoints"]["continuous"] is True

    second = run_demo(workspace, output_path=tmp_path / "second.json", reset=True)
    assert second["demo_digest"] == first["demo_digest"]
    assert second["baseline"] == first["baseline"]

