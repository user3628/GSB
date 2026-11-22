from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

from mmcoop.config import load_config
from mmcoop.demo import run_demo
from mmcoop.generator import generate_demo
from mmcoop.service import WorkspaceService

from .conftest import PROJECT_ROOT


def _file_hashes(root: Path) -> dict[str, str]:
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_demo_generator_is_byte_deterministic(tmp_path: Path, config_path: Path):
    config = load_config(config_path)
    first = tmp_path / "first"
    second = tmp_path / "second"
    generate_demo(first, 20261123, config)
    generate_demo(second, 20261123, config)
    assert _file_hashes(first) == _file_hashes(second)
    manifest = json.loads((first / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["text_count"] == 5
    assert manifest["image_count"] == 6
    assert manifest["missing_link_text_ids"] == ["t04"]
    assert manifest["duplicate_content_groups"][0]["text_ids"] == ["t02", "t05"]


def test_cli_init_build_status_and_injection(tmp_path: Path, config_path: Path):
    data_dir = tmp_path / "cli-data"
    workspace = tmp_path / "cli-workspace"

    def run(*arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                "-m",
                "mmcoop",
                "--config",
                str(config_path),
                "--data",
                str(data_dir),
                "--workspace",
                str(workspace),
                *arguments,
            ],
            cwd=PROJECT_ROOT,
            text=True,
            capture_output=True,
            timeout=60,
        )

    initialized = run("init-demo", "--fresh-workspace")
    assert initialized.returncode == 0, initialized.stderr
    assert json.loads(initialized.stdout)["data"]["text_count"] == 5
    built = run("build", "--through-stage", "text_encoding")
    assert built.returncode == 0, built.stderr
    assert json.loads(built.stdout)["data"]["status"] == "partial_completed"
    completed = run("build")
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout)["data"]["status"] == "completed"
    status = run("status")
    assert status.returncode == 0, status.stderr
    assert json.loads(status.stdout)["data"]["status"] == "ready"
    injected = run("inject", "--kind", "missing_ref", "--sample-id", "i01")
    assert injected.returncode == 2
    error = json.loads(injected.stdout)["error"]
    assert error["code"] == "E_MISSING_REF"
    assert error["stage"] == "projection_alignment"


def test_demo_report_is_deterministic(tmp_path: Path, config_path: Path):
    first_service = WorkspaceService(config_path, tmp_path / "data-a", tmp_path / "workspace-a")
    first = run_demo(first_service, seed=20261123, report_path=tmp_path / "report-a.json")
    second_service = WorkspaceService(config_path, tmp_path / "data-b", tmp_path / "workspace-b")
    second = run_demo(second_service, seed=20261123, report_path=tmp_path / "report-b.json")
    assert first == second
    assert first["baseline"]["full_scratch_match"] is True
    assert first["incremental"]["image_feature"]["reused_stages"] == ["text_encoding"]
    assert first["stop_resume"]["resume_matches_full_run"] is True
    assert [item["code"] for item in first["injections"]] == [
        "E_PROJECTION_INVALID",
        "E_INVALID_OPERATOR",
        "E_TEXT_TOO_LONG",
        "E_NUMERIC",
        "E_MISSING_REF",
    ]
