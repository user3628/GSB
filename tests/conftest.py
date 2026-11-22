from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from multiscale_fusion.engine import FusionEngine
from multiscale_fusion.samplegen import generate_demo_bundle
from multiscale_fusion.util import pretty_json_bytes, write_bytes
from multiscale_fusion.workspace import import_bundle

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def bundle_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    path = tmp_path_factory.mktemp("demo_bundle") / "bundle"
    generate_demo_bundle(path, seed=20261123)
    return path


@pytest.fixture()
def workspace(tmp_path: Path, bundle_dir: Path) -> Path:
    path = tmp_path / "workspace"
    import_bundle(bundle_dir, path, force=True)
    return path


@pytest.fixture()
def engine(workspace: Path) -> FusionEngine:
    return FusionEngine(workspace)


def manifest_dict(workspace: Path) -> dict[str, Any]:
    return json.loads((workspace / "input" / "manifest.json").read_text(encoding="utf-8"))


def write_manifest_dict(workspace: Path, value: dict[str, Any]) -> None:
    write_bytes(workspace / "input" / "manifest.json", pretty_json_bytes(value))


def layer_dict(value: dict[str, Any], name: str) -> dict[str, Any]:
    return next(layer for layer in value["tasks"][0]["layers"] if layer["name"] == name)


def file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def key_artifacts(workspace: Path, task_id: str = "demo_fpn") -> dict[str, str]:
    output = workspace / "outputs" / task_id
    names = [
        "p3.npy",
        "p4.npy",
        "p5.npy",
        "final.npy",
        "reports/quantization.json",
        "reports/sensitivity.json",
    ]
    return {name: file_sha(output / name) for name in names}


def clone_workspace_input(source_workspace: Path, target_workspace: Path, bundle_tmp: Path) -> None:
    bundle_like = bundle_tmp / "bundle"
    if bundle_like.exists():
        shutil.rmtree(bundle_like)
    shutil.copytree(source_workspace / "input", bundle_like)
    import_report = bundle_like / "import_report.json"
    if import_report.exists():
        import_report.unlink()
    import_bundle(bundle_like, target_workspace, force=True)


def run_cli(*args: str, env_extra: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT / "src") + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [sys.executable, "-m", "multiscale_fusion", *args],
        cwd=str(ROOT),
        env=env,
        text=True,
        capture_output=True,
        timeout=60,
    )


def popen_cli(*args: str) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT / "src") + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    return subprocess.Popen(
        [sys.executable, "-m", "multiscale_fusion", *args],
        cwd=str(ROOT),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
