from __future__ import annotations

from pathlib import Path

from shardseek.config import load_config
from shardseek.pipeline import Pipeline
from shardseek.workspace import Workspace


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLES = PROJECT_ROOT / "samples"
MANIFEST = SAMPLES / "manifest.jsonl"


def workspace_for(root: str | Path, *, clean: bool = True) -> Workspace:
    workspace = Workspace(root, project_root=PROJECT_ROOT)
    if clean:
        workspace.clean()
    else:
        workspace.init()
    return workspace


def build_samples(root: str | Path, config_name: str = "baseline_8bit") -> tuple[Workspace, dict]:
    workspace = workspace_for(root)
    workspace.import_manifest(MANIFEST)
    workspace.set_config(config_name)
    result = Pipeline(workspace).build(mode="full")
    return workspace, result


def baseline_config() -> dict:
    return load_config("baseline_8bit", PROJECT_ROOT)


def compact_config() -> dict:
    return load_config("compact_4bit", PROJECT_ROOT)

