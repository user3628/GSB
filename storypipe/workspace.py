from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .canonical import atomic_write_json, digest_paths, read_json, read_jsonl
from .config import load_config, normalize_config
from .errors import PipelineError
from .models import Shot, parse_shot


@dataclass(frozen=True)
class InputBundle:
    workspace: Path
    input_dir: Path
    config: dict[str, Any]
    shots: list[Shot]
    script_path: Path

    @property
    def image_dir(self) -> Path:
        return self.input_dir / "images"

    @property
    def shots_by_id(self) -> dict[str, Shot]:
        return {shot.shot_id: shot for shot in self.shots}


def load_bundle(workspace: Path) -> InputBundle:
    input_dir = workspace / "input"
    config_path = input_dir / "config.json"
    script_path = input_dir / "shots.jsonl"
    if not config_path.exists():
        raise PipelineError(
            "E_MISSING_ASSET", f"config not found: {config_path}", stage="import"
        )
    if not script_path.exists():
        raise PipelineError(
            "E_MISSING_ASSET", f"script not found: {script_path}", stage="import"
        )
    config = load_config(config_path)
    try:
        rows = read_jsonl(script_path)
    except ValueError as exc:
        raise PipelineError("E_INVALID_SHOT", str(exc), stage="schedule") from exc
    if not rows:
        raise PipelineError("E_INVALID_SHOT", "shot script is empty", stage="schedule")
    shots: list[Shot] = []
    seen: set[str] = set()
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise PipelineError(
                "E_INVALID_SHOT",
                "each JSONL record must be an object",
                stage="schedule",
                details={"line": index + 1},
            )
        shot = parse_shot(row, index)
        if shot.shot_id in seen:
            raise PipelineError(
                "E_INVALID_SHOT",
                f"duplicate shot_id {shot.shot_id}",
                stage="schedule",
                shot_id=shot.shot_id,
            )
        seen.add(shot.shot_id)
        shots.append(shot)
    return InputBundle(
        workspace=workspace,
        input_dir=input_dir,
        config=config,
        shots=shots,
        script_path=script_path,
    )


def import_sample(sample_dir: Path, workspace: Path, *, replace: bool = True) -> dict[str, Any]:
    sample_dir = sample_dir.resolve()
    workspace = workspace.resolve()
    if not (sample_dir / "shots.jsonl").exists():
        raise PipelineError(
            "E_MISSING_ASSET", f"sample script not found: {sample_dir / 'shots.jsonl'}", stage="import"
        )
    if not (sample_dir / "config.json").exists():
        raise PipelineError(
            "E_MISSING_ASSET", f"sample config not found: {sample_dir / 'config.json'}", stage="import"
        )
    image_dir = sample_dir / "images"
    if not image_dir.is_dir():
        raise PipelineError(
            "E_MISSING_ASSET", f"sample images not found: {image_dir}", stage="import"
        )
    input_dir = workspace / "input"
    staging = workspace / ".input-import-tmp"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    for filename in ("shots.jsonl", "config.json", "sample_manifest.json"):
        source = sample_dir / filename
        if source.exists():
            shutil.copy2(source, staging / filename)
    shutil.copytree(image_dir, staging / "images")
    if input_dir.exists() and not replace:
        shutil.rmtree(staging)
        raise PipelineError("E_IMPORT_CONFLICT", "input directory already exists", stage="import")
    old_input: Path | None = None
    if input_dir.exists():
        old_input = workspace / ".input-old"
        if old_input.exists():
            shutil.rmtree(old_input)
        input_dir.replace(old_input)
    staging.replace(input_dir)
    if old_input is not None:
        shutil.rmtree(old_input)
    manifest_path = input_dir / "sample_manifest.json"
    manifest = read_json(manifest_path) if manifest_path.exists() else {}
    return {
        "workspace": str(workspace),
        "input_dir": str(input_dir),
        "sample_scale": manifest.get("scale"),
        "sample_digest": manifest.get("sample_digest"),
        "image_digest": digest_paths((input_dir / "images").glob("*.png"), input_dir),
    }


def create_workspace_from_config(workspace: Path, config: dict[str, Any]) -> None:
    workspace.mkdir(parents=True, exist_ok=True)
    atomic_write_json(workspace / "input" / "config.json", normalize_config(config))
