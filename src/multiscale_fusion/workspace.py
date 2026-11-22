from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .errors import FusionError
from .manifest import Manifest
from .util import resolve_under, sha256_bytes, sha256_file, write_bytes, write_json


def import_bundle(
    bundle_dir: str | Path,
    workspace: str | Path,
    *,
    shard_overrides: Mapping[str, str] | None = None,
    force: bool = False,
) -> dict[str, Any]:
    bundle_dir = Path(bundle_dir).resolve()
    workspace = Path(workspace).resolve()
    manifest_source = bundle_dir / "manifest.json"
    if not manifest_source.is_file():
        raise FusionError("E_INVALID_STAGE", f"manifest not found in bundle: {manifest_source}", details={"path": str(manifest_source)})
    manifest = Manifest.load(manifest_source)
    input_dir = workspace / "input"
    if input_dir.exists() and any(input_dir.iterdir()) and not force:
        raise FusionError(
            "E_INVALID_PARAM",
            f"workspace input directory already contains files: {input_dir}",
            details={"path": str(input_dir), "hint": "pass force=True to replace the imported bundle"},
        )
    overrides = dict(shard_overrides or {})
    relative_paths: set[str] = set()
    for task in manifest.tasks:
        for layer in task.layers:
            relative_paths.add(layer.input)
            if layer.reference:
                relative_paths.add(layer.reference)
        if task.final_reference:
            relative_paths.add(task.final_reference)
    copied: dict[str, Any] = {}
    for relative in sorted(relative_paths):
        if relative in overrides:
            source = Path(overrides[relative]).resolve()
        else:
            source = resolve_under(bundle_dir, relative, field_name="manifest_path")
        if not source.is_file():
            raise FusionError.invalid_stage(
                f"import source is missing for {relative!r}",
                details={"relative_path": relative, "source": str(source)},
            )
        target = input_dir / relative
        data = source.read_bytes()
        write_bytes(target, data)
        copied[relative] = {
            "source": str(source),
            "target": str(target),
            "sha256": sha256_bytes(data),
            "size": len(data),
        }
    manifest_data = manifest_source.read_bytes()
    write_bytes(input_dir / "manifest.json", manifest_data)
    (workspace / "state").mkdir(parents=True, exist_ok=True)
    report = {
        "ok": True,
        "workspace": str(workspace),
        "bundle_dir": str(bundle_dir),
        "manifest_sha256": sha256_bytes(manifest_data),
        "tasks": [task.id for task in manifest.tasks],
        "copied": copied,
    }
    write_json(input_dir / "import_report.json", report)
    return report


def load_workspace_manifest(workspace: str | Path) -> Manifest:
    path = Path(workspace).resolve() / "input" / "manifest.json"
    if not path.is_file():
        raise FusionError("E_INVALID_STAGE", f"workspace manifest not found: {path}", details={"path": str(path)})
    return Manifest.load(path)
