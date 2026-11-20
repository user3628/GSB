from __future__ import annotations

import copy
import re
import shutil
from pathlib import Path
from typing import Any

from .util import (
    WorkbenchError,
    atomic_write_json,
    ensure_dir,
    read_json,
    relpath,
    resolve_workspace_path,
)


def import_dataset(
    root: Path,
    manifest_path: str | Path,
    *,
    name: str,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Copy a local manifest and its images into a namespaced workspace dataset."""

    root = root.resolve()
    source = resolve_workspace_path(root, manifest_path, must_exist=True)
    if not source.is_file():
        raise WorkbenchError("E_IMPORT_INVALID", "manifest source must be a file", {"path": str(source)})
    slug = _slug(name)
    destination = root / "samples" / "imports" / slug
    if destination.exists():
        if not overwrite:
            raise WorkbenchError(
                "E_IMPORT_EXISTS",
                "import destination already exists",
                {"path": relpath(root, destination)},
            )
        shutil.rmtree(destination)
    images_dir = ensure_dir(destination / "images")
    try:
        raw = read_json(source)
    except Exception as exc:
        raise WorkbenchError("E_IMPORT_INVALID", "import manifest is not valid JSON", {"error": str(exc)}) from exc
    if not isinstance(raw, dict) or not isinstance(raw.get("samples"), list):
        raise WorkbenchError("E_IMPORT_INVALID", "import manifest must contain a samples list")
    rewritten: list[dict[str, Any]] = []
    copied: list[dict[str, str]] = []
    for index, sample in enumerate(raw["samples"]):
        if not isinstance(sample, dict) or not isinstance(sample.get("path"), str):
            raise WorkbenchError(
                "E_IMPORT_INVALID",
                "each imported sample needs a string path",
                {"manifest_record_index": index},
            )
        source_image = (source.parent / sample["path"]).resolve()
        try:
            source_image.relative_to(root)
        except ValueError as exc:
            raise WorkbenchError(
                "E_OUTSIDE_WORKSPACE",
                "imported image must already be stored inside the workspace",
                {"path": str(source_image)},
            ) from exc
        if not source_image.is_file():
            raise WorkbenchError(
                "E_NOT_FOUND",
                "imported image file does not exist",
                {"path": relpath(root, source_image)},
            )
        image_id = sample.get("image_id") if isinstance(sample.get("image_id"), str) else f"sample{index:03d}"
        suffix = source_image.suffix.lower() or ".png"
        filename = f"{_slug(image_id)}-{index:03d}{suffix}"
        target = images_dir / filename
        shutil.copyfile(source_image, target)
        changed = dict(sample)
        changed["path"] = f"images/{filename}"
        rewritten.append(changed)
        copied.append(
            {
                "from": relpath(root, source_image),
                "to": relpath(root, target),
            }
        )
    imported = dict(raw)
    imported["samples"] = rewritten
    imported["imported_from"] = relpath(root, source)
    imported["dataset_id"] = raw.get("dataset_id", name)
    manifest_target = destination / "manifest.json"
    atomic_write_json(manifest_target, imported, pretty=True)

    config_path = root / "configs" / "default.json"
    if config_path.exists():
        config_data = read_json(config_path)
    else:
        from .config import DEFAULT_CONFIG

        config_data = copy.deepcopy(DEFAULT_CONFIG)
    config_data.setdefault("dataset", {})["manifest"] = relpath(root, manifest_target)
    atomic_write_json(config_path, config_data, pretty=True)
    return {
        "name": slug,
        "manifest": relpath(root, manifest_target),
        "config": relpath(root, config_path),
        "sample_count": len(rewritten),
        "copied": copied,
    }


def _slug(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-._")
    if not cleaned:
        raise WorkbenchError("E_IMPORT_INVALID", "import name/image_id becomes empty after sanitization")
    return cleaned[:96]


__all__ = ["import_dataset"]
