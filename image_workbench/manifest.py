from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

from .errors import E_VALIDATION, WorkbenchError
from .hashing import canonical_json
from .masks import MaskRegion
from .model import FragmentModel, default_metadata
from .numeric import validate_strength

MANIFEST_FORMAT = "image-workbench-manifest"


DEFAULT_FORMAT_CONFIG: dict[str, Any] = {
    "png": {"compress_level": 6, "mask_channel": "alpha"},
    "jpeg": {"quality": 95, "subsampling": 0, "mask_transport": "companion-json"},
    "json": {"descriptor_version": 1},
}


def validate_format_config(value: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise WorkbenchError(E_VALIDATION, "format_config must be an object")
    try:
        canonical_json(value)
    except (TypeError, ValueError) as exc:
        raise WorkbenchError(E_VALIDATION, "format_config must contain finite JSON data") from exc
    for section_name in ("png", "jpeg", "json"):
        section = value.get(section_name, {})
        if not isinstance(section, dict):
            raise WorkbenchError(E_VALIDATION, f"format_config.{section_name} must be an object")
    png = value.get("png", {})
    if "compress_level" in png:
        level = png["compress_level"]
        if isinstance(level, bool) or not isinstance(level, int) or not 0 <= level <= 9:
            raise WorkbenchError(E_VALIDATION, "PNG compress_level must be an integer within [0, 9]")
    jpeg = value.get("jpeg", {})
    if "quality" in jpeg:
        quality = jpeg["quality"]
        if isinstance(quality, bool) or not isinstance(quality, int) or not 1 <= quality <= 100:
            raise WorkbenchError(E_VALIDATION, "JPEG quality must be an integer within [1, 100]")
    if "subsampling" in jpeg:
        subsampling = jpeg["subsampling"]
        if isinstance(subsampling, bool) or not isinstance(subsampling, int) or subsampling not in {0, 1, 2}:
            raise WorkbenchError(E_VALIDATION, "JPEG subsampling must be 0, 1 or 2")
    return copy.deepcopy(value)


@dataclass
class ProjectManifest:
    project_name: str
    fragments: list[FragmentModel]
    metadata: dict[str, Any]
    format_config: dict[str, Any]
    source_path: Path | None = None


def load_rgb_image(path: Path) -> tuple[np.ndarray, dict[str, Any]]:
    try:
        with Image.open(path) as image:
            image.load()
            source_format = (image.format or path.suffix.lstrip(".")).lower()
            if source_format == "jpg":
                source_format = "jpeg"
            if image.mode in {"RGBA", "LA"} or (image.mode == "P" and "transparency" in image.info):
                rgba = image.convert("RGBA")
                array = np.asarray(rgba, dtype=np.uint8)
                rgb = array[:, :, :3].copy()
            else:
                rgb = np.asarray(image.convert("RGB"), dtype=np.uint8).copy()
            return rgb, {
                "source_format": source_format,
                "source_mode": image.mode,
                "source_size": [image.width, image.height],
            }
    except FileNotFoundError as exc:
        raise WorkbenchError(E_VALIDATION, f"Fragment image not found: {path}") from exc
    except Exception as exc:
        raise WorkbenchError(E_VALIDATION, f"Unable to read fragment image {path}: {exc}") from exc


def load_manifest(path: str | Path) -> ProjectManifest:
    manifest_path = Path(path).resolve()
    try:
        document = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise WorkbenchError(E_VALIDATION, f"Manifest not found: {manifest_path}") from exc
    except json.JSONDecodeError as exc:
        raise WorkbenchError(E_VALIDATION, f"Manifest is not valid JSON: {exc}") from exc
    if document.get("format", MANIFEST_FORMAT) != MANIFEST_FORMAT:
        raise WorkbenchError(E_VALIDATION, "Manifest format marker is unsupported")
    if int(document.get("schema_version", -1)) != 2:
        raise WorkbenchError(
            E_VALIDATION,
            "This manifest is not schema version 2; use legacy import for version 1",
            details={"actual": document.get("schema_version")},
        )
    raw_fragments = document.get("fragments")
    if not isinstance(raw_fragments, list) or not raw_fragments:
        raise WorkbenchError(E_VALIDATION, "Manifest fragments must be a non-empty list")
    project_metadata = copy.deepcopy(document.get("metadata", {}))
    project_metadata.setdefault("schema_version", 2)
    project_metadata.setdefault("codec_version", "2.0.0")
    project_metadata.setdefault("workbench_version", "2.0.0")
    project_metadata.setdefault("manifest_format", MANIFEST_FORMAT)
    project_metadata.setdefault("manifest_path", str(manifest_path))
    models: list[FragmentModel] = []
    for raw in raw_fragments:
        fragment_id = str(raw.get("id", ""))
        image_value = raw.get("image")
        if not image_value:
            raise WorkbenchError(E_VALIDATION, "Manifest fragment is missing image path", fragment_id=fragment_id or None)
        image_path = (manifest_path.parent / str(image_value)).resolve()
        pixels, image_info = load_rgb_image(image_path)
        mask_value = raw.get("mask", {})
        rects = mask_value.get("rects", mask_value if isinstance(mask_value, list) else [])
        mask = MaskRegion.from_dict(rects)
        metadata = default_metadata(
            image_info["source_format"],
            source_schema_version=2,
            source_file=str(image_path.relative_to(manifest_path.parent)) if image_path.is_relative_to(manifest_path.parent) else str(image_path),
            source_mode=image_info["source_mode"],
            manifest_name=document.get("project_name", "untitled"),
        )
        if raw.get("metadata"):
            metadata.update(copy.deepcopy(raw["metadata"]))
        style_reference = str(raw.get("style_reference", fragment_id))
        if style_reference == "self":
            style_reference = fragment_id
        style_strength = validate_strength(
            raw.get("style_strength", 0.0),
            fragment_id=fragment_id,
            field="style_strength",
        )
        raw_iterations = raw.get("repair_iterations", 1)
        if isinstance(raw_iterations, bool):
            raise WorkbenchError(
                E_VALIDATION,
                "repair_iterations must be an integer",
                fragment_id=fragment_id,
            )
        try:
            repair_iterations = int(raw_iterations)
        except (TypeError, ValueError) as exc:
            raise WorkbenchError(
                E_VALIDATION,
                "repair_iterations must be an integer",
                fragment_id=fragment_id,
            ) from exc
        models.append(
            FragmentModel(
                fragment_id=fragment_id,
                pixels=pixels,
                mask=mask,
                style_reference=style_reference,
                style_strength=style_strength,
                repair_iterations=repair_iterations,
                output_format=str(raw.get("output_format", "png")),
                metadata=metadata,
                loss_items=copy.deepcopy(raw.get("loss_items", [])),
            )
        )
    ids = [model.fragment_id for model in models]
    if len(ids) != len(set(ids)):
        raise WorkbenchError(E_VALIDATION, "Manifest contains duplicate fragment identifiers")
    known = set(ids)
    for model in models:
        if model.style_reference not in known:
            raise WorkbenchError(
                E_VALIDATION,
                "style_reference does not identify a fragment in this manifest",
                fragment_id=model.fragment_id,
                details={"style_reference": model.style_reference},
            )
    format_config = copy.deepcopy(DEFAULT_FORMAT_CONFIG)
    format_config.update(copy.deepcopy(document.get("format_config", {})))
    format_config = validate_format_config(format_config)
    try:
        canonical_json(project_metadata)
        canonical_json(format_config)
    except (TypeError, ValueError) as exc:
        raise WorkbenchError(E_VALIDATION, "Manifest metadata or format configuration is not JSON-safe") from exc
    return ProjectManifest(
        project_name=str(document.get("project_name", manifest_path.stem)),
        fragments=models,
        metadata=project_metadata,
        format_config=format_config,
        source_path=manifest_path,
    )

