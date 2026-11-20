from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .errors import E_LEGACY, E_NUMERIC, E_VALIDATION, WorkbenchError
from .hashing import digest_json
from .manifest import DEFAULT_FORMAT_CONFIG, load_rgb_image
from .masks import MaskRect, MaskRegion
from .model import FragmentModel, default_metadata


@dataclass(frozen=True)
class LegacyConfig:
    """Rules used when upgrading schema-v1 manifests.

    ``strength_percent_base`` and coordinate precision are deliberately
    configurable; IDs are stable hashes salted by ``id_salt`` and prefixed by
    ``id_prefix``. Defaults preserve the legacy pixel mask exactly.
    """

    id_prefix: str = "legacy"
    id_salt: str = "schema-v1"
    strength_percent_base: float = 100.0
    coordinate_decimals: int = 12
    coordinate_origin: str = "top-left"
    default_repair_iterations: int = 4
    default_output_format: str = "png"

    def to_dict(self) -> dict[str, Any]:
        return {
            "id_prefix": self.id_prefix,
            "id_salt": self.id_salt,
            "strength_percent_base": self.strength_percent_base,
            "coordinate_decimals": self.coordinate_decimals,
            "coordinate_origin": self.coordinate_origin,
            "default_repair_iterations": self.default_repair_iterations,
            "default_output_format": self.default_output_format,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> LegacyConfig:
        if not value:
            return cls()
        allowed = {
            "id_prefix",
            "id_salt",
            "strength_percent_base",
            "coordinate_decimals",
            "coordinate_origin",
            "default_repair_iterations",
            "default_output_format",
        }
        unknown = set(value) - allowed
        if unknown:
            raise WorkbenchError(E_VALIDATION, "Unknown legacy compatibility configuration fields", details={"fields": sorted(unknown)})
        config = cls(**value)
        if not isinstance(config.id_prefix, str) or not config.id_prefix:
            raise WorkbenchError(E_VALIDATION, "legacy id_prefix must be non-empty")
        if not np.isfinite(float(config.strength_percent_base)) or float(config.strength_percent_base) <= 0:
            raise WorkbenchError(E_NUMERIC, "legacy strength_percent_base must be finite and positive", pixel=[0, 0])
        if not isinstance(config.coordinate_decimals, int) or not 0 <= config.coordinate_decimals <= 15:
            raise WorkbenchError(E_VALIDATION, "coordinate_decimals must be within [0, 15]")
        if config.coordinate_origin not in {"top-left", "bottom-left"}:
            raise WorkbenchError(E_VALIDATION, "coordinate_origin must be top-left or bottom-left")
        return config


def _legacy_rect_to_normalized(rect: dict[str, Any], width: int, height: int, config: LegacyConfig) -> MaskRect:
    try:
        x = float(rect["x"])
        y = float(rect["y"])
        w = float(rect["width"])
        h = float(rect["height"])
    except (KeyError, TypeError, ValueError) as exc:
        raise WorkbenchError(E_LEGACY, "Legacy pixel mask must contain numeric x, y, width and height") from exc
    if not all(np.isfinite(value) for value in (x, y, w, h)):
        raise WorkbenchError(E_NUMERIC, "Legacy mask coordinate is NaN or Inf", pixel=[0, 0])
    if not all(value.is_integer() for value in (x, y, w, h)):
        raise WorkbenchError(E_LEGACY, "Legacy pixel mask coordinates and dimensions must be integers")
    if x < 0 or y < 0 or w <= 0 or h <= 0 or x + w > width or y + h > height:
        raise WorkbenchError(
            E_LEGACY,
            "Legacy mask exceeds image bounds",
            details={"rect": [x, y, w, h], "size": [width, height]},
        )
    y_start = height - (y + h) if config.coordinate_origin == "bottom-left" else y
    y_end = height - y if config.coordinate_origin == "bottom-left" else y + h
    digits = config.coordinate_decimals
    return MaskRect(
        round(x / width, digits),
        round(y_start / height, digits),
        round((x + w) / width, digits),
        round(y_end / height, digits),
    )


def _stable_legacy_id(
    image_name: str,
    rect: dict[str, Any],
    strength_percent: Any,
    style_reference: Any,
    config: LegacyConfig,
) -> str:
    payload = {
        "salt": config.id_salt,
        "image": Path(image_name).as_posix(),
        "mask": rect,
        "style_strength_percent": strength_percent,
        "style_reference": style_reference,
    }
    suffix = digest_json(payload)[:16]
    prefix = re.sub(r"[^A-Za-z0-9._-]", "-", config.id_prefix)
    prefix = prefix.strip("._-")[:80] or "legacy"
    return f"{prefix}-{suffix}"


def upgrade_legacy_manifest(
    path: str | Path,
    config: LegacyConfig | dict[str, Any] | None = None,
) -> tuple[ProjectManifestUpgrade, dict[str, Any]]:
    manifest_path = Path(path).resolve()
    rules = config if isinstance(config, LegacyConfig) else LegacyConfig.from_dict(config)
    try:
        document = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise WorkbenchError(E_LEGACY, f"Legacy manifest not found: {manifest_path}") from exc
    except json.JSONDecodeError as exc:
        raise WorkbenchError(E_LEGACY, f"Legacy manifest is not valid JSON: {exc}") from exc
    if int(document.get("schema_version", -1)) != 1:
        raise WorkbenchError(E_LEGACY, "Expected a schema version 1 legacy manifest", details={"actual": document.get("schema_version")})
    raw_fragments = document.get("fragments")
    if not isinstance(raw_fragments, list) or not raw_fragments:
        raise WorkbenchError(E_LEGACY, "Legacy manifest fragments must be a non-empty list")
    prepared: list[dict[str, Any]] = []
    path_to_id: dict[Path, str] = {}
    for raw in raw_fragments:
        image_value = raw.get("image") or raw.get("file")
        if not image_value:
            raise WorkbenchError(E_LEGACY, "Legacy fragment is missing its image path")
        image_path = (manifest_path.parent / str(image_value)).resolve()
        pixels, image_info = load_rgb_image(image_path)
        height, width = pixels.shape[:2]
        rect_raw = raw.get("mask_pixels") or raw.get("mask")
        if not isinstance(rect_raw, dict):
            raise WorkbenchError(E_LEGACY, "Legacy fragment is missing pixel mask coordinates", details={"image": str(image_value)})
        strength_value = raw.get("style_strength_percent", raw.get("strength_percent"))
        if isinstance(strength_value, bool):
            raise WorkbenchError(E_NUMERIC, "Legacy style strength percent must be numeric", pixel=[0, 0])
        try:
            if isinstance(strength_value, float) and not strength_value.is_integer():
                raise ValueError("not an integer")
            strength_percent = int(strength_value)
        except (TypeError, ValueError) as exc:
            raise WorkbenchError(E_NUMERIC, "Legacy style strength percent must be an integer", pixel=[0, 0]) from exc
        if strength_percent < 0 or strength_percent > rules.strength_percent_base:
            raise WorkbenchError(
                E_NUMERIC,
                "Legacy style strength percent is outside the configured range",
                pixel=[0, 0],
                details={"value": strength_percent, "base": rules.strength_percent_base},
            )
        style_reference_raw = raw.get("style_reference", image_value)
        fragment_id = _stable_legacy_id(str(image_value), rect_raw, strength_percent, style_reference_raw, rules)
        if fragment_id in path_to_id.values():
            raise WorkbenchError(E_LEGACY, "Legacy manifest produced a duplicate stable fragment identifier")
        mask_rect = _legacy_rect_to_normalized(rect_raw, width, height, rules)
        expected_mask = np.zeros((height, width), dtype=bool)
        x0 = int(rect_raw["x"])
        y0 = int(rect_raw["y"])
        if rules.coordinate_origin == "bottom-left":
            y0 = height - (y0 + int(rect_raw["height"]))
        x1 = x0 + int(rect_raw["width"])
        y1 = y0 + int(rect_raw["height"])
        expected_mask[y0:y1, x0:x1] = True
        actual_mask = MaskRegion((mask_rect,)).rasterize(width, height)
        if not np.array_equal(expected_mask, actual_mask):
            raise WorkbenchError(
                E_LEGACY,
                "Configured coordinate normalization no longer preserves the legacy mask pixel set",
                fragment_id=fragment_id,
                details={"lost_items": ["mask"], "suggestion": "increase coordinate_decimals or use the default rule"},
            )
        prepared.append(
            {
                "raw": raw,
                "image_path": image_path,
                "image_value": str(image_value),
                "pixels": pixels,
                "image_info": image_info,
                "mask_rect": mask_rect,
                "strength_percent": strength_percent,
                "style_reference_raw": style_reference_raw,
                "fragment_id": fragment_id,
            }
        )
        path_to_id[image_path] = fragment_id
    models: list[FragmentModel] = []
    for item in prepared:
        reference_raw = item["style_reference_raw"]
        if isinstance(reference_raw, str):
            reference_path = (manifest_path.parent / reference_raw).resolve()
            if reference_path not in path_to_id:
                raise WorkbenchError(
                    E_LEGACY,
                    "Legacy style_reference cannot be resolved to a manifest image",
                    fragment_id=item["fragment_id"],
                    details={"style_reference": reference_raw},
                )
            style_reference = path_to_id[reference_path]
        else:
            raise WorkbenchError(E_LEGACY, "Legacy style_reference must be an image path")
        strength = float(item["strength_percent"] / rules.strength_percent_base)
        metadata = default_metadata(
            item["image_info"]["source_format"],
            source_schema_version=1,
            source_file=item["image_value"],
            legacy_style_strength_percent=item["strength_percent"],
            legacy_mask_pixels=copy.deepcopy(item["raw"].get("mask_pixels") or item["raw"].get("mask")),
            legacy_upgrade_config=rules.to_dict(),
            legacy_source_manifest=str(manifest_path),
        )
        models.append(
            FragmentModel(
                fragment_id=item["fragment_id"],
                pixels=item["pixels"],
                mask=MaskRegion((item["mask_rect"],)),
                style_reference=style_reference,
                style_strength=strength,
                repair_iterations=int(item["raw"].get("repair_iterations", rules.default_repair_iterations)),
                output_format=str(item["raw"].get("output_format", rules.default_output_format)),
                metadata=metadata,
            )
        )
    project_name = str(document.get("project_name", manifest_path.stem))
    project_metadata = default_metadata(
        "legacy-manifest-v1",
        source_schema_version=1,
        source_manifest=str(manifest_path),
        legacy_upgrade_config=rules.to_dict(),
        project_name=project_name,
    )
    source_paths = {model.fragment_id: str(item["image_path"]) for model, item in zip(models, prepared, strict=True)}
    v2_document = _v2_document(models, project_name, project_metadata, source_paths)
    result = ProjectManifestUpgrade(
        project_name=project_name,
        fragments=models,
        metadata=project_metadata,
        format_config=copy.deepcopy(DEFAULT_FORMAT_CONFIG),
        source_path=manifest_path,
        source_v2_document=v2_document,
    )
    return result, v2_document


def _v2_document(
    models: list[FragmentModel],
    project_name: str,
    project_metadata: dict[str, Any],
    source_paths: dict[str, str],
) -> dict[str, Any]:
    return {
        "format": "image-workbench-manifest",
        "schema_version": 2,
        "project_name": project_name,
        "metadata": copy.deepcopy(project_metadata),
        "format_config": copy.deepcopy(DEFAULT_FORMAT_CONFIG),
        "fragments": [
            {
                "id": model.fragment_id,
                "image": source_paths[model.fragment_id],
                "mask": {
                    "coordinate_space": "normalized-xyxy-half-open",
                    "rects": model.mask.to_lists(),
                },
                "style_reference": model.style_reference,
                "style_strength": model.style_strength,
                "repair_iterations": model.repair_iterations,
                "output_format": model.output_format,
                "metadata": copy.deepcopy(model.metadata),
            }
            for model in models
        ],
    }


@dataclass
class ProjectManifestUpgrade:
    project_name: str
    fragments: list[FragmentModel]
    metadata: dict[str, Any]
    format_config: dict[str, Any]
    source_path: Path | None
    source_v2_document: dict[str, Any]


def export_upgraded_manifest(upgrade: ProjectManifestUpgrade, path: str | Path) -> None:
    from .atomic import atomic_write_json

    atomic_write_json(Path(path), upgrade.source_v2_document)

