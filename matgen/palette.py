"""Reference-card loading, palette matching, and deterministic quantization."""

from __future__ import annotations

import io
import math
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, UnidentifiedImageError

from .canonical import fingerprint, read_bytes, read_json, sha256_bytes
from .errors import (
    E_DIM_MISMATCH,
    E_INVALID_RECIPE,
    E_NUMERIC,
    E_PALETTE_INVALID,
    MatgenError,
)
from .noise import bilinear_resample, validate_target_resolution


PALETTE_SCHEMA = "matgen.palette.v1"
MATERIAL_SCHEMA = "matgen.material.v1"
MODULATION_X_SCALE = 0.75
MODULATION_ENERGY_SCALE = 0.35
MODULATION_BIAS_SCALE = 0.10
BASE_TINT_SCALE = 0.04
SHADE_OFFSET = 0.30
SHADE_SCALE = 0.70
ROUGHNESS_CONTROL_SCALE = 0.08
ROUGHNESS_ENERGY_SCALE = 0.03
METALNESS_CONTROL_SCALE = 0.06
METALNESS_ENERGY_SCALE = 0.02


def _palette_error(material_id: str | None, message: str, **details: Any) -> MatgenError:
    return MatgenError(
        code=E_PALETTE_INVALID,
        message=message,
        material_id=material_id,
        stage="palette",
        details=details,
    )


def validate_unit_color(value: Any, *, material_id: str | None, label: str) -> list[float]:
    if not isinstance(value, (list, tuple)) or len(value) != 3:
        raise MatgenError(
            code=E_INVALID_RECIPE,
            message=f"{label} must contain three channels",
            material_id=material_id,
            stage="palette",
        )
    channels: list[float] = []
    for channel in value:
        if isinstance(channel, bool) or not isinstance(channel, (int, float)):
            raise MatgenError(
                code=E_NUMERIC,
                message=f"{label} contains a non-numeric channel",
                material_id=material_id,
                stage="palette",
                details={"value": repr(channel)},
            )
        numeric = float(channel)
        if not math.isfinite(numeric):
            raise MatgenError(
                code=E_NUMERIC,
                message=f"{label} contains a non-finite channel",
                material_id=material_id,
                stage="palette",
                details={"value": repr(channel)},
            )
        if not 0.0 <= numeric <= 1.0:
            raise MatgenError(
                code=E_INVALID_RECIPE,
                message=f"{label} channels must be in [0, 1]",
                material_id=material_id,
                stage="palette",
                details={"value": numeric},
            )
        channels.append(numeric)
    return channels


def validate_unit_scalar(value: Any, *, material_id: str | None, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MatgenError(
            code=E_NUMERIC,
            message=f"{label} must be numeric",
            material_id=material_id,
            stage="palette",
            details={"value": repr(value)},
        )
    numeric = float(value)
    if not math.isfinite(numeric):
        raise MatgenError(
            code=E_NUMERIC,
            message=f"{label} must be finite",
            material_id=material_id,
            stage="palette",
            details={"value": repr(value)},
        )
    if not 0.0 <= numeric <= 1.0:
        raise MatgenError(
            code=E_INVALID_RECIPE,
            message=f"{label} must be in [0, 1]",
            material_id=material_id,
            stage="palette",
            details={"value": numeric},
        )
    return numeric


def _unique_sorted_palette(pixels: np.ndarray) -> np.ndarray:
    if pixels.ndim != 2 or pixels.shape[1] != 3:
        raise ValueError("palette pixels must have shape (n, 3)")
    unique = {tuple(int(channel) for channel in pixel) for pixel in pixels}
    return np.asarray(sorted(unique), dtype=np.uint8)


def _fallback_palette(base_color: list[float]) -> np.ndarray:
    shades = np.arange(16, dtype=np.float64) / 15.0
    shades = 0.18 + 0.82 * shades
    colors = np.asarray(base_color, dtype=np.float64)[None, :] * shades[:, None]
    quantized = np.rint(np.clip(colors, 0.0, 1.0) * 255.0).astype(np.uint8)
    return _unique_sorted_palette(quantized)


def _load_reference_card(
    material_id: str | None,
    card_name: str,
    cards_path: str | Path | None,
) -> tuple[np.ndarray, dict[str, Any]]:
    if cards_path is None:
        raise _palette_error(
            material_id,
            "a palette card was requested but no card manifest was supplied",
            palette_card=card_name,
        )
    manifest_path = Path(cards_path)
    try:
        manifest = read_json(manifest_path)
    except MatgenError as exc:
        raise _palette_error(
            material_id,
            "palette card manifest is unreadable",
            palette_card=card_name,
            reason=exc.message,
        ) from exc
    if not isinstance(manifest, dict):
        raise _palette_error(material_id, "palette manifest must be a JSON object")
    cards = manifest.get("cards")
    if not isinstance(cards, dict) or card_name not in cards:
        raise _palette_error(
            material_id,
            "palette card name is not present in the manifest",
            palette_card=card_name,
        )
    card = cards[card_name]
    if not isinstance(card, dict):
        raise _palette_error(material_id, "palette card entry must be an object", palette_card=card_name)
    if card.get("empty") is True:
        raise _palette_error(material_id, "palette card is explicitly empty", palette_card=card_name)

    image_name = manifest.get("image")
    if not isinstance(image_name, str) or not image_name:
        raise _palette_error(material_id, "palette manifest has no image name")
    image_path = (manifest_path.parent / image_name).resolve()
    image_bytes = read_bytes(image_path)
    try:
        with Image.open(io.BytesIO(image_bytes)) as source_image:
            actual_size = [source_image.width, source_image.height]
            image = source_image.convert("RGB")
    except (UnidentifiedImageError, OSError) as exc:
        raise _palette_error(
            material_id,
            "palette PNG cannot be decoded",
            image=str(image_path),
            reason=str(exc),
        ) from exc

    declared_image_size = manifest.get("image_size")
    if declared_image_size != actual_size:
        raise MatgenError(
            code=E_DIM_MISMATCH,
            message="palette image dimensions do not match the manifest declaration",
            material_id=material_id,
            stage="palette",
            details={"declared": declared_image_size, "actual": actual_size},
        )

    box = card.get("box")
    if not isinstance(box, (list, tuple)) or len(box) != 4 or any(
        isinstance(value, bool) or not isinstance(value, int) for value in box
    ):
        raise _palette_error(
            material_id,
            "palette card box must be [left, top, right, bottom]",
            palette_card=card_name,
            box=box,
        )
    left, top, right, bottom = [int(value) for value in box]
    declared_size = card.get("declared_size")
    actual_box_size = [max(0, right - left), max(0, bottom - top)]
    if actual_box_size[0] == 0 or actual_box_size[1] == 0:
        raise _palette_error(
            material_id,
            "palette card box is empty",
            palette_card=card_name,
            box=box,
        )
    if declared_size != actual_box_size:
        raise MatgenError(
            code=E_DIM_MISMATCH,
            message="palette card crop dimensions do not match declared_size",
            material_id=material_id,
            stage="palette",
            details={"palette_card": card_name, "declared": declared_size, "actual": actual_box_size},
        )
    if left < 0 or top < 0 or right > actual_size[0] or bottom > actual_size[1]:
        raise MatgenError(
            code=E_DIM_MISMATCH,
            message="palette card box lies outside the reference image",
            material_id=material_id,
            stage="palette",
            details={"palette_card": card_name, "box": box, "image_size": actual_size},
        )
    crop = np.asarray(image.crop((left, top, right, bottom)), dtype=np.uint8)
    palette_u8 = _unique_sorted_palette(crop.reshape(-1, 3))
    if palette_u8.size == 0:
        raise _palette_error(material_id, "palette card contains no colors", palette_card=card_name)
    metadata = {
        "source": "reference_card",
        "palette_card": card_name,
        "manifest_name": manifest_path.name,
        "manifest_sha256": sha256_bytes(canonical_manifest_bytes(manifest)),
        "image_name": image_name,
        "image_sha256": sha256_bytes(image_bytes),
        "image_size": actual_size,
        "box": [left, top, right, bottom],
        "declared_size": declared_size,
        "unique_color_count": int(palette_u8.shape[0]),
    }
    return palette_u8, metadata


def canonical_manifest_bytes(manifest: Any) -> bytes:
    from .canonical import canonical_json_bytes

    return canonical_json_bytes(manifest)


def load_palette(
    recipe: dict[str, Any],
    *,
    cards_path: str | Path | None,
) -> dict[str, Any]:
    material_id = recipe.get("material_id")
    base_color = validate_unit_color(recipe.get("base_color"), material_id=material_id, label="base_color")
    card_name = recipe.get("palette_card")
    if card_name is None:
        palette_u8 = _fallback_palette(base_color)
        metadata = {
            "source": "base_color_fallback",
            "palette_card": None,
            "unique_color_count": int(palette_u8.shape[0]),
            "fallback_base_color": base_color,
        }
    else:
        if not isinstance(card_name, str) or not card_name:
            raise _palette_error(material_id, "palette_card must be a non-empty string")
        palette_u8, metadata = _load_reference_card(material_id, card_name, cards_path)

    if palette_u8.ndim != 2 or palette_u8.shape[0] == 0 or palette_u8.shape[1] != 3:
        raise _palette_error(material_id, "palette is empty or malformed")
    if palette_u8.dtype != np.uint8 or np.any(palette_u8 > 255):
        raise _palette_error(material_id, "palette contains illegal channel values")
    palette_float = palette_u8.astype(np.float64) / 255.0
    source_fingerprint = fingerprint(
        {
            "metadata": metadata,
            "palette_rgb_u8": palette_u8.tolist(),
        }
    )
    return {
        "schema": PALETTE_SCHEMA,
        "material_id": material_id,
        "palette_u8": palette_u8,
        "palette_float": palette_float,
        "metadata": metadata,
        "source_fingerprint": source_fingerprint,
    }


def nearest_palette_indices(target_rgb: np.ndarray, palette_float: np.ndarray) -> np.ndarray:
    """Nearest RGB match with deterministic first-index tie breaking."""

    if target_rgb.ndim != 3 or target_rgb.shape[2] != 3:
        raise ValueError("target_rgb must have shape (height, width, 3)")
    if palette_float.ndim != 2 or palette_float.shape[1] != 3 or palette_float.shape[0] == 0:
        raise ValueError("palette_float must have shape (n, 3)")
    flat_target = target_rgb.reshape(-1, 3)
    best_indices = np.empty(flat_target.shape[0], dtype=np.int64)
    best_distances = np.full(flat_target.shape[0], np.inf, dtype=np.float64)
    chunk_size = 128
    for start in range(0, palette_float.shape[0], chunk_size):
        stop = min(start + chunk_size, palette_float.shape[0])
        difference = flat_target[:, None, :] - palette_float[None, start:stop, :]
        distances = np.sum(difference * difference, axis=2)
        local_indices = np.argmin(distances, axis=1)
        local_distances = distances[np.arange(flat_target.shape[0]), local_indices]
        improve = local_distances < best_distances
        best_indices[improve] = start + local_indices[improve]
        best_distances[improve] = local_distances[improve]
    return best_indices.reshape(target_rgb.shape[:2])


def validate_quantized_texture(texture: np.ndarray, *, material_id: str | None) -> np.ndarray:
    if not isinstance(texture, np.ndarray) or texture.ndim != 3 or texture.shape[2] not in (3, 4):
        raise _palette_error(material_id, "quantized texture must be an HxWx3 or HxWx4 array")
    if texture.dtype != np.uint8:
        raise _palette_error(material_id, "quantized texture dtype must be uint8")
    if texture.size and (int(texture.min()) < 0 or int(texture.max()) > 255):
        raise _palette_error(
            material_id,
            "quantized texture contains illegal channel values",
            minimum=int(texture.min()),
            maximum=int(texture.max()),
        )
    return texture


def quantize_texture(
    indices: np.ndarray,
    palette_u8: np.ndarray,
    *,
    material_id: str | None,
) -> np.ndarray:
    if indices.ndim != 2:
        raise _palette_error(material_id, "palette index map must be two-dimensional")
    if palette_u8.ndim != 2 or palette_u8.shape[1] != 3:
        raise _palette_error(material_id, "palette must have shape (n, 3)")
    if np.any(indices < 0) or np.any(indices >= palette_u8.shape[0]):
        raise _palette_error(material_id, "palette index map is out of range")
    rgb = palette_u8[indices]
    alpha = np.full((*rgb.shape[:2], 1), 255, dtype=np.uint8)
    rgba = np.concatenate((rgb, alpha), axis=2)
    return validate_quantized_texture(rgba, material_id=material_id).copy()


def derived_control_values(control_vector: list[float], base_color: list[float]) -> dict[str, float | list[float]]:
    vector = np.asarray(control_vector, dtype=np.float64)
    energy = float(np.mean(np.abs(vector)))
    tint = np.clip(np.asarray(base_color, dtype=np.float64) + BASE_TINT_SCALE * vector[:3], 0.0, 1.0)
    return {
        "energy": energy,
        "bias": float(vector[0]),
        "tint": [float(value) for value in tint],
        "gray_x_scale": MODULATION_X_SCALE + MODULATION_ENERGY_SCALE * energy,
        "roughness_input": float(vector[3]),
        "metalness_input": float(vector[4]),
    }


def modulate_gray(gray: np.ndarray, derived: dict[str, Any]) -> np.ndarray:
    modulation = (
        0.5
        + (gray - 0.5) * float(derived["gray_x_scale"])
        + MODULATION_BIAS_SCALE * float(derived["bias"])
    )
    return np.clip(modulation, 0.0, 1.0)


def map_roughness(recipe_roughness: float, derived: dict[str, Any]) -> float:
    mapped = (
        recipe_roughness
        + ROUGHNESS_CONTROL_SCALE * float(derived["roughness_input"])
        + ROUGHNESS_ENERGY_SCALE * (float(derived["energy"]) - 0.5)
    )
    return float(np.clip(mapped, 0.0, 1.0))


def map_metalness(recipe_metalness: float, derived: dict[str, Any]) -> float:
    mapped = (
        recipe_metalness
        + METALNESS_CONTROL_SCALE * float(derived["metalness_input"])
        - METALNESS_ENERGY_SCALE * (float(derived["energy"]) - 0.5)
    )
    return float(np.clip(mapped, 0.0, 1.0))


def encode_png(rgba: np.ndarray) -> bytes:
    buffer = io.BytesIO()
    Image.fromarray(rgba[:, :, :3]).save(
        buffer,
        format="PNG",
        optimize=False,
        compress_level=9,
    )
    return buffer.getvalue()


def build_palette_artifacts(
    recipe: dict[str, Any],
    control_artifact: dict[str, Any],
    noise_artifact: dict[str, Any],
    palette_spec: dict[str, Any],
) -> dict[str, Any]:
    material_id = recipe.get("material_id")
    base_color = validate_unit_color(recipe.get("base_color"), material_id=material_id, label="base_color")
    roughness = validate_unit_scalar(recipe.get("roughness"), material_id=material_id, label="roughness")
    metalness = validate_unit_scalar(recipe.get("metalness"), material_id=material_id, label="metalness")
    target_width, target_height = validate_target_resolution(recipe)
    source_field = noise_artifact["field"]
    if noise_artifact["target_width"] != target_width or noise_artifact["target_height"] != target_height:
        raise MatgenError(
            code=E_DIM_MISMATCH,
            message="noise artifact target resolution differs from recipe",
            material_id=material_id,
            stage="palette",
        )
    gray = bilinear_resample(source_field, target_height, target_width)
    if not np.all(np.isfinite(gray)):
        raise MatgenError(
            code=E_NUMERIC,
            message="resampled gray field contains non-finite values",
            material_id=material_id,
            stage="palette",
        )
    derived = derived_control_values(control_artifact["control_vector"], base_color)
    modulation = modulate_gray(gray, derived)
    tint = np.asarray(derived["tint"], dtype=np.float64)
    target_rgb = tint[None, None, :] * (SHADE_OFFSET + SHADE_SCALE * modulation)[:, :, None]
    target_rgb = np.clip(target_rgb, 0.0, 1.0)
    indices = nearest_palette_indices(target_rgb, palette_spec["palette_float"])
    rgba = quantize_texture(indices, palette_spec["palette_u8"], material_id=material_id)
    png_bytes = encode_png(rgba)
    mapped_roughness = map_roughness(roughness, derived)
    mapped_metalness = map_metalness(metalness, derived)
    texture_mean_rgb = np.mean(rgba[:, :, :3].astype(np.float64) / 255.0, axis=(0, 1))
    material = {
        "schema": MATERIAL_SCHEMA,
        "material_id": material_id,
        "description": recipe.get("description"),
        "base_color": base_color,
        "roughness": mapped_roughness,
        "metalness": mapped_metalness,
        "texture": {
            "format": "rgba8",
            "width": target_width,
            "height": target_height,
            "channels": 4,
            "quantization_bits": 8,
            "palette_index_count": int(np.unique(indices).size),
            "palette_size": int(palette_spec["palette_u8"].shape[0]),
            "mean_rgb": [float(value) for value in texture_mean_rgb],
        },
        "palette": {
            **palette_spec["metadata"],
            "rgb_u8": palette_spec["palette_u8"].tolist(),
        },
        "control_energy": float(derived["energy"]),
        "derived_tint": [float(value) for value in tint],
    }
    return {
        "schema": "matgen.palette_artifacts.v1",
        "material_id": material_id,
        "material": material,
        "texture_rgba": rgba,
        "texture_png": png_bytes,
        "texture_sha256": sha256_bytes(rgba.tobytes(order="C")),
        "png_sha256": sha256_bytes(png_bytes),
        "palette_indices": indices,
        "gray_resampled": gray,
        "modulated_gray": modulation,
        "target_rgb": target_rgb,
        "palette_spec": palette_spec,
        "derived": derived,
        "recipe_roughness": roughness,
        "recipe_metalness": metalness,
    }


def palette_stage_fingerprint(
    recipe: dict[str, Any],
    control_digest: str,
    noise_digest: str,
    palette_source_digest: str,
) -> str:
    target_width, target_height = validate_target_resolution(recipe)
    return fingerprint(
        {
            "base_color": recipe.get("base_color"),
            "roughness": recipe.get("roughness"),
            "metalness": recipe.get("metalness"),
            "resolution": [target_width, target_height],
            "palette_card": recipe.get("palette_card"),
            "palette_source_digest": palette_source_digest,
            "control_digest": control_digest,
            "noise_digest": noise_digest,
            "mapping_version": "palette-mapping-v1",
        }
    )

