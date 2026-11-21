"""Stage 1: grayscale/contrast preprocessing and per-keypoint heatmaps."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
from PIL import Image, UnidentifiedImageError

from .config import PipelineConfig
from .constants import PREPROCESS, SCHEMA_VERSION
from .errors import invalid_param
from .models import ShardSpec, coordinate_to_pixel
from .util import encode_array, file_sha256, finalize_artifact


def preprocess_shard(
    shard: ShardSpec,
    config: PipelineConfig,
    *,
    input_fingerprint: str,
    image_path: str | None = None,
) -> dict[str, Any]:
    stage = PREPROCESS
    if not math.isfinite(config.gaussian_radius) or config.gaussian_radius < 0:
        raise invalid_param(
            "gaussian_radius must be a finite non-negative number",
            stage=stage,
            shard_id=shard.shard_id,
            details={"gaussian_radius": config.gaussian_radius},
        )
    if not math.isfinite(config.contrast_factor) or config.contrast_factor < 0:
        raise invalid_param(
            "contrast_factor must be a finite non-negative number",
            stage=stage,
            shard_id=shard.shard_id,
            details={"contrast_factor": config.contrast_factor},
        )
    actual_path = image_path or shard.image_path
    try:
        with Image.open(actual_path) as image:
            rgb = image.convert("RGB")
            actual_width, actual_height = rgb.size
    except (OSError, UnidentifiedImageError) as exc:
        raise invalid_param(
            f"cannot decode shard image: {exc}",
            stage=stage,
            shard_id=shard.shard_id,
            details={"image": str(actual_path)},
        ) from exc
    if (actual_width, actual_height) != (shard.width, shard.height):
        raise invalid_param(
            "manifest image dimensions do not match decoded PNG dimensions",
            stage=stage,
            shard_id=shard.shard_id,
            details={
                "declared": [shard.width, shard.height],
                "actual": [actual_width, actual_height],
            },
        )

    heatmap_width = config.heatmap_width if config.heatmap_width is not None else actual_width
    heatmap_height = config.heatmap_height if config.heatmap_height is not None else actual_height
    if (heatmap_width, heatmap_height) != (actual_width, actual_height):
        raise invalid_param(
            "heatmap dimensions must match image dimensions",
            stage=stage,
            shard_id=shard.shard_id,
            details={
                "image_size": [actual_width, actual_height],
                "heatmap_size": [heatmap_width, heatmap_height],
            },
        )

    grayscale = np.asarray(rgb.convert("L"), dtype=np.float32) / np.float32(255.0)
    contrasted = np.clip(
        (grayscale - np.float32(0.5)) * np.float32(config.contrast_factor)
        + np.float32(0.5),
        0.0,
        1.0,
    ).astype(np.float32, copy=False)

    heatmaps: dict[str, np.ndarray] = {
        name: np.zeros((actual_height, actual_width), dtype=np.float32)
        for name in shard.keypoints
    }
    visible_counts = {name: 0 for name in shard.keypoints}
    for instance in shard.instances:
        for name, point in instance.keypoints.items():
            if name not in heatmaps or not point.visible:
                continue
            visible_counts[name] += 1
            center_x = coordinate_to_pixel(point.x, actual_width)
            center_y = coordinate_to_pixel(point.y, actual_height)
            blob = _gaussian_blob(
                actual_height,
                actual_width,
                center_y,
                center_x,
                config.gaussian_radius,
            )
            np.maximum(heatmaps[name], blob, out=heatmaps[name])

    invisible_keypoints = sorted(name for name, count in visible_counts.items() if count == 0)
    heatmap_payload: dict[str, Any] = {}
    for name in shard.keypoints:
        array = heatmaps[name].astype("<f4", copy=False)
        zero = bool(not np.any(array))
        heatmap_payload[name] = {
            **encode_array(array),
            "invisible": name in invisible_keypoints,
            "zero": zero,
            "visible_instance_count": visible_counts[name],
        }

    payload = {
        "schema_version": SCHEMA_VERSION,
        "stage": PREPROCESS,
        "shard_id": shard.shard_id,
        "input_fingerprint": input_fingerprint,
        "config_version": config.stage_version(PREPROCESS),
        "image": {
            "sha256": file_sha256(actual_path),
            "width": actual_width,
            "height": actual_height,
        },
        "coordinate_convention": "normalized_topleft_xy",
        "gaussian_radius": config.gaussian_radius,
        "contrast_factor": config.contrast_factor,
        "grayscale": encode_array(contrasted),
        "heatmaps": heatmap_payload,
        "invisible_keypoints": invisible_keypoints,
    }
    return finalize_artifact(payload)


def decode_grayscale(artifact: dict[str, Any]) -> np.ndarray:
    from .util import decode_array

    return decode_array(artifact["grayscale"])


def decode_heatmaps(artifact: dict[str, Any]) -> dict[str, np.ndarray]:
    from .util import decode_array

    return {
        name: decode_array(payload)
        for name, payload in sorted(artifact["heatmaps"].items())
    }


def _gaussian_blob(
    height: int,
    width: int,
    center_y: float,
    center_x: float,
    radius: float,
) -> np.ndarray:
    if radius == 0:
        result = np.zeros((height, width), dtype=np.float32)
        y = min(max(int(round(center_y)), 0), height - 1)
        x = min(max(int(round(center_x)), 0), width - 1)
        result[y, x] = np.float32(1.0)
        return result
    extent = max(1, int(math.ceil(radius * 3.0)))
    x_min = max(0, int(math.floor(center_x)) - extent)
    x_max = min(width - 1, int(math.ceil(center_x)) + extent)
    y_min = max(0, int(math.floor(center_y)) - extent)
    y_max = min(height - 1, int(math.ceil(center_y)) + extent)
    yy, xx = np.mgrid[y_min : y_max + 1, x_min : x_max + 1]
    denominator = np.float64(2.0) * np.float64(radius) * np.float64(radius)
    squared = (xx.astype(np.float64) - center_x) ** 2 + (
        yy.astype(np.float64) - center_y
    ) ** 2
    values = np.exp(-squared / denominator).astype(np.float32)
    result = np.zeros((height, width), dtype=np.float32)
    result[y_min : y_max + 1, x_min : x_max + 1] = values
    return result
