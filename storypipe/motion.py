from __future__ import annotations

from typing import Any

import numpy as np

from .models import Shot


IDENTITY_MATRIX = [
    [1.0, 0.0, 0.0],
    [0.0, 1.0, 0.0],
    [0.0, 0.0, 1.0],
]


def frame_progress(local_frame: int, duration_frames: int) -> float:
    if duration_frames <= 1:
        return 0.0
    return float(local_frame) / float(duration_frames - 1)


def transform_matrix(
    shot: Shot,
    local_frame: int,
    width: int,
    height: int,
    config: dict[str, Any],
) -> list[list[float]]:
    if shot.camera_move == "static":
        return [row[:] for row in IDENTITY_MATRIX]
    progress = frame_progress(local_frame, shot.duration_frames)
    motion = config["motion"]
    if shot.camera_move == "pan":
        start = motion["pan"]["start"]
        end = motion["pan"]["end"]
        tx = (start[0] + (end[0] - start[0]) * progress) * width
        ty = (start[1] + (end[1] - start[1]) * progress) * height
        return [[1.0, 0.0, tx], [0.0, 1.0, ty], [0.0, 0.0, 1.0]]
    zoom = motion["zoom"]
    scale = zoom["scale_start"] + (zoom["scale_end"] - zoom["scale_start"]) * progress
    inverse = 1.0 / scale
    center_x = zoom["center"][0] * width
    center_y = zoom["center"][1] * height
    drift_x = zoom["drift"][0] * width * progress
    drift_y = zoom["drift"][1] * height * progress
    tx = center_x * (1.0 - inverse) + drift_x
    ty = center_y * (1.0 - inverse) + drift_y
    return [[inverse, 0.0, tx], [0.0, inverse, ty], [0.0, 0.0, 1.0]]


def bilinear_sample(image: np.ndarray, matrix: list[list[float]]) -> np.ndarray:
    if image.ndim != 3 or image.shape[2] != 3:
        raise ValueError("image must have shape HxWx3")
    height, width = image.shape[:2]
    xs = np.arange(width, dtype=np.float64)
    ys = np.arange(height, dtype=np.float64)
    matrix_array = np.asarray(matrix, dtype=np.float64)
    source_x = matrix_array[0, 0] * xs[None, :] + matrix_array[0, 1] * ys[:, None] + matrix_array[0, 2]
    source_y = matrix_array[1, 0] * xs[None, :] + matrix_array[1, 1] * ys[:, None] + matrix_array[1, 2]
    source_x = np.clip(source_x, 0.0, float(width - 1))
    source_y = np.clip(source_y, 0.0, float(height - 1))
    x0 = np.floor(source_x).astype(np.intp)
    y0 = np.floor(source_y).astype(np.intp)
    x1 = np.minimum(x0 + 1, width - 1)
    y1 = np.minimum(y0 + 1, height - 1)
    fx = (source_x - x0).astype(np.float32)[..., None]
    fy = (source_y - y0).astype(np.float32)[..., None]
    top = image[y0, x0] * (np.float32(1.0) - fx) + image[y0, x1] * fx
    bottom = image[y1, x0] * (np.float32(1.0) - fx) + image[y1, x1] * fx
    result = top * (np.float32(1.0) - fy) + bottom * fy
    return np.clip(result, np.float32(0.0), np.float32(1.0)).astype(np.float32, copy=False)


def compose_frame(
    image: np.ndarray,
    shot: Shot,
    local_frame: int,
    config: dict[str, Any],
) -> tuple[np.ndarray, list[list[float]]]:
    height, width = image.shape[:2]
    matrix = transform_matrix(shot, local_frame, width, height, config)
    return bilinear_sample(image, matrix), matrix


def blend_frames(sources: list[tuple[np.ndarray, float]]) -> np.ndarray:
    if not sources:
        raise ValueError("at least one blend source is required")
    result = np.zeros_like(sources[0][0], dtype=np.float32)
    for frame, weight in sources:
        result += np.asarray(frame, dtype=np.float32) * np.float32(weight)
    return np.clip(result, np.float32(0.0), np.float32(1.0))
