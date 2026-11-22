"""Deterministic NumPy operator kernels.

All floating-point work uses float64. Final RGB quantization uses round-half-up
(``floor(x + 0.5)``) followed by an 8-bit clamp. No random, model or external
image service is involved.
"""

from __future__ import annotations

import math
import time
from typing import Any

import numpy as np

from .errors import E_TASK_TIMEOUT, WorkbenchError


KERNEL_VERSION = "ops-v1"
PNG_ENCODER_VERSION = "png-rgb-v1"
PARSER_VERSION = "chain-v1"

OPERATOR_NAMES = ("crop", "resize", "rotate", "color_shift")


def quantize_rgb(values: np.ndarray) -> np.ndarray:
    clipped = np.clip(values, 0.0, 255.0)
    return np.floor(clipped + 0.5).astype(np.uint8)


def crop(pixels: np.ndarray, params: dict[str, Any]) -> np.ndarray:
    x = int(params["x"])
    y = int(params["y"])
    width = int(params["width"])
    height = int(params["height"])
    image_h, image_w, _ = pixels.shape
    x0 = max(0, x)
    y0 = max(0, y)
    x1 = min(image_w, x + width)
    y1 = min(image_h, y + height)
    if x1 <= x0 or y1 <= y0:
        raise ValueError("crop has empty intersection with current image")
    return pixels[y0:y1, x0:x1, :].copy()


def _nearest_indices(dst_size: int, src_size: int) -> np.ndarray:
    positions = np.arange(dst_size, dtype=np.float64)
    source = (positions + 0.5) * (float(src_size) / float(dst_size)) - 0.5
    indices = np.floor(source + 0.5).astype(np.int64)
    return np.clip(indices, 0, src_size - 1)


def _bilinear_axis(dst_size: int, src_size: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    positions = np.arange(dst_size, dtype=np.float64)
    source = (positions + 0.5) * (float(src_size) / float(dst_size)) - 0.5
    source = np.clip(source, 0.0, float(src_size - 1))
    lower = np.floor(source).astype(np.int64)
    upper = np.minimum(lower + 1, src_size - 1)
    weight = source - lower.astype(np.float64)
    return lower, upper, weight


def resize(pixels: np.ndarray, params: dict[str, Any]) -> np.ndarray:
    width = int(params["width"])
    height = int(params["height"])
    mode = str(params.get("mode", "bilinear"))
    src_h, src_w, _ = pixels.shape
    if mode == "nearest":
        rows = _nearest_indices(height, src_h)
        columns = _nearest_indices(width, src_w)
        return pixels[rows[:, None], columns[None, :], :].copy()
    if mode != "bilinear":
        raise ValueError(f"unsupported resize mode: {mode}")
    y0, y1, wy = _bilinear_axis(height, src_h)
    x0, x1, wx = _bilinear_axis(width, src_w)
    wy = wy[:, None, None]
    wx = wx[None, :, None]
    top_left = pixels[y0[:, None], x0[None, :], :].astype(np.float64)
    top_right = pixels[y0[:, None], x1[None, :], :].astype(np.float64)
    bottom_left = pixels[y1[:, None], x0[None, :], :].astype(np.float64)
    bottom_right = pixels[y1[:, None], x1[None, :], :].astype(np.float64)
    top = top_left * (1.0 - wx) + top_right * wx
    bottom = bottom_left * (1.0 - wx) + bottom_right * wx
    output = top * (1.0 - wy) + bottom * wy
    return quantize_rgb(output)


def rotate(pixels: np.ndarray, params: dict[str, Any]) -> np.ndarray:
    angle = float(params["angle"])
    normalized = angle % 360.0
    if normalized == 0.0 or normalized == 180.0:
        cosine = 1.0 if normalized == 0.0 else -1.0
        sine = 0.0
    elif normalized == 90.0:
        cosine, sine = 0.0, 1.0
    elif normalized == 270.0:
        cosine, sine = 0.0, -1.0
    else:
        radians = math.radians(normalized)
        cosine = math.cos(radians)
        sine = math.sin(radians)

    height, width, _ = pixels.shape
    center_x = (width - 1) / 2.0
    center_y = (height - 1) / 2.0
    y_grid, x_grid = np.indices((height, width), dtype=np.float64)
    dx = x_grid - center_x
    dy = y_grid - center_y
    source_x = center_x + cosine * dx - sine * dy
    source_y = center_y + sine * dx + cosine * dy
    sample_x = np.floor(source_x + 0.5).astype(np.int64)
    sample_y = np.floor(source_y + 0.5).astype(np.int64)
    sample_x = np.clip(sample_x, 0, width - 1)
    sample_y = np.clip(sample_y, 0, height - 1)
    return pixels[sample_y, sample_x, :].copy()


def color_shift(pixels: np.ndarray, params: dict[str, Any]) -> np.ndarray:
    deltas = np.array(
        [params.get("red", 0.0), params.get("green", 0.0), params.get("blue", 0.0)],
        dtype=np.float64,
    )
    output = pixels.astype(np.float64) + deltas[None, None, :]
    return quantize_rgb(output)


OPERATORS = {
    "crop": crop,
    "resize": resize,
    "rotate": rotate,
    "color_shift": color_shift,
}


def apply_chain(
    pixels: np.ndarray,
    operations: list[dict[str, Any]],
    *,
    deadline: float | None = None,
    task_id: str | None = None,
) -> np.ndarray:
    current = np.asarray(pixels, dtype=np.uint8)
    for position, operation in enumerate(operations):
        if deadline is not None and time.monotonic() > deadline:
            raise WorkbenchError(
                E_TASK_TIMEOUT,
                "task exceeded its execution time budget",
                task_id=task_id,
                stage="executed",
                position=position,
                reason="TASK_TIME_BUDGET_EXCEEDED",
            )
        name = operation["op"]
        params = operation.get("params", {})
        current = OPERATORS[name](current, params)
    if deadline is not None and time.monotonic() > deadline:
        raise WorkbenchError(
            E_TASK_TIMEOUT,
            "task exceeded its execution time budget",
            task_id=task_id,
            stage="executed",
            reason="TASK_TIME_BUDGET_EXCEEDED",
        )
    return np.ascontiguousarray(current)
