"""Deterministic bilinear-warp renderer and frame/error summaries."""

from __future__ import annotations

from typing import Any

import numpy as np

from .manifest import STATE_KEYS
from .util import frame_fingerprint


def render_frame(
    base_frame: np.ndarray,
    initial_state: dict[str, Any],
    state: dict[str, Any],
    render_config: dict[str, Any],
) -> np.ndarray:
    """Warp a canonical frame with an inverse affine map and bilinear sampling.

    The destination-to-source mapping is centered on the canvas. Translation is the
    state delta multiplied by ``translation_gain``. Deformation expands X and
    compresses Y symmetrically around the center. Edge coordinates are clamped
    (edge extension), then the float image is rounded to uint8.
    """

    base = np.asarray(base_frame)
    if base.ndim != 2:
        raise ValueError("base_frame must be two-dimensional")
    height, width = base.shape
    source = base.astype(np.float64, copy=False)

    gain = float(render_config["translation_gain"])
    deformation_gain = float(render_config["deformation_scale"])
    dx = (float(state["x"]) - float(initial_state["x"])) * gain
    dy = (float(state["y"]) - float(initial_state["y"])) * gain
    deform_delta = (
        float(state["deformation"]) - float(initial_state["deformation"])
    ) * deformation_gain

    scale_x = max(0.25, 1.0 + deform_delta)
    scale_y = max(0.25, 1.0 - 0.5 * deform_delta)
    center_x = (width - 1.0) / 2.0
    center_y = (height - 1.0) / 2.0

    dst_x = np.arange(width, dtype=np.float64)[None, :]
    dst_y = np.arange(height, dtype=np.float64)[:, None]
    src_x = (dst_x - center_x - dx) / scale_x + center_x
    src_y = (dst_y - center_y - dy) / scale_y + center_y

    x0 = np.floor(src_x).astype(np.int64)
    y0 = np.floor(src_y).astype(np.int64)
    x1 = x0 + 1
    y1 = y0 + 1
    wx = src_x - x0
    wy = src_y - y0

    x0c = np.clip(x0, 0, width - 1)
    x1c = np.clip(x1, 0, width - 1)
    y0c = np.clip(y0, 0, height - 1)
    y1c = np.clip(y1, 0, height - 1)

    top = source[y0c, x0c] * (1.0 - wx) + source[y0c, x1c] * wx
    bottom = source[y1c, x0c] * (1.0 - wx) + source[y1c, x1c] * wx
    warped = top * (1.0 - wy) + bottom * wy
    return np.rint(np.clip(warped, 0.0, 255.0)).astype(np.uint8)


def summarize_frame(frame: np.ndarray) -> dict[str, Any]:
    values = frame.astype(np.float64, copy=False)
    return {
        "mean": float(values.mean()),
        "variance": float(values.var()),
        "fingerprint": frame_fingerprint(frame),
    }


def mean_absolute_error(prediction: np.ndarray, truth: np.ndarray) -> float:
    if prediction.shape != truth.shape:
        raise ValueError("prediction and truth frame shapes differ")
    difference = prediction.astype(np.float64) - truth.astype(np.float64)
    return float(np.abs(difference).mean())


def state_components(state: dict[str, Any]) -> dict[str, float]:
    return {key: float(state[key]) for key in STATE_KEYS}
