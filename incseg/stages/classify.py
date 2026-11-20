"""Stage 1: per-pixel semantic classification.

Each pixel is scored against every configured class color (negative squared
distance, normalized to [0, 1]). A configurable neighborhood smoothing weight
mixes in the fraction of same-class neighbors; iterations are synchronous so
the result is order independent. Ties break deterministically toward the
lowest class id.
"""

from __future__ import annotations

import numpy as np

from ..config import parse_hex_color
from .common import neighbor_count_map, valid_neighbor_count

_MAX_DIST2 = 3.0 * 255.0 * 255.0


def run_classify(image_rgb: np.ndarray, tile) -> np.ndarray:
    pairs = sorted(tile.color_map.items(), key=lambda kv: kv[1])
    class_ids = np.array([class_id for _, class_id in pairs], dtype=np.int64)
    colors = np.array([parse_hex_color(color) for color, _ in pairs], dtype=np.float64)

    pixels = image_rgb.astype(np.float64)
    dist2 = ((pixels[:, :, None, :] - colors[None, None, :, :]) ** 2).sum(axis=3)
    base_score = 1.0 - dist2 / _MAX_DIST2  # (H, W, K), higher is better

    labels = np.argmax(base_score, axis=2).astype(np.int32)
    if tile.smooth_iterations > 0 and tile.smooth_weight > 0.0:
        valid = valid_neighbor_count(labels.shape, 8).astype(np.float64)
        for _ in range(tile.smooth_iterations):
            frac = np.empty_like(base_score)
            for idx in range(len(class_ids)):
                same = neighbor_count_map(labels == idx, 8).astype(np.float64)
                frac[:, :, idx] = same / valid
            score = base_score + tile.smooth_weight * frac
            labels = np.argmax(score, axis=2).astype(np.int32)

    return class_ids[labels].astype(np.uint8)
