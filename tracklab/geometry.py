"""Normalized box validation, pixel conversion, and error metrics."""

from __future__ import annotations

import math
from typing import Any, Sequence

from .errors import E_INVALID_REGION, raise_error

Box = list[float]


def validate_box(box: Sequence[Any], *, sequence_id: str, frame_index: int, label: str = "box") -> Box:
    if not isinstance(box, (list, tuple)) or len(box) != 4:
        raise_error(
            E_INVALID_REGION,
            f"{label} must contain four normalized coordinates",
            context={"sequence_id": sequence_id, "frame_index": frame_index, "box": box, "diagnostic": "invalid_box_shape"},
        )
    try:
        x, y, w, h = (float(v) for v in box)
    except (TypeError, ValueError):
        raise_error(
            E_INVALID_REGION,
            f"{label} contains non-numeric coordinates",
            context={"sequence_id": sequence_id, "frame_index": frame_index, "box": box, "diagnostic": "non_numeric_box"},
        )
    if not all(math.isfinite(v) for v in (x, y, w, h)):
        raise_error(
            E_INVALID_REGION,
            f"{label} contains non-finite coordinates",
            context={"sequence_id": sequence_id, "frame_index": frame_index, "box": box, "diagnostic": "non_finite_box"},
        )
    if w <= 0.0 or h <= 0.0:
        raise_error(
            E_INVALID_REGION,
            f"{label} is empty",
            context={"sequence_id": sequence_id, "frame_index": frame_index, "box": [x, y, w, h], "diagnostic": "empty_box"},
        )
    eps = 1.0e-9
    if x < -eps or y < -eps or x + w > 1.0 + eps or y + h > 1.0 + eps:
        raise_error(
            E_INVALID_REGION,
            f"{label} leaves the image",
            context={"sequence_id": sequence_id, "frame_index": frame_index, "box": [x, y, w, h], "diagnostic": "box_outside_image"},
        )
    return [x, y, w, h]


def box_to_pixels(box: Sequence[float], width: int, height: int, **error_context: Any) -> tuple[int, int, int, int]:
    x, y, w, h = (float(v) for v in box)
    x0 = int(math.floor(x * width + 0.5))
    y0 = int(math.floor(y * height + 0.5))
    x1 = int(math.floor((x + w) * width + 0.5))
    y1 = int(math.floor((y + h) * height + 0.5))
    x0 = max(0, min(width, x0))
    y0 = max(0, min(height, y0))
    x1 = max(0, min(width, x1))
    y1 = max(0, min(height, y1))
    if x1 <= x0 or y1 <= y0:
        raise_error(
            E_INVALID_REGION,
            "region maps to zero pixels",
            context={**error_context, "box": [x, y, w, h], "image_width": width, "image_height": height, "diagnostic": "empty_pixel_region"},
        )
    return x0, y0, x1, y1


def box_center(box: Sequence[float]) -> list[float]:
    return [float(box[0]) + float(box[2]) / 2.0, float(box[1]) + float(box[3]) / 2.0]


def iou(box_a: Sequence[float], box_b: Sequence[float]) -> float:
    ax, ay, aw, ah = (float(v) for v in box_a)
    bx, by, bw, bh = (float(v) for v in box_b)
    ix0 = max(ax, bx)
    iy0 = max(ay, by)
    ix1 = min(ax + aw, bx + bw)
    iy1 = min(ay + ah, by + bh)
    iw = max(0.0, ix1 - ix0)
    ih = max(0.0, iy1 - iy0)
    intersection = iw * ih
    union = aw * ah + bw * bh - intersection
    if union <= 0:
        return 0.0
    return max(0.0, min(1.0, intersection / union))


def center_error_normalized(box_pred: Sequence[float], box_true: Sequence[float]) -> float:
    pcx, pcy = box_center(box_pred)
    tcx, tcy = box_center(box_true)
    scale = max(float(box_true[2]), float(box_true[3]), 1.0e-12)
    return math.hypot(pcx - tcx, pcy - tcy) / scale


def shifted_box(box: Sequence[float], dx: float, dy: float, scale: float = 1.0) -> Box:
    cx = float(box[0]) + float(box[2]) / 2.0 + dx
    cy = float(box[1]) + float(box[3]) / 2.0 + dy
    w = float(box[2]) * scale
    h = float(box[3]) * scale
    return [cx - w / 2.0, cy - h / 2.0, w, h]
