"""Stage 2: deterministic local-maximum decoding and subpixel correction."""

from __future__ import annotations

import math
from collections import deque
from typing import Any

import numpy as np

from .config import PipelineConfig
from .constants import ARTIFACT_ROUND_DIGITS, DECODE, SCHEMA_VERSION
from .errors import invalid_param
from .models import pixel_to_coordinate
from .preprocess import decode_heatmaps
from .util import finalize_artifact, round_float


def decode_peaks(
    preprocess_artifact: dict[str, Any],
    config: PipelineConfig,
    *,
    input_fingerprint: str,
) -> dict[str, Any]:
    shard_id = str(preprocess_artifact["shard_id"])
    if config.max_candidates < 0:
        raise invalid_param(
            "max_candidates must be non-negative",
            stage=DECODE,
            shard_id=shard_id,
            details={"max_candidates": config.max_candidates},
        )
    if config.subpixel_radius < 0:
        raise invalid_param(
            "subpixel_radius must be non-negative",
            stage=DECODE,
            shard_id=shard_id,
            details={"subpixel_radius": config.subpixel_radius},
        )
    if config.peak_threshold < 0:
        raise invalid_param(
            "peak_threshold must be non-negative",
            stage=DECODE,
            shard_id=shard_id,
            details={"peak_threshold": config.peak_threshold},
        )
    width = int(preprocess_artifact["image"]["width"])
    height = int(preprocess_artifact["image"]["height"])
    heatmaps = decode_heatmaps(preprocess_artifact)
    records: list[dict[str, Any]] = []
    all_candidates: list[dict[str, Any]] = []

    for keypoint in sorted(heatmaps):
        heatmap = heatmaps[keypoint]
        if heatmap.shape != (height, width):
            raise ValueError(f"corrupt preprocess heatmap shape for {keypoint}")
        candidates = _candidates_for_heatmap(
            heatmap,
            keypoint=keypoint,
            config=config,
            width=width,
            height=height,
        )
        candidates.sort(
            key=lambda item: (
                -float(item["response"]),
                float(item["pixel_y"]),
                float(item["pixel_x"]),
            )
        )
        candidates = candidates[: config.max_candidates]
        marker = preprocess_artifact["heatmaps"][keypoint]
        if candidates:
            reason = None
        elif marker.get("invisible"):
            reason = "invisible_zero_heatmap"
        elif config.max_candidates == 0:
            reason = "candidate_limit_zero"
        elif not np.any(heatmap):
            reason = "zero_response"
        else:
            reason = "no_local_maximum"
        records.append(
            {
                "keypoint": keypoint,
                "candidates": candidates,
                "empty": not candidates,
                "reason": reason,
                "heatmap_zero": bool(marker.get("zero")),
                "invisible": bool(marker.get("invisible")),
            }
        )
        all_candidates.extend(candidates)

    all_candidates.sort(
        key=lambda item: (
            -float(item["response"]),
            str(item["keypoint"]),
            float(item["pixel_y"]),
            float(item["pixel_x"]),
        )
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "stage": DECODE,
        "shard_id": shard_id,
        "input_fingerprint": input_fingerprint,
        "config_version": config.stage_version(DECODE),
        "upstream_digest": preprocess_artifact["artifact_digest"],
        "image": dict(preprocess_artifact["image"]),
        "keypoints": records,
        "candidates": all_candidates,
        "candidate_count": len(all_candidates),
        "empty_candidate_records": sum(1 for record in records if record["empty"]),
    }
    return finalize_artifact(payload)


def _candidates_for_heatmap(
    heatmap: np.ndarray,
    *,
    keypoint: str,
    config: PipelineConfig,
    width: int,
    height: int,
) -> list[dict[str, Any]]:
    if config.max_candidates == 0 or not np.any(heatmap):
        return []
    padded = np.pad(heatmap, 1, mode="constant", constant_values=0.0)
    local_maximum = np.ones_like(heatmap, dtype=bool)
    for delta_y in (-1, 0, 1):
        for delta_x in (-1, 0, 1):
            if delta_x == 0 and delta_y == 0:
                continue
            neighbor = padded[1 + delta_y : 1 + delta_y + height, 1 + delta_x : 1 + delta_x + width]
            local_maximum &= heatmap >= neighbor
    local_maximum &= heatmap > np.float32(config.peak_threshold)

    visited = np.zeros_like(local_maximum, dtype=bool)
    peak_coordinates: list[tuple[int, int]] = []
    for y, x in np.argwhere(local_maximum):
        y = int(y)
        x = int(x)
        if visited[y, x]:
            continue
        queue: deque[tuple[int, int]] = deque([(y, x)])
        visited[y, x] = True
        component: list[tuple[int, int]] = []
        while queue:
            current_y, current_x = queue.popleft()
            component.append((current_y, current_x))
            for delta_y in (-1, 0, 1):
                for delta_x in (-1, 0, 1):
                    if delta_x == 0 and delta_y == 0:
                        continue
                    next_y = current_y + delta_y
                    next_x = current_x + delta_x
                    if not (0 <= next_y < height and 0 <= next_x < width):
                        continue
                    if visited[next_y, next_x] or not local_maximum[next_y, next_x]:
                        continue
                    visited[next_y, next_x] = True
                    queue.append((next_y, next_x))
        component.sort(key=lambda item: (-float(heatmap[item]), item[0], item[1]))
        peak_coordinates.append(component[0])

    results: list[dict[str, Any]] = []
    for peak_y, peak_x in peak_coordinates:
        subpixel_x, subpixel_y = _weighted_centroid(
            heatmap, peak_x, peak_y, config.subpixel_radius
        )
        response = float(heatmap[peak_y, peak_x])
        results.append(
            {
                "keypoint": keypoint,
                "pixel_x": round_float(subpixel_x, ARTIFACT_ROUND_DIGITS),
                "pixel_y": round_float(subpixel_y, ARTIFACT_ROUND_DIGITS),
                "normalized_x": round_float(
                    pixel_to_coordinate(subpixel_x, width), ARTIFACT_ROUND_DIGITS
                ),
                "normalized_y": round_float(
                    pixel_to_coordinate(subpixel_y, height), ARTIFACT_ROUND_DIGITS
                ),
                "peak_pixel_x": int(peak_x),
                "peak_pixel_y": int(peak_y),
                "response": round_float(response, ARTIFACT_ROUND_DIGITS),
            }
        )
    return results


def _weighted_centroid(
    heatmap: np.ndarray, peak_x: int, peak_y: int, radius: int
) -> tuple[float, float]:
    height, width = heatmap.shape
    x_min = max(0, peak_x - radius)
    x_max = min(width - 1, peak_x + radius)
    y_min = max(0, peak_y - radius)
    y_max = min(height - 1, peak_y + radius)
    region = heatmap[y_min : y_max + 1, x_min : x_max + 1].astype(np.float64)
    if not np.any(region > 0):
        return float(peak_x), float(peak_y)
    weights = region
    total = float(weights.sum())
    if total <= 0 or not math.isfinite(total):
        return float(peak_x), float(peak_y)
    yy, xx = np.mgrid[y_min : y_max + 1, x_min : x_max + 1]
    center_x = float((xx * weights).sum() / total)
    center_y = float((yy * weights).sum() / total)
    return center_x, center_y
