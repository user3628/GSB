"""Stage 1: deterministic connected-component region detection."""

from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple

import numpy as np

from .canonical import rounded
from .errors import E_INVALID_REGION, PipelineError
from .imaging import connected_components, contiguous_groups


def _normalized_rect(rect: Sequence[int], width: int, height: int) -> List[float]:
    x0, y0, x1, y1 = [int(value) for value in rect]
    return [
        rounded(x0 / width),
        rounded(y0 / height),
        rounded(x1 / width),
        rounded(y1 / height),
    ]


def validate_region(
    rect: Sequence[int], width: int, height: int, *, shard_id: str
) -> Tuple[int, int, int, int]:
    if len(rect) != 4:
        raise PipelineError(
            E_INVALID_REGION,
            "region must contain four pixel coordinates",
            shard_id=shard_id,
            location=list(rect),
            diagnostics={"reason": "invalid_coordinate_count"},
        )
    x0, y0, x1, y1 = [int(value) for value in rect]
    if x1 <= x0 or y1 <= y0:
        raise PipelineError(
            E_INVALID_REGION,
            "detected region has zero width or height",
            shard_id=shard_id,
            location=[x0, y0, x1, y1],
            diagnostics={"reason": "zero_area", "image_size": [width, height]},
        )
    if x0 < 0 or y0 < 0 or x1 > width or y1 > height:
        raise PipelineError(
            E_INVALID_REGION,
            "detected region is outside image bounds",
            shard_id=shard_id,
            location=[x0, y0, x1, y1],
            diagnostics={"reason": "out_of_bounds", "image_size": [width, height]},
        )
    return x0, y0, x1, y1


def _candidate_from_component(
    component: Dict[str, int],
    crop: np.ndarray,
    width: int,
    height: int,
    config: Dict[str, Any],
    kind: str,
) -> Dict[str, Any]:
    x0, y0, x1, y1 = (
        component["x0"],
        component["y0"],
        component["x1"],
        component["y1"],
    )
    row_coverage = crop.mean(axis=1) if crop.shape[1] else np.zeros(0)
    column_coverage = crop.mean(axis=0) if crop.shape[0] else np.zeros(0)
    row_lines = int(np.count_nonzero(row_coverage >= float(config["line_coverage"])))
    column_lines = int(np.count_nonzero(column_coverage >= float(config["line_coverage"])))
    row_groups = contiguous_groups(
        np.flatnonzero(row_coverage >= float(config["line_coverage"])).tolist()
    )
    column_groups = contiguous_groups(
        np.flatnonzero(column_coverage >= float(config["line_coverage"])).tolist()
    )
    grid_lines = row_lines + column_lines
    grid_line_groups = len(row_groups) + len(column_groups)
    area = max(1, component["area"])
    bbox_area = max(1, (x1 - x0) * (y1 - y0))
    density = area / bbox_area
    if kind == "table":
        confidence = min(0.999, 0.55 + grid_lines / 80.0 + min(density, 0.5) * 0.2)
    else:
        aspect = (x1 - x0) / max(1, y1 - y0)
        aspect_score = max(0.0, 1.0 - abs(aspect - 1.0) / 3.0)
        confidence = min(0.999, 0.45 + aspect_score * 0.25 + min(density, 0.3))
    return {
        "kind": kind,
        "pixel_rect": [x0, y0, x1, y1],
        "region": _normalized_rect([x0, y0, x1, y1], width, height),
        "confidence": rounded(confidence),
        "evidence": {
            "method": "connected_component+row_column_projection",
            "component_pixels": component["area"],
            "bbox_area": bbox_area,
            "density": rounded(density),
            "full_row_pixels": row_lines,
            "full_column_pixels": column_lines,
            "row_line_groups": [list(group) for group in row_groups],
            "column_line_groups": [list(group) for group in column_groups],
            "projection_line_score": grid_lines,
            "projection_line_groups": grid_line_groups,
        },
    }


def detect_regions(
    gray: np.ndarray,
    *,
    shard_id: str,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    if gray.ndim != 2 or gray.shape[0] <= 0 or gray.shape[1] <= 0:
        raise PipelineError(
            E_INVALID_REGION,
            "image is empty",
            shard_id=shard_id,
            location=[0, 0, 0, 0],
            diagnostics={"reason": "zero_area_image"},
        )
    height, width = gray.shape
    binary = gray < int(config["threshold"])
    labels, components = connected_components(binary, return_labels=True)

    table_candidates: List[Dict[str, Any]] = []
    chart_candidates: List[Dict[str, Any]] = []
    for component in components:
        component_width = component["x1"] - component["x0"]
        component_height = component["y1"] - component["y0"]
        if component["area"] < int(config["min_component_pixels"]):
            continue
        if component_width < int(config["min_component_width"]):
            continue
        if component_height < int(config["min_component_height"]):
            continue
        x0, y0, x1, y1 = (
            component["x0"],
            component["y0"],
            component["x1"],
            component["y1"],
        )
        crop = labels[y0:y1, x0:x1] == int(component["label"])
        row_indices = np.flatnonzero(
            crop.mean(axis=1) >= float(config["line_coverage"])
        )
        column_indices = np.flatnonzero(
            crop.mean(axis=0) >= float(config["line_coverage"])
        )
        row_lines = int(row_indices.size)
        column_lines = int(column_indices.size)
        row_line_groups = len(contiguous_groups(row_indices.tolist()))
        column_line_groups = len(contiguous_groups(column_indices.tolist()))
        grid_line_groups = row_line_groups + column_line_groups
        if grid_line_groups >= int(config["table_min_grid_lines"]):
            table_candidates.append(
                _candidate_from_component(component, crop, width, height, config, "table")
            )
        else:
            bbox_area = component_width * component_height
            if bbox_area >= 0.02 * width * height:
                chart_candidates.append(
                    _candidate_from_component(component, crop, width, height, config, "chart")
                )

    table_candidates.sort(
        key=lambda candidate: (
            -candidate["evidence"]["projection_line_groups"],
            -candidate["evidence"]["projection_line_score"],
            -(candidate["pixel_rect"][2] - candidate["pixel_rect"][0])
            * (candidate["pixel_rect"][3] - candidate["pixel_rect"][1]),
            tuple(candidate["pixel_rect"]),
        )
    )
    chart_candidates.sort(
        key=lambda candidate: (
            -(
                (candidate["pixel_rect"][2] - candidate["pixel_rect"][0])
                * (candidate["pixel_rect"][3] - candidate["pixel_rect"][1])
            ),
            tuple(candidate["pixel_rect"]),
        )
    )

    if not table_candidates:
        raise PipelineError(
            E_INVALID_REGION,
            "no table candidate was detected",
            shard_id=shard_id,
            location=[0, 0, width, height],
            diagnostics={"reason": "missing_table_candidate", "component_count": len(components)},
        )
    if not chart_candidates:
        raise PipelineError(
            E_INVALID_REGION,
            "no chart candidate was detected",
            shard_id=shard_id,
            location=[0, 0, width, height],
            diagnostics={"reason": "missing_chart_candidate", "component_count": len(components)},
        )

    selected_table = table_candidates[0]
    selected_chart = chart_candidates[0]
    validate_region(selected_table["pixel_rect"], width, height, shard_id=shard_id)
    validate_region(selected_chart["pixel_rect"], width, height, shard_id=shard_id)

    return {
        "shard_id": shard_id,
        "image_size": [width, height],
        "config": {
            "threshold": int(config["threshold"]),
            "line_coverage": float(config["line_coverage"]),
            "table_min_grid_lines": int(config["table_min_grid_lines"]),
        },
        "table": selected_table,
        "chart": selected_chart,
        "candidates": {
            "table": table_candidates[: int(config["max_regions_per_kind"])],
            "chart": chart_candidates[: int(config["max_regions_per_kind"])],
        },
    }
