"""Text detection via connected components + row-projection merging.

The pipeline is intentionally simple and fully deterministic:
grayscale -> fixed threshold binarization -> two-pass connected components ->
row band grouping -> gap-based character merging -> candidate line boxes.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .config import DETECT_STRATEGY_PARAMS, MergeConfig, WorkbenchConfig
from .errors import DetectInvalid
from .glyphs import binarize, to_grayscale


@dataclass
class Component:
    label: int
    x0: int
    y0: int
    x1: int
    y1: int
    area: int


def label_components(mask: np.ndarray) -> list[Component]:
    """Two-pass 4-connectivity labeling with an on-the-fly union-find."""
    height, width = mask.shape
    labels = np.zeros((height, width), dtype=np.int32)
    parent: list[int] = [0]

    def find(value: int) -> int:
        root = value
        while parent[root] != root:
            root = parent[root]
        while parent[value] != value:
            parent[value], value = root, parent[value]
        return root

    def union(left: int, right: int) -> None:
        root_left, root_right = find(left), find(right)
        if root_left != root_right:
            if root_left < root_right:
                parent[root_right] = root_left
            else:
                parent[root_left] = root_right

    next_label = 1
    for y in range(height):
        for x in range(width):
            if not mask[y, x]:
                continue
            up = labels[y - 1, x] if y > 0 else 0
            left = labels[y, x - 1] if x > 0 else 0
            if up == 0 and left == 0:
                labels[y, x] = next_label
                parent.append(next_label)
                next_label += 1
            elif up != 0 and left == 0:
                labels[y, x] = find(up)
            elif up == 0 and left != 0:
                labels[y, x] = find(left)
            else:
                union(up, left)
                labels[y, x] = find(up)

    stats: dict[int, list[int]] = {}
    ys, xs = np.nonzero(labels)
    for y, x in zip(ys.tolist(), xs.tolist()):
        root = find(int(labels[y, x]))
        current = stats.get(root)
        if current is None:
            stats[root] = [x, y, x, y, 1]
        else:
            current[0] = min(current[0], x)
            current[1] = min(current[1], y)
            current[2] = max(current[2], x)
            current[3] = max(current[3], y)
            current[4] += 1
    return [
        Component(label=label, x0=x0, y0=y0, x1=x1 + 1, y1=y1 + 1, area=area)
        for label, (x0, y0, x1, y1, area) in sorted(stats.items())
    ]


def _effective_params(config: WorkbenchConfig, strategy_id: str) -> MergeConfig:
    overrides = DETECT_STRATEGY_PARAMS[strategy_id]
    base = config.merge
    return MergeConfig(
        min_component_area=overrides.get("min_component_area", base.min_component_area),
        min_component_height=overrides.get("min_component_height", base.min_component_height),
        row_band_tol=overrides.get("row_band_tol", base.row_band_tol),
        gap_ratio=overrides.get("gap_ratio", base.gap_ratio),
        pad_x=overrides.get("pad_x", base.pad_x),
        pad_y=overrides.get("pad_y", base.pad_y),
    )


def _merge_components(components: list[Component], params: MergeConfig) -> list[Component]:
    kept = [
        component
        for component in components
        if component.area >= params.min_component_area
        and (component.y1 - component.y0) >= params.min_component_height
    ]
    if not kept:
        return []
    # Row projection: build occupied-row runs from the vertical component spans
    # and split runs separated by at least row_band_tol empty rows.
    max_y = max(component.y1 for component in kept)
    row_occupied = np.zeros(max_y + 1, dtype=bool)
    for component in kept:
        row_occupied[component.y0 : component.y1] = True
    ranges: list[list[int]] = []
    run_start: int | None = None
    empty_run = 0
    for row in range(max_y + 1):
        if row_occupied[row]:
            if run_start is None:
                run_start = row
            empty_run = 0
        else:
            empty_run += 1
            if run_start is not None and empty_run >= params.row_band_tol:
                ranges.append([run_start, row - empty_run + 1])
                run_start = None
    if run_start is not None:
        ranges.append([run_start, max_y + 1])
    bands: list[list[Component]] = [[] for _ in ranges]
    for component in kept:
        overlaps = [
            index
            for index, (start, end) in enumerate(ranges)
            if not (component.y1 <= start or component.y0 >= end)
        ]
        band_index = overlaps[0] if overlaps else min(range(len(ranges)), key=lambda idx: abs(ranges[idx][0] - component.y0))
        bands[band_index].append(component)

    boxes: list[Component] = []
    median_height = float(np.median([component.y1 - component.y0 for component in kept]))
    for band in bands:
        ordered = sorted(band, key=lambda item: item.x0)
        if not ordered:
            continue
        current = ordered[0]
        current = Component(current.label, current.x0, current.y0, current.x1, current.y1, current.area)
        for component in ordered[1:]:
            gap = component.x0 - current.x1
            if gap <= max(1, int(round(params.gap_ratio * median_height))):
                current = Component(
                    current.label,
                    min(current.x0, component.x0),
                    min(current.y0, component.y0),
                    max(current.x1, component.x1),
                    max(current.y1, component.y1),
                    current.area + component.area,
                )
            else:
                boxes.append(current)
                current = component
        boxes.append(current)
    return sorted(boxes, key=lambda item: (item.y0, item.x0))


def detect_shard(
    image,
    config: WorkbenchConfig,
    image_id: str,
    strategy_id: str,
) -> dict:
    """Run detection on one shard and return a serializable result record."""
    params = _effective_params(config, strategy_id)
    gray = to_grayscale(image)
    mask = binarize(gray, config.binarize.threshold)
    components = label_components(mask)
    raw_boxes = _merge_components(components, params)
    height, width = mask.shape

    candidates: list[dict] = []
    errors: list[dict] = []
    total_foreground = max(1, int(mask.sum()))
    for index, box in enumerate(raw_boxes):
        invalid_reasons = []
        if box.x0 < 0 or box.y0 < 0 or box.x1 > width or box.y1 > height:
            invalid_reasons.append("out_of_bounds")
        if box.x1 <= box.x0 or box.y1 <= box.y0:
            invalid_reasons.append("zero_size")
        if invalid_reasons:
            errors.append(
                {
                    "code": "E_DETECT_INVALID",
                    "image_id": image_id,
                    "box_index": index,
                    "reasons": invalid_reasons,
                    "pixel_box": {"x0": box.x0, "y0": box.y0, "x1": box.x1, "y1": box.y1},
                }
            )
            continue
        x0 = max(0, box.x0 - params.pad_x)
        y0 = max(0, box.y0 - params.pad_y)
        x1 = min(width, box.x1 + params.pad_x)
        y1 = min(height, box.y1 + params.pad_y)
        coverage = int(mask[y0:y1, x0:x1].sum())
        area = max(1, (x1 - x0) * (y1 - y0))
        confidence = round(min(1.0, 0.45 + 0.55 * coverage / area), config.confidence_decimals)
        candidates.append(
            {
                "candidate_id": f"{image_id}:cand:{index:03d}",
                "box_index": index,
                "strategy": strategy_id,
                "confidence": confidence,
                "pixel_box": {"x0": x0, "y0": y0, "x1": x1, "y1": y1},
                "box": {
                    "x0": round(x0 / width, 6),
                    "y0": round(y0 / height, 6),
                    "x1": round(x1 / width, 6),
                    "y1": round(y1 / height, 6),
                },
                "foreground_share": round(coverage / total_foreground, config.confidence_decimals),
            }
        )

    return {
        "image_id": image_id,
        "strategy": strategy_id,
        "params": {
            "threshold": config.binarize.threshold,
            "min_component_area": params.min_component_area,
            "min_component_height": params.min_component_height,
            "row_band_tol": params.row_band_tol,
            "gap_ratio": params.gap_ratio,
            "pad_x": params.pad_x,
            "pad_y": params.pad_y,
        },
        "component_count": len(components),
        "candidate_count": len(candidates),
        "candidates": candidates,
        "errors": errors,
    }


def detect_cost(strategy_id: str, component_count: int, candidate_count: int) -> int:
    """Abstract deterministic compute cost used by the budget decision."""
    tier = {"detect_fast": 1, "detect_balanced": 2, "detect_deep": 4}[strategy_id]
    return tier * (8 + 2 * component_count + 6 * candidate_count)


def assert_no_invalid(result: dict) -> None:
    """Helper used by tests to surface retained detection errors as exceptions."""
    if result["errors"]:
        raise DetectInvalid(
            "detection produced invalid boxes",
            {"image_id": result["image_id"], "errors": result["errors"]},
        )
