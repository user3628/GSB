"""Stage 2: table line, header and merged-cell recovery."""

from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple

import numpy as np

from .canonical import rounded
from .errors import E_TABLE_STRUCTURE, PipelineError
from .imaging import contiguous_groups


class _DisjointSet:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))

    def find(self, value: int) -> int:
        while self.parent[value] != value:
            self.parent[value] = self.parent[self.parent[value]]
            value = self.parent[value]
        return value

    def union(self, left: int, right: int) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root != right_root:
            if left_root < right_root:
                self.parent[right_root] = left_root
            else:
                self.parent[left_root] = right_root


def _line_centers(groups: Sequence[Tuple[int, int]]) -> List[int]:
    return [int((start + end) // 2) for start, end in groups]


def _detect_line_groups(
    mask: np.ndarray, axis: str, coverage: float
) -> List[Tuple[int, int]]:
    if axis == "row":
        values = mask.mean(axis=1)
    else:
        values = mask.mean(axis=0)
    indices = np.flatnonzero(values >= coverage).tolist()
    return contiguous_groups(indices)


def _segment_has_line(
    mask: np.ndarray,
    *,
    y0: int,
    y1: int,
    x0: int,
    x1: int,
    threshold: float,
) -> bool:
    if y1 <= y0 or x1 <= x0:
        return False
    segment = mask[y0:y1, x0:x1]
    return bool(segment.size and float(segment.mean()) >= threshold)


def validate_cell_structures(
    cells: Sequence[Dict[str, Any]],
    *,
    rows: int,
    cols: int,
    shard_id: str,
) -> None:
    for cell in cells:
        row = int(cell["row"])
        col = int(cell["col"])
        row_span = int(cell["row_span"])
        col_span = int(cell["col_span"])
        pixel_range = cell.get("pixel_range", [0, 0, 0, 0])
        if (
            row < 0
            or col < 0
            or row_span < 1
            or col_span < 1
            or row + row_span > rows
            or col + col_span > cols
        ):
            raise PipelineError(
                E_TABLE_STRUCTURE,
                "merged cell span is outside declared table dimensions",
                shard_id=shard_id,
                location=[row, col, row_span, col_span],
                diagnostics={
                    "reason": "span_out_of_bounds",
                    "declared_rows": rows,
                    "declared_cols": cols,
                },
            )
        if len(pixel_range) != 4 or pixel_range[2] <= pixel_range[0] or pixel_range[3] <= pixel_range[1]:
            raise PipelineError(
                E_TABLE_STRUCTURE,
                "cell has zero area",
                shard_id=shard_id,
                location=[row, col, row_span, col_span],
                diagnostics={"reason": "zero_area_cell", "pixel_range": list(pixel_range)},
            )


def _header_rows(
    rgb: np.ndarray,
    boundaries_x: Sequence[int],
    boundaries_y: Sequence[int],
    rows: int,
    cols: int,
    threshold: float,
) -> int:
    row_means: List[float] = []
    for row in range(rows):
        samples: List[float] = []
        for col in range(cols):
            x0, x1 = boundaries_x[col], boundaries_x[col + 1]
            y0, y1 = boundaries_y[row], boundaries_y[row + 1]
            left = x0 + max(2, (x1 - x0) // 5)
            right = x1 - max(2, (x1 - x0) // 5)
            top = y0 + max(2, (y1 - y0) // 5)
            bottom = y1 - max(2, (y1 - y0) // 5)
            if right <= left or bottom <= top:
                continue
            patch = rgb[top:bottom, left:right]
            if patch.size:
                samples.append(float(patch.mean()))
        row_means.append(float(np.mean(samples)) if samples else 255.0)
    count = 0
    for mean_value in row_means:
        if mean_value < threshold:
            count += 1
        else:
            break
    return count


def restore_table(
    rgb: np.ndarray,
    gray: np.ndarray,
    *,
    region: Sequence[int],
    declared: Dict[str, Any],
    config: Dict[str, Any],
    shard_id: str,
) -> Dict[str, Any]:
    x0, y0, x1, y1 = [int(value) for value in region]
    if x1 <= x0 or y1 <= y0:
        raise PipelineError(
            E_TABLE_STRUCTURE,
            "table region has zero area",
            shard_id=shard_id,
            location=[x0, y0, x1, y1],
            diagnostics={"reason": "zero_area_region"},
        )
    crop_gray = gray[y0:y1, x0:x1]
    crop_rgb = rgb[y0:y1, x0:x1]
    mask = crop_gray < 60
    row_groups = _detect_line_groups(mask, "row", float(config["line_coverage"]))
    column_groups = _detect_line_groups(mask, "column", float(config["line_coverage"]))
    detected_rows = max(0, len(row_groups) - 1)
    detected_cols = max(0, len(column_groups) - 1)
    declared_rows = int(declared["rows"])
    declared_cols = int(declared["cols"])

    if detected_rows != declared_rows or detected_cols != declared_cols:
        if detected_rows == declared_cols and detected_cols == declared_rows:
            reason = "row_column_direction_conflict"
        else:
            reason = "row_column_count_mismatch"
        raise PipelineError(
            E_TABLE_STRUCTURE,
            "detected table dimensions conflict with the manifest declaration",
            shard_id=shard_id,
            location=[detected_rows, detected_cols],
            diagnostics={
                "reason": reason,
                "detected_rows": detected_rows,
                "detected_cols": detected_cols,
                "declared_rows": declared_rows,
                "declared_cols": declared_cols,
                "row_line_groups": [list(group) for group in row_groups],
                "column_line_groups": [list(group) for group in column_groups],
            },
        )
    if detected_rows < 1 or detected_cols < 1:
        raise PipelineError(
            E_TABLE_STRUCTURE,
            "table has no data grid",
            shard_id=shard_id,
            location=[detected_rows, detected_cols],
            diagnostics={"reason": "empty_grid"},
        )

    boundaries_x = _line_centers(column_groups)
    boundaries_y = _line_centers(row_groups)
    dsu = _DisjointSet(detected_rows * detected_cols)

    def cell_id(row: int, col: int) -> int:
        return row * detected_cols + col

    segment_threshold = float(config["segment_coverage"])
    for boundary in range(1, detected_rows):
        line_start, line_end = row_groups[boundary]
        for col in range(detected_cols):
            left = boundaries_x[col] + 2
            right = boundaries_x[col + 1] - 2
            present = _segment_has_line(
                mask,
                y0=line_start,
                y1=line_end,
                x0=left,
                x1=right,
                threshold=segment_threshold,
            )
            if not present:
                dsu.union(cell_id(boundary - 1, col), cell_id(boundary, col))
    for boundary in range(1, detected_cols):
        line_start, line_end = column_groups[boundary]
        for row in range(detected_rows):
            top = boundaries_y[row] + 2
            bottom = boundaries_y[row + 1] - 2
            present = _segment_has_line(
                mask,
                y0=top,
                y1=bottom,
                x0=line_start,
                x1=line_end,
                threshold=segment_threshold,
            )
            if not present:
                dsu.union(cell_id(row, boundary - 1), cell_id(row, boundary))

    components: Dict[int, List[Tuple[int, int]]] = {}
    for row in range(detected_rows):
        for col in range(detected_cols):
            root = dsu.find(cell_id(row, col))
            components.setdefault(root, []).append((row, col))

    cells: List[Dict[str, Any]] = []
    for members in components.values():
        member_rows = [row for row, _ in members]
        member_cols = [col for _, col in members]
        top_row, bottom_row = min(member_rows), max(member_rows)
        left_col, right_col = min(member_cols), max(member_cols)
        row_span = bottom_row - top_row + 1
        col_span = right_col - left_col + 1
        if len(members) != row_span * col_span:
            raise PipelineError(
                E_TABLE_STRUCTURE,
                "detected merge is not rectangular",
                shard_id=shard_id,
                location=[top_row, left_col, row_span, col_span],
                diagnostics={"reason": "non_rectangular_merge", "member_count": len(members)},
            )
        pixel_range = [
            x0 + boundaries_x[left_col],
            y0 + boundaries_y[top_row],
            x0 + boundaries_x[left_col + col_span],
            y0 + boundaries_y[top_row + row_span],
        ]
        normalized_range = [
            rounded((pixel_range[0] - x0) / max(1, x1 - x0)),
            rounded((pixel_range[1] - y0) / max(1, y1 - y0)),
            rounded((pixel_range[2] - x0) / max(1, x1 - x0)),
            rounded((pixel_range[3] - y0) / max(1, y1 - y0)),
        ]
        cells.append(
            {
                "row": top_row,
                "col": left_col,
                "row_span": row_span,
                "col_span": col_span,
                "pixel_range": pixel_range,
                "normalized_range": normalized_range,
            }
        )
    cells.sort(key=lambda item: (item["row"], item["col"]))
    validate_cell_structures(
        cells, rows=detected_rows, cols=detected_cols, shard_id=shard_id
    )
    header_rows = _header_rows(
        crop_rgb,
        boundaries_x,
        boundaries_y,
        detected_rows,
        detected_cols,
        float(config["header_brightness"]),
    )
    if header_rows <= 0 or header_rows >= detected_rows:
        raise PipelineError(
            E_TABLE_STRUCTURE,
            "header row detection produced an invalid range",
            shard_id=shard_id,
            location=[header_rows, 0],
            diagnostics={"reason": "invalid_header_rows", "detected_rows": detected_rows},
        )
    return {
        "shard_id": shard_id,
        "region_pixel": [x0, y0, x1, y1],
        "detected_rows": detected_rows,
        "detected_cols": detected_cols,
        "declared_rows": declared_rows,
        "declared_cols": declared_cols,
        "header_rows": header_rows,
        "header_region_pixel": [
            x0 + boundaries_x[0],
            y0 + boundaries_y[0],
            x0 + boundaries_x[detected_cols],
            y0 + boundaries_y[header_rows],
        ],
        "cells": cells,
        "line_evidence": {
            "method": "row_column_projection_peaks_and_missing_segments",
            "row_boundaries_pixel": [x0 + value for value in boundaries_y],
            "column_boundaries_pixel": [x0 + value for value in boundaries_x],
            "row_line_groups_local": [list(group) for group in row_groups],
            "column_line_groups_local": [list(group) for group in column_groups],
            "segment_coverage_threshold": segment_threshold,
        },
    }
