"""Stage 4: manifest-truth validation metrics."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .canonical import rounded
from .imaging import iou


def _merge_map_from_cells(cells: Iterable[Dict[str, Any]]) -> Dict[Tuple[int, int], Tuple[int, int, int, int]]:
    mapping: Dict[Tuple[int, int], Tuple[int, int, int, int]] = {}
    for cell in cells:
        row = int(cell["row"])
        col = int(cell["col"])
        row_span = int(cell["row_span"])
        col_span = int(cell["col_span"])
        origin = (row, col, row_span, col_span)
        for rr in range(row, row + row_span):
            for cc in range(col, col + col_span):
                mapping[(rr, cc)] = origin
    return mapping


def _truth_merge_map(table_truth: Dict[str, Any]) -> Dict[Tuple[int, int], Tuple[int, int, int, int]]:
    if table_truth.get("cells"):
        return _merge_map_from_cells(table_truth["cells"])
    rows = int(table_truth["rows"])
    cols = int(table_truth["cols"])
    mapping: Dict[Tuple[int, int], Tuple[int, int, int, int]] = {}
    merges = table_truth.get("merges", [])
    lookup: Dict[Tuple[int, int], Dict[str, int]] = {}
    for merge in merges:
        for rr in range(int(merge["row"]), int(merge["row"]) + int(merge["row_span"])):
            for cc in range(int(merge["col"]), int(merge["col"]) + int(merge["col_span"])):
                lookup[(rr, cc)] = merge
    for row in range(rows):
        for col in range(cols):
            merge = lookup.get((row, col))
            if merge is None:
                mapping[(row, col)] = (row, col, 1, 1)
            else:
                mapping[(row, col)] = (
                    int(merge["row"]),
                    int(merge["col"]),
                    int(merge["row_span"]),
                    int(merge["col_span"]),
                )
    return mapping


def _cell_accuracy(table_truth: Dict[str, Any], table_result: Optional[Dict[str, Any]]) -> float:
    rows = int(table_truth["rows"])
    cols = int(table_truth["cols"])
    total = max(1, rows * cols)
    truth_map = _truth_merge_map(table_truth)
    if not table_result or not table_result.get("cells"):
        return 0.0
    predicted = _merge_map_from_cells(table_result["cells"])
    matches = 0
    for row in range(rows):
        for col in range(cols):
            if predicted.get((row, col)) == truth_map.get((row, col)):
                matches += 1
    return rounded(matches / total)


def _chart_metrics(
    chart_truth: Dict[str, Any], chart_result: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    truth_series = {
        str(item["name"]): [float(value) for value in item.get("values", [])]
        for item in chart_truth.get("series", [])
    }
    predicted_series: Dict[str, List[float]] = {}
    if chart_result:
        for item in chart_result.get("series", []):
            predicted_series[str(item["name"])] = [float(point["y"]) for point in item.get("points", [])]
    elif chart_result is None:
        predicted_series = {}
    series_metrics: List[Dict[str, Any]] = []
    all_absolute_errors: List[float] = []
    for name, truth_values in truth_series.items():
        predicted_values = predicted_series.get(name)
        if predicted_values is None:
            series_metrics.append(
                {
                    "name": name,
                    "point_count": 0,
                    "truth_point_count": len(truth_values),
                    "missing": True,
                    "mae": None,
                    "max_abs_error": None,
                }
            )
            continue
        count = min(len(predicted_values), len(truth_values))
        errors = [abs(predicted_values[index] - truth_values[index]) for index in range(count)]
        all_absolute_errors.extend(errors)
        series_metrics.append(
            {
                "name": name,
                "point_count": count,
                "truth_point_count": len(truth_values),
                "missing": count != len(truth_values),
                "mae": rounded(sum(errors) / count) if errors else None,
                "max_abs_error": rounded(max(errors)) if errors else None,
            }
        )
    mean_abs_error = (
        rounded(sum(all_absolute_errors) / len(all_absolute_errors))
        if all_absolute_errors
        else None
    )
    max_abs_error = rounded(max(all_absolute_errors)) if all_absolute_errors else None
    return {
        "series": series_metrics,
        "mean_abs_error": mean_abs_error,
        "max_abs_error": max_abs_error,
        "compared_points": len(all_absolute_errors),
    }


def validate_shard(
    shard: Dict[str, Any],
    detection_result: Optional[Dict[str, Any]],
    table_result: Optional[Dict[str, Any]],
    chart_result: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    truth = shard["truth"]
    table_truth = truth["table"]
    chart_truth = truth["chart"]
    table_iou = 0.0
    chart_iou = 0.0
    if detection_result:
        if detection_result.get("table", {}).get("region"):
            table_iou = rounded(iou(detection_result["table"]["region"], table_truth["region"]))
        if detection_result.get("chart", {}).get("region"):
            chart_iou = rounded(iou(detection_result["chart"]["region"], chart_truth["region"]))
    cell_accuracy = _cell_accuracy(table_truth, table_result)
    header_accuracy = None
    if table_result:
        header_accuracy = 1.0 if int(table_result.get("header_rows", -1)) == int(table_truth["header_rows"]) else 0.0
    chart_metrics = _chart_metrics(chart_truth, chart_result)
    return {
        "shard_id": shard["id"],
        "shard_index": int(shard["index"]),
        "table_region_iou": table_iou,
        "chart_region_iou": chart_iou,
        "cell_row_col_accuracy": cell_accuracy,
        "header_accuracy": header_accuracy,
        "chart": chart_metrics,
    }
