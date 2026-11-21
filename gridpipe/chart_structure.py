"""Stage 3: chart axis decoding and bar/line data extraction."""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np

from .canonical import rounded
from .errors import E_CHART_AXIS, PipelineError
from .imaging import (
    column_groups,
    connected_components,
    contiguous_groups,
    decode_bitmap_label,
)


def _parse_tick_value(text: str) -> float:
    cleaned = text.replace(" ", "")
    if not cleaned or "?" in cleaned:
        raise ValueError(f"undecodable tick label: {text!r}")
    return float(cleaned)


def _check_uniform_spacing(
    positions: Sequence[float],
    values: Sequence[float],
    *,
    pixel_tolerance: float,
    value_tolerance: float,
) -> Optional[str]:
    if len(positions) < 2:
        return "too_few_ticks"
    position_differences = np.diff(np.asarray(positions, dtype=float))
    value_differences = np.diff(np.asarray(values, dtype=float))
    if np.any(position_differences == 0) or np.any(value_differences == 0):
        return "zero_tick_spacing"
    if len(position_differences) >= 2:
        if float(position_differences.max() - position_differences.min()) > pixel_tolerance:
            return "nonuniform_pixel_spacing"
        if float(value_differences.max() - value_differences.min()) > max(
            value_tolerance, 1e-7 * max(1.0, max(abs(value) for value in values))
        ):
            return "nonuniform_value_spacing"
    return None


def _axis_error(
    shard_id: str,
    region: Sequence[int],
    reason: str,
    *,
    x_range: Optional[Sequence[float]] = None,
    y_range: Optional[Sequence[float]] = None,
    partial: Optional[Any] = None,
    diagnostics: Optional[Dict[str, Any]] = None,
) -> PipelineError:
    details: Dict[str, Any] = {
        "reason": reason,
        "x_range": list(x_range) if x_range is not None else None,
        "y_range": list(y_range) if y_range is not None else None,
    }
    if diagnostics:
        details.update(diagnostics)
    return PipelineError(
        E_CHART_AXIS,
        f"chart axis validation failed: {reason}",
        shard_id=shard_id,
        location=[int(value) for value in region],
        diagnostics=details,
        partial=partial,
    )


def _find_plot_component(
    components: Sequence[Dict[str, Any]], width: int, height: int
) -> Optional[Dict[str, Any]]:
    candidates = []
    for component in components:
        x0, y0, x1, y1 = (
            component["x0"],
            component["y0"],
            component["x1"],
            component["y1"],
        )
        if x0 <= 2 or y0 <= 2 or x1 >= width - 2 or y1 >= height - 2:
            continue
        if x1 - x0 < 0.40 * width or y1 - y0 < 0.35 * height:
            continue
        candidates.append(component)
    if not candidates:
        return None
    candidates.sort(
        key=lambda item: (
            -((item["x1"] - item["x0"]) * (item["y1"] - item["y0"])),
            item["x0"],
            item["y0"],
        )
    )
    return candidates[0]


def _window_density(mask: np.ndarray, box: int) -> np.ndarray:
    height, width = mask.shape
    if box <= 0 or height < box or width < box:
        return np.zeros((max(1, height), max(1, width)), dtype=np.int32)
    integral = np.zeros((height + 1, width + 1), dtype=np.int32)
    integral[1:, 1:] = np.cumsum(np.cumsum(mask.astype(np.int32), axis=0), axis=1)
    return (
        integral[box:, box:]
        - integral[:-box, box:]
        - integral[box:, :-box]
        + integral[:-box, :-box]
    )


def _series_geometry(
    rgb: np.ndarray,
    plot_box: Tuple[int, int, int, int],
    config: Dict[str, Any],
    shard_id: str,
) -> List[Dict[str, Any]]:
    px0, py0, px1, py1 = plot_box
    inner_x0, inner_y0 = px0 + 2, py0 + 2
    inner_x1, inner_y1 = px1 - 2, py1 - 2
    inner = rgb[inner_y0:inner_y1, inner_x0:inner_x1]
    if inner.size == 0:
        return []
    channel_delta = inner.max(axis=2).astype(np.int16) - inner.min(axis=2).astype(np.int16)
    colored = channel_delta >= int(config["color_channel_delta"])
    if not bool(colored.any()):
        return []
    colors, counts = np.unique(inner[colored].reshape(-1, 3), axis=0, return_counts=True)
    selected: List[Tuple[Tuple[int, int, int], int]] = []
    for color, count in zip(colors, counts.tolist()):
        if count >= int(config["min_series_pixels"]):
            selected.append((tuple(int(value) for value in color), int(count)))
    selected.sort(key=lambda item: (-item[1], item[0]))

    geometry: List[Dict[str, Any]] = []
    for color, count in selected:
        color_mask = np.all(inner == np.asarray(color, dtype=np.uint8), axis=2)
        components = connected_components(color_mask)
        if not components:
            continue
        plot_width = inner_x1 - inner_x0
        max_component_width = max(component["x1"] - component["x0"] for component in components)
        kind = "bar" if max_component_width < 0.5 * plot_width else "line"
        pixel_points: List[List[int]] = []
        component_boxes: List[List[int]] = []
        if kind == "bar":
            usable = [
                component
                for component in components
                if component["x1"] - component["x0"] >= 3
                and component["y1"] - component["y0"] >= 1
            ]
            usable.sort(key=lambda item: (item["x0"], item["y0"]))
            for component in usable:
                center_x = inner_x0 + (component["x0"] + component["x1"] - 1) // 2
                top_y = inner_y0 + component["y0"]
                pixel_points.append([center_x, top_y])
                component_boxes.append(
                    [
                        inner_x0 + component["x0"],
                        inner_y0 + component["y0"],
                        inner_x0 + component["x1"],
                        inner_y0 + component["y1"],
                    ]
                )
        else:
            box = int(config["marker_box_size"])
            density = _window_density(color_mask, box)
            if density.size:
                column_scores = density.max(axis=0)
                threshold = int(config["marker_density_threshold"])
                columns = np.flatnonzero(column_scores >= threshold).tolist()
                groups = contiguous_groups(columns, merge_gap=1)
                for start, end in groups:
                    local_columns = column_scores[start:end]
                    best_offset = int(np.argmax(local_columns))
                    best_x = start + best_offset
                    best_y = int(np.argmax(density[:, best_x]))
                    center_x = inner_x0 + best_x + box // 2
                    center_y = inner_y0 + best_y + box // 2
                    pixel_points.append([center_x, center_y])
                if not pixel_points:
                    component = components[0]
                    pixel_points.append(
                        [
                            inner_x0 + (component["x0"] + component["x1"] - 1) // 2,
                            inner_y0 + (component["y0"] + component["y1"] - 1) // 2,
                        ]
                    )
            pixel_points.sort(key=lambda point: (point[0], point[1]))
        geometry.append(
            {
                "type": kind,
                "color": [int(value) for value in color],
                "colored_pixels": count,
                "pixel_points": pixel_points,
                "component_boxes": component_boxes,
            }
        )
    # Stable naming after geometry extraction.  The synthetic visual legend is
    # decorative; semantic labels remain deterministic series identifiers.
    type_counts: Dict[str, int] = {"bar": 0, "line": 0}
    geometry.sort(key=lambda item: (item["type"], item["pixel_points"][0][0] if item["pixel_points"] else 0, item["color"]))
    for item in geometry:
        type_counts[item["type"]] += 1
        item["name"] = f"{item['type']}-{type_counts[item['type']]}"
    return geometry


def _decode_axes(
    mask: np.ndarray,
    plot_box: Tuple[int, int, int, int],
    panel_size: Tuple[int, int],
    config: Dict[str, Any],
    shard_id: str,
    region: Sequence[int],
    partial: Optional[Any],
) -> Dict[str, Any]:
    px0, py0, px1, py1 = plot_box
    panel_width, panel_height = panel_size
    ticks: Dict[str, Any] = {}
    x_tick_band = mask[py1 + 1 : py1 + 5, px0 + 2 : px1 - 2]
    x_tick_indices = np.flatnonzero(x_tick_band.any(axis=0)) + px0 + 2
    x_tick_groups = contiguous_groups(x_tick_indices.tolist())
    y_tick_band = mask[py0 + 2 : py1 - 2, max(0, px0 - 6) : max(0, px0 - 1)]
    y_tick_indices = np.flatnonzero(y_tick_band.any(axis=1)) + py0 + 2
    y_tick_groups = contiguous_groups(y_tick_indices.tolist())
    if len(x_tick_groups) < 2 or len(y_tick_groups) < 2:
        raise _axis_error(
            shard_id,
            region,
            "missing_axis_ticks",
            partial=partial,
            diagnostics={
                "x_tick_groups": [list(group) for group in x_tick_groups],
                "y_tick_groups": [list(group) for group in y_tick_groups],
            },
        )
    x_tick_pixels = [int((start + end) // 2) for start, end in x_tick_groups]
    y_tick_pixels = [int((start + end) // 2) for start, end in y_tick_groups]

    scale = int(config["glyph_scale"])
    x_label_band_start = py1 + int(config["axis_band"]) - 1
    x_label_band_end = max(x_label_band_start + 1, panel_height - 2)
    x_label_groups = column_groups(
        mask,
        2,
        panel_width - 2,
        x_label_band_start,
        x_label_band_end,
        int(config["label_gap"]),
    )
    x_labels: List[Tuple[int, str]] = []
    for left, right in x_label_groups:
        sub = mask[x_label_band_start:x_label_band_end, left:right]
        rows = np.flatnonzero(sub.any(axis=1))
        if not len(rows):
            continue
        top = x_label_band_start + int(rows[0])
        bottom = x_label_band_start + int(rows[-1]) + 1
        text = decode_bitmap_label(mask, (left, top, right, bottom), scale)
        x_labels.append(((left + right) // 2, text))
    if len(x_labels) != len(x_tick_pixels):
        raise _axis_error(
            shard_id,
            region,
            "x_tick_label_count_mismatch",
            partial=partial,
            diagnostics={
                "tick_count": len(x_tick_pixels),
                "label_count": len(x_labels),
                "labels": [text for _, text in x_labels],
            },
        )
    x_values: List[float] = []
    for tick, (center, text) in zip(x_tick_pixels, x_labels):
        if abs(center - tick) > max(8, (x_tick_pixels[1] - x_tick_pixels[0]) // 2):
            raise _axis_error(
                shard_id,
                region,
                "x_tick_label_alignment",
                partial=partial,
                diagnostics={"tick": tick, "label_center": center, "label": text},
            )
        try:
            x_values.append(_parse_tick_value(text))
        except ValueError as exc:
            raise _axis_error(
                shard_id,
                region,
                "undecodable_x_tick",
                partial=partial,
                diagnostics={"text": text, "error": str(exc)},
            ) from exc

    y_values: List[float] = []
    for tick in y_tick_pixels:
        top = max(0, tick - 9)
        bottom = min(panel_height, tick + 11)
        band = mask[top:bottom, 2 : max(3, px0 - 7)]
        rows, columns = np.nonzero(band)
        if not len(columns):
            raise _axis_error(
                shard_id,
                region,
                "missing_y_tick_label",
                partial=partial,
                diagnostics={"tick": tick},
            )
        label_box = (
            2 + int(columns.min()),
            top + int(rows.min()),
            2 + int(columns.max()) + 1,
            top + int(rows.max()) + 1,
        )
        text = decode_bitmap_label(mask, label_box, scale)
        try:
            y_values.append(_parse_tick_value(text))
        except ValueError as exc:
            raise _axis_error(
                shard_id,
                region,
                "undecodable_y_tick",
                partial=partial,
                diagnostics={"tick": tick, "text": text, "error": str(exc)},
            ) from exc

    x_problem = _check_uniform_spacing(
        x_tick_pixels,
        x_values,
        pixel_tolerance=float(config["pixel_spacing_tolerance"]),
        value_tolerance=float(config["value_tolerance"]),
    )
    y_problem = _check_uniform_spacing(
        y_tick_pixels,
        y_values,
        pixel_tolerance=float(config["pixel_spacing_tolerance"]),
        value_tolerance=float(config["value_tolerance"]),
    )
    if x_problem or y_problem:
        raise _axis_error(
            shard_id,
            region,
            x_problem or y_problem or "nonuniform_tick_spacing",
            x_range=[min(x_values), max(x_values)],
            y_range=[min(y_values), max(y_values)],
            partial=partial,
            diagnostics={
                "x_tick_pixels": x_tick_pixels,
                "y_tick_pixels": y_tick_pixels,
                "x_tick_values": x_values,
                "y_tick_values": y_values,
            },
        )
    x_range = [min(x_values), max(x_values)]
    y_range = [min(y_values), max(y_values)]
    if x_range[1] == x_range[0] or y_range[1] == y_range[0]:
        raise _axis_error(
            shard_id,
            region,
            "zero_axis_span",
            x_range=x_range,
            y_range=y_range,
            partial=partial,
            diagnostics={"x_tick_pixels": x_tick_pixels, "y_tick_pixels": y_tick_pixels},
        )
    ticks = {
        "x": [{"pixel": pixel, "value": rounded(value)} for pixel, value in zip(x_tick_pixels, x_values)],
        "y": [{"pixel": pixel, "value": rounded(value)} for pixel, value in zip(y_tick_pixels, y_values)],
        "x_range": [rounded(value) for value in x_range],
        "y_range": [rounded(value) for value in y_range],
    }
    return ticks


def _map_value(pixel: float, pixel0: float, pixel1: float, value0: float, value1: float) -> float:
    if pixel1 == pixel0:
        raise ZeroDivisionError("zero pixel span")
    return value0 + (pixel - pixel0) * (value1 - value0) / (pixel1 - pixel0)


def extract_chart(
    rgb: np.ndarray,
    gray: np.ndarray,
    *,
    region: Sequence[int],
    config: Dict[str, Any],
    shard_id: str,
) -> Dict[str, Any]:
    x0, y0, x1, y1 = [int(value) for value in region]
    if x1 <= x0 or y1 <= y0:
        raise PipelineError(
            E_CHART_AXIS,
            "chart region has zero area",
            shard_id=shard_id,
            location=[x0, y0, x1, y1],
            diagnostics={"reason": "zero_area_region", "x_range": [0, 0], "y_range": [0, 0]},
        )
    crop_rgb = rgb[y0:y1, x0:x1]
    crop_gray = gray[y0:y1, x0:x1]
    mask = crop_gray < 60
    panel_height, panel_width = mask.shape
    components = connected_components(mask)
    plot_component = _find_plot_component(components, panel_width, panel_height)
    if plot_component is None:
        raise PipelineError(
            E_CHART_AXIS,
            "chart plotting rectangle was not found",
            shard_id=shard_id,
            location=[x0, y0, x1, y1],
            diagnostics={"reason": "missing_plot_rectangle", "x_range": None, "y_range": None},
        )
    plot_box_local = (
        int(plot_component["x0"]),
        int(plot_component["y0"]),
        int(plot_component["x1"]),
        int(plot_component["y1"]),
    )
    plot_box_global = [
        x0 + plot_box_local[0],
        y0 + plot_box_local[1],
        x0 + plot_box_local[2],
        y0 + plot_box_local[3],
    ]
    geometry = _series_geometry(crop_rgb, plot_box_local, config, shard_id)
    partial = {
        "plot_rect_pixel": plot_box_global,
        "series": geometry,
    }
    ticks = _decode_axes(
        mask,
        plot_box_local,
        (panel_width, panel_height),
        config,
        shard_id,
        [x0, y0, x1, y1],
        partial,
    )

    x_ticks_pixels = [item["pixel"] for item in ticks["x"]]
    x_ticks_values = [item["value"] for item in ticks["x"]]
    y_ticks_pixels = [item["pixel"] for item in ticks["y"]]
    y_ticks_values = [item["value"] for item in ticks["y"]]
    x_range = ticks["x_range"]
    y_range = ticks["y_range"]
    mapped_series: List[Dict[str, Any]] = []
    invalid_points: List[Dict[str, Any]] = []
    tolerance = float(config["value_tolerance"])
    for item in geometry:
        points: List[Dict[str, Any]] = []
        for pixel_x, pixel_y in item["pixel_points"]:
            x_value = _map_value(
                pixel_x,
                x_ticks_pixels[0],
                x_ticks_pixels[-1],
                x_ticks_values[0],
                x_ticks_values[-1],
            )
            y_value = _map_value(
                pixel_y,
                y_ticks_pixels[0],
                y_ticks_pixels[-1],
                y_ticks_values[0],
                y_ticks_values[-1],
            )
            if (
                x_value < x_range[0] - tolerance
                or x_value > x_range[1] + tolerance
                or y_value < y_range[0] - tolerance
                or y_value > y_range[1] + tolerance
            ):
                invalid_points.append(
                    {
                        "series": item["name"],
                        "pixel": [pixel_x, pixel_y],
                        "value": [rounded(x_value), rounded(y_value)],
                    }
                )
                continue
            points.append(
                {
                    "x": rounded(x_value),
                    "y": rounded(y_value),
                    "pixel": [int(pixel_x), int(pixel_y)],
                }
            )
        mapped_series.append(
            {
                "name": item["name"],
                "type": item["type"],
                "color": item["color"],
                "points": points,
                "values": [point["y"] for point in points],
            }
        )
    if invalid_points:
        raise _axis_error(
            shard_id,
            [x0, y0, x1, y1],
            "data_point_outside_axis_range",
            x_range=x_range,
            y_range=y_range,
            partial={
                "plot_rect_pixel": plot_box_global,
                "valid_series": [series for series in mapped_series if series["points"]],
                "invalid_points": invalid_points,
            },
            diagnostics={"invalid_points": invalid_points, "ticks": ticks},
        )
    if not mapped_series or any(not series["points"] for series in mapped_series):
        raise _axis_error(
            shard_id,
            [x0, y0, x1, y1],
            "no_series_points_extracted",
            x_range=x_range,
            y_range=y_range,
            partial={"plot_rect_pixel": plot_box_global, "series": mapped_series},
        )
    return {
        "shard_id": shard_id,
        "region_pixel": [x0, y0, x1, y1],
        "plot_rect_pixel": plot_box_global,
        "x_range": x_range,
        "y_range": y_range,
        "ticks": ticks,
        "series": mapped_series,
        "evidence": {
            "method": "bitmap_tick_decode+linear_pixel_mapping+color_components",
            "plot_component_pixels": int(plot_component["area"]),
            "series_count": len(mapped_series),
        },
    }
