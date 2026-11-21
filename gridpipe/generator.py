"""Deterministic synthetic shard generator.

The images intentionally use only rectangles and a tiny bitmap font.  No OCR
model, external data set, GPU or network resource is involved.
"""

from __future__ import annotations

import copy
import math
import random
import shutil
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from PIL import Image, ImageDraw

from .canonical import atomic_write_bytes, atomic_write_json, digest_file, dump_bytes, sha256_bytes
from .config import default_configs
from .imaging import bitmap_text_width, draw_bitmap_text


LINE_COLOR = (18, 20, 22)
HEADER_FILL = (92, 120, 145)
DATA_FILLS = ((232, 239, 243), (212, 226, 235))
HEADER_TEXT = (244, 247, 250)
DATA_TEXT = (83, 96, 108)


def _normalized(rect: Sequence[int], width: int, height: int) -> List[float]:
    x0, y0, x1, y1 = rect
    return [
        round(x0 / width, 6),
        round(y0 / height, 6),
        round(x1 / width, 6),
        round(y1 / height, 6),
    ]


def _merge_map(rows: int, cols: int, merges: Iterable[Dict[str, int]]) -> Dict[Tuple[int, int], int]:
    mapping: Dict[Tuple[int, int], int] = {}
    for merge_id, merge in enumerate(merges):
        row = int(merge["row"])
        col = int(merge["col"])
        row_span = int(merge["row_span"])
        col_span = int(merge["col_span"])
        for rr in range(row, row + row_span):
            for cc in range(col, col + col_span):
                mapping[(rr, cc)] = merge_id
    return mapping


def _truth_cells(rows: int, cols: int, merges: List[Dict[str, int]]) -> List[Dict[str, int]]:
    occupied: set[Tuple[int, int]] = set()
    cells: List[Dict[str, int]] = []
    merge_lookup: Dict[Tuple[int, int], Dict[str, int]] = {}
    for merge in merges:
        for rr in range(merge["row"], merge["row"] + merge["row_span"]):
            for cc in range(merge["col"], merge["col"] + merge["col_span"]):
                merge_lookup[(rr, cc)] = merge
    for row in range(rows):
        for col in range(cols):
            if (row, col) in occupied:
                continue
            merge = merge_lookup.get((row, col))
            if merge is None:
                cell = {"row": row, "col": col, "row_span": 1, "col_span": 1}
            else:
                cell = {
                    "row": merge["row"],
                    "col": merge["col"],
                    "row_span": merge["row_span"],
                    "col_span": merge["col_span"],
                }
            cells.append(cell)
            for rr in range(cell["row"], cell["row"] + cell["row_span"]):
                for cc in range(cell["col"], cell["col"] + cell["col_span"]):
                    occupied.add((rr, cc))
    cells.sort(key=lambda item: (item["row"], item["col"]))
    return cells


def _draw_table(
    draw: ImageDraw.ImageDraw,
    region: Tuple[int, int, int, int],
    rows: int,
    cols: int,
    merges: List[Dict[str, int]],
    header_rows: int,
    rng: random.Random,
    text_samples: List[str],
) -> None:
    x0, y0, x1, y1 = region
    xs = [x0 + int(round(index * (x1 - x0) / cols)) for index in range(cols + 1)]
    ys = [y0 + int(round(index * (y1 - y0) / rows)) for index in range(rows + 1)]
    xs[0], xs[-1] = x0, x1
    ys[0], ys[-1] = y0, y1
    fill_lookup = _merge_map(rows, cols, merges)

    for row in range(rows):
        for col in range(cols):
            fill = HEADER_FILL if row < header_rows else DATA_FILLS[(row + col + rng.randrange(0, 1)) % 2]
            merge_id = fill_lookup.get((row, col))
            if merge_id is not None:
                merge = merges[merge_id]
                left = xs[merge["col"]]
                right = xs[merge["col"] + merge["col_span"]]
                top = ys[merge["row"]]
                bottom = ys[merge["row"] + merge["row_span"]]
            else:
                left, right = xs[col], xs[col + 1]
                top, bottom = ys[row], ys[row + 1]
            draw.rectangle([left + 1, top + 1, right - 2, bottom - 2], fill=fill)

    def horizontal_segment(boundary: int, col: int) -> None:
        left, right = xs[col], xs[col + 1]
        if 0 < boundary < rows:
            above = fill_lookup.get((boundary - 1, col), -1)
            below = fill_lookup.get((boundary, col), -2)
            if above == below and above >= 0:
                return
        line_y = min(max(ys[boundary], y0), y1 - 2)
        draw.rectangle([left, line_y, right - 1, line_y + 1], fill=LINE_COLOR)

    def vertical_segment(boundary: int, row: int) -> None:
        top, bottom = ys[row], ys[row + 1]
        if 0 < boundary < cols:
            left_cell = fill_lookup.get((row, boundary - 1), -1)
            right_cell = fill_lookup.get((row, boundary), -2)
            if left_cell == right_cell and left_cell >= 0:
                return
        line_x = min(max(xs[boundary], x0), x1 - 2)
        draw.rectangle([line_x, top, line_x + 1, bottom - 1], fill=LINE_COLOR)

    for boundary in range(rows + 1):
        for col in range(cols):
            horizontal_segment(boundary, col)
    for boundary in range(cols + 1):
        for row in range(rows):
            vertical_segment(boundary, row)

    # Mixed Chinese/English bitmap labels.  Text is deliberately disconnected
    # from the grid so line projection remains stable.
    for col in range(cols):
        label = text_samples[col % len(text_samples)]
        if col < 2:
            label = text_samples[col]
        tx = xs[col] + 5
        ty = ys[0] + max(4, (ys[header_rows] - ys[0] - 18) // 2)
        max_width = xs[col + 1] - xs[col] - 10
        while label and bitmap_text_width(label, 1) > max_width:
            label = label[:-1]
        draw_bitmap_text(draw, label, tx, ty, scale=1, fill=HEADER_TEXT)

    for row in range(header_rows, rows):
        for col in range(cols):
            if (row, col) not in fill_lookup or fill_lookup[(row, col)] >= 0:
                token = f"{chr(65 + (row + col) % 26)}{row + 1}{col + 1}"
                tx = xs[col] + 5
                ty = ys[row] + max(4, (ys[row + 1] - ys[row] - 9) // 2)
                draw_bitmap_text(draw, token[:4], tx, ty, scale=1, fill=DATA_TEXT)


def _map_axis(value: float, minimum: float, maximum: float, start: int, end: int) -> int:
    if maximum == minimum:
        return start
    return int(round(start + (value - minimum) * (end - start) / (maximum - minimum)))


def _draw_chart(
    draw: ImageDraw.ImageDraw,
    panel: Tuple[int, int, int, int],
    kind: str,
    x_range: List[float],
    y_range: List[float],
    x_ticks: List[float],
    y_ticks: List[float],
    series: Dict[str, Any],
    color: Tuple[int, int, int],
    rng: random.Random,
) -> None:
    px0, py0, px1, py1 = panel
    draw.rectangle([px0, py0, px1 - 1, py1 - 1], outline=LINE_COLOR, width=2)
    title = "SALES 销售" if kind == "bar" else "TREND 趋势"
    draw_bitmap_text(draw, title, px0 + 8, py0 + 8, scale=1, fill=LINE_COLOR)

    plot_x0 = px0 + 52
    plot_y0 = py0 + 34
    plot_x1 = px1 - 18
    plot_y1 = py1 - 38
    draw.rectangle([plot_x0, plot_y0, plot_x1 - 1, plot_y1 - 1], outline=LINE_COLOR, width=2)

    inner_x0 = plot_x0 + 2
    inner_x1 = plot_x1 - 2
    inner_y0 = plot_y0 + 2
    inner_y1 = plot_y1 - 2
    xmin, xmax = float(x_range[0]), float(x_range[1])
    ymin, ymax = float(y_range[0]), float(y_range[1])

    x_tick_pixels: List[int] = []
    y_tick_pixels: List[int] = []
    for value in x_ticks:
        tx = _map_axis(float(value), xmin, xmax, inner_x0, inner_x1 - 1)
        x_tick_pixels.append(tx)
        draw.rectangle([tx, plot_y1 + 1, tx, plot_y1 + 4], fill=LINE_COLOR)
        label = str(int(value)) if float(value).is_integer() else f"{float(value):g}"
        width = bitmap_text_width(label, 2)
        draw_bitmap_text(
            draw,
            label,
            tx - width // 2,
            plot_y1 + 7,
            scale=2,
            fill=LINE_COLOR,
            underline=True,
        )
    for value in y_ticks:
        ty = _map_axis(float(value), ymin, ymax, inner_y1 - 1, inner_y0)
        y_tick_pixels.append(ty)
        draw.rectangle([plot_x0 - 5, ty, plot_x0 - 2, ty], fill=LINE_COLOR)
        label = str(int(value)) if float(value).is_integer() else f"{float(value):g}"
        width = bitmap_text_width(label, 2)
        draw_bitmap_text(
            draw,
            label,
            plot_x0 - 7 - width,
            ty - 7,
            scale=2,
            fill=LINE_COLOR,
            underline=True,
        )

    x_values = [float(value) for value in series["x"]]
    y_values = [float(value) for value in series["values"]]
    points = [
        (
            _map_axis(xvalue, xmin, xmax, inner_x0, inner_x1 - 1),
            _map_axis(yvalue, ymin, ymax, inner_y1 - 1, inner_y0),
        )
        for xvalue, yvalue in zip(x_values, y_values)
    ]
    if kind == "bar":
        half_width = max(6, (inner_x1 - inner_x0) // max(4 * len(points), 8))
        zero_y = _map_axis(max(ymin, 0.0), ymin, ymax, inner_y1 - 1, inner_y0)
        for px, py in points:
            draw.rectangle(
                [px - half_width, py, px + half_width, max(py, zero_y)],
                fill=color,
            )
    else:
        for start, end in zip(points, points[1:]):
            draw.line([start, end], fill=color, width=2)
        for px, py in points:
            draw.rectangle([px - 3, py - 3, px + 3, py + 3], fill=color)

    # A small deterministic legend swatch, outside the plot rectangle.
    legend_x = px1 - 18 - bitmap_text_width(series["name"].upper(), 1) - 12
    draw.rectangle([legend_x, py0 + 9, legend_x + 5, py0 + 14], fill=color)
    draw_bitmap_text(draw, series["name"].upper(), legend_x + 8, py0 + 8, scale=1, fill=LINE_COLOR)


def _shard_specs() -> List[Dict[str, Any]]:
    return [
        {
            "width": 640,
            "height": 420,
            "size_tier": "S",
            "table_panel": (20, 24, 300, 396),
            "chart_panel": (340, 24, 620, 396),
            "table": {
                "rows": 5,
                "cols": 4,
                "header_rows": 1,
                "merges": [{"row": 1, "col": 1, "row_span": 2, "col_span": 2}],
            },
            "chart": {
                "kind": "bar",
                "x_range": [0, 5],
                "y_range": [0, 100],
                "x_ticks": [0, 1, 2, 3, 4, 5],
                "y_ticks": [0, 25, 50, 75, 100],
                "x": [1, 2, 3, 4],
                "values": [45, 72, 58, 91],
            },
        },
        {
            "width": 680,
            "height": 440,
            "size_tier": "M",
            "table_panel": (24, 28, 330, 412),
            "chart_panel": (366, 28, 656, 412),
            "table": {
                "rows": 6,
                "cols": 5,
                "header_rows": 1,
                "merges": [
                    {"row": 1, "col": 2, "row_span": 1, "col_span": 2},
                    {"row": 2, "col": 1, "row_span": 2, "col_span": 1},
                ],
            },
            "chart": {
                "kind": "line",
                "x_range": [0, 6],
                "y_range": [0, 120],
                "x_ticks": [0, 1, 2, 3, 4, 5, 6],
                "y_ticks": [0, 30, 60, 90, 120],
                "x": [1, 2, 3, 4, 5],
                "values": [18, 47, 82, 59, 104],
            },
        },
        {
            "width": 720,
            "height": 460,
            "size_tier": "M",
            "table_panel": (24, 36, 350, 424),
            "chart_panel": (382, 36, 696, 424),
            "table": {
                "rows": 5,
                "cols": 4,
                "header_rows": 1,
                "merges": [{"row": 1, "col": 0, "row_span": 2, "col_span": 2}],
            },
            "chart": {
                "kind": "bar",
                "x_range": [0, 5],
                "y_range": [0, 100],
                "x_ticks": [0, 1, 2, 3, 4, 5],
                "y_ticks": [0, 20, 40, 60, 80, 100],
                "x": [1, 2, 3, 4],
                "values": [66, 34, 88, 52],
            },
        },
        {
            "width": 760,
            "height": 480,
            "size_tier": "L",
            "table_panel": (28, 38, 370, 442),
            "chart_panel": (402, 38, 732, 442),
            "table": {
                "rows": 6,
                "cols": 5,
                "header_rows": 1,
                "merges": [
                    {"row": 1, "col": 1, "row_span": 2, "col_span": 1},
                    {"row": 3, "col": 2, "row_span": 1, "col_span": 2},
                ],
            },
            "chart": {
                "kind": "line",
                "x_range": [0, 6],
                "y_range": [0, 120],
                "x_ticks": [0, 1, 2, 3, 4, 5, 6],
                "y_ticks": [0, 20, 40, 60, 80, 100, 120],
                "x": [1, 2, 3, 4, 5],
                "values": [110, 88, 73, 41, 16],
            },
        },
        {
            "width": 700,
            "height": 500,
            "size_tier": "L",
            "table_panel": (24, 30, 340, 470),
            "chart_panel": (376, 30, 676, 470),
            "table": {
                "rows": 6,
                "cols": 4,
                "header_rows": 1,
                "merges": [
                    {"row": 1, "col": 0, "row_span": 1, "col_span": 2},
                    {"row": 2, "col": 2, "row_span": 2, "col_span": 2},
                ],
            },
            "chart": {
                "kind": "line",
                "x_range": [0, 5],
                "y_range": [0, 100],
                "x_ticks": [0, 1, 2, 3, 4, 5],
                "y_ticks": [0, 25, 50, 75, 100],
                "x": [1, 2, 3, 4],
                "values": [25, 61, 44, 96],
            },
        },
    ]


def _palette(seed: int, index: int, kind: str) -> Tuple[int, int, int]:
    candidates = {
        "bar": [(42, 96, 178), (205, 78, 57), (43, 143, 112), (130, 78, 164)],
        "line": [(224, 104, 42), (40, 132, 174), (183, 62, 108), (72, 145, 68)],
    }
    return candidates[kind][(seed + index) % len(candidates[kind])]


def _make_manifest(dest: Path, seed: int) -> Dict[str, Any]:
    configs = default_configs()
    specs = _shard_specs()
    colors = {
        "bar": (42, 96, 178),
        "line": (224, 104, 42),
    }
    shards: List[Dict[str, Any]] = []
    images_dir = dest / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    for index, spec in enumerate(specs, start=1):
        rng = random.Random(seed * 1009 + index * 7919)
        width, height = int(spec["width"]), int(spec["height"])
        image = Image.new("RGB", (width, height), (248, 249, 250))
        draw = ImageDraw.Draw(image)
        table_panel = tuple(spec["table_panel"])
        chart_panel = tuple(spec["chart_panel"])
        table_truth = copy.deepcopy(spec["table"])
        chart_truth = copy.deepcopy(spec["chart"])
        kind = chart_truth["kind"]
        color = _palette(seed, index, kind)
        jitter = [int(round(value + rng.uniform(-3.0, 3.0))) for value in chart_truth["values"]]
        jitter = [min(int(chart_truth["y_range"][1]) - 2, max(int(chart_truth["y_range"][0]) + 1, value)) for value in jitter]
        chart_truth["values"] = jitter
        series = {
            "name": f"{kind}-1",
            "type": kind,
            "x": [float(value) for value in chart_truth["x"]],
            "values": [float(value) for value in jitter],
        }
        text_samples = (
            ["销售 SALES", "季度 QTR", "区域 AREA", "2026", "增长率"]
            if index % 2
            else ["订单 ORDER", "月份 MONTH", "同比 YOY", "2026", "客户"]
        )
        _draw_table(
            draw,
            table_panel,
            int(table_truth["rows"]),
            int(table_truth["cols"]),
            table_truth["merges"],
            int(table_truth["header_rows"]),
            rng,
            text_samples,
        )
        _draw_chart(
            draw,
            chart_panel,
            kind,
            chart_truth["x_range"],
            chart_truth["y_range"],
            chart_truth["x_ticks"],
            chart_truth["y_ticks"],
            series,
            color,
            rng,
        )

        shard_id = f"shard-{index:04d}"
        image_name = f"{shard_id}.png"
        image_path = images_dir / image_name
        image.save(image_path, format="PNG", optimize=False, compress_level=6)

        table_truth_out = {
            "region": _normalized(table_panel, width, height),
            "rows": int(table_truth["rows"]),
            "cols": int(table_truth["cols"]),
            "header_rows": int(table_truth["header_rows"]),
            "merges": copy.deepcopy(table_truth["merges"]),
            "cells": _truth_cells(
                int(table_truth["rows"]), int(table_truth["cols"]), table_truth["merges"]
            ),
            "text_samples": text_samples,
        }
        chart_truth_out = {
            "region": _normalized(chart_panel, width, height),
            "kind": kind,
            "x_range": [float(value) for value in chart_truth["x_range"]],
            "y_range": [float(value) for value in chart_truth["y_range"]],
            "x_ticks": [float(value) for value in chart_truth["x_ticks"]],
            "y_ticks": [float(value) for value in chart_truth["y_ticks"]],
            "series": [series],
        }
        shards.append(
            {
                "id": shard_id,
                "index": index,
                "image": f"images/{image_name}",
                "width": width,
                "height": height,
                "pixel_count": width * height,
                "size_tier": spec["size_tier"],
                "truth": {"table": table_truth_out, "chart": chart_truth_out},
            }
        )

    manifest = {
        "schema_version": 1,
        "seed": seed,
        "generator": {"name": "gridpipe.synthetic", "version": "1.0.0"},
        "budget": copy.deepcopy(configs["budget"]),
        "configs": {
            "detection": configs["detection"],
            "table": configs["table"],
            "chart": configs["chart"],
        },
        "shards": shards,
    }
    return manifest


def generate_samples(
    destination: str | Path,
    *,
    seed: int = 20261122,
    clean: bool = True,
) -> Dict[str, Any]:
    dest = Path(destination).resolve()
    dest.mkdir(parents=True, exist_ok=True)
    images_dir = dest / "images"
    if clean and images_dir.exists():
        shutil.rmtree(images_dir)
    images_dir.mkdir(parents=True, exist_ok=True)

    manifest = _make_manifest(dest, seed)
    manifest_bytes = atomic_write_json(dest / "manifest.json", manifest)
    image_digests = []
    for shard in manifest["shards"]:
        path = dest / shard["image"]
        image_digests.append(
            {
                "shard_id": shard["id"],
                "path": shard["image"],
                "bytes": path.stat().st_size,
                "sha256": digest_file(path),
            }
        )
    summary = {
        "schema_version": 1,
        "seed": seed,
        "manifest": "manifest.json",
        "manifest_sha256": sha256_bytes(manifest_bytes),
        "images": image_digests,
        "content_sha256": sha256_bytes(
            dump_bytes({"manifest_sha256": sha256_bytes(manifest_bytes), "images": image_digests})
        ),
    }
    atomic_write_json(dest / "generation.json", summary)
    return summary
