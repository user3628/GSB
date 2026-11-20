"""阶段二：栏结构推断。

依据正文框水平起点与宽度分布（直方图 + 1-D 聚类）判定栏数与栏区间：
- 栏区间互不重叠且连续覆盖「有效版面宽度」；
- 宽度占有效宽度比例超阈值的框识别为通栏框并单独标记（column_index=-1）；
- 无正文框或任何框无法归属栏时返回 E_COLUMN_UNASSIGNED；
- 记录判定依据与参与投票的框数量。
"""

from __future__ import annotations

from typing import Any

import numpy as np

from ..config import STAGE_COLUMN
from ..errors import LayoutError
from ..hashing import digest


def run(normalized: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    cfg = config[STAGE_COLUMN]
    doc_id = normalized["doc_id"]
    body = [box for box in normalized["boxes"] if box["role"] == "body"]
    if not body:
        raise LayoutError(
            "E_COLUMN_UNASSIGNED",
            f"文档 {doc_id} 不存在可参与栏判定的正文框",
            {"doc_id": doc_id, "box_id": None, "reason": "no_body_box"},
        )

    x0_values = np.array([box["bbox_px"][0] for box in body], dtype=float)
    x1_values = np.array([box["bbox_px"][2] for box in body], dtype=float)
    widths = x1_values - x0_values
    left_edge = float(x0_values.min())
    right_edge = float(x1_values.max())
    effective_width = round(right_edge - left_edge, 6)
    if effective_width <= 0:
        raise LayoutError(
            "E_COLUMN_UNASSIGNED",
            f"文档 {doc_id} 有效版面宽度非正",
            {"doc_id": doc_id, "box_id": None, "reason": "non_positive_width"},
        )

    bins = int(cfg["histogram_bins"])
    left_hist, left_edges = np.histogram(x0_values, bins=bins)
    width_hist, width_edges = np.histogram(widths, bins=bins)
    median_width = float(np.median(widths))
    width_ratios = widths / effective_width
    spanning_ratio = float(cfg["spanning_width_ratio"])
    candidate_boxes = [box for box in body if box["width"] / effective_width < spanning_ratio]

    if not candidate_boxes or median_width / effective_width >= float(cfg["single_column_ratio"]):
        column_count = 1
        decision = "median_width_single_column" if candidate_boxes else "all_boxes_spanning"
        intervals = [{"column_index": 0, "x0": round(left_edge, 3), "x1": round(right_edge, 3)}]
    else:
        intervals, decision = _cluster_columns(
            candidate_boxes, left_edge, right_edge, effective_width, cfg
        )
        column_count = len(intervals)

    if column_count <= 0:
        raise LayoutError(
            "E_COLUMN_UNASSIGNED",
            f"文档 {doc_id} 推断栏数为零",
            {"doc_id": doc_id, "box_id": None, "reason": "zero_column_count"},
        )

    assignments = _assign_boxes(body, intervals, effective_width, spanning_ratio, doc_id)
    spanning_boxes = [item["box_id"] for item in assignments if item["spanning"]]

    return {
        "doc_id": doc_id,
        "column_count": column_count,
        "columns": intervals,
        "effective_layout": {
            "x0": round(left_edge, 3),
            "x1": round(right_edge, 3),
            "width": round(effective_width, 3),
        },
        "assignments": assignments,
        "spanning_box_ids": spanning_boxes,
        "evidence": {
            "decision": decision,
            "voting_box_count": len(candidate_boxes),
            "body_box_count": len(body),
            "median_width_ratio": round(median_width / effective_width, 6),
            "left_edge_histogram": {
                "counts": [int(v) for v in left_hist],
                "bin_edges": [round(float(v), 3) for v in left_edges],
            },
            "width_histogram": {
                "counts": [int(v) for v in width_hist],
                "bin_edges": [round(float(v), 3) for v in width_edges],
            },
            "spanning_width_ratio": spanning_ratio,
        },
    }


def _cluster_columns(
    candidate_boxes: list[dict[str, Any]],
    left_edge: float,
    right_edge: float,
    effective_width: float,
    cfg: dict[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    gap_threshold = effective_width * float(cfg["cluster_gap_ratio"])
    ordered = sorted(candidate_boxes, key=lambda box: (box["bbox_px"][0], box["box_id"]))
    clusters: list[list[dict[str, Any]]] = [[ordered[0]]]
    for box in ordered[1:]:
        prev_left = clusters[-1][-1]["bbox_px"][0]
        if box["bbox_px"][0] - prev_left > gap_threshold:
            clusters.append([box])
        else:
            clusters[-1].append(box)

    boundaries = [left_edge]
    for idx in range(len(clusters) - 1):
        cluster_max = max(item["bbox_px"][2] for item in clusters[idx])
        next_min = min(item["bbox_px"][0] for item in clusters[idx + 1])
        boundaries.append(round((cluster_max + next_min) / 2.0, 3))
    boundaries.append(right_edge)

    intervals = []
    for idx in range(len(clusters)):
        x0 = round(boundaries[idx], 3)
        x1 = round(boundaries[idx + 1], 3)
        if x1 <= x0:
            x1 = round(x0 + 0.001, 3)
        intervals.append({"column_index": idx, "x0": x0, "x1": x1})
    return intervals, "left_edge_clustering"


def _assign_boxes(
    body: list[dict[str, Any]],
    intervals: list[dict[str, Any]],
    effective_width: float,
    spanning_ratio: float,
    doc_id: str,
) -> list[dict[str, Any]]:
    assignments: list[dict[str, Any]] = []
    for box in sorted(body, key=lambda item: (item["bbox_px"][1], item["bbox_px"][0], item["box_id"])):
        x0, _, x1, _ = box["bbox_px"]
        width_ratio = box["width"] / effective_width
        if width_ratio >= spanning_ratio:
            assignments.append(
                {
                    "box_id": box["box_id"],
                    "column_index": -1,
                    "spanning": True,
                    "width_ratio": round(width_ratio, 6),
                    "overlap": None,
                }
            )
            continue
        best = None
        best_overlap = 0.0
        for interval in intervals:
            overlap = max(0.0, min(x1, interval["x1"]) - max(x0, interval["x0"]))
            if overlap > best_overlap:
                best_overlap = overlap
                best = interval
        if best is None or best_overlap <= 0.0:
            raise LayoutError(
                "E_COLUMN_UNASSIGNED",
                f"文档 {doc_id} 框 {box['box_id']} 无法归属任何栏",
                {
                    "doc_id": doc_id,
                    "box_id": box["box_id"],
                    "reason": "no_column_overlap",
                    "bbox_px": box["bbox_px"],
                },
            )
        assignments.append(
            {
                "box_id": box["box_id"],
                "column_index": best["column_index"],
                "spanning": False,
                "width_ratio": round(width_ratio, 6),
                "overlap": round(best_overlap, 3),
            }
        )
    return assignments


def input_fingerprint(normalized: dict[str, Any]) -> str:
    """阶段二输入指纹：阶段一产物中的框几何与角色（合并记录同样影响下游）。"""
    content = {
        "doc_id": normalized["doc_id"],
        "boxes": [
            {
                "box_id": box["box_id"],
                "bbox_px": box["bbox_px"],
                "width": box["width"],
                "height": box["height"],
                "role": box["role"],
                "source_box_ids": box["source_box_ids"],
            }
            for box in normalized["boxes"]
        ],
        "merge_records": normalized["merge_records"],
    }
    return digest(content)
