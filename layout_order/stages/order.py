"""阶段三：块序列化与阅读顺序生成。

规则：
- 页眉块置顶、页脚块置底；正文块按「栏内自上而下、栏间自左向右」排序；
- 通栏（跨栏）块与并列块通过有向偏好图 + Kahn 拓扑排序按可配置优先级裁决；
- 同一文档内框不得重复；空块或裁决成环返回 E_ORDER_INVALID；
- 顺序置信度由相邻块的垂直间隙与栏归属显式计算。
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from ..config import STAGE_ORDER
from ..errors import LayoutError
from ..hashing import digest


def run(
    normalized: dict[str, Any],
    column_result: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    doc_id = normalized["doc_id"]
    cfg = config[STAGE_ORDER]
    boxes = normalized["boxes"]
    if not boxes:
        raise LayoutError(
            "E_ORDER_INVALID",
            f"文档 {doc_id} 不存在任何块",
            {"doc_id": doc_id, "block_position": None, "reason": "empty_document"},
        )

    assignment = {item["box_id"]: item for item in column_result["assignments"]}
    blocks: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for position, box in enumerate(
        sorted(boxes, key=lambda item: (item["bbox_px"][1], item["bbox_px"][0], item["box_id"]))
    ):
        if not box["source_box_ids"]:
            raise LayoutError(
                "E_ORDER_INVALID",
                f"文档 {doc_id} 位置 {position} 为空块",
                {"doc_id": doc_id, "block_position": position, "box_id": box["box_id"],
                 "reason": "empty_block"},
            )
        for source_id in box["source_box_ids"]:
            if source_id in seen_ids:
                raise LayoutError(
                    "E_ORDER_INVALID",
                    f"文档 {doc_id} 框 {source_id} 在块序列中重复",
                    {"doc_id": doc_id, "block_position": position, "box_id": source_id,
                     "reason": "duplicate_box"},
                )
            seen_ids.add(source_id)
        column_index = assignment[box["box_id"]]["column_index"] if box["role"] == "body" else None
        blocks.append(
            {
                "block_id": box["box_id"],
                "role": box["role"],
                "column_index": column_index,
                "spanning": box["role"] == "body" and column_index == -1,
                "bbox_px": box["bbox_px"],
                "source_box_ids": list(box["source_box_ids"]),
                "text": box["text"],
                "lang": box["lang"],
                "position_hint": position,
            }
        )

    headers = sorted((b for b in blocks if b["role"] == "header"),
                     key=lambda b: (b["bbox_px"][1], b["bbox_px"][0], b["block_id"]))
    footers = sorted((b for b in blocks if b["role"] == "footer"),
                     key=lambda b: (b["bbox_px"][1], b["bbox_px"][0], b["block_id"]))
    body_blocks = [b for b in blocks if b["role"] == "body"]

    ordered_body, arbitration_edges = _order_body(body_blocks, cfg, doc_id)
    ordered_blocks = headers + ordered_body + footers
    confidence, pair_scores = _confidence(ordered_body, cfg)

    sequence = [
        {
            "order_index": idx,
            "block_id": block["block_id"],
            "role": block["role"],
            "column_index": block["column_index"],
            "spanning": block["spanning"],
            "source_box_ids": block["source_box_ids"],
            "text": block["text"],
            "lang": block["lang"],
            "bbox_px": block["bbox_px"],
        }
        for idx, block in enumerate(ordered_blocks)
    ]
    return {
        "doc_id": doc_id,
        "block_sequence": sequence,
        "arbitration": {
            "span_priority": cfg["span_priority"],
            "edge_count": len(arbitration_edges),
            "edges": arbitration_edges,
        },
        "confidence": confidence,
        "pair_scores": pair_scores,
        "stats": {
            "block_count": len(sequence),
            "header_count": len(headers),
            "footer_count": len(footers),
            "spanning_block_count": sum(1 for item in sequence if item["spanning"]),
        },
    }


def _order_body(
    body_blocks: list[dict[str, Any]], cfg: dict[str, Any], doc_id: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ids = [block["block_id"] for block in body_blocks]
    block_by_id = {block["block_id"]: block for block in body_blocks}
    edges: set[tuple[str, str]] = set()
    mode = cfg["span_priority"]

    column_blocks = [b for b in body_blocks if not b["spanning"]]
    spanning_blocks = [b for b in body_blocks if b["spanning"]]

    # 每栏内部保持严格纵向链（无环）。栏间顺序不使用桥接边，而由拓扑
    # 就绪队列的 (column_index, top) 排序键保证，避免与通栏块裁决成环。
    columns: dict[int, list[dict[str, Any]]] = {}
    for block in column_blocks:
        columns.setdefault(block["column_index"], []).append(block)
    for column_index in columns:
        col = sorted(columns[column_index],
                     key=lambda b: (b["bbox_px"][1], b["bbox_px"][0], b["block_id"]))
        for upper, lower in zip(col, col[1:]):
            edges.add((upper["block_id"], lower["block_id"]))

    def add_edge(src: str, dst: str) -> None:
        if src != dst:
            edges.add((src, dst))

    ordered_spans = sorted(spanning_blocks,
                           key=lambda b: (b["bbox_px"][1], b["block_id"]))
    for span, nxt in zip(ordered_spans, ordered_spans[1:]):
        add_edge(span["block_id"], nxt["block_id"])

    flat_columns = {
        idx: sorted(blocks, key=lambda b: (b["bbox_px"][1], b["block_id"]))
        for idx, blocks in columns.items()
    }

    if mode == "span_first":
        # 通栏块整体前置：最后一个通栏块指向每栏第一个栏块
        if ordered_spans:
            for col in flat_columns.values():
                if col:
                    add_edge(ordered_spans[-1]["block_id"], col[0]["block_id"])
    elif mode == "column_first":
        # 通栏块整体后置：每栏最后一个栏块指向第一个通栏块
        if ordered_spans:
            for col in flat_columns.values():
                if col:
                    add_edge(col[-1]["block_id"], ordered_spans[0]["block_id"])
    else:
        # flow：通栏块按纵向位置插入；仅连接每列紧邻其上下的栏块，
        # 同高时通栏优先（>= 归入 after），保证确定性且无环。
        for span in ordered_spans:
            span_top = span["bbox_px"][1]
            for col in flat_columns.values():
                before = [b["block_id"] for b in col if b["bbox_px"][1] < span_top]
                after = [b["block_id"] for b in col if b["bbox_px"][1] >= span_top]
                if before:
                    add_edge(before[-1], span["block_id"])
                if after:
                    add_edge(span["block_id"], after[0])

    ordered_ids = _topo_sort(ids, edges, doc_id, block_by_id)
    edge_list = [
        {"from": src, "to": dst, "from_position": block_by_id[src]["position_hint"],
         "to_position": block_by_id[dst]["position_hint"]}
        for src, dst in sorted(edges)
    ]
    return [block_by_id[block_id] for block_id in ordered_ids], edge_list


def _topo_sort(
    ids: list[str],
    edges: set[tuple[str, str]],
    doc_id: str,
    block_by_id: dict[str, dict[str, Any]],
) -> list[str]:
    indegree: dict[str, int] = {block_id: 0 for block_id in ids}
    adjacency: dict[str, list[str]] = defaultdict(list)
    for src, dst in edges:
        adjacency[src].append(dst)
        indegree[dst] += 1
    for key in adjacency:
        adjacency[key].sort()

    def ready_key(block_id: str) -> tuple[Any, float, float, str]:
        block = block_by_id[block_id]
        # 通栏块（column_index=-1）在并列时优先；普通块按栏序、再按纵向位置
        column_key = -1 if block["spanning"] else block["column_index"]
        return (column_key, block["bbox_px"][1], block["bbox_px"][0], block_id)

    ready = sorted((block_id for block_id, deg in indegree.items() if deg == 0), key=ready_key)
    result: list[str] = []
    while ready:
        current = ready.pop(0)
        result.append(current)
        for nxt in adjacency[current]:
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                ready.append(nxt)
                ready.sort(key=ready_key)

    if len(result) != len(ids):
        cycle_nodes = sorted(block_id for block_id, deg in indegree.items() if deg > 0)
        positions = [block_by_id[block_id]["position_hint"] for block_id in cycle_nodes]
        raise LayoutError(
            "E_ORDER_INVALID",
            f"文档 {doc_id} 阅读顺序裁决出现环状依赖",
            {"doc_id": doc_id, "block_position": positions, "cycle_blocks": cycle_nodes,
             "reason": "arbitration_cycle"},
        )
    return result


def _confidence(
    ordered_body: list[dict[str, Any]], cfg: dict[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    weights = cfg["confidence_weights"]
    gap_reference = float(cfg["gap_reference_px"])
    pair_scores: list[dict[str, Any]] = []
    if len(ordered_body) < 2:
        overall = {"vertical_gap": 1.0, "column_membership": 1.0, "overall": 1.0,
                   "pair_count": 0}
        return overall, pair_scores

    gap_values: list[float] = []
    column_values: list[float] = []
    for prev, nxt in zip(ordered_body, ordered_body[1:]):
        vertical_gap = max(0.0, nxt["bbox_px"][1] - prev["bbox_px"][3])
        gap_score = max(0.0, 1.0 - vertical_gap / gap_reference)
        if prev["spanning"] or nxt["spanning"] or prev["column_index"] == nxt["column_index"]:
            column_score = 1.0
        elif nxt["column_index"] == prev["column_index"] + 1:
            column_score = 0.8
        else:
            column_score = 0.5
        pair_scores.append(
            {
                "prev_block": prev["block_id"],
                "next_block": nxt["block_id"],
                "vertical_gap_px": round(vertical_gap, 3),
                "gap_score": round(gap_score, 6),
                "prev_column": prev["column_index"],
                "next_column": nxt["column_index"],
                "column_score": column_score,
            }
        )
        gap_values.append(gap_score)
        column_values.append(column_score)

    gap_mean = sum(gap_values) / len(gap_values)
    column_mean = sum(column_values) / len(column_values)
    overall = round(
        gap_mean * weights["vertical_gap"] + column_mean * weights["column_membership"], 6
    )
    return (
        {
            "vertical_gap": round(gap_mean, 6),
            "column_membership": round(column_mean, 6),
            "overall": overall,
            "pair_count": len(pair_scores),
            "weights": dict(weights),
        },
        pair_scores,
    )


def input_fingerprint(normalized: dict[str, Any], column_result: dict[str, Any]) -> str:
    """阶段三输入指纹：栏归属 + 块几何（页眉页脚与合并结果亦包含）。"""
    content = {
        "doc_id": normalized["doc_id"],
        "boxes": [
            {
                "box_id": box["box_id"],
                "bbox_px": box["bbox_px"],
                "role": box["role"],
                "source_box_ids": box["source_box_ids"],
            }
            for box in normalized["boxes"]
        ],
        "assignments": column_result["assignments"],
        "column_count": column_result["column_count"],
    }
    return digest(content)
