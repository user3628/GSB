"""阶段一：版面元素规范化与框合并。

- 归一化坐标 -> 像素坐标（固定小数位，宽高由像素坐标直接推导）
- 越界坐标 / 零面积框 -> E_BOX_INVALID（携带 doc_id 与 box_id）
- 依据页边距离与字号档位识别页眉、页脚并单独归类
- 对水平/垂直间隙小于阈值的相邻框按阅读方向做确定性合并
"""

from __future__ import annotations

from typing import Any

from ..config import STAGE_NORMALIZE
from ..errors import LayoutError
from ..hashing import digest

_FONT_RANK = {"xsmall": 0, "small": 1, "body": 2, "large": 3, "xlarge": 4}


def validate_raw_document(document: dict[str, Any]) -> None:
    doc_id = document.get("doc_id", "<unknown>")
    page_w = document["page_width"]
    page_h = document["page_height"]
    if page_w <= 0 or page_h <= 0:
        raise LayoutError(
            "E_BOX_INVALID",
            f"文档 {doc_id} 页面尺寸非法",
            {"doc_id": doc_id, "box_id": None},
        )
    seen: set[str] = set()
    for box in document["boxes"]:
        box_id = box.get("box_id")
        if box_id in seen:
            raise LayoutError(
                "E_BOX_INVALID",
                f"文档 {doc_id} 框 {box_id} 标识重复",
                {"doc_id": doc_id, "box_id": box_id},
            )
        seen.add(box_id)
        x0, y0, x1, y1 = box["bbox_norm"]
        for name, value in zip(("x0", "y0", "x1", "y1"), (x0, y0, x1, y1)):
            if not isinstance(value, (int, float)) or not 0.0 <= value <= 1.0:
                raise LayoutError(
                    "E_BOX_INVALID",
                    f"文档 {doc_id} 框 {box_id} 坐标 {name}={value} 越界",
                    {"doc_id": doc_id, "box_id": box_id, "field": name, "value": value},
                )
        if x1 <= x0 or y1 <= y0:
            raise LayoutError(
                "E_BOX_INVALID",
                f"文档 {doc_id} 框 {box_id} 为零面积框",
                {"doc_id": doc_id, "box_id": box_id, "bbox_norm": [x0, y0, x1, y1]},
            )


def run(document: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    validate_raw_document(document)
    cfg = config[STAGE_NORMALIZE]
    decimals = cfg["coord_decimals"]
    doc_id = document["doc_id"]
    page_w = float(document["page_width"])
    page_h = float(document["page_height"])

    pixel_boxes: list[dict[str, Any]] = []
    for box in document["boxes"]:
        x0, y0, x1, y1 = box["bbox_norm"]
        px = [
            round(x0 * page_w, decimals),
            round(y0 * page_h, decimals),
            round(x1 * page_w, decimals),
            round(y1 * page_h, decimals),
        ]
        role, edge_info = _classify_role(box, cfg)
        pixel_boxes.append(
            {
                "box_id": box["box_id"],
                "bbox_norm": [x0, y0, x1, y1],
                "bbox_px": px,
                "width": round(px[2] - px[0], decimals),
                "height": round(px[3] - px[1], decimals),
                "font_tier": box["font_tier"],
                "text": box["text"],
                "lang": box["lang"],
                "role": role,
                "edge_distance_norm": edge_info,
                "merged": False,
                "source_box_ids": [box["box_id"]],
            }
        )

    merge_records, merged_boxes, absorbed = _merge_body_boxes(pixel_boxes, cfg, decimals)

    output_boxes = [box for box in pixel_boxes if box["box_id"] not in absorbed]
    output_boxes.extend(merged_boxes)
    output_boxes.sort(key=lambda box: (box["bbox_px"][1], box["bbox_px"][0], box["box_id"]))

    role_counts = {"header": 0, "footer": 0, "body": 0}
    for box in output_boxes:
        role_counts[box["role"]] += 1

    return {
        "doc_id": doc_id,
        "page_width": page_w,
        "page_height": page_h,
        "boxes": output_boxes,
        "merge_records": merge_records,
        "stats": {
            "input_box_count": len(pixel_boxes),
            "output_box_count": len(output_boxes),
            "merged_group_count": len(merge_records),
            "role_counts": role_counts,
        },
    }


def _classify_role(box: dict[str, Any], cfg: dict[str, Any]) -> tuple[str, dict[str, float]]:
    x0, y0, x1, y1 = box["bbox_norm"]
    top_gap = round(y0, 4)
    bottom_gap = round(1.0 - y1, 4)
    margin = cfg["header_footer_margin"]
    small_tiers = set(cfg["header_footer_font_tiers"])
    edge = {"top": top_gap, "bottom": bottom_gap, "left": round(x0, 4), "right": round(1.0 - x1, 4)}
    if box["font_tier"] in small_tiers:
        if top_gap <= margin and top_gap <= bottom_gap:
            return "header", edge
        if bottom_gap <= margin:
            return "footer", edge
    return "body", edge


def _merge_body_boxes(
    pixel_boxes: list[dict[str, Any]], cfg: dict[str, Any], decimals: int
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], set[str]]:
    gap_threshold = float(cfg["merge_gap_px"])
    overlap_ratio = float(cfg["merge_overlap_ratio"])
    body = [box for box in pixel_boxes if box["role"] == "body"]
    body.sort(key=lambda box: (box["bbox_px"][1], box["bbox_px"][0], box["box_id"]))
    parent = list(range(len(body)))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)

    pair_notes: dict[tuple[int, int], dict[str, Any]] = {}
    for i in range(len(body)):
        for j in range(i + 1, len(body)):
            note = _merge_note(body[i], body[j], gap_threshold, overlap_ratio)
            if note is not None:
                union(i, j)
                pair_notes[(i, j)] = note

    groups: dict[int, list[int]] = {}
    for idx in range(len(body)):
        groups.setdefault(find(idx), []).append(idx)

    merge_records: list[dict[str, Any]] = []
    merged_boxes: list[dict[str, Any]] = []
    absorbed: set[str] = set()
    ordered_roots = sorted(
        groups,
        key=lambda root: (
            min(body[idx]["bbox_px"][1] for idx in groups[root]),
            min(body[idx]["bbox_px"][0] for idx in groups[root]),
            min(body[idx]["box_id"] for idx in groups[root]),
        ),
    )
    for seq, root in enumerate(ordered_roots, start=1):
        members = sorted(groups[root], key=lambda idx: (body[idx]["bbox_px"][1], body[idx]["bbox_px"][0]))
        if len(members) == 1:
            continue
        merge_id = f"M{seq:04d}"
        member_boxes = [body[idx] for idx in members]
        member_ids = [box["box_id"] for box in member_boxes]
        x0 = min(box["bbox_px"][0] for box in member_boxes)
        y0 = min(box["bbox_px"][1] for box in member_boxes)
        x1 = max(box["bbox_px"][2] for box in member_boxes)
        y1 = max(box["bbox_px"][3] for box in member_boxes)
        gaps = [
            note
            for (i, j), note in pair_notes.items()
            if i in members and j in members
        ]
        langs = {box["lang"] for box in member_boxes}
        tiers = sorted(member_boxes, key=lambda box: _FONT_RANK.get(box["font_tier"], 99))
        merged_norm = [
            round(min(box["bbox_norm"][0] for box in member_boxes), 4),
            round(min(box["bbox_norm"][1] for box in member_boxes), 4),
            round(max(box["bbox_norm"][2] for box in member_boxes), 4),
            round(max(box["bbox_norm"][3] for box in member_boxes), 4),
        ]
        merged = {
            "box_id": merge_id,
            "bbox_norm": merged_norm,
            "bbox_px": [
                round(x0, decimals),
                round(y0, decimals),
                round(x1, decimals),
                round(y1, decimals),
            ],
            "width": round(x1 - x0, decimals),
            "height": round(y1 - y0, decimals),
            "font_tier": tiers[0]["font_tier"] if len({b["font_tier"] for b in member_boxes}) == 1 else "body",
            "text": " ".join(box["text"] for box in member_boxes),
            "lang": next(iter(langs)) if len(langs) == 1 else "mixed",
            "role": "body",
            "edge_distance_norm": {
                "top": merged_norm[1],
                "bottom": round(1.0 - merged_norm[3], 4),
                "left": merged_norm[0],
                "right": round(1.0 - merged_norm[2], 4),
            },
            "merged": True,
            "source_box_ids": member_ids,
        }
        merge_records.append(
            {
                "merge_id": merge_id,
                "member_box_ids": member_ids,
                "direction": _group_direction(member_boxes),
                "pair_gaps": gaps,
                "result_bbox_px": merged["bbox_px"],
            }
        )
        merged_boxes.append(merged)
        absorbed.update(member_ids)
    return merge_records, merged_boxes, absorbed



def _group_direction(member_boxes: list[dict[str, Any]]) -> str:
    xs = {box["bbox_px"][0] for box in member_boxes}
    ys = {box["bbox_px"][1] for box in member_boxes}
    if len(xs) >= len(ys):
        return "horizontal"
    return "vertical"


def _merge_note(
    a: dict[str, Any], b: dict[str, Any], gap_threshold: float, overlap_ratio: float
) -> dict[str, Any] | None:
    ax0, ay0, ax1, ay1 = a["bbox_px"]
    bx0, by0, bx1, by1 = b["bbox_px"]
    # 水平相邻（同一阅读行，从左到右）
    v_overlap = max(0.0, min(ay1, by1) - max(ay0, by0))
    h_gap = max(0.0, max(ax0, bx0) - min(ax1, bx1))
    if (
        h_gap < gap_threshold
        and v_overlap >= overlap_ratio * min(ay1 - ay0, by1 - by0)
        and max(ax0, bx0) >= min(ax1, bx1) - 1e-9
    ):
        return {"direction": "horizontal", "gap_px": round(h_gap, 3),
                "left": a["box_id"] if ax0 <= bx0 else b["box_id"],
                "right": b["box_id"] if ax0 <= bx0 else a["box_id"]}
    # 垂直相邻（同一栏，自上而下）
    h_overlap = max(0.0, min(ax1, bx1) - max(ax0, bx0))
    v_gap = max(0.0, max(ay0, by0) - min(ay1, by1))
    if (
        v_gap < gap_threshold
        and h_overlap >= overlap_ratio * min(ax1 - ax0, bx1 - bx0)
        and max(ay0, by0) >= min(ay1, by1) - 1e-9
    ):
        return {"direction": "vertical", "gap_px": round(v_gap, 3),
                "top": a["box_id"] if ay0 <= by0 else b["box_id"],
                "bottom": b["box_id"] if ay0 <= by0 else a["box_id"]}
    return None


def input_fingerprint(document: dict[str, Any]) -> str:
    """阶段一输入指纹：仅页面尺寸与原始框内容（不含参考顺序）。"""
    content = {
        "doc_id": document["doc_id"],
        "page_width": document["page_width"],
        "page_height": document["page_height"],
        "boxes": document["boxes"],
    }
    return digest(content)
