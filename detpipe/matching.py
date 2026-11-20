"""Per-class IoU matching core.

Coordinates arrive as normalized ``xywh`` (values in ``[0, 1]``).  They are
converted to pixel boxes using the image width/height before IoU is computed,
so areas scale by the real image size as required.  Matching is greedy: ground
truth boxes are visited in input order and each one claims the best still-free
candidate of the same class.  Ties on IoU (and score) are broken by the
candidate's original index, which makes the result fully deterministic.
"""
from __future__ import annotations

from typing import Any


def to_pixel_box(box: list[float], width: int, height: int) -> tuple[float, float, float, float]:
    x, y, w, h = box
    x1 = x * width
    y1 = y * height
    x2 = (x + w) * width
    y2 = (y + h) * height
    return x1, y1, x2, y2


def iou_normalized(box_a: list[float], box_b: list[float], width: int, height: int) -> float:
    ax1, ay1, ax2, ay2 = to_pixel_box(box_a, width, height)
    bx1, by1, bx2, by2 = to_pixel_box(box_b, width, height)

    inter_w = max(0.0, min(ax2, bx2) - max(ax1, bx1))
    inter_h = max(0.0, min(ay2, by2) - max(ay1, by1))
    inter = inter_w * inter_h
    area_a = max(0.0, ax2 - ax1) * max(0.0, ay2 - ay1)
    area_b = max(0.0, bx2 - bx1) * max(0.0, by2 - by1)
    union = area_a + area_b - inter
    if union <= 0.0:
        return 0.0
    return inter / union


def _round6(value: float) -> float:
    return round(float(value), 6)


def match_image(sample: dict[str, Any], iou_threshold: float, score_threshold: float) -> dict[str, Any]:
    width = int(sample["width"])
    height = int(sample["height"])
    gts = sample["ground_truth"]
    candidates = sample["candidates"]

    by_class_cands: dict[str, list[int]] = {}
    for idx, cand in enumerate(candidates):
        by_class_cands.setdefault(cand["category"], []).append(idx)

    used: set[int] = set()
    matches: list[dict[str, Any]] = []
    unmatched_gts: list[dict[str, Any]] = []
    # best IoU of every candidate against any GT of the same class
    best_iou: dict[int, float] = {idx: 0.0 for idx in range(len(candidates))}
    best_gt: dict[int, int] = {}

    for gt_idx, gt in enumerate(gts):
        for cand_idx in by_class_cands.get(gt["category"], []):
            value = iou_normalized(gt["bbox"], candidates[cand_idx]["bbox"], width, height)
            if value > best_iou[cand_idx]:
                best_iou[cand_idx] = value
                best_gt[cand_idx] = gt_idx

        eligible = [
            idx
            for idx in by_class_cands.get(gt["category"], [])
            if idx not in used and best_iou[idx] >= iou_threshold
        ]
        if not eligible:
            unmatched_gts.append(
                {"gt_index": gt_idx, "category": gt["category"], "bbox": gt["bbox"]}
            )
            continue
        # Highest IoU first, then highest score, then smallest candidate index.
        chosen = min(
            eligible,
            key=lambda idx: (
                -best_iou[idx],
                -float(candidates[idx]["score"]),
                idx,
            ),
        )
        used.add(chosen)
        cand = candidates[chosen]
        matches.append(
            {
                "gt_index": gt_idx,
                "candidate_index": chosen,
                "candidate_id": cand.get("candidate_id", f"c{chosen}"),
                "category": gt["category"],
                "source": cand["source"],
                "score": _round6(cand["score"]),
                "iou": _round6(best_iou[chosen]),
            }
        )

    duplicates: list[dict[str, Any]] = []
    background: list[dict[str, Any]] = []
    low_confidence: list[dict[str, Any]] = []
    for idx, cand in enumerate(candidates):
        if idx in used:
            continue
        entry = {
            "candidate_index": idx,
            "candidate_id": cand.get("candidate_id", f"c{idx}"),
            "category": cand["category"],
            "source": cand["source"],
            "score": _round6(cand["score"]),
            "iou": _round6(best_iou[idx]),
        }
        if best_iou[idx] >= iou_threshold:
            entry["duplicates_gt_index"] = best_gt.get(idx)
            duplicates.append(entry)
        elif float(cand["score"]) >= score_threshold:
            background.append(entry)
        else:
            low_confidence.append(entry)

    return {
        "image_id": sample["image_id"],
        "width": width,
        "height": height,
        "num_ground_truth": len(gts),
        "num_candidates": len(candidates),
        "matches": matches,
        "unmatched_ground_truth": unmatched_gts,
        "duplicate_candidates": duplicates,
        "background_candidates": background,
        "low_confidence_candidates": low_confidence,
    }
