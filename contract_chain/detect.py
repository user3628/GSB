"""Stage 1: deterministic candidate-box generation and category-locked IoU matching."""
from __future__ import annotations

from fractions import Fraction

import numpy as np

from .codec import quantize_fraction, stable_sorted
from .config import DetectConfig


def _pixel_box(xywh, width, height):
    x, y, w, h = xywh
    return (x * width, y * height, (x + w) * width, (y + h) * height)


def iou_pixels(box_a, box_b, width, height) -> Fraction:
    """IoU computed from normalized xywh converted to pixel-area space."""
    ax1, ay1, ax2, ay2 = _pixel_box(box_a, width, height)
    bx1, by1, bx2, by2 = _pixel_box(box_b, width, height)
    inter_w = max(0.0, min(ax2, bx2) - max(ax1, bx1))
    inter_h = max(0.0, min(ay2, by2) - max(ay1, by1))
    inter = inter_w * inter_h
    area_a = max(0.0, ax2 - ax1) * max(0.0, ay2 - ay1)
    area_b = max(0.0, bx2 - bx1) * max(0.0, by2 - by1)
    union = area_a + area_b - inter
    if union <= 0.0:
        return Fraction(0)
    return Fraction(str(inter / union))


def _clip_box(xywh):
    x, y, w, h = (min(0.98, max(0.0, float(v))) for v in xywh)
    w = min(w, 1.0 - x - 0.005)
    h = min(h, 1.0 - y - 0.005)
    return [round(x, 6), round(y, 6), round(max(0.01, w), 6), round(max(0.01, h), 6)]


def generate_candidates(sample: dict, config: DetectConfig, seed: int) -> list:
    """Generate perturbation candidates around GT regions plus random background boxes."""
    candidates = []
    for page_index, page in enumerate(sample["pages"]):
        rng = np.random.default_rng(seed + 7919 * page_index)
        width, height = page["width"], page["height"]
        for region_index, region in enumerate(page["regions"]):
            local_rng = np.random.default_rng(
                seed + 104729 * page_index + 31 * region_index
            )
            for cand_index in range(config.candidates_per_region):
                delta = local_rng.normal(0.0, config.sigma, size=4)
                xywh = _clip_box(np.array(region["xywh"], dtype=float) + delta)
                candidates.append(
                    {
                        "candidate_id": "%s-cand-%d-%d"
                        % (page["page_id"], region_index, cand_index),
                        "page_id": page["page_id"],
                        "category": region["category"],
                        "xywh": xywh,
                        "origin": "perturbation",
                        "source_region_id": region["region_id"],
                        "width": width,
                        "height": height,
                    }
                )
        categories = sorted({region["category"] for region in page["regions"]})
        for bg_index in range(config.background_candidates_per_page):
            xywh = _clip_box(
                (
                    rng.uniform(0.02, 0.8),
                    rng.uniform(0.02, 0.85),
                    rng.uniform(0.05, 0.2),
                    rng.uniform(0.03, 0.12),
                )
            )
            category = categories[int(rng.integers(0, len(categories)))]
            candidates.append(
                {
                    "candidate_id": "%s-bg-%d" % (page["page_id"], bg_index),
                    "page_id": page["page_id"],
                    "category": category,
                    "xywh": xywh,
                    "origin": "background",
                    "source_region_id": None,
                    "width": width,
                    "height": height,
                }
            )
    return stable_sorted(candidates, key=lambda item: item["candidate_id"])


def run_detection(sample: dict, config: DetectConfig, seed: int) -> dict:
    """Stage-1 product: candidate list, one-to-one IoU matches and background diagnostics."""
    candidates = generate_candidates(sample, config, seed)
    candidate_by_id = {item["candidate_id"]: item for item in candidates}
    pages_out = []
    diagnostics = []
    total_matches = 0
    total_candidates = 0

    for page in sample["pages"]:
        page_candidates = [c for c in candidates if c["page_id"] == page["page_id"]]
        total_candidates += len(page_candidates)
        pairs = []
        for region in page["regions"]:
            for candidate in page_candidates:
                if candidate["category"] != region["category"]:
                    continue
                score = iou_pixels(
                    candidate["xywh"], region["xywh"], page["width"], page["height"]
                )
                if score >= config.threshold:
                    pairs.append((score, candidate["candidate_id"], region["region_id"]))
        pairs = stable_sorted(pairs, key=lambda item: (-item[0], item[1], item[2]))

        matched_regions = set()
        matched_candidates = set()
        matches = []
        for score, candidate_id, region_id in pairs:
            if region_id in matched_regions or candidate_id in matched_candidates:
                continue
            matched_regions.add(region_id)
            matched_candidates.add(candidate_id)
            confidence = quantize_fraction(score, config.confidence_precision)
            match = {
                "match_id": "match-%s-%d" % (page["page_id"], len(matches) + 1),
                "page_id": page["page_id"],
                "region_id": region_id,
                "candidate_id": candidate_id,
                "category": candidate_by_id[candidate_id]["category"],
                "iou_scaled": confidence,
                "iou_scale": 10 ** config.confidence_precision,
                "confidence_scaled": confidence,
                "confidence_scale": 10 ** config.confidence_precision,
                "candidate_xywh": candidate_by_id[candidate_id]["xywh"],
            }
            matches.append(match)
            total_matches += 1

        background_ids = []
        for candidate in page_candidates:
            if candidate["candidate_id"] not in matched_candidates:
                background_ids.append(candidate["candidate_id"])
                diagnostics.append(
                    {
                        "type": "unmatched_candidate",
                        "candidate_id": candidate["candidate_id"],
                        "page_id": page["page_id"],
                        "category": candidate["category"],
                        "origin": candidate["origin"],
                        "marked_as": "background",
                    }
                )
        pages_out.append(
            {
                "page_id": page["page_id"],
                "width": page["width"],
                "height": page["height"],
                "candidates": [
                    {
                        key: value
                        for key, value in candidate.items()
                        if key in ("candidate_id", "category", "xywh", "origin", "source_region_id")
                    }
                    for candidate in page_candidates
                ],
                "matches": matches,
                "background_candidate_ids": background_ids,
            }
        )

    return {
        "stage": "detect",
        "seed": seed,
        "pages": pages_out,
        "diagnostics": diagnostics,
        "stats": {
            "candidate_count": total_candidates,
            "match_count": total_matches,
            "background_count": len(diagnostics),
        },
    }


def region_binding_index(sample: dict, detection: dict) -> dict:
    """Map (page_id, region_id) to GT coordinates plus the matched candidate evidence."""
    match_by_region = {}
    for page in detection["pages"]:
        for match in page["matches"]:
            match_by_region[(page["page_id"], match["region_id"])] = match
    index = {}
    for page in sample["pages"]:
        for region in page["regions"]:
            key = (page["page_id"], region["region_id"])
            match = match_by_region.get(key)
            index["%s|%s" % (page["page_id"], region["region_id"])] = {
                "page_id": page["page_id"],
                "region_id": region["region_id"],
                "category": region["category"],
                "gt_xywh": region["xywh"],
                "page_width": page["width"],
                "page_height": page["height"],
                "match": match,
                "matched": match is not None,
            }
    return index
