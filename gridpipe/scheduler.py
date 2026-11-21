"""Budget-aware deterministic shard admission scheduling."""

from __future__ import annotations

import math
from typing import Any, Dict, List, Sequence

from .errors import E_BUDGET, PipelineError


def _estimated_cost(shard: Dict[str, Any], budget: Dict[str, Any]) -> int:
    width = int(shard["width"])
    height = int(shard["height"])
    if width <= 0 or height <= 0:
        raise PipelineError(
            E_BUDGET,
            "shard dimensions must be positive before admission",
            shard_id=str(shard.get("id", "")),
            location=[0, 0, width, height],
            diagnostics={"reason": "invalid_dimensions"},
        )
    tier = str(shard["size_tier"])
    factors = budget.get("tier_factors", {})
    if tier not in factors:
        raise PipelineError(
            E_BUDGET,
            f"unknown shard size tier: {tier}",
            shard_id=str(shard.get("id", "")),
            diagnostics={"tier": tier, "known_tiers": sorted(factors)},
        )
    actual = width * height
    factor = float(factors[tier])
    if factor < 1.0:
        raise PipelineError(
            E_BUDGET,
            "tier factor must be at least 1.0 so reservations cover actual pixels",
            shard_id=str(shard.get("id", "")),
            diagnostics={"tier": tier, "factor": factor},
        )
    estimated = int(math.ceil(actual * factor))
    declared = shard.get("pixel_count")
    if declared is not None and int(declared) != actual:
        raise PipelineError(
            E_BUDGET,
            "manifest pixel_count does not equal width*height",
            shard_id=str(shard.get("id", "")),
            diagnostics={"declared": declared, "actual": actual},
        )
    return max(actual, estimated)


def build_schedule(shards: Sequence[Dict[str, Any]], budget: Dict[str, Any]) -> Dict[str, Any]:
    total_budget = int(budget["total_pixels"])
    worker_quota = int(budget["worker_pixel_quota"])
    max_workers = int(budget["max_workers"])
    if total_budget < 0 or worker_quota < 1 or max_workers < 1:
        raise PipelineError(
            E_BUDGET,
            "budget constraints must be non-negative with positive worker limits",
            diagnostics={
                "total_pixels": total_budget,
                "worker_pixel_quota": worker_quota,
                "max_workers": max_workers,
            },
        )
    acceptance_order = str(budget.get("acceptance_order", "index"))
    indexed = list(enumerate(shards))
    if acceptance_order == "index":
        ordered = sorted(indexed, key=lambda item: (int(item[1]["index"]), item[0]))
    elif acceptance_order == "smallest_first":
        ordered = sorted(
            indexed,
            key=lambda item: (int(item[1]["width"]) * int(item[1]["height"]), int(item[1]["index"])),
        )
    else:
        raise PipelineError(
            E_BUDGET,
            f"unknown acceptance order: {acceptance_order}",
            diagnostics={"acceptance_order": acceptance_order},
        )

    reserved = 0
    actual = 0
    accepted: List[Dict[str, Any]] = []
    unprocessed: List[Dict[str, Any]] = []
    for original_position, shard in ordered:
        cost = _estimated_cost(shard, budget)
        width = int(shard["width"])
        height = int(shard["height"])
        if cost > worker_quota:
            unprocessed.append(
                {
                    "shard_id": shard["id"],
                    "shard_index": int(shard["index"]),
                    "estimated_pixels": cost,
                    "actual_pixels": width * height,
                    "reason": "worker_pixel_quota_exceeded",
                    "detail": f"estimated {cost} > per-worker quota {worker_quota}",
                }
            )
            continue
        if reserved + cost > total_budget:
            unprocessed.append(
                {
                    "shard_id": shard["id"],
                    "shard_index": int(shard["index"]),
                    "estimated_pixels": cost,
                    "actual_pixels": width * height,
                    "reason": "total_pixel_budget_exceeded",
                    "detail": (
                        f"remaining {max(0, total_budget - reserved)} < estimated {cost}"
                    ),
                }
            )
            continue
        reserved += cost
        actual += width * height
        accepted.append(
            {
                "shard_id": shard["id"],
                "shard_index": int(shard["index"]),
                "manifest_position": int(original_position),
                "size_tier": shard["size_tier"],
                "width": width,
                "height": height,
                "actual_pixels": width * height,
                "estimated_pixels": cost,
                "cumulative_reserved_pixels": reserved,
            }
        )

    accepted.sort(key=lambda item: (item["shard_index"], item["manifest_position"]))
    unprocessed.sort(key=lambda item: (item["shard_index"], item["shard_id"]))
    return {
        "acceptance_order": acceptance_order,
        "total_pixel_budget": total_budget,
        "worker_pixel_quota": worker_quota,
        "max_workers": max_workers,
        "effective_workers": min(max_workers, max(1, len(accepted))),
        "reserved_pixels": reserved,
        "actual_pixels": actual,
        "within_total_budget": reserved <= total_budget,
        "accepted": accepted,
        "unprocessed": unprocessed,
    }
