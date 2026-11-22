"""Budget-aware shard admission and nominal worker-lane planning."""

from __future__ import annotations

from typing import Any

from .config import budget_config


def estimate_shard_evaluations(shard: dict[str, Any]) -> int:
    """Conservative reserve: one unit per sample plus every GT x prediction pair.

    Matching only evaluates same-class valid pairs, so this estimate is always at
    least the real candidate count and can safely reserve the global budget.
    """

    estimate = 0
    for sample in shard["samples"]:
        ground_truth_count = sum(len(items) for items in sample["ground_truth"].values())
        prediction_count = len(sample["predictions"])
        estimate += 1 + ground_truth_count * prediction_count
    return estimate


def make_budget_plan(shards: list[dict[str, Any]], config: dict[str, Any]) -> dict[str, Any]:
    budget = budget_config(config)
    global_limit = int(budget["max_evaluations"])
    worker_quota = int(budget["per_worker_quota"])
    max_workers = int(budget["max_workers"])

    lanes: list[dict[str, Any]] = []
    accepted: list[dict[str, Any]] = []
    unprocessed: list[dict[str, Any]] = []
    reserved = 0

    for shard in sorted(shards, key=lambda item: (item["shard_index"], item["shard_id"])):
        estimate = estimate_shard_evaluations(shard)
        base = {
            "shard_id": shard["shard_id"],
            "shard_index": shard["shard_index"],
            "sample_count": len(shard["samples"]),
            "prediction_count": sum(len(sample["predictions"]) for sample in shard["samples"]),
            "estimated_evaluations": estimate,
        }
        remaining = global_limit - reserved
        if estimate > remaining:
            unprocessed.append(
                {
                    **base,
                    "code": "E_GLOBAL_BUDGET_EXHAUSTED",
                    "reason": "Shard estimate does not fit the remaining global evaluation budget.",
                    "remaining_global_budget": remaining,
                }
            )
            continue
        if estimate > worker_quota:
            unprocessed.append(
                {
                    **base,
                    "code": "E_PER_WORKER_QUOTA_EXCEEDED",
                    "reason": "Shard estimate exceeds one worker's lifetime quota.",
                    "per_worker_quota": worker_quota,
                }
            )
            continue

        lane_index: int | None = None
        if len(lanes) < max_workers:
            lanes.append(
                {
                    "lane": len(lanes),
                    "reserved_evaluations": 0,
                    "shard_ids": [],
                }
            )
            lane_index = len(lanes) - 1
        else:
            fitting = [
                (int(lane["reserved_evaluations"]), index)
                for index, lane in enumerate(lanes)
                if int(lane["reserved_evaluations"]) + estimate <= worker_quota
            ]
            if fitting:
                lane_index = min(fitting)[1]
            else:
                unprocessed.append(
                    {
                        **base,
                        "code": "E_WORKER_CAPACITY_EXHAUSTED",
                        "reason": "No worker lane can accept this shard without exceeding its quota.",
                        "per_worker_quota": worker_quota,
                        "max_workers": max_workers,
                    }
                )
                continue

        lane = lanes[lane_index]
        lane["reserved_evaluations"] = int(lane["reserved_evaluations"]) + estimate
        lane["shard_ids"].append(shard["shard_id"])
        reserved += estimate
        accepted.append(
            {
                **base,
                "lane": lane_index,
                "reserved_budget_after_accept": reserved,
            }
        )

    plan: dict[str, Any] = {
        "schema_version": 1,
        "budget": {
            **budget,
            "reserved_evaluations": reserved,
            "remaining_evaluations": global_limit - reserved,
        },
        "admission_order": "shard_index_ascending",
        "accepted": accepted,
        "unprocessed": unprocessed,
        "worker_lanes": lanes,
        "planned_concurrency": len(lanes),
        "execution_worker_limit": max(1, min(max_workers, len(lanes))) if lanes else 0,
    }
    return plan

