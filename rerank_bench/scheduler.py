"""Budget-constrained shard admission and concurrency scheduling.

Admission is decided up-front from actual candidate counts so the
invariant "cumulative evaluations never exceed the global budget" holds
by construction at every moment of the run.
"""

from __future__ import annotations

from .errors import E_BUDGET_EXCEEDED, E_QUOTA_EXCEEDED


def estimate_cost(candidate_count: int) -> int:
    """Estimated evaluation units for a shard: one per candidate."""
    return candidate_count


def plan_schedule(shard_infos: list, config) -> dict:
    """Decide admitted shards and concurrency under three constraints.

    shard_infos: [{"shard_id", "shard_index", "candidates"}] sorted by
    shard_index. Returns a deterministic plan.
    """
    admitted = []
    rejected = []
    spent = 0
    for info in sorted(shard_infos, key=lambda x: x["shard_index"]):
        cost = estimate_cost(info["candidates"])
        entry = {
            "shard_id": info["shard_id"],
            "shard_index": info["shard_index"],
            "estimated_cost": cost,
        }
        if cost > config.per_worker_quota:
            rejected.append({
                **entry,
                "code": E_QUOTA_EXCEEDED,
                "reason": f"estimated cost {cost} exceeds per-worker quota "
                          f"{config.per_worker_quota}",
            })
            continue
        if spent + cost > config.global_eval_budget:
            rejected.append({
                **entry,
                "code": E_BUDGET_EXCEEDED,
                "reason": f"admitting would raise cumulative evaluations to "
                          f"{spent + cost}, above global budget "
                          f"{config.global_eval_budget}",
            })
            continue
        spent += cost
        admitted.append(entry)
    concurrency = min(config.max_workers, max(1, len(admitted)))
    return {
        "admitted": admitted,
        "rejected": rejected,
        "concurrency": concurrency,
        "reserved_evaluations": spent,
        "global_eval_budget": config.global_eval_budget,
        "per_worker_quota": config.per_worker_quota,
        "max_workers": config.max_workers,
    }
