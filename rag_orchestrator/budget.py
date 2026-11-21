"""Deterministic fixed-credit admission planning."""

from __future__ import annotations

from typing import Any


def _estimate(subquery: dict[str, Any], document_shard_count: int, budget: dict[str, Any]) -> int:
    return int(
        budget["cost_per_subquery"]
        + budget["cost_per_doc_shard"] * document_shard_count
        + budget["cost_per_term"] * max(1, int(subquery.get("term_count", len(subquery.get("search_terms", [])))))
    )


def plan_budget(
    subqueries: list[dict[str, Any]],
    document_shard_count: int,
    budget_config: dict[str, Any],
) -> dict[str, Any]:
    """Admit subqueries under global and per-channel fixed-point quotas.

    Latency is intentionally not simulated here. It is measured from actual
    stage completion in the orchestrator, as required by the task.
    """

    global_quota = int(budget_config["global_retrieval_quota"])
    per_channel_quota = int(budget_config["per_channel_quota"])
    channel_count = int(budget_config["max_concurrent_channels"])
    channels = [
        {"channel": index, "quota": per_channel_quota, "used": 0, "accepted_subquery_indices": []}
        for index in range(channel_count)
    ]
    global_used = 0
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    ordered = sorted(subqueries, key=lambda item: (int(item["priority"]), int(item["index"])))
    for subquery in ordered:
        estimate = _estimate(subquery, document_shard_count, budget_config)
        item = {
            "subquery_index": int(subquery["index"]),
            "priority": int(subquery["priority"]),
            "text": subquery["text"],
            "estimate_credits": estimate,
        }
        if estimate > per_channel_quota:
            rejected.append({**item, "reason": "per_channel_quota", "channel": None})
            continue
        if global_used + estimate > global_quota:
            rejected.append({**item, "reason": "global_quota", "channel": None})
            continue

        fitting = [channel for channel in channels if channel["used"] + estimate <= per_channel_quota]
        if not fitting:
            rejected.append({**item, "reason": "channel_capacity", "channel": None})
            continue
        channel = min(fitting, key=lambda value: (value["used"] + estimate, value["channel"]))
        channel["used"] += estimate
        channel["accepted_subquery_indices"].append(item["subquery_index"])
        global_used += estimate
        accepted.append({**item, "channel": channel["channel"]})

    estimated_latency_ms = round(
        global_used * float(budget_config["latency_estimate_ms_per_credit"]), 6
    )
    return {
        "model": "fixed-credit-v1",
        "global_quota": global_quota,
        "global_used": global_used,
        "global_remaining": global_quota - global_used,
        "per_channel_quota": per_channel_quota,
        "max_concurrent_channels": channel_count,
        "latency_budget_ms": float(budget_config["latency_budget_ms"]),
        "estimated_latency_ms": estimated_latency_ms,
        "document_shard_count": document_shard_count,
        "accepted": accepted,
        "rejected": rejected,
        "accepted_count": len(accepted),
        "rejected_count": len(rejected),
        "degraded": bool(rejected),
        "channels": channels,
    }
