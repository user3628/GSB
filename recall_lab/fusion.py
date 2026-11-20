from __future__ import annotations

import math
from typing import Any

from .config import LabConfig, SOURCE_ORDER
from .errors import BusinessError, ErrorCode


def _finite(value: float, *, query_id: str, item_id: str, field: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise BusinessError(
            ErrorCode.NUMERIC,
            "non-finite fusion value",
            {"query_id": query_id, "item_id": item_id, "field": field},
        )
    return number


def _normalized_scores(candidates: list[dict[str, Any]]) -> dict[str, float]:
    if not candidates:
        return {}
    values = [float(candidate["score"]) for candidate in candidates]
    low = min(values)
    high = max(values)
    span = high - low
    if span == 0.0:
        return {candidate["item_id"]: 1.0 for candidate in candidates}
    return {
        candidate["item_id"]: (float(candidate["score"]) - low) / span
        for candidate in candidates
    }


def fuse_recall(
    recall_artifact: dict[str, Any],
    *,
    config: LabConfig,
    recall_digest: str,
    input_fingerprint: str,
) -> dict[str, Any]:
    decision = recall_artifact["decision"]
    top_k = int(decision["fusion_top_k"])
    strategy = config.fusion.strategy
    dedup_rule = config.fusion.dedup_rule
    query_results: list[dict[str, Any]] = []

    for query in recall_artifact["queries"]:
        query_id = query["query_id"]
        merged: dict[str, dict[str, Any]] = {}
        route_stats: dict[str, Any] = {}
        for source in SOURCE_ORDER:
            route = query["routes"][source]
            candidates = route.get("candidates", [])
            weight = float(config.fusion.weights[source])
            normalized = _normalized_scores(candidates) if strategy == "weighted_sum" else {}
            route_stats[source] = {
                "status": route.get("status"),
                "weight": weight,
                "emitted": len(candidates),
                "allocated_depth": route.get("allocated_depth", 0),
            }
            if not route.get("enabled"):
                continue
            for candidate in candidates:
                item_id = candidate["item_id"]
                rank = int(candidate["rank"])
                raw_score = _finite(
                    candidate["score"],
                    query_id=query_id,
                    item_id=item_id,
                    field="route_score",
                )
                if strategy == "weighted_sum":
                    normalized_score = _finite(
                        normalized[item_id],
                        query_id=query_id,
                        item_id=item_id,
                        field="normalized_score",
                    )
                    contribution = _finite(
                        weight * normalized_score,
                        query_id=query_id,
                        item_id=item_id,
                        field="contribution",
                    )
                elif strategy == "rrf":
                    normalized_score = None
                    contribution = _finite(
                        weight / (config.fusion.rrf_k + rank),
                        query_id=query_id,
                        item_id=item_id,
                        field="contribution",
                    )
                else:  # parse_config should make this unreachable
                    raise BusinessError(ErrorCode.INVALID_INPUT, "unsupported fusion strategy", {"value": strategy})

                entry = merged.setdefault(
                    item_id,
                    {
                        "item_id": item_id,
                        "sources": [],
                        "source_count": 0,
                        "best_rank": rank,
                    },
                )
                entry["sources"].append(
                    {
                        "source": source,
                        "rank": rank,
                        "score": raw_score,
                        "normalized_score": normalized_score,
                        "weight": weight,
                        "contribution": contribution,
                    }
                )
                entry["source_count"] += 1
                entry["best_rank"] = min(entry["best_rank"], rank)

        fused: list[dict[str, Any]] = []
        for item_id in sorted(merged):
            entry = merged[item_id]
            entry["sources"].sort(key=lambda source: SOURCE_ORDER.index(source["source"]))
            fused_score = math.fsum(source["contribution"] for source in entry["sources"])
            fused_score = _finite(fused_score, query_id=query_id, item_id=item_id, field="fused_score")
            fused.append(
                {
                    "item_id": item_id,
                    "fused_score": fused_score,
                    "source_count": entry["source_count"],
                    "best_rank": entry["best_rank"],
                    "sources": entry["sources"],
                }
            )
        fused.sort(key=lambda item: (-item["fused_score"], item["best_rank"], item["item_id"]))
        if len({item["item_id"] for item in fused}) != len(fused):
            raise BusinessError(
                ErrorCode.INTERNAL,
                "deduplication invariant failed",
                {"query_id": query_id},
            )
        for rank, item in enumerate(fused, start=1):
            item["fused_rank"] = rank
        truncated = fused[:top_k]
        merged_source_count = sum(max(0, item["source_count"] - 1) for item in fused)
        query_results.append(
            {
                "query_id": query_id,
                "strategy": strategy,
                "top_k": top_k,
                "unique_candidates": len(fused),
                "merged_duplicate_count": merged_source_count,
                "routes": route_stats,
                "fused": truncated,
            }
        )

    return {
        "format": "fusion-artifact-v1",
        "scale": recall_artifact["scale"],
        "seed": recall_artifact["seed"],
        "item_count": recall_artifact["item_count"],
        "query_count": recall_artifact["query_count"],
        "config_version": config.fingerprint,
        "input_fingerprint": input_fingerprint,
        "recall_digest": recall_digest,
        "strategy": strategy,
        "dedup_rule": dedup_rule,
        "top_k": top_k,
        "decision": decision,
        "fusion_config": config.fusion_payload,
        "queries": query_results,
    }
