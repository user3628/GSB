from __future__ import annotations

from typing import Any

from .config import LabConfig, SOURCE_ORDER
from .data import Query
from .errors import BusinessError, ErrorCode
from .util import quantize


def _round_ratio(numerator: int | float, denominator: int | float) -> float:
    if denominator == 0:
        return 0.0
    value = quantize(float(numerator) / float(denominator), 10)
    assert value is not None
    return value


def _average(values: list[float | int]) -> float:
    if not values:
        return 0.0
    value = quantize(sum(float(item) for item in values) / len(values), 10)
    assert value is not None
    return value


def build_summary(
    recall_artifact: dict[str, Any],
    fusion_artifact: dict[str, Any],
    *,
    queries: tuple[Query, ...],
    config: LabConfig,
    input_fingerprint: str,
    fusion_digest: str,
) -> dict[str, Any]:
    query_by_id = {query.query_id: query for query in queries}
    route_aggregate = {
        source: {
            "enabled": fusion_artifact["decision"]["routes"][source]["enabled"],
            "effective_depth": fusion_artifact["decision"]["routes"][source]["effective_depth"],
            "reason": fusion_artifact["decision"]["routes"][source]["reason"],
            "reference_hits": 0,
            "queries_with_hit": 0,
            "candidate_emissions": 0,
        }
        for source in SOURCE_ORDER
    }
    unique_recalled_items: set[str] = set()
    unique_fused_items: set[str] = set()
    precision_values: list[float] = []
    recall_values: list[float] = []
    rank_deviations: list[int] = []
    judged_queries = 0
    queries_with_fused_hit = 0
    query_metrics: list[dict[str, Any]] = []
    union_sizes: list[int] = []

    recall_query_by_id = {item["query_id"]: item for item in recall_artifact["queries"]}
    for fused_query in fusion_artifact["queries"]:
        query_id = fused_query["query_id"]
        query = query_by_id.get(query_id)
        if query is None:
            raise BusinessError(
                ErrorCode.INTERNAL,
                "fusion query is absent from input bundle",
                {"query_id": query_id},
            )
        references = list(dict.fromkeys(query.reference_item_ids))
        reference_ranks = {item_id: rank for rank, item_id in enumerate(references, start=1)}
        recalled_union: set[str] = set()
        per_route: dict[str, Any] = {}

        route_item_ids: dict[str, set[str]] = {source: set() for source in SOURCE_ORDER}
        recall_query = recall_query_by_id.get(query_id)
        if recall_query is None:
            raise BusinessError(
                ErrorCode.INTERNAL,
                "fusion query is absent from recall artifact",
                {"query_id": query_id},
            )
        for source in SOURCE_ORDER:
            route_item_ids[source] = {
                candidate["item_id"] for candidate in recall_query["routes"][source].get("candidates", [])
            }
            route_ids = route_item_ids[source]
            recalled_union.update(route_ids)
            hits = route_ids & set(references)
            route_aggregate[source]["reference_hits"] += len(hits)
            route_aggregate[source]["queries_with_hit"] += int(bool(hits))
            route_aggregate[source]["candidate_emissions"] += fused_query["routes"][source]["emitted"]
            per_route[source] = {
                "recalled_candidates": len(route_ids),
                "reference_hits": len(hits),
                "emitted": fused_query["routes"][source]["emitted"],
            }

        unique_recalled_items.update(recalled_union)
        union_sizes.append(len(recalled_union))
        fused_items = fused_query["fused"]
        fused_ids = [item["item_id"] for item in fused_items]
        unique_fused_items.update(fused_ids)

        intersection = [item_id for item_id in fused_ids if item_id in reference_ranks]
        if references:
            judged_queries += 1
            if intersection:
                queries_with_fused_hit += 1
            precision = _round_ratio(len(intersection), len(fused_ids))
            recall = _round_ratio(len(intersection), len(references))
            precision_values.append(precision)
            recall_values.append(recall)
            fused_rank_map = {item_id: rank for rank, item_id in enumerate(fused_ids, start=1)}
            for item_id in intersection:
                rank_deviations.append(abs(fused_rank_map[item_id] - reference_ranks[item_id]))

        query_metrics.append(
            {
                "query_id": query_id,
                "reference_count": len(references),
                "candidate_union_size": len(recalled_union),
                "fused_count": len(fused_ids),
                "reference_overlap_at_k": len(intersection),
                "precision_at_k": _round_ratio(len(intersection), len(fused_ids)),
                "recall_at_k": _round_ratio(len(intersection), len(references)),
                "routes": per_route,
            }
        )

    query_count = len(fusion_artifact["queries"])
    for source in SOURCE_ORDER:
        route_aggregate[source]["hit_rate"] = _round_ratio(
            route_aggregate[source]["queries_with_hit"],
            query_count,
        )

    item_count = int(fusion_artifact["item_count"])
    summary = {
        "format": "scale-summary-v1",
        "scale": fusion_artifact["scale"],
        "seed": fusion_artifact["seed"],
        "config_version": config.fingerprint,
        "input_fingerprint": input_fingerprint,
        "fusion_digest": fusion_digest,
        "item_count": item_count,
        "query_count": query_count,
        "judged_query_count": judged_queries,
        "decision": {
            "enabled_sources": fusion_artifact["decision"]["enabled_sources"],
            "disabled_sources": fusion_artifact["decision"]["disabled_sources"],
            "used_evaluations": fusion_artifact["decision"]["used_evaluations"],
            "global_budget": fusion_artifact["decision"]["global_budget"],
            "fusion_top_k": fusion_artifact["decision"]["fusion_top_k"],
            "routes": {
                source: {
                    "enabled": fusion_artifact["decision"]["routes"][source]["enabled"],
                    "effective_depth": fusion_artifact["decision"]["routes"][source]["effective_depth"],
                    "planned_evaluations": fusion_artifact["decision"]["routes"][source]["planned_evaluations"],
                    "reason": fusion_artifact["decision"]["routes"][source]["reason"],
                    "reason_text": fusion_artifact["decision"]["routes"][source]["reason_text"],
                }
                for source in SOURCE_ORDER
            },
        },
        "routes": route_aggregate,
        "fusion": {
            "strategy": fusion_artifact["strategy"],
            "dedup_rule": fusion_artifact["dedup_rule"],
            "top_k": fusion_artifact["top_k"],
            "precision_at_k": _average(precision_values),
            "recall_at_k": _average(recall_values),
            "queries_with_reference_hit": queries_with_fused_hit,
            "query_hit_rate": _round_ratio(queries_with_fused_hit, judged_queries),
            "average_rank_deviation": _average(rank_deviations),
            "rank_deviation_pairs": len(rank_deviations),
        },
        "candidate_coverage": _round_ratio(len(unique_recalled_items), item_count),
        "unique_recalled_items": len(unique_recalled_items),
        "unique_fused_items": len(unique_fused_items),
        "average_query_candidate_union": _average(union_sizes),
        "per_query": query_metrics,
    }
    return summary
