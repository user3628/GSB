"""Evidence reranking, deterministic diversification, and fragment budgets."""

from __future__ import annotations

import math
from typing import Any

from .errors import E_EVIDENCE_EMPTY, E_NUMERIC, EngineError
from .normalize import tokenize
from .util import require_finite, stable_round


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 1.0
    union = left | right
    if not union:
        return 1.0
    return len(left & right) / len(union)


def _rerank_candidate(
    candidate: dict[str, Any],
    query_terms: set[str],
    rerank: dict[str, Any],
    query_id: str | None,
) -> dict[str, Any]:
    tokens = set(tokenize(candidate["title"] + " " + candidate["body"]))
    coverage = len(tokens & query_terms) / max(1, len(query_terms))
    character_count = len(candidate["body"])
    ideal = max(1, int(rerank["ideal_fragment_chars"]))
    length_fit = max(0.0, 1.0 - abs(character_count - ideal) / ideal)
    try:
        coverage_component = rerank["coverage_weight"] * math.log1p(coverage * 9.0)
        length_component = rerank["length_fit_weight"] * math.log1p(1.0 + length_fit)
        total = candidate["retrieval_score"] + coverage_component + length_component
        for name, value in (
            ("coverage", coverage_component),
            ("length_fit", length_component),
            ("total", total),
        ):
            require_finite(value, name)
    except (ValueError, OverflowError) as exc:
        raise EngineError(
            E_NUMERIC,
            "rerank score is NaN or Inf",
            query_id=query_id,
            doc_id=candidate.get("doc_id"),
            details={"subquery_index": candidate.get("subquery_index"), "numeric_error": str(exc)},
        ) from exc

    result = dict(candidate)
    result["tokens"] = sorted(tokens)
    result["term_coverage"] = stable_round(coverage)
    result["fragment_chars"] = character_count
    result["rerank_components"] = {
        "retrieval_log": candidate["retrieval_score"],
        "coverage_log": stable_round(coverage_component),
        "length_fit_log": stable_round(length_component),
        "total_log": stable_round(total),
    }
    result["rerank_score"] = stable_round(total)
    return result


def run_rerank(
    normalized: dict[str, Any],
    dispatch_product: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    query_id = normalized.get("query_id")
    rerank = config["rerank"]
    max_evidence = int(rerank["max_evidence"])
    if max_evidence == 0:
        raise EngineError(
            E_EVIDENCE_EMPTY,
            "evidence limit is zero",
            query_id=query_id,
            details={"reason": "max_evidence_zero"},
        )

    query_terms = set(normalized["expanded_query_terms"])
    reranked_results: list[dict[str, Any]] = []
    for subquery_result in dispatch_product["subquery_results"]:
        if subquery_result["status"] != "accepted":
            reranked_results.append(
                {
                    "subquery_index": subquery_result["subquery_index"],
                    "subquery_text": subquery_result["subquery_text"],
                    "priority": subquery_result["priority"],
                    "status": subquery_result["status"],
                    "candidates": [],
                }
            )
            continue
        candidates = [
            _rerank_candidate(candidate, query_terms, rerank, query_id)
            for candidate in subquery_result["candidates"]
        ]
        candidates.sort(
            key=lambda item: (
                -item["rerank_score"],
                -item["retrieval_score"],
                item["doc_id"],
                item["source"],
            )
        )
        reranked_results.append(
            {
                "subquery_index": subquery_result["subquery_index"],
                "subquery_text": subquery_result["subquery_text"],
                "priority": subquery_result["priority"],
                "status": "accepted",
                "candidates": candidates,
            }
        )

    reranked_results.sort(
        key=lambda item: (item["priority"], item["subquery_index"])
    )
    max_total_chars = int(rerank["max_total_chars"])
    min_fragment_chars = int(rerank["min_fragment_chars"])
    similarity_threshold = float(rerank["mmr_similarity_threshold"])
    used_chars = 0
    selected: list[dict[str, Any]] = []
    selected_doc_ids: set[str] = set()
    selected_token_sets: list[set[str]] = []
    discarded: list[dict[str, Any]] = []

    for result in reranked_results:
        if result["status"] != "accepted":
            continue
        for candidate in result["candidates"]:
            doc_id = candidate["doc_id"]
            token_set = set(candidate.get("tokens", []))
            if len(selected) >= max_evidence:
                discarded.append(
                    {
                        "doc_id": doc_id,
                        "subquery_index": result["subquery_index"],
                        "reason": "evidence_limit",
                        "char_count": candidate["fragment_chars"],
                    }
                )
                continue
            if doc_id in selected_doc_ids:
                discarded.append(
                    {
                        "doc_id": doc_id,
                        "subquery_index": result["subquery_index"],
                        "reason": "duplicate_doc_across_subqueries",
                        "char_count": candidate["fragment_chars"],
                    }
                )
                continue
            max_similarity = max(
                (_jaccard(token_set, existing) for existing in selected_token_sets),
                default=0.0,
            )
            if max_similarity >= similarity_threshold and selected:
                discarded.append(
                    {
                        "doc_id": doc_id,
                        "subquery_index": result["subquery_index"],
                        "reason": "mmr_overlap",
                        "max_similarity": stable_round(max_similarity),
                        "char_count": candidate["fragment_chars"],
                    }
                )
                continue
            remaining = max_total_chars - used_chars
            if remaining < min_fragment_chars or candidate["fragment_chars"] < min_fragment_chars:
                discarded.append(
                    {
                        "doc_id": doc_id,
                        "subquery_index": result["subquery_index"],
                        "reason": (
                            "minimum_fragment_budget"
                            if remaining < min_fragment_chars
                            else "fragment_below_minimum"
                        ),
                        "char_count": candidate["fragment_chars"],
                        "remaining_chars": remaining,
                        "min_fragment_chars": min_fragment_chars,
                    }
                )
                continue
            if candidate["fragment_chars"] > remaining:
                discarded.append(
                    {
                        "doc_id": doc_id,
                        "subquery_index": result["subquery_index"],
                        "reason": "char_budget",
                        "char_count": candidate["fragment_chars"],
                        "remaining_chars": remaining,
                        "min_fragment_chars": min_fragment_chars,
                    }
                )
                continue

            evidence_index = len(selected) + 1
            evidence = {
                "evidence_index": evidence_index,
                "citation_id": f"[{evidence_index}]",
                "doc_id": doc_id,
                "title": candidate["title"],
                "text": candidate["body"],
                "char_count": candidate["fragment_chars"],
                "lang": candidate["lang"],
                "source": candidate["source"],
                "source_shards": list(candidate["source_shards"]),
                "subquery_index": result["subquery_index"],
                "subquery_text": result["subquery_text"],
                "retrieval_score": candidate["retrieval_score"],
                "term_coverage": candidate["term_coverage"],
                "rerank_score": candidate["rerank_score"],
                "rerank_components": candidate["rerank_components"],
                "matched_terms": list(candidate["matched_terms"]),
            }
            selected.append(evidence)
            selected_doc_ids.add(doc_id)
            selected_token_sets.append(token_set)
            used_chars += candidate["fragment_chars"]

    if not selected:
        reasons = sorted({item["reason"] for item in discarded})
        raise EngineError(
            E_EVIDENCE_EMPTY,
            "no evidence fragment fits the configured evidence budget",
            query_id=query_id,
            details={
                "reason": "no_selectable_evidence",
                "discard_reasons": reasons,
                "discarded_count": len(discarded),
                "max_total_chars": max_total_chars,
                "min_fragment_chars": min_fragment_chars,
            },
        )

    return {
        "query_id": query_id,
        "query_terms": sorted(query_terms),
        "subquery_results": reranked_results,
        "evidence": selected,
        "discarded_evidence": discarded,
        "evidence_budget": {
            "max_evidence": max_evidence,
            "max_total_chars": max_total_chars,
            "min_fragment_chars": min_fragment_chars,
            "used_chars": used_chars,
            "remaining_chars": max_total_chars - used_chars,
            "evidence_count": len(selected),
        },
    }
