"""Stage 2: local deterministic retrieval and log-domain scoring.

A paragraph score combines, entirely in the log domain:

* lexical overlap between the rewritten query and the paragraph,
* paragraph version freshness,
* a source-channel prior from the configuration.

Results are ordered by descending score and then ascending doc_id.  Empty
queries, zero candidates and non-finite numbers raise stable business errors
instead of silently returning empty lists.
"""
from __future__ import annotations

from typing import Any, Dict, List

import numpy as np

from .errors import (E_BUDGET_EXCEEDED, E_NUMERIC, E_RETRIEVAL_EMPTY,
                     WorkbenchError)
from .io_utils import digest_json
from .planner import STRATEGY_EXACT, evaluation_pool
from .text import jaccard, token_set

STAGE_NAME = "retrieval"


def _log_component(value: float, *, session_id: str, turn_index: int,
                   component: str) -> float:
    if not bool(np.isfinite(value)):
        raise WorkbenchError(
            E_NUMERIC, f"non-finite retrieval component: {component}",
            stage=STAGE_NAME, session_id=session_id,
            turn_index=turn_index, context={"component": component})
    return float(np.log(value))


def score_paragraph(query: str, doc: Dict[str, Any],
                    config: Dict[str, Any],
                    max_version: int) -> Dict[str, Any]:
    retrieval_cfg = config["retrieval"]
    weights = retrieval_cfg["weights"]
    priors = retrieval_cfg["channel_prior"]
    query_tokens = token_set(query)
    doc_tokens = token_set(doc["text"])
    overlap = jaccard(query_tokens, doc_tokens)

    # All components live in the log domain.  Small epsilon terms keep the
    # functions finite while preserving ordering.
    log_overlap = float(np.log(overlap + np.float64(1e-9)))
    freshness_ratio = (int(doc["version"]) + 1.0) / (max_version + 1.0)
    log_freshness = float(np.log(freshness_ratio))
    channel_prior = float(priors.get(doc["channel"], 0.0))
    log_channel = float(np.log(np.exp(channel_prior)))  # identity, log-domain

    score = float(np.sum(np.array([
        weights["overlap"] * log_overlap,
        weights["freshness"] * log_freshness,
        weights["channel"] * log_channel,
    ], dtype=np.float64)))
    components = {
        "log_overlap": log_overlap,
        "log_freshness": log_freshness,
        "log_channel": log_channel,
        "overlap_jaccard": overlap,
        "channel_prior": channel_prior,
        "version": int(doc["version"]),
    }
    return {"doc_id": doc["doc_id"], "score": score,
            "components": components,
            "evaluated": True}


def retrieve(query_product: Dict[str, Any], docs: List[Dict[str, Any]],
             config: Dict[str, Any], plan: Dict[str, Any],
             budget_charge: Dict[str, int]) -> Dict[str, Any]:
    session_id = query_product["session_id"]
    turn_index = query_product["turn_index"]
    query = query_product["rewritten_query"]

    if not query.strip():
        raise WorkbenchError(
            E_RETRIEVAL_EMPTY, "rewritten query is empty",
            stage=STAGE_NAME, session_id=session_id,
            turn_index=turn_index)

    query_tokens = token_set(query)
    strategy = plan["strategy"]
    if strategy == STRATEGY_EXACT:
        pool = evaluation_pool(strategy, docs, query_tokens, len(docs))
    else:
        pool = evaluation_pool(
            strategy, docs, query_tokens,
            plan["planned_evaluations"],
        )
    # The global budget is the hard ceiling: never evaluate beyond it.
    pool = pool[:budget_charge["charged"]]
    if not pool:
        cause = (
            E_BUDGET_EXCEEDED
            if plan["remaining_budget_before"] <= 0
            else E_RETRIEVAL_EMPTY
        )
        raise WorkbenchError(
            E_RETRIEVAL_EMPTY,
            "no candidate paragraph remained after budget/strategy filtering",
            stage=STAGE_NAME, session_id=session_id,
            turn_index=turn_index,
            context={"strategy": strategy,
                     "remaining_budget": plan["remaining_budget_before"],
                     "cause": cause})

    max_version = max(int(doc["version"]) for doc in docs) if docs else 0
    scored: List[Dict[str, Any]] = []
    for doc in pool:
        result = score_paragraph(query, doc, config, max_version)
        if not bool(np.isfinite(result["score"])):
            raise WorkbenchError(
                E_NUMERIC, "retrieval score is NaN or infinite",
                stage=STAGE_NAME, session_id=session_id,
                turn_index=turn_index,
                context={"doc_id": doc["doc_id"]})
        result["paragraph"] = {
            "doc_id": doc["doc_id"], "text": doc["text"],
            "version": doc["version"], "channel": doc["channel"],
            "lang": doc["lang"],
        }
        scored.append(result)

    scored.sort(key=lambda row: (-row["score"], row["doc_id"]))
    limit = int(config["retrieval"]["evidence_limit"])
    candidates = scored[:limit]
    if not candidates:  # pragma: no cover - guarded by empty pool above
        raise WorkbenchError(
            E_RETRIEVAL_EMPTY, "all candidates were filtered out",
            stage=STAGE_NAME, session_id=session_id, turn_index=turn_index)

    product = {
        "session_id": session_id,
        "turn_index": turn_index,
        "stage": STAGE_NAME,
        "rewritten_query": query,
        "strategy": strategy,
        "plan": plan,
        "budget_charge": budget_charge,
        "evaluation_count": len(pool),
        "candidate_count": len(candidates),
        "candidates": candidates,
        "query_lang": query_product.get("lang", ""),
    }
    product["fingerprint"] = digest_json({
        "rewritten_query": query,
        "strategy": strategy,
        "candidates": [
            {"doc_id": row["doc_id"], "score": row["score"],
             "components": row["components"]} for row in candidates
        ],
        "budget_charge": budget_charge,
    })
    return product
