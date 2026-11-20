"""Pipeline stage computations: assemble, score, rerank.

Each stage returns its JSON-serialisable artifact directly. Artifacts
carry exact fixed-point micro integers (used by downstream stages) plus
6-decimal string views (for humans), so every byte is deterministic
across runs, processes and platforms.
"""

from __future__ import annotations

import numpy as np

from .errors import E_MISSING_FEATURE, PipelineError
from .hashing import SCALE, embedding_vector, micro_to_str, to_micro

STAGE_ORDER = ["assemble", "score", "rerank", "aggregate"]
SHARD_STAGES = ["assemble", "score", "rerank"]


def assemble_stage(groups, candidates_by_query, config) -> dict:
    """Stage 1: hash embeddings concatenated with numeric features."""
    out_groups = []
    for group in groups:
        qid = group["query_id"]
        cands = candidates_by_query.get(qid, [])
        if not cands:
            raise PipelineError(
                E_MISSING_FEATURE,
                f"empty candidate group for query {qid}",
                query_id=qid, field="candidates",
            )
        items = []
        for cand in cands:
            feats = cand.get("features")
            if not isinstance(feats, dict):
                raise PipelineError(
                    E_MISSING_FEATURE,
                    f"candidate {cand.get('doc_id')} missing features object",
                    query_id=qid, field="features",
                )
            feats_micro = {}
            for name in config.features:
                if name not in feats:
                    raise PipelineError(
                        E_MISSING_FEATURE,
                        f"candidate {cand.get('doc_id')} missing feature {name}",
                        query_id=qid, field=name,
                    )
                feats_micro[name] = to_micro(feats[name])
            items.append({
                "doc_id": cand["doc_id"],
                "category": cand["category"],
                "language": cand["language"],
                "coarse_score_micro": to_micro(cand.get("coarse_score", 0)),
                "embedding": embedding_vector(qid, cand["doc_id"],
                                              config.embedding_dim),
                "features_micro": feats_micro,
            })
        out_groups.append({
            "query_id": qid,
            "locale": group["locale"],
            "channel": group["channel"],
            "items": items,
        })
    return {"query_groups": out_groups}


def score_stage(assemble_artifact: dict, config) -> dict:
    """Stage 2: linear scoring, fixed-point quantisation, component detail.

    Uses numpy int64 vector arithmetic: exact and platform-deterministic.
    """
    weights = config.weights_micro
    emb_w = config.embedding_weight_micro
    dim = config.embedding_dim
    names = config.features
    weight_vec = np.array([weights[n] for n in names], dtype=np.int64)
    out_groups = []
    for group in assemble_artifact["query_groups"]:
        scored_items = []
        for item in group["items"]:
            feature_vec = np.array([item["features_micro"][n] for n in names],
                                   dtype=np.int64)
            contribs = (weight_vec * feature_vec) // SCALE
            components = {n: int(c) for n, c in zip(names, contribs)}
            total = int(contribs.sum())
            emb_avg = int(np.asarray(item["embedding"], dtype=np.int64).sum()
                          // dim)
            emb_contrib = emb_w * emb_avg // SCALE
            components["embedding"] = emb_contrib
            total += emb_contrib
            scored_items.append({
                "doc_id": item["doc_id"],
                "category": item["category"],
                "language": item["language"],
                "score_micro": total,
                "components_micro": components,
            })
        ranked = sorted(scored_items,
                        key=lambda it: (-it["score_micro"], it["doc_id"]))
        cands = []
        for rank, it in enumerate(ranked):
            cands.append({
                "rank": rank,
                "doc_id": it["doc_id"],
                "category": it["category"],
                "language": it["language"],
                "score_micro": it["score_micro"],
                "score": micro_to_str(it["score_micro"]),
                "components_micro": it["components_micro"],
                "components": {
                    k: micro_to_str(v) for k, v in it["components_micro"].items()
                },
            })
        out_groups.append({
            "query_id": group["query_id"],
            "locale": group["locale"],
            "channel": group["channel"],
            "candidates": cands,
        })
    return {"query_groups": out_groups}


def rerank_stage(score_artifact: dict, config) -> dict:
    """Stage 3: windowed greedy diversity selection with decision trace."""
    cat_pen = config.category_penalty_micro
    lang_pen = config.language_penalty_micro
    dedup_pen = config.dedup_penalty_micro
    out_groups = []
    for group in score_artifact["query_groups"]:
        remaining = [dict(c) for c in group["candidates"]]
        n = len(remaining)
        requested_window = config.window_size
        window = min(requested_window, n)
        truncated = requested_window > n
        selected = []
        trace = []
        step = 0
        while remaining:
            window_slice = selected[-window:] if window else []
            cat_counts = {}
            lang_counts = {}
            docid_counts = {}
            for it in selected:
                docid_counts[it["doc_id"]] = docid_counts.get(it["doc_id"], 0) + 1
            for it in window_slice:
                cat_counts[it["category"]] = cat_counts.get(it["category"], 0) + 1
                lang_counts[it["language"]] = lang_counts.get(it["language"], 0) + 1
            best_idx = None
            best_key = None
            considered = []
            for idx, cand in enumerate(remaining):
                cc = cat_counts.get(cand["category"], 0)
                lc = lang_counts.get(cand["language"], 0)
                dc = docid_counts.get(cand["doc_id"], 0)
                penalty = cat_pen * cc + lang_pen * lc + dedup_pen * dc
                adjusted = cand["score_micro"] - penalty
                considered.append({
                    "doc_id": cand["doc_id"],
                    "precision_micro": cand["score_micro"],
                    "category_hits": cc,
                    "language_hits": lc,
                    "duplicate_hits": dc,
                    "penalty_micro": penalty,
                    "adjusted_micro": adjusted,
                })
                key = (-adjusted, -cand["score_micro"], cand["doc_id"])
                if best_key is None or key < best_key:
                    best_key = key
                    best_idx = idx
            chosen = remaining.pop(best_idx)
            chosen_view = next(
                c for c in considered if c["doc_id"] == chosen["doc_id"])
            trace.append({
                "step": step,
                "chosen": _trace_candidate_json(chosen_view),
                "considered": [_trace_candidate_json(c) for c in considered],
            })
            selected.append(chosen)
            step += 1
        out_groups.append({
            "query_id": group["query_id"],
            "locale": group["locale"],
            "channel": group["channel"],
            "requested_window": requested_window,
            "effective_window": window,
            "window_truncated": truncated,
            "selected": [{
                "rank": rank,
                "doc_id": it["doc_id"],
                "category": it["category"],
                "language": it["language"],
                "score_micro": it["score_micro"],
                "precision_score": micro_to_str(it["score_micro"]),
            } for rank, it in enumerate(selected)],
            "trace": trace,
        })
    return {"query_groups": out_groups}


def _trace_candidate_json(c: dict) -> dict:
    return {
        "doc_id": c["doc_id"],
        "precision_score": micro_to_str(c["precision_micro"]),
        "category_hits": c["category_hits"],
        "language_hits": c["language_hits"],
        "duplicate_hits": c["duplicate_hits"],
        "penalty": micro_to_str(c["penalty_micro"]),
        "adjusted_score": micro_to_str(c["adjusted_micro"]),
    }
