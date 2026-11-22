"""阶段四：对数域裁决分合成、确定性仲裁与分层验证汇总。"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Set

import numpy as np

from .config import Config
from .util import fingerprint


def _token_set(normalized_query: str) -> Set[str]:
    return {tok for tok in normalized_query.split() if tok} or {normalized_query}


def overlap_ratio(rewritten: str, original: str) -> float:
    """词项重叠度（Jaccard，按空白与中文单字粗切）。"""
    def pieces(text: str) -> Set[str]:
        out: Set[str] = set()
        for word in text.split():
            if any("\u4e00" <= ch <= "\u9fff" for ch in word):
                out.update(ch for ch in word if "\u4e00" <= ch <= "\u9fff")
            else:
                out.add(word)
        return out or {text}

    a = pieces(rewritten)
    b = pieces(original)
    inter = len(a & b)
    union = len(a | b)
    return round(inter / union if union else 0.0, 9)


def score_candidates(candidate_bundle: Dict[str, Any], plan: Dict[str, Any],
                     cfg: Config) -> List[Dict[str, Any]]:
    """在对数域合成裁决分：log(confidence)*w_c + log(route_weight)*w_r + overlap*w_o。"""
    adj = cfg.adjudication
    candidates = candidate_bundle["candidates"]
    original = plan["normalized_query"]

    scores: List[Dict[str, Any]] = []
    for cand in candidates:
        confidence = float(cand["confidence"])
        route_weight = cfg.route_weight(cand["route"])
        overlap = overlap_ratio(cand["rewritten_query"], original)
        # log1p 保护零置信；权重取 log，负数时按 -inf 处理的退化由 max 下限避免
        log_conf = float(np.log(max(confidence, 1e-12)))
        log_route = float(np.log(max(route_weight, 1e-12)))
        score = (adj["log_confidence_weight"] * log_conf
                 + adj["route_weight"] * log_route
                 + adj["overlap_weight"] * overlap)
        scores.append({
            "candidate_id": cand["candidate_id"],
            "route": cand["route"],
            "rewritten_query": cand["rewritten_query"],
            "confidence": round(confidence, 9),
            "route_weight": round(route_weight, 9),
            "overlap": overlap,
            "log_confidence": round(log_conf, 9),
            "log_route_weight": round(log_route, 9),
            "score": round(float(score), 9),
            "basis": cand["basis"],
            "candidate_fingerprint": cand["candidate_fingerprint"],
        })

    # 确定性仲裁：分数降序 → 路线固定优先序 → 候选指纹升序
    route_order = {"synonym": 0, "bridge": 1, "glyph": 2}
    scores.sort(key=lambda s: (-s["score"], route_order[s["route"]],
                               s["candidate_fingerprint"]))
    for rank, item in enumerate(scores):
        item["rank"] = rank
    return scores


def adjudicate(candidate_bundle: Dict[str, Any], plan: Dict[str, Any],
               cfg: Config) -> Dict[str, Any]:
    scored = score_candidates(candidate_bundle, plan, cfg)
    max_adopt = int(cfg.adjudication["max_adoptions_per_query"])
    original = plan["normalized_query"]

    adopted: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    seen_texts: Set[str] = {original}

    for item in scored:
        text = item["rewritten_query"]
        decision = dict(item)
        if len(adopted) >= max_adopt:
            decision["decision"] = "rejected"
            decision["reject_reason"] = "ADOPTION_LIMIT"
            rejected.append(decision)
            continue
        if not text.strip():
            decision["decision"] = "rejected"
            decision["reject_reason"] = "EMPTY_REWRITE"
            rejected.append(decision)
            continue
        if text == original:
            decision["decision"] = "rejected"
            decision["reject_reason"] = "DUPLICATE_OF_ORIGINAL"
            rejected.append(decision)
            continue
        if text in seen_texts:
            decision["decision"] = "rejected"
            decision["reject_reason"] = "DUPLICATE_REWRITE"
            rejected.append(decision)
            continue
        seen_texts.add(text)
        decision["decision"] = "adopted"
        adopted.append(decision)

    return {
        "query_id": plan["query_id"],
        "status": "adjudicated",
        "original_query": original,
        "adoptions": adopted,
        "rejected": rejected,
        "adopted_queries": [a["rewritten_query"] for a in adopted],
        "validation_fingerprint": fingerprint({
            "adoptions": adopted,
            "rejected": rejected,
        }),
    }


# ---- 分层汇总 -------------------------------------------------------------
def build_summary(results: List[Dict[str, Any]],
                  plans: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """按 lang 与 channel 分层输出采纳率与路命中分布。"""
    strata: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"queries": 0, "adopted": 0, "queries_with_adoption": 0,
                 "candidates": 0, "route_hits": defaultdict(int)})

    for result in results:
        plan = plans[result["query_id"]]
        key = f"{plan.get('lang')}|{plan.get('channel')}"
        bucket = strata[key]
        bucket["queries"] += 1
        adopted_count = len(result.get("adopted_queries", []))
        bucket["adopted"] += adopted_count
        if adopted_count > 0:
            bucket["queries_with_adoption"] += 1
        for candidate in result.get("scored_candidates", []):
            bucket["candidates"] += 1
            bucket["route_hits"][candidate["route"]] += 1

    summary: Dict[str, Any] = {"by_lang_channel": {}, "totals": {
        "queries": 0, "adopted": 0, "queries_with_adoption": 0,
        "candidates": 0, "route_hits": defaultdict(int)}}
    for key in sorted(strata):
        bucket = strata[key]
        lang, channel = key.split("|", 1)
        adoption_rate = round(bucket["adopted"] / bucket["queries"], 9) \
            if bucket["queries"] else 0.0
        query_adoption_rate = round(
            bucket["queries_with_adoption"] / bucket["queries"], 9) \
            if bucket["queries"] else 0.0
        summary["by_lang_channel"][key] = {
            "lang": lang,
            "channel": channel,
            "queries": bucket["queries"],
            "adopted": bucket["adopted"],
            "adoption_rate": adoption_rate,
            "query_adoption_rate": query_adoption_rate,
            "candidate_count": bucket["candidates"],
            "route_hits": dict(sorted(bucket["route_hits"].items())),
        }
        summary["totals"]["queries"] += bucket["queries"]
        summary["totals"]["adopted"] += bucket["adopted"]
        summary["totals"]["queries_with_adoption"] += \
            bucket["queries_with_adoption"]
        summary["totals"]["candidates"] += bucket["candidates"]
        for route, count in bucket["route_hits"].items():
            summary["totals"]["route_hits"][route] += count
    totals = summary["totals"]
    totals["adoption_rate"] = round(
        totals["adopted"] / totals["queries"], 9) if totals["queries"] else 0.0
    totals["query_adoption_rate"] = round(
        totals["queries_with_adoption"] / totals["queries"], 9) \
        if totals["queries"] else 0.0
    totals["route_hits"] = dict(sorted(totals["route_hits"].items()))
    summary["summary_fingerprint"] = fingerprint(summary["by_lang_channel"])
    return summary
