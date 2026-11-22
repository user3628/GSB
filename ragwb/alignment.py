"""Stage 3: evidence alignment, conflict detection and adjudication.

Candidate paragraphs are clustered greedily by token-overlap similarity using
the configured alignment tolerance.  Within a fact cluster, paragraphs that
report different numeric values or dates are marked as conflicting.  A single
paragraph is adopted per cluster from a log-domain credibility score that
blends paragraph version, channel prior and language consistency.  Every tie
has a deterministic arbitration rule and no paragraph is ever discarded:
demoted and fully-conflicted evidence stays in the product and later in the
evidence chain.
"""
from __future__ import annotations

import math
import re
from typing import Any, Dict, List

import numpy as np

from .io_utils import digest_json
from .text import fact_signature, jaccard, token_set

_ENTITY_TAILS = (
    "电池", "芯片", "手机", "无人机", "路由器", "显示器", "键盘",
    "充电器", "传感器",
)
_EN_ENTITY_TAILS = (
    "battery", "chip", "phone", "drone", "router", "display",
    "keyboard", "charger", "sensor",
)

# Deterministic cross-language entity aliases for the local demo corpus.
_ENTITY_ALIASES = {
    "nimbus drone": "nimbus_drone",
    "nimbus 无人机": "nimbus_drone",
    "nimbus无人机": "nimbus_drone",
    "aurora battery": "aurora_battery",
    "极光电池": "aurora_battery",
    "starriver chip": "starriver_chip",
    "星河芯片": "starriver_chip",
    "yunling phone": "yunling_phone",
    "云岭手机": "yunling_phone",
}


def _entity_key(text: str) -> str:
    head = _entity_head(text)
    if not head:
        return ""
    normalized = " ".join(head.lower().split())
    return _ENTITY_ALIASES.get(normalized, normalized)


def _entity_head(text: str) -> str:
    """Deterministic head entity used to group facts about the same thing."""
    for tail in _ENTITY_TAILS:
        index = text.find(tail)
        if index > 0:
            prefix = text[:index]
            head = re.split(r"[，。？?！!、：:，]", prefix)[-1].strip()
            if head:
                return head + tail
    lowered = text.lower()
    for tail in _EN_ENTITY_TAILS:
        match = re.search(r"([a-z][a-z0-9]*)\s+" + re.escape(tail),
                          lowered)
        if match:
            return f"{match.group(1)} {tail}"
    return ""

STAGE_NAME = "alignment"


def _similarity(left: str, right: str) -> float:
    return jaccard(token_set(left), token_set(right))


def _has_conflict(texts: List[str]) -> bool:
    signatures = [fact_signature(text) for text in texts]
    number_sets = {sig[0] for sig in signatures}
    date_sets = {sig[1] for sig in signatures}
    return len(number_sets) > 1 or len(date_sets) > 1


def _credibility(paragraph: Dict[str, Any], query_lang: str,
                 config: Dict[str, Any], max_version: int) -> Dict[str, Any]:
    weights = config["retrieval"]["weights"]
    priors = config["retrieval"]["channel_prior"]
    version = int(paragraph["version"])
    log_version = float(np.log((version + 1.0) / (max_version + 1.0)))
    channel_prior = float(priors.get(paragraph["channel"], 0.0))
    log_channel = channel_prior  # already a log-prior
    if query_lang in ("mixed", "other", "", paragraph["lang"]):
        lang_bonus = 1.0
    else:
        lang_bonus = 0.5
    log_language = float(np.log(lang_bonus))
    score = float(np.sum(np.array([
        weights["credibility_version"] * log_version,
        weights["credibility_channel"] * log_channel,
        weights["credibility_language"] * log_language,
    ], dtype=np.float64)))
    return {
        "credibility": score,
        "log_version": log_version,
        "log_channel": log_channel,
        "log_language": log_language,
        "channel_prior": channel_prior,
        "language_bonus": lang_bonus,
    }


def _cluster(candidates: List[Dict[str, Any]],
             tolerance: float) -> List[List[Dict[str, Any]]]:
    threshold = 1.0 - tolerance
    clusters: List[List[Dict[str, Any]]] = []
    for candidate in sorted(candidates, key=lambda row: row["doc_id"]):
        candidate_key = _entity_key(candidate["paragraph"]["text"])
        placed = False
        for cluster in clusters:
            anchor = cluster[0]["paragraph"]["text"]
            anchor_key = _entity_key(anchor)
            same_entity = bool(
                candidate_key and anchor_key
                and candidate_key == anchor_key
            )
            similar = _similarity(
                anchor, candidate["paragraph"]["text"]) >= threshold
            if same_entity or similar:
                cluster.append(candidate)
                placed = True
                break
        if not placed:
            clusters.append([candidate])
    return clusters


def align(retrieval_product: Dict[str, Any],
          config: Dict[str, Any]) -> Dict[str, Any]:
    tolerance = float(config["retrieval"]["alignment_tolerance"])
    query_lang = retrieval_product.get("query_lang", "")
    candidates = retrieval_product["candidates"]
    paragraphs = [row["paragraph"] for row in candidates]
    max_version = max(int(doc["version"]) for doc in paragraphs)
    priors = config["retrieval"]["channel_prior"]

    clusters_raw = _cluster(candidates, tolerance)
    clusters_out: List[Dict[str, Any]] = []
    adopted: List[Dict[str, Any]] = []
    demoted: List[Dict[str, Any]] = []

    for cluster_index, cluster in enumerate(clusters_raw):
        texts = [row["paragraph"]["text"] for row in cluster]
        conflict = _has_conflict(texts)
        scored_rows = []
        for row in cluster:
            credibility = _credibility(
                row["paragraph"], query_lang, config, max_version)
            scored_rows.append((row, credibility))

        # Deterministic total order: credibility, version, channel prior,
        # then ascending doc_id.  No two rows can remain tied after doc_id.
        scored_rows.sort(
            key=lambda pair: (
                -pair[1]["credibility"],
                -int(pair[0]["paragraph"]["version"]),
                -float(priors.get(pair[0]["paragraph"]["channel"], 0.0)),
                pair[0]["doc_id"],
            )
        )
        winner_row, winner_cred = scored_rows[0]
        runner_up_cred = scored_rows[1][1] \
            if len(scored_rows) > 1 else None
        fully_conflicted = bool(
            conflict and len(scored_rows) > 1 and runner_up_cred is not None
            and math.isclose(winner_cred["credibility"],
                             runner_up_cred["credibility"],
                             rel_tol=0.0,
                             abs_tol=1e-12)
        )

        members = []
        for rank, (row, credibility) in enumerate(scored_rows):
            paragraph = row["paragraph"]
            if rank == 0:
                disposition = "adopted"
                if conflict:
                    reason = ("conflict_tie_arbitration"
                              if fully_conflicted
                              else "highest_credibility_resolves_conflict")
                else:
                    reason = "highest_credibility"
            else:
                if conflict:
                    disposition = "demoted"
                    reason = ("conflicting_number_or_date_overridden"
                              if not fully_conflicted
                              else "fully_conflicted_cluster_demoted")
                else:
                    disposition = "supporting"
                    reason = "duplicate_fact_deduplicated"
            entry = {
                "doc_id": paragraph["doc_id"],
                "disposition": disposition,
                "reason": reason,
                "credibility": credibility,
                "retrieval_score": row["score"],
            }
            members.append(entry)
            if disposition == "adopted":
                adopted.append({
                    "doc_id": paragraph["doc_id"],
                    "paragraph": paragraph,
                    "cluster_index": cluster_index,
                    "adoption_reason": reason,
                    "credibility": credibility,
                    "fully_conflicted_cluster": fully_conflicted,
                })
            else:
                demoted.append({
                    "doc_id": paragraph["doc_id"],
                    "paragraph": paragraph,
                    "cluster_index": cluster_index,
                    "disposition": disposition,
                    "reason": reason,
                    "credibility": credibility,
                })

        clusters_out.append({
            "cluster_index": cluster_index,
            "has_conflict": conflict,
            "fully_conflicted": fully_conflicted,
            "adopted_doc_id": winner_row["doc_id"],
            "members": members,
            "evidence_retained": [
                row["paragraph"]["doc_id"] for row in cluster
            ],
        })

    product = {
        "session_id": retrieval_product["session_id"],
        "turn_index": retrieval_product["turn_index"],
        "stage": STAGE_NAME,
        "alignment_tolerance": tolerance,
        "cluster_count": len(clusters_out),
        "clusters": clusters_out,
        "adopted": adopted,
        "demoted": demoted,
    }
    product["fingerprint"] = digest_json({
        "alignment_tolerance": tolerance,
        "clusters": clusters_out,
        "adopted_doc_ids": [row["doc_id"] for row in adopted],
        "demoted": [
            {"doc_id": row["doc_id"], "disposition": row["disposition"],
             "reason": row["reason"]} for row in demoted
        ],
    })
    return product
