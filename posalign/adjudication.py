"""Mapping-conflict adjudication over aligned token/tag pairs."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from .config import GovernanceConfig


def _priority_order(config: GovernanceConfig) -> list[str]:
    ordered: list[str] = []
    for tag in config.adjudication.candidate_priority:
        if tag not in ordered:
            ordered.append(tag)
    for tag in config.standard_tags:
        if tag not in ordered:
            ordered.append(tag)
    return ordered


def _priority_score(tag: str, order: list[str]) -> float:
    try:
        index = order.index(tag)
    except ValueError:
        return 0.0
    if len(order) <= 1:
        return 1.0
    return 1.0 - (index / (len(order) - 1))


def _score_candidate(
    *,
    count: int,
    weighted_count: float,
    total_count: int,
    total_weighted: float,
    priority: float,
    config: GovernanceConfig,
) -> tuple[float, dict[str, float]]:
    frequency = count / total_count if total_count else 0.0
    if total_weighted > 0.0:
        reliability = weighted_count / total_weighted
    else:
        reliability = frequency
    scores = {
        "frequency": frequency,
        "annotator_reliability": reliability,
        "candidate_priority": priority,
    }
    confidence = sum(config.adjudication.weights[name] * scores[name] for name in config.adjudication.strategy_order)
    return confidence, scores


def adjudicate(align_payloads: list[dict[str, Any]], config: GovernanceConfig) -> dict[str, Any]:
    evidence: dict[str, dict[str, Any]] = {}
    diagnostics: list[dict[str, Any]] = []
    priority_order = _priority_order(config)

    for payload in sorted(align_payloads, key=lambda item: item["shard_id"]):
        for sentence in payload["sentences"]:
            for pair in sentence["pairs"]:
                old_tag = pair["old_tag"]
                new_tag = pair["new_tag"]
                label = evidence.setdefault(
                    old_tag,
                    {
                        "old_tag": old_tag,
                        "candidate_meta": {},
                        "counts": defaultdict(int),
                        "weighted": defaultdict(float),
                        "annotators": defaultdict(lambda: defaultdict(int)),
                        "sent_ids": defaultdict(set),
                        "variants": set(),
                        "observed_new_tags": set(),
                    },
                )
                label["variants"].add(pair["old_tag_raw"])
                for candidate in pair["old_candidates"]:
                    tag = candidate["tag"]
                    meta = label["candidate_meta"].setdefault(
                        tag,
                        {
                            "tag": tag,
                            "sources": set(),
                            "rules": set(),
                        },
                    )
                    meta["sources"].update(candidate.get("sources", [candidate.get("source", "rule")]))
                    meta["rules"].update(candidate.get("rules", []))
                candidate_tags = {candidate["tag"] for candidate in pair["old_candidates"]}
                if new_tag not in candidate_tags:
                    meta = label["candidate_meta"].setdefault(
                        new_tag,
                        {"tag": new_tag, "sources": set(), "rules": set()},
                    )
                    meta["sources"].add("observed")
                    diagnostics.append(
                        {
                            "code": "observed_candidate_not_generated",
                            "old_tag": old_tag,
                            "candidate": new_tag,
                            "sent_id": pair.get("sent_id", sentence["sent_id"]),
                            "meaning": "a new annotation vote was retained although candidate generation did not predict it",
                        }
                    )
                if pair.get("new_tag_diagnostic") == "unknown_new_tag":
                    diagnostics.append(
                        {
                            "code": "unknown_new_tag",
                            "old_tag": old_tag,
                            "candidate": new_tag,
                            "sent_id": sentence["sent_id"],
                        }
                    )
                reliability = config.adjudication.annotator_reliability.get(
                    pair["annotator"], config.adjudication.default_annotator_reliability
                )
                label["counts"][new_tag] += 1
                label["weighted"][new_tag] += reliability
                label["annotators"][new_tag][pair["annotator"]] += 1
                label["sent_ids"][new_tag].add(sentence["sent_id"])
                label["observed_new_tags"].add(new_tag)

    labels: list[dict[str, Any]] = []
    for old_tag in sorted(evidence):
        item = evidence[old_tag]
        total_count = sum(item["counts"].values())
        total_weighted = sum(item["weighted"].values())
        candidates: list[dict[str, Any]] = []
        candidate_tags = set(item["candidate_meta"]) | set(item["counts"])
        for tag in sorted(candidate_tags):
            meta = item["candidate_meta"].get(
                tag, {"tag": tag, "sources": {"observed"}, "rules": set()}
            )
            count = int(item["counts"].get(tag, 0))
            weighted_count = float(item["weighted"].get(tag, 0.0))
            confidence, score_breakdown = _score_candidate(
                count=count,
                weighted_count=weighted_count,
                total_count=total_count,
                total_weighted=total_weighted,
                priority=_priority_score(tag, priority_order),
                config=config,
            )
            candidates.append(
                {
                    "tag": tag,
                    "sources": sorted(meta["sources"]),
                    "rules": sorted(meta["rules"]),
                    "vote_count": count,
                    "weighted_vote_count": round(weighted_count, 6),
                    "annotator_votes": {
                        annotator: votes
                        for annotator, votes in sorted(item["annotators"].get(tag, {}).items())
                    },
                    "sent_ids": sorted(item["sent_ids"].get(tag, set())),
                    "confidence": round(confidence, 6),
                    "_confidence_exact": confidence,
                    "score_breakdown": {
                        name: round(value, 6) for name, value in score_breakdown.items()
                    },
                    "_score_exact": score_breakdown,
                }
            )

        if not candidates:
            labels.append(
                {
                    "old_tag": old_tag,
                    "original_variants": sorted(item["variants"]),
                    "mapping_status": "unmapped",
                    "resolved": {
                        "old_tag": old_tag,
                        "new_tag": None,
                        "confidence": 0.0,
                        "accepted": False,
                        "reason": "no_candidate",
                    },
                    "candidates": [],
                    "diagnostics": [
                        {
                            "code": "unmapped_old_tag",
                            "message": "no alias, rule, or observed candidate exists",
                        }
                    ],
                }
            )
            continue

        def rank_key(candidate: dict[str, Any]) -> tuple[Any, ...]:
            strategy_values = tuple(
                -(config.adjudication.weights[name] * candidate["_score_exact"][name])
                for name in config.adjudication.strategy_order
            )
            return (*strategy_values, candidate["_confidence_exact"], candidate["tag"])

        ordered = sorted(candidates, key=rank_key)
        winner = ordered[0]
        winner_tag = winner["tag"]
        accepted = (
            winner["_confidence_exact"]
            >= config.adjudication.confidence_threshold
        )

        def tie_reason(candidate: dict[str, Any]) -> str:
            for strategy in config.adjudication.strategy_order:
                candidate_contribution = (
                    config.adjudication.weights[strategy] * candidate["_score_exact"][strategy]
                )
                winner_contribution = (
                    config.adjudication.weights[strategy] * winner["_score_exact"][strategy]
                )
                if candidate_contribution != winner_contribution:
                    return f"lost_on_{strategy}"
            if candidate["tag"] > winner["tag"]:
                return "tie_lost_lexicographic_tag_order"
            return "not_selected"

        candidate_output: list[dict[str, Any]] = []
        for candidate in ordered:
            clean = {key: value for key, value in candidate.items() if not key.startswith("_")}
            if candidate["tag"] == winner_tag:
                clean["decision"] = "selected"
                clean["rejection_reasons"] = []
            else:
                reasons: list[str] = []
                if candidate["vote_count"] == 0:
                    reasons.append("zero_observed_votes")
                if candidate["confidence"] < winner["confidence"]:
                    reasons.append("lower_composite_confidence")
                reasons.append(tie_reason(candidate))
                clean["decision"] = "rejected"
                clean["rejection_reasons"] = reasons
            candidate_output.append(clean)

        label_diagnostics: list[dict[str, Any]] = []
        if len(item["observed_new_tags"]) > 1:
            label_diagnostics.append(
                {
                    "code": "mapping_conflict",
                    "message": "multiple standard tags were observed for one normalized old tag",
                    "observed_tags": sorted(item["observed_new_tags"]),
                    "selected": winner_tag,
                }
            )
        if not accepted:
            label_diagnostics.append(
                {
                    "code": "confidence_below_threshold",
                    "message": "winner is retained but marked unaccepted",
                    "confidence": winner["confidence"],
                    "threshold": config.adjudication.confidence_threshold,
                }
            )
        rejected = [candidate for candidate in candidate_output if candidate["decision"] == "rejected"]
        labels.append(
            {
                "old_tag": old_tag,
                "original_variants": sorted(item["variants"]),
                "mapping_status": "resolved",
                "resolved": {
                    "old_tag": old_tag,
                    "new_tag": winner_tag,
                    "confidence": winner["confidence"],
                    "accepted": accepted,
                    "threshold": config.adjudication.confidence_threshold,
                    "vote_count": winner["vote_count"],
                    "weighted_vote_count": winner["weighted_vote_count"],
                    "arbitration": {
                        "strategy_order": list(config.adjudication.strategy_order),
                        "tie_breaker": config.adjudication.tie_breaker,
                        "rejected_candidates": [
                            {
                                "tag": candidate["tag"],
                                "confidence": candidate["confidence"],
                                "reasons": candidate["rejection_reasons"],
                            }
                            for candidate in rejected
                        ],
                    },
                },
                "candidates": candidate_output,
                "diagnostics": label_diagnostics,
            }
        )

    resolved = [
        label["resolved"] for label in labels if label["mapping_status"] == "resolved"
    ]
    candidate_index = {label["old_tag"]: label["candidates"] for label in labels}
    return {
        "schema": "posalign.adjudication.v1",
        "governance": {
            "adjudication_fingerprint": config.adjudication_fingerprint,
            "strategy_order": list(config.adjudication.strategy_order),
            "weights": config.adjudication.weights,
            "candidate_priority": list(config.adjudication.candidate_priority),
            "confidence_threshold": config.adjudication.confidence_threshold,
            "annotator_reliability": config.adjudication.annotator_reliability,
        },
        "labels": labels,
        "resolved": resolved,
        "candidate_index": candidate_index,
        "diagnostics": diagnostics,
        "stats": {
            "labels": len(labels),
            "resolved": len(resolved),
            "unmapped": sum(1 for label in labels if label["mapping_status"] == "unmapped"),
            "low_confidence": sum(1 for label in labels if label.get("diagnostics") and any(d["code"] == "confidence_below_threshold" for d in label["diagnostics"])),
            "conflicts": sum(1 for label in labels if any(d["code"] == "mapping_conflict" for d in label.get("diagnostics", []))),
            "rejected_candidates": sum(
                len([c for c in label["candidates"] if c["decision"] == "rejected"])
                for label in labels
            ),
        },
    }

