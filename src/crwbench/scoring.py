"""Stage 3: log-domain category scoring and 0-3 level mapping."""
from __future__ import annotations

from .config import CATEGORY_KEYS, Config
from .features import UNKNOWN_KEYWORD_TABLE

NEG_INF = -1.0e12


def _log_sum_exp(values: list[float]) -> float:
    if not values:
        return NEG_INF
    head = values[0]
    for value in values[1:]:
        if head >= value:
            head = head + _log1p_exp(value - head)
        else:
            head = value + _log1p_exp(head - value)
    return head


def _log1p_exp(diff: float) -> float:
    import math

    if diff < -30:
        return 0.0
    return math.log1p(math.exp(diff))


def map_level(score: float, thresholds: list[float], default_level: int) -> int:
    level = default_level
    for idx, threshold in enumerate(thresholds):
        if score >= threshold:
            level = idx + 1
    return level


def score_clause(feature: dict, config: Config) -> dict:
    positive: dict[str, list[float]] = {k: [] for k in CATEGORY_KEYS}
    negative: dict[str, list[float]] = {k: [] for k in CATEGORY_KEYS}
    for hit in feature["hits"]:
        import math

        contribution = math.log(hit["weight"]) + math.log(hit["multiplier"])
        bucket = negative if hit["negated"] else positive
        bucket[hit["category"]].append(contribution)

    unknown_set = {u["category"] for u in feature["unknown_categories"]}
    category_scores = []
    for category in CATEGORY_KEYS:
        pos_score = _log_sum_exp(positive[category])
        neg_score = _log_sum_exp(negative[category])
        if category in unknown_set:
            score = 0.0
            reason = UNKNOWN_KEYWORD_TABLE
        elif pos_score == NEG_INF and neg_score == NEG_INF:
            score = 0.0
            reason = "no_keyword_hit"
        else:
            # Negative polarity cancels positive evidence in the log domain.
            score = pos_score - (0.0 if neg_score == NEG_INF else neg_score)
            reason = "log_sum_exp_positive_minus_negative"
        category_scores.append(
            {
                "category": category,
                "score": round(score, 6),
                "positive_log_sum": round(0.0 if pos_score == NEG_INF else pos_score, 6),
                "negative_log_sum": round(0.0 if neg_score == NEG_INF else neg_score, 6),
                "basis": reason,
            }
        )

    ordered = sorted(category_scores, key=lambda c: (-c["score"], c["category"]))
    top = ordered[0]
    thresholds = config.level_thresholds
    below = all(c["score"] < thresholds[0] for c in category_scores)
    if below or feature["status"] != "ok":
        level = int(config.scoring["default_level"])
        reason = (
            config.scoring["default_reason"]
            if feature["status"] == "ok"
            else "empty_text_default_low_risk"
        )
    else:
        level = map_level(top["score"], thresholds, int(config.scoring["default_level"]))
        reason = "top_category_score_threshold_mapping"
    # Direction invariant: a higher clause score can never map to a lower level.
    assert map_level(top["score"], thresholds, 0) >= map_level(
        top["score"] - 1.0, thresholds, 0
    )
    return {
        "clause_id": feature["clause_id"],
        "contract_id": feature.get("contract_id", ""),
        "human_level": feature.get("human_level"),
        "status": feature["status"],
        "clause_score": round(top["score"], 6),
        "level": level,
        "dominant_category": top["category"],
        "level_basis": reason,
        "category_scores": category_scores,
        "diagnostics": list(feature["diagnostics"]),
    }


def run(features: list[dict], config: Config) -> list[dict]:
    return [score_clause(f, config) for f in features]
