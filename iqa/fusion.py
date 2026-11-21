"""Stage 4: weighted fusion, grade decision and expected-range comparison."""

import math

from .config import DIMENSIONS
from .errors import BusinessError, E_NUMERIC, E_INVALID_CONFIG
from .hashing import digest, round_floats

STAGE = "stage4_fusion"
SEMANTIC_SOURCE = "semantic"


def _dimension_scores(metrics_record, semantic_record):
    scores = {
        "sharpness": metrics_record["product"]["sharpness"],
        "color": metrics_record["product"]["color"],
        "structure": metrics_record["product"]["structure"],
        "semantic": semantic_record["product"],
    }
    return scores


def fuse(metrics_record, semantic_record, config):
    config.validate()
    image_id = metrics_record["image_id"]
    raw_scores = _dimension_scores(metrics_record, semantic_record)

    available = {}
    skipped = {}
    for dimension in DIMENSIONS:
        entry = raw_scores[dimension]
        score = entry["score"]
        if score is None:
            skipped[dimension] = entry.get("skipped_reason") or "dimension unavailable"
        else:
            available[dimension] = float(score)

    raw_weights = {dimension: float(config.weights[dimension]) for dimension in DIMENSIONS}
    for dimension, weight in raw_weights.items():
        if weight < 0:
            raise BusinessError(
                E_INVALID_CONFIG,
                f"negative weight for dimension {dimension}",
                {"image_id": image_id, "dimension": dimension, "weight": weight},
            )

    active_weight_sum = sum(raw_weights[dimension] for dimension in available)
    if active_weight_sum <= 0:
        raise BusinessError(
            E_NUMERIC,
            "sum of weights over available dimensions is zero",
            {"image_id": image_id, "dimensions": sorted(available.keys())},
        )
    if all(score == 0.0 for score in available.values()):
        raise BusinessError(
            E_NUMERIC,
            "all available dimension scores are zero",
            {"image_id": image_id, "dimensions": sorted(available.keys())},
        )

    renorm = {
        dimension: raw_weights[dimension] / active_weight_sum for dimension in available
    }
    contributions = {}
    if config.fusion_domain == "log":
        for dimension, score in available.items():
            contributions[dimension] = renorm[dimension] * math.log(
                max(score, config.epsilon)
            )
        final_score = float(math.exp(sum(contributions.values())))
    else:
        for dimension, score in available.items():
            contributions[dimension] = renorm[dimension] * score
        final_score = float(sum(contributions.values()))
    final_score = float(min(max(final_score, 0.0), 1.0))

    thresholds = config.thresholds
    if final_score >= thresholds["excellent"]:
        grade = "excellent"
    elif final_score >= thresholds["good"]:
        grade = "good"
    else:
        grade = "poor"

    # Deterministic tie handling: highest score wins, fixed dimension order
    # breaks equal scores; semantic is never used as the ranking tiebreaker.
    ranking_order = [dimension for dimension in DIMENSIONS if dimension in available]
    top_dimension = max(
        ranking_order,
        key=lambda dimension: (available[dimension], -ranking_order.index(dimension)),
    )

    product = {
        "image_id": image_id,
        "final_score": final_score,
        "grade": grade,
        "available_dimensions": sorted(available.keys()),
        "skipped_dimensions": skipped,
        "raw_scores": {dimension: raw_scores[dimension] for dimension in DIMENSIONS},
        "raw_weights": raw_weights,
        "active_weight_sum": active_weight_sum,
        "renormalized_weights": renorm,
        "contributions": contributions,
        "contribution_domain": config.fusion_domain,
        "fusion_domain": config.fusion_domain,
        "tie_break_rule": "highest score, then fixed dimension order; semantic never breaks ties",
        "top_dimension": top_dimension,
    }
    record = {
        "stage": STAGE,
        "image_id": image_id,
        "input_fingerprint": digest(
            {
                "metrics": metrics_record["fingerprint"],
                "semantic": semantic_record["fingerprint"],
                "config_version": config.version(),
            }
        ),
        "product": product,
    }
    product = round_floats(product)
    record["product"] = product
    record["product_digest"] = digest(product)
    record["fingerprint"] = digest(
        {"input": record["input_fingerprint"], "product": record["product_digest"]}
    )
    return record


def compare_expected(record, metadata):
    """Compare the final score with the declared expected range."""
    low = float(metadata["expected_range"][0])
    high = float(metadata["expected_range"][1])
    score = record["product"]["final_score"]
    if low > high:
        raise BusinessError(
            E_INVALID_CONFIG,
            "expected_range lower bound exceeds upper bound",
            {"image_id": metadata["image_id"], "expected_range": [low, high]},
        )
    within = low <= score <= high
    deviation = 0.0 if within else (low - score if score < low else score - high)
    return {
        "image_id": metadata["image_id"],
        "expected_range": [low, high],
        "final_score": score,
        "within_range": bool(within),
        "deviation": float(abs(deviation)),
    }
