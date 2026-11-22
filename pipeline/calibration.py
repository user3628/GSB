from __future__ import annotations

import bisect
import math
from typing import Any

from .errors import (
    E_CALIBRATION_LOW_SAMPLES,
    E_EMPTY_CLASS,
    E_FPR_UNREACHABLE,
)
from .util import canonical_digest, stable_float


def _threshold_false_positives(sorted_negative_scores: list[float], threshold: float) -> int:
    return len(sorted_negative_scores) - bisect.bisect_left(sorted_negative_scores, threshold)


def _threshold_hits(sorted_positive_scores: list[float], threshold: float) -> int:
    return len(sorted_positive_scores) - bisect.bisect_left(sorted_positive_scores, threshold)


def choose_threshold(
    positive_scores: list[float],
    negative_scores: list[float],
    threshold_floor: float,
    threshold_max: float,
    target_fpr: float,
) -> dict[str, Any]:
    sorted_negative = sorted(negative_scores)
    sorted_positive = sorted(positive_scores)
    candidates: set[float] = {float(threshold_floor), float(threshold_max)}
    for score in sorted_negative:
        if threshold_floor < score < threshold_max:
            candidates.add(math.nextafter(score, math.inf))
    ordered = sorted(candidate for candidate in candidates if threshold_floor <= candidate <= threshold_max)
    selected: float | None = None
    for candidate in ordered:
        false_positives = _threshold_false_positives(sorted_negative, candidate)
        allowed = target_fpr * len(sorted_negative)
        if false_positives <= allowed:
            selected = candidate
            break
    reachable = selected is not None
    threshold = selected if selected is not None else float(threshold_max)
    false_positives = _threshold_false_positives(sorted_negative, threshold)
    hits = _threshold_hits(sorted_positive, threshold)
    negative_count = len(sorted_negative)
    positive_count = len(sorted_positive)
    return {
        "threshold": stable_float(threshold),
        "threshold_floor": stable_float(float(threshold_floor)),
        "threshold_max": stable_float(float(threshold_max)),
        "target_fpr": stable_float(float(target_fpr)),
        "hit_count": hits,
        "hit_rate": (hits / positive_count) if positive_count else 0.0,
        "false_positive_count": false_positives,
        "false_positive_rate": (false_positives / negative_count) if negative_count else 0.0,
        "target_met": reachable,
        "positive_sample_count": positive_count,
        "negative_sample_count": negative_count,
    }


def calibrate(
    batch_id: str,
    labels: tuple[str, ...],
    documents: dict[str, dict[str, Any]],
    holdout_features: list[dict[str, Any]],
    holdout_scores: list[dict[str, Any]],
    train_doc_ids: set[str],
    holdout_doc_ids: set[str],
    thresholds: dict[str, float],
    target_fpr: float,
    threshold_max: float,
    min_samples: int,
    fold_modulus: int,
    fold_remainder: int,
    batch_label_counts: dict[str, int],
) -> dict[str, Any]:
    score_by_doc = {row["doc_id"]: row for row in holdout_scores}
    holdout_rows: list[dict[str, Any]] = []
    for feature in sorted(holdout_features, key=lambda row: row["doc_id"]):
        doc_id = feature["doc_id"]
        score_row = score_by_doc[doc_id]
        true_label = documents[doc_id].get("label")
        holdout_rows.append(
            {
                "doc_id": doc_id,
                "true_label": true_label,
                "probabilities": score_row["probabilities"],
                "score_error_codes": score_row["score_error_codes"],
            }
        )
    by_label: dict[str, Any] = {}
    for label in labels:
        positive: list[tuple[str, float]] = []
        negative: list[tuple[str, float]] = []
        for row in holdout_rows:
            if row["score_error_codes"]:
                continue
            score = row["probabilities"][label]
            if row["true_label"] == label:
                positive.append((row["doc_id"], score))
            else:
                negative.append((row["doc_id"], score))
        positive.sort(key=lambda item: item[0])
        negative.sort(key=lambda item: item[0])
        choice = choose_threshold(
            [score for _doc_id, score in positive],
            [score for _doc_id, score in negative],
            thresholds[label],
            threshold_max,
            target_fpr,
        )
        review_reasons: list[str] = []
        if batch_label_counts.get(label, 0) == 0:
            review_reasons.append(E_EMPTY_CLASS)
        if len(positive) < min_samples or len(negative) < min_samples:
            review_reasons.append(E_CALIBRATION_LOW_SAMPLES)
        if not choice["target_met"]:
            review_reasons.append(E_FPR_UNREACHABLE)
        sample_ids = sorted(row["doc_id"] for row in holdout_rows)
        by_label[label] = {
            **choice,
            "review_required": bool(review_reasons),
            "review_reasons": review_reasons,
            "samples": {
                "positive": [{"doc_id": doc_id, "score": score} for doc_id, score in positive],
                "negative": [{"doc_id": doc_id, "score": score} for doc_id, score in negative],
                "holdout_doc_ids": sample_ids,
                "range": (
                    {"first_doc_id": sample_ids[0], "last_doc_id": sample_ids[-1], "count": len(sample_ids)}
                    if sample_ids
                    else {"first_doc_id": None, "last_doc_id": None, "count": 0}
                ),
                "positive_ids_digest": canonical_digest([doc_id for doc_id, _score in positive]),
                "negative_ids_digest": canonical_digest([doc_id for doc_id, _score in negative]),
            },
        }
    error_counts: dict[str, int] = {}
    for row in holdout_rows:
        for code in row["score_error_codes"]:
            error_counts[code] = error_counts.get(code, 0) + 1
    result = {
        "schema": "threshold-calibration-v1",
        "batch_id": batch_id,
        "labels": list(labels),
        "target_fpr": target_fpr,
        "threshold_max": threshold_max,
        "min_samples_per_class": min_samples,
        "fold": {
            "modulus": fold_modulus,
            "remainder": fold_remainder,
            "train_doc_count": len(train_doc_ids),
            "holdout_doc_count": len(holdout_doc_ids),
            "train_ids_digest": canonical_digest(sorted(train_doc_ids)),
            "holdout_ids_digest": canonical_digest(sorted(holdout_doc_ids)),
        },
        "holdout_scores": holdout_rows,
        "classes": by_label,
        "holdout_score_error_counts": {key: error_counts[key] for key in sorted(error_counts)},
    }
    result["calibration_digest"] = canonical_digest(result)
    return result
