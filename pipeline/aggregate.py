from __future__ import annotations

from typing import Any, Iterable

from .errors import E_THRESHOLD_REJECTED
from .util import canonical_digest, parse_fixed_decimal


WEIGHT_SCALE = 1_000_000_000


def _ratio(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def _metrics_from_counts(tp: int, fp: int, fn: int, weight_units: int) -> dict[str, Any]:
    precision_numerator = weight_units * tp
    precision_denominator = weight_units * (tp + fp)
    recall_numerator = weight_units * tp
    recall_denominator = weight_units * (tp + fn)
    f1_numerator = 2 * weight_units * tp
    f1_denominator = weight_units * (2 * tp + fp + fn)
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "support": tp + fn,
        "predicted_positive": tp + fp,
        "precision": _ratio(precision_numerator, precision_denominator),
        "recall": _ratio(recall_numerator, recall_denominator),
        "f1": _ratio(f1_numerator, f1_denominator),
        "_weighted_numerators": {
            "precision": precision_numerator,
            "precision_denominator": precision_denominator,
            "recall": recall_numerator,
            "recall_denominator": recall_denominator,
            "f1": f1_numerator,
            "f1_denominator": f1_denominator,
        },
    }


def _aggregate_scope(
    results: Iterable[dict[str, Any]],
    labels: tuple[str, ...],
    weight_units: dict[str, int],
) -> dict[str, Any]:
    counts = {label: {"tp": 0, "fp": 0, "fn": 0} for label in labels}
    eligible = 0
    errors: dict[str, int] = {}
    rejected = 0
    review_required = 0
    predicted_count = 0
    correct = 0
    for row in results:
        if row.get("error_code"):
            errors[row["error_code"]] = errors.get(row["error_code"], 0) + 1
            continue
        if not row.get("metric_eligible", True):
            continue
        eligible += 1
        if row.get("review_required"):
            review_required += 1
        predicted = row.get("predicted_label")
        true_label = row.get("true_label")
        if predicted is None:
            rejected += 1
            if true_label in counts:
                counts[true_label]["fn"] += 1
            continue
        predicted_count += 1
        if predicted == true_label:
            correct += 1
            counts[predicted]["tp"] += 1
        else:
            counts[predicted]["fp"] += 1
            if true_label in counts:
                counts[true_label]["fn"] += 1
    class_metrics: dict[str, Any] = {}
    weighted = {
        "precision_numerator": 0,
        "precision_denominator": 0,
        "recall_numerator": 0,
        "recall_denominator": 0,
        "f1_numerator": 0,
        "f1_denominator": 0,
    }
    for label in labels:
        metric = _metrics_from_counts(
            counts[label]["tp"], counts[label]["fp"], counts[label]["fn"], weight_units[label]
        )
        numerators = metric.pop("_weighted_numerators")
        weighted["precision_numerator"] += numerators["precision"]
        weighted["precision_denominator"] += numerators["precision_denominator"]
        weighted["recall_numerator"] += numerators["recall"]
        weighted["recall_denominator"] += numerators["recall_denominator"]
        weighted["f1_numerator"] += numerators["f1"]
        weighted["f1_denominator"] += numerators["f1_denominator"]
        metric["class_weight"] = weight_units[label] / WEIGHT_SCALE
        metric["class_weight_units"] = weight_units[label]
        class_metrics[label] = metric
    return {
        "sample_count": eligible,
        "predicted_count": predicted_count,
        "threshold_rejected_count": rejected,
        "review_required_count": review_required,
        "correct_count": correct,
        "accuracy": (correct / eligible) if eligible else None,
        "error_counts": {key: errors[key] for key in sorted(errors)},
        "classes": class_metrics,
        "weighted": {
            "scale": WEIGHT_SCALE,
            "precision_numerator": weighted["precision_numerator"],
            "precision_denominator": weighted["precision_denominator"],
            "recall_numerator": weighted["recall_numerator"],
            "recall_denominator": weighted["recall_denominator"],
            "f1_numerator": weighted["f1_numerator"],
            "f1_denominator": weighted["f1_denominator"],
            "precision": _ratio(weighted["precision_numerator"], weighted["precision_denominator"]),
            "recall": _ratio(weighted["recall_numerator"], weighted["recall_denominator"]),
            "f1": _ratio(weighted["f1_numerator"], weighted["f1_denominator"]),
        },
    }


def summarize_results(
    results: Iterable[dict[str, Any]],
    labels: tuple[str, ...],
    languages: tuple[str, ...],
    channels: tuple[str, ...],
    class_weights: dict[str, float],
) -> dict[str, Any]:
    sorted_rows = sorted(results, key=lambda row: row["doc_id"])
    weight_units = {label: parse_fixed_decimal(str(class_weights[label]), WEIGHT_SCALE) for label in labels}
    overall = _aggregate_scope(sorted_rows, labels, weight_units)
    by_language: dict[str, Any] = {}
    for language in languages:
        rows = [row for row in sorted_rows if row.get("language") == language]
        by_language[language] = _aggregate_scope(rows, labels, weight_units)
    by_channel: dict[str, Any] = {}
    for channel in channels:
        rows = [row for row in sorted_rows if row.get("channel") == channel]
        by_channel[channel] = _aggregate_scope(rows, labels, weight_units)
    combined: dict[str, Any] = {}
    for language in languages:
        for channel in channels:
            key = f"{language}|{channel}"
            rows = [
                row
                for row in sorted_rows
                if row.get("language") == language and row.get("channel") == channel
            ]
            combined[key] = _aggregate_scope(rows, labels, weight_units)
    summary = {
        "schema": "weighted-stratified-summary-v1",
        "aggregation": {
            "method": "integer_counts_and_fixed_point_weights",
            "weight_scale": WEIGHT_SCALE,
            "sort_key": "doc_id",
            "doc_count": len(sorted_rows),
        },
        "labels": list(labels),
        "languages": list(languages),
        "channels": list(channels),
        "overall": overall,
        "by_language": by_language,
        "by_channel": by_channel,
        "by_language_channel": combined,
        "first_doc_id": sorted_rows[0]["doc_id"] if sorted_rows else None,
        "last_doc_id": sorted_rows[-1]["doc_id"] if sorted_rows else None,
    }
    summary["results_digest"] = canonical_digest(sorted_rows)
    summary["summary_digest"] = canonical_digest(summary)
    return summary


def reject_result(
    document: dict[str, Any],
    error_code: str,
    calibration_ref: dict[str, Any],
    score_ref: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "batch_id": document["batch_id"],
        "doc_id": document["doc_id"],
        "true_label": document.get("label"),
        "language": document.get("lang"),
        "channel": document.get("channel"),
        "risk_level": document.get("risk_level"),
        "error_code": error_code,
        "predicted_label": None,
        "decision": "ERROR",
        "decision_reasons": [error_code],
        "review_required": False,
        "metric_eligible": False,
        "probabilities": {},
        "log_scores": {},
        "calibration_ref": calibration_ref,
        "score_evidence": score_ref,
    }


def threshold_reject_result(
    document: dict[str, Any],
    score_row: dict[str, Any],
    calibration: dict[str, Any],
    calibration_ref: dict[str, Any],
    score_ref: dict[str, Any],
) -> dict[str, Any]:
    class_decisions = {
        label: {
            "probability": score_row["probabilities"][label],
            "threshold": calibration["classes"][label]["threshold"],
            "accepted": score_row["probabilities"][label] >= calibration["classes"][label]["threshold"],
            "calibration_review_required": calibration["classes"][label]["review_required"],
        }
        for label in sorted(calibration["classes"])
    }
    return {
        "batch_id": document["batch_id"],
        "doc_id": document["doc_id"],
        "true_label": document.get("label"),
        "language": document.get("lang"),
        "channel": document.get("channel"),
        "risk_level": document.get("risk_level"),
        "error_code": None,
        "predicted_label": None,
        "decision": "REJECT",
        "decision_reasons": [E_THRESHOLD_REJECTED],
        "review_required": False,
        "metric_eligible": True,
        "probabilities": score_row["probabilities"],
        "log_scores": score_row["log_scores"],
        "class_decisions": class_decisions,
        "calibration_ref": calibration_ref,
        "score_evidence": score_ref,
    }
