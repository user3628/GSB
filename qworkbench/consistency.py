from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Callable, Mapping, Sequence

import numpy as np

from .config import ConsistencyConfig


def _average_ranks(values: np.ndarray) -> np.ndarray:
    order = np.argsort(values, kind="mergesort")
    ranks = np.empty(len(values), dtype=np.float64)
    index = 0
    while index < len(order):
        end = index + 1
        while end < len(order) and values[order[end]] == values[order[index]]:
            end += 1
        average_rank = (index + 1 + end) / 2.0
        ranks[order[index:end]] = average_rank
        index = end
    return ranks


def spearman_rank_correlation(left: Sequence[float], right: Sequence[float]) -> dict[str, Any]:
    if len(left) != len(right):
        raise ValueError("Spearman inputs must have equal length")
    if len(left) < 2:
        return {"value": None, "reason": "fewer_than_two_annotated_samples", "n": len(left)}
    x = np.asarray(left, dtype=np.float64)
    y = np.asarray(right, dtype=np.float64)
    if np.all(x == x[0]) or np.all(y == y[0]):
        return {"value": None, "reason": "constant_rank_vector", "n": len(left)}
    rx = _average_ranks(x)
    ry = _average_ranks(y)
    rx_centered = rx - float(rx.mean())
    ry_centered = ry - float(ry.mean())
    denominator = float(np.linalg.norm(rx_centered) * np.linalg.norm(ry_centered))
    if denominator == 0.0:
        return {"value": None, "reason": "zero_rank_variance", "n": len(left)}
    value = float(np.dot(rx_centered, ry_centered) / denominator)
    return {"value": round(max(-1.0, min(1.0, value)), 12), "reason": None, "n": len(left)}


def _bucket_index(value: float, buckets: int) -> int:
    return min(buckets - 1, max(0, int(math.floor(value * buckets))))


def validate_consistency(
    selected_records: Sequence[Mapping[str, Any]],
    annotations: Mapping[str, float],
    config: ConsistencyConfig,
    source_weights: Mapping[str, float],
    stop_checker: Callable[[], None] | None = None,
) -> dict[str, Any]:
    check = stop_checker or (lambda: None)
    enriched: list[dict[str, Any]] = []
    paired: list[dict[str, Any]] = []
    invalid_annotations: list[dict[str, Any]] = []
    unannotated: list[str] = []
    for record in selected_records:
        check()
        item = dict(record)
        sample_id = str(record["id"])
        annotation = annotations.get(sample_id)
        if annotation is None:
            item["annotation_score"] = None
            item["annotation_normalized"] = None
            item["quality_annotation_delta"] = None
            item["consistency_status"] = "UNANNOTATED"
            unannotated.append(sample_id)
            enriched.append(item)
            continue
        if not config.annotation_min <= float(annotation) <= config.annotation_max:
            item["annotation_score"] = float(annotation)
            item["annotation_normalized"] = None
            item["quality_annotation_delta"] = None
            item["consistency_status"] = "INVALID_ANNOTATION_RANGE"
            invalid_annotations.append(
                {
                    "id": sample_id,
                    "source": record["source"],
                    "annotation": float(annotation),
                    "allowed": [config.annotation_min, config.annotation_max],
                }
            )
            enriched.append(item)
            continue
        normalized = (float(annotation) - config.annotation_min) / (
            config.annotation_max - config.annotation_min
        )
        quality = float(record["quality_score"])
        delta = abs(quality - normalized)
        status = "OUTLIER" if delta > config.sample_outlier_threshold else "WITHIN_THRESHOLD"
        item["annotation_score"] = float(annotation)
        item["annotation_normalized"] = normalized
        item["quality_annotation_delta"] = delta
        item["consistency_status"] = status
        enriched.append(item)
        paired.append(
            {
                "id": sample_id,
                "source": str(record["source"]),
                "lang": str(record["lang"]),
                "quality": quality,
                "annotation": normalized,
                "delta": delta,
                "signed_delta": quality - normalized,
            }
        )

    quality_buckets: list[dict[str, Any]] = []
    annotation_buckets: list[dict[str, Any]] = []
    bucket_members: dict[int, list[dict[str, Any]]] = defaultdict(list)
    annotation_histogram = [0] * config.buckets
    for pair in paired:
        bucket_members[_bucket_index(pair["quality"], config.buckets)].append(pair)
        annotation_histogram[_bucket_index(pair["annotation"], config.buckets)] += 1
    paired_count = len(paired)
    weighted_abs_bias = 0.0
    for bucket in range(config.buckets):
        members = bucket_members.get(bucket, [])
        if members:
            mean_quality = sum(member["quality"] for member in members) / len(members)
            mean_annotation = sum(member["annotation"] for member in members) / len(members)
            mean_delta = sum(member["delta"] for member in members) / len(members)
        else:
            mean_quality = None
            mean_annotation = None
            mean_delta = None
        signed_bias = (
            mean_annotation - mean_quality
            if mean_quality is not None and mean_annotation is not None
            else None
        )
        count = len(members)
        if signed_bias is not None:
            weighted_abs_bias += count / paired_count * abs(signed_bias)
        quality_buckets.append(
            {
                "bucket": bucket,
                "range": [bucket / config.buckets, (bucket + 1) / config.buckets],
                "count": count,
                "fraction": count / paired_count if paired_count else 0.0,
                "mean_quality": mean_quality,
                "mean_annotation": mean_annotation,
                "mean_abs_delta": mean_delta,
                "signed_bias_annotation_minus_quality": signed_bias,
            }
        )
        annotation_buckets.append(
            {
                "bucket": bucket,
                "range": [bucket / config.buckets, (bucket + 1) / config.buckets],
                "count": annotation_histogram[bucket],
                "fraction": annotation_histogram[bucket] / paired_count if paired_count else 0.0,
            }
        )
    distribution_l1 = sum(
        abs(quality_buckets[index]["fraction"] - annotation_buckets[index]["fraction"])
        for index in range(config.buckets)
    ) / 2.0
    max_bucket_bias = max(
        (
            abs(bucket["signed_bias_annotation_minus_quality"])
            for bucket in quality_buckets
            if bucket["signed_bias_annotation_minus_quality"] is not None
        ),
        default=0.0,
    )

    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for pair in paired:
        by_source[pair["source"]].append(pair)
    source_review: dict[str, dict[str, Any]] = {}
    for source in sorted(set(str(record["source"]) for record in selected_records)):
        members = by_source.get(source, [])
        if not members:
            source_review[source] = {
                "status": "INSUFFICIENT_ANNOTATIONS",
                "annotated_count": 0,
                "current_target_weight": source_weights.get(source),
                "recommendation": "Collect annotations before changing the source weight.",
            }
            continue
        mean_abs_delta = sum(pair["delta"] for pair in members) / len(members)
        signed_bias = sum(pair["signed_delta"] for pair in members) / len(members)
        outlier_count = sum(1 for pair in members if pair["delta"] > config.sample_outlier_threshold)
        needs_review = (
            len(members) >= config.min_annotated_for_review
            and mean_abs_delta > config.source_bias_threshold
        )
        if needs_review:
            status = "NEEDS_REVIEW"
            recommendation = (
                "Freeze the current source weight, inspect its bucket deviations, signed bias, outliers, "
                "and template families, then either recalibrate annotations or revise the source prior. "
                "Keep all samples in the diagnostic mixture until review completes."
            )
        elif len(members) < config.min_annotated_for_review:
            status = "INSUFFICIENT_ANNOTATIONS"
            recommendation = "Add more annotations before reviewing this source weight."
        else:
            status = "OK"
            recommendation = "No weight change indicated by the current annotation slice."
        source_review[source] = {
            "status": status,
            "weight_status": "PENDING_REVIEW" if needs_review else status,
            "annotated_count": len(members),
            "current_target_weight": source_weights.get(source),
            "mean_abs_delta": mean_abs_delta,
            "signed_bias_quality_minus_annotation": signed_bias,
            "outlier_count": outlier_count,
            "threshold": config.source_bias_threshold,
            "recommendation": recommendation,
        }

    spearman = spearman_rank_correlation(
        [pair["quality"] for pair in paired],
        [pair["annotation"] for pair in paired],
    )
    outliers = sorted(
        (
            {
                "id": pair["id"],
                "source": pair["source"],
                "lang": pair["lang"],
                "quality_score": pair["quality"],
                "annotation_normalized": pair["annotation"],
                "absolute_delta": pair["delta"],
                "threshold": config.sample_outlier_threshold,
            }
            for pair in paired
            if pair["delta"] > config.sample_outlier_threshold
        ),
        key=lambda item: item["id"],
    )
    review_sources = sorted(
        source for source, review in source_review.items() if review["status"] == "NEEDS_REVIEW"
    )
    summary = {
        "mixture_samples": len(enriched),
        "annotated_samples": paired_count,
        "annotation_coverage": paired_count / len(enriched) if enriched else 0.0,
        "unannotated_samples": len(unannotated),
        "invalid_annotations": len(invalid_annotations),
        "bucket_weighted_abs_bias": weighted_abs_bias,
        "max_bucket_abs_bias": max_bucket_bias,
        "bucket_distribution_l1": distribution_l1,
        "spearman": spearman,
        "outlier_count": len(outliers),
        "review_required": bool(review_sources) or weighted_abs_bias > config.source_bias_threshold,
        "review_sources": review_sources,
        "source_review": source_review,
        "policy": (
            "Deviations never cause silent deletion. Outliers are listed, source weights are marked "
            "PENDING_REVIEW, and the diagnostic mixture retains every sampled record."
        ),
    }
    report = {
        "summary": summary,
        "quality_buckets": quality_buckets,
        "annotation_buckets": annotation_buckets,
        "outliers": outliers,
        "invalid_annotations": invalid_annotations,
        "unannotated_ids": sorted(unannotated),
        "spearman": spearman,
        "source_review": source_review,
    }
    return {"records": enriched, "summary": summary, "report": report}
