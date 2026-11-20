"""Stage 2: model-benchmark aggregation and deterministic exclusions."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable

import numpy as np

from .util import (
    digest_json,
    fixed_to_string,
    rounded_ratio,
)


def _exclusion_reasons(record: dict[str, Any], config: dict[str, Any]) -> list[str]:
    exclusions = config["exclusions"]
    reasons: list[str] = []
    if not record["rule_applicable"]:
        reasons.append("scoring_not_applicable")

    benchmark_languages = exclusions.get("benchmark_languages", {})
    expected_languages = benchmark_languages.get(record["benchmark_id"])
    if expected_languages is not None:
        if not record["language"]:
            reasons.append("missing_language")
        elif record["language"] not in expected_languages:
            reasons.append("language_mismatch")

    output_chars = record["normalized_output_chars"]
    if output_chars > exclusions["max_normalized_chars"]:
        reasons.append("output_length_above_limit")
    if output_chars < exclusions["min_normalized_chars"]:
        reasons.append("output_length_below_limit")

    allowed_question_types = exclusions.get("allowed_question_types", [])
    if allowed_question_types and record["question_type"] not in allowed_question_types:
        reasons.append("unsupported_question_type")
    if exclusions.get("exclude_empty_output") and "empty_output" in record["score_reasons"]:
        reasons.append("empty_output_excluded")
    return reasons


def _quantiles(score_micros: list[int]) -> dict[str, int] | None:
    if not score_micros:
        return None
    values = np.percentile(
        np.asarray(score_micros, dtype=np.int64),
        [10, 50, 90],
        method="nearest",
    )
    return {
        "p10": int(values[0]),
        "p50": int(values[1]),
        "p90": int(values[2]),
    }


def _quantile_strings(quantiles: dict[str, int] | None) -> dict[str, str | None]:
    if quantiles is None:
        return {"p10": None, "p50": None, "p90": None}
    return {key: fixed_to_string(value) for key, value in quantiles.items()}


def run_stage2(
    stage1: dict[str, Any],
    config: dict[str, Any],
    dependency_fingerprint: str,
    *,
    on_unit: Callable[[], None] | None = None,
) -> dict[str, Any]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in stage1["samples"]:
        grouped[(record["model_id"], record["benchmark_id"])].append(record)

    groups: list[dict[str, Any]] = []
    for (model_id, benchmark_id), records in sorted(grouped.items()):
        valid_records: list[dict[str, Any]] = []
        excluded_records: list[dict[str, Any]] = []
        for record in sorted(
            records,
            key=lambda item: (item["sample_id"], item["run_id"], item["model_id"]),
        ):
            reasons = _exclusion_reasons(record, config)
            if reasons:
                excluded_records.append(
                    {
                        "sample_id": record["sample_id"],
                        "run_id": record["run_id"],
                        "model_id": record["model_id"],
                        "benchmark_id": record["benchmark_id"],
                        "source_batch": record["source_batch"],
                        "reasons": reasons,
                        "score_reasons": list(record["score_reasons"]),
                    }
                )
                continue
            valid_records.append(
                {
                    "record_key": record["record_key"],
                    "sample_id": record["sample_id"],
                    "run_id": record["run_id"],
                    "model_id": record["model_id"],
                    "benchmark_id": record["benchmark_id"],
                    "source_batch": record["source_batch"],
                    "language": record["language"],
                    "question_type": record["question_type"],
                    "score_micros": record["score_micros"],
                    "normalized_output_chars": record["normalized_output_chars"],
                }
            )
        scores = [record["score_micros"] for record in valid_records]
        mean_micros = rounded_ratio(sum(scores), len(scores)) if scores else None
        quantiles = _quantiles(scores)
        groups.append(
            {
                "model_id": model_id,
                "benchmark_id": benchmark_id,
                "total_samples": len(records),
                "valid_samples": len(valid_records),
                "excluded_samples": len(excluded_records),
                "mean_score_micros": mean_micros,
                "mean_score": fixed_to_string(mean_micros),
                "quantiles_micros": quantiles,
                "quantiles": _quantile_strings(quantiles),
                "valid_records": valid_records,
                "exclusions": excluded_records,
            }
        )
        if on_unit is not None:
            on_unit()

    return {
        "schema_version": 1,
        "stage": 2,
        "stage_name": "aggregate_and_exclude",
        "config_version": config["version"],
        "dependency_fingerprint": dependency_fingerprint,
        "upstream_stage1_digest": digest_json(stage1),
        "groups": groups,
        "stats": {
            "group_count": len(groups),
            "total_samples": sum(group["total_samples"] for group in groups),
            "valid_samples": sum(group["valid_samples"] for group in groups),
            "excluded_samples": sum(group["excluded_samples"] for group in groups),
        },
    }
