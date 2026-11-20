"""Stage 1: deterministic text normalization and automatic scoring."""

from __future__ import annotations

from typing import Any, Callable

from .inputs import InputBundle
from .util import (
    SCORE_SCALE,
    digest_json,
    fixed_from_number,
    fixed_to_string,
    normalize_fixed_weights,
    normalize_text,
    token_overlap_micros,
    weighted_fixed_mean,
)


def _scoring_weights(config: dict[str, Any]) -> tuple[int, int]:
    scoring = config["scoring"]
    weights = normalize_fixed_weights(
        [
            fixed_from_number(scoring["exact_weight"]),
            fixed_from_number(scoring["overlap_weight"]),
        ]
    )
    return weights[0], weights[1]


def score_record(
    record: dict[str, Any],
    exact_weight: int,
    overlap_weight: int,
) -> dict[str, Any]:
    raw_output = record.get("output")
    raw_output = "" if raw_output is None else str(raw_output)
    raw_reference = record.get("reference_answer")
    normalized_output = normalize_text(raw_output)
    normalized_reference = normalize_text(raw_reference)
    reasons: list[str] = []
    if not normalized_output:
        reasons.append("empty_output")
    reference_missing = raw_reference is None or not normalized_reference
    if reference_missing:
        reasons.append("missing_reference_answer")

    applicable = not reference_missing
    exact_score = 0
    overlap_score = 0
    if applicable:
        exact_score = int(normalized_output == normalized_reference)
        overlap_score = token_overlap_micros(normalized_output, normalized_reference)
        if overlap_score == 0:
            reasons.append("zero_token_overlap")
        if exact_score:
            reasons.append("exact_match")
    composite = 0
    if applicable:
        composite = weighted_fixed_mean(
            [exact_score * SCORE_SCALE, overlap_score],
            [exact_weight, overlap_weight],
        )
    if composite == 0 and applicable and not reasons:
        reasons.append("zero_composite_score")

    return {
        "record_key": record["record_key"],
        "run_id": record["run_id"],
        "model_id": record["model_id"],
        "benchmark_id": record["benchmark_id"],
        "sample_id": record["sample_id"],
        "source_batch": record["source_batch"],
        "language": record["language"],
        "question_type": record["question_type"],
        "raw_output": raw_output,
        "raw_output_digest": digest_json(raw_output),
        "reference_answer": raw_reference,
        "reference_answer_digest": digest_json(raw_reference),
        "normalized_output": normalized_output,
        "normalized_reference": normalized_reference,
        "normalized_output_chars": len(normalized_output),
        "normalized_reference_chars": len(normalized_reference),
        "exact_match": exact_score,
        "token_overlap_micros": overlap_score,
        "score_micros": composite,
        "score": fixed_to_string(composite),
        "rule_applicable": applicable,
        "score_reasons": reasons,
    }


def run_stage1(
    bundle: InputBundle,
    config: dict[str, Any],
    dependency_fingerprint: str,
    *,
    on_unit: Callable[[], None] | None = None,
) -> dict[str, Any]:
    exact_weight, overlap_weight = _scoring_weights(config)
    scored: list[dict[str, Any]] = []
    for record in bundle.records:
        scored.append(score_record(record, exact_weight, overlap_weight))
        if on_unit is not None:
            on_unit()
    exact_matches = sum(record["exact_match"] for record in scored)
    empty_outputs = sum(
        "empty_output" in record["score_reasons"] for record in scored
    )
    missing_references = sum(
        "missing_reference_answer" in record["score_reasons"] for record in scored
    )
    zero_overlaps = sum(
        "zero_token_overlap" in record["score_reasons"] for record in scored
    )
    return {
        "schema_version": 1,
        "stage": 1,
        "stage_name": "normalize_and_score",
        "config_version": config["version"],
        "dependency_fingerprint": dependency_fingerprint,
        "scoring_weights_micros": {
            "exact": exact_weight,
            "overlap": overlap_weight,
        },
        "source_snapshot": bundle.source_snapshot,
        "samples": scored,
        "stats": {
            "record_count": len(scored),
            "exact_matches": exact_matches,
            "empty_outputs": empty_outputs,
            "missing_references": missing_references,
            "zero_overlaps": zero_overlaps,
        },
    }
