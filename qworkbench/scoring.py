from __future__ import annotations

import math
from collections import Counter, defaultdict
from typing import Any, Callable, Mapping, Sequence

from .config import FACTOR_NAMES, ScoringConfig, WorkbenchConfig
from .numeric import aggregate_quality_scores, weighted_geometric_fixed
from .util import text_tokens


def _factor_length(record: Mapping[str, Any], config: ScoringConfig) -> tuple[float, str | None]:
    token_count = int(record["whitespace_output_tokens"])
    if token_count == 0:
        return 0.0, "empty_output"
    target = float(config.target_tokens)
    long_limit = target * config.long_output_multiplier
    if token_count < target:
        return max(0.0, token_count / target), "output_below_target_length"
    if token_count > long_limit:
        return max(0.0, long_limit / token_count), "output_above_long_limit"
    return 1.0, None


def _factor_overlap(record: Mapping[str, Any]) -> tuple[float, str | None]:
    instruction_tokens = set(text_tokens(record["instruction"]))
    output_tokens = set(text_tokens(record["output"]))
    if not instruction_tokens:
        return 0.0, "empty_instruction_tokens"
    if not output_tokens:
        return 0.0, "empty_output_tokens"
    union = instruction_tokens | output_tokens
    if not union:
        return 0.0, "empty_token_union"
    overlap = len(instruction_tokens & output_tokens) / len(union)
    return overlap, None if overlap > 0.0 else "no_lexical_overlap"


def _factor_diversity(record: Mapping[str, Any], template_counts: Counter[str]) -> tuple[float, str | None]:
    instruction = record["instruction"]
    if not instruction:
        return 0.0, "empty_instruction"
    count = template_counts[instruction]
    return 1.0 / math.sqrt(count), None


def raw_quality_factors(
    record: Mapping[str, Any],
    config: ScoringConfig,
    template_counts: Counter[str],
) -> tuple[dict[str, float], dict[str, str], dict[str, str]]:
    factors: dict[str, float] = {}
    reasons: dict[str, str] = {}
    notes: dict[str, str] = {}
    length, length_reason = _factor_length(record, config)
    overlap, overlap_reason = _factor_overlap(record)
    diversity, diversity_reason = _factor_diversity(record, template_counts)
    factors["length_penalty"] = length
    factors["lexical_overlap"] = overlap
    factors["template_diversity"] = diversity
    if length_reason:
        reasons["length_penalty"] = length_reason
    if overlap_reason:
        reasons["lexical_overlap"] = overlap_reason
    if diversity_reason:
        reasons["template_diversity"] = diversity_reason
    source = record["source"]
    if source in config.source_priors:
        factors["source_prior"] = float(config.source_priors[source])
    else:
        factors["source_prior"] = float(config.unknown_source_prior)
        notes["source_prior"] = f"unknown_source:{source}; used unknown_source_prior"
    return factors, reasons, notes


def score_records(
    records: Sequence[Mapping[str, Any]],
    config: WorkbenchConfig,
    stop_checker: Callable[[], None] | None = None,
) -> dict[str, Any]:
    check = stop_checker or (lambda: None)
    ordered = sorted(records, key=lambda record: str(record["id"]))
    template_counts = Counter(record["instruction"] for record in ordered)
    weights = {name: float(config.scoring.weights[name]) for name in FACTOR_NAMES}
    scored: list[dict[str, Any]] = []
    fallback_counter: Counter[str] = Counter()
    source_stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"count": 0, "score_sum": 0.0, "fallback_count": 0}
    )
    for record in ordered:
        factors, reasons, notes = raw_quality_factors(record, config.scoring, template_counts)
        result = weighted_geometric_fixed(
            sample_id=str(record["id"]),
            factors=factors,
            weights=weights,
            reasons=reasons,
            quant=config.quantization,
        )
        for fallback in result["fallback_reasons"]:
            fallback_counter[f"{fallback['factor']}:{fallback['reason']}"] += 1
        score_record = {
            "id": record["id"],
            "sample_id": result["sample_id"],
            "source": record["source"],
            "lang": record["lang"],
            "instruction": record["instruction"],
            "input": record["input"],
            "output": record["output"],
            "normalized_concat": record["normalized_concat"],
            "simhash_hex": record["simhash_hex"],
            "whitespace_output_tokens": record["whitespace_output_tokens"],
            "whitespace_sample_tokens": sum(
                len(str(record[field]).split()) for field in ("instruction", "input", "output")
            ),
            "factors": result["factors"],
            "fallback_reasons": result["fallback_reasons"],
            "factor_notes": notes,
            "raw_factors": factors,
            "log_score_fixed_units": result["log_score_fixed_units"],
            "log_score": result["log_score"],
            "quality_score": result["quality_score"],
            "log_error_bound": result["log_error_bound"],
            "relative_error_bound": result["relative_error_bound"],
            "effective_digits": result["effective_digits"],
            "weight_sum_units": result["weight_sum_units"],
            "quantization": result["quantization"],
        }
        scored.append(score_record)
        stats = source_stats[str(record["source"])]
        stats["count"] += 1
        stats["score_sum"] += result["quality_score"]
        stats["fallback_count"] += len(result["fallback_reasons"])
        check()

    aggregation = aggregate_quality_scores(scored)
    for source, stats in source_stats.items():
        stats["mean_quality"] = stats["score_sum"] / stats["count"]
        del stats["score_sum"]
    diagnostic_samples = [
        {
            "sample_id": record["id"],
            "factors": record["factors"],
            "fallback_reasons": record["fallback_reasons"],
            "factor_notes": record["factor_notes"],
            "log_score_fixed_units": record["log_score_fixed_units"],
            "log_score": record["log_score"],
            "quality_score": record["quality_score"],
            "log_error_bound": record["log_error_bound"],
            "relative_error_bound": record["relative_error_bound"],
            "effective_digits": record["effective_digits"],
            "quantization": record["quantization"],
        }
        for record in scored
    ]
    digits = [record["effective_digits"] for record in scored]
    scores = [record["quality_score"] for record in scored]
    summary = {
        "scored_samples": len(scored),
        "score_min": min(scores) if scores else None,
        "score_max": max(scores) if scores else None,
        "score_mean": aggregation["mean_quality"],
        "effective_digits_min": min(digits) if digits else None,
        "fallback_counts": dict(sorted(fallback_counter.items())),
        "fallback_samples": sum(1 for record in scored if record["fallback_reasons"]),
        "unknown_source_samples": sum(1 for record in scored if "source_prior" in record["factor_notes"]),
        "template_count": len(template_counts),
        "repeated_template_count": sum(1 for count in template_counts.values() if count > 1),
        "source_stats": dict(sorted(source_stats.items())),
        "weight_sum_units": (1 << config.quantization.fraction_bits),
    }
    diagnostics = {
        "sample_count": len(scored),
        "fraction_bits": config.quantization.fraction_bits,
        "scale": config.quantization.scale,
        "weights": weights,
        "corpus_aggregation": aggregation["aggregation"],
        "mean_log_score": aggregation["mean_log_score"],
        "factor_weighted_log_contribution_means": aggregation[
            "factor_weighted_log_contribution_means"
        ],
        "samples": diagnostic_samples,
        "numeric_error_policy": (
            "NaN, Inf, negative factors, >1 factors, and zero factors without a declared "
            "fallback reason raise E_NUMERIC with sample_id and factor."
        ),
    }
    return {"records": scored, "summary": summary, "diagnostics": diagnostics}
