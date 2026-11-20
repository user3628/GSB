from __future__ import annotations

import math
from decimal import Decimal, ROUND_FLOOR, getcontext
from fractions import Fraction
from typing import Any, Mapping, Sequence

from .config import QuantizationConfig
from .errors import numeric_error


def _round_half_away(value: float) -> int:
    return math.floor(value + 0.5) if value >= 0 else math.ceil(value - 0.5)


def _fixed_weight_units(weights: Mapping[str, float], scale: int, order: Sequence[str]) -> dict[str, int]:
    fractions = {name: Fraction(str(weights[name])) for name in order}
    total = sum(fractions.values(), Fraction(0))
    normalized = {name: fractions[name] / total for name in order}
    exact_units = {name: normalized[name] * scale for name in order}
    units = {name: int(exact_units[name].numerator // exact_units[name].denominator) for name in order}
    remaining = scale - sum(units.values())
    ranked = sorted(
        order,
        key=lambda name: (-(exact_units[name] - units[name]), order.index(name)),
    )
    for name in ranked[:remaining]:
        units[name] += 1
    return units


def _validate_factor(sample_id: str, factor: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise numeric_error(sample_id, factor, value, "not a number")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise numeric_error(sample_id, factor, value, "NaN or Inf")
    if numeric < 0.0:
        raise numeric_error(sample_id, factor, value, "negative factor")
    if numeric > 1.0:
        raise numeric_error(sample_id, factor, value, "factor above 1")
    return numeric


def weighted_geometric_fixed(
    sample_id: str,
    factors: Mapping[str, float],
    weights: Mapping[str, float],
    reasons: Mapping[str, str],
    quant: QuantizationConfig,
) -> dict[str, Any]:
    """Compute a weighted geometric mean using quantized factors and fixed-point log terms."""
    order = list(weights.keys())
    scale = quant.scale
    weight_units = _fixed_weight_units(weights, scale, order)
    factor_details: dict[str, dict[str, Any]] = {}
    log_score_units = 0
    total_log_error_bound = 0.0
    fallback_reasons: list[dict[str, str]] = []

    for name in order:
        raw = _validate_factor(sample_id, name, factors.get(name))
        reason = reasons.get(name)
        if raw == 0.0:
            if not reason:
                raise numeric_error(sample_id, name, raw, "zero factor without fallback reason")
            raw = quant.min_factor
            fallback_reasons.append({"factor": name, "reason": reason, "floor": quant.min_factor})
        factor_units = max(1, _round_half_away(raw * scale))
        factor_q = factor_units / scale
        if factor_q <= 0.0 or not math.isfinite(factor_q):
            raise numeric_error(sample_id, name, raw, "quantized factor is not positive")
        log_factor = math.log(factor_q)
        normalized_weight = weight_units[name] / scale
        weighted_log = normalized_weight * log_factor
        log_term_units = _round_half_away(weighted_log * scale)
        log_score_units += log_term_units

        factor_abs_error = abs(factor_q - raw)
        lower = factor_q - factor_abs_error
        if lower <= 0.0:
            factor_log_error_bound = abs(log_factor) + math.log(scale)
        else:
            factor_log_error_bound = max(
                abs(math.log((factor_q + factor_abs_error) / factor_q)),
                abs(math.log(lower / factor_q)),
            )
        exact_normalized_weight = Fraction(weight_units[name], scale)
        weight_error_bound = float(abs(exact_normalized_weight - Fraction(str(weights[name]))) / sum(
            Fraction(str(value)) for value in weights.values()
        ))
        weight_effect_bound = weight_error_bound * abs(log_factor)
        factor_effect_bound = normalized_weight * factor_log_error_bound
        term_quant_bound = 0.5 / scale
        factor_total_bound = factor_effect_bound + weight_effect_bound + term_quant_bound
        total_log_error_bound += factor_total_bound
        factor_details[name] = {
            "raw_factor": raw,
            "quantized_factor": factor_q,
            "factor_units": factor_units,
            "log_factor": log_factor,
            "normalized_weight": normalized_weight,
            "weight_units": weight_units[name],
            "log_contribution": log_factor,
            "weighted_log_contribution": weighted_log,
            "quantized_weighted_log_contribution": log_term_units / scale,
            "log_term_units": log_term_units,
            "factor_quantization_error_bound": factor_effect_bound,
            "weight_quantization_error_bound": weight_effect_bound,
            "term_quantization_error_bound": term_quant_bound,
            "total_log_error_bound": factor_total_bound,
        }
    log_score = log_score_units / scale
    score = math.exp(log_score)
    if not math.isfinite(score) or score <= 0.0:
        raise numeric_error(sample_id, "weighted_geometric_mean", score, "non-finite final score")
    relative_error_bound = math.expm1(total_log_error_bound) if total_log_error_bound < 50 else float("inf")
    if relative_error_bound <= 0.0:
        effective_digits = 15
    elif not math.isfinite(relative_error_bound) or relative_error_bound >= 1.0:
        effective_digits = 0
    else:
        effective_digits = max(0, min(15, int(math.floor(-math.log10(relative_error_bound)))))
    return {
        "sample_id": sample_id,
        "factors": factor_details,
        "fallback_reasons": fallback_reasons,
        "log_score_fixed_units": log_score_units,
        "log_score": log_score,
        "quality_score": score,
        "log_error_bound": total_log_error_bound,
        "relative_error_bound": relative_error_bound,
        "effective_digits": effective_digits,
        "weight_sum_units": sum(weight_units.values()),
        "quantization": {
            "fraction_bits": quant.fraction_bits,
            "scale": scale,
            "min_factor": quant.min_factor,
        },
    }


def _pairwise_float_sum(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    if len(values) <= 8:
        total = 0.0
        for value in values:
            total += value
        return total
    middle = len(values) // 2
    return _pairwise_float_sum(values[:middle]) + _pairwise_float_sum(values[middle:])


def aggregate_quality_scores(score_records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    getcontext().prec = 60
    if not score_records:
        return {
            "count": 0,
            "mean_quality": None,
            "aggregation": None,
            "factor_contribution_means": {},
        }
    ordered = sorted(score_records, key=lambda record: str(record["sample_id"]))
    values = [float(record["quality_score"]) for record in ordered]
    log_units = [int(record["log_score_fixed_units"]) for record in ordered]
    scales = {int(record["quantization"]["scale"]) for record in ordered}
    if len(scales) != 1:
        raise ValueError("all score records must use the same quantization scale")
    scale = scales.pop()
    sequential = 0.0
    for value in values:
        sequential += value
    pairwise = _pairwise_float_sum(values)
    count = len(values)
    mean = sequential / count
    decimal_scale = Decimal(scale)
    reference_scores = [((Decimal(unit) / decimal_scale).exp()) for unit in log_units]
    high_precision_sum = sum(reference_scores, Decimal(0))
    high_precision_mean = high_precision_sum / Decimal(count)
    uint = 2.0**-53
    gamma = count * uint / (1.0 - count * uint) if count * uint < 0.5 else float("inf")
    higham_bound = gamma * sum(abs(value) for value in values)
    quant_bounds = []
    for record in ordered:
        bound = float(record["log_error_bound"])
        score = float(record["quality_score"])
        quant_bounds.append(score * math.expm1(bound) if bound < 50 else float("inf"))
    quantization_bound = sum(quant_bounds)
    ref_mean_float = float(high_precision_mean)
    sequential_delta = abs(sequential / count - ref_mean_float)
    pairwise_delta = abs(pairwise / count - ref_mean_float)
    factor_names = sorted(ordered[0]["factors"].keys())
    contribution_means = {
        name: sum(float(record["factors"][name]["weighted_log_contribution"]) for record in ordered) / count
        for name in factor_names
    }
    return {
        "count": count,
        "mean_quality": mean,
        "mean_log_score": sum(log_units) / (scale * count),
        "aggregation": {
            "order": "sample_id_ascending",
            "sequential_sum": sequential,
            "pairwise_sum": pairwise,
            "sequential_mean": sequential / count,
            "pairwise_mean": pairwise / count,
            "high_precision_sum": str(high_precision_sum),
            "high_precision_mean": str(high_precision_mean),
            "sequential_high_precision_delta": sequential_delta,
            "pairwise_high_precision_delta": pairwise_delta,
            "observed_sequential_pairwise_difference": abs(sequential - pairwise) / count,
            "higham_float_bound": higham_bound / count,
            "quantization_bound": quantization_bound / count,
            "combined_deviation_bound": (higham_bound + quantization_bound) / count,
            "note": (
                "Sequential summation is order-sensitive; pairwise summation has O(log n) depth. "
                "Both are compared with a 60-digit Decimal reference. The reported bound is the "
                "sum of Higham's gamma_n floating bound and the sample-wise fixed-point log bound."
            ),
        },
        "factor_weighted_log_contribution_means": contribution_means,
    }
