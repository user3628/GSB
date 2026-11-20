"""Exact-ish numerical core for statistics, error contracts, and quotas.

No binary float participates in a persisted statistic or admission decision.
Quantiles are evaluated with Decimal and rendered as fixed-decimal strings.
Quota factors use an integer fixed-point representation.
"""

from __future__ import annotations

import hashlib
import json
from decimal import Context, Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_EVEN, localcontext
from functools import wraps
from typing import Any, Iterable, Mapping, Sequence

from .config import GuardConfig, Interpolation


P_LEVELS: tuple[tuple[str, Decimal], ...] = (
    ("P50", Decimal("0.50")),
    ("P95", Decimal("0.95")),
    ("P99", Decimal("0.99")),
)
DECIMAL_PRECISION = 50


def _decimal_precision(function: Any) -> Any:
    @wraps(function)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with localcontext(Context(prec=DECIMAL_PRECISION, rounding=ROUND_HALF_EVEN)):
            return function(*args, **kwargs)

    return wrapper


def parse_decimal(value: Any, field: str) -> Decimal:
    try:
        if isinstance(value, Decimal):
            return value
        if isinstance(value, bool):
            raise ValueError
        return Decimal(str(value))
    except Exception as exc:  # noqa: BLE001 - converted into a domain error by caller
        raise ValueError(f"{field} must be a finite decimal") from exc


def clamp_decimal(value: Decimal, low: Decimal, high: Decimal) -> Decimal:
    if low > high:
        raise ValueError("invalid clamp bounds")
    return low if value < low else high if value > high else value


def fixed_decimal_text(value: Decimal | int | str, places: int) -> str:
    quantum = Decimal(1).scaleb(-places)
    return format(Decimal(value).quantize(quantum, rounding=ROUND_HALF_EVEN), "f")


def quantize_decimal(value: Decimal | int | str, places: int) -> Decimal:
    quantum = Decimal(1).scaleb(-places)
    return Decimal(value).quantize(quantum, rounding=ROUND_HALF_EVEN)


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def neumaier_sum(values: Iterable[Decimal]) -> tuple[Decimal, Decimal]:
    """Neumaier compensated summation.

    Returns ``(sum, compensation)``. For 0/1 good-event indicators the sum is
    exact under the active Decimal context, and compensation is zero.
    """

    total = Decimal(0)
    compensation = Decimal(0)
    for value in values:
        updated = total + value
        if abs(total) >= abs(value):
            compensation += (total - updated) + value
        else:
            compensation += (value - updated) + total
        total = updated
    return total, compensation


def _required_samples(level: str, config: GuardConfig) -> int:
    if level == "P50":
        return config.min_samples_p50
    if level == "P95":
        return config.min_samples_p95
    if level == "P99":
        return config.min_samples_p99
    raise ValueError(f"unsupported quantile level: {level}")


@_decimal_precision
def quantile_value(
    sorted_latencies: Sequence[int],
    probability: Decimal,
    method: Interpolation,
) -> tuple[Decimal | None, dict[str, Any]]:
    """Compute one quantile. Returns None only for an empty sample set."""

    count = len(sorted_latencies)
    if count == 0:
        return None, {"status": "EMPTY"}
    if probability < 0 or probability > 1:
        raise ValueError("quantile probability must be within 0..1")

    if method == "nearest_rank":
        rank_decimal = (probability * count).to_integral_value(rounding=ROUND_CEILING)
        rank = max(1, min(count, int(rank_decimal)))
        return Decimal(sorted_latencies[rank - 1]), {
            "rank": rank,
            "definition": "ceil(p*n), one-based, sorted ascending",
        }

    if method == "linear":
        # R type 7 / NumPy-compatible default linear interpolation.
        position = probability * Decimal(count - 1)
        lower = int(position.to_integral_value(rounding=ROUND_FLOOR))
        upper = min(count - 1, lower + 1)
        fraction = position - Decimal(lower)
        value = Decimal(sorted_latencies[lower])
        if upper != lower:
            value += fraction * Decimal(sorted_latencies[upper] - sorted_latencies[lower])
        return value, {
            "definition": "R type 7: (n-1)*p, linear between adjacent order statistics",
            "position": fixed_decimal_text(position, 9),
            "lower_index_zero_based": lower,
            "upper_index_zero_based": upper,
            "fraction": fixed_decimal_text(fraction, 9),
        }

    raise ValueError(f"unsupported interpolation method: {method}")


def _wilson_interval(success_count: int, sample_count: int, z: Decimal) -> tuple[Decimal, Decimal]:
    n = Decimal(sample_count)
    p = Decimal(success_count) / n
    denominator = Decimal(1) + (z * z) / n
    center = (p + (z * z) / (Decimal(2) * n)) / denominator
    variance_term = p * (Decimal(1) - p) / n + (z * z) / (Decimal(4) * n * n)
    margin = z * variance_term.sqrt() / denominator
    low = max(Decimal(0), center - margin)
    high = min(Decimal(1), center + margin)
    return low, high


def _empirical_cdf_quantile(sorted_latencies: Sequence[int], probability: Decimal) -> int:
    if not sorted_latencies:
        raise ValueError("empty sample set")
    if probability <= 0:
        return sorted_latencies[0]
    if probability >= 1:
        return sorted_latencies[-1]
    rank = int((probability * len(sorted_latencies)).to_integral_value(rounding=ROUND_CEILING))
    return sorted_latencies[max(1, min(len(sorted_latencies), rank)) - 1]


def _dkw_error(confidence: Decimal, sample_count: int) -> Decimal:
    alpha = Decimal(1) - confidence
    if alpha <= 0 or sample_count <= 0:
        raise ValueError("invalid DKW inputs")
    # Decimal has ln() and sqrt() on supported Python versions.
    return ((Decimal(2) / alpha).ln() / (Decimal(2) * Decimal(sample_count))).sqrt()


def _sample_tuples(samples: Sequence[Mapping[str, Any]]) -> list[tuple[int, str, int, int, int, int]]:
    result: list[tuple[int, str, int, int, int, int]] = []
    for sample in samples:
        result.append(
            (
                int(sample["timestamp_ms"]),
                str(sample.get("batch_id", "")),
                int(sample.get("sequence", 0)),
                int(sample.get("event_index", 0)),
                int(bool(sample["success"])),
                int(sample["latency_us"]),
            )
        )
    result.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
    return result


def sample_digest(samples: Sequence[Mapping[str, Any]]) -> str:
    return sha256_json(_sample_tuples(samples))


def samples_payload(samples: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    tuples = _sample_tuples(samples)
    return {
        "fields": [
            "timestamp_ms",
            "batch_id",
            "sequence",
            "event_index",
            "success",
            "latency_us",
        ],
        "rows": [list(row) for row in tuples],
    }


def _quantile_results(
    sorted_latencies: Sequence[int],
    method: Interpolation,
    config: GuardConfig,
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for level, probability in P_LEVELS:
        required = _required_samples(level, config)
        value, meta = quantile_value(sorted_latencies, probability, method)
        sufficient = len(sorted_latencies) >= required
        result[level] = {
            "probability": fixed_decimal_text(probability, config.probability_decimal_places),
            "method": method,
            "required_samples": required,
            "sufficient": sufficient,
            "business_code_if_strict": None if sufficient else "E_INSUFFICIENT_SAMPLES",
            "value_us": (
                fixed_decimal_text(value, config.latency_decimal_places)
                if value is not None and sufficient
                else None
            ),
            "rounding": config.rounding_mode,
            "decimal_places": config.latency_decimal_places,
            **meta,
        }
    return result


@_decimal_precision
def compute_window_stats(
    *,
    route: str,
    start_ms: int,
    end_ms: int,
    samples: Sequence[Mapping[str, Any]],
    config: GuardConfig,
) -> dict[str, Any]:
    """Build the complete deterministic statistics and error contract."""

    config.validate()
    if start_ms >= end_ms:
        raise ValueError("window start must be less than end")
    count = len(samples)
    if count == 0:
        raise ValueError("a persisted window must contain at least one sample")

    tuples = _sample_tuples(samples)
    latencies = sorted(row[5] for row in tuples)
    indicator_values = [Decimal(row[4]) for row in tuples]
    compensated_total, compensation = neumaier_sum(indicator_values)
    success_count = sum(row[4] for row in tuples)
    failure_count = count - success_count
    success_rate = Decimal(success_count) / Decimal(count)

    z = Decimal("1.959963984540054")
    success_low, success_high = _wilson_interval(success_count, count, z)

    nearest = _quantile_results(latencies, "nearest_rank", config)
    linear = _quantile_results(latencies, "linear", config)

    method_comparison: dict[str, Any] = {}
    max_deviation = Decimal(0)
    for level, _ in P_LEVELS:
        nr_value = nearest[level]["value_us"]
        linear_value = linear[level]["value_us"]
        if nr_value is None or linear_value is None:
            method_comparison[level] = {
                "delta_linear_minus_nearest_us": None,
                "abs_deviation_us": None,
                "status": "INSUFFICIENT_SAMPLES",
            }
            continue
        delta = Decimal(linear_value) - Decimal(nr_value)
        absolute = abs(delta)
        max_deviation = max(max_deviation, absolute)
        method_comparison[level] = {
            "delta_linear_minus_nearest_us": fixed_decimal_text(
                delta, config.latency_decimal_places
            ),
            "abs_deviation_us": fixed_decimal_text(absolute, config.latency_decimal_places),
            "status": "OK",
        }

    confidence = config.confidence_level
    dkw_epsilon = _dkw_error(confidence, count)
    numeric_bound = Decimal(1).scaleb(-config.latency_decimal_places)
    error_bounds: dict[str, Any] = {}
    for level, probability in P_LEVELS:
        low_probability = max(Decimal(0), probability - dkw_epsilon)
        high_probability = probability + dkw_epsilon
        lower_us = _empirical_cdf_quantile(latencies, low_probability)
        if high_probability >= 1:
            upper_us = max(config.support_max_latency_us, latencies[-1])
            upper_source = "configured_support_max"
        else:
            upper_us = _empirical_cdf_quantile(latencies, high_probability)
            upper_source = "order_statistic"
        sampling_width = Decimal(upper_us) - Decimal(lower_us)
        spread_text = method_comparison[level]["abs_deviation_us"]
        spread = Decimal(spread_text) if spread_text is not None else Decimal(0)
        combined = sampling_width + spread + numeric_bound
        nearest_value = nearest[level]["value_us"]
        relative_spread = None if nearest_value is None else spread / max(Decimal(1), Decimal(nearest_value))
        relative_combined = (
            None if nearest_value is None else combined / max(Decimal(1), Decimal(nearest_value))
        )
        error_bounds[level] = {
            "confidence": fixed_decimal_text(confidence, 6),
            "method": "DKW distribution-free empirical-CDF band",
            "dkw_epsilon": fixed_decimal_text(dkw_epsilon, config.probability_decimal_places),
            "lower_probability": fixed_decimal_text(
                low_probability, config.probability_decimal_places
            ),
            "upper_probability": fixed_decimal_text(
                high_probability, config.probability_decimal_places
            ),
            "lower_us": fixed_decimal_text(lower_us, config.latency_decimal_places),
            "upper_us": fixed_decimal_text(upper_us, config.latency_decimal_places),
            "upper_source": upper_source,
            "statistical_width_us": fixed_decimal_text(
                sampling_width, config.latency_decimal_places
            ),
            "method_spread_us": fixed_decimal_text(spread, config.latency_decimal_places),
            "numeric_rounding_bound_us": fixed_decimal_text(
                numeric_bound, config.latency_decimal_places
            ),
            "combined_upper_bound_us": fixed_decimal_text(
                combined, config.latency_decimal_places
            ),
            "relative_method_spread": (
                None if relative_spread is None else fixed_decimal_text(relative_spread, 12)
            ),
            "relative_combined_upper_bound": (
                None if relative_combined is None else fixed_decimal_text(relative_combined, 12)
            ),
        }

    last_row = tuples[-1]
    last_failure_ts = next((row[0] for row in reversed(tuples) if row[4] == 0), None)
    consecutive_failures = 0
    for row in reversed(tuples):
        if row[4] == 1:
            break
        consecutive_failures += 1
    consecutive_successes = 0
    for row in reversed(tuples):
        if row[4] == 0:
            break
        consecutive_successes += 1

    rate_places = config.rate_decimal_places
    stats: dict[str, Any] = {
        "stats_version": 1,
        "window": {
            "route": route,
            "start_ms": start_ms,
            "end_ms": end_ms,
            "length_ms": end_ms - start_ms,
            "boundary": "[start_ms,end_ms)",
            "timestamp_resolution_ms": 1,
            "latency_resolution_us": 1,
        },
        "samples": {
            "sample_count": count,
            "success_count": success_count,
            "failure_count": failure_count,
            "consecutive_failures": consecutive_failures,
            "consecutive_successes": consecutive_successes,
            "last_event_ts_ms": last_row[0],
            "last_failure_ts_ms": last_failure_ts,
            "digest": sample_digest(samples),
        },
        "success_rate": {
            "value": fixed_decimal_text(success_rate, rate_places),
            "exact_fraction": f"{success_count}/{count}",
            "decimal_places": rate_places,
            "rounding": config.rounding_mode,
            "confidence_95": {
                "method": "Wilson score interval with exact Decimal arithmetic",
                "low": fixed_decimal_text(success_low, rate_places),
                "high": fixed_decimal_text(success_high, rate_places),
            },
            "compensated_sum": {
                "method": "Neumaier compensated summation over 0/1 good-event indicators",
                "sum": fixed_decimal_text(compensated_total, 0),
                "compensation_term": fixed_decimal_text(compensation, 30),
                "accumulation_error_upper_bound": fixed_decimal_text(Decimal(0), rate_places),
                "explanation": (
                    "Good-event indicators are exactly representable integers. With Decimal "
                    "arithmetic the compensated count is exact; only final fixed-decimal "
                    "rendering contributes at most half a unit in the last shown place."
                ),
            },
        },
        "quantiles": {
            "nearest_rank": nearest,
            "linear": linear,
            "primary_method": config.primary_interpolation,
        },
        "method_comparison": {
            "max_abs_deviation_us": fixed_decimal_text(
                max_deviation, config.latency_decimal_places
            ),
            "per_quantile": method_comparison,
            "note": (
                "The deviation is computed from the two methods over the exact same sample set "
                "and the same window version."
            ),
        },
        "error_propagation": {
            "confidence_level": fixed_decimal_text(confidence, 6),
            "quantile_bounds": error_bounds,
            "deterministic_numeric_bound_us": fixed_decimal_text(
                numeric_bound, config.latency_decimal_places
            ),
            "success_rate_rounding_bound": fixed_decimal_text(
                Decimal(1).scaleb(-rate_places), rate_places
            ),
            "success_rate_accumulation_error_bound": fixed_decimal_text(
                Decimal(0), rate_places
            ),
            "window_length_ms": end_ms - start_ms,
            "window_step_ms": config.step_ms,
            "sample_density_per_1000ms": fixed_decimal_text(
                Decimal(count) * Decimal(1000) / Decimal(end_ms - start_ms), 6
            ),
            "assumptions": [
                "The DKW quantile band assumes samples in one window are i.i.d. draws from one distribution.",
                "Changing window length changes the sample composition; no numerical bound can remove that statistical effect without a distribution model.",
                "Latency values are integer microseconds; success indicators are exact 0/1 values.",
                "All displayed values use ROUND_HALF_EVEN at the declared decimal places.",
            ],
        },
        "numeric_contract": {
            "arithmetic": (
                f"Python Decimal with precision {DECIMAL_PRECISION}; integer fixed point for quota factors"
            ),
            "rounding": config.rounding_mode,
            "decimal_places": {
                "latency_us": config.latency_decimal_places,
                "rate": config.rate_decimal_places,
                "factor": config.factor_decimal_places,
                "probability": config.probability_decimal_places,
            },
            "window_boundary": "lower-inclusive, upper-exclusive: [start_ms,end_ms)",
            "sample_order": "timestamp_ms, batch_id, sequence, event_index",
            "quantile_methods": {
                "nearest_rank": "ceil(p*n), one-based; integer endpoint",
                "linear": "R type 7: linear interpolation at (n-1)*p",
            },
        },
    }
    return stats


def primary_quantile_us(stats: Mapping[str, Any], level: str = "P95") -> Decimal | None:
    method = stats["quantiles"]["primary_method"]
    value = stats["quantiles"][method][level]["value_us"]
    return None if value is None else Decimal(value)


def _fixed_from_decimal(value: Decimal, scale: int) -> int:
    return int((value * Decimal(scale)).to_integral_value(rounding=ROUND_HALF_EVEN))


def _signed_fixed_round(value: Decimal) -> int:
    return int(value.to_integral_value(rounding=ROUND_HALF_EVEN))


def _factor_step(
    name: str,
    exact: Decimal,
    low: Decimal,
    high: Decimal,
    places: int,
) -> tuple[Decimal, dict[str, Any]]:
    bounded = clamp_decimal(exact, low, high)
    return bounded, {
        "name": name,
        "exact": str(exact),
        "value": fixed_decimal_text(bounded, places),
        "bounds": [fixed_decimal_text(low, places), fixed_decimal_text(high, places)],
        "clamped": bounded != exact,
    }


@_decimal_precision
def propagate_metrics(
    window_stats: Sequence[Mapping[str, Any]],
    weights: Sequence[Decimal],
) -> dict[str, Any]:
    """Propagate success and latency over newest-first closed windows."""

    if not window_stats:
        raise ValueError("at least one window is required")
    selected = list(window_stats[: len(weights)])
    selected_weights = list(weights[: len(selected)])
    weight_total, weight_compensation = neumaier_sum(selected_weights)
    if weight_total <= 0:
        raise ValueError("propagation weights must sum to a positive value")

    weighted_terms: list[Decimal] = []
    windows_out: list[dict[str, Any]] = []
    p95_values: list[Decimal] = []
    for window, weight in zip(selected, selected_weights, strict=True):
        exact_fraction = window.get("success_rate", {}).get("exact_fraction")
        if exact_fraction:
            numerator, denominator = exact_fraction.split("/", 1)
            rate = Decimal(numerator) / Decimal(denominator)
        else:
            rate = Decimal(window["success_rate"]["value"])
        p95 = primary_quantile_us(window, "P95")
        if p95 is None:
            raise ValueError("propagated window has insufficient P95 samples")
        normalized = weight / weight_total
        weighted_terms.append(normalized * rate)
        p95_values.append(p95)
        windows_out.append(
            {
                "window_version": window.get("window_version"),
                "window_version_id": window.get("window_version_id"),
                "start_ms": window["window"]["start_ms"],
                "end_ms": window["window"]["end_ms"],
                "sample_count": window["samples"]["sample_count"],
                "success_rate": window["success_rate"]["value"],
                "p95_us": fixed_decimal_text(p95, 6),
                "weight": fixed_decimal_text(normalized, 9),
                "error_contract": window.get("error_contract"),
            }
        )

    weighted_success, weighted_compensation = neumaier_sum(weighted_terms)
    return {
        "method": "newest-first weighted propagation; conservative max P95",
        "weights_normalized_newest_first": [fixed_decimal_text(w / weight_total, 9) for w in selected_weights],
        "weight_compensation": fixed_decimal_text(weight_compensation, 30),
        "weighted_success_rate": fixed_decimal_text(weighted_success, 12),
        "weighted_success_rate_exact": str(weighted_success),
        "weighted_success_compensation": fixed_decimal_text(weighted_compensation, 30),
        "propagated_p95_us": fixed_decimal_text(max(p95_values), 6),
        "p95_rule": "maximum primary-method P95 across selected windows",
        "windows": windows_out,
    }


@_decimal_precision
def derive_quota(
    *,
    propagation: Mapping[str, Any],
    breaker_state: str,
    current_concurrency: int,
    previous_quota: int | None,
    budget_watermark: Decimal,
    target_latency_us: int,
    config: GuardConfig,
) -> dict[str, Any]:
    """Compute a fixed-point adaptive quota with damping and bounded steps."""

    config.validate()
    if current_concurrency < 0:
        raise ValueError("current_concurrency must be non-negative")
    if target_latency_us <= 0:
        raise ValueError("target_latency_us must be positive")

    success_rate = Decimal(
        propagation.get("weighted_success_rate_exact", propagation["weighted_success_rate"])
    )
    p95 = Decimal(propagation["propagated_p95_us"])
    budget = clamp_decimal(budget_watermark, Decimal(0), Decimal(1))
    target_latency = Decimal(target_latency_us)

    success_factor, success_step = _factor_step(
        "success_factor", success_rate, Decimal(0), Decimal(1), config.factor_decimal_places
    )
    latency_factor, latency_step = _factor_step(
        "observed_latency_factor",
        target_latency / max(p95, Decimal(1)) if p95 > 0 else Decimal(1),
        Decimal(0),
        Decimal(1),
        config.factor_decimal_places,
    )
    budget_factor, budget_step = _factor_step(
        "budget_factor", budget, Decimal(0), Decimal(1), config.factor_decimal_places
    )

    quality = (
        config.quality_success_weight * success_factor
        + config.quality_latency_weight * latency_factor
        + config.quality_budget_weight * budget_factor
    )
    quality = clamp_decimal(quality, Decimal(0), Decimal(1))
    base_unclamped = Decimal(config.max_quota) * quality
    base_quota = min(config.max_quota, max(config.min_quota, int(base_unclamped.to_integral_value(rounding=ROUND_FLOOR))))

    concurrency_ratio = Decimal(current_concurrency) / Decimal(max(1, config.target_concurrency))
    concurrency_exact = Decimal(1) - concurrency_ratio
    concurrency_factor, concurrency_step = _factor_step(
        "concurrency_factor",
        concurrency_exact,
        config.concurrency_factor_floor,
        Decimal(1),
        config.factor_decimal_places,
    )
    latency_scale, latency_scale_step = _factor_step(
        "target_latency_scale_factor",
        target_latency / max(p95, Decimal(1)),
        Decimal(0),
        Decimal(1),
        config.factor_decimal_places,
    )
    scale_exact = clamp_decimal(concurrency_factor * latency_scale, Decimal(0), Decimal(1))
    scale_fp = _fixed_from_decimal(scale_exact, config.fixed_point_scale)
    if scale_fp < 0 or scale_fp > config.fixed_point_scale:
        raise AssertionError("fixed-point scale escaped its bounds")

    scaled_quota = (base_quota * scale_fp) // config.fixed_point_scale
    scaled_quota = min(config.max_quota, max(config.min_quota, scaled_quota))

    weights = {
        "CLOSED": config.breaker_weight_closed_bp,
        "HALF_OPEN": config.breaker_weight_half_open_bp,
        "OPEN": config.breaker_weight_open_bp,
    }
    if breaker_state not in weights:
        raise ValueError(f"unknown breaker state: {breaker_state}")
    breaker_weight = weights[breaker_state]
    breaker_adjusted = (scaled_quota * breaker_weight) // 10_000
    half_open_floor = 0
    if breaker_state == "HALF_OPEN" and breaker_adjusted < 1:
        breaker_adjusted = 1
        half_open_floor = 1
    breaker_adjusted = min(config.max_quota, max(config.min_quota, breaker_adjusted))

    target = breaker_adjusted
    if previous_quota is None:
        previous = target
        delta = 0
        damped = target
        damping_action = "INITIAL"
    else:
        previous = previous_quota
        delta = target - previous
        if abs(delta) <= config.damping_deadband:
            damped = previous
            damping_action = "DEADBAND_HOLD"
        else:
            damped = previous + _signed_fixed_round(
                Decimal(delta) * Decimal(config.damping_alpha_bp) / Decimal(10_000)
            )
            damping_action = "DAMPED_STEP"
    damped = min(config.max_quota, max(config.min_quota, damped))

    # Conservative unit-level propagation bound. It is an engineering envelope,
    # never an SLA or a claim that random sampling error disappears.
    success_ci = Decimal(0)
    method_spread = Decimal(0)
    # The source window contracts are embedded in propagation when available.
    for window in propagation.get("windows", []):
        source = window.get("error_contract")
        if source:
            success_ci = max(success_ci, Decimal(source["success_rate_ci_half_width"]))
            method_spread = max(method_spread, Decimal(source["method_spread_us"]))
    statistical_units = (
        Decimal(config.max_quota)
        * config.quality_success_weight
        * success_ci
    )
    numeric_units = (
        Decimal(config.max_quota)
        * config.quality_latency_weight
        * (method_spread / max(p95, Decimal(1)))
        + Decimal(1)
    )
    total_units = int((statistical_units + numeric_units).to_integral_value(rounding=ROUND_CEILING))

    if breaker_state == "OPEN":
        reason = "BREAKER_OPEN"
    elif breaker_state == "HALF_OPEN":
        reason = "BREAKER_HALF_OPEN_DERATED"
    elif success_factor < Decimal("0.50"):
        reason = "LOW_SUCCESS_RATE"
    elif p95 > target_latency:
        reason = "HIGH_LATENCY"
    elif budget_factor < Decimal("0.50"):
        reason = "LOW_BUDGET"
    else:
        reason = "HEALTHY"

    steps = [
        success_step,
        latency_step,
        budget_step,
        {
            "name": "weighted_quality",
            "exact": str(quality),
            "value": fixed_decimal_text(quality, config.factor_decimal_places),
            "formula": "0.45*success + 0.35*latency + 0.20*budget",
        },
        {
            "name": "base_quota",
            "exact": str(base_unclamped),
            "operation": "floor(max_quota * weighted_quality), then clamp",
            "value": base_quota,
            "bounds": [config.min_quota, config.max_quota],
        },
        concurrency_step,
        latency_scale_step,
        {
            "name": "scale_factor_fixed_point",
            "exact": str(scale_exact),
            "value": fixed_decimal_text(scale_exact, config.factor_decimal_places),
            "fixed_point": scale_fp,
            "fixed_point_scale": config.fixed_point_scale,
            "bounds": [0, config.fixed_point_scale],
            "rounding": config.rounding_mode,
        },
        {
            "name": "concurrency_scaled_quota",
            "operation": "floor(base_quota * scale_fixed_point / scale)",
            "value": scaled_quota,
            "bounds": [config.min_quota, config.max_quota],
        },
        {
            "name": "breaker_weight",
            "state": breaker_state,
            "value_bp": breaker_weight,
            "decimal": fixed_decimal_text(Decimal(breaker_weight) / Decimal(10_000), 6),
        },
        {
            "name": "breaker_adjusted_quota",
            "value": breaker_adjusted,
            "half_open_probe_floor": half_open_floor,
            "bounds": [config.min_quota, config.max_quota],
        },
        {
            "name": "damped_quota",
            "previous": previous,
            "target": target,
            "delta": delta,
            "alpha_bp": config.damping_alpha_bp,
            "deadband": config.damping_deadband,
            "action": damping_action,
            "value": damped,
            "bounds": [config.min_quota, config.max_quota],
        },
    ]

    return {
        "quota_version": 1,
        "final_quota": damped,
        "target_before_damping": target,
        "previous_quota": previous_quota,
        "reason": reason,
        "current_concurrency": current_concurrency,
        "target_concurrency": config.target_concurrency,
        "budget_watermark": fixed_decimal_text(budget, 9),
        "target_latency_us": target_latency_us,
        "formula": {
            "quality": "0.45*success_factor + 0.35*latency_factor + 0.20*budget_factor",
            "base_quota": "floor(max_quota * quality)",
            "scale": "clamp(concurrency_factor * target_latency_scale, 0, 1)",
            "breaker": "floor(concurrency_scaled * breaker_weight_bp / 10000)",
            "damping": "previous + ROUND_HALF_EVEN(delta * alpha_bp / 10000), deadband first",
        },
        "fixed_point_contract": {
            "scale_denominator": config.fixed_point_scale,
            "basis_points_denominator": 10_000,
            "rounding": config.rounding_mode,
            "bounds_checked_each_step": True,
        },
        "error_propagation": {
            "statistical_sensitivity_units": int(
                statistical_units.to_integral_value(rounding=ROUND_CEILING)
            ),
            "numeric_sensitivity_units": int(
                numeric_units.to_integral_value(rounding=ROUND_CEILING)
            ),
            "total_error_upper_bound_units": total_units,
            "explanation": (
                "An engineering envelope derived from the success-rate confidence half-width and "
                "observed interpolation spread. It is not an SLA guarantee and does not remove "
                "sampling uncertainty."
            ),
        },
        "steps": steps,
        "propagation": dict(propagation),
    }


def max_quantile_method_deviation(stats: Mapping[str, Any]) -> str:
    return str(stats["method_comparison"]["max_abs_deviation_us"])

