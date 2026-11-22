"""Deterministic derived metrics, arbitration, and numeric error accounting."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from typing import Any, Iterable, Mapping, Sequence

from .fixedpoint import (
    FRACTIONAL_DIGITS,
    ROUNDING_RULE,
    SCALE,
    SUMMATION_RULE,
    CompensatedSum,
    decimal_to_q,
    float_summation_cross_check,
    fraction_to_decimal_string,
    normalize_integer_weights,
    q_div,
    q_div_int,
    q_mul,
    q_to_string,
)
from .time_contract import renewal_margin_detail

DEFAULT_FAIRNESS_RELATIVE_TOLERANCE = Decimal("0.000001")
DEFAULT_REBALANCE_RELATIVE_TOLERANCE = Decimal("0.05")


@dataclass(frozen=True, slots=True)
class LeaseFact:
    lock_key: str
    holder_id: str
    version: int
    granted_at_ms: int
    expires_at_ms: int
    ttl_ms: int
    renew_count: int
    weight_text: str
    weight_q: int


@dataclass(frozen=True, slots=True)
class HolderFact:
    holder_id: str
    weight_text: str
    weight_q: int


@dataclass(frozen=True, slots=True)
class Candidate:
    holder_id: str
    weight_text: str
    weight_q: int
    ttl_ms: int


def fixed_metadata() -> dict[str, object]:
    return {
        "scale": SCALE,
        "fractional_digits": FRACTIONAL_DIGITS,
        "representation": "signed integer Q units; 1.0 == 1000000000",
        "rounding": ROUNDING_RULE,
        "division_rounding": "final rational result rounded once with ties-to-even",
        "normalization": "largest remainder; remainder desc; holder_id asc",
        "summation": SUMMATION_RULE,
        "binary_float_in_canonical_path": False,
    }


def _fixed(q: int) -> dict[str, object]:
    return {"q": q, "decimal": q_to_string(q)}


def shared_distribution(
    actual_weights: Mapping[str, int], target_weights: Mapping[str, int]
) -> dict[str, object]:
    """Normalize actual and target holder weights independently.

    Both inputs are integer Q weights.  Every returned share is a Q integer and
    each side sums exactly to SCALE.
    """

    holders = sorted(set(actual_weights) | set(target_weights))
    actual_nonzero = {holder: actual_weights.get(holder, 0) for holder in holders if actual_weights.get(holder, 0) > 0}
    target_nonzero = {holder: target_weights.get(holder, 0) for holder in holders if target_weights.get(holder, 0) > 0}
    actual_shares, actual_trace = normalize_integer_weights(actual_nonzero, trace_name="actual_lease_share")
    target_shares, target_trace = normalize_integer_weights(target_nonzero, trace_name="target_weight_share")
    return {
        "actual_shares": actual_shares,
        "target_shares": target_shares,
        "actual_trace": actual_trace,
        "target_trace": target_trace,
    }


def weighted_fairness_from_shares(
    actual_shares: Mapping[str, int], target_shares: Mapping[str, int]
) -> dict[str, object]:
    """Compute F = 1 - sum(target_share_i * abs(actual_share_i-target_share_i)).

    The multiplication of each term is rounded once to Q.  All term summation is
    Kahan-compensated.  Terms and holders are iterated in holder-id order.
    """

    holders = sorted(set(actual_shares) | set(target_shares))
    summation = CompensatedSum()
    term_rows: list[dict[str, object]] = []
    term_qs: list[int] = []
    cumulative_rounding = Fraction(0)
    for holder in holders:
        actual = actual_shares.get(holder, 0)
        target = target_shares.get(holder, 0)
        absolute_deviation = abs(actual - target)
        term_q, error = q_mul(target, absolute_deviation)
        cumulative_rounding += abs(error)
        summation.add_q(term_q)
        term_qs.append(term_q)
        term_rows.append(
            {
                "holder_id": holder,
                "actual_share": _fixed(actual),
                "target_share": _fixed(target),
                "absolute_deviation": _fixed(absolute_deviation),
                "multiplication_inputs_q": [target, absolute_deviation],
                "term_q": term_q,
                "multiplication_rounding_error_q": fraction_to_decimal_string(error),
            }
        )
    weighted_deviation_q = summation.rounded_q()
    fairness_q = max(0, min(SCALE, SCALE - weighted_deviation_q))
    return {
        "fairness": _fixed(fairness_q),
        "weighted_deviation": _fixed(weighted_deviation_q),
        "terms": term_rows,
        "term_qs": term_qs,
        "compensation": SUMMATION_RULE,
        "kahan_compensation_q": str(summation.compensation),
        "summation_term_rounding_error_q": fraction_to_decimal_string(cumulative_rounding),
        "formula": "1 - sum_i(target_share_i * abs(actual_share_i - target_share_i))",
    }


def _active_rows(leases: Sequence[LeaseFact], now_ms: int) -> list[LeaseFact]:
    return sorted(
        [lease for lease in leases if lease.expires_at_ms > now_ms],
        key=lambda lease: (lease.lock_key, lease.holder_id, lease.version),
    )


def _holder_targets(
    active: Sequence[LeaseFact], holders: Sequence[HolderFact]
) -> tuple[dict[str, int], list[dict[str, object]]]:
    declared = {holder.holder_id: holder for holder in holders}
    active_holders = sorted({lease.holder_id for lease in active})
    targets: dict[str, int] = {}
    input_trace: list[dict[str, object]] = []
    for holder_id in active_holders:
        holder = declared.get(holder_id)
        if holder is None:
            fallback = max(lease.weight_q for lease in active if lease.holder_id == holder_id)
            target_q = fallback
            weight_text = q_to_string(fallback)
            source = "fallback_active_lease_weight"
        else:
            target_q = holder.weight_q
            weight_text = holder.weight_text
            source = "holders.declared_weight"
        targets[holder_id] = max(0, target_q)
        expected_q, error_q = decimal_to_q(weight_text)
        input_trace.append(
            {
                "holder_id": holder_id,
                "source": source,
                "input": weight_text,
                "persisted_weight_q": target_q,
                "requantized_q": expected_q,
                "input_quantization_error_q": str(error_q),
            }
        )
    return targets, input_trace


def build_metrics(
    leases: Sequence[LeaseFact],
    holders: Sequence[HolderFact],
    *,
    registered_keys: int,
    now_ms: int,
    revision: int,
    rebalance_tolerance_q: int,
) -> dict[str, object]:
    active = _active_rows(leases, now_ms)
    active_keys = sorted({lease.lock_key for lease in active})
    actual_weights: dict[str, int] = {}
    for lease in active:
        actual_weights[lease.holder_id] = actual_weights.get(lease.holder_id, 0) + lease.weight_q

    targets, target_input_trace = _holder_targets(active, holders)
    distribution = shared_distribution(actual_weights, targets)
    actual_shares: dict[str, int] = distribution["actual_shares"]  # type: ignore[assignment]
    target_shares: dict[str, int] = distribution["target_shares"]  # type: ignore[assignment]
    fairness_data = weighted_fairness_from_shares(actual_shares, target_shares)

    total_active_weight = sum(actual_weights.values())
    lease_rows: list[dict[str, object]] = []
    lease_input_error_q = Fraction(0)
    margin_error_total_q = Fraction(0)
    lease_share_error_total_q = Fraction(0)
    for lease in active:
        margin_q, remaining, margin_error = renewal_margin_detail(
            lease.expires_at_ms, lease.ttl_ms, now_ms
        )
        share_q, share_error = q_div_int(lease.weight_q, total_active_weight)
        requantized_q, input_error_q = decimal_to_q(lease.weight_text)
        lease_input_error_q += abs(Fraction(input_error_q))
        margin_error_total_q += abs(margin_error)
        lease_share_error_total_q += abs(share_error)
        lease_rows.append(
            {
                "lock_key": lease.lock_key,
                "holder_id": lease.holder_id,
                "version": lease.version,
                "granted_at_ms": lease.granted_at_ms,
                "expires_at_ms": lease.expires_at_ms,
                "ttl_ms": lease.ttl_ms,
                "remaining_ms": remaining,
                "renew_count": lease.renew_count,
                "weight": _fixed(lease.weight_q),
                "weight_input": lease.weight_text,
                "weight_requantized_q": requantized_q,
                "weight_input_quantization_error_q": str(input_error_q),
                "renewal_margin_ratio": _fixed(margin_q),
                "renewal_margin_rounding_error_q": fraction_to_decimal_string(margin_error),
                "lease_weight_share": _fixed(share_q),
                "lease_weight_share_rounding_error_q": fraction_to_decimal_string(share_error),
            }
        )

    holder_rows: list[dict[str, object]] = []
    suggestions: list[dict[str, object]] = []
    suppressed: list[dict[str, object]] = []
    rebalance_relative_error_total_q = Fraction(0)
    for holder_id in sorted(set(actual_shares) | set(target_shares)):
        actual_q = actual_shares.get(holder_id, 0)
        target_q = target_shares.get(holder_id, 0)
        deviation_q = actual_q - target_q
        absolute_q = abs(deviation_q)
        if target_q > 0:
            relative_q, relative_error = q_div(absolute_q, target_q)
            relative_value: str | None = q_to_string(relative_q)
        else:
            relative_q = SCALE if actual_q > 0 else 0
            relative_error = Fraction(0)
            relative_value = None
        rebalance_relative_error_total_q += abs(relative_error)
        exceeds = relative_q > rebalance_tolerance_q
        detail = {
            "holder_id": holder_id,
            "actual_lease_share": _fixed(actual_q),
            "target_weight_share": _fixed(target_q),
            "deviation": _fixed(deviation_q),
            "absolute_deviation": _fixed(absolute_q),
            "relative_deviation": relative_value,
            "relative_deviation_q": relative_q,
            "relative_division_rounding_error_q": fraction_to_decimal_string(relative_error),
            "active_lock_count": sum(1 for lease in active if lease.holder_id == holder_id),
            "rule": "generate only when relative_deviation > declared relative tolerance",
            "triggered": exceeds,
            "tolerance": _fixed(rebalance_tolerance_q),
        }
        if exceeds:
            suggestions.append(detail)
        else:
            suppressed.append(detail)
        holder_rows.append(
            {
                "holder_id": holder_id,
                "declared_weight": _fixed(targets.get(holder_id, 0)),
                "actual_lease_weight_q": actual_weights.get(holder_id, 0),
                "actual_lease_share": _fixed(actual_q),
                "target_weight_share": _fixed(target_q),
                "deviation": _fixed(deviation_q),
                "relative_deviation": relative_value,
                "active_lock_count": detail["active_lock_count"],
            }
        )

    occupancy_q, occupancy_error = q_div_int(len(active_keys), registered_keys)
    share_terms_q = [actual_shares[holder] for holder in sorted(actual_shares)]
    fairness_terms_q = list(fairness_data["term_qs"])  # type: ignore[arg-type]

    input_error_q = Fraction(0)
    for row in target_input_trace:
        input_error_q += abs(Fraction(Decimal(str(row["input_quantization_error_q"]))))
    normalization_error_q = Fraction(
        Decimal(str(distribution["actual_trace"]["cumulative_abs_rounding_error_q"]))  # type: ignore[index]
    ) + Fraction(
        Decimal(str(distribution["target_trace"]["cumulative_abs_rounding_error_q"]))  # type: ignore[index]
    )
    term_rounding_error_q = Fraction(
        Decimal(str(fairness_data["summation_term_rounding_error_q"]))
    )
    cumulative_abs_error_q = (
        input_error_q + lease_input_error_q + normalization_error_q + term_rounding_error_q
    )
    all_derived_error_q = (
        cumulative_abs_error_q
        + margin_error_total_q
        + lease_share_error_total_q
        + abs(occupancy_error)
        + rebalance_relative_error_total_q
    )
    holder_count = max(1, len(set(actual_shares) | set(target_shares)))
    conservative_bound_q = holder_count * 4 + len(active)

    return {
        "ok": True,
        "code": "OK",
        "now_ms": now_ms,
        "revision": revision,
        "fixed_point": fixed_metadata(),
        "cluster": {
            "occupied_keys": len(active_keys),
            "registered_keys": registered_keys,
            "occupancy": _fixed(occupancy_q),
            "occupancy_rounding_error_q": fraction_to_decimal_string(occupancy_error),
            "formula": "occupied active lock keys / registered lock keys",
            "occupied_key_list": active_keys,
        },
        "leases": lease_rows,
        "holders": holder_rows,
        "weighted_fairness": fairness_data["fairness"],
        "fairness_detail": {
            "formula": fairness_data["formula"],
            "weighted_deviation": fairness_data["weighted_deviation"],
            "terms": fairness_data["terms"],
            "summation": SUMMATION_RULE,
            "kahan_compensation_q": fairness_data["kahan_compensation_q"],
        },
        "rebalance": {
            "relative_tolerance": _fixed(rebalance_tolerance_q),
            "rule": "relative deviation = abs(actual_share-target_share)/target_share; suggest iff > tolerance",
            "suggestions": suggestions,
            "suppressed": suppressed,
            "suggestion_count": len(suggestions),
        },
        "rounding": {
            "rule": ROUNDING_RULE,
            "target_input_quantization": target_input_trace,
            "lease_input_quantization": [
                {
                    "lock_key": row["lock_key"],
                    "holder_id": row["holder_id"],
                    "input": row["weight_input"],
                    "persisted_weight_q": row["weight"]["q"],  # type: ignore[index]
                    "requantized_q": row["weight_requantized_q"],
                    "input_quantization_error_q": row["weight_input_quantization_error_q"],
                }
                for row in lease_rows
            ],
            "actual_share_normalization": distribution["actual_trace"],
            "target_share_normalization": distribution["target_trace"],
            "fairness_multiplication_terms": fairness_data["terms"],
            "cumulative_abs_rounding_error_q_decimal": fraction_to_decimal_string(cumulative_abs_error_q),
            "cumulative_abs_rounding_error_q_units": fraction_to_decimal_string(cumulative_abs_error_q),
            "fairness_pipeline_cumulative_abs_error_q": fraction_to_decimal_string(
                cumulative_abs_error_q
            ),
            "margin_total_abs_error_q": fraction_to_decimal_string(margin_error_total_q),
            "lease_share_total_abs_error_q": fraction_to_decimal_string(
                lease_share_error_total_q
            ),
            "occupancy_abs_error_q": fraction_to_decimal_string(abs(occupancy_error)),
            "rebalance_relative_total_abs_error_q": fraction_to_decimal_string(
                rebalance_relative_error_total_q
            ),
            "all_derived_metrics_cumulative_abs_error_q": fraction_to_decimal_string(
                all_derived_error_q
            ),
            "conservative_bound_q_units": conservative_bound_q,
            "bound_formula": "per holder: <=1 Q unit in each normalization (x2), <=0.5 fairness-term rounding, <=0.5 declared-input quantization; plus <=1 Q unit per active lease weight input; reported bound is 4*n_holders + n_leases Q units",
            "canonical_summation_error_q_units": 0,
        },
        "numerical_cross_check": {
            "non_canonical": True,
            "actual_share_sum": {
                "canonical_q": sum(share_terms_q),
                "canonical_decimal": q_to_string(sum(share_terms_q)),
                **float_summation_cross_check(share_terms_q),
            },
            "fairness_term_sum": float_summation_cross_check(fairness_terms_q),
        },
        "cache": {
            "revision": revision,
            "basis": "active lease rows whose expires_at_ms > now_ms",
        },
    }


def build_arbitration(
    active_leases: Sequence[LeaseFact],
    holders: Sequence[HolderFact],
    candidates: Sequence[Candidate],
    *,
    now_ms: int,
    relative_tolerance_q: int,
) -> dict[str, object]:
    """Choose one candidate using projected weighted fairness.

    Candidate evaluation is independent of submission order: candidates are
    canonicalized by holder id before comparison.  Metrics within the relative
    tolerance are tied and resolved by holder-id lexicographic order.
    """

    if not candidates:
        raise ValueError("at least one candidate is required")
    if len({candidate.holder_id for candidate in candidates}) != len(candidates):
        raise ValueError("candidate holder IDs must be unique")
    ordered = sorted(candidates, key=lambda candidate: candidate.holder_id)
    active = _active_rows(active_leases, now_ms)
    declared = {holder.holder_id: holder.weight_q for holder in holders}
    current_actual: dict[str, int] = {}
    for lease in active:
        current_actual[lease.holder_id] = current_actual.get(lease.holder_id, 0) + lease.weight_q
    current_target = {holder: declared.get(holder, weight) for holder, weight in current_actual.items()}
    current_distribution = shared_distribution(current_actual, current_target)
    current_fairness = weighted_fairness_from_shares(
        current_distribution["actual_shares"],  # type: ignore[arg-type]
        current_distribution["target_shares"],  # type: ignore[arg-type]
    )

    evaluations: list[dict[str, object]] = []
    for candidate in ordered:
        projected_actual = dict(current_actual)
        projected_actual[candidate.holder_id] = projected_actual.get(candidate.holder_id, 0) + candidate.weight_q
        projected_declared = dict(declared)
        projected_declared[candidate.holder_id] = candidate.weight_q
        projected_target = {
            holder: projected_declared.get(holder, weight) for holder, weight in projected_actual.items()
        }
        distribution = shared_distribution(projected_actual, projected_target)
        fairness = weighted_fairness_from_shares(
            distribution["actual_shares"],  # type: ignore[arg-type]
            distribution["target_shares"],  # type: ignore[arg-type]
        )
        fairness_q = fairness["fairness"]["q"]  # type: ignore[index]
        actual_share_q = distribution["actual_shares"].get(candidate.holder_id, 0)  # type: ignore[union-attr]
        target_share_q = distribution["target_shares"].get(candidate.holder_id, 0)  # type: ignore[union-attr]
        evaluations.append(
            {
                "holder_id": candidate.holder_id,
                "weight": _fixed(candidate.weight_q),
                "weight_input": candidate.weight_text,
                "ttl_ms": candidate.ttl_ms,
                "projected_weighted_fairness": _fixed(fairness_q),
                "projected_actual_lease_share": _fixed(actual_share_q),
                "projected_target_weight_share": _fixed(target_share_q),
                "projected_deviation": _fixed(actual_share_q - target_share_q),
                "projected_fairness_q": fairness_q,
            }
        )

    winner = evaluations[0]
    for candidate in evaluations[1:]:
        comparison = compare_fairness_q(
            candidate["projected_fairness_q"],  # type: ignore[arg-type]
            winner["projected_fairness_q"],  # type: ignore[arg-type]
            relative_tolerance_q,
        )
        if comparison["relation"] == "greater":
            winner = candidate

    comparisons: list[dict[str, object]] = []
    for candidate in evaluations:
        if candidate is winner:
            continue
        result = compare_fairness_q(
            candidate["projected_fairness_q"],  # type: ignore[arg-type]
            winner["projected_fairness_q"],  # type: ignore[arg-type]
            relative_tolerance_q,
        )
        if result["within_tolerance"]:
            basis = "metrics tied within relative tolerance -> holder_id lexicographic ascending"
        elif result["relation"] == "greater":
            basis = "left projected fairness is higher"
        else:
            basis = "right projected fairness is higher"
        comparisons.append(
            {
                "left_holder_id": candidate["holder_id"],
                "right_holder_id": winner["holder_id"],
                "left_projected_fairness": candidate["projected_weighted_fairness"],
                "right_projected_fairness": winner["projected_weighted_fairness"],
                "absolute_difference": _fixed(result["absolute_difference_q"]),
                "relative_difference": result["relative_difference"],
                "tolerance": _fixed(relative_tolerance_q),
                "within_tolerance": result["within_tolerance"],
                "final_basis": basis,
            }
        )

    selected_public = {
        "holder_id": winner["holder_id"],
        "weight": winner["weight"],
        "weight_input": winner["weight_input"],
        "ttl_ms": winner["ttl_ms"],
        "projected_weighted_fairness": winner["projected_weighted_fairness"],
    }
    return {
        "winner": selected_public,
        "candidates": evaluations,
        "comparisons": comparisons,
        "tolerance": {
            "q": relative_tolerance_q,
            "decimal": q_to_string(relative_tolerance_q),
            "rule": "tie iff abs(a-b)*SCALE <= tolerance_q*max(abs(a),abs(b))",
        },
        "selection_rule": "highest projected weighted fairness; within relative tolerance holder_id ascending",
        "current_weighted_fairness": current_fairness["fairness"],
        "submission_order_independent": True,
    }


def compare_fairness_q(left_q: int, right_q: int, tolerance_q: int) -> dict[str, object]:
    difference = left_q - right_q
    absolute = abs(difference)
    maximum = max(abs(left_q), abs(right_q))
    threshold_times_scale = tolerance_q * maximum
    within = absolute * SCALE <= threshold_times_scale
    if within:
        relation = "tie"
    elif difference > 0:
        relation = "greater"
    else:
        relation = "less"
    if maximum == 0:
        relative = "0.000000000"
    else:
        relative_q, _ = q_div(absolute, maximum)
        relative = q_to_string(relative_q)
    return {
        "left_q": left_q,
        "right_q": right_q,
        "absolute_difference_q": absolute,
        "relative_difference": relative,
        "tolerance_q": tolerance_q,
        "within_tolerance": within,
        "relation": relation,
    }


def numerical_stability_benchmark(
    terms_q: Iterable[int], ttl_values_ms: Iterable[int]
) -> dict[str, object]:
    terms = list(terms_q)
    ttls = sorted(ttl_values_ms)
    span_ratio = (max(ttls) / min(ttls)) if ttls and min(ttls) > 0 else 0.0
    return {
        "mixed_magnitude_terms": float_summation_cross_check(terms),
        "ttl_values_ms": ttls,
        "ttl_span_ratio": span_ratio,
        "ttl_three_order_statement": span_ratio >= 1000,
        "repeatability_contract": "same persisted rows, same now_ms, same canonical integers -> bit-identical Q outputs",
    }

