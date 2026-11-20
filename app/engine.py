from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from fractions import Fraction
from typing import Any, Iterable

from app.fixed import (
    ROUNDING_MODE,
    SCALE,
    TrackedFixed,
    abs_fixed,
    add_fixed,
    clamp_unit,
    compare_ge,
    compare_in_interval,
    compare_le,
    div_to_dimensionless,
    fixed_from_decimal,
    fixed_from_int,
    fixed_one,
    format_error_bound,
    format_fixed,
    integer_ceil_fraction,
    kahan_sum_fixed,
    mul_dimensionless,
    naive_sum_fixed,
    relative_tolerance_micros,
    sub_fixed,
)
from app.snapshot import ComponentConfig, Snapshot


@dataclass
class RoundingLedger:
    events: list[dict[str, Any]] = field(default_factory=list)
    _sequence: int = 0

    def record(
        self,
        *,
        component_id: str,
        stage: str,
        operation: str,
        dimension: str,
        result: TrackedFixed,
        increment: Fraction,
        rounding_applied: bool = True,
    ) -> None:
        self._sequence += 1
        self.events.append(
            {
                "step": self._sequence,
                "component_id": component_id,
                "stage": stage,
                "operation": operation,
                "dimension": dimension,
                "rounding_mode": ROUNDING_MODE if rounding_applied else "EXACT",
                "result_micros": result.micros,
                "result_fixed": format_fixed(result.micros),
                "step_error_bound_micros": integer_ceil_fraction(max(Fraction(0), increment)),
                "step_error_bound": format_error_bound(max(Fraction(0), increment)),
                "cumulative_error_bound_micros": result.error_micros,
                "cumulative_error_bound": format_error_bound(result.error),
            }
        )

    @property
    def operation_count(self) -> int:
        return len(self.events)


def _convert(ledger: RoundingLedger, component_id: str, stage: str, raw: Decimal, dimension: str) -> TrackedFixed:
    result = fixed_from_decimal(raw)
    ledger.record(
        component_id=component_id,
        stage=stage,
        operation="decimal_to_fixed",
        dimension=dimension,
        result=result,
        increment=result.error,
        rounding_applied=result.error != 0,
    )
    return result


def _divide(
    ledger: RoundingLedger,
    component_id: str,
    stage: str,
    numerator: TrackedFixed,
    denominator: TrackedFixed,
    dimension: str,
) -> TrackedFixed:
    input_error = numerator.error + denominator.error
    result = div_to_dimensionless(numerator, denominator)
    ledger.record(
        component_id=component_id,
        stage=stage,
        operation="divide_fixed",
        dimension=dimension,
        result=result,
        increment=result.error - input_error,
    )
    return result


def _multiply(
    ledger: RoundingLedger,
    component_id: str,
    stage: str,
    left: TrackedFixed,
    right: TrackedFixed,
    dimension: str,
) -> TrackedFixed:
    input_error = left.error + right.error
    result = mul_dimensionless(left, right)
    ledger.record(
        component_id=component_id,
        stage=stage,
        operation="multiply_dimensionless",
        dimension=dimension,
        result=result,
        increment=result.error - input_error,
    )
    return result


def _subtract(
    ledger: RoundingLedger,
    component_id: str,
    stage: str,
    left: TrackedFixed,
    right: TrackedFixed,
    dimension: str,
) -> TrackedFixed:
    result = sub_fixed(left, right)
    ledger.record(
        component_id=component_id,
        stage=stage,
        operation="subtract_exact",
        dimension=dimension,
        result=result,
        increment=Fraction(0),
        rounding_applied=False,
    )
    return result


def _add(
    ledger: RoundingLedger,
    component_id: str,
    stage: str,
    left: TrackedFixed,
    right: TrackedFixed,
    dimension: str,
) -> TrackedFixed:
    result = add_fixed(left, right)
    ledger.record(
        component_id=component_id,
        stage=stage,
        operation="add_exact",
        dimension=dimension,
        result=result,
        increment=Fraction(0),
        rounding_applied=False,
    )
    return result


def _fixed_json(value: TrackedFixed) -> dict[str, Any]:
    return {
        "value": format_fixed(value.micros),
        "micros": value.micros,
        "cumulative_error": format_error_bound(value.error),
        "cumulative_error_micros": value.error_micros,
        "error_rounding": "CEILING_TO_MICRO",
    }


def _effective_interval_json(value_micros: int, lower: int, upper: int, rel_tol_micros: int) -> dict[str, Any]:
    ok, effective_lower, effective_upper = compare_in_interval(value_micros, lower, upper, rel_tol_micros)
    return {
        "inside": ok,
        "declared_interval": [format_fixed(lower), format_fixed(upper)],
        "effective_interval": [format_fixed(effective_lower), format_fixed(effective_upper)],
        "relative_tolerance": format_fixed(rel_tol_micros),
        "lower_tolerance_micros": relative_tolerance_micros(lower, value_micros, rel_tol_micros),
        "upper_tolerance_micros": relative_tolerance_micros(upper, value_micros, rel_tol_micros),
    }


def calculate_metrics(snapshot: Snapshot) -> dict[str, Any]:
    """Calculate all metrics using one deterministic fixed-point and order contract."""
    ledger = RoundingLedger()
    policy_relative_tolerance = fixed_from_decimal(snapshot.policy.relative_tolerance)
    policy_min_consistency = fixed_from_decimal(snapshot.policy.min_weighted_consistency)
    rel_tol_micros = policy_relative_tolerance.micros
    one = fixed_one()

    converted: dict[str, dict[str, TrackedFixed]] = {}
    for component in snapshot.ordered_components:
        component_id = component.component_id
        converted[component_id] = {
            "quota": _convert(ledger, component_id, "input.quota", component.quota, "quota_units"),
            "ratio": _convert(ledger, component_id, "input.ratio", component.ratio, "dimensionless"),
            "ratio_tolerance": _convert(
                ledger,
                component_id,
                "input.ratio_tolerance",
                component.ratio_tolerance,
                "dimensionless",
            ),
            "threshold": _convert(ledger, component_id, "input.threshold", component.threshold, "threshold_units"),
            "usage": _convert(ledger, component_id, "input.usage", component.usage, "threshold_units"),
            "min_headroom_ratio": _convert(
                ledger,
                component_id,
                "input.min_headroom_ratio",
                component.min_headroom_ratio,
                "dimensionless",
            ),
            "limit": fixed_from_int(component.limit),
            "weight": _convert(ledger, component_id, "input.weight", component.weight, "weight_units"),
        }

    ordered_ids = [component.component_id for component in snapshot.ordered_components]
    total_quota = kahan_sum_fixed([converted[component_id]["quota"] for component_id in ordered_ids])
    total_weight = kahan_sum_fixed([converted[component_id]["weight"] for component_id in ordered_ids])
    total_quota_value = TrackedFixed(total_quota.total_micros, total_quota.error)
    total_weight_value = TrackedFixed(total_weight.total_micros, total_weight.error)
    ledger.record(
        component_id="<all>",
        stage="normalization.total_quota",
        operation="kahan_sum_fixed",
        dimension="quota_units",
        result=total_quota_value,
        increment=total_quota.error,
        rounding_applied=False,
    )
    ledger.record(
        component_id="<all>",
        stage="normalization.total_weight",
        operation="kahan_sum_fixed",
        dimension="weight_units",
        result=total_weight_value,
        increment=total_weight.error,
        rounding_applied=False,
    )

    component_metrics: dict[str, dict[str, Any]] = {}
    checks: list[dict[str, Any]] = []
    differences: list[dict[str, Any]] = []
    contributions: list[TrackedFixed] = []
    ranking: list[tuple[bool, int, str]] = []

    for component in snapshot.ordered_components:
        component_id = component.component_id
        values = converted[component_id]
        prefix = f"component.{component_id}"

        quota_share = _divide(
            ledger, component_id, f"{prefix}.quota_share", values["quota"], total_quota_value, "dimensionless"
        )
        headroom_abs = _subtract(
            ledger, component_id, f"{prefix}.headroom_abs", values["threshold"], values["usage"], "threshold_units"
        )
        headroom_ratio = _divide(
            ledger, component_id, f"{prefix}.headroom_ratio", headroom_abs, values["threshold"], "dimensionless"
        )
        normalized_weight = _divide(
            ledger, component_id, f"{prefix}.normalized_weight", values["weight"], total_weight_value, "dimensionless"
        )
        limit_utilization = _divide(
            ledger, component_id, f"{prefix}.limit_utilization", values["usage"], values["limit"], "dimensionless"
        )

        declared_lower = _subtract(
            ledger,
            component_id,
            f"{prefix}.declared_ratio_lower",
            values["ratio"],
            values["ratio_tolerance"],
            "dimensionless",
        )
        declared_upper = _add(
            ledger,
            component_id,
            f"{prefix}.declared_ratio_upper",
            values["ratio"],
            values["ratio_tolerance"],
            "dimensionless",
        )
        deviation = abs_fixed(
            _subtract(
                ledger,
                component_id,
                f"{prefix}.ratio_deviation",
                quota_share,
                values["ratio"],
                "dimensionless",
            )
        )
        normalized_gap = _divide(
            ledger,
            component_id,
            f"{prefix}.normalized_gap",
            deviation,
            values["ratio_tolerance"],
            "dimensionless",
        )
        share_score = clamp_unit(
            _subtract(ledger, component_id, f"{prefix}.share_score", one, normalized_gap, "dimensionless")
        )
        contribution = _multiply(
            ledger,
            component_id,
            f"{prefix}.weighted_contribution",
            normalized_weight,
            share_score,
            "dimensionless",
        )
        contributions.append(contribution)

        ratio_interval = _effective_interval_json(
            quota_share.micros, declared_lower.micros, declared_upper.micros, rel_tol_micros
        )
        headroom_ok = compare_ge(
            headroom_ratio.micros, values["min_headroom_ratio"].micros, rel_tol_micros
        )
        limit_ok = compare_le(limit_utilization.micros, one.micros, rel_tol_micros)
        component_ok = ratio_interval["inside"] and headroom_ok and limit_ok
        ranking.append((component_ok, share_score.micros, component_id))

        checks.extend(
            [
                {
                    "check": "quota_share_interval",
                    "component_id": component_id,
                    "ok": ratio_interval["inside"],
                    "actual": format_fixed(quota_share.micros),
                    "declared_interval": ratio_interval["declared_interval"],
                    "effective_interval": ratio_interval["effective_interval"],
                },
                {
                    "check": "minimum_headroom_ratio",
                    "component_id": component_id,
                    "ok": headroom_ok,
                    "actual": format_fixed(headroom_ratio.micros),
                    "minimum": format_fixed(values["min_headroom_ratio"].micros),
                    "relative_tolerance": format_fixed(rel_tol_micros),
                },
                {
                    "check": "integer_limit_utilization",
                    "component_id": component_id,
                    "ok": limit_ok,
                    "actual": format_fixed(limit_utilization.micros),
                    "maximum": "1.000000",
                    "relative_tolerance": format_fixed(rel_tol_micros),
                },
            ]
        )

        ratio_violation = max(
            declared_lower.micros - quota_share.micros,
            quota_share.micros - declared_upper.micros,
            0,
        )
        headroom_violation = max(values["min_headroom_ratio"].micros - headroom_ratio.micros, 0)
        limit_violation = max(limit_utilization.micros - one.micros, 0)
        differences.append(
            {
                "component_id": component_id,
                "component_consistent": component_ok,
                "quota_share": format_fixed(quota_share.micros),
                "declared_ratio": format_fixed(values["ratio"].micros),
                "ratio_violation": format_fixed(ratio_violation),
                "headroom_ratio": format_fixed(headroom_ratio.micros),
                "headroom_violation": format_fixed(headroom_violation),
                "limit_utilization": format_fixed(limit_utilization.micros),
                "limit_violation": format_fixed(limit_violation),
                "weight": format_fixed(values["weight"].micros),
                "normalized_weight": format_fixed(normalized_weight.micros),
                "share_score": format_fixed(share_score.micros),
            }
        )

        component_metrics[component_id] = {
            "quota": format_fixed(values["quota"].micros),
            "quota_share": {
                **_fixed_json(quota_share),
                "dimension": "dimensionless",
                **ratio_interval,
                "declared_ratio": format_fixed(values["ratio"].micros),
                "declared_ratio_tolerance": format_fixed(values["ratio_tolerance"].micros),
            },
            "threshold_headroom": {
                **_fixed_json(headroom_abs),
                "dimension": "threshold_units",
            },
            "headroom_ratio": {
                **_fixed_json(headroom_ratio),
                "dimension": "dimensionless",
                "minimum": format_fixed(values["min_headroom_ratio"].micros),
                "meets_minimum_with_tolerance": headroom_ok,
            },
            "limit": values["limit"].micros // SCALE,
            "limit_utilization": {
                **_fixed_json(limit_utilization),
                "dimension": "dimensionless",
                "maximum": "1.000000",
                "meets_maximum_with_tolerance": limit_ok,
            },
            "normalized_weight": {
                **_fixed_json(normalized_weight),
                "dimension": "dimensionless",
            },
            "share_score": {
                **_fixed_json(share_score),
                "dimension": "dimensionless",
                "formula": "clamp(1 - abs(quota_share - ratio) / ratio_tolerance, 0, 1)",
            },
            "weighted_contribution": {
                **_fixed_json(contribution),
                "dimension": "dimensionless",
            },
            "component_consistent": component_ok,
        }

    weighted = kahan_sum_fixed(contributions)
    weighted_value = TrackedFixed(weighted.total_micros, weighted.error)
    naive_weighted = naive_sum_fixed(contributions)
    ledger.record(
        component_id="<all>",
        stage="weighted_consistency.total",
        operation="kahan_sum_fixed",
        dimension="dimensionless",
        result=weighted_value,
        increment=weighted.error,
        rounding_applied=False,
    )
    weighted_ok = compare_ge(weighted.total_micros, policy_min_consistency.micros, rel_tol_micros)
    checks.append(
        {
            "check": "minimum_weighted_consistency",
            "component_id": "<all>",
            "ok": weighted_ok,
            "actual": format_fixed(weighted.total_micros),
            "minimum": format_fixed(policy_min_consistency.micros),
            "relative_tolerance": format_fixed(rel_tol_micros),
        }
    )

    failed_checks = [check for check in checks if not check["ok"]]
    ranking.sort(key=lambda item: (item[0], item[1], item[2]))
    critical_component = ranking[0][2] if ranking else None

    error_cover: dict[str, dict[str, Any]] = {}
    for component_id, metrics in component_metrics.items():
        for metric_name in ("quota_share", "headroom_ratio", "limit_utilization"):
            metric = metrics[metric_name]
            tolerance = relative_tolerance_micros(
                metric["micros"], metric["micros"], rel_tol_micros
            )
            error_cover[f"{component_id}.{metric_name}"] = {
                "error_bound": metric["cumulative_error"],
                "error_bound_micros": metric["cumulative_error_micros"],
                "relative_tolerance_micros": tolerance,
                "covered": metric["cumulative_error_micros"] <= tolerance,
            }
    weighted_tolerance = relative_tolerance_micros(
        weighted.total_micros, weighted.total_micros, rel_tol_micros
    )
    error_cover["weighted_consistency"] = {
        "error_bound": format_error_bound(weighted.error),
        "error_bound_micros": integer_ceil_fraction(weighted.error),
        "relative_tolerance_micros": weighted_tolerance,
        "covered": integer_ceil_fraction(weighted.error) <= weighted_tolerance,
    }

    status = "CONSISTENT" if weighted_ok and not any(item[0] is False for item in ranking) else "INCONSISTENT"
    precision_contract = {
        "representation": "signed_integer_micros",
        "scale": SCALE,
        "decimal_places": 6,
        "rounding_mode": ROUNDING_MODE,
        "ratio_rounding": "ROUND_HALF_EVEN at every division and dimensionless multiplication",
        "summation": "Kahan-compensated integer accumulation over component_id lexicographic order",
        "order_policy": "component_id_lexicographic",
        "decimal_serialization": "fixed 6-place strings; integer micros retained separately",
        "error_bound": "conservative absolute bound in micros, propagated through each operation and ceil-rounded",
    }
    result = {
        "snapshot_version": snapshot.snapshot_version,
        "generated_at": snapshot.generated_at,
        "checksum": snapshot.checksum,
        "precision_contract": precision_contract,
        "components": component_metrics,
        "weighted_consistency": {
            **_fixed_json(weighted_value),
            "dimension": "dimensionless",
            "method": "kahan_sum_fixed",
            "canonical_order": ordered_ids,
            "naive_fixed_value": format_fixed(naive_weighted.total_micros),
            "kahan_compensation": format_fixed(weighted.compensation_micros),
            "minimum_required": format_fixed(policy_min_consistency.micros),
            "meets_minimum_with_tolerance": weighted_ok,
        },
        "conclusion": {
            "status": status,
            "consistent": status == "CONSISTENT",
            "checks": checks,
            "failed_checks": failed_checks,
            "critical_component": critical_component,
            "tie_break": "component_id_lexicographic",
            "decision_basis": "fixed-point central values with relative-tolerance boundary comparisons",
            "relative_tolerance": format_fixed(rel_tol_micros),
        },
        "difference_report": differences,
        "error_cover": error_cover,
        "all_error_bounds_covered": all(item["covered"] for item in error_cover.values()),
    }

    explanation = {
        "snapshot_version": snapshot.snapshot_version,
        "checksum": snapshot.checksum,
        "precision_contract": precision_contract,
        "rounding": {
            "operation_count": ledger.operation_count,
            "events": ledger.events,
            "per_metric_error_bounds": error_cover,
            "propagation_note": "Each operation's cumulative bound includes input bounds, first-order propagation, and at most one half-micro rounding residual; reported bounds are ceiling-rounded to micros.",
        },
        "comparison_contract": {
            "relative_tolerance": format_fixed(rel_tol_micros),
            "formula": "accept when value >= lower - ceil(rel_tol*max(|lower|,|value|,1)) or value <= upper + ceil(rel_tol*max(|upper|,|value|,1))",
            "zero_reference": "1.000000 (one whole unit)",
            "tie_break": "component_id_lexicographic",
            "error_bounds_are_reported_separately": True,
        },
        "sum_stability": {
            "method": "kahan_sum_fixed",
            "canonical_order": ordered_ids,
            "naive_fixed_sum": format_fixed(naive_weighted.total_micros),
            "kahan_fixed_sum": format_fixed(weighted.total_micros),
            "kahan_compensation_micros": weighted.compensation_micros,
            "integer_sum_error_bound": "0.000000",
            "operation_order_independent": True,
        },
    }
    return {"metrics": result, "explanation": explanation}


def serialized_metric_views(result: dict[str, Any]) -> Iterable[tuple[str, int, int, int, str]]:
    """Yield DB-ready component metric rows: id, share, headroom, weight, JSON."""
    import json

    for component_id in sorted(result["components"]):
        metric = result["components"][component_id]
        yield (
            component_id,
            metric["quota_share"]["micros"],
            metric["headroom_ratio"]["micros"],
            metric["normalized_weight"]["micros"],
            json.dumps(metric, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
        )
