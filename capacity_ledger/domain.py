"""Integer-only cost derivation and tolerance-aware budget decisions."""

from __future__ import annotations

from typing import Any

from .fixedpoint import (
    PPM,
    SCALE,
    exact_ratio_text,
    format_cents,
    format_fixed,
    format_ppm,
    raw_exceeds,
    relative_exceeds,
    round_half_up_ratio,
)
from .models import CapacitySnapshot, ServiceCapacity


def service_cost_cents(service: ServiceCapacity) -> int:
    """Cents per hour; exact integer arithmetic and no intermediate rounding."""

    per_replica = (
        service.cores_per_replica * service.price_core_cents_per_core_hour
        + service.memory_gb_per_replica * service.price_memory_cents_per_gb_hour
    )
    return service.replicas * per_replica


def _constraint(
    *,
    constraint: str,
    kind: str,
    service: str | None,
    observed_cents: int | None = None,
    observed_ppm: int | None = None,
    limit_cents: int | None = None,
    limit_ppm: int | None = None,
    tolerance_ppm: int,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "constraint": constraint,
        "kind": kind,
        "tolerance_ppm": tolerance_ppm,
        "comparison": "EXACT_CROSS_MULTIPLY_RELATIVE_TOLERANCE",
    }
    if service is not None:
        value["service"] = service
    if observed_cents is not None:
        value["observed_cents_per_hour"] = observed_cents
        value["observed_cents_per_hour_fixed"] = format_cents(observed_cents)
    if observed_ppm is not None:
        value["observed_ppm"] = observed_ppm
        value["observed_fixed"] = format_ppm(observed_ppm)
    if limit_cents is not None:
        value["limit_cents_per_hour"] = limit_cents
        value["limit_cents_per_hour_fixed"] = format_cents(limit_cents)
    if limit_ppm is not None:
        value["limit_ppm"] = limit_ppm
        value["limit_fixed"] = format_ppm(limit_ppm)
    return value


def _decide_constraint(
    violations: list[dict[str, Any]],
    within_tolerance: list[dict[str, Any]],
    *,
    actual_numerator: int,
    actual_denominator: int,
    limit_numerator: int,
    limit_denominator: int,
    tolerance_ppm: int,
    value: dict[str, Any],
) -> None:
    if relative_exceeds(
        actual_numerator,
        actual_denominator,
        limit_numerator,
        limit_denominator,
        tolerance_ppm,
    ):
        violations.append(value)
    elif raw_exceeds(
        actual_numerator,
        actual_denominator,
        limit_numerator,
        limit_denominator,
    ):
        within_tolerance.append(value)


def calculate_metrics(
    snapshot: CapacitySnapshot,
    *,
    prior_metrics: dict[str, Any] | None = None,
    changed_services: set[str] | None = None,
    force_full: bool = False,
) -> dict[str, Any]:
    """Derive all costs and decisions from a snapshot.

    The incremental path reuses leaf service costs from ``prior_metrics`` and
    recomputes only changed leaves plus aggregates.  Both paths evaluate the
    same exact aggregate formulas.  A full path is used for tests and whenever
    the SLO normalization base changes.
    """

    services = snapshot.services
    reuse_leaf_costs = (
        not force_full
        and prior_metrics is not None
        and changed_services is not None
        and changed_services.issubset(services)
    )

    costs: dict[str, int] = {}
    if reuse_leaf_costs:
        prior_services = prior_metrics.get("services", {})
        for name, service in services.items():
            if name in changed_services:
                costs[name] = service_cost_cents(service)
            else:
                prior_cost = prior_services.get(name, {}).get("cost_cents_per_hour")
                if not isinstance(prior_cost, int) or isinstance(prior_cost, bool):
                    costs[name] = service_cost_cents(service)
                else:
                    costs[name] = prior_cost
    else:
        for name, service in services.items():
            costs[name] = service_cost_cents(service)

    total_cost = sum(costs.values())
    weight_denominator = sum(service.slo_target_ppm for service in services.values())
    if weight_denominator <= 0:
        raise ValueError("SLO weight denominator must be positive")
    weighted_numerator = sum(
        costs[name] * service.slo_target_ppm for name, service in services.items()
    )
    weighted_index_micro_cents = round_half_up_ratio(
        weighted_numerator * SCALE, weight_denominator
    )
    weighted_rounding_error_numerator = (
        weighted_index_micro_cents * weight_denominator
        - weighted_numerator * SCALE
    )

    service_metrics: dict[str, Any] = {}
    share_rounding: list[dict[str, Any]] = []
    share_ppm_total = 0
    for name, service in sorted(services.items()):
        cost = costs[name]
        if total_cost == 0:
            share_ppm = 0
            share_error_numerator = 0
        else:
            share_ppm = round_half_up_ratio(cost * PPM, total_cost)
            share_error_numerator = share_ppm * total_cost - cost * PPM
        share_ppm_total += share_ppm
        service_metrics[name] = {
            "replicas": service.replicas,
            "cores_per_replica": service.cores_per_replica,
            "memory_gb_per_replica": service.memory_gb_per_replica,
            "cost_cents_per_hour": cost,
            "cost_cents_per_hour_fixed": format_cents(cost),
            "share_ppm": share_ppm,
            "share_fixed": format_ppm(share_ppm),
            "share_exact_fraction": exact_ratio_text(cost, total_cost),
            "budget_cents_per_hour": service.budget_cents_per_hour,
            "budget_cents_per_hour_fixed": format_cents(service.budget_cents_per_hour),
            "budget_remaining_cents_per_hour": service.budget_cents_per_hour - cost,
            "budget_remaining_cents_per_hour_fixed": format_cents(
                service.budget_cents_per_hour - cost
            ),
            "max_cost_share_ppm": service.max_cost_share_ppm,
            "max_cost_share_fixed": format_ppm(service.max_cost_share_ppm),
            "slo_target_ppm": service.slo_target_ppm,
            "slo_target_fixed": format_ppm(service.slo_target_ppm),
            "rounding": {
                "share_method": "ROUND_HALF_UP",
                "share_scale": PPM,
                "share_error_numerator": share_error_numerator,
                "share_error_denominator": total_cost,
            },
        }
        share_rounding.append(
            {
                "service": name,
                "rounded_ppm": share_ppm,
                "error_numerator": share_error_numerator,
                "error_denominator": total_cost,
            }
        )

    cumulative_share_error_ppm = share_ppm_total - PPM if total_cost else 0
    aggregate = {
        "total_cost_cents_per_hour": total_cost,
        "total_cost_cents_per_hour_fixed": format_cents(total_cost),
        "total_budget_cents_per_hour": snapshot.total_budget_cents_per_hour,
        "total_budget_cents_per_hour_fixed": format_cents(
            snapshot.total_budget_cents_per_hour
        ),
        "total_budget_remaining_cents_per_hour": (
            snapshot.total_budget_cents_per_hour - total_cost
        ),
        "total_budget_remaining_cents_per_hour_fixed": format_cents(
            snapshot.total_budget_cents_per_hour - total_cost
        ),
        "weight_denominator_ppm": weight_denominator,
        "weighted_cost_numerator": weighted_numerator,
        "weighted_cost_index_micro_cents": weighted_index_micro_cents,
        "weighted_cost_index_fixed_cents": format_fixed(weighted_index_micro_cents),
        "weighted_cost_index_exact_fraction": exact_ratio_text(
            weighted_numerator * SCALE, weight_denominator
        ),
    }

    violations: list[dict[str, Any]] = []
    within_tolerance: list[dict[str, Any]] = []
    for name, service in sorted(services.items()):
        cost = costs[name]
        service_budget = _constraint(
            constraint=f"service_budget:{name}",
            kind="SERVICE_BUDGET",
            service=name,
            observed_cents=cost,
            limit_cents=service.budget_cents_per_hour,
            tolerance_ppm=snapshot.tolerance_ppm,
        )
        _decide_constraint(
            violations,
            within_tolerance,
            actual_numerator=cost,
            actual_denominator=1,
            limit_numerator=service.budget_cents_per_hour,
            limit_denominator=1,
            tolerance_ppm=snapshot.tolerance_ppm,
            value=service_budget,
        )

        share_ppm = service_metrics[name]["share_ppm"]
        share_constraint = _constraint(
            constraint=f"service_cost_share:{name}",
            kind="SERVICE_COST_SHARE",
            service=name,
            observed_ppm=share_ppm,
            limit_ppm=service.max_cost_share_ppm,
            tolerance_ppm=snapshot.tolerance_ppm,
        )
        share_constraint["observed_exact_fraction"] = exact_ratio_text(cost, total_cost)
        if total_cost == 0:
            share_violates = False
            share_raw_exceeds = False
        else:
            share_violates = relative_exceeds(
                cost,
                total_cost,
                service.max_cost_share_ppm,
                PPM,
                snapshot.tolerance_ppm,
            )
            share_raw_exceeds = raw_exceeds(
                cost, total_cost, service.max_cost_share_ppm, PPM
            )
        if share_violates:
            violations.append(share_constraint)
        elif share_raw_exceeds:
            within_tolerance.append(share_constraint)

    total_constraint = _constraint(
        constraint="total_budget",
        kind="TOTAL_BUDGET",
        service=None,
        observed_cents=total_cost,
        limit_cents=snapshot.total_budget_cents_per_hour,
        tolerance_ppm=snapshot.tolerance_ppm,
    )
    _decide_constraint(
        violations,
        within_tolerance,
        actual_numerator=total_cost,
        actual_denominator=1,
        limit_numerator=snapshot.total_budget_cents_per_hour,
        limit_denominator=1,
        tolerance_ppm=snapshot.tolerance_ppm,
        value=total_constraint,
    )

    violations.sort(key=lambda item: item["constraint"])
    within_tolerance.sort(key=lambda item: item["constraint"])
    verdict = {
        "status": "VIOLATION" if violations else "PASS",
        "violations": violations,
        "within_tolerance": within_tolerance,
        "ordering": "stable lexicographic constraint id",
        "primary_constraint": violations[0]["constraint"] if violations else None,
        "decision_method": "EXACT_INTEGER_RATIOS_WITH_RELATIVE_TOLERANCE",
    }

    rounding = {
        "contract": "INTEGER_ONLY_DECISIONS",
        "steps": [
            {
                "artifact": "service_cost",
                "method": "EXACT_INTEGER_MULTIPLY_ADD",
                "rounding_units": 0,
            },
            {
                "artifact": "total_cost",
                "method": "EXACT_INTEGER_SUM",
                "rounding_units": 0,
            },
            {
                "artifact": "service_share",
                "method": "ROUND_HALF_UP",
                "scale": PPM,
                "per_service": share_rounding,
                "cumulative_error_ppm": cumulative_share_error_ppm,
            },
            {
                "artifact": "weighted_cost_index",
                "method": "ROUND_HALF_UP",
                "scale": SCALE,
                "error_numerator": weighted_rounding_error_numerator,
                "error_denominator": weight_denominator,
            },
            {
                "artifact": "budget_decisions",
                "method": "EXACT_CROSS_MULTIPLY_RELATIVE_TOLERANCE",
                "tolerance_ppm": snapshot.tolerance_ppm,
                "rounding_units": 0,
            },
        ],
        "cumulative_rounding_error_ppm": cumulative_share_error_ppm,
        "note": (
            "Rounded shares and weighted index are presentation values only; "
            "all budget and share decisions use exact integer ratios."
        ),
    }

    return {
        "services": service_metrics,
        "aggregate": aggregate,
        "verdict": verdict,
        "rounding": rounding,
        "exact_weights": {
            "weight_denominator_ppm": weight_denominator,
            "weighted_cost_numerator": weighted_numerator,
        },
    }

