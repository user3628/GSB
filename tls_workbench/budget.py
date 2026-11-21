"""Handshake budget accumulation, overflow adjudication and downgrade policy."""

from __future__ import annotations

from dataclasses import dataclass

from . import fixedpoint as fp
from .errors import Codes, WorkbenchError
from .kahan import kahan_sum_fixed, summation_audit

# Non-critical diagnostic items that downgrade is allowed to skip.
DOWNGRADEABLE_ITEMS = frozenset(
    {
        "STRENGTH_SCORE_DETAIL",
        "CHAIN_MARGIN_NARRATIVE",
        "SUMMATION_ORDER_AUDIT",
    }
)
# Items downgrade is never allowed to skip.
PROTECTED_ITEMS = frozenset(
    {
        "CERT_SIGNATURE_VERIFICATION",
        "CERT_VALIDITY_VERIFICATION",
        "PATH_CONSTRUCTION",
    }
)


@dataclass(frozen=True)
class StageEstimate:
    stage: str
    base_ms_fixed: int
    chain_overhead_fixed: int
    skipped: bool = False


def stage_estimate(stage: str, base_ms_fixed: int, chain_overhead_ms_fixed: int,
                   skipped: bool = False) -> StageEstimate:
    return StageEstimate(
        stage=stage,
        base_ms_fixed=int(base_ms_fixed),
        chain_overhead_fixed=int(chain_overhead_ms_fixed),
        skipped=skipped,
    )


def accumulate_budget(estimates: list[StageEstimate]) -> dict:
    active = [item for item in estimates if not item.skipped]
    stage_totals = [
        fp.add(item.base_ms_fixed, item.chain_overhead_fixed) for item in active
    ]
    kahan = kahan_sum_fixed(stage_totals)
    total = kahan.total
    shares = []
    for item, stage_total in zip(active, stage_totals):
        share = fp.from_fraction(stage_total, 1) if total == 0 else fp.div(
            fp.mul(stage_total, fp.from_int(100)), total
        )
        shares.append(
            {
                "stage": item.stage,
                "base_ms_fixed": item.base_ms_fixed,
                "chain_overhead_ms_fixed": item.chain_overhead_fixed,
                "stage_total_ms_fixed": stage_total,
                "stage_total_ms": fp.to_str(stage_total),
                "share_percent_fixed": share,
                "share_percent": fp.to_str(share),
                "skipped": item.skipped,
            }
        )
    share_sum = kahan_sum_fixed([item["share_percent_fixed"] for item in shares])
    return {
        "total_budget_ms_fixed": total,
        "total_budget_ms": fp.to_str(total),
        "kahan_compensation": kahan.compensation,
        "share_sum_percent": fp.to_str(share_sum.total),
        "stages": shares,
        "kahan_steps": list(kahan.per_step),
        "rounding": fp.ROUNDING,
    }


def naive_vs_compensated_audit(estimates: list[StageEstimate]) -> dict:
    active = [item for item in estimates if not item.skipped]
    stage_totals_fixed = [
        fp.add(item.base_ms_fixed, item.chain_overhead_fixed) for item in active
    ]
    stage_totals_float = [fp.to_float(value) for value in stage_totals_fixed]
    audit = summation_audit(stage_totals_float, stage_totals_fixed)
    audit["stages"] = [item.stage for item in active]
    # Reverse and interleaved orderings prove the canonical conclusion is
    # independent of floating-point evaluation order.
    audit["reverse_order_naive_total"] = repr(
        sum(reversed(stage_totals_float))
    )
    audit["interleaved_order_naive_total"] = repr(
        sum(stage_totals_float[::2]) + sum(stage_totals_float[1::2])
    )
    return audit


def adjudicate(budget: dict, total_limit_ms_fixed: int,
               per_stage_limits: dict[str, int]) -> dict:
    violations = []
    for stage in budget["stages"]:
        limit = per_stage_limits.get(stage["stage"])
        if limit is not None and stage["stage_total_ms_fixed"] > limit:
            violations.append(
                {
                    "stage": stage["stage"],
                    "kind": "PER_STAGE_LIMIT",
                    "actual_ms_fixed": stage["stage_total_ms_fixed"],
                    "limit_ms_fixed": limit,
                }
            )
    if budget["total_budget_ms_fixed"] > total_limit_ms_fixed:
        violations.append(
            {
                "stage": "TOTAL",
                "kind": "TOTAL_LIMIT",
                "actual_ms_fixed": budget["total_budget_ms_fixed"],
                "limit_ms_fixed": total_limit_ms_fixed,
            }
        )
    if violations:
        trigger = violations[0]
        raise WorkbenchError(
            Codes.BUDGET_EXCEEDED,
            f"budget exceeded at stage {trigger['stage']}",
            {
                "violations": violations,
                "trigger_stage": trigger["stage"],
                "actual_ms_fixed": trigger["actual_ms_fixed"],
                "limit_ms_fixed": trigger["limit_ms_fixed"],
                "total_budget_ms_fixed": budget["total_budget_ms_fixed"],
            },
        )
    return {
        "within_budget": True,
        "total_limit_ms_fixed": total_limit_ms_fixed,
        "per_stage_limits": per_stage_limits,
    }


def apply_downgrade(requested_items: list[str]) -> dict:
    """Validate a downgrade request item by item.

    Protected verification items can never be skipped; any attempt returns
    ``E_DOWNGRADE_FORBIDDEN`` and nothing is marked skipped.
    """
    forbidden = [item for item in requested_items if item in PROTECTED_ITEMS]
    unknown = [
        item
        for item in requested_items
        if item not in DOWNGRADEABLE_ITEMS and item not in PROTECTED_ITEMS
    ]
    if forbidden or unknown:
        raise WorkbenchError(
            Codes.DOWNGRADE_FORBIDDEN,
            "downgrade request contains protected or unknown items",
            {"forbidden": forbidden, "unknown": unknown},
        )
    return {
        "skipped_items": sorted(requested_items),
        "protected_items": sorted(PROTECTED_ITEMS),
        "allowed_items": sorted(DOWNGRADEABLE_ITEMS),
        "item_decisions": [
            {
                "item": item,
                "decision": "SKIPPED",
                "rationale": "non-critical diagnostic item",
            }
            for item in sorted(requested_items)
        ],
    }
