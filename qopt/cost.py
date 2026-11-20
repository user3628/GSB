"""Plan cost model and the numerical precision contract.

Cost constants are fixed so plans are reproducible. Every cost emitted
to JSON is rounded to COST_DECIMALS decimals (ROUND_HALF_EVEN) and
carries a rounding note with the accumulated rounding-error bound.
Plan comparison uses a relative tolerance (never bare float equality);
ties resolve through a stable ordering key.
"""

from .numeric import (COST_DECIMALS, PLAN_ABS_TOL, PLAN_REL_TOL, compare_costs,
                      round_cost, rounding_contract)

# Fixed cost constants (documented, part of the reproducibility contract).
SEQ_PAGE_COST = 1.0
RANDOM_PAGE_COST = 4.0
CPU_TUPLE_COST = 0.01
CPU_INDEX_TUPLE_COST = 0.005
CPU_OPERATOR_COST = 0.0025

_ADDITIVE_COMPONENTS = 3  # cpu_cost, io_cost, total_cost


class CostBreakdown:
    """CPU / IO decomposition of an access path or a whole plan.

    selectivity_sensitive_cost is the informational subset of cpu+io
    that scales with the estimated selectivity (lookups, tuple fetches,
    residual filters); it is NOT additive on top of cpu+io.
    """

    __slots__ = ("cpu_cost", "io_cost", "sel_sensitive_cost")

    def __init__(self, cpu_cost, io_cost, sel_sensitive_cost):
        self.cpu_cost = cpu_cost
        self.io_cost = io_cost
        self.sel_sensitive_cost = sel_sensitive_cost

    @property
    def total(self):
        return self.cpu_cost + self.io_cost

    def to_json(self):
        return {
            "cpu_cost": round_cost(self.cpu_cost),
            "io_cost": round_cost(self.io_cost),
            "selectivity_sensitive_cost": round_cost(self.sel_sensitive_cost),
            "total_cost": round_cost(self.total),
            "rounding": rounding_contract(_ADDITIVE_COMPONENTS),
            "note": ("selectivity_sensitive_cost is an informational subset "
                     "of cpu_cost+io_cost, not an additive component"),
        }


def choose_path(paths):
    """Pick the best access path; returns (best, excluded-with-reasons).

    Comparison uses the relative tolerance; ties are broken by the
    stable path key (lexicographic), so the outcome is reproducible.
    """
    ordered = sorted(paths, key=lambda p: p.key)
    best = ordered[0]
    for p in ordered[1:]:
        if compare_costs(p.cost.total, best.cost.total) < 0:
            best = p
    excluded = []
    for p in ordered:
        if p is best:
            continue
        cmp = compare_costs(p.cost.total, best.cost.total)
        if cmp == 0:
            reason = ("cost ties with chosen path within rel_tol=%.3g; "
                      "stable tie-break key '%s' loses to '%s'"
                      % (PLAN_REL_TOL, p.key, best.key))
        else:
            reason = ("cost %.6f exceeds chosen %.6f (rel_tol=%.3g, abs_tol=%.3g)"
                      % (p.cost.total, best.cost.total, PLAN_REL_TOL, PLAN_ABS_TOL))
        excluded.append((p, reason))
    return best, excluded


__all__ = [
    "CostBreakdown", "choose_path", "compare_costs",
    "SEQ_PAGE_COST", "RANDOM_PAGE_COST", "CPU_TUPLE_COST",
    "CPU_INDEX_TUPLE_COST", "CPU_OPERATOR_COST",
    "COST_DECIMALS", "PLAN_REL_TOL", "PLAN_ABS_TOL",
]
