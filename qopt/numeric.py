"""Numerically stable primitives shared by every estimator.

Design contract
---------------
* All accumulations that feed selectivity/cardinality decisions use
  Kahan-Neumaier compensated summation over IEEE-754 binary64 values in
  a fixed iteration order, so repeated evaluation of the same snapshot
  yields bitwise-identical results.
* Error bounds are reported next to the sums so callers can judge the
  accuracy impact of skewed histograms (mixed huge/tiny bucket counts).
* Cost values emitted to JSON are rounded to a fixed number of decimals
  with ROUND_HALF_EVEN; plan comparison uses a relative tolerance and a
  deterministic tie-break instead of bare float comparison.
"""

import math
from decimal import ROUND_HALF_EVEN, Decimal

# Unit roundoff for IEEE-754 binary64 (u = 2**-53).
UNIT_ROUNDOFF = 2.0**-53

COST_DECIMALS = 6
_COST_QUANT = Decimal(1).scaleb(-COST_DECIMALS)

# Plan comparison tolerance: two plans whose total costs differ by less
# than this relative amount are considered tied and resolved by a stable
# ordering key, which keeps plan choice reproducible.
PLAN_REL_TOL = 1e-9
PLAN_ABS_TOL = 1e-12


def kahan_sum(values):
    """Kahan-Neumaier compensated summation.

    For a sequence x_1..x_n with S = sum(|x_i|) the leading-order error
    bound is 2*u*S (u = 2**-53), independent of n, whereas naive left to
    right summation is bounded by (n-1)*u*S.  The iteration order is the
    caller-provided order, so results are bitwise reproducible.
    """
    total = 0.0
    compensation = 0.0
    for value in values:
        t = total + value
        if abs(total) >= abs(value):
            compensation += (total - t) + value
        else:
            compensation += (value - t) + total
        total = t
    return total + compensation


def naive_sum(values):
    """Plain left-to-right summation, kept for error-bound comparisons."""
    total = 0.0
    for value in values:
        total += value
    return total


def summation_error_report(values):
    """Quantified error bounds for Kahan vs. naive summation.

    Returns a dict with the compensated result, the naive result, the
    observed absolute difference and the theoretical leading-order
    bounds (naive: (n-1)*u*S, Kahan: 2*u*S, with S = sum(|x_i|)).
    """
    values = list(values)
    n = len(values)
    sum_abs = kahan_sum([abs(v) for v in values])
    kahan = kahan_sum(values)
    naive = naive_sum(values)
    return {
        "n_terms": n,
        "sum_abs": sum_abs,
        "kahan_result": kahan,
        "naive_result": naive,
        "observed_abs_diff": abs(kahan - naive),
        "naive_error_bound": (n - 1) * UNIT_ROUNDOFF * sum_abs if n > 0 else 0.0,
        "kahan_error_bound": 2.0 * UNIT_ROUNDOFF * sum_abs,
        "unit_roundoff": UNIT_ROUNDOFF,
    }


def clamp01(x):
    """Clamp a selectivity-like value into the closed interval [0, 1]."""
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    return x


def xlog(x):
    """Natural log extended to 0 (returns -inf); used for log-domain work."""
    if x <= 0.0:
        return -math.inf
    return math.log(x)


def round_cost(x):
    """Round a cost component to the contractual number of decimals.

    Uses Decimal(repr(x)) with ROUND_HALF_EVEN so the mapping is
    deterministic and independent of the platform's float formatting.
    """
    rounded = float(Decimal(repr(x)).quantize(_COST_QUANT, rounding=ROUND_HALF_EVEN))
    if rounded == 0.0:
        return 0.0  # normalise -0.0
    return rounded


def rounding_contract(additive_components):
    """Human-readable rounding-error note attached to cost JSON output."""
    per_component = 0.5 * 10.0**-COST_DECIMALS
    return {
        "decimals": COST_DECIMALS,
        "mode": "ROUND_HALF_EVEN",
        "max_component_rounding_error": per_component,
        "additive_components": additive_components,
        "max_accumulated_rounding_error": per_component * additive_components,
    }


def compare_costs(a, b):
    """Tolerant three-way comparison of plan costs.

    Returns 0 when |a-b| <= PLAN_ABS_TOL + PLAN_REL_TOL*max(|a|,|b|,1),
    else -1/1.  Never relies on bare float equality.
    """
    diff = a - b
    scale = max(abs(a), abs(b), 1.0)
    if abs(diff) <= PLAN_ABS_TOL + PLAN_REL_TOL * scale:
        return 0
    return -1 if diff < 0 else 1
