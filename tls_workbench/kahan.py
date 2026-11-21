"""Compensated summation helpers and naive-summation error accounting."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

from . import fixedpoint as fp


@dataclass(frozen=True)
class FixedSumResult:
    total: int
    compensation: int
    terms: int
    per_step: tuple[dict, ...]


def kahan_sum_fixed(terms: Sequence[int]) -> FixedSumResult:
    """Exact Kahan summation over fixed-point integers.

    Because the underlying integer additions are exact, the compensation is
    always zero at scale; it is retained and reported to make the contract
    explicit and to defend the pipeline if a future step introduces
    non-exact intermediate scaling.
    """
    total = 0
    compensation = 0
    steps: list[dict] = []
    for index, term in enumerate(terms):
        adjusted = term - compensation
        candidate = total + adjusted
        compensation = (candidate - total) - adjusted
        total = candidate
        steps.append(
            {
                "index": index,
                "term": term,
                "adjusted": adjusted,
                "running_total": total,
                "compensation": compensation,
            }
        )
    return FixedSumResult(total, compensation, len(terms), tuple(steps))


@dataclass(frozen=True)
class FloatSumResult:
    total: float
    per_step: tuple[dict, ...]


def kahan_sum_float(terms: Iterable[float]) -> FloatSumResult:
    total = 0.0
    compensation = 0.0
    steps: list[dict] = []
    for index, term in enumerate(terms):
        adjusted = term - compensation
        candidate = total + adjusted
        compensation = (candidate - total) - adjusted
        total = candidate
        steps.append(
            {
                "index": index,
                "term": term,
                "running_total": total,
                "compensation": compensation,
            }
        )
    return FloatSumResult(total, tuple(steps))


def naive_sum_float(terms: Sequence[float]) -> FloatSumResult:
    total = 0.0
    steps: list[dict] = []
    for index, term in enumerate(terms):
        total = total + term
        steps.append({"index": index, "term": term, "running_total": total})
    return FloatSumResult(total, tuple(steps))


def summation_audit(terms_float: Sequence[float], terms_fixed: Sequence[int]) -> dict:
    """Compare naive vs compensated float accumulation against the fixed-point
    canonical total and report a quantitative error bound.

    The theoretical bound for naive summation of ``n`` IEEE-754 additions is
    ``(n - 1) * eps * sum(|terms|)``; we report both that a-priori bound and
    the observed deltas.
    """
    naive = naive_sum_float(terms_float)
    kahan = kahan_sum_float(terms_float)
    canonical = kahan_sum_fixed(terms_fixed)
    canonical_float = fp.to_float(canonical.total)
    eps = 2.0 ** -53
    absolute_sum = sum(abs(value) for value in terms_float)
    bound = max(0, len(terms_float) - 1) * eps * absolute_sum
    return {
        "rounding": fp.ROUNDING,
        "canonical_total_fixed": canonical.total,
        "canonical_total": fp.to_str(canonical.total),
        "naive_float_total": repr(naive.total),
        "kahan_float_total": repr(kahan.total),
        "naive_abs_error_vs_canonical": repr(abs(naive.total - canonical_float)),
        "kahan_abs_error_vs_canonical": repr(abs(kahan.total - canonical_float)),
        "naive_error_upper_bound": repr(bound),
        "bound_respected": abs(naive.total - canonical_float) <= bound + absolute_sum * eps,
        "term_count": len(terms_float),
    }
