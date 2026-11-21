from __future__ import annotations

import hashlib
import json
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
from typing import Any, Iterable, Sequence

import numpy as np

from .errors import BusinessError, weight_zero


DECIMAL_PRECISION = 80
WEIGHT_SCALE = 18
PROGRESS_SCALE = 12
ROUNDING_MODE = "ROUND_HALF_EVEN"
ZERO = Decimal("0")
ONE = Decimal("1")


def decimal_value(value: Any, *, field: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        raise BusinessError(
            "E_REPORT_INVALID",
            f"{field} 必须是有限数值",
            http_status=422,
            details={"field": field, "value": value},
        )
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except Exception as exc:  # pragma: no cover - Decimal has stable errors
        raise BusinessError(
            "E_REPORT_INVALID",
            f"{field} 不是合法数值",
            http_status=422,
            details={"field": field, "value": value},
        ) from exc
    if not result.is_finite():
        raise BusinessError(
            "E_REPORT_INVALID",
            f"{field} 必须是有限数值",
            http_status=422,
            details={"field": field, "value": value},
        )
    return result


def parse_nonnegative_int(value: Any, *, field: str, upper: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise BusinessError(
            "E_REPORT_INVALID",
            f"{field} 必须是非负整数",
            http_status=422,
            details={"field": field, "value": value},
        )
    if value < 0:
        raise BusinessError(
            "E_REPORT_INVALID",
            f"{field} 不能为负数",
            http_status=422,
            details={"field": field, "value": value},
        )
    if value > upper:
        raise BusinessError(
            "E_REPORT_INVALID",
            f"{field} 超出有符号 64 位整数上界",
            http_status=422,
            details={"field": field, "value": value, "maximum": upper},
        )
    return value


def quantize_decimal(
    value: Decimal,
    scale: int,
    *,
    rounding: str = ROUND_HALF_EVEN,
) -> tuple[Decimal, Decimal]:
    quantum = Decimal(1).scaleb(-scale)
    with localcontext() as ctx:
        ctx.prec = DECIMAL_PRECISION
        ctx.rounding = rounding
        rounded = value.quantize(quantum)
    return rounded, value - rounded


def fixed_string(value: Decimal, scale: int) -> str:
    rounded, _ = quantize_decimal(value, scale)
    with localcontext() as ctx:
        ctx.prec = DECIMAL_PRECISION
        return format(rounded, f".{scale}f")


def decimal_string(value: Decimal, sig_digits: int = 32) -> str:
    """Canonical diagnostic rendering without accidentally rounding to float."""
    if value == 0:
        return "0"
    with localcontext() as ctx:
        ctx.prec = max(sig_digits, 1)
        # normalize() removes insignificant trailing zeros but may use exponent
        # notation. Expanding it keeps persisted JSON human-readable.
        return format(value.normalize(), "f")


def canonical_json(payload: Any) -> str:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def digest_payload(payload: Any) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def normalize_weights(
    configured_weights: dict[str, Decimal],
    present_replicas: Iterable[str],
    mode: str,
) -> dict[str, Any]:
    """Normalize using one explicit scheme and a deterministic residual pass.

    Raw weights are computed at DECIMAL_PRECISION. Each present weight is then
    quantized to WEIGHT_SCALE with ROUND_HALF_EVEN. Any residual is distributed
    by largest-remainder first, breaking equal remainders by replica id
    lexicographic order. Missing replicas always have an effective weight of 0.
    """

    replica_ids = sorted(configured_weights)
    present = sorted(set(present_replicas))
    if any(replica_id not in configured_weights for replica_id in present):
        raise BusinessError(
            "E_REPORT_INVALID",
            "上报副本不属于副本组",
            http_status=422,
            details={"unknown_replicas": sorted(set(present) - set(configured_weights))},
        )

    with localcontext() as ctx:
        ctx.prec = DECIMAL_PRECISION
        ctx.rounding = ROUND_HALF_EVEN
        full_total = sum((configured_weights[r] for r in replica_ids), ZERO)
        present_total = sum((configured_weights[r] for r in present), ZERO)

        if mode == "ALL_EQUAL":
            denominator = Decimal(len(replica_ids))
            raw = {
                replica_id: (
                    ONE / denominator if replica_id in present else ZERO
                )
                for replica_id in replica_ids
            }
            target_total = Decimal(len(present)) / denominator
            denominator_text = decimal_string(denominator)
        elif mode == "REPORTED_RATIO":
            if present_total <= 0:
                raise weight_zero(
                    "REPORTED_RATIO 下已上报副本的权重合计必须为正",
                    {
                        "present_replicas": present,
                        "configured_weights": {
                            key: decimal_string(value)
                            for key, value in sorted(configured_weights.items())
                        },
                    },
                )
            denominator = present_total
            raw = {
                replica_id: (
                    configured_weights[replica_id] / denominator
                    if replica_id in present
                    else ZERO
                )
                for replica_id in replica_ids
            }
            target_total = sum((raw[r] for r in present), ZERO)
            denominator_text = decimal_string(denominator)
        elif mode == "MISSING_ZERO":
            if full_total <= 0:
                raise weight_zero(
                    "副本组权重合计必须为正",
                    {"configured_weights": {
                        key: decimal_string(value)
                        for key, value in sorted(configured_weights.items())
                    }},
                )
            denominator = full_total
            raw = {
                replica_id: (
                    configured_weights[replica_id] / denominator
                    if replica_id in present
                    else ZERO
                )
                for replica_id in replica_ids
            }
            target_total = sum((raw[r] for r in present), ZERO)
            denominator_text = decimal_string(denominator)
        else:  # Defensive: service validates before calling this function.
            raise BusinessError(
                "E_NORMALIZATION_UNKNOWN",
                f"未知归一化方案: {mode!r}",
                http_status=400,
            )

        quantum = Decimal(1).scaleb(-WEIGHT_SCALE)
        rounded = {
            replica_id: (
                raw[replica_id].quantize(quantum, rounding=ROUND_HALF_EVEN)
                if replica_id in present
                else ZERO.quantize(quantum)
            )
            for replica_id in replica_ids
        }
        before_residual = sum((rounded[r] for r in present), ZERO)
        residual = target_total - before_residual
        residual_units = int(
            (residual / quantum).to_integral_value(rounding=ROUND_HALF_EVEN)
        )
        allocations: list[dict[str, Any]] = []
        if residual_units:
            step = 1 if residual_units > 0 else -1
            remaining = abs(residual_units)
            if step > 0:
                order = sorted(
                    present,
                    key=lambda r: (-(raw[r] - rounded[r]), r),
                )
            else:
                order = sorted(
                    present,
                    key=lambda r: ((raw[r] - rounded[r]), r),
                )
            if not order:
                order = []
            cursor = 0
            while remaining and order:
                replica_id = order[cursor % len(order)]
                rounded[replica_id] += quantum * step
                allocations.append(
                    {
                        "replica_id": replica_id,
                        "delta_units": step,
                        "reason": "LARGEST_REMAINDER",
                    }
                )
                cursor += 1
                remaining -= 1

        rounded_total = sum((rounded[r] for r in present), ZERO)
        normalization_error = rounded_total - target_total
        weights_fixed = {
            replica_id: fixed_string(rounded[replica_id], WEIGHT_SCALE)
            for replica_id in replica_ids
        }
        raw_weights_fixed = {
            replica_id: fixed_string(raw[replica_id], 24)
            for replica_id in replica_ids
        }

        return {
            "mode": mode,
            "denominator": denominator_text,
            "present_replicas": present,
            "missing_replicas": [
                replica_id
                for replica_id in replica_ids
                if replica_id not in present
            ],
            "raw_weights": raw_weights_fixed,
            "normalized_weights": weights_fixed,
            "normalized_weight_decimal": rounded,
            "present_weight_sum": fixed_string(rounded_total, WEIGHT_SCALE),
            "present_weight_sum_decimal": rounded_total,
            "target_present_sum": fixed_string(target_total, WEIGHT_SCALE),
            "target_present_sum_decimal": target_total,
            "rounding": {
                "mode": ROUNDING_MODE,
                "scale": WEIGHT_SCALE,
                "residual_units": residual_units,
                "residual_allocations": allocations,
                "cumulative_error": decimal_string(normalization_error),
            },
        }


def kahan_sum(values: Sequence[float]) -> float:
    total = 0.0
    compensation = 0.0
    for value in values:
        adjusted = value - compensation
        next_total = total + adjusted
        compensation = (next_total - total) - adjusted
        total = next_total
    return total


def compensated_sum_diagnostics(
    terms: Sequence[tuple[str, Decimal]],
    *,
    label: str,
) -> tuple[Decimal, dict[str, Any]]:
    """Return the canonical Decimal sum plus reproducible error diagnostics."""

    canonical_terms = sorted(terms, key=lambda item: item[0])
    with localcontext() as ctx:
        ctx.prec = DECIMAL_PRECISION
        exact = sum((value for _, value in canonical_terms), ZERO)
        absolute_sum = sum((abs(value) for _, value in canonical_terms), ZERO)
        float_values = [float(value) for _, value in canonical_terms]

        naive = 0.0
        for value in float_values:
            naive += value
        kahan = kahan_sum(float_values)
        numpy_naive = float(
            np.sum(np.asarray(float_values, dtype=np.float64), dtype=np.float64)
        )

        unit_roundoff = Decimal.from_float(float(np.finfo(np.float64).eps / 2.0))
        count = Decimal(max(len(canonical_terms), 1))
        naive_bound = count * unit_roundoff * absolute_sum
        kahan_bound = (Decimal(2) + count * unit_roundoff) * unit_roundoff * absolute_sum

        naive_error = abs(Decimal.from_float(naive) - exact)
        kahan_error = abs(Decimal.from_float(kahan) - exact)
        numpy_error = abs(Decimal.from_float(numpy_naive) - exact)

        diagnostics = {
            "label": label,
            "term_count": len(canonical_terms),
            "canonical_order": [key for key, _ in canonical_terms],
            "decimal_sum": decimal_string(exact),
            "naive_float_sum": repr(naive),
            "kahan_float_sum": repr(kahan),
            "numpy_float_sum": repr(numpy_naive),
            "naive_abs_error": decimal_string(naive_error),
            "kahan_abs_error": decimal_string(kahan_error),
            "numpy_abs_error": decimal_string(numpy_error),
            "unit_roundoff": decimal_string(unit_roundoff),
            "absolute_term_sum": decimal_string(absolute_sum),
            "naive_error_bound": decimal_string(naive_bound),
            "kahan_error_bound": decimal_string(kahan_bound),
            "naive_within_bound": naive_error <= naive_bound,
            "kahan_within_bound": kahan_error <= kahan_bound,
            "bound_formula": {
                "naive": "n * u * sum(abs(term))",
                "kahan": "(2 + n*u) * u * sum(abs(term))",
            },
        }
        return exact, diagnostics


def convergence_decision(
    actual_progress: Decimal,
    target_progress: Decimal,
    relative_tolerance: Decimal,
    *,
    canonical_order: Sequence[str],
) -> dict[str, Any]:
    delta = abs(actual_progress - target_progress)
    scale = max(abs(actual_progress), abs(target_progress))
    allowed_delta = relative_tolerance * scale
    converged = delta <= allowed_delta
    boundary_tie = allowed_delta > 0 and delta == allowed_delta
    return {
        "converged": converged,
        "comparison": {
            "actual_progress": fixed_string(actual_progress, PROGRESS_SCALE),
            "target_progress": fixed_string(target_progress, PROGRESS_SCALE),
            "delta": decimal_string(delta),
            "relative_tolerance": decimal_string(relative_tolerance),
            "comparison_scale": decimal_string(scale),
            "allowed_delta": decimal_string(allowed_delta),
            "formula": "abs(actual-target) <= tolerance * max(abs(actual),abs(target))",
            "boundary_tie": boundary_tie,
            "boundary_rule": "DELTA_EQUAL_TO_ALLOWED_IS_CONVERGED",
        },
        "tie_break": {
            "rule": "INCLUSIVE_TOLERANCE_BOUNDARY_THEN_LEXICOGRAPHIC_CANONICAL_ORDER",
            "canonical_order": list(canonical_order),
            "applied": boundary_tie,
        },
    }

