"""Deterministic network diagnostics statistics and root-cause ranking."""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from decimal import Decimal, ROUND_HALF_UP
from fractions import Fraction
from typing import Any, Iterable, Sequence

import numpy as np

from .errors import insufficient_samples
from .models import DiagnosticRecord


MIN_RTT_SAMPLES_DEFAULT = 5
QUANTILE_ORDER = (50, 90, 95, 99)
FAILURE_WEIGHTS = {"timeout": 1.0, "refused": 0.8, "reset": 0.6, "error": 0.5, "none": 0.0}


def _fixed(value: Fraction | int | float, places: int) -> str:
    quant = Decimal(1).scaleb(-places)
    if isinstance(value, Fraction):
        decimal = Decimal(value.numerator) / Decimal(value.denominator)
    elif isinstance(value, int):
        decimal = Decimal(value)
    else:
        decimal = Decimal(str(value))
    return f"{decimal.quantize(quant, rounding=ROUND_HALF_UP):.{places}f}"


def _nearest_rank(sorted_values: Sequence[int], percentile: int) -> int:
    if len(sorted_values) == 0:
        raise ValueError("nearest-rank requires at least one sample")
    if percentile <= 0:
        return int(sorted_values[0])
    n = len(sorted_values)
    rank = (percentile * n + 99) // 100
    rank = min(max(rank, 1), n)
    return int(sorted_values[rank - 1])


def _compensated_sum(values: Sequence[int]) -> dict[str, Any]:
    """Neumaier compensated summation with a conservative floating error bound."""

    total = 0.0
    compensation = 0.0
    for value in values:
        term = float(value)
        next_total = total + term
        if abs(total) >= abs(term):
            compensation += (total - next_total) + term
        else:
            compensation += (term - next_total) + total
        total = next_total
    compensated = total + compensation
    exact = sum(int(value) for value in values)
    n = len(values)
    unit_roundoff = 2**-53
    sum_abs = math.fsum(abs(float(value)) for value in values)
    error_bound = (n + 4) * unit_roundoff * sum_abs + 4 * unit_roundoff * abs(compensated) + 1e-9
    return {
        "method": "Neumaier compensated summation",
        "exact_integer_sum_us": exact,
        "compensated_sum_us": compensated,
        "absolute_error_us": abs(compensated - exact),
        "error_upper_bound_us": error_bound,
        "bound_note": "上界=(n+4)·2^-53·Σ|xi|+4·2^-53·|Σxi|+1ns，覆盖浮点舍入与补偿项。",
    }


def _rtt_block(rtt_values: Sequence[int]) -> dict[str, Any]:
    ordered = np.sort(np.asarray(rtt_values, dtype=np.int64))
    quantiles = {
        f"p{percentile}": _nearest_rank(ordered, percentile) for percentile in QUANTILE_ORDER
    }
    return {
        "sample_count": len(rtt_values),
        "min": int(ordered[0]),
        "max": int(ordered[-1]),
        **quantiles,
        "quantile_method": "nearest-rank",
        "quantile_definition": "rank=ceil(p·n)，n>0；取排序后第 rank 个样本（1 基），无插值。",
        "quantile_status": "ok",
        "cumulative": _compensated_sum(rtt_values),
    }


def _jitter_block(rtt_records: Sequence[DiagnosticRecord]) -> dict[str, Any]:
    ordered = sorted(rtt_records, key=lambda record: (record.timestamp_ms, record.ordinal))
    differences = [
        abs(ordered[index].rtt_us - ordered[index - 1].rtt_us)
        for index in range(1, len(ordered))
    ]
    if not differences:
        return {
            "pair_count": 0,
            "mean_abs_successive_us": None,
            "mean_abs_successive_fraction": None,
            "p95_abs_successive_us": None,
            "rounding": "half-up-2dp for mean; integer nearest-rank for p95",
        }
    sorted_diffs = np.sort(np.asarray(differences, dtype=np.int64))
    mean = Fraction(sum(differences), len(differences))
    return {
        "pair_count": len(differences),
        "status": "ok",
        "mean_abs_successive_us": _fixed(mean, 2),
        "mean_abs_successive_fraction": f"{float(mean):.12f}",
        "p95_abs_successive_us": _nearest_rank(sorted_diffs, 95),
        "rounding": "half-up-2dp for mean; integer nearest-rank for p95",
        "definition": "jitter=mean(|RTT[i]-RTT[i-1]|)，按 (timestamp_ms, ordinal) 排序，仅成功且 RTT>0 的样本。",
    }


def _fault_level(loss_fraction: float, jitter_mean: float | None, failure_counts: dict[str, int]) -> str:
    timeout_count = failure_counts.get("timeout", 0)
    if loss_fraction >= 0.5 or timeout_count >= 3 or (jitter_mean is not None and jitter_mean >= 5_000):
        return "critical"
    if loss_fraction >= 0.1 or any(failure_counts.values()) or (
        jitter_mean is not None and jitter_mean >= 2_000
    ):
        return "degraded"
    return "healthy"


def compute_path_stats(
    path_node: str,
    records: Sequence[DiagnosticRecord],
    *,
    start_ms: int | None = None,
    end_ms: int | None = None,
    min_samples: int = MIN_RTT_SAMPLES_DEFAULT,
    allow_insufficient: bool = False,
) -> dict[str, Any]:
    """Compute statistics for one path. Window is [start_ms, end_ms)."""

    filtered = [
        record
        for record in records
        if record.path_node == path_node
        and (start_ms is None or record.timestamp_ms >= start_ms)
        and (end_ms is None or record.timestamp_ms < end_ms)
    ]
    total = len(filtered)
    successful = sum(1 for record in filtered if record.success)
    failed = total - successful
    rtt_records = [
        record for record in filtered if record.success and record.rtt_us > 0 and record.failure_type == "none"
    ]
    rtt_values = [record.rtt_us for record in rtt_records]
    actual_samples = len(rtt_values)
    insufficient = actual_samples < min_samples
    if insufficient and not allow_insufficient:
        raise insufficient_samples(
            min_samples,
            actual_samples,
            path_node=path_node,
            total_records=total,
            note="不输出 0 值伪分位；请扩大窗口/路径或降低 min_samples。",
        )

    failure_counts = Counter(
        record.failure_type for record in filtered if not record.success
    )
    failure_ordered = {
        key: int(failure_counts.get(key, 0))
        for key in ("timeout", "refused", "reset", "error", "none")
        if failure_counts.get(key, 0)
    }
    loss_fraction = Fraction(failed, total) if total else Fraction(0, 1)

    if actual_samples and not insufficient:
        rtt_block: dict[str, Any] = _rtt_block(rtt_values)
        jitter = _jitter_block(rtt_records)
        jitter_mean = (
            float(jitter["mean_abs_successive_fraction"])
            if jitter["mean_abs_successive_fraction"] is not None
            else None
        )
        quantile_status = "ok"
    elif actual_samples:
        rtt_block = {
            "sample_count": actual_samples,
            "min": min(rtt_values),
            "max": max(rtt_values),
            "p50": None,
            "p90": None,
            "p95": None,
            "p99": None,
            "quantile_method": "nearest-rank",
            "quantile_status": "E_INSUFFICIENT_SAMPLES",
            "cumulative": _compensated_sum(rtt_values),
            "note": "有效样本数低于声明门槛；不输出分位或抖动结论。",
        }
        jitter = {
            "pair_count": max(0, actual_samples - 1),
            "mean_abs_successive_us": None,
            "mean_abs_successive_fraction": None,
            "p95_abs_successive_us": None,
            "rounding": "not applicable",
            "status": "E_INSUFFICIENT_SAMPLES",
            "definition": "有效 RTT 样本不足，未计算抖动统计。",
        }
        jitter_mean = None
        quantile_status = "E_INSUFFICIENT_SAMPLES"
    else:
        rtt_block = {
            "sample_count": 0,
            "min": None,
            "max": None,
            "p50": None,
            "p90": None,
            "p95": None,
            "p99": None,
            "quantile_method": "nearest-rank",
            "quantile_status": "E_INSUFFICIENT_SAMPLES",
            "cumulative": {
                "method": "Neumaier compensated summation",
                "exact_integer_sum_us": 0,
                "compensated_sum_us": 0.0,
                "absolute_error_us": 0.0,
                "error_upper_bound_us": 0.0,
                "note": "无有效 RTT 样本；此处的 0 是空集合标识，不是分位值。",
            },
        }
        jitter = {
            "pair_count": 0,
            "mean_abs_successive_us": None,
            "mean_abs_successive_fraction": None,
            "p95_abs_successive_us": None,
            "rounding": "not applicable",
            "definition": "无有效 RTT 样本，未计算抖动。",
        }
        jitter_mean = None
        quantile_status = "E_INSUFFICIENT_SAMPLES"

    payload = {
        "path_node": path_node,
        "window": {
            "start_ms": start_ms,
            "end_ms": end_ms,
            "inclusion": "[start_ms, end_ms)",
        },
        "sample": {
            "total": total,
            "successful": successful,
            "failed": failed,
            "rtt_samples": actual_samples,
            "required_rtt_samples": min_samples,
            "quantile_status": quantile_status,
        },
        "loss": {
            "failed": failed,
            "total": total,
            "fraction": _fixed(loss_fraction, 6),
            "fraction_value": float(loss_fraction),
            "loss_rate": float(loss_fraction),
            "percent": _fixed(loss_fraction * 100, 2),
            "loss_percent": _fixed(loss_fraction * 100, 2),
            "rounding": "ROUND_HALF_UP, 2 decimal places for percent; 6 for fraction",
        },
        "rtt_us": rtt_block,
        "jitter_us": jitter,
        "failure_types": failure_ordered,
        "fault_level": _fault_level(float(loss_fraction), jitter_mean, failure_ordered),
        "numerical_contract": {
            "quantiles": "deterministic nearest-rank",
            "loss": "failed/total, Decimal ROUND_HALF_UP fixed precision",
            "cumulative_delay": "Neumaier compensation + conservative error bound",
            "jitter": "mean absolute successive RTT difference",
            "window": "start inclusive, end exclusive",
            "deterministic": True,
        },
    }
    digest_payload = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    payload["calculation_digest"] = hashlib.sha256(digest_payload.encode("utf-8")).hexdigest()
    return payload


def _failure_weight(failure_counts: dict[str, int]) -> float:
    count = sum(failure_counts.values())
    if count == 0:
        return 0.0
    weighted = sum(FAILURE_WEIGHTS.get(kind, 0.5) * count for kind, count in failure_counts.items())
    return weighted / count


def root_cause_candidates(
    records: Sequence[DiagnosticRecord],
    *,
    start_ms: int | None = None,
    end_ms: int | None = None,
    min_samples: int = MIN_RTT_SAMPLES_DEFAULT,
) -> dict[str, Any]:
    paths = sorted({record.path_node for record in records})
    candidates: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    for path_node in paths:
        stats = compute_path_stats(
            path_node,
            records,
            start_ms=start_ms,
            end_ms=end_ms,
            min_samples=min_samples,
            allow_insufficient=True,
        )
        failure_counts = stats["failure_types"]
        failure_weight = _failure_weight(failure_counts)
        loss_fraction = float(stats["loss"]["fraction_value"])
        jitter_value = stats["jitter_us"]["mean_abs_successive_fraction"]
        jitter_mean = float(jitter_value) if jitter_value is not None else None
        jitter_component = min(jitter_mean / 5_000.0, 1.0) if jitter_mean is not None else 0.0
        loss_component = loss_fraction
        score = 0.55 * loss_component + 0.25 * jitter_component + 0.20 * failure_weight
        candidate = {
            "path_node": path_node,
            "fault_level": stats["fault_level"],
            "rank_score": _fixed(score, 6),
            "rank_basis": {
                "loss_fraction": loss_fraction,
                "loss_failed": stats["loss"]["failed"],
                "loss_total": stats["loss"]["total"],
                "jitter_mean_abs_successive_us": jitter_mean,
                "jitter_pair_count": stats["jitter_us"]["pair_count"],
                "failure_types": failure_counts,
                "failure_weighted_mean": _fixed(failure_weight, 6),
                "formula": "0.55*loss + 0.25*min(jitter/5000,1) + 0.20*failure_weight",
            },
            "sample": stats["sample"],
            "loss": stats["loss"],
            "rtt_us": {
                key: stats["rtt_us"].get(key)
                for key in ("p50", "p95", "p99", "quantile_method", "quantile_status", "sample_count")
            },
            "jitter_us": stats["jitter_us"],
            "evidence_digest": stats["calculation_digest"],
        }
        candidates.append(candidate)
        if stats["sample"]["rtt_samples"] < min_samples:
            warnings.append(
                {
                    "code": "E_INSUFFICIENT_SAMPLES",
                    "path_node": path_node,
                    "required_samples": min_samples,
                    "actual_samples": stats["sample"]["rtt_samples"],
                    "effect": "该路径不输出 RTT 分位或抖动数值，也不以 0 冒充；排序仅使用可核对项。",
                }
            )

    candidates.sort(
        key=lambda item: (
            -Decimal(item["rank_score"]),
            -Decimal(str(item["rank_basis"]["loss_fraction"])),
            item["path_node"],
        )
    )
    payload = {
        "window": {"start_ms": start_ms, "end_ms": end_ms, "inclusion": "[start_ms, end_ms)"},
        "required_rtt_samples": min_samples,
        "candidates": candidates,
        "warnings": warnings,
        "ranking_contract": {
            "higher_score_is_worse": True,
            "tie_break": "score desc, loss fraction desc, path_node asc",
            "fault_levels": {
                "critical": "loss>=50% or timeout failures>=3 or jitter>=5000us",
                "degraded": "loss>=10% or any failure or jitter>=2000us",
                "healthy": "otherwise",
            },
        },
    }
    digest_payload = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    payload["calculation_digest"] = hashlib.sha256(digest_payload.encode("utf-8")).hexdigest()
    return payload


def compare_statistics(
    stats_a: dict[str, Any],
    stats_b: dict[str, Any],
    *,
    relative_tolerance: float = 1e-9,
    absolute_tolerance: float = 0.0,
) -> dict[str, Any]:
    if relative_tolerance < 0 or absolute_tolerance < 0:
        raise ValueError("tolerances must be non-negative")

    def close(a: float, b: float) -> bool:
        return math.isclose(a, b, rel_tol=relative_tolerance, abs_tol=absolute_tolerance)

    loss_a = float(stats_a["loss"]["fraction_value"])
    loss_b = float(stats_b["loss"]["fraction_value"])
    jitter_a_raw = stats_a["jitter_us"]["mean_abs_successive_fraction"]
    jitter_b_raw = stats_b["jitter_us"]["mean_abs_successive_fraction"]
    p95_a = stats_a["rtt_us"].get("p95")
    p95_b = stats_b["rtt_us"].get("p95")
    jitter_comparable = jitter_a_raw is not None and jitter_b_raw is not None
    jitter_a = float(jitter_a_raw) if jitter_comparable else None
    jitter_b = float(jitter_b_raw) if jitter_comparable else None
    payload = {
        "path_a": stats_a["path_node"],
        "path_b": stats_b["path_node"],
        "tolerance": {
            "relative": relative_tolerance,
            "absolute": absolute_tolerance,
            "definition": "math.isclose(a,b,rel_tol,abs_tol)",
        },
        "comparisons": {
            "loss_fraction_equal": close(loss_a, loss_b),
            "loss_fraction_a": loss_a,
            "loss_fraction_b": loss_b,
            "jitter_equal": close(jitter_a, jitter_b) if jitter_comparable else None,
            "jitter_mean_a": jitter_a,
            "jitter_mean_b": jitter_b,
            "p95_equal": p95_a == p95_b if p95_a is not None and p95_b is not None else None,
            "p95_a": p95_a,
            "p95_b": p95_b,
        },
        "note": "比例与抖动使用相对容差；整数分位使用精确相等。无法比较的项返回 null，不伪填 0。",
    }
    digest_payload = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    payload["calculation_digest"] = hashlib.sha256(digest_payload.encode("utf-8")).hexdigest()
    return payload


def stats_snapshot(records: Sequence[DiagnosticRecord], *, min_samples: int = MIN_RTT_SAMPLES_DEFAULT) -> dict[str, Any]:
    paths = sorted({record.path_node for record in records})
    path_stats = [
        compute_path_stats(
            path_node,
            records,
            min_samples=min_samples,
            allow_insufficient=True,
        )
        for path_node in paths
    ]
    payload = {
        "path_count": len(paths),
        "record_count": len(records),
        "required_rtt_samples": min_samples,
        "paths": path_stats,
    }
    digest_payload = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    payload["calculation_digest"] = hashlib.sha256(digest_payload.encode("utf-8")).hexdigest()
    return payload


def filter_records(
    records: Iterable[DiagnosticRecord],
    *,
    path_node: str | None = None,
    start_ms: int | None = None,
    end_ms: int | None = None,
) -> list[DiagnosticRecord]:
    return [
        record
        for record in records
        if (path_node is None or record.path_node == path_node)
        and (start_ms is None or record.timestamp_ms >= start_ms)
        and (end_ms is None or record.timestamp_ms < end_ms)
    ]
