"""Deterministic percentile, ratio and threshold evaluation."""

from __future__ import annotations

import hashlib
import json
from fractions import Fraction
from typing import Any

from .fixedpoint import (
    compare_at_least,
    compare_at_most,
    fixed_decimal,
    fixed_value,
    quantize,
    round_fraction,
    scale_for,
)
from .quantiles import percentile_snapshot


def evaluate_round(
    plan: dict[str, Any],
    stage: dict[str, Any],
    samples: list[dict[str, Any]],
    *,
    wall_seconds: float | None = None,
) -> dict[str, Any]:
    contract = plan["numeric_contract"]
    ratio_decimals = contract["ratio_decimals"]
    latency_decimals = contract["latency_decimals"]
    throughput_decimals = contract["throughput_decimals"]
    rounding = contract["rounding"]
    method = contract["quantile_method"]
    min_samples = contract["min_samples"]
    tolerance_ppm = contract["relative_tolerance_ppm"]
    ratio_scale = scale_for(ratio_decimals)
    throughput_scale = scale_for(throughput_decimals)

    canonical_samples = [
        {
            "index": sample["index"],
            "latency_us": int(sample["latency_us"]),
            "ok": bool(sample["ok"]),
            "error_kind": sample.get("error_kind"),
            "http_status": sample.get("http_status"),
            "timed_out": bool(sample.get("timed_out", False)),
        }
        for sample in sorted(samples, key=lambda item: item["index"])
    ]
    sample_digest = _digest(canonical_samples)
    latencies_ms = [Fraction(item["latency_us"], 1000) for item in canonical_samples]
    sample_count = len(canonical_samples)
    error_count = sum(1 for item in canonical_samples if not item["ok"])
    timeout_count = sum(1 for item in canonical_samples if item["timed_out"])
    success_count = sample_count - error_count

    p50 = percentile_snapshot(
        latencies_ms,
        Fraction(1, 2),
        method,
        decimals=latency_decimals,
        rounding=rounding,
        min_samples=min_samples,
    )
    p95 = percentile_snapshot(
        latencies_ms,
        Fraction(95, 100),
        method,
        decimals=latency_decimals,
        rounding=rounding,
        min_samples=min_samples,
    )
    p99 = percentile_snapshot(
        latencies_ms,
        Fraction(99, 100),
        method,
        decimals=latency_decimals,
        rounding=rounding,
        min_samples=min_samples,
    )
    nearest_snapshot = percentile_snapshot(
        latencies_ms,
        Fraction(99, 100),
        "nearest_rank",
        decimals=latency_decimals,
        rounding=rounding,
        min_samples=min_samples,
    )
    linear_snapshot = percentile_snapshot(
        latencies_ms,
        Fraction(99, 100),
        "linear",
        decimals=latency_decimals,
        rounding=rounding,
        min_samples=min_samples,
    )
    p99_difference_fixed = nearest_snapshot["fixed"] - linear_snapshot["fixed"]

    total_latency_us = sum(item["latency_us"] for item in canonical_samples)
    if total_latency_us > 0 and success_count > 0:
        throughput_exact = Fraction(
            success_count * stage["concurrency"] * 1_000_000,
            total_latency_us,
        )
    else:
        throughput_exact = Fraction(0)
    throughput_fixed = quantize(throughput_exact, throughput_scale, rounding)
    error_rate_exact = Fraction(error_count, sample_count) if sample_count else Fraction(0)
    error_rate_fixed = quantize(error_rate_exact, ratio_scale, rounding)
    ratio_ledger = _ratio_ledger(
        canonical_samples,
        scale=ratio_scale,
        decimals=ratio_decimals,
        rounding=rounding,
        concurrency=stage["concurrency"],
    )

    threshold_checks = {
        "p99_latency_ms_max": {
            "triggered": False,
            **compare_at_most(
                p99["fixed"],
                plan["stored_thresholds"]["p99_latency_ms_max"]["fixed"],
                tolerance_ppm,
                actual_decimals=latency_decimals,
                threshold_decimals=latency_decimals,
            ),
        },
        "error_rate_max": {
            "triggered": False,
            **compare_at_most(
                error_rate_fixed,
                plan["stored_thresholds"]["error_rate_max"]["fixed"],
                tolerance_ppm,
                actual_decimals=ratio_decimals,
                threshold_decimals=ratio_decimals,
            ),
        },
        "min_throughput_rps": {
            "triggered": False,
            **compare_at_least(
                throughput_fixed,
                plan["stored_thresholds"]["min_throughput_rps"]["fixed"],
                tolerance_ppm,
                actual_decimals=throughput_decimals,
                threshold_decimals=throughput_decimals,
            ),
        },
    }
    failed = []
    for name, check in threshold_checks.items():
        if not check["passed"]:
            check["triggered"] = True
            failed.append(
                {
                    "threshold": name,
                    "actual": check["actual"],
                    "limit": check["threshold"],
                    "relation": check["relation"],
                    "pass_boundary_fixed": check["pass_boundary_fixed"],
                    "relative_tolerance_ppm": check["relative_tolerance_ppm"],
                }
            )
    verdict = "REJECT" if failed else "PASS"

    metrics = {
        "sample_count": sample_count,
        "sample_digest": sample_digest,
        "success_count": success_count,
        "error_count": error_count,
        "timeout_count": timeout_count,
        "quantile_method": method,
        "quantile_sample_count": sample_count,
        "latency_ms": {
            "p50": p50,
            "p95": p95,
            "p99": p99,
        },
        "throughput_rps": {
            "value": fixed_value(throughput_fixed, throughput_decimals),
            "exact_fraction": _fraction_text(throughput_exact),
            "definition": contract["throughput_definition"],
            "formula": "success_count * concurrency * 1000000 / sum_latency_us",
            "inputs": {
                "success_count": success_count,
                "concurrency": stage["concurrency"],
                "sum_latency_us": total_latency_us,
            },
        },
        "error_rate": {
            "value": fixed_value(error_rate_fixed, ratio_decimals),
            "exact_fraction": _fraction_text(error_rate_exact),
            "exact_quantized_once_fixed": error_rate_fixed,
            "ledger": ratio_ledger,
        },
        "observed_wall_seconds": round(wall_seconds, 6) if wall_seconds is not None else None,
    }
    p99_comparison = {
        "sample_count": sample_count,
        "nearest_rank": nearest_snapshot,
        "linear": linear_snapshot,
        "selected_method": method,
        "selected_p99_fixed": p99["fixed"],
        "nearest_minus_linear_fixed": p99_difference_fixed,
        "nearest_minus_linear_ms": fixed_decimal(p99_difference_fixed, latency_decimals),
        "explanation": (
            "最近秩选择排序后的离散样本；线性插值在相邻秩序点间用精确有理数插值。"
            "差异仅由插值定义产生，不使用二进制浮点。"
        ),
    }
    reason = {
        "verdict": verdict,
        "summary": (
            "all threshold checks passed"
            if verdict == "PASS"
            else "failed threshold checks: " + ", ".join(item["threshold"] for item in failed)
        ),
        "failed_thresholds": failed,
        "tolerance_policy": (
            "整数定点比较；允许边界=阈值定点值±ceil(阈值定点值*相对容差ppm/1000000)，"
            "不使用裸浮点比较"
        ),
        "quantile_policy": (
            f"{method}；P99需至少{max(min_samples, 100)}个样本，"
            f"本档样本数={sample_count}"
        ),
        "rounding_policy": rounding,
        "ratio_verdict_value": "exact_fraction_quantized_once",
    }
    digest_metrics = dict(metrics)
    digest_metrics.pop("observed_wall_seconds", None)
    computation_material = {
        "plan_id": plan["plan_id"],
        "plan_version": plan.get("version"),
        "plan_fingerprint": plan.get("fingerprint"),
        "stage_index": stage["stage_index"],
        "concurrency": stage["concurrency"],
        "quantile_method": method,
        "rounding": rounding,
        "sample_digest": sample_digest,
        "verdict": verdict,
        "metrics": digest_metrics,
        "threshold_checks": threshold_checks,
        "p99_comparison": p99_comparison,
    }
    computation_digest = _digest(computation_material)
    sample_payload = {
        "sample_count": sample_count,
        "success_count": success_count,
        "error_count": error_count,
        "timeout_count": timeout_count,
        "sample_digest": sample_digest,
        "samples": canonical_samples,
        "aggregate": {
            "latency_us_sum": total_latency_us,
            "latency_us_min": min(item["latency_us"] for item in canonical_samples),
            "latency_us_max": max(item["latency_us"] for item in canonical_samples),
            "sample_digest": sample_digest,
            "quantile_sample_count": sample_count,
        },
    }
    return {
        "verdict": verdict,
        "quantile_method": method,
        "metrics": metrics,
        "threshold_checks": threshold_checks,
        "reason": reason,
        "p99_comparison": p99_comparison,
        "computation_digest": computation_digest,
        "sample_payload": sample_payload,
    }


def _ratio_ledger(
    samples: list[dict[str, Any]],
    *,
    scale: int,
    decimals: int,
    rounding: str,
    concurrency: int,
) -> dict[str, Any]:
    sample_count = len(samples)
    error_count = sum(1 for sample in samples if not sample["ok"])
    exact_units = Fraction(error_count * scale, sample_count)
    exact_fixed = quantize(exact_units / scale, scale, rounding)
    batch_count = min(8, max(1, concurrency, 1))
    batch_count = min(batch_count, max(1, sample_count))
    batches: list[dict[str, int]] = [
        {"total": 0, "errors": 0} for _ in range(batch_count)
    ]
    for sample in samples:
        batches[sample["index"] % batch_count]["total"] += 1
        if not sample["ok"]:
            batches[sample["index"] % batch_count]["errors"] += 1

    steps: list[dict[str, Any]] = []
    cumulative_fixed = 0
    for batch_index, batch in enumerate(batches):
        total = batch["total"]
        errors = batch["errors"]
        if total == 0:
            continue
        rate_exact_units = Fraction(errors * scale, total)
        rate_fixed = quantize(rate_exact_units / scale, scale, rounding)
        rate_error_units = rate_exact_units - rate_fixed
        weight_exact_units = Fraction(total * scale, sample_count)
        weight_fixed = quantize(weight_exact_units / scale, scale, rounding)
        weight_error_units = weight_exact_units - weight_fixed
        contribution_exact_units = Fraction(rate_fixed * weight_fixed, scale)
        contribution_fixed = round_fraction(contribution_exact_units, rounding)
        contribution_error_units = contribution_exact_units - contribution_fixed
        cumulative_fixed += contribution_fixed
        steps.extend(
            [
                {
                    "batch": batch_index,
                    "step": "batch_error_rate",
                    "operation": "round(errors_i/samples_i * scale)",
                    "input": {"errors": errors, "samples": total},
                    "rounding": rounding,
                    "output_fixed": rate_fixed,
                    "output_decimal": fixed_decimal(rate_fixed, decimals),
                    "rounding_delta_units": _fraction_text(rate_error_units),
                },
                {
                    "batch": batch_index,
                    "step": "batch_weight",
                    "operation": "round(samples_i/total_samples * scale)",
                    "input": {"samples": total, "total_samples": sample_count},
                    "rounding": rounding,
                    "output_fixed": weight_fixed,
                    "output_decimal": fixed_decimal(weight_fixed, decimals),
                    "rounding_delta_units": _fraction_text(weight_error_units),
                },
                {
                    "batch": batch_index,
                    "step": "weighted_contribution",
                    "operation": "round(rate_fixed_i * weight_fixed_i / scale)",
                    "input": {"rate_fixed": rate_fixed, "weight_fixed": weight_fixed},
                    "rounding": rounding,
                    "output_fixed": contribution_fixed,
                    "output_decimal": fixed_decimal(contribution_fixed, decimals),
                    "rounding_delta_units": _fraction_text(contribution_error_units),
                },
            ]
        )
    cumulative_delta = cumulative_fixed - exact_fixed
    return {
        "method": "per_batch_rate_weight_accumulation",
        "scale": scale,
        "decimals": decimals,
        "rounding": rounding,
        "batch_count": batch_count,
        "equivalent_reference": {
            "operation": "round(total_errors/total_samples * scale)",
            "exact_fraction": _fraction_text(Fraction(error_count, sample_count)),
            "fixed": exact_fixed,
            "decimal": fixed_decimal(exact_fixed, decimals),
        },
        "steps": steps,
        "cumulative": {
            "fixed": cumulative_fixed,
            "decimal": fixed_decimal(cumulative_fixed, decimals),
            "cumulative_rounding_error_fixed": cumulative_delta,
            "cumulative_rounding_error_decimal": fixed_decimal(cumulative_delta, decimals),
            "cumulative_rounding_error_ppm": _fraction_text(
                Fraction(cumulative_delta * 1_000_000, scale)
            ),
        },
        "verdict_source": "equivalent_reference.fixed (exact rational rounded once)",
        "error_bound_note": (
            "每步均为确定性定点舍入；累计误差以相对每次精确贡献的定点差值记录。"
            "裁决采用等效参考值，相对容差吸收边界附近的定点量化差异。"
        ),
    }


def _fraction_text(value: Fraction) -> str:
    return f"{value.numerator}/{value.denominator}"


def _digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()

