from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.config import PRECISION_TOLERANCE
from app.fixedpoint import (
    OUTPUT_UNIT,
    ROUNDING_MODE,
    US_PER_OUTPUT_QUANTUM,
    CompensatedIntSum,
    QuantileValue,
    compensated_sum_us,
    output_conversion_trace,
    quantile_set,
)
from app.scheduler import BatchPlan
from app.spec import model_contribution
from app.units import relative_ppm, us_to_ms_x100


@dataclass
class PrecisionCheck:
    scope: str
    subject_id: str
    metric: str
    unit: str
    truth: int
    estimate: int
    absolute_deviation: int
    relative_deviation_ppm: int | None
    absolute_bound: int
    relative_bound_ppm: int
    passed: bool
    note: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "scope": self.scope,
            "subject_id": self.subject_id,
            "metric": self.metric,
            "unit": self.unit,
            "truth": self.truth,
            "estimate": self.estimate,
            "absolute_deviation": self.absolute_deviation,
            "relative_deviation_ppm": self.relative_deviation_ppm,
            "absolute_bound": self.absolute_bound,
            "relative_bound_ppm": self.relative_bound_ppm,
            "passed": self.passed,
            "note": self.note,
            "tolerance_policy": PRECISION_TOLERANCE["policy"],
        }


@dataclass
class BatchComputation:
    batch: BatchPlan
    quantiles: list[QuantileValue]
    checks: list[PrecisionCheck]
    engine_trace: dict[str, Any]
    error_chain: dict[str, Any]
    rounding_summary: dict[str, Any]
    validation_state: str
    failed_fields: list[str] = field(default_factory=list)
    deterministic_digest: str = ""

    @property
    def passed(self) -> bool:
        return self.validation_state == "VERIFIED"

    def as_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch.batch_id,
            "validation_state": self.validation_state,
            "failed_fields": self.failed_fields,
            "quantiles": [quantile.as_dict() for quantile in self.quantiles],
            "precision_checks": [check.as_dict() for check in self.checks],
            "engine_trace": self.engine_trace,
            "error_chain": self.error_chain,
            "rounding": self.rounding_summary,
            "deterministic_digest": self.deterministic_digest,
        }


def _check(
    *,
    scope: str,
    subject_id: str,
    metric: str,
    unit: str,
    truth: int,
    estimate: int,
    absolute_bound: int,
    relative_bound_ppm: int,
    note: str,
) -> PrecisionCheck:
    deviation = abs(estimate - truth)
    rel = relative_ppm(deviation, truth)
    passed = deviation <= absolute_bound and rel is not None and rel <= relative_bound_ppm
    return PrecisionCheck(
        scope=scope,
        subject_id=subject_id,
        metric=metric,
        unit=unit,
        truth=truth,
        estimate=estimate,
        absolute_deviation=deviation,
        relative_deviation_ppm=rel,
        absolute_bound=absolute_bound,
        relative_bound_ppm=relative_bound_ppm,
        passed=passed,
        note=note,
    )


def compare_member_truth(member, model) -> tuple[PrecisionCheck, PrecisionCheck]:
    truth_memory, truth_latency = model_contribution(model, member.samples)
    memory_check = _check(
        scope="member",
        subject_id=member.request.request_id,
        metric="memory_bytes",
        unit="bytes",
        truth=truth_memory,
        estimate=member.memory_bytes,
        absolute_bound=int(PRECISION_TOLERANCE["memory_abs_dev_bytes"]),
        relative_bound_ppm=0,
        note="truth = declared sample memory bytes * samples; engine = integer product",
    )
    latency_check = _check(
        scope="member",
        subject_id=member.request.request_id,
        metric="latency_us",
        unit="us",
        truth=truth_latency,
        estimate=member.latency_us,
        absolute_bound=int(PRECISION_TOLERANCE["latency_abs_dev_us"]),
        relative_bound_ppm=int(PRECISION_TOLERANCE["relative_dev_ppm"]),
        note="truth = declared unit cost us * samples; engine = fixed-point product",
    )
    return memory_check, latency_check


def compute_batch(batch: BatchPlan) -> BatchComputation:
    member_latencies = [member.latency_us for member in batch.members]
    quantiles = quantile_set(member_latencies)
    checks: list[PrecisionCheck] = []

    for member in batch.members:
        memory_check, latency_check = compare_member_truth(member, member.model)
        checks.extend((memory_check, latency_check))

    truth_memory = sum(member.samples * member.model.memory_bytes for member in batch.members)
    truth_latency = sum(member.samples * member.model.cost_us for member in batch.members)
    memory_check = _check(
        scope="batch",
        subject_id=batch.batch_id,
        metric="memory_bytes",
        unit="bytes",
        truth=truth_memory,
        estimate=batch.memory_bytes,
        absolute_bound=int(PRECISION_TOLERANCE["memory_abs_dev_bytes"]),
        relative_bound_ppm=0,
        note="truth = direct sample-count * byte multiply/add; engine = integer byte accumulation",
    )
    latency_check = _check(
        scope="batch",
        subject_id=batch.batch_id,
        metric="latency_us",
        unit="us",
        truth=truth_latency,
        estimate=us_to_ms_x100(batch.latency_us) * US_PER_OUTPUT_QUANTUM,
        absolute_bound=int(PRECISION_TOLERANCE["latency_abs_dev_us"]),
        relative_bound_ppm=int(PRECISION_TOLERANCE["relative_dev_ppm"]),
        note="truth = direct sample-count * cost multiply/add; engine = compensated integer sum then fixed output rounding",
    )
    checks.extend((memory_check, latency_check))

    for quantile in quantiles:
        checks.append(
            _check(
                scope="quantile",
                subject_id=batch.batch_id,
                metric=quantile.label.lower(),
                unit="us",
                truth=quantile.raw_us,
                estimate=quantile.rounded_us,
                absolute_bound=int(PRECISION_TOLERANCE["latency_abs_dev_us"]),
                relative_bound_ppm=int(PRECISION_TOLERANCE["relative_dev_ppm"]),
                note="truth = nearest-rank over declared member contributions; engine = same rank with fixed output rounding",
            )
        )

    total_conversion = output_conversion_trace(batch.latency_us)
    quantile_conversions = [
        {
            "label": quantile.label,
            **output_conversion_trace(quantile.raw_us),
        }
        for quantile in quantiles
    ]
    rounding_errors = [int(total_conversion["rounding_error_us"])] + [
        int(item["rounding_error_us"]) for item in quantile_conversions
    ]
    rounding_summary = {
        "output_unit": OUTPUT_UNIT,
        "rounding_mode": ROUNDING_MODE,
        "quantum_us": US_PER_OUTPUT_QUANTUM,
        "half_quantum_us": US_PER_OUTPUT_QUANTUM // 2,
        "batch_latency_rounding_bit": bool(total_conversion["rounding_bit"]),
        "batch_latency_rounding_direction": total_conversion["rounding_direction"],
        "cumulative_rounding_error_us_signed": sum(rounding_errors),
        "cumulative_rounding_error_us_abs": sum(abs(value) for value in rounding_errors),
        "per_value": [total_conversion, *quantile_conversions],
    }

    compensated: CompensatedIntSum = compensated_sum_us(member_latencies)
    engine_trace = {
        "contract": "fixedpoint-v1",
        "memory": {
            "representation": "signed 64-bit integer bytes",
            "inputs": [
                {
                    "request_id": member.request.request_id,
                    "samples": member.samples,
                    "unit_memory_bytes": member.model.memory_bytes,
                    "product_bytes": member.memory_bytes,
                }
                for member in batch.members
            ],
            "sum_bytes": batch.memory_bytes,
            "rounding_error_bytes": 0,
        },
        "latency": {
            "representation": "signed 64-bit integer microseconds",
            "compensated_sum": compensated.as_dict(),
            "output_conversion": total_conversion,
            "quantile_observations": {
                "unit": "batch member contribution = samples * declared unit cost",
                "values_us": member_latencies,
                "method": "nearest_rank",
            },
        },
        "quantiles": [quantile.as_dict() for quantile in quantiles],
    }

    error_chain = {
        "chain": [
            {
                "stage": 1,
                "name": "declared-unit-multiply",
                "operation": "samples * unit_cost_us",
                "domain": "integer us",
                "rounding_error_us": 0,
                "values": member_latencies,
            },
            {
                "stage": 2,
                "name": "compensated-accumulation",
                "operation": "Neumaier integer fixed-point sum",
                "domain": "integer us",
                "raw_total_us": compensated.total,
                "compensation_us": compensated.compensation,
                "result_us": compensated.value,
                "rounding_error_us": 0,
            },
            {
                "stage": 3,
                "name": "millisecond-output-quantization",
                "operation": f"{ROUNDING_MODE} to {OUTPUT_UNIT}",
                "domain": OUTPUT_UNIT,
                "input_us": compensated.value,
                "output_ms_fixed": total_conversion["output_ms_fixed"],
                "rounded_us": total_conversion["rounded_us"],
                "rounding_error_us": total_conversion["rounding_error_us"],
                "rounding_bit": total_conversion["rounding_bit"],
            },
            {
                "stage": 4,
                "name": "quantile-output-quantization",
                "operation": "nearest-rank then fixed-point millisecond rounding",
                "domain": OUTPUT_UNIT,
                "values": quantile_conversions,
                "cumulative_rounding_error_us": rounding_summary[
                    "cumulative_rounding_error_us_signed"
                ],
            },
        ],
        "truth_source": "declared sample count * declared unit value, followed by direct integer multiply/add",
        "estimate_source": "engine compensated integer accumulation followed by fixed-point conversion",
        "precision_tolerance": PRECISION_TOLERANCE,
    }

    failed_fields = [
        f"{check.scope}:{check.subject_id}:{check.metric}"
        for check in checks
        if not check.passed
    ]
    return BatchComputation(
        batch=batch,
        quantiles=quantiles,
        checks=checks,
        engine_trace=engine_trace,
        error_chain=error_chain,
        rounding_summary=rounding_summary,
        validation_state="VALIDATION_FAILED" if failed_fields else "VERIFIED",
        failed_fields=failed_fields,
    )


def compute_run_quantiles(batches: list[BatchPlan]) -> list[QuantileValue]:
    return quantile_set([batch.latency_us for batch in batches])
