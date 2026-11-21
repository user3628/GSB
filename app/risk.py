from __future__ import annotations

import hashlib
import math
import random
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP
from typing import Any, Sequence

from .policy import PolicySnapshot, decimal_text


FIXED_SCALE = 1_000_000
ROUNDING_MODE = "ROUND_HALF_UP"
ROUNDING_QUANTUM = Decimal("0.000001")
P90_METHOD = "nearest_rank_ceil_0.90"


def q6(value: Decimal) -> Decimal:
    return value.quantize(ROUNDING_QUANTUM, rounding=ROUND_HALF_UP)


def fixed_micros(value: Decimal) -> int:
    return int((value * FIXED_SCALE).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


@dataclass(frozen=True)
class Observation:
    sequence: int
    timestamp_ms: int
    latency_micros: int
    success: bool
    metric_type: str = "CANDIDATE"

    @property
    def latency_ms(self) -> Decimal:
        return (Decimal(self.latency_micros) / Decimal(1000)).quantize(
            Decimal("0.001"), rounding=ROUND_HALF_UP
        )


@dataclass(frozen=True)
class SampleStats:
    sample_count: int
    success_count: int
    error_rate: Decimal
    success_rate: Decimal
    p90_micros: int
    p90_ms: Decimal
    p90_rank: int
    p90_method: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "sample_count": self.sample_count,
            "success_count": self.success_count,
            "error_rate": decimal_text(self.error_rate),
            "success_rate": decimal_text(self.success_rate),
            "p90_latency_ms": decimal_text(self.p90_ms),
            "p90": {
                "method": self.p90_method,
                "sample_size": self.sample_count,
                "rank": self.p90_rank,
                "latency_micros": self.p90_micros,
            },
        }


def compute_stats(observations: Sequence[Observation]) -> SampleStats:
    count = len(observations)
    if count == 0:
        raise ValueError("compute_stats requires at least one observation")
    success_count = sum(1 for item in observations if item.success)
    error_rate = q6(Decimal(count - success_count) / Decimal(count))
    success_rate = q6(Decimal(success_count) / Decimal(count))
    ordered = sorted(item.latency_micros for item in observations)
    rank = max(1, int(math.ceil(0.90 * count)))
    p90_micros = ordered[rank - 1]
    p90_ms = (Decimal(p90_micros) / Decimal(1000)).quantize(
        Decimal("0.001"), rounding=ROUND_HALF_UP
    )
    return SampleStats(
        sample_count=count,
        success_count=success_count,
        error_rate=error_rate,
        success_rate=success_rate,
        p90_micros=p90_micros,
        p90_ms=p90_ms,
        p90_rank=rank,
        p90_method=P90_METHOD,
    )


@dataclass(frozen=True)
class BaselineStats:
    sample_count: int
    success_count: int
    error_rate: Decimal
    success_rate: Decimal
    p90_micros: int
    p90_ms: Decimal

    @classmethod
    def from_mapping(cls, raw: dict[str, Any]) -> "BaselineStats":
        sample_count = int(raw["sample_count"])
        success_count = int(raw["success_count"])
        p90_micros = int(raw["latency_p90_micros"])
        return cls(
            sample_count=sample_count,
            success_count=success_count,
            error_rate=q6(Decimal(sample_count - success_count) / Decimal(sample_count)),
            success_rate=q6(Decimal(success_count) / Decimal(sample_count)),
            p90_micros=p90_micros,
            p90_ms=(Decimal(p90_micros) / Decimal(1000)).quantize(
                Decimal("0.001"), rounding=ROUND_HALF_UP
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "sample_count": self.sample_count,
            "success_count": self.success_count,
            "error_rate": decimal_text(self.error_rate),
            "success_rate": decimal_text(self.success_rate),
            "p90_latency_ms": decimal_text(self.p90_ms),
            "latency_p90_micros": self.p90_micros,
        }


def risk_level(score: Decimal, policy: PolicySnapshot) -> str:
    if score >= Decimal("90"):
        return "CRITICAL"
    if score >= policy.risk_threshold:
        return "HIGH"
    if score >= policy.risk_threshold * Decimal("0.5"):
        return "MEDIUM"
    return "LOW"


class RoundingLedger:
    def __init__(self) -> None:
        self.steps: list[dict[str, Any]] = []
        self.cumulative_error = Decimal("0")

    def add(
        self,
        step_id: str,
        exact: Decimal,
        *,
        unit: str = "ratio",
        quantize: Decimal = ROUNDING_QUANTUM,
    ) -> Decimal:
        rounded = exact.quantize(quantize, rounding=ROUND_HALF_UP)
        error = abs(exact - rounded)
        self.cumulative_error += error
        self.steps.append(
            {
                "id": step_id,
                "exact": format(exact, ".12f"),
                "rounded": decimal_text(rounded),
                "fixed_point_scale": FIXED_SCALE,
                "fixed_point_value": fixed_micros(rounded),
                "rounding": f"{ROUNDING_MODE}@{decimal_text(quantize)}",
                "absolute_error": format(error, ".12f"),
                "unit": unit,
            }
        )
        return rounded

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": ROUNDING_MODE,
            "decimal_places": 6,
            "fixed_point_scale": FIXED_SCALE,
            "fixed_point_representation": "score_or_ratio * 1,000,000 as integer",
            "steps": self.steps,
            "cumulative_rounding_error": format(self.cumulative_error, ".12f"),
            "cumulative_rounding_error_micros": fixed_micros(self.cumulative_error),
            "note": "累计误差为每一步有符号值量化后的绝对误差之和。",
        }


@dataclass(frozen=True)
class RiskResult:
    risk_score: Decimal
    risk_level: str
    risk_threshold: Decimal
    stats: SampleStats
    baseline: BaselineStats
    components: dict[str, dict[str, Any]]
    ledger: RoundingLedger
    formula: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "risk_score": decimal_text(self.risk_score),
            "risk_score_fixed_point": fixed_micros(self.risk_score),
            "risk_level": self.risk_level,
            "risk_threshold": decimal_text(self.risk_threshold),
            "candidate_stats": self.stats.as_dict(),
            "baseline_stats": self.baseline.as_dict(),
            "components": self.components,
            "rounding": self.ledger.as_dict(),
            "formula": self.formula,
            "p90_contract": {
                "method": P90_METHOD,
                "definition": "升序排序，rank=max(1, ceil(0.90*n))，取第 rank 个样本。",
                "sample_size": self.stats.sample_count,
            },
        }

def calculate_risk(
    observations: Sequence[Observation],
    baseline: BaselineStats,
    policy: PolicySnapshot,
    ledger: RoundingLedger | None = None,
) -> RiskResult:
    stats = compute_stats(observations)
    ledger = ledger or RoundingLedger()

    # Sample-rate conversion is also recorded: a sample window built from raw
    # request outcomes can still have an exact repeating decimal.
    error_exact = Decimal(stats.sample_count - stats.success_count) / Decimal(
        stats.sample_count
    )
    error_rate = ledger.add("candidate.error_rate", error_exact)
    success_exact = Decimal(stats.success_count) / Decimal(stats.sample_count)
    success_rate = ledger.add("candidate.success_rate", success_exact)

    candidate_p90_ms = ledger.add(
        "candidate.p90_latency_ms",
        Decimal(stats.p90_micros) / Decimal(1000),
        unit="milliseconds",
        quantize=Decimal("0.001"),
    )

    error_excess_exact = max(Decimal("0"), error_rate - baseline.error_rate)
    error_excess = ledger.add("error.excess_over_baseline", error_excess_exact)
    error_normalized_exact = min(
        Decimal("1"), error_excess / policy.thresholds["error_rate"]
    )
    error_normalized = ledger.add("error.normalized_pressure", error_normalized_exact)
    error_component_exact = (
        error_normalized * policy.weights["error_rate"] * Decimal("100")
    )
    error_component = ledger.add("error.weighted_points", error_component_exact, unit="points")

    baseline_p90 = Decimal(baseline.p90_micros) / Decimal(1000)
    latency_ratio_exact = max(
        Decimal("0"),
        candidate_p90_ms / baseline_p90 - Decimal("1"),
    )
    latency_ratio = ledger.add("latency.p90_ratio_increase", latency_ratio_exact)
    latency_normalized_exact = min(
        Decimal("1"),
        latency_ratio / policy.thresholds["p90_latency_increase"],
    )
    latency_normalized = ledger.add("latency.normalized_pressure", latency_normalized_exact)
    latency_component_exact = (
        latency_normalized * policy.weights["latency"] * Decimal("100")
    )
    latency_component = ledger.add(
        "latency.weighted_points", latency_component_exact, unit="points"
    )

    success_drop_exact = max(Decimal("0"), baseline.success_rate - success_rate)
    success_drop = ledger.add("success.drop_from_baseline", success_drop_exact)
    success_normalized_exact = min(
        Decimal("1"), success_drop / policy.thresholds["success_rate_drop"]
    )
    success_normalized = ledger.add("success.normalized_pressure", success_normalized_exact)
    success_component_exact = (
        success_normalized * policy.weights["success_drop"] * Decimal("100")
    )
    success_component = ledger.add(
        "success.weighted_points", success_component_exact, unit="points"
    )

    score_exact = error_component + latency_component + success_component
    score = ledger.add("risk.total", score_exact, unit="points")
    components = {
        "error_rate": {
            "baseline": decimal_text(baseline.error_rate),
            "candidate": decimal_text(error_rate),
            "excess": decimal_text(error_excess),
            "threshold": decimal_text(policy.thresholds["error_rate"]),
            "normalized_pressure": decimal_text(error_normalized),
            "weight": decimal_text(policy.weights["error_rate"]),
            "points": decimal_text(error_component),
            "points_fixed_point": fixed_micros(error_component),
        },
        "p90_latency": {
            "baseline_ms": decimal_text(baseline.p90_ms),
            "candidate_ms": decimal_text(stats.p90_ms),
            "increase_ratio": decimal_text(latency_ratio),
            "threshold_ratio": decimal_text(policy.thresholds["p90_latency_increase"]),
            "normalized_pressure": decimal_text(latency_normalized),
            "weight": decimal_text(policy.weights["latency"]),
            "points": decimal_text(latency_component),
            "points_fixed_point": fixed_micros(latency_component),
        },
        "success_rate": {
            "baseline": decimal_text(baseline.success_rate),
            "candidate": decimal_text(success_rate),
            "drop": decimal_text(success_drop),
            "threshold": decimal_text(policy.thresholds["success_rate_drop"]),
            "normalized_pressure": decimal_text(success_normalized),
            "weight": decimal_text(policy.weights["success_drop"]),
            "points": decimal_text(success_component),
            "points_fixed_point": fixed_micros(success_component),
        },
    }
    result = RiskResult(
        risk_score=score,
        risk_level=risk_level(score, policy),
        risk_threshold=policy.risk_threshold,
        stats=stats,
        baseline=baseline,
        components=components,
        ledger=ledger,
        formula=(
            "risk = 100 * (w_error * min(1, max(0, error_rate-baseline_error)/error_threshold)"
            " + w_latency * min(1, max(0, p90/baseline_p90-1)/latency_threshold)"
            " + w_success * min(1, max(0, baseline_success-success_rate)/success_threshold))"
        ),
    )
    return result


def systematic_sample(
    observations: Sequence[Observation], cap: int
) -> tuple[list[Observation], dict[str, Any]]:
    ordered = sorted(observations, key=lambda item: item.sequence)
    total = len(ordered)
    if total <= cap:
        selected = list(ordered)
        method = "exhaustive"
        stride = 1
    else:
        selected_indices = [int(math.floor(i * total / cap)) for i in range(cap)]
        selected = [ordered[index] for index in selected_indices]
        method = "systematic_even_by_sorted_sequence"
        stride = total / cap
    sample_rate = (Decimal(len(selected)) / Decimal(total)).quantize(
        ROUNDING_QUANTUM, rounding=ROUND_HALF_UP
    )
    return selected, {
        "method": method,
        "sample_cap": cap,
        "population_size": total,
        "sample_size": len(selected),
        "sample_rate": decimal_text(sample_rate),
        "sample_rate_decimal": format(sample_rate, ".6f"),
        "stride": decimal_text(Decimal(str(stride))) if total > cap else "1",
        "selected_sequences": [item.sequence for item in selected],
        "note": (
            "按序号升序做确定性等距抽样；同一批次、窗口版本和策略版本会得到同一组样本。"
            if total > cap
            else "总体不超过上限，快速模式使用了窗口内全部观测。"
        ),
    }


def _deterministic_seed(
    observations: Sequence[Observation],
    policy: PolicySnapshot,
    salt: str,
) -> int:
    material = "|".join(
        [policy.digest, salt]
        + [f"{item.sequence}:{item.timestamp_ms}:{item.latency_micros}:{int(item.success)}" for item in observations]
    )
    return int(hashlib.sha256(material.encode("utf-8")).hexdigest()[:16], 16)


def bootstrap_interval(
    sample: Sequence[Observation],
    baseline: BaselineStats,
    policy: PolicySnapshot,
    *,
    salt: str,
    resamples: int = 400,
) -> dict[str, Any]:
    if len(sample) == 0:
        raise ValueError("bootstrap_interval requires observations")
    seed = _deterministic_seed(sample, policy, salt)
    rng = random.Random(seed)
    values: list[Decimal] = []
    for _ in range(resamples):
        draw = [sample[rng.randrange(len(sample))] for _ in range(len(sample))]
        values.append(calculate_risk(draw, baseline, policy).risk_score)
    values.sort()
    alpha = (Decimal("1") - policy.fast.confidence_level) / Decimal("2")

    def percentile_rank(p: Decimal) -> int:
        # Nearest-rank percentile, deterministic for a fixed bootstrap draw.
        rank = int(
            (p * Decimal(len(values))).quantize(Decimal("1"), rounding=ROUND_CEILING)
        )
        return min(len(values), max(1, rank))

    lower = values[percentile_rank(alpha) - 1]
    upper = values[percentile_rank(Decimal("1") - alpha) - 1]
    return {
        "method": "deterministic_bootstrap_percentile_nearest_rank",
        "confidence_level": decimal_text(policy.fast.confidence_level),
        "resamples": resamples,
        "seed": str(seed),
        "lower": decimal_text(q6(lower)),
        "upper": decimal_text(q6(upper)),
        "lower_fixed_point": fixed_micros(q6(lower)),
        "upper_fixed_point": fixed_micros(q6(upper)),
        "percentile_rule": "nearest-rank ceil(p*N), clamped to [1,N]",
    }


def decision_for_full(
    risk: RiskResult,
    policy: PolicySnapshot,
    window_state: str,
) -> tuple[str, str | None, str]:
    if risk.risk_score < policy.risk_threshold:
        if window_state == "CLOSED":
            return "RELEASE", "PASSED", "完整窗口风险分低于阈值，允许放行。"
        return "CONTINUE_OBSERVING", None, "风险可接受，但窗口尚未关闭，继续观察。"
    if policy.auto_rollback_enabled:
        return "AUTO_ROLLBACK", "ROLLING_BACK", "风险分达到阈值且自动回滚已启用。"
    return "ESCALATE", "ESCALATED", "风险分达到阈值但自动回滚关闭，升级人工处置。"


def decision_for_fast(
    risk: RiskResult,
    interval: dict[str, Any],
    policy: PolicySnapshot,
    window_state: str,
) -> dict[str, Any]:
    point = risk.risk_score
    lower = Decimal(str(interval["lower"]))
    upper = Decimal(str(interval["upper"]))
    threshold = policy.risk_threshold
    bound = policy.fast.confidence_upper_bound
    threshold_crossed = point >= threshold
    interval_straddles = lower < threshold <= upper
    upper_gap = max(Decimal("0"), upper - point)
    confidence_bound_exceeded = upper_gap > bound
    reasons: list[str] = ["快速模式结论仅用于预判，不能直接触发自动回滚。"]
    if threshold_crossed:
        reasons.append("点估计风险分已越过风险阈值。")
    if interval_straddles:
        reasons.append("置信区间跨越风险阈值，方向不完全确定。")
    if confidence_bound_exceeded:
        reasons.append(
            f"置信上界与点估计的间距 {decimal_text(q6(upper_gap))} 超过配置上限 {decimal_text(bound)}。"
        )
    requires_full_review = threshold_crossed or interval_straddles
    if requires_full_review:
        action = "REQUIRES_FULL_REVIEW"
        narrative = "快速证据不足或已越阈值，需完整裁决复核；未修改批次状态。"
    elif window_state == "CLOSED":
        action = "FAST_RELEASE_ADVISORY"
        narrative = "快速点估计与置信区间均低于阈值，仅给出放行建议；正式放行需完整裁决。"
    else:
        action = "FAST_CONTINUE_OBSERVING"
        narrative = "快速结论建议继续观察；正式状态迁移需完整裁决。"
    return {
        "action": action,
        "requires_full_review": requires_full_review,
        "can_auto_rollback": False,
        "threshold_crossed": threshold_crossed,
        "confidence_interval_straddles_threshold": interval_straddles,
        "confidence_upper_bound": decimal_text(bound),
        "confidence_upper_gap": decimal_text(q6(upper_gap)),
        "confidence_bound_exceeded": confidence_bound_exceeded,
        "reasons": reasons,
        "narrative": narrative,
    }
