from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from .errors import BusinessError


def decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def to_decimal(value: Any, field: str, code: str = "E_POLICY_INVALID") -> Decimal:
    subject = {
        "E_BATCH_INVALID": "批次",
        "E_OBSERVATION_INVALID": "观测",
    }.get(code, "策略")
    if isinstance(value, bool) or value is None:
        raise BusinessError(
            code,
            f"{subject}字段必须是有限数值",
            {"field": field, "value": value},
        )
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise BusinessError(
            code,
            f"{subject}字段必须是有限数值",
            {"field": field, "value": value},
        ) from None
    if not result.is_finite():
        raise BusinessError(
            code,
            f"{subject}字段必须是有限数值",
            {"field": field, "value": value},
        )
    return result


def require_int(value: Any, field: str, minimum: int, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise BusinessError(
            "E_POLICY_INVALID",
            "策略字段必须是整数",
            {"field": field, "value": value},
        )
    if value < minimum or (maximum is not None and value > maximum):
        raise BusinessError(
            "E_POLICY_INVALID",
            "策略整数字段超出允许范围",
            {"field": field, "value": value, "minimum": minimum, "maximum": maximum},
        )
    return value


def require_object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise BusinessError(
            "E_POLICY_INVALID",
            "策略字段必须是对象",
            {"field": field, "value": value},
        )
    return value


@dataclass(frozen=True)
class FastPolicy:
    sample_cap: int
    confidence_level: Decimal
    confidence_upper_bound: Decimal

    def as_dict(self) -> dict[str, Any]:
        return {
            "sample_cap": self.sample_cap,
            "confidence_level": decimal_text(self.confidence_level),
            "confidence_upper_bound": decimal_text(self.confidence_upper_bound),
        }


@dataclass(frozen=True)
class PolicySnapshot:
    version: int
    generated_at_ms: int
    window_ms: int
    late_tolerance_ms: int
    min_samples: int
    thresholds: dict[str, Decimal]
    weights: dict[str, Decimal]
    risk_threshold: Decimal
    auto_rollback_enabled: bool
    cooldown_ms: int
    fast: FastPolicy
    digest: str
    canonical_json: str

    @classmethod
    def from_mapping(cls, raw: Any) -> "PolicySnapshot":
        payload = require_object(raw, "$")
        version = require_int(payload.get("version"), "version", 1)
        generated_at_ms = require_int(payload.get("generated_at_ms"), "generated_at_ms", 1)
        window_ms = require_int(payload.get("window_ms"), "window_ms", 1_000)
        late_tolerance_ms = require_int(
            payload.get("late_tolerance_ms"), "late_tolerance_ms", 0
        )
        if late_tolerance_ms >= window_ms:
            raise BusinessError(
                "E_POLICY_INVALID",
                "迟到容忍必须小于观察窗口长度",
                {
                    "field": "late_tolerance_ms",
                    "late_tolerance_ms": late_tolerance_ms,
                    "window_ms": window_ms,
                },
            )
        min_samples = require_int(payload.get("min_samples"), "min_samples", 1)
        risk_threshold = to_decimal(payload.get("risk_threshold"), "risk_threshold")
        if not (Decimal("0") < risk_threshold <= Decimal("100")):
            raise BusinessError(
                "E_POLICY_INVALID",
                "风险阈值必须位于 (0, 100]",
                {"field": "risk_threshold", "value": decimal_text(risk_threshold)},
            )
        auto_rollback_enabled = payload.get("auto_rollback_enabled")
        if not isinstance(auto_rollback_enabled, bool):
            raise BusinessError(
                "E_POLICY_INVALID",
                "自动回滚开关必须是布尔值",
                {
                    "field": "auto_rollback_enabled",
                    "value": auto_rollback_enabled,
                },
            )
        cooldown_ms = require_int(payload.get("cooldown_ms"), "cooldown_ms", 0)

        thresholds_raw = require_object(payload.get("thresholds"), "thresholds")
        thresholds = {
            "error_rate": to_decimal(thresholds_raw.get("error_rate"), "thresholds.error_rate"),
            "p90_latency_increase": to_decimal(
                thresholds_raw.get("p90_latency_increase"),
                "thresholds.p90_latency_increase",
            ),
            "success_rate_drop": to_decimal(
                thresholds_raw.get("success_rate_drop"),
                "thresholds.success_rate_drop",
            ),
        }
        threshold_limits = {
            "error_rate": (Decimal("0"), Decimal("1")),
            "p90_latency_increase": (Decimal("0"), Decimal("10")),
            "success_rate_drop": (Decimal("0"), Decimal("1")),
        }
        for name, (lower, upper) in threshold_limits.items():
            if not (lower < thresholds[name] <= upper):
                raise BusinessError(
                    "E_POLICY_INVALID",
                    "指标判定阈值超出允许范围",
                    {
                        "field": f"thresholds.{name}",
                        "value": decimal_text(thresholds[name]),
                        "allowed": (
                            f"({decimal_text(lower)}, {decimal_text(upper)}]"
                        ),
                    },
                )

        default_weights = {
            "error_rate": Decimal("0.40"),
            "latency": Decimal("0.35"),
            "success_drop": Decimal("0.25"),
        }
        weights_raw = payload.get("weights", default_weights)
        weights_raw = require_object(weights_raw, "weights")
        weights = {
            key: to_decimal(weights_raw.get(key, default_weights[key]), f"weights.{key}")
            for key in default_weights
        }
        if any(weight <= 0 for weight in weights.values()):
            raise BusinessError(
                "E_POLICY_INVALID",
                "风险分项权重必须大于 0",
                {"field": "weights", "weights": {k: decimal_text(v) for k, v in weights.items()}},
            )
        if abs(sum(weights.values()) - Decimal("1")) > Decimal("0.000000001"):
            raise BusinessError(
                "E_POLICY_INVALID",
                "风险分项权重之和必须为 1",
                {"field": "weights", "sum": decimal_text(sum(weights.values()))},
            )

        fast_raw = require_object(payload.get("fast"), "fast")
        fast = FastPolicy(
            sample_cap=require_int(fast_raw.get("sample_cap"), "fast.sample_cap", 1),
            confidence_level=to_decimal(
                fast_raw.get("confidence_level"), "fast.confidence_level"
            ),
            confidence_upper_bound=to_decimal(
                fast_raw.get("confidence_upper_bound"), "fast.confidence_upper_bound"
            ),
        )
        if not (Decimal("0.5") <= fast.confidence_level < Decimal("1")):
            raise BusinessError(
                "E_POLICY_INVALID",
                "快速模式置信水平必须位于 [0.5, 1)",
                {
                    "field": "fast.confidence_level",
                    "value": decimal_text(fast.confidence_level),
                },
            )
        if fast.confidence_upper_bound < 0:
            raise BusinessError(
                "E_POLICY_INVALID",
                "置信上界不能为负数",
                {
                    "field": "fast.confidence_upper_bound",
                    "value": decimal_text(fast.confidence_upper_bound),
                },
            )
        if fast.sample_cap < min_samples:
            raise BusinessError(
                "E_POLICY_INVALID",
                "快速采样上限不得小于最小样本数",
                {
                    "field": "fast.sample_cap",
                    "sample_cap": fast.sample_cap,
                    "min_samples": min_samples,
                },
            )

        normalized = {
            "version": version,
            "generated_at_ms": generated_at_ms,
            "window_ms": window_ms,
            "late_tolerance_ms": late_tolerance_ms,
            "min_samples": min_samples,
            "thresholds": {key: decimal_text(value) for key, value in thresholds.items()},
            "weights": {key: decimal_text(value) for key, value in weights.items()},
            "risk_threshold": decimal_text(risk_threshold),
            "auto_rollback_enabled": auto_rollback_enabled,
            "cooldown_ms": cooldown_ms,
            "fast": fast.as_dict(),
        }
        canonical_json = json.dumps(
            normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        digest = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
        supplied_digest = payload.get("digest")
        if supplied_digest is not None:
            if not isinstance(supplied_digest, str) or supplied_digest.lower() != digest:
                raise BusinessError(
                    "E_POLICY_INVALID",
                    "策略摘要与规范化内容不一致",
                    {
                        "field": "digest",
                        "supplied": supplied_digest,
                        "computed": digest,
                    },
                )
        return cls(
            version=version,
            generated_at_ms=generated_at_ms,
            window_ms=window_ms,
            late_tolerance_ms=late_tolerance_ms,
            min_samples=min_samples,
            thresholds=thresholds,
            weights=weights,
            risk_threshold=risk_threshold,
            auto_rollback_enabled=auto_rollback_enabled,
            cooldown_ms=cooldown_ms,
            fast=fast,
            digest=digest,
            canonical_json=canonical_json,
        )

    @classmethod
    def from_storage(cls, row: Any) -> "PolicySnapshot":
        data = json.loads(row["canonical_json"])
        data["digest"] = row["digest"]
        return cls.from_mapping(data)

    def as_dict(self) -> dict[str, Any]:
        data = json.loads(self.canonical_json)
        data["digest"] = self.digest
        return data
