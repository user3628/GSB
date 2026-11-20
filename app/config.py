from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .errors import BusinessError, E_REQUEST_INVALID


@dataclass(frozen=True, slots=True)
class EngineConfig:
    tumbling_size_ms: int = 1000
    sliding_size_ms: int = 2000
    sliding_step_ms: int = 500
    session_gap_ms: int = 700
    allowed_lateness_ms: int = 0
    late_policy: str = "queue"
    active_window_soft_limit: int = 64
    active_window_hard_limit: int = 96
    late_queue_soft_limit: int = 64
    late_queue_hard_limit: int = 96
    batch_event_hard_limit: int = 10000
    sum_decimals: int = 6
    default_quantiles: tuple[float, ...] = (0.5, 0.9, 0.95, 0.99)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "EngineConfig":
        known = {field for field in cls.__dataclass_fields__}
        unknown = sorted(set(raw) - known)
        if unknown:
            raise BusinessError(E_REQUEST_INVALID, "Unknown config keys", details={"keys": unknown})
        data = dict(raw)
        if "default_quantiles" in data:
            data["default_quantiles"] = tuple(float(value) for value in data["default_quantiles"])
        try:
            config = cls(**data)
        except (TypeError, ValueError) as exc:
            raise BusinessError(E_REQUEST_INVALID, f"Invalid config: {exc}") from exc
        config.validate()
        return config

    def validate(self) -> None:
        integer_fields = (
            "tumbling_size_ms",
            "sliding_size_ms",
            "sliding_step_ms",
            "session_gap_ms",
            "allowed_lateness_ms",
            "active_window_soft_limit",
            "active_window_hard_limit",
            "late_queue_soft_limit",
            "late_queue_hard_limit",
            "batch_event_hard_limit",
            "sum_decimals",
        )
        for field in integer_fields:
            value = getattr(self, field)
            if not isinstance(value, int) or isinstance(value, bool):
                raise BusinessError(E_REQUEST_INVALID, f"{field} must be an integer")
        if self.tumbling_size_ms <= 0 or self.sliding_size_ms <= 0 or self.sliding_step_ms <= 0:
            raise BusinessError(E_REQUEST_INVALID, "Window sizes and step must be positive")
        if self.sliding_step_ms >= self.sliding_size_ms:
            raise BusinessError(E_REQUEST_INVALID, "sliding_step_ms must be smaller than sliding_size_ms")
        if self.session_gap_ms < 0 or self.allowed_lateness_ms < 0:
            raise BusinessError(E_REQUEST_INVALID, "Gap and lateness must be non-negative")
        if self.late_policy not in {"queue", "reject"}:
            raise BusinessError(E_REQUEST_INVALID, "late_policy must be queue or reject")
        for soft, hard, name in (
            (self.active_window_soft_limit, self.active_window_hard_limit, "active_window"),
            (self.late_queue_soft_limit, self.late_queue_hard_limit, "late_queue"),
        ):
            if soft <= 0 or hard <= 0 or soft > hard:
                raise BusinessError(E_REQUEST_INVALID, f"{name} limits must satisfy 0 < soft <= hard")
        if self.batch_event_hard_limit <= 0:
            raise BusinessError(E_REQUEST_INVALID, "batch_event_hard_limit must be positive")
        if not 0 <= self.sum_decimals <= 12:
            raise BusinessError(E_REQUEST_INVALID, "sum_decimals must be between 0 and 12")
        if not self.default_quantiles:
            raise BusinessError(E_REQUEST_INVALID, "default_quantiles cannot be empty")
        for probability in self.default_quantiles:
            if not 0.0 < probability <= 1.0:
                raise BusinessError(E_REQUEST_INVALID, "Quantiles must satisfy 0 < p <= 1")

    def canonical_dict(self) -> dict[str, Any]:
        raw = asdict(self)
        raw["default_quantiles"] = list(self.default_quantiles)
        return raw

    @property
    def fingerprint(self) -> str:
        canonical = json.dumps(
            self.canonical_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def public_dict(self) -> dict[str, Any]:
        data = self.canonical_dict()
        data.update(
            {
                "config_fingerprint": self.fingerprint,
                "window_boundaries": {
                    "tumbling": "[start_ms, end_ms)",
                    "sliding": "[start_ms, end_ms)",
                    "session": "[first_event_ts_ms, last_event_ts_ms] with close after last_event + session_gap",
                },
                "sum_rounding": {
                    "method": "Neumaier compensated summation, then ROUND_HALF_EVEN",
                    "decimals": self.sum_decimals,
                    "json_representation": "fixed-width decimal string",
                },
                "quantile_method": "nearest-rank: ceil(p * sample_size), clamped to [1, sample_size]",
                "late_event_policy": self.late_policy,
            }
        )
        return data


def load_config(path: str | Path | None = None) -> EngineConfig:
    selected = Path(path or os.environ.get("DWE_CONFIG", "samples/config.json"))
    if not selected.exists():
        raise BusinessError(E_REQUEST_INVALID, f"Config file not found: {selected}")
    try:
        raw = json.loads(selected.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BusinessError(E_REQUEST_INVALID, f"Cannot read config: {exc}") from exc
    if not isinstance(raw, dict):
        raise BusinessError(E_REQUEST_INVALID, "Config root must be an object")
    return EngineConfig.from_dict(raw)
