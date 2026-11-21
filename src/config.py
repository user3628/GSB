"""Backtest configuration: YAML loading, validation and version fingerprint.

The configuration is the single source of truth for every tunable number.
A canonical SHA-256 fingerprint of the validated configuration is embedded
in every checkpoint so that any configuration change is detected.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, asdict
from typing import Any, Dict, List

import yaml

from .errors import ConfigError


METHODS = ("moving_average", "exponential_smoothing", "linear_ar")


@dataclass
class BacktestConfig:
    # method selection
    method: str = "moving_average"
    ar_lags: int = 3
    es_alpha: float = 0.3
    es_phi: float = 0.9
    # rolling window geometry
    window_length: int = 24
    step: int = 6
    horizon: int = 3
    # parallelism and budgets
    max_workers: int = 2
    global_fit_budget: int = 1000
    worker_fit_quota: int = 1000
    per_fit_time_budget_ms: int = 2000
    # data quality rules
    max_missing_ratio: float = 0.2
    changepoint_threshold: float = 0.5
    # diagnostics
    confidence_level: float = 0.95
    version: str = "v1"

    def validate(self) -> None:
        if self.method not in METHODS:
            raise ConfigError(
                f"unknown method {self.method!r}, expected one of {METHODS}",
                context={"method": self.method},
            )
        if self.window_length <= 1:
            raise ConfigError("window_length must be > 1",
                              context={"window_length": self.window_length})
        if self.step <= 0:
            raise ConfigError("step must be positive", context={"step": self.step})
        if self.horizon <= 0:
            raise ConfigError("horizon must be positive",
                              context={"horizon": self.horizon})
        if not 0 < self.max_missing_ratio < 1:
            raise ConfigError("max_missing_ratio must be inside (0, 1)",
                              context={"max_missing_ratio": self.max_missing_ratio})
        if not 0.0 < self.es_alpha <= 1.0:
            raise ConfigError("es_alpha must be inside (0, 1]",
                              context={"es_alpha": self.es_alpha})
        if not 0.0 < self.es_phi <= 1.0:
            raise ConfigError("es_phi must be inside (0, 1]",
                              context={"es_phi": self.es_phi})
        if self.ar_lags < 1:
            raise ConfigError("ar_lags must be >= 1",
                              context={"ar_lags": self.ar_lags})
        if self.ar_lags >= self.window_length:
            raise ConfigError("ar_lags must be smaller than window_length",
                              context={"ar_lags": self.ar_lags,
                                       "window_length": self.window_length})
        if self.max_workers < 1:
            raise ConfigError("max_workers must be >= 1",
                              context={"max_workers": self.max_workers})
        if self.global_fit_budget < 1:
            raise ConfigError("global_fit_budget must be >= 1",
                              context={"global_fit_budget": self.global_fit_budget})
        if self.worker_fit_quota < 1:
            raise ConfigError("worker_fit_quota must be >= 1",
                              context={"worker_fit_quota": self.worker_fit_quota})
        if self.per_fit_time_budget_ms <= 0:
            raise ConfigError("per_fit_time_budget_ms must be positive",
                              context={"per_fit_time_budget_ms":
                                       self.per_fit_time_budget_ms})
        if not 0.0 < self.confidence_level < 1.0:
            raise ConfigError("confidence_level must be inside (0, 1)",
                              context={"confidence_level": self.confidence_level})

    def canonical_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        # keep only fields that influence predictions/scheduling
        return {key: data[key] for key in sorted(data)}

    def fingerprint(self) -> str:
        payload = canonical_json(self.canonical_dict())
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def canonical_json(data: Any) -> str:
    """Deterministic JSON encoding used for fingerprints and persisted outputs."""
    import json
    return json.dumps(data, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


def load_config(path: str) -> BacktestConfig:
    with open(path, "r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    if not isinstance(raw, dict):
        raise ConfigError("YAML root must be a mapping", context={"path": path})
    allowed = set(BacktestConfig.__dataclass_fields__)  # type: ignore[attr-defined]
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ConfigError(f"unknown configuration keys: {unknown}",
                          context={"unknown": unknown})
    cfg = BacktestConfig(**raw)
    cfg.validate()
    return cfg


def dump_config(cfg: BacktestConfig, path: str) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        yaml.safe_dump(cfg.to_dict(), handle, sort_keys=True,
                       default_flow_style=False)


__all__: List[str] = ["BacktestConfig", "METHODS", "load_config", "dump_config",
                      "canonical_json"]
