"""Validated, versioned workbench configuration."""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields, replace
import math
from typing import Any

from .errors import WorkbenchError
from .util import sha256_json


ALGORITHM_VERSION = "gsb-hgbt-1.0.0"


@dataclass(frozen=True)
class Config:
    bins: int = 24
    rounds: int = 18
    learning_rate: float = 0.12
    max_depth: int = 3
    min_leaf: int = 20
    l2: float = 1.0
    calibrator: str = "platt"
    calibration_folds: int = 3
    calibration_seed: int = 20261121
    platt_max_iter: int = 100
    platt_tol: float = 1e-12
    diagnostic_bins: int = 10
    quantize_decimals: int = 12

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def version(self) -> str:
        return sha256_json(self.to_dict())

    def with_patch(self, patch: dict[str, Any]) -> "Config":
        allowed = {field.name for field in fields(self)}
        unknown = sorted(set(patch) - allowed)
        if unknown:
            raise WorkbenchError(
                "E_INVALID_PARAM",
                "unknown configuration field(s)",
                {"fields": unknown, "allowed": sorted(allowed)},
            )
        casted: dict[str, Any] = {}
        for key, value in patch.items():
            current = getattr(self, key)
            try:
                if isinstance(current, bool):
                    casted[key] = bool(value)
                elif isinstance(current, int):
                    casted[key] = int(value)
                elif isinstance(current, float):
                    casted[key] = float(value)
                else:
                    casted[key] = str(value)
            except (TypeError, ValueError) as exc:
                raise WorkbenchError(
                    "E_INVALID_PARAM",
                    f"cannot parse configuration field {key}",
                    {"field": key, "value": value},
                ) from exc
        return replace(self, **casted).validate()

    def validate(self) -> "Config":
        errors: dict[str, str] = {}
        if not isinstance(self.bins, int) or self.bins <= 0:
            errors["bins"] = "must be a positive integer"
        elif self.bins > 1024:
            errors["bins"] = "must be <= 1024"
        if not isinstance(self.rounds, int) or self.rounds <= 0:
            errors["rounds"] = "must be a positive integer"
        if not isinstance(self.learning_rate, float) or not math.isfinite(self.learning_rate) or not (0.0 < self.learning_rate <= 1.0):
            errors["learning_rate"] = "must satisfy 0 < learning_rate <= 1"
        if not isinstance(self.max_depth, int) or not (1 <= self.max_depth <= 12):
            errors["max_depth"] = "must be an integer in [1, 12]"
        if not isinstance(self.min_leaf, int) or self.min_leaf < 1:
            errors["min_leaf"] = "must be a positive integer"
        if not isinstance(self.l2, float) or not math.isfinite(self.l2) or self.l2 < 0.0:
            errors["l2"] = "must be >= 0"
        if self.calibrator not in {"platt", "isotonic"}:
            errors["calibrator"] = "must be 'platt' or 'isotonic'"
        if not isinstance(self.calibration_folds, int) or not (2 <= self.calibration_folds <= 20):
            errors["calibration_folds"] = "must be an integer in [2, 20]"
        if not isinstance(self.calibration_seed, int) or self.calibration_seed < 0:
            errors["calibration_seed"] = "must be a non-negative integer"
        if not isinstance(self.platt_max_iter, int) or not (1 <= self.platt_max_iter <= 1000):
            errors["platt_max_iter"] = "must be an integer in [1, 1000]"
        if not isinstance(self.platt_tol, float) or not math.isfinite(self.platt_tol) or not (0.0 < self.platt_tol <= 1.0):
            errors["platt_tol"] = "must satisfy 0 < platt_tol <= 1"
        if not isinstance(self.diagnostic_bins, int) or not (1 <= self.diagnostic_bins <= 100):
            errors["diagnostic_bins"] = "must be an integer in [1, 100]"
        if not isinstance(self.quantize_decimals, int) or not (0 <= self.quantize_decimals <= 15):
            errors["quantize_decimals"] = "must be an integer in [0, 15]"
        if errors:
            raise WorkbenchError("E_INVALID_PARAM", "invalid configuration", {"fields": errors})
        return self

    def require_training_ready(self) -> "Config":
        self.validate()
        if self.bins <= 0:
            raise WorkbenchError("E_INVALID_PARAM", "bins must be positive", {"bins": self.bins})
        if not (0.0 < self.learning_rate <= 1.0):
            raise WorkbenchError(
                "E_INVALID_PARAM",
                "learning_rate must satisfy 0 < learning_rate <= 1",
                {"learning_rate": self.learning_rate},
            )
        return self

    def stage_config_version(self, stage: str) -> str:
        fields_by_stage = {
            "binning": ("bins",),
            "boosting": ("rounds", "learning_rate", "max_depth", "min_leaf", "l2"),
            "calibration": (
                "calibrator",
                "calibration_folds",
                "calibration_seed",
                "platt_max_iter",
                "platt_tol",
                "quantize_decimals",
            ),
            "diagnostics": ("diagnostic_bins",),
        }
        if stage not in fields_by_stage:
            raise WorkbenchError("E_INVALID_STAGE", f"unknown stage: {stage}", {"stage": stage})
        payload = {"algorithm_version": ALGORITHM_VERSION, stage: {name: getattr(self, name) for name in fields_by_stage[stage]}}
        return sha256_json(payload)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Config":
        return cls().with_patch(value)


DEFAULT_CONFIG = Config().validate()


CONFIG_STAGE_FIELDS = {
    "bins": "binning",
    "rounds": "boosting",
    "learning_rate": "boosting",
    "max_depth": "boosting",
    "min_leaf": "boosting",
    "l2": "boosting",
    "calibrator": "calibration",
    "calibration_folds": "calibration",
    "calibration_seed": "calibration",
    "platt_max_iter": "calibration",
    "platt_tol": "calibration",
    "quantize_decimals": "calibration",
    "diagnostic_bins": "diagnostics",
}
