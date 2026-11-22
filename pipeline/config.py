from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import (
    E_INVALID_CONFIG,
    E_INVALID_FPR_LIMIT,
    E_INVALID_SMOOTHING,
    PipelineError,
)
from .util import canonical_digest
from . import yaml_lite


@dataclass(frozen=True)
class CalibrationConfig:
    fold_modulus: int
    fold_remainder: int
    min_samples_per_class: int


@dataclass(frozen=True)
class Config:
    version: str
    hash_dim: int
    smoothing: float
    unknown_token: str
    labels: tuple[str, ...]
    thresholds: dict[str, float]
    target_fpr: float
    threshold_max: float
    evaluation_budget: int
    workers: int
    class_weights: dict[str, float]
    calibration: CalibrationConfig
    digest: str

    def stage_config(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "hash_dim": self.hash_dim,
            "smoothing": self.smoothing,
            "unknown_token": self.unknown_token,
            "labels": list(self.labels),
            "thresholds": self.thresholds,
            "target_fpr": self.target_fpr,
            "threshold_max": self.threshold_max,
            "evaluation_budget": self.evaluation_budget,
            "workers": self.workers,
            "class_weights": self.class_weights,
            "calibration": {
                "fold_modulus": self.calibration.fold_modulus,
                "fold_remainder": self.calibration.fold_remainder,
                "min_samples_per_class": self.calibration.min_samples_per_class,
            },
        }


def _invalid(message: str, **details: Any) -> PipelineError:
    return PipelineError(E_INVALID_CONFIG, message, details)


def _number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _invalid(f"{field} must be numeric", field=field)
    result = float(value)
    if not math.isfinite(result):
        raise _invalid(f"{field} must be finite", field=field)
    return result


def _integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _invalid(f"{field} must be an integer", field=field)
    return value


def from_dict(raw: dict[str, Any]) -> Config:
    if not isinstance(raw, dict):
        raise _invalid("configuration root must be a mapping")
    required = {
        "version",
        "hash_dim",
        "smoothing",
        "unknown_token",
        "labels",
        "thresholds",
        "target_fpr",
        "threshold_max",
        "evaluation_budget",
        "workers",
        "class_weights",
        "calibration",
    }
    missing = sorted(required - set(raw))
    if missing:
        raise _invalid("missing configuration fields", fields=missing)
    version = raw["version"]
    if not isinstance(version, str) or not version:
        raise _invalid("version must be a non-empty string")
    hash_dim = _integer(raw["hash_dim"], "hash_dim")
    if hash_dim <= 0 or hash_dim > 1_000_000:
        raise _invalid("hash_dim must be between 1 and 1000000", field="hash_dim", value=hash_dim)
    smoothing = _number(raw["smoothing"], "smoothing")
    if smoothing < 0:
        raise PipelineError(
            E_INVALID_SMOOTHING,
            "smoothing must be non-negative; zero is accepted but produces explicit zero-probability diagnostics",
            {"smoothing": smoothing},
        )
    unknown_token = raw["unknown_token"]
    if not isinstance(unknown_token, str) or not unknown_token:
        raise _invalid("unknown_token must be a non-empty string")
    labels_raw = raw["labels"]
    if not isinstance(labels_raw, list) or not labels_raw:
        raise _invalid("labels must be a non-empty list")
    labels: list[str] = []
    for label in labels_raw:
        if not isinstance(label, str) or not label:
            raise _invalid("every label must be a non-empty string")
        if label in labels:
            raise _invalid("labels must be unique", label=label)
        labels.append(label)
    thresholds_raw = raw["thresholds"]
    if not isinstance(thresholds_raw, dict):
        raise _invalid("thresholds must be a mapping")
    thresholds: dict[str, float] = {}
    for label in labels:
        if label not in thresholds_raw:
            raise _invalid("missing threshold", label=label)
        thresholds[label] = _number(thresholds_raw[label], f"thresholds.{label}")
        if not 0.0 <= thresholds[label] <= 1.0:
            raise _invalid("threshold must be within [0, 1]", label=label, threshold=thresholds[label])
    target_fpr = _number(raw["target_fpr"], "target_fpr")
    if not 0.0 <= target_fpr <= 1.0:
        raise PipelineError(
            E_INVALID_FPR_LIMIT,
            "target_fpr must be within [0, 1]",
            {"target_fpr": target_fpr},
        )
    threshold_max = _number(raw["threshold_max"], "threshold_max")
    if not 0.0 <= threshold_max <= 1.0:
        raise _invalid("threshold_max must be within [0, 1]", threshold_max=threshold_max)
    for label, threshold in thresholds.items():
        if threshold > threshold_max:
            raise _invalid(
                "configured threshold exceeds threshold_max",
                label=label,
                threshold=threshold,
                threshold_max=threshold_max,
            )
    evaluation_budget = _integer(raw["evaluation_budget"], "evaluation_budget")
    if evaluation_budget < 0:
        raise _invalid("evaluation_budget must be non-negative")
    workers = _integer(raw["workers"], "workers")
    if workers < 1:
        raise _invalid("workers must be at least one")
    weights_raw = raw["class_weights"]
    if not isinstance(weights_raw, dict):
        raise _invalid("class_weights must be a mapping")
    class_weights: dict[str, float] = {}
    for label in labels:
        if label not in weights_raw:
            raise _invalid("missing class weight", label=label)
        weight = _number(weights_raw[label], f"class_weights.{label}")
        if weight <= 0:
            raise _invalid("class weight must be positive", label=label, weight=weight)
        class_weights[label] = weight
    calibration_raw = raw["calibration"]
    if not isinstance(calibration_raw, dict):
        raise _invalid("calibration must be a mapping")
    calibration_fields = {"fold_modulus", "fold_remainder", "min_samples_per_class"}
    missing_calibration = sorted(calibration_fields - set(calibration_raw))
    if missing_calibration:
        raise _invalid("missing calibration fields", fields=missing_calibration)
    fold_modulus = _integer(calibration_raw["fold_modulus"], "calibration.fold_modulus")
    fold_remainder = _integer(calibration_raw["fold_remainder"], "calibration.fold_remainder")
    min_samples = _integer(
        calibration_raw["min_samples_per_class"], "calibration.min_samples_per_class"
    )
    if fold_modulus < 2:
        raise _invalid("calibration.fold_modulus must be at least 2")
    if not 0 <= fold_remainder < fold_modulus:
        raise _invalid("calibration.fold_remainder must be in [0, fold_modulus)")
    if min_samples < 0:
        raise _invalid("calibration.min_samples_per_class must be non-negative")
    normalized = {
        "version": version,
        "hash_dim": hash_dim,
        "smoothing": smoothing,
        "unknown_token": unknown_token,
        "labels": labels,
        "thresholds": thresholds,
        "target_fpr": target_fpr,
        "threshold_max": threshold_max,
        "evaluation_budget": evaluation_budget,
        "workers": workers,
        "class_weights": class_weights,
        "calibration": {
            "fold_modulus": fold_modulus,
            "fold_remainder": fold_remainder,
            "min_samples_per_class": min_samples,
        },
    }
    return Config(
        version=version,
        hash_dim=hash_dim,
        smoothing=smoothing,
        unknown_token=unknown_token,
        labels=tuple(labels),
        thresholds=thresholds,
        target_fpr=target_fpr,
        threshold_max=threshold_max,
        evaluation_budget=evaluation_budget,
        workers=workers,
        class_weights=class_weights,
        calibration=CalibrationConfig(
            fold_modulus=fold_modulus,
            fold_remainder=fold_remainder,
            min_samples_per_class=min_samples,
        ),
        digest=canonical_digest(normalized),
    )


def load(path: Path | str) -> Config:
    try:
        raw = yaml_lite.load(str(path))
    except Exception as exc:
        raise _invalid("unable to parse YAML configuration", path=str(path), reason=str(exc)) from exc
    return from_dict(raw)
