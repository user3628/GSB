"""Configuration merging, validation and stable fingerprints."""

from __future__ import annotations

import copy
import math
from typing import Any, Iterable

from .canonical import digest
from .errors import InvalidCV, InvalidHyperparameter

DEFAULT_CONFIG: dict[str, Any] = {
    "schema_version": 1,
    "dataset_id": "small",
    "preprocessing": {
        "numeric_normalization": "standard",
        "numeric_imputation": "median",
        "categorical_encoding": "onehot",
        "categorical_imputation": "mode",
        "target_encoding_smoothing": 10.0,
    },
    "cv": {
        "folds": 5,
        "seed": 20261120,
        "stratify": True,
        "metric": "log_loss",
    },
    "regularization": {
        "mode": "elasticnet",
        "strengths": [0.02],
        "l1_ratios": [0.5],
    },
    "solver": {
        "name": "coordinate_descent",
        "tolerance": 0.000001,
        "max_iter": 250,
        "objective_limit": 1000000.0,
    },
    "selection": {
        "tie_break": "smaller_strength_then_larger_l1_ratio",
    },
}

_ALLOWED_TOP_LEVEL = {
    "schema_version",
    "dataset_id",
    "preprocessing",
    "cv",
    "regularization",
    "solver",
    "selection",
}
_ALLOWED_KEYS = {
    "preprocessing": {
        "numeric_normalization",
        "numeric_imputation",
        "categorical_encoding",
        "categorical_imputation",
        "target_encoding_smoothing",
    },
    "cv": {"folds", "seed", "stratify", "metric"},
    "regularization": {"mode", "strengths", "l1_ratios"},
    "solver": {"name", "tolerance", "max_iter", "objective_limit"},
    "selection": {"tie_break"},
}


def default_config(dataset_id: str = "small") -> dict[str, Any]:
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["dataset_id"] = dataset_id
    return config


def deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(patch, dict):
        raise InvalidHyperparameter("Configuration patch must be an object")
    unknown = set(patch) - _ALLOWED_TOP_LEVEL
    if unknown:
        raise InvalidHyperparameter(
            "Unknown configuration field",
            details={"fields": sorted(unknown)},
        )
    merged = copy.deepcopy(base)
    for key, value in patch.items():
        if key in _ALLOWED_KEYS:
            if not isinstance(value, dict):
                raise InvalidHyperparameter(f"{key} must be an object")
            bad = set(value) - _ALLOWED_KEYS[key]
            if bad:
                raise InvalidHyperparameter(
                    f"Unknown {key} field", details={"fields": sorted(bad)}
                )
            merged.setdefault(key, {})
            for subkey, subvalue in value.items():
                merged[key][subkey] = copy.deepcopy(subvalue)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _finite_number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise InvalidHyperparameter(f"{name} must be a finite number")
    number = float(value)
    if not math.isfinite(number):
        raise InvalidHyperparameter(f"{name} must be finite")
    return number


def _integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidHyperparameter(f"{name} must be an integer")
    return value


def _number_list(value: Any, name: str) -> list[float]:
    if not isinstance(value, list) or not value:
        raise InvalidHyperparameter(f"{name} must be a non-empty array")
    if len(value) > 30:
        raise InvalidHyperparameter(f"{name} may contain at most 30 values")
    result: list[float] = []
    for item in value:
        number = _finite_number(item, name)
        if number < 0:
            raise InvalidHyperparameter(f"{name} values must be non-negative")
        result.append(number)
    return sorted(set(result))


def validate_config(config: dict[str, Any]) -> dict[str, Any]:
    """Validate and canonicalize a full config without mutating the input."""

    if not isinstance(config, dict):
        raise InvalidHyperparameter("Configuration must be an object")
    unknown = set(config) - _ALLOWED_TOP_LEVEL
    if unknown:
        raise InvalidHyperparameter(
            "Unknown configuration field", details={"fields": sorted(unknown)}
        )
    cfg = deep_merge(DEFAULT_CONFIG, config)
    if cfg.get("schema_version") != 1:
        raise InvalidHyperparameter("schema_version must be 1")
    dataset_id = cfg.get("dataset_id")
    if not isinstance(dataset_id, str) or not dataset_id.strip():
        raise InvalidHyperparameter("dataset_id must be a non-empty string")

    prep = cfg["preprocessing"]
    if prep["numeric_normalization"] not in {"standard", "minmax", "none"}:
        raise InvalidHyperparameter(
            "numeric_normalization must be standard, minmax or none"
        )
    if prep["numeric_imputation"] not in {"mean", "median", "zero"}:
        raise InvalidHyperparameter("numeric_imputation must be mean, median or zero")
    if prep["categorical_encoding"] not in {"onehot", "target"}:
        raise InvalidHyperparameter("categorical_encoding must be onehot or target")
    if prep["categorical_imputation"] not in {"mode", "missing_token"}:
        raise InvalidHyperparameter(
            "categorical_imputation must be mode or missing_token"
        )
    smoothing = _finite_number(
        prep["target_encoding_smoothing"], "target_encoding_smoothing"
    )
    if smoothing < 0:
        raise InvalidHyperparameter("target_encoding_smoothing must be non-negative")
    prep["target_encoding_smoothing"] = smoothing

    cv = cfg["cv"]
    folds = _integer(cv["folds"], "cv.folds")
    if folds < 2:
        raise InvalidCV(
            "Cross-validation requires at least two folds",
            details={"fold": None, "reason": "folds must be >= 2", "folds": folds},
        )
    if folds > 20:
        raise InvalidHyperparameter("cv.folds must be <= 20")
    seed = _integer(cv["seed"], "cv.seed")
    if seed < 0:
        raise InvalidHyperparameter("cv.seed must be non-negative")
    if not isinstance(cv["stratify"], bool):
        raise InvalidHyperparameter("cv.stratify must be a boolean")
    if cv["metric"] not in {"log_loss", "accuracy"}:
        raise InvalidHyperparameter("cv.metric must be log_loss or accuracy")
    cv["folds"] = folds
    cv["seed"] = seed

    reg = cfg["regularization"]
    mode = reg["mode"]
    if mode not in {"l1", "l2", "elasticnet"}:
        raise InvalidHyperparameter("regularization.mode must be l1, l2 or elasticnet")
    strengths = _number_list(reg["strengths"], "regularization.strengths")
    raw_ratios = reg["l1_ratios"]
    if not isinstance(raw_ratios, list) or not raw_ratios:
        raise InvalidHyperparameter("regularization.l1_ratios must be a non-empty array")
    ratios: list[float] = []
    for ratio in raw_ratios:
        value = _finite_number(ratio, "regularization.l1_ratios")
        if value < 0.0 or value > 1.0:
            raise InvalidHyperparameter(
                "regularization.l1_ratios values must be between 0 and 1",
                details={"value": value, "allowed": [0.0, 1.0]},
            )
        ratios.append(value)
    ratios = sorted(set(ratios))
    if mode == "l1":
        ratios = [1.0]
    elif mode == "l2":
        ratios = [0.0]
    reg["mode"] = mode
    reg["strengths"] = strengths
    reg["l1_ratios"] = ratios

    solver = cfg["solver"]
    if solver["name"] not in {"coordinate_descent", "proximal_gradient"}:
        raise InvalidHyperparameter(
            "solver.name must be coordinate_descent or proximal_gradient"
        )
    tolerance = _finite_number(solver["tolerance"], "solver.tolerance")
    if tolerance <= 0:
        raise InvalidHyperparameter("solver.tolerance must be > 0")
    max_iter = _integer(solver["max_iter"], "solver.max_iter")
    if max_iter < 1 or max_iter > 10000:
        raise InvalidHyperparameter("solver.max_iter must be between 1 and 10000")
    objective_limit = _finite_number(solver["objective_limit"], "solver.objective_limit")
    if objective_limit <= 0:
        raise InvalidHyperparameter("solver.objective_limit must be > 0")
    solver["tolerance"] = tolerance
    solver["max_iter"] = max_iter
    solver["objective_limit"] = objective_limit

    selection = cfg["selection"]
    if selection["tie_break"] != "smaller_strength_then_larger_l1_ratio":
        raise InvalidHyperparameter("Unsupported selection.tie_break")
    return cfg


def task_candidates(config: dict[str, Any]) -> list[dict[str, float]]:
    reg = config["regularization"]
    result = []
    for strength in reg["strengths"]:
        for ratio in reg["l1_ratios"]:
            result.append({"strength": float(strength), "l1_ratio": float(ratio)})
    return result


def config_version(config: dict[str, Any]) -> str:
    return digest({"kind": "config-version", "config": config})
