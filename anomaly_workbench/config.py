"""Configuration defaults, validation and version fingerprints."""

from __future__ import annotations

import copy
import math
from typing import Any, Iterable

from .errors import WorkbenchError
from .util import digest_object

CONFIG_SCHEMA_VERSION = 1
STAGES = ("preprocess", "fit", "score", "diagnostics")


DEFAULT_CONFIG: dict[str, Any] = {
    "schema_version": CONFIG_SCHEMA_VERSION,
    "preprocessing": {
        "missing_strategy": "median",
        "standardization": "robust",
        "robust_epsilon": 1e-9,
        "heavy_tail_abs_skew": 2.0,
        "heavy_tail_kurtosis": 7.0,
        "redundancy_threshold": 0.98,
        "clip_standardized": 12.0,
    },
    "detectors": {
        "covariance": {
            "shrinkage": 0.05,
            "winsor_limit": 5.0,
            "epsilon": 1e-12,
        },
        "forest": {
            "n_estimators": 48,
            "sample_size": 256,
            "max_depth": 12,
            "seed": 20261122,
        },
    },
    "scoring": {
        "weights": {"covariance": 0.5, "forest": 0.5},
        "normalization": {
            "covariance_transform": "log1p",
            "forest_transform": "logit",
            "center_quantile": 0.5,
            "scale_quantiles": [0.25, 0.75],
            "scale_floor": 1e-9,
            "clip": 20.0,
        },
        "threshold_quantile": 0.97,
        "comparison": "greater_than",
    },
    "diagnostics": {
        "quantiles": [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99],
        "sensitivity_quantiles": [0.8, 0.85, 0.9, 0.95, 0.97, 0.99],
        "confidence_z": 1.96,
    },
}


def _expect_mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        _invalid(f"{name} must be an object")
    return value


def _invalid(message: str, details: dict[str, Any] | None = None) -> None:
    raise WorkbenchError("E_INVALID_CONFIG", message, stage="config", details=details or {})


def _number(
    value: Any,
    name: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
    allow_minimum: bool = True,
    allow_maximum: bool = True,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _invalid(f"{name} must be numeric", {"value": value})
    converted = float(value)
    if not math.isfinite(converted):
        _invalid(f"{name} must be finite", {"value": value})
    if minimum is not None:
        if converted < minimum or (not allow_minimum and converted == minimum):
            _invalid(f"{name} is outside the permitted range", {"value": value, "minimum": minimum})
    if maximum is not None:
        if converted > maximum or (not allow_maximum and converted == maximum):
            _invalid(f"{name} is outside the permitted range", {"value": value, "maximum": maximum})
    return converted


def _integer(
    value: Any,
    name: str,
    *,
    minimum: int,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        _invalid(f"{name} must be an integer", {"value": value})
    if value < minimum or (maximum is not None and value > maximum):
        _invalid(f"{name} is outside the permitted range", {"value": value})
    return value


def merge_config(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = merge_config(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def parse_set_overrides(values: Iterable[str]) -> dict[str, Any]:
    patch: dict[str, Any] = {}
    for assignment in values:
        if "=" not in assignment:
            _invalid("--set expects dotted.path=value", {"assignment": assignment})
        dotted, raw = assignment.split("=", 1)
        try:
            # JSON covers booleans, numbers, strings, lists and objects.
            value = __import__("json").loads(raw)
        except Exception:
            value = raw
        cursor = patch
        parts = dotted.split(".")
        for part in parts[:-1]:
            cursor = cursor.setdefault(part, {})
            if not isinstance(cursor, dict):
                _invalid("configuration path collides with a scalar", {"path": dotted})
        cursor[parts[-1]] = value
    return patch


def validate_config(config: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize a complete configuration object.

    Negative detector weights are intentionally accepted here: they are a
    numeric-domain failure and must be rejected by the scoring stage with
    ``E_NUMERIC`` rather than silently turning into a decision.
    """

    root = _expect_mapping(config, "config")
    if root.get("schema_version", CONFIG_SCHEMA_VERSION) != CONFIG_SCHEMA_VERSION:
        _invalid("unsupported schema_version", {"schema_version": root.get("schema_version")})
    result = copy.deepcopy(DEFAULT_CONFIG)
    result = merge_config(result, root)

    preprocessing = _expect_mapping(result["preprocessing"], "preprocessing")
    if preprocessing["missing_strategy"] not in {"median", "mean", "zero"}:
        _invalid("unsupported missing_strategy", {"value": preprocessing["missing_strategy"]})
    if preprocessing["standardization"] not in {"robust", "zscore"}:
        _invalid("unsupported standardization", {"value": preprocessing["standardization"]})
    preprocessing["robust_epsilon"] = _number(
        preprocessing["robust_epsilon"], "preprocessing.robust_epsilon", minimum=0.0, allow_minimum=False
    )
    preprocessing["heavy_tail_abs_skew"] = _number(
        preprocessing["heavy_tail_abs_skew"], "preprocessing.heavy_tail_abs_skew", minimum=0.0
    )
    preprocessing["heavy_tail_kurtosis"] = _number(
        preprocessing["heavy_tail_kurtosis"], "preprocessing.heavy_tail_kurtosis", minimum=0.0
    )
    preprocessing["redundancy_threshold"] = _number(
        preprocessing["redundancy_threshold"],
        "preprocessing.redundancy_threshold",
        minimum=0.0,
        maximum=1.0,
        allow_minimum=False,
        allow_maximum=False,
    )
    preprocessing["clip_standardized"] = _number(
        preprocessing["clip_standardized"],
        "preprocessing.clip_standardized",
        minimum=0.0,
        allow_minimum=False,
    )

    detectors = _expect_mapping(result["detectors"], "detectors")
    covariance = _expect_mapping(detectors["covariance"], "detectors.covariance")
    covariance["shrinkage"] = _number(
        covariance["shrinkage"], "detectors.covariance.shrinkage", minimum=0.0, maximum=1.0
    )
    covariance["winsor_limit"] = _number(
        covariance["winsor_limit"], "detectors.covariance.winsor_limit", minimum=0.0, allow_minimum=False
    )
    covariance["epsilon"] = _number(
        covariance["epsilon"], "detectors.covariance.epsilon", minimum=0.0, allow_minimum=False
    )
    forest = _expect_mapping(detectors["forest"], "detectors.forest")
    forest["n_estimators"] = _integer(
        forest["n_estimators"], "detectors.forest.n_estimators", minimum=1, maximum=512
    )
    forest["sample_size"] = _integer(
        forest["sample_size"], "detectors.forest.sample_size", minimum=2, maximum=100000
    )
    forest["max_depth"] = _integer(
        forest["max_depth"], "detectors.forest.max_depth", minimum=1, maximum=64
    )
    forest["seed"] = _integer(forest["seed"], "detectors.forest.seed", minimum=0, maximum=2**63 - 1)

    scoring = _expect_mapping(result["scoring"], "scoring")
    weights = _expect_mapping(scoring["weights"], "scoring.weights")
    for detector in ("covariance", "forest"):
        if detector not in weights:
            _invalid("missing detector weight", {"detector": detector})
        weights[detector] = _number(weights[detector], f"scoring.weights.{detector}")
    normalization = _expect_mapping(scoring["normalization"], "scoring.normalization")
    if normalization["covariance_transform"] not in {"log1p", "identity"}:
        _invalid("unsupported covariance_transform", {"value": normalization["covariance_transform"]})
    if normalization["forest_transform"] not in {"logit", "identity"}:
        _invalid("unsupported forest_transform", {"value": normalization["forest_transform"]})
    normalization["center_quantile"] = _number(
        normalization["center_quantile"],
        "scoring.normalization.center_quantile",
        minimum=0.0,
        maximum=1.0,
    )
    scale_quantiles = normalization["scale_quantiles"]
    if not isinstance(scale_quantiles, list) or len(scale_quantiles) != 2:
        _invalid("scale_quantiles must contain two values", {"value": scale_quantiles})
    normalization["scale_quantiles"] = [
        _number(value, "scoring.normalization.scale_quantiles", minimum=0.0, maximum=1.0)
        for value in scale_quantiles
    ]
    if normalization["scale_quantiles"][0] > normalization["scale_quantiles"][1]:
        _invalid("scale_quantiles must be sorted")
    normalization["scale_floor"] = _number(
        normalization["scale_floor"],
        "scoring.normalization.scale_floor",
        minimum=0.0,
        allow_minimum=False,
    )
    normalization["clip"] = _number(
        normalization["clip"], "scoring.normalization.clip", minimum=0.0, allow_minimum=False
    )
    scoring["threshold_quantile"] = _number(
        scoring["threshold_quantile"],
        "scoring.threshold_quantile",
        minimum=0.0,
        maximum=1.0,
        allow_minimum=False,
        allow_maximum=False,
    )
    if scoring["comparison"] not in {"greater_than", "greater_equal"}:
        _invalid("unsupported threshold comparison", {"value": scoring["comparison"]})

    diagnostics = _expect_mapping(result["diagnostics"], "diagnostics")
    for field in ("quantiles", "sensitivity_quantiles"):
        values = diagnostics[field]
        if not isinstance(values, list) or not values:
            _invalid(f"diagnostics.{field} must be a non-empty list")
        normalized = [
            _number(value, f"diagnostics.{field}", minimum=0.0, maximum=1.0) for value in values
        ]
        if normalized != sorted(normalized):
            _invalid(f"diagnostics.{field} must be sorted")
        diagnostics[field] = normalized
    diagnostics["confidence_z"] = _number(
        diagnostics["confidence_z"], "diagnostics.confidence_z", minimum=0.0, allow_minimum=False
    )
    return result


def default_config() -> dict[str, Any]:
    return validate_config(copy.deepcopy(DEFAULT_CONFIG))


def config_version(config: dict[str, Any]) -> str:
    return digest_object({"config": validate_config(config), "schema": CONFIG_SCHEMA_VERSION})


def stage_config_fingerprints(config: dict[str, Any]) -> dict[str, str]:
    validated = validate_config(config)
    return {
        "preprocess": digest_object(
            {"schema": CONFIG_SCHEMA_VERSION, "preprocessing": validated["preprocessing"]}
        ),
        "fit": digest_object(
            {
                "schema": CONFIG_SCHEMA_VERSION,
                "preprocessing": validated["preprocessing"],
                "detectors": validated["detectors"],
            }
        ),
        "score": digest_object(
            {
                "schema": CONFIG_SCHEMA_VERSION,
                "preprocessing": validated["preprocessing"],
                "detectors": validated["detectors"],
                "scoring": validated["scoring"],
            }
        ),
        "diagnostics": digest_object(
            {
                "schema": CONFIG_SCHEMA_VERSION,
                "preprocessing": validated["preprocessing"],
                "detectors": validated["detectors"],
                "scoring": validated["scoring"],
                "diagnostics": validated["diagnostics"],
            }
        ),
    }


def earliest_stage_for_config_change(old: dict[str, Any], new: dict[str, Any]) -> str | None:
    old_fps = stage_config_fingerprints(old)
    new_fps = stage_config_fingerprints(new)
    for stage in STAGES:
        if old_fps[stage] != new_fps[stage]:
            return stage
    return None


def changed_leaf_paths(old: Any, new: Any, prefix: str = "") -> list[str]:
    if isinstance(old, dict) and isinstance(new, dict):
        paths: list[str] = []
        for key in sorted(set(old) | set(new)):
            child = f"{prefix}.{key}" if prefix else key
            if key not in old or key not in new:
                paths.append(child)
            else:
                paths.extend(changed_leaf_paths(old[key], new[key], child))
        return paths
    if old != new:
        return [prefix]
    return []
