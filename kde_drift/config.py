"""Configuration normalization and validation."""

from __future__ import annotations

import copy
import math
from typing import Any, Mapping

from .canonical import digest_object
from .errors import WorkbenchError


DEFAULT_CONFIG: dict[str, Any] = {
    "schema_version": 1,
    "kernel": "gaussian",
    "bandwidth": {"rule": "silverman", "value": None},
    "imputation": {"method": "mean"},
    "grid": {"points": 128},
    "chunk_size": 256,
    "summation": "kahan",
    "mmd": {
        "kernel": "gaussian",
        "bandwidth_scale": 1.0,
        "chunk_size": 256,
        "max_eval_samples": 2048,
        "bootstrap_sample_size": 128,
    },
    "kl": {"epsilon": 1e-12},
    "drift": {
        "threshold": 0.08,
        "alpha": 0.05,
        "bootstrap_resamples": 64,
        "seed": 20261121,
    },
}


def _config_error(path: str, message: str, value: Any = None) -> WorkbenchError:
    details: dict[str, Any] = {"path": path}
    if value is not None:
        details["value"] = value
    return WorkbenchError("E_CONFIG", message, location=path, details=details)


def _as_mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _config_error(path, "expected an object", type(value).__name__)
    return value


def _merge_known(
    defaults: Mapping[str, Any], supplied: Mapping[str, Any], path: str
) -> dict[str, Any]:
    result = copy.deepcopy(dict(defaults))
    for key, value in supplied.items():
        if key not in defaults:
            raise _config_error(f"{path}.{key}" if path else key, "unknown configuration key")
        default_value = defaults[key]
        if isinstance(default_value, Mapping):
            result[key] = _merge_known(
                default_value,
                _as_mapping(value, f"{path}.{key}" if path else key),
                f"{path}.{key}" if path else key,
            )
        else:
            result[key] = copy.deepcopy(value)
    return result


def _number(value: Any, path: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _config_error(path, "expected a number", value)
    result = float(value)
    if minimum is not None and math.isfinite(result) and result < minimum:
        raise _config_error(path, f"must be >= {minimum}", value)
    return result


def _integer(value: Any, path: str, *, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _config_error(path, "expected an integer", value)
    if value < minimum:
        raise _config_error(path, f"must be >= {minimum}", value)
    return value


def normalize_config(supplied: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Merge a partial config with defaults and validate its shape.

    Non-finite numeric kernel parameters are deliberately retained so the
    bandwidth/density stage can report ``E_NUMERIC`` with column and location.
    """

    user = {} if supplied is None else dict(_as_mapping(supplied, "config"))
    config = _merge_known(DEFAULT_CONFIG, user, "")

    if config["schema_version"] != 1:
        raise _config_error("schema_version", "unsupported schema version", config["schema_version"])

    kernel = str(config["kernel"]).lower()
    if kernel not in {"gaussian", "epanechnikov"}:
        raise _config_error("kernel", "kernel must be gaussian or epanechnikov", config["kernel"])
    config["kernel"] = kernel

    bandwidth = _as_mapping(config["bandwidth"], "bandwidth")
    rule = str(bandwidth["rule"]).lower()
    if rule not in {"silverman", "scott", "fixed"}:
        raise _config_error(
            "bandwidth.rule", "rule must be silverman, scott, or fixed", bandwidth["rule"]
        )
    bandwidth["rule"] = rule
    if rule == "fixed":
        if bandwidth["value"] is None:
            raise _config_error("bandwidth.value", "fixed bandwidth requires a value")
        bandwidth["value"] = _number(bandwidth["value"], "bandwidth.value")
    else:
        bandwidth["value"] = None

    imputation = _as_mapping(config["imputation"], "imputation")
    method = str(imputation["method"]).lower()
    if method not in {"mean", "median", "zero"}:
        raise _config_error(
            "imputation.method", "method must be mean, median, or zero", imputation["method"]
        )
    imputation["method"] = method

    grid = _as_mapping(config["grid"], "grid")
    grid["points"] = _integer(grid["points"], "grid.points", minimum=8)
    if grid["points"] > 4096:
        raise _config_error("grid.points", "must be <= 4096", grid["points"])

    config["chunk_size"] = _integer(config["chunk_size"], "chunk_size", minimum=1)
    summation = str(config["summation"]).lower()
    if summation not in {"kahan", "pairwise"}:
        raise _config_error(
            "summation", "summation must be kahan or pairwise", config["summation"]
        )
    config["summation"] = summation

    mmd = _as_mapping(config["mmd"], "mmd")
    mmd_kernel = str(mmd["kernel"]).lower()
    if mmd_kernel not in {"gaussian", "epanechnikov"}:
        raise _config_error(
            "mmd.kernel", "kernel must be gaussian or epanechnikov", mmd["kernel"]
        )
    mmd["kernel"] = mmd_kernel
    mmd["bandwidth_scale"] = _number(mmd["bandwidth_scale"], "mmd.bandwidth_scale")
    if not math.isfinite(mmd["bandwidth_scale"]) or mmd["bandwidth_scale"] <= 0:
        raise _config_error("mmd.bandwidth_scale", "must be > 0", mmd["bandwidth_scale"])
    mmd["chunk_size"] = _integer(mmd["chunk_size"], "mmd.chunk_size", minimum=1)
    mmd["max_eval_samples"] = _integer(
        mmd["max_eval_samples"], "mmd.max_eval_samples", minimum=2
    )
    mmd["bootstrap_sample_size"] = _integer(
        mmd["bootstrap_sample_size"], "mmd.bootstrap_sample_size", minimum=2
    )

    kl = _as_mapping(config["kl"], "kl")
    kl["epsilon"] = _number(kl["epsilon"], "kl.epsilon")
    if not math.isfinite(kl["epsilon"]) or kl["epsilon"] <= 0:
        raise _config_error("kl.epsilon", "must be > 0", kl["epsilon"])

    drift = _as_mapping(config["drift"], "drift")
    drift["threshold"] = _number(drift["threshold"], "drift.threshold")
    drift["alpha"] = _number(drift["alpha"], "drift.alpha")
    for key in ("threshold", "alpha"):
        value = drift[key]
        if not math.isfinite(value) or not 0.0 <= value <= 1.0:
            raise _config_error(f"drift.{key}", "must be in [0, 1]", value)
    drift["bootstrap_resamples"] = _integer(
        drift["bootstrap_resamples"], "drift.bootstrap_resamples", minimum=2
    )
    drift["seed"] = _integer(drift["seed"], "drift.seed", minimum=0)

    return config


def config_fingerprint(config: Mapping[str, Any], dataset_fingerprint: str) -> str:
    return digest_object({"config": config, "dataset": dataset_fingerprint})


CONFIG_PATHS = {
    "kernel": ("kernel",),
    "bandwidth": ("bandwidth",),
    "imputation": ("imputation",),
    "grid": ("grid",),
    "chunk_size": ("chunk_size",),
    "summation": ("summation",),
    "mmd": ("mmd",),
    "kl": ("kl",),
    "drift": ("drift",),
}


def get_path(mapping: Mapping[str, Any], path: tuple[str, ...]) -> Any:
    value: Any = mapping
    for key in path:
        value = value[key]
    return value
