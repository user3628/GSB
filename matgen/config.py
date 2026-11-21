"""Configuration defaults, validation, and version fingerprints."""

from __future__ import annotations

import copy
import math
from pathlib import Path
from typing import Any

from .canonical import fingerprint, read_json
from .errors import E_INVALID_CONFIG, MatgenError


ALGORITHM_VERSION = "matgen-core-1.0.0"
CHECKPOINT_SCHEMA = "matgen.checkpoint.v1"
LEDGER_SCHEMA = "matgen.ledger.v1"
ANCHOR_SCHEMA = "matgen.anchor.v1"

DEFAULT_CONFIG: dict[str, Any] = {
    "schema": "matgen.config.v1",
    "algorithm_version": ALGORITHM_VERSION,
    "hash_seed": 243001,
    "sample_count": 12,
    "sample_seed": 99173,
    "octave_error_budget": 5e-13,
    "total_error_budget": 0.01,
    "max_threads": 4,
    "noise_base_scale": 2,
    "noise_base_min": 16,
    "noise_base_max": 64,
}


def _config_error(message: str, details: dict[str, Any] | None = None) -> MatgenError:
    return MatgenError(
        code=E_INVALID_CONFIG,
        message=message,
        details=details or {},
    )


def normalize_config(raw: dict[str, Any] | None = None) -> dict[str, Any]:
    """Merge and validate a user supplied configuration."""

    config = copy.deepcopy(DEFAULT_CONFIG)
    if raw:
        if not isinstance(raw, dict):
            raise _config_error("configuration must be a JSON object")
        config.update(raw)

    for key in ("hash_seed", "sample_count", "sample_seed", "max_threads", "noise_base_scale", "noise_base_min", "noise_base_max"):
        value = config.get(key)
        if isinstance(value, bool) or not isinstance(value, int):
            raise _config_error(f"{key} must be an integer", {"key": key, "value": repr(value)})

    if config["hash_seed"] < 1:
        raise _config_error("hash_seed must be positive")
    if not 1 <= config["sample_count"] <= 256:
        raise _config_error("sample_count must be in [1, 256]")
    if config["sample_seed"] < 0:
        raise _config_error("sample_seed must be non-negative")
    if not 1 <= config["max_threads"] <= 64:
        raise _config_error("max_threads must be in [1, 64]")
    if config["noise_base_scale"] < 1:
        raise _config_error("noise_base_scale must be at least 1")
    if not 2 <= config["noise_base_min"] <= config["noise_base_max"] <= 2048:
        raise _config_error("noise base dimensions must satisfy 2 <= min <= max <= 2048")

    for key in ("octave_error_budget", "total_error_budget"):
        value = config.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise _config_error(f"{key} must be numeric")
        numeric = float(value)
        if not math.isfinite(numeric) or numeric <= 0.0:
            raise _config_error(f"{key} must be finite and positive")
        config[key] = numeric

    if not isinstance(config.get("algorithm_version"), str) or not config["algorithm_version"]:
        raise _config_error("algorithm_version must be a non-empty string")
    if config["algorithm_version"] != ALGORITHM_VERSION:
        raise _config_error(
            "unsupported algorithm_version",
            {"supported": ALGORITHM_VERSION, "received": config["algorithm_version"]},
        )
    return config


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    if path is None:
        return normalize_config()
    payload = read_json(path)
    return normalize_config(payload)


_STAGE_CONFIG_KEYS: dict[str, tuple[str, ...]] = {
    "control": ("schema", "algorithm_version", "hash_seed"),
    "noise": (
        "schema",
        "algorithm_version",
        "noise_base_scale",
        "noise_base_min",
        "noise_base_max",
    ),
    "palette": ("schema", "algorithm_version"),
    "diagnostics": (
        "schema",
        "algorithm_version",
        "sample_count",
        "sample_seed",
        "octave_error_budget",
        "total_error_budget",
    ),
}


def stage_config(config: dict[str, Any], stage: str) -> dict[str, Any]:
    try:
        keys = _STAGE_CONFIG_KEYS[stage]
    except KeyError as exc:
        raise _config_error("unknown stage", {"stage": stage}) from exc
    return {key: config[key] for key in keys}


def stage_config_fingerprint(config: dict[str, Any], stage: str) -> str:
    return fingerprint(stage_config(config, stage))


def full_config_fingerprint(config: dict[str, Any]) -> str:
    """Fingerprint only settings that can affect persisted outputs."""

    output_relevant = {
        key: value
        for key, value in config.items()
        if key not in {"max_threads"}
    }
    return fingerprint(output_relevant)


def config_version_label(config: dict[str, Any]) -> str:
    return f"{config['algorithm_version']}+{full_config_fingerprint(config)[:16]}"


def stage_config_version_label(config: dict[str, Any], stage: str) -> str:
    return f"{config['algorithm_version']}+{stage_config_fingerprint(config, stage)[:16]}"

