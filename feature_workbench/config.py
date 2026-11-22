from __future__ import annotations

import copy
import math
from pathlib import Path
from typing import Any

import yaml

from .errors import WorkbenchError
from .util import atomic_write_text, digest_object, read_json


CONFIG_SCHEMA_VERSION = 1

DEFAULT_CONFIG: dict[str, Any] = {
    "seed": 20260317,
    "encoding": {
        "strategy": "one_hot",
        "missing": {
            "numeric": "median",
            "categorical": "constant",
            "constant": "__MISSING__",
        },
        "target": {
            "folds": 5,
            "smoothing": 10.0,
            "fold_seed": 20260317,
        },
        "hash": {
            "n_features": 8,
            "algorithm": "blake2b",
        },
    },
    "selection": {
        "method": "variance",
        "threshold": 0.01,
    },
    "budget": {
        "global_feature_evaluations": 1_000_000,
        "per_worker_evaluations": 250_000,
        "max_workers": 4,
        "query_latency_ms": 250,
    },
    "checkpoint": {
        "atomic_writes": True,
    },
}


def _merge_dict(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _merge_dict(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def load_yaml(path: Path) -> dict[str, Any]:
    path = Path(path)
    if not path.exists():
        raise WorkbenchError("E_NOT_FOUND", "Configuration file not found", {"path": str(path)})
    try:
        with path.open("r", encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle)
    except (OSError, yaml.YAMLError) as exc:
        raise WorkbenchError(
            "E_CONFIG_INVALID",
            "Configuration YAML could not be parsed",
            {"path": str(path), "reason": str(exc)},
        ) from exc
    if not isinstance(loaded, dict):
        raise WorkbenchError(
            "E_CONFIG_INVALID",
            "Configuration root must be a mapping",
            {"path": str(path)},
        )
    return loaded


def normalize_config(raw: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise WorkbenchError("E_CONFIG_INVALID", "Configuration root must be a mapping")
    config = _merge_dict(DEFAULT_CONFIG, raw)

    seed = config.get("seed")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise WorkbenchError("E_CONFIG_INVALID", "seed must be an integer", {"value": seed})

    encoding = config.get("encoding")
    if not isinstance(encoding, dict):
        raise WorkbenchError("E_CONFIG_INVALID", "encoding must be a mapping")
    strategy = encoding.get("strategy")
    if strategy not in {"one_hot", "target", "hash"}:
        raise WorkbenchError(
            "E_CONFIG_INVALID",
            "encoding.strategy must be one of one_hot, target, hash",
            {"value": strategy},
        )
    missing = encoding.get("missing")
    if not isinstance(missing, dict):
        raise WorkbenchError("E_CONFIG_INVALID", "encoding.missing must be a mapping")
    numeric_fill = missing.get("numeric")
    if numeric_fill not in {"mean", "median", "mode", "zero", "constant"}:
        raise WorkbenchError(
            "E_CONFIG_INVALID",
            "encoding.missing.numeric must be mean, median, mode, zero, or constant",
            {"value": numeric_fill},
        )
    categorical_fill = missing.get("categorical")
    if categorical_fill not in {"mode", "constant"}:
        raise WorkbenchError(
            "E_CONFIG_INVALID",
            "encoding.missing.categorical must be mode or constant",
            {"value": categorical_fill},
        )
    constant = str(missing.get("constant", "__MISSING__"))
    if not constant:
        raise WorkbenchError("E_CONFIG_INVALID", "encoding.missing.constant must not be empty")
    missing["constant"] = constant

    target = encoding.get("target")
    if not isinstance(target, dict):
        raise WorkbenchError("E_CONFIG_INVALID", "encoding.target must be a mapping")
    folds = target.get("folds")
    if isinstance(folds, bool) or not isinstance(folds, int) or folds < 2:
        raise WorkbenchError(
            "E_CONFIG_INVALID",
            "encoding.target.folds must be an integer >= 2",
            {"value": folds},
        )
    smoothing = target.get("smoothing")
    if isinstance(smoothing, bool) or not isinstance(smoothing, (int, float)):
        raise WorkbenchError("E_CONFIG_INVALID", "encoding.target.smoothing must be numeric")
    smoothing = float(smoothing)
    if not math.isfinite(smoothing) or smoothing < 0:
        raise WorkbenchError(
            "E_CONFIG_INVALID",
            "encoding.target.smoothing must be finite and >= 0",
            {"value": smoothing},
        )
    fold_seed = target.get("fold_seed", seed)
    if isinstance(fold_seed, bool) or not isinstance(fold_seed, int):
        raise WorkbenchError(
            "E_CONFIG_INVALID",
            "encoding.target.fold_seed must be an integer",
            {"value": fold_seed},
        )
    target["folds"] = folds
    target["smoothing"] = smoothing
    target["fold_seed"] = fold_seed

    hash_config = encoding.get("hash")
    if not isinstance(hash_config, dict):
        raise WorkbenchError("E_CONFIG_INVALID", "encoding.hash must be a mapping")
    n_features = hash_config.get("n_features")
    if isinstance(n_features, bool) or not isinstance(n_features, int) or n_features < 1:
        raise WorkbenchError(
            "E_CONFIG_INVALID",
            "encoding.hash.n_features must be an integer >= 1",
            {"value": n_features},
        )
    algorithm = hash_config.get("algorithm", "blake2b")
    if algorithm != "blake2b":
        raise WorkbenchError(
            "E_CONFIG_INVALID",
            "encoding.hash.algorithm currently supports blake2b only",
            {"value": algorithm},
        )
    hash_config["algorithm"] = algorithm

    selection = config.get("selection")
    if not isinstance(selection, dict):
        raise WorkbenchError("E_CONFIG_INVALID", "selection must be a mapping")
    method = selection.get("method")
    if method not in {"variance", "mutual_info", "rfe"}:
        raise WorkbenchError(
            "E_CONFIG_INVALID",
            "selection.method must be variance, mutual_info, or rfe",
            {"value": method},
        )
    selection["threshold"] = validate_threshold(method, selection.get("threshold"))

    budget = config.get("budget")
    if not isinstance(budget, dict):
        raise WorkbenchError("E_CONFIG_INVALID", "budget must be a mapping")
    for key in (
        "global_feature_evaluations",
        "per_worker_evaluations",
        "max_workers",
    ):
        value = budget.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise WorkbenchError(
                "E_CONFIG_INVALID",
                f"budget.{key} must be a positive integer",
                {"value": value},
            )
    latency = budget.get("query_latency_ms")
    if isinstance(latency, bool) or not isinstance(latency, (int, float)) or float(latency) <= 0:
        raise WorkbenchError(
            "E_CONFIG_INVALID",
            "budget.query_latency_ms must be a positive number",
            {"value": latency},
        )
    budget["query_latency_ms"] = float(latency)
    checkpoint = config.get("checkpoint")
    if not isinstance(checkpoint, dict):
        raise WorkbenchError("E_CONFIG_INVALID", "checkpoint must be a mapping")
    checkpoint.setdefault("atomic_writes", True)
    if checkpoint["atomic_writes"] is not True:
        raise WorkbenchError("E_CONFIG_INVALID", "checkpoint.atomic_writes must be true")

    return config


def validate_threshold(method: str, threshold: Any) -> float:
    if isinstance(threshold, bool) or not isinstance(threshold, (int, float)):
        raise WorkbenchError(
            "E_INVALID_THRESHOLD",
            "selection.threshold must be numeric",
            {"method": method, "threshold": threshold},
        )
    threshold = float(threshold)
    if not math.isfinite(threshold):
        raise WorkbenchError(
            "E_INVALID_THRESHOLD",
            "selection.threshold must be finite",
            {"method": method, "threshold": threshold},
        )
    if method in {"variance", "mutual_info"} and threshold < 0:
        raise WorkbenchError(
            "E_INVALID_THRESHOLD",
            "selection.threshold must be >= 0 for variance and mutual_info",
            {"method": method, "threshold": threshold},
        )
    if method == "rfe" and not (0 < threshold <= 1):
        raise WorkbenchError(
            "E_INVALID_THRESHOLD",
            "selection.threshold must be in (0, 1] for rfe",
            {"method": method, "threshold": threshold},
        )
    return threshold


def config_digest(config: dict[str, Any]) -> str:
    return digest_object(config)


def load_config(path: Path) -> dict[str, Any]:
    return normalize_config(load_yaml(path))


def dump_config(path: Path, config: dict[str, Any]) -> None:
    text = yaml.safe_dump(config, sort_keys=False, allow_unicode=True)
    atomic_write_text(path, text)


def parse_scalar(text: str) -> Any:
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError:
        return text


def set_config_value(config: dict[str, Any], dotted_key: str, raw_value: Any) -> dict[str, Any]:
    if not dotted_key:
        raise WorkbenchError("E_CONFIG_INVALID", "Configuration key must not be empty")
    updated = copy.deepcopy(config)
    cursor = updated
    parts = dotted_key.split(".")
    for part in parts[:-1]:
        next_value = cursor.get(part)
        if not isinstance(next_value, dict):
            raise WorkbenchError(
                "E_CONFIG_INVALID",
                "Cannot update a non-mapping configuration section",
                {"key": dotted_key, "section": part},
            )
        cursor = next_value
    cursor[parts[-1]] = raw_value
    return normalize_config(updated)


def update_config_file(path: Path, changes: dict[str, Any]) -> dict[str, Any]:
    current = load_yaml(path)
    updated = copy.deepcopy(current)
    for dotted_key, value in changes.items():
        cursor = updated
        parts = dotted_key.split(".")
        for part in parts[:-1]:
            section = cursor.get(part)
            if not isinstance(section, dict):
                section = {}
                cursor[part] = section
            cursor = section
        cursor[parts[-1]] = value
    normalized = normalize_config(updated)
    dump_config(path, normalized)
    return normalized


def load_versions_baseline(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return read_json(path)
    except (OSError, ValueError) as exc:
        raise WorkbenchError(
            "E_STATE_INVALID",
            "Version baseline is unreadable",
            {"path": str(path), "reason": str(exc)},
        ) from exc

