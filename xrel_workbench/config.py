from __future__ import annotations

import copy
import math
from pathlib import Path
from typing import Any

import yaml

from .errors import BusinessError, E_CONFIG
from .util import atomic_write_text, digest_value


DEFAULT_CONFIG: dict[str, Any] = {
    "version": 3,
    "normalization": {
        "unicode_form": "NFKC",
        "collapse_whitespace": True,
        "cjk_ngram_sizes": [1, 2],
    },
    "mapping": {
        "lexical_weight": 0.55,
        "bridge_weight": 0.45,
        "char_ngram_sizes": [2, 3],
        "bm25": {"k1": 1.2, "b": 0.75},
        "smooth_strength": 0.35,
        "quantization_bits": 12,
    },
    "scoring": {
        "score_clip": 6.0,
        "epsilon": 1e-12,
    },
    "grading": {
        "labels": ["不相关", "弱相关", "相关", "强相关"],
        "thresholds": [0.45, 1.0, 1.75],
        "reference_distribution": [0.25, 0.25, 0.25, 0.25],
        "max_distribution_deviation": 0.2,
        "max_human_offset": 1,
        "max_boundary_deviation": 0.6,
        "boundary_warning_margin": 0.02,
    },
    "diagnostics": {
        "reference_precision": 50,
        "crosscheck_tolerance": 1e-12,
        "max_significant_digits": 12,
    },
}


def default_config() -> dict[str, Any]:
    return copy.deepcopy(DEFAULT_CONFIG)


def load_config(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = yaml.safe_load(handle)
    except (OSError, yaml.YAMLError) as exc:
        raise BusinessError(E_CONFIG, f"cannot read scoring config: {exc}") from exc
    if not isinstance(value, dict):
        raise BusinessError(E_CONFIG, "scoring config root must be a mapping")
    validate_config(value)
    return value


def validate_config(config: dict[str, Any]) -> None:
    try:
        if config["normalization"]["unicode_form"] != "NFKC":
            raise ValueError("normalization.unicode_form must be NFKC")
        if not isinstance(config["normalization"]["collapse_whitespace"], bool):
            raise ValueError("normalization.collapse_whitespace must be boolean")
        sizes = config["normalization"]["cjk_ngram_sizes"]
        if sizes != [1, 2]:
            raise ValueError("normalization.cjk_ngram_sizes must be [1, 2]")
        char_sizes = config["mapping"]["char_ngram_sizes"]
        if not char_sizes or any(int(size) <= 0 for size in char_sizes):
            raise ValueError("mapping.char_ngram_sizes must contain positive integers")
        bits = int(config["mapping"]["quantization_bits"])
        if bits < 2 or bits > 30:
            raise ValueError("mapping.quantization_bits must be between 2 and 30")
        k1 = float(config["mapping"]["bm25"]["k1"])
        b = float(config["mapping"]["bm25"]["b"])
        if not math.isfinite(k1) or not math.isfinite(b):
            raise ValueError("BM25 parameters must be finite")
        if k1 < 0 or not 0 <= b <= 1:
            raise ValueError("BM25 requires k1 >= 0 and 0 <= b <= 1")
        smooth = float(config["mapping"]["smooth_strength"])
        if not math.isfinite(smooth) or smooth < 0:
            raise ValueError("smooth_strength must be non-negative")
        clip = float(config["scoring"]["score_clip"])
        if not math.isfinite(clip) or clip <= 0:
            raise ValueError("score_clip must be positive")
        thresholds = [float(value) for value in config["grading"]["thresholds"]]
        if len(thresholds) != 3 or any(not math.isfinite(value) for value in thresholds) or thresholds != sorted(thresholds):
            raise ValueError("grading.thresholds must contain three increasing values")
        distribution = [float(value) for value in config["grading"]["reference_distribution"]]
        if len(distribution) != 4 or any(not math.isfinite(value) for value in distribution) or abs(sum(distribution) - 1.0) > 1e-9:
            raise ValueError("grading.reference_distribution must contain four values summing to 1")
        if any(value < 0 for value in distribution):
            raise ValueError("grading.reference_distribution cannot be negative")
        precision = int(config["diagnostics"]["reference_precision"])
        if precision < 20:
            raise ValueError("diagnostics.reference_precision must be at least 20")
    except (KeyError, TypeError, ValueError) as exc:
        raise BusinessError(E_CONFIG, f"invalid scoring config: {exc}") from exc


def config_fingerprint(config: dict[str, Any]) -> str:
    return digest_value(config)


def config_version(config: dict[str, Any]) -> str:
    return f"yaml-v{config.get('version', 0)}-{config_fingerprint(config)[:12]}"


def write_config(path: Path, config: dict[str, Any]) -> None:
    validate_config(config)
    text = yaml.safe_dump(
        config,
        allow_unicode=True,
        sort_keys=True,
        default_flow_style=False,
    )
    atomic_write_text(path, text)


def set_config_value(config: dict[str, Any], dotted_path: str, value: Any) -> dict[str, Any]:
    updated = copy.deepcopy(config)
    parts = dotted_path.split(".")
    if not parts or any(not part for part in parts):
        raise BusinessError(E_CONFIG, f"invalid config path: {dotted_path}")
    cursor: dict[str, Any] = updated
    for part in parts[:-1]:
        child = cursor.get(part)
        if not isinstance(child, dict):
            raise BusinessError(E_CONFIG, f"config path does not exist: {dotted_path}")
        cursor = child
    if parts[-1] not in cursor:
        raise BusinessError(E_CONFIG, f"config path does not exist: {dotted_path}")
    cursor[parts[-1]] = value
    validate_config(updated)
    return updated


def stage_config_slices(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "normalization": copy.deepcopy(config["normalization"]),
        "mapping": copy.deepcopy(config["mapping"]),
        "scoring": copy.deepcopy(config["scoring"]),
        "grading": copy.deepcopy(config["grading"]),
        "diagnostics": copy.deepcopy(config["diagnostics"]),
    }
