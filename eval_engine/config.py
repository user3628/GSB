"""Configuration loading, defaults, validation, and semantic digests."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from .errors import EngineError
from .util import digest_json, read_json


DEFAULT_CONFIG: dict[str, Any] = {
    "version": "1",
    "scoring": {
        "exact_weight": 0.6,
        "overlap_weight": 0.4,
    },
    "exclusions": {
        "max_normalized_chars": 240,
        "min_normalized_chars": 0,
        "benchmark_languages": {},
        "allowed_question_types": [
            "reasoning",
            "knowledge",
            "instruction_following",
        ],
        "exclude_empty_output": False,
    },
    "profiling": {
        "min_samples_per_layer": 2,
        "min_samples_per_dimension": 4,
        "dimensions": {
            "reasoning": {
                "weight": 0.40,
                "layers": {"zh:reasoning": 0.55, "en:reasoning": 0.45},
            },
            "knowledge": {
                "weight": 0.35,
                "layers": {"zh:knowledge": 0.50, "en:knowledge": 0.50},
            },
            "instruction_following": {
                "weight": 0.25,
                "layers": {
                    "zh:instruction_following": 0.50,
                    "en:instruction_following": 0.50,
                },
            },
        },
    },
    "ranking": {
        "score_tolerance": 0.01,
        "min_valid_samples": 1,
        "min_benchmark_coverage": 1,
        "top_k": 1,
    },
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
            and key not in {"dimensions", "benchmark_languages", "layers"}
        ):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _require_number(value: Any, field: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EngineError(
            "E_INVALID_CONFIG",
            f"configuration field {field} must be a number",
            details={"field": field},
        )
    number = float(value)
    if minimum is not None and number < minimum:
        raise EngineError(
            "E_INVALID_CONFIG",
            f"configuration field {field} must be >= {minimum}",
            details={"field": field, "value": value},
        )
    return number


def _require_int(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise EngineError(
            "E_INVALID_CONFIG",
            f"configuration field {field} must be an integer",
            details={"field": field},
        )
    if value < minimum:
        raise EngineError(
            "E_INVALID_CONFIG",
            f"configuration field {field} must be >= {minimum}",
            details={"field": field, "value": value},
        )
    return value


def normalize_config(raw: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise EngineError("E_INVALID_CONFIG", "configuration root must be an object")
    config = _deep_merge(DEFAULT_CONFIG, raw)
    config["version"] = str(config.get("version", "1"))

    scoring = config["scoring"]
    exact_weight = _require_number(
        scoring.get("exact_weight"), "scoring.exact_weight", minimum=0
    )
    overlap_weight = _require_number(
        scoring.get("overlap_weight"), "scoring.overlap_weight", minimum=0
    )
    if exact_weight + overlap_weight <= 0:
        raise EngineError(
            "E_INVALID_CONFIG",
            "scoring weights must have a positive sum",
            details={"field": "scoring"},
        )
    scoring["exact_weight"] = exact_weight
    scoring["overlap_weight"] = overlap_weight

    exclusions = config["exclusions"]
    exclusions["max_normalized_chars"] = _require_int(
        exclusions.get("max_normalized_chars"),
        "exclusions.max_normalized_chars",
        minimum=1,
    )
    exclusions["min_normalized_chars"] = _require_int(
        exclusions.get("min_normalized_chars"),
        "exclusions.min_normalized_chars",
        minimum=0,
    )
    if (
        exclusions["min_normalized_chars"]
        > exclusions["max_normalized_chars"]
    ):
        raise EngineError(
            "E_INVALID_CONFIG",
            "exclusions.min_normalized_chars must not exceed max_normalized_chars",
        )
    benchmark_languages = exclusions.get("benchmark_languages", {})
    if not isinstance(benchmark_languages, dict):
        raise EngineError(
            "E_INVALID_CONFIG",
            "exclusions.benchmark_languages must be an object",
        )
    normalized_languages: dict[str, list[str]] = {}
    for benchmark_id, languages in benchmark_languages.items():
        if not isinstance(languages, list) or not all(
            isinstance(item, str) and item for item in languages
        ):
            raise EngineError(
                "E_INVALID_CONFIG",
                "benchmark language lists must contain non-empty strings",
                details={"benchmark_id": benchmark_id},
            )
        normalized_languages[str(benchmark_id)] = sorted(set(languages))
    exclusions["benchmark_languages"] = normalized_languages
    allowed_question_types = exclusions.get("allowed_question_types", [])
    if not isinstance(allowed_question_types, list) or not all(
        isinstance(item, str) and item for item in allowed_question_types
    ):
        raise EngineError(
            "E_INVALID_CONFIG",
            "exclusions.allowed_question_types must be a string list",
        )
    exclusions["allowed_question_types"] = sorted(set(allowed_question_types))
    exclusions["exclude_empty_output"] = bool(
        exclusions.get("exclude_empty_output", False)
    )

    profiling = config["profiling"]
    profiling["min_samples_per_layer"] = _require_int(
        profiling.get("min_samples_per_layer"),
        "profiling.min_samples_per_layer",
        minimum=1,
    )
    profiling["min_samples_per_dimension"] = _require_int(
        profiling.get("min_samples_per_dimension"),
        "profiling.min_samples_per_dimension",
        minimum=1,
    )
    dimensions = profiling.get("dimensions")
    if not isinstance(dimensions, dict) or not dimensions:
        raise EngineError(
            "E_INVALID_CONFIG",
            "profiling.dimensions must be a non-empty object",
        )
    normalized_dimensions: dict[str, Any] = {}
    for dimension_id, dimension in dimensions.items():
        if not isinstance(dimension, dict):
            raise EngineError(
                "E_INVALID_CONFIG",
                "each capability dimension must be an object",
                details={"dimension": dimension_id},
            )
        weight = _require_number(
            dimension.get("weight"),
            f"profiling.dimensions.{dimension_id}.weight",
            minimum=0,
        )
        layers = dimension.get("layers")
        if not isinstance(layers, dict) or not layers:
            raise EngineError(
                "E_INVALID_CONFIG",
                "each capability dimension needs at least one layer",
                details={"dimension": dimension_id},
            )
        normalized_layers: dict[str, float] = {}
        for layer_key, layer_weight in layers.items():
            if not isinstance(layer_key, str) or layer_key.count(":") != 1:
                raise EngineError(
                    "E_INVALID_CONFIG",
                    "layer keys must use the language:question_type form",
                    details={"dimension": dimension_id, "layer": layer_key},
                )
            language, question_type = layer_key.split(":", 1)
            if not language or not question_type:
                raise EngineError(
                    "E_INVALID_CONFIG",
                    "layer keys must contain a language and question type",
                    details={"dimension": dimension_id, "layer": layer_key},
                )
            normalized_layers[layer_key] = _require_number(
                layer_weight,
                f"profiling.dimensions.{dimension_id}.layers.{layer_key}",
                minimum=0,
            )
        if sum(normalized_layers.values()) <= 0:
            raise EngineError(
                "E_INVALID_CONFIG",
                "layer weights must have a positive sum",
                details={"dimension": dimension_id},
            )
        normalized_dimensions[str(dimension_id)] = {
            "weight": weight,
            "layers": dict(sorted(normalized_layers.items())),
        }
    if sum(item["weight"] for item in normalized_dimensions.values()) <= 0:
        raise EngineError(
            "E_INVALID_CONFIG",
            "dimension weights must have a positive sum",
        )
    profiling["dimensions"] = dict(sorted(normalized_dimensions.items()))

    ranking = config["ranking"]
    ranking["score_tolerance"] = _require_number(
        ranking.get("score_tolerance"),
        "ranking.score_tolerance",
        minimum=0,
    )
    ranking["min_valid_samples"] = _require_int(
        ranking.get("min_valid_samples"),
        "ranking.min_valid_samples",
        minimum=0,
    )
    ranking["min_benchmark_coverage"] = _require_int(
        ranking.get("min_benchmark_coverage"),
        "ranking.min_benchmark_coverage",
        minimum=0,
    )
    ranking["top_k"] = _require_int(
        ranking.get("top_k"), "ranking.top_k", minimum=1
    )
    return config


def load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise EngineError(
            "E_INVALID_CONFIG",
            "configuration file is missing",
            details={"path": str(path)},
        )
    try:
        raw = read_json(path)
    except (OSError, ValueError) as exc:
        raise EngineError(
            "E_INVALID_CONFIG",
            "configuration file is not valid JSON",
            details={"path": str(path), "error": str(exc)},
        ) from exc
    return normalize_config(raw)


def config_component_digests(config: dict[str, Any]) -> dict[str, str]:
    return {
        "scoring": digest_json(config["scoring"]),
        "exclusions": digest_json(config["exclusions"]),
        "profiling": digest_json(config["profiling"]),
        "ranking": digest_json(config["ranking"]),
    }


def config_digest(config: dict[str, Any]) -> str:
    return digest_json(config)


def default_config() -> dict[str, Any]:
    return copy.deepcopy(DEFAULT_CONFIG)
