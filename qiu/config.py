"""Configuration defaults and validation helpers."""

from __future__ import annotations

import copy
import math
from typing import Any

from .errors import QueryError


DEFAULT_CONFIG: dict[str, Any] = {
    "normalization": {
        "allowed_locales": ["en-US", "zh-CN", "zh-TW"],
        "max_query_chars": 160,
    },
    "classifier": {
        "class_weights": {
            "informational": 1.0,
            "local_service": 1.0,
            "navigation": 1.0,
            "transactional": 1.0,
        },
        "feature_scale": 0.35,
        "nb_scale": 1.0,
        "smoothing": 1.0,
        "token_feature_weights": {},
    },
    "slots": {
        "expected_dict_version": "slot-dict-v1",
        "type_priority": {"brand": 30, "category": 40, "region": 20},
    },
    "policy": {
        "category_intent_map": {},
        "channel_defaults": {},
        "confidence_threshold": 0.62,
        "final_default": "informational",
    },
}


def load_config(raw: dict[str, Any]) -> dict[str, Any]:
    """Merge a local config object over deterministic defaults."""

    if not isinstance(raw, dict):
        raise ValueError("configuration root must be an object")
    merged = copy.deepcopy(DEFAULT_CONFIG)
    for section, values in raw.items():
        if section in merged and isinstance(merged[section], dict) and isinstance(values, dict):
            _deep_merge(merged[section], values)
        else:
            merged[section] = copy.deepcopy(values)
    validate_config(merged)
    return merged


def _deep_merge(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key, value in source.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_merge(target[key], value)
        else:
            target[key] = copy.deepcopy(value)


def validate_config(config: dict[str, Any]) -> None:
    threshold = config["policy"].get("confidence_threshold")
    if not isinstance(threshold, (int, float)) or not math.isfinite(float(threshold)):
        raise ValueError("policy.confidence_threshold must be finite")
    if not 0.0 <= float(threshold) <= 1.0:
        raise ValueError("policy.confidence_threshold must be in [0, 1]")

    max_chars = config["normalization"].get("max_query_chars")
    if not isinstance(max_chars, int) or isinstance(max_chars, bool) or max_chars <= 0:
        raise ValueError("normalization.max_query_chars must be a positive integer")

    allowed = config["normalization"].get("allowed_locales")
    if not isinstance(allowed, list) or not allowed or not all(isinstance(item, str) for item in allowed):
        raise ValueError("normalization.allowed_locales must be a non-empty string list")

    smoothing = config["classifier"].get("smoothing")
    if not isinstance(smoothing, (int, float)) or not math.isfinite(float(smoothing)) or float(smoothing) <= 0:
        raise ValueError("classifier.smoothing must be a finite positive number")

    for key in ("nb_scale", "feature_scale"):
        value = config["classifier"].get(key)
        if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            raise ValueError(f"classifier.{key} must be finite")


def config_slice(config: dict[str, Any], section: str) -> dict[str, Any]:
    value = config.get(section)
    if not isinstance(value, dict):
        raise ValueError(f"configuration section {section!r} must be an object")
    return value


def invalid_locale_error(locale: Any, allowed: list[str], query_id: str | None) -> QueryError:
    return QueryError(
        "E_INVALID_LOCALE",
        "validation",
        "locale is not in the configured allow-list",
        query_id=query_id,
        details={"allowed_locales": sorted(allowed), "locale": locale},
    )
