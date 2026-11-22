from __future__ import annotations

from copy import deepcopy
from typing import Any


DEFAULT_CONFIG: dict[str, Any] = {
    "schema_version": 1,
    "config_version": "shotcheck-config-1",
    "split": {
        "unicode_form": "NFKC",
        "collapse_whitespace": True,
        "unit_labels": [
            "subject_description",
            "scene_description",
            "constraint",
            "duration",
        ],
    },
    "duration": {
        "fps": 25,
    },
    "coverage": {
        "negative_window_chars": 18,
        "weights": {
            "must_subject_coverage": 0.45,
            "scene_coverage": 0.25,
            "duration_consistency": 0.30,
        },
        "review_min_score": 0.70,
    },
    "strategy": {
        "small_shot_threshold": 12,
        "small_char_threshold": 2500,
        "truncation_min_shot_count": 24,
        "truncation_min_chars": 4000,
        "chunk_shots": 8,
        "chunk_overhead_cost": 60,
        "term_scan_cost": 0.75,
        "char_scan_cost": 4.0,
        "check_depth_budget": 240_000,
    },
}


def default_config() -> dict[str, Any]:
    return deepcopy(DEFAULT_CONFIG)
