"""Configuration defaults and fingerprint helpers."""

from __future__ import annotations

import copy
from typing import Any, Dict

from .canonical import digest_json


PIPELINE_SCHEMA_VERSION = 1

DETECTION_CONFIG: Dict[str, Any] = {
    "threshold": 60,
    "min_component_pixels": 60,
    "min_component_width": 24,
    "min_component_height": 24,
    "line_coverage": 0.55,
    "table_min_grid_lines": 8,
    "max_regions_per_kind": 1,
}

TABLE_CONFIG: Dict[str, Any] = {
    "line_coverage": 0.30,
    "segment_coverage": 0.45,
    "line_band": 1,
    "header_brightness": 170.0,
    "min_cell_dimension": 2,
}

CHART_CONFIG: Dict[str, Any] = {
    "axis_band": 8,
    "tick_min_pixels": 3,
    "label_gap": 3,
    "glyph_scale": 2,
    "color_channel_delta": 30,
    "min_series_pixels": 24,
    "marker_min_thickness": 6,
    "marker_box_size": 7,
    "marker_density_threshold": 30,
    "value_tolerance": 1e-4,
    "pixel_spacing_tolerance": 2.0,
}

BUDGET_CONFIG: Dict[str, Any] = {
    "total_pixels": 1350000,
    "worker_pixel_quota": 420000,
    "max_workers": 2,
    "acceptance_order": "index",
    "tier_factors": {"S": 1.0, "M": 1.05, "L": 1.1},
}

STAGES = ("detection", "table", "chart", "validation")
STAGE_INDEX = {name: index + 1 for index, name in enumerate(STAGES)}


def default_configs() -> Dict[str, Any]:
    return {
        "schema_version": PIPELINE_SCHEMA_VERSION,
        "detection": copy.deepcopy(DETECTION_CONFIG),
        "table": copy.deepcopy(TABLE_CONFIG),
        "chart": copy.deepcopy(CHART_CONFIG),
        "budget": copy.deepcopy(BUDGET_CONFIG),
    }


def merge_configs(base: Dict[str, Any], override: Dict[str, Any] | None) -> Dict[str, Any]:
    result = copy.deepcopy(base)
    if not override:
        return result
    for key, value in override.items():
        if key in ("detection", "table", "chart", "budget") and isinstance(value, dict):
            result[key].update(copy.deepcopy(value))
        else:
            result[key] = copy.deepcopy(value)
    return result


def config_version(configs: Dict[str, Any]) -> str:
    return digest_json(
        {
            "schema_version": configs.get("schema_version", PIPELINE_SCHEMA_VERSION),
            "detection": configs["detection"],
            "table": configs["table"],
            "chart": configs["chart"],
            "budget": configs["budget"],
        }
    )


def stage_config_version(configs: Dict[str, Any], stage: str) -> str:
    if stage == "detection":
        value = configs["detection"]
    elif stage == "table":
        value = configs["table"]
    elif stage == "chart":
        value = configs["chart"]
    elif stage == "validation":
        value = configs["budget"]
    else:
        raise ValueError(f"unknown stage: {stage}")
    return digest_json({"schema_version": PIPELINE_SCHEMA_VERSION, "stage": stage, "config": value})
