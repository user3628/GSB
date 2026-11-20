from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

from .canonical import digest_value, read_json
from .errors import PipelineError


DEFAULT_CONFIG: dict[str, Any] = {
    "config_version": "1",
    "schedule": {
        "timebase": "microsecond",
        "rounding": "half_up",
    },
    "motion": {
        "pan": {
            "start": [-0.08, -0.03],
            "end": [0.08, 0.03],
        },
        "zoom": {
            "scale_start": 1.0,
            "scale_end": 1.16,
            "center": [0.5, 0.5],
            "drift": [0.02, -0.01],
        },
        "sampling": "bilinear",
        "clip_to_image": True,
    },
    "transition": {
        "hard_cut": {"window_frames": 0},
        "crossfade": {"window_frames": 2},
    },
    "cache": {
        "memory_budget_bytes": 8 * 1024 * 1024,
        "segment_fill_ratio": 0.80,
        "small_frame_threshold": 180,
        "full_cache_min_frames": 240,
        "workspace_frame_factor": 4.0,
    },
    "encode": {
        "format": "png",
        "compression": 6,
        "quantization": "floor_x255_plus_half_clip",
    },
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise PipelineError("E_INVALID_CONFIG", f"{name} must be a positive integer")
    return value


def normalize_config(raw: dict[str, Any] | None) -> dict[str, Any]:
    if raw is not None and not isinstance(raw, dict):
        raise PipelineError("E_INVALID_CONFIG", "config must be a JSON object")
    config = _deep_merge(DEFAULT_CONFIG, raw or {})
    config["config_version"] = str(config.get("config_version", "1"))
    config["cache"]["memory_budget_bytes"] = _positive_int(
        config["cache"].get("memory_budget_bytes"), "cache.memory_budget_bytes"
    )
    fill = config["cache"].get("segment_fill_ratio")
    if not isinstance(fill, (int, float)) or not 0 < float(fill) <= 1:
        raise PipelineError("E_INVALID_CONFIG", "cache.segment_fill_ratio must be in (0, 1]")
    config["cache"]["segment_fill_ratio"] = float(fill)
    factor = config["cache"].get("workspace_frame_factor", 4.0)
    if not isinstance(factor, (int, float)) or float(factor) < 1:
        raise PipelineError("E_INVALID_CONFIG", "cache.workspace_frame_factor must be >= 1")
    config["cache"]["workspace_frame_factor"] = float(factor)
    for key in ("small_frame_threshold", "full_cache_min_frames"):
        config["cache"][key] = _positive_int(config["cache"].get(key), f"cache.{key}")
    for transition_type in ("hard_cut", "crossfade"):
        section = config["transition"].get(transition_type)
        if not isinstance(section, dict):
            raise PipelineError(
                "E_INVALID_CONFIG", f"transition.{transition_type} must be an object"
            )
        window = section.get("window_frames")
        if isinstance(window, bool) or not isinstance(window, int) or window < 0:
            raise PipelineError(
                "E_INVALID_CONFIG",
                f"transition.{transition_type}.window_frames must be a non-negative integer",
            )
    if config["transition"]["hard_cut"]["window_frames"] != 0:
        raise PipelineError(
            "E_INVALID_CONFIG", "hard_cut.window_frames must be zero"
        )
    if config["transition"]["crossfade"]["window_frames"] <= 0:
        raise PipelineError(
            "E_INVALID_CONFIG", "crossfade.window_frames must be greater than zero"
        )
    if config["encode"].get("format") != "png":
        raise PipelineError("E_INVALID_CONFIG", "encode.format must be png")
    compression = config["encode"].get("compression")
    if isinstance(compression, bool) or not isinstance(compression, int) or not 0 <= compression <= 9:
        raise PipelineError("E_INVALID_CONFIG", "encode.compression must be in [0, 9]")
    config["motion"]["pan"]["start"] = _pair(config["motion"]["pan"]["start"], "pan.start")
    config["motion"]["pan"]["end"] = _pair(config["motion"]["pan"]["end"], "pan.end")
    config["motion"]["zoom"]["center"] = _pair(
        config["motion"]["zoom"]["center"], "zoom.center"
    )
    config["motion"]["zoom"]["drift"] = _pair(
        config["motion"]["zoom"]["drift"], "zoom.drift"
    )
    for key in ("scale_start", "scale_end"):
        value = config["motion"]["zoom"].get(key)
        if not isinstance(value, (int, float)) or float(value) <= 0:
            raise PipelineError("E_INVALID_CONFIG", f"zoom.{key} must be positive")
        config["motion"]["zoom"][key] = float(value)
    return config


def _pair(value: Any, name: str) -> list[float]:
    if (
        not isinstance(value, list)
        or len(value) != 2
        or any(isinstance(item, bool) or not isinstance(item, (int, float)) for item in value)
    ):
        raise PipelineError("E_INVALID_CONFIG", f"motion.{name} must be two numbers")
    return [float(value[0]), float(value[1])]


def load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise PipelineError("E_MISSING_ASSET", f"config not found: {path}", stage="import")
    try:
        raw = read_json(path)
    except Exception as exc:
        raise PipelineError("E_INVALID_CONFIG", f"cannot read config: {exc}", stage="import")
    return normalize_config(raw)


def config_digest(config: dict[str, Any]) -> str:
    return digest_value(config)
