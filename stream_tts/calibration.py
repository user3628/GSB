"""Frozen local latency calibration used by the reproducible scheduler."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

from .acoustic import AcousticPredictor, benchmark_unit_ms
from .config import EngineConfig
from .utils import atomic_write_json, read_json, sha256_json


def calibration_digest(raw: dict[str, Any]) -> str:
    return sha256_json(raw)


def ensure_calibration(config: EngineConfig, predictor: AcousticPredictor) -> dict[str, Any]:
    path = config.resolved_path("calibration")
    existing = read_json(path)
    if (
        isinstance(existing, dict)
        and existing.get("schema_version") == 1
        and existing.get("benchmark_key")
        == predictor.weight_digest + ":" + config.model_structure_digest()
        and isinstance(existing.get("unit_ms"), (int, float))
        and math.isfinite(float(existing["unit_ms"]))
        and float(existing["unit_ms"]) > 0
    ):
        return existing
    measured = benchmark_unit_ms(config, predictor)
    measured["calibration_digest"] = calibration_digest(measured)
    atomic_write_json(path, measured)
    return measured


def load_calibration(path: Path) -> dict[str, Any]:
    raw = read_json(path)
    if not isinstance(raw, dict) or "unit_ms" not in raw:
        raise FileNotFoundError(path)
    return raw
