"""Deterministic formant-trajectory pronunciation modeling."""

from __future__ import annotations

import math
from typing import Any

import numpy as np

from .config import Config
from .errors import ToneLabError
from .models import PronUnit, Shard
from .util import canonical_json_bytes, sha256_bytes


def _round(value: float, digits: int) -> float:
    return float(round(float(value), digits))


def _pitch_at(shard: Shard, normalized_time: float) -> float:
    times = np.asarray([point[0] for point in shard.pitch_curve], dtype=np.float64)
    values = np.asarray([point[1] for point in shard.pitch_curve], dtype=np.float64)
    if times.size == 1:
        return float(values[0])
    return float(np.interp(normalized_time, times, values))


def _unit_weights(units: list[PronUnit], time_ms: float, transition_ms: float) -> list[float]:
    weights: list[float] = []
    for unit in units:
        if unit.start_ms <= time_ms <= unit.end_ms:
            weights.append(1.0)
            continue
        if time_ms < unit.start_ms:
            distance = unit.start_ms - time_ms
        else:
            distance = time_ms - unit.end_ms
        if transition_ms > 0 and distance < transition_ms:
            weight = 1.0 - distance / transition_ms
            weights.append(max(0.0, weight))
        else:
            weights.append(0.0)
    if not any(weights):
        nearest = min(
            range(len(units)),
            key=lambda idx: min(abs(time_ms - units[idx].start_ms), abs(time_ms - units[idx].end_ms)),
        )
        weights = [0.0] * len(units)
        weights[nearest] = 1.0
    return weights


def build_articulation(shard: Shard, features: dict[str, Any], config: Config) -> dict[str, Any]:
    settings = config.data["model"]
    digits = int(settings["round_digits"])
    transition_ms = float(settings["transition_ms"])
    pitch_min = float(settings["pitch_min"])
    pitch_max = float(settings["pitch_max"])
    f0_min = float(settings["f0_min_hz"])
    f0_max = float(settings["f0_max_hz"])
    formant_count = int(settings["formant_count"])
    if not shard.units:
        raise ToneLabError(
            "E_VALIDATION",
            "pronunciation modeling requires at least one unit",
            shard_id=shard.shard_id,
        )
    frames: list[dict[str, Any]] = []
    for frame in features.get("frames", []):
        time_ms = float(frame["center_ms"])
        normalized_time = min(1.0, max(0.0, time_ms / shard.duration_ms)) if shard.duration_ms else 0.0
        pitch_value = _pitch_at(shard, normalized_time)
        if not math.isfinite(pitch_value) or pitch_value < pitch_min or pitch_value > pitch_max:
            raise ToneLabError(
                "E_NUMERIC",
                "interpolated pitch value is outside the configured range",
                shard_id=shard.shard_id,
                position={
                    "kind": "model_frame",
                    "frame_index": frame["index"],
                    "time_ms": time_ms,
                },
                context={"value": pitch_value, "allowed_range": [pitch_min, pitch_max]},
            )
        f0_hz = shard.reference_f0_mean * pitch_value
        if not math.isfinite(f0_hz) or f0_hz < f0_min or f0_hz > f0_max:
            raise ToneLabError(
                "E_NUMERIC",
                "synthesized f0 is outside the configured range",
                shard_id=shard.shard_id,
                position={"kind": "model_frame", "frame_index": frame["index"], "time_ms": time_ms},
                context={"value": f0_hz, "allowed_range": [f0_min, f0_max]},
            )
        weights = _unit_weights(shard.units, time_ms, transition_ms)
        total_weight = float(sum(weights))
        if total_weight <= 0:
            total_weight = 1.0
        formants = [0.0] * formant_count
        unit_indices: dict[int, float] = {}
        for unit_index, (unit, weight) in enumerate(zip(shard.units, weights)):
            if weight <= 0:
                continue
            unit_indices[unit_index] = round(weight / total_weight, digits)
            for formant_index in range(formant_count):
                target = (
                    float(unit.formants_hz[formant_index])
                    if formant_index < len(unit.formants_hz)
                    else float(unit.formants_hz[-1])
                )
                formants[formant_index] += target * weight
        formants = [value / total_weight for value in formants]
        frames.append(
            {
                "index": int(frame["index"]),
                "time_ms": _round(time_ms, digits),
                "normalized_time": _round(normalized_time, digits),
                "pitch_norm": _round(pitch_value, digits),
                "f0_hz": _round(f0_hz, digits),
                "formants_hz": [_round(value, digits) for value in formants],
                "gain": _round(float(frame.get("rms", 0.0)), digits),
                "unit_weights": {str(key): value for key, value in sorted(unit_indices.items())},
            }
        )
    artifact = {
        "format": "tonelab.articulation",
        "schema": 1,
        "shard_id": shard.shard_id,
        "timbre_id": shard.timbre_id,
        "structure_version": shard.structure_version,
        "source_structure_version": shard.source_structure_version,
        "sample_count": shard.sample_count,
        "sample_rate": shard.sample_rate,
        "reference_f0_mean": _round(shard.reference_f0_mean, digits),
        "transition_ms": _round(transition_ms, digits),
        "formant_count": formant_count,
        "frame_count": len(frames),
        "frames": frames,
    }
    artifact["artifact_sha256"] = sha256_bytes(canonical_json_bytes(artifact))
    return artifact

