"""Explicit fixed-point quantization with estimated error bounds."""

from __future__ import annotations

import math
from typing import Any

import numpy as np

from .config import Config
from .models import Shard
from .util import canonical_json_bytes, sha256_bytes


def _effective_scale(configured_scale: float, bits: int, max_abs: float) -> float:
    if max_abs <= 0.0:
        return float(configured_scale)
    max_quantized = float((1 << (int(bits) - 1)) - 1)
    limit = max_quantized / max_abs
    if limit >= configured_scale:
        return float(configured_scale)
    power = 2.0 ** math.floor(math.log2(limit))
    return max(power, 2.0 ** -20)


def _quantize(
    values: list[float],
    scale: float,
    bits: int,
    field: str,
    *,
    enforce_bits: bool = True,
) -> tuple[list[int], float, float]:
    array = np.asarray(values, dtype=np.float64)
    scaled = array * float(scale)
    quantized = np.rint(scaled).astype(np.int64)
    recovered = quantized.astype(np.float64) / float(scale)
    errors = np.abs(recovered - array)
    actual_max = float(np.max(errors)) if errors.size else 0.0
    bound = 0.5 / float(scale)
    if enforce_bits and quantized.size:
        max_quantized = (1 << (int(bits) - 1)) - 1
        if int(np.max(np.abs(quantized))) > max_quantized:
            raise AssertionError(f"{field} quantization exceeded signed {bits}-bit range")
    if actual_max > bound + 1e-15:
        # Round-half-even can reach the bound, never exceed it beyond float noise.
        raise AssertionError(f"{field} quantization exceeded estimated bound")
    return [int(value) for value in quantized], bound, actual_max


def quantize_shard(shard: Shard, articulation: dict[str, Any], config: Config) -> dict[str, Any]:
    settings = config.data["quantization"]
    pitch_scale = float(settings["pitch_scale"])
    formant_scale = float(settings["formant_scale"])
    f0_scale = float(settings["f0_scale"])
    time_scale = 1_000_000_000.0
    curve_times = [point[0] for point in shard.pitch_curve]
    curve_values = [point[1] for point in shard.pitch_curve]
    pitch_bits = int(settings["pitch_bits"])
    formant_bits = int(settings["formant_bits"])
    f0_bits = int(settings["f0_bits"])
    pitch_scale = _effective_scale(pitch_scale, pitch_bits, max((abs(v) for v in curve_values), default=0.0))
    times_q, time_bound, time_actual = _quantize(curve_times, time_scale, 64, "pitch_curve.time", enforce_bits=False)
    values_q, pitch_bound, pitch_actual = _quantize(curve_values, pitch_scale, pitch_bits, "pitch_curve.value")
    formant_values = [value for frame in articulation["frames"] for value in frame["formants_hz"]]
    formant_scale = _effective_scale(formant_scale, formant_bits, max((abs(v) for v in formant_values), default=0.0))
    formants_q, formant_bound, formant_actual = _quantize(formant_values, formant_scale, formant_bits, "formants_hz")
    f0_values = [float(frame["f0_hz"]) for frame in articulation["frames"]]
    f0_scale = _effective_scale(f0_scale, f0_bits, max((abs(v) for v in f0_values), default=0.0))
    f0_q, f0_bound, f0_actual = _quantize(f0_values, f0_scale, f0_bits, "f0_hz")
    formant_count = int(articulation["formant_count"])
    formant_rows = [
        formants_q[index : index + formant_count]
        for index in range(0, len(formants_q), formant_count)
    ]
    artifact = {
        "format": "tonelab.quantized",
        "schema": 1,
        "shard_id": shard.shard_id,
        "structure_version": shard.structure_version,
        "source_structure_version": shard.source_structure_version,
        "sample_count": shard.sample_count,
        "sample_rate": shard.sample_rate,
        "pitch_curve": {
            "time_scale": int(time_scale),
            "pitch_scale": int(pitch_scale),
            "times_q": times_q,
            "values_q": values_q,
            "value_bits": pitch_bits,
            "error_bound": pitch_bound,
            "max_observed_error": pitch_actual,
            "time_error_bound": time_bound,
            "time_max_observed_error": time_actual,
        },
        "formant_trajectory": {
            "formant_scale": int(formant_scale),
            "formant_bits": formant_bits,
            "formant_count": formant_count,
            "rows_q": formant_rows,
            "error_bound": formant_bound,
            "max_observed_error": formant_actual,
        },
        "f0_trajectory": {
            "f0_scale": int(f0_scale),
            "f0_bits": f0_bits,
            "values_q": f0_q,
            "error_bound": f0_bound,
            "max_observed_error": f0_actual,
        },
        "quantization_bits": {
            "pitch": pitch_bits,
            "formant": formant_bits,
            "f0": f0_bits,
        },
        "per_shard_scales": {
            "pitch_scale": pitch_scale,
            "formant_scale": formant_scale,
            "f0_scale": f0_scale,
            "time_scale": time_scale,
            "configured": {
                "pitch_scale": float(settings["pitch_scale"]),
                "formant_scale": float(settings["formant_scale"]),
                "f0_scale": float(settings["f0_scale"]),
            },
        },
    }
    artifact["artifact_sha256"] = sha256_bytes(canonical_json_bytes(artifact))
    return artifact

