from __future__ import annotations

import hashlib
import math
from typing import Any, Iterable

import numpy as np

from .errors import ConfigHubError, ErrorCode
from .models import SampleSpec
from .util import canonical_json, normalize_config, sha256_text


DEFAULT_CONFIG_KEYS: tuple[str, ...] = (
    "w0",
    "w1",
    "w2",
    "normalization_scale",
    "threshold",
)


def validate_sample_spec(spec: SampleSpec) -> SampleSpec:
    if not isinstance(spec.seed, int) or spec.seed < 0:
        raise ConfigHubError(
            ErrorCode.E_VALIDATION, "sample seed must be a non-negative integer", status_code=422
        )
    if not isinstance(spec.sample_size, int) or not (1 <= spec.sample_size <= 1_000_000):
        raise ConfigHubError(
            ErrorCode.E_VALIDATION,
            "sample_size must be an integer in [1, 1000000]",
            status_code=422,
        )
    if not isinstance(spec.feature_count, int) or not (1 <= spec.feature_count <= 32):
        raise ConfigHubError(
            ErrorCode.E_VALIDATION,
            "feature_count must be an integer in [1, 32]",
            status_code=422,
        )
    return spec


def generate_samples(spec: SampleSpec) -> np.ndarray:
    """Generate the fixed local tensor without touching NumPy global RNG state."""

    validate_sample_spec(spec)
    rng = np.random.Generator(np.random.PCG64(spec.seed))
    return rng.uniform(-1.0, 1.0, size=(spec.sample_size, spec.feature_count)).astype(
        np.float64, copy=False
    )


def _weights_for(config: dict[str, float], feature_count: int) -> np.ndarray:
    if "weights" in config:
        # A scalar list cannot occur in the normalized config model, so this is
        # intentionally left to the explicit w0..wN representation.
        raise ConfigHubError(
            ErrorCode.E_VALIDATION,
            "weights must be expressed as w0..wN numeric configuration items",
            status_code=422,
        )
    keys = [f"w{i}" for i in range(feature_count)]
    missing = [key for key in keys if key not in config]
    if missing:
        raise ConfigHubError(
            ErrorCode.E_VALIDATION,
            "missing weight configuration keys",
            status_code=422,
            details={"missing": missing},
        )
    weights = np.asarray([config[key] for key in keys], dtype=np.float64)
    if not np.all(np.isfinite(weights)):
        raise ConfigHubError(ErrorCode.E_VALIDATION, "weights must be finite", status_code=422)
    return weights


def _normalization_for(config: dict[str, float]) -> float:
    for key in ("normalization_scale", "scale", "ratio"):
        if key in config:
            value = float(config[key])
            if value == 0.0 or not math.isfinite(value):
                raise ConfigHubError(
                    ErrorCode.E_VALIDATION,
                    "normalization scale must be finite and non-zero",
                    status_code=422,
                )
            return value
    raise ConfigHubError(
        ErrorCode.E_VALIDATION,
        "missing normalization_scale (or scale/ratio) configuration key",
        status_code=422,
    )


def _threshold_for(config: dict[str, float]) -> float:
    if "threshold" not in config or not math.isfinite(float(config["threshold"])):
        raise ConfigHubError(
            ErrorCode.E_VALIDATION,
            "missing or invalid threshold configuration key",
            status_code=422,
        )
    return float(config["threshold"])


def _array_digest(*arrays: np.ndarray) -> str:
    digest = hashlib.sha256()
    for array in arrays:
        canonical = np.ascontiguousarray(array, dtype="<f8")
        digest.update(int(canonical.size).to_bytes(8, "little", signed=False))
        digest.update(canonical.tobytes(order="C"))
    return digest.hexdigest()


def _float_stats(values: np.ndarray) -> dict[str, float]:
    return {
        "sum": float(np.sum(values, dtype=np.float64)),
        "mean": float(np.mean(values, dtype=np.float64)),
        "min": float(np.min(values)),
        "max": float(np.max(values)),
    }


def compute_pipeline(spec: SampleSpec, config_version: int, config: dict[str, Any]) -> dict[str, Any]:
    """Compute a deterministic summary for one exact snapshot version."""

    validate_sample_spec(spec)
    normalized_config = normalize_config(config)
    weights = _weights_for(normalized_config, spec.feature_count)
    scale = _normalization_for(normalized_config)
    threshold = _threshold_for(normalized_config)

    samples = generate_samples(spec)
    weighted_sum = np.asarray(samples @ weights, dtype=np.float64)
    normalized = np.asarray(weighted_sum / scale, dtype=np.float64)
    flags = np.asarray(normalized >= threshold, dtype=np.bool_)

    samples_digest = _array_digest(samples)
    weighted_sum_digest = _array_digest(weighted_sum)
    normalized_digest = _array_digest(normalized)
    flags_digest = _array_digest(flags.astype(np.float64))
    result_material = {
        "algorithm": "weighted-sum-normalize-threshold-v1",
        "sample": spec.as_dict(),
        "sample_digest": samples_digest,
        "config_version": int(config_version),
        "config": normalized_config,
        "weighted_sum_digest": weighted_sum_digest,
        "normalized_digest": normalized_digest,
        "threshold_flags_digest": flags_digest,
    }
    result_digest = sha256_text(canonical_json(result_material))
    selected_count = int(np.count_nonzero(flags))
    return {
        "algorithm": result_material["algorithm"],
        "sample": spec.as_dict(),
        "sample_digest": samples_digest,
        "config_version": int(config_version),
        "config_digest": sha256_text(canonical_json(normalized_config)),
        "result_digest": result_digest,
        "weighted_sum_digest": weighted_sum_digest,
        "normalized_digest": normalized_digest,
        "threshold_flags_digest": flags_digest,
        "stats": {
            "weighted_sum": _float_stats(weighted_sum),
            "normalized": _float_stats(normalized),
            "threshold": threshold,
            "selected_count": selected_count,
            "unselected_count": int(spec.sample_size - selected_count),
            "selected_ratio": selected_count / spec.sample_size,
        },
    }


def required_keys(spec: SampleSpec) -> tuple[str, ...]:
    validate_sample_spec(spec)
    return tuple(f"w{i}" for i in range(spec.feature_count)) + ("threshold",)


def missing_keys(config: dict[str, Any], keys: Iterable[str]) -> list[str]:
    return [key for key in keys if key not in config]

