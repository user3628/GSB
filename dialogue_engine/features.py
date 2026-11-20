"""Deterministic frame-level acoustic analysis and turn-level aggregation."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
from scipy.signal import get_window

from .config import FeatureConfig
from .errors import E_EMPTY_TURN, E_FEATURE_DIM, E_INVALID_STAGE, E_NUMERIC, as_error
from .manifest import ShardData, TurnAnnotation
from .serialization import round_floats


VECTOR_BASE_DIM = 16


def _hz_to_mel(frequency: np.ndarray | float) -> np.ndarray | float:
    return 2595.0 * np.log10(1.0 + np.asarray(frequency) / 700.0)


def _mel_to_hz(mel: np.ndarray | float) -> np.ndarray | float:
    return 700.0 * (10.0 ** (np.asarray(mel) / 2595.0) - 1.0)


def mel_filterbank(sample_rate: int, fft_size: int, bands: int) -> np.ndarray:
    if bands < 1:
        raise ValueError("bands must be positive")
    nyquist = sample_rate / 2.0
    mel_points = np.linspace(
        float(_hz_to_mel(0.0)), float(_hz_to_mel(nyquist)), bands + 2
    )
    hz_points = np.asarray(_mel_to_hz(mel_points), dtype=np.float64)
    frequencies = np.linspace(0.0, nyquist, fft_size // 2 + 1, dtype=np.float64)
    bank = np.zeros((bands, frequencies.shape[0]), dtype=np.float64)
    for band in range(bands):
        left, center, right = hz_points[band : band + 3]
        if center <= left:
            center = left + 1e-9
        if right <= center:
            right = center + 1e-9
        rising = (frequencies - left) / (center - left)
        falling = (right - frequencies) / (right - center)
        bank[band] = np.maximum(0.0, np.minimum(rising, falling))
    empty = np.flatnonzero(bank.sum(axis=1) <= 0.0)
    for band in empty:
        nearest = int(
            np.argmin(np.abs(frequencies - hz_points[band + 1]))
        )
        bank[band, nearest] = 1.0
    return bank


def _linear_slope(values: np.ndarray) -> float:
    if values.size < 2:
        return 0.0
    x = np.linspace(0.0, 1.0, values.size, dtype=np.float64)
    x_centered = x - x.mean()
    denominator = float(np.dot(x_centered, x_centered))
    if denominator <= 0.0:
        return 0.0
    return float(np.dot(x_centered, values - values.mean()) / denominator)


def extract_turn_features(
    shard: ShardData,
    turn: TurnAnnotation,
    config: FeatureConfig,
) -> dict[str, Any]:
    """Analyze one turn and return a canonicalizable fixed-dimension artifact."""

    config.validate()
    frame_samples = config.frame_samples
    hop_samples = config.hop_samples
    start = turn.start_sample
    end = turn.end_sample
    if start < 0 or end > shard.sample_count or start >= end:
        raise as_error(
            E_EMPTY_TURN,
            "turn does not contain samples",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="features",
        )
    if end - start < frame_samples:
        raise as_error(
            E_EMPTY_TURN,
            f"turn has {end - start} samples, fewer than one {frame_samples}-sample frame",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="features",
        )

    raw = np.asarray(shard.samples[start:end], dtype=np.float64) / 32768.0
    frame_starts = np.arange(0, raw.size - frame_samples + 1, hop_samples)
    if frame_starts.size == 0:
        raise as_error(
            E_EMPTY_TURN,
            "no complete frame can be extracted from the turn",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="features",
        )
    indices = frame_starts[:, None] + np.arange(frame_samples, dtype=np.int64)[None, :]
    frames = raw[indices]
    window = get_window(config.window, frame_samples, fftbins=True).astype(np.float64)
    windowed = frames * window[None, :]
    power = np.abs(np.fft.rfft(windowed, n=config.fft_size, axis=1)) ** 2
    frequencies = np.fft.rfftfreq(config.fft_size, d=1.0 / shard.sample_rate)

    log_energy = np.log(
        np.mean(frames * frames, axis=1, dtype=np.float64) + config.energy_floor
    )
    signs = np.signbit(frames)
    zcr = np.mean(signs[:, 1:] != signs[:, :-1], axis=1, dtype=np.float64)
    power_sum = power.sum(axis=1)
    centroid = np.zeros(power_sum.shape, dtype=np.float64)
    nonzero_power = power_sum > 0.0
    centroid[nonzero_power] = (
        power[nonzero_power] @ frequencies
    ) / power_sum[nonzero_power]
    centroid /= shard.sample_rate / 2.0

    energy_peak = float(np.max(log_energy))
    if energy_peak <= math.log(config.energy_floor) + 1.0:
        raise as_error(
            E_EMPTY_TURN,
            "turn is digital silence and contains no speech energy",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="features",
        )
    threshold = energy_peak - config.pause_db_below_peak
    voiced = log_energy >= threshold
    if not np.any(voiced):
        raise as_error(
            E_EMPTY_TURN,
            "turn contains no frame above the speech-energy threshold",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="features",
        )
    pause_ratio = float(1.0 - np.mean(voiced, dtype=np.float64))
    voiced_ratio = 1.0 - pause_ratio

    filters = mel_filterbank(shard.sample_rate, config.fft_size, config.mel_bands)
    mel_energy = power @ filters.T
    mel_log = np.log(mel_energy + config.energy_floor)
    mel_means = np.mean(mel_log[voiced], axis=0, dtype=np.float64)

    voiced_energy = log_energy[voiced]
    vector = np.concatenate(
        (
            np.asarray(
                [
                    float(np.mean(log_energy)),
                    float(np.std(log_energy)),
                    float(np.min(log_energy)),
                    float(np.max(log_energy)),
                    _linear_slope(log_energy),
                    _linear_slope(voiced_energy),
                    float(np.mean(zcr)),
                    float(np.std(zcr)),
                    float(np.mean(centroid)),
                    float(np.std(centroid)),
                    pause_ratio,
                    voiced_ratio,
                    float(log_energy[-1] - log_energy[0]),
                    float(voiced_energy[-1] - voiced_energy[0]),
                    float(np.mean(voiced_energy)),
                    float(np.std(voiced_energy)),
                ],
                dtype=np.float64,
            ),
            mel_means,
        )
    )
    if vector.shape[0] != config.dimension:
        raise as_error(
            E_FEATURE_DIM,
            f"built feature dimension {vector.shape[0]} != expected {config.dimension}",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="features",
        )
    if not np.all(np.isfinite(vector)):
        raise as_error(
            E_NUMERIC,
            "acoustic feature vector contains NaN or Inf",
            shard_id=shard.shard_id,
            turn_index=turn.index,
            stage="features",
        )
    artifact = {
        "schema_version": 1,
        "shard_id": shard.shard_id,
        "turn_index": turn.index,
        "feature_dim": int(vector.shape[0]),
        "vector": [float(value) for value in vector],
        "frame_count": int(frames.shape[0]),
        "hop_samples": hop_samples,
        "frame_samples": frame_samples,
        "fft_size": config.fft_size,
        "duration_ms": float((end - start) * 1000.0 / shard.sample_rate),
        "pause_ratio": pause_ratio,
        "voiced_ratio": voiced_ratio,
        "voiced_frame_count": int(np.count_nonzero(voiced)),
        "config_version": config.version,
    }
    return round_floats(artifact, config.decimals)


def validate_feature_artifact(
    artifact: Any,
    config: FeatureConfig,
    *,
    shard_id: str,
    turn_index: int,
) -> np.ndarray:
    if not isinstance(artifact, dict):
        raise as_error(
            E_INVALID_STAGE,
            "feature artifact is not an object",
            shard_id=shard_id,
            turn_index=turn_index,
            stage="features",
        )
    vector = artifact.get("vector")
    if not isinstance(vector, list) or len(vector) != config.dimension:
        actual = len(vector) if isinstance(vector, list) else None
        raise as_error(
            E_FEATURE_DIM,
            f"feature dimension {actual!r} != expected {config.dimension}",
            shard_id=shard_id,
            turn_index=turn_index,
            stage="features",
            actual_dimension=actual,
            expected_dimension=config.dimension,
        )
    array = np.asarray(vector, dtype=np.float64)
    if not np.all(np.isfinite(array)):
        raise as_error(
            E_NUMERIC,
            "feature vector contains NaN or Inf",
            shard_id=shard_id,
            turn_index=turn_index,
            stage="features",
        )
    if artifact.get("feature_dim") != config.dimension:
        raise as_error(
            E_FEATURE_DIM,
            "feature artifact feature_dim does not match vector length",
            shard_id=shard_id,
            turn_index=turn_index,
            stage="features",
        )
    return array

