"""Framing and dual-feature frame scoring."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np
from scipy.signal import get_window

from .config import VADConfig
from .errors import VadError


@dataclass(frozen=True)
class FeatureVector:
    log_energy_db: float
    spectral_flatness: float
    normalized_energy: float
    normalized_flatness: float
    weighted_score: float

    @property
    def dimensions(self) -> int:
        return 2

    def as_dict(self) -> dict[str, float]:
        return {
            "log_energy_db": self.log_energy_db,
            "spectral_flatness": self.spectral_flatness,
            "normalized_energy": self.normalized_energy,
            "normalized_flatness": self.normalized_flatness,
            "weighted_score": self.weighted_score,
        }


@dataclass(frozen=True)
class FrameInfo:
    index: int
    start_sample: int
    end_sample: int
    is_short: bool
    samples: np.ndarray

    @property
    def length(self) -> int:
        return self.end_sample - self.start_sample


def iter_frames(
    samples: np.ndarray, config: VADConfig, *, start_sample: int = 0
) -> Iterable[FrameInfo]:
    total = int(samples.shape[0])
    start = start_sample
    index = 0
    while start < total:
        end = min(start + config.frame_samples, total)
        yield FrameInfo(
            index=index,
            start_sample=start,
            end_sample=end,
            is_short=end - start < config.frame_samples,
            samples=samples[start:end],
        )
        start += config.hop_samples
        index += 1


def score_frame(
    frame: FrameInfo,
    config: VADConfig,
    *,
    injected_dimensions: int | None = None,
) -> FeatureVector:
    if injected_dimensions is not None and injected_dimensions != 2:
        raise VadError(
            "E_FEATURE_MISMATCH",
            "feature vector dimension does not match the two-feature scorer",
            sample_position=frame.start_sample,
            frame_index=frame.index,
            details={"expected_dimensions": 2, "actual_dimensions": injected_dimensions},
        )

    if frame.length <= 0:
        raise VadError(
            "E_FEATURE_MISMATCH",
            "empty frame cannot produce features",
            sample_position=frame.start_sample,
            frame_index=frame.index,
        )

    window = get_window(config.window, frame.length, fftbins=True)
    normalized = frame.samples.astype(np.float64) / 32768.0
    windowed = normalized * window

    mean_square = float(np.mean(np.square(windowed), dtype=np.float64))
    rms = float(np.sqrt(mean_square))
    log_energy_db = 20.0 * float(np.log10(max(rms, 1e-12)))

    spectrum = np.fft.rfft(windowed, n=config.frame_samples)
    power = np.square(np.abs(spectrum)) + np.finfo(np.float64).tiny
    arithmetic_mean = float(np.mean(power, dtype=np.float64))
    geometric_mean = float(np.exp(np.mean(np.log(power), dtype=np.float64)))
    spectral_flatness = min(1.0, max(0.0, geometric_mean / max(arithmetic_mean, 1e-300)))

    energy_span = config.energy_ceiling_db - config.energy_floor_db
    normalized_energy = min(
        1.0, max(0.0, (log_energy_db - config.energy_floor_db) / energy_span)
    )
    normalized_flatness = 1.0 - spectral_flatness
    weighted_score = (
        config.energy_weight * normalized_energy
        + config.flatness_weight * normalized_flatness
    )
    return FeatureVector(
        log_energy_db=round(log_energy_db, 6),
        spectral_flatness=round(spectral_flatness, 9),
        normalized_energy=round(normalized_energy, 9),
        normalized_flatness=round(normalized_flatness, 9),
        weighted_score=round(min(1.0, max(0.0, weighted_score)), 9),
    )


def feature_array(vector: FeatureVector) -> np.ndarray:
    return np.asarray(
        [vector.log_energy_db, vector.spectral_flatness], dtype=np.float64
    )


def inspect_features(samples: np.ndarray, config: VADConfig) -> list[dict[str, Any]]:
    config.validate()
    return [
        {
            "frame_index": frame.index,
            "start_sample": frame.start_sample,
            "end_sample": frame.end_sample,
            "is_short": frame.is_short,
            **score_frame(frame, config).as_dict(),
        }
        for frame in iter_frames(samples, config)
    ]

