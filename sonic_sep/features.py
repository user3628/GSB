"""Stage 2: band log-energy and spectral-flatness extraction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from .config import SeparationConfig
from .errors import numeric_error
from .util import digest_array, digest_json


EPS = 1e-15


@dataclass
class BandFeatures:
    features: np.ndarray
    band_edges_hz: np.ndarray
    bin_band: np.ndarray
    digest: str

    def summary(self) -> dict[str, Any]:
        return {
            "shape": list(self.features.shape),
            "feature_names": ["log_energy", "spectral_flatness"],
            "band_edges_hz": [float(value) for value in self.band_edges_hz],
            "feature_digest": self.digest,
        }


def band_edges(config: SeparationConfig) -> np.ndarray:
    return np.linspace(0.0, float(config.nyquist_hz), config.band_count + 1, dtype=np.float64)


def frequency_bin_to_band(config: SeparationConfig) -> np.ndarray:
    frequencies = np.fft.rfftfreq(config.frame_length, d=1.0 / config.sample_rate)
    width = float(config.band_width_hz)
    bands = np.floor(frequencies / width).astype(np.int64)
    return np.clip(bands, 0, config.band_count - 1)


def extract_features(
    spectra: np.ndarray,
    config: SeparationConfig,
    *,
    slice_id: str,
    frame_indices: np.ndarray | None = None,
) -> BandFeatures:
    spec = np.asarray(spectra)
    if spec.ndim != 2:
        raise numeric_error(
            "spectrum matrix must be two-dimensional",
            slice_id=slice_id,
            band_index=0,
            frame_index=0,
        )
    expected_bins = config.frame_length // 2 + 1
    if spec.shape[1] != expected_bins:
        raise numeric_error(
            "feature dimension does not match the configured frequency-bin count",
            slice_id=slice_id,
            band_index=0,
            frame_index=0,
            expected_bins=expected_bins,
            actual_bins=int(spec.shape[1]),
        )
    if not np.all(np.isfinite(spec.real)) or not np.all(np.isfinite(spec.imag)):
        bad = int(np.flatnonzero(~(np.isfinite(spec.real) & np.isfinite(spec.imag)))[0])
        raise numeric_error(
            "spectrum matrix contains NaN or Inf",
            slice_id=slice_id,
            band_index=0,
            frame_index=bad if frame_indices is None else int(frame_indices[bad]),
        )
    n_frames = spec.shape[0]
    edges = band_edges(config)
    mapper = frequency_bin_to_band(config)
    features = np.empty((n_frames, config.band_count, 2), dtype=np.float64)
    power = np.abs(spec) ** 2
    for band in range(config.band_count):
        bin_indices = np.flatnonzero(mapper == band)
        if bin_indices.size == 0:
            raise numeric_error(
                "frequency band contains no FFT bins",
                slice_id=slice_id,
                band_index=band,
                frame_index=0,
            )
        band_power = power[:, bin_indices]
        log_energy = np.log(np.sum(band_power, axis=1) + EPS)
        geometric = np.exp(np.mean(np.log(band_power + EPS), axis=1))
        arithmetic = np.mean(band_power, axis=1) + EPS
        flatness = geometric / arithmetic
        for column, values in ((0, log_energy), (1, flatness)):
            finite = np.isfinite(values)
            if not np.all(finite):
                local_frame = int(np.flatnonzero(~finite)[0])
                raise numeric_error(
                    "spectral feature contains NaN or Inf",
                    slice_id=slice_id,
                    band_index=band,
                    frame_index=local_frame if frame_indices is None else int(frame_indices[local_frame]),
                )
            features[:, band, column] = values
    digest = feature_digest(features, edges, mapper)
    return BandFeatures(features=features, band_edges_hz=edges, bin_band=mapper, digest=digest)


def validate_feature_matrix(
    features: np.ndarray,
    expected_bands: int,
    *,
    slice_id: str,
) -> None:
    array = np.asarray(features)
    if array.ndim != 3 or array.shape[1] != expected_bands or array.shape[2] != 2:
        raise numeric_error(
            "feature matrix dimensions do not match band_count",
            slice_id=slice_id,
            band_index=0,
            frame_index=0,
            expected_shape=[None, int(expected_bands), 2],
            actual_shape=list(array.shape),
        )
    finite = np.isfinite(array)
    if not np.all(finite):
        frames, bands, _ = np.nonzero(~finite)
        raise numeric_error(
            "feature matrix contains NaN or Inf",
            slice_id=slice_id,
            band_index=int(bands[0]),
            frame_index=int(frames[0]),
        )


def feature_digest(features: np.ndarray, edges: np.ndarray, mapper: np.ndarray) -> str:
    return digest_json(
        {
            "features": digest_array(features),
            "band_edges_hz": digest_array(edges),
            "bin_band": digest_array(mapper),
        }
    )


def features_input_fingerprint(stft_input_fp: str, config: SeparationConfig) -> str:
    return digest_json(
        {
            "stft_input_fingerprint": stft_input_fp,
            "stage_config": config.stage_fingerprint("features"),
        }
    )

