"""Stage 1: deterministic Hann-window framing and short-time Fourier transform."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np

from .config import SeparationConfig
from .errors import invalid_config, numeric_error
from .util import digest_array, digest_json


@dataclass
class StftResult:
    spectra: np.ndarray
    frame_indices: np.ndarray
    frame_starts: np.ndarray
    valid_lengths: np.ndarray
    padded: np.ndarray
    window: np.ndarray
    n_samples: int
    digest: str

    def summary(self) -> dict[str, Any]:
        return {
            "frame_count": int(self.spectra.shape[0]),
            "frequency_bins": int(self.spectra.shape[1]),
            "padded_frame_count": int(np.count_nonzero(self.padded)),
            "first_frame_sample_offset": int(self.frame_starts[0]) if len(self.frame_starts) else None,
            "last_frame_sample_offset": int(self.frame_starts[-1]) if len(self.frame_starts) else None,
            "spectrum_digest": self.digest,
        }


def hann_window(frame_length: int) -> np.ndarray:
    return np.hanning(frame_length).astype(np.float64)


def frame_count(n_samples: int, hop_length: int) -> int:
    return max(1, int(np.ceil(n_samples / hop_length)))


def compute_stft(
    audio: np.ndarray,
    config: SeparationConfig,
    *,
    slice_id: str,
    frame_indices: Iterable[int] | None = None,
) -> StftResult:
    if config.frame_length <= 0:
        raise invalid_config("frame_length must be positive", slice_id=slice_id, param="frame_length")
    if config.hop_length <= 0:
        raise invalid_config("hop_length must be positive", slice_id=slice_id, param="hop_length")
    if config.hop_length > config.frame_length:
        raise invalid_config("hop_length must not exceed frame_length", slice_id=slice_id, param="hop_length")
    samples = np.asarray(audio)
    if samples.ndim != 1:
        raise numeric_error("audio must be a one-dimensional PCM array", slice_id=slice_id, band_index=0, frame_index=0)
    if not np.issubdtype(samples.dtype, np.integer):
        if not np.all(np.isfinite(samples)):
            first = int(np.flatnonzero(~np.isfinite(samples))[0])
            raise numeric_error(
                "audio contains NaN or Inf",
                slice_id=slice_id,
                band_index=0,
                frame_index=max(0, first // config.hop_length),
            )
    n_samples = int(samples.size)
    if n_samples <= 0:
        raise invalid_config("audio slice is empty", slice_id=slice_id, param="n_samples")
    total_frames = frame_count(n_samples, config.hop_length)
    indices = np.arange(total_frames, dtype=np.int64) if frame_indices is None else np.asarray(list(frame_indices), dtype=np.int64)
    if indices.ndim != 1:
        raise numeric_error("frame_indices must be one-dimensional", slice_id=slice_id, band_index=0, frame_index=0)
    if len(indices) and (indices.min() < 0 or indices.max() >= total_frames):
        bad = int(indices[(indices < 0) | (indices >= total_frames)][0])
        raise numeric_error(
            "frame index is outside the available frame range",
            slice_id=slice_id,
            band_index=0,
            frame_index=max(0, bad),
        )
    window = hann_window(config.frame_length)
    spectra = np.empty((len(indices), config.frame_length // 2 + 1), dtype=np.complex128)
    starts = indices * config.hop_length
    valid_lengths = np.empty(len(indices), dtype=np.int64)
    padded = np.empty(len(indices), dtype=bool)
    source = samples.astype(np.float64, copy=False)
    if np.issubdtype(samples.dtype, np.integer):
        source = source / 32768.0
    for row, (frame_index, start) in enumerate(zip(indices.tolist(), starts.tolist())):
        stop = min(n_samples, start + config.frame_length)
        valid = max(0, stop - start)
        frame = np.zeros(config.frame_length, dtype=np.float64)
        frame[:valid] = source[start:stop]
        spectra[row] = np.fft.rfft(window * frame)
        valid_lengths[row] = valid
        padded[row] = start + config.frame_length > n_samples
    digest = stft_digest(spectra, indices, starts, valid_lengths, padded, config, n_samples)
    return StftResult(
        spectra=spectra,
        frame_indices=indices,
        frame_starts=starts,
        valid_lengths=valid_lengths,
        padded=padded,
        window=window,
        n_samples=n_samples,
        digest=digest,
    )


def stft_digest(
    spectra: np.ndarray,
    frame_indices: np.ndarray,
    frame_starts: np.ndarray,
    valid_lengths: np.ndarray,
    padded: np.ndarray,
    config: SeparationConfig,
    n_samples: int,
) -> str:
    return digest_json(
        {
            "spectra": digest_array(spectra),
            "frame_indices": digest_array(frame_indices),
            "frame_starts": digest_array(frame_starts),
            "valid_lengths": digest_array(valid_lengths),
            "padded": digest_array(padded),
            "config": config.stage_fingerprint("stft"),
            "n_samples": int(n_samples),
        }
    )


def stft_input_fingerprint(slice_truth: Any, config: SeparationConfig) -> str:
    return digest_json(
        {
            "slice_sha256": slice_truth.sha256,
            "n_samples": int(slice_truth.n_samples),
            "sample_rate": int(slice_truth.sample_rate),
            "stage_config": config.stage_fingerprint("stft"),
        }
    )


def block_stft_digest(result_spectra: np.ndarray, starts: np.ndarray, padded: np.ndarray) -> str:
    return digest_json(
        {
            "spectra": digest_array(result_spectra),
            "starts": digest_array(starts),
            "padded": digest_array(padded),
        }
    )

