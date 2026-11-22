"""Framing, amplitude spectra, and deterministic 2-D peak detection."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .config import ShardConfig
from .errors import E_NO_PEAK, FingerprintError
from .wavio import WavData


@dataclass(frozen=True)
class Peak:
    frame: int
    freq: int
    amplitude: float

    def as_dict(self) -> dict[str, int | float]:
        return {"frame": self.frame, "freq": self.freq, "amplitude": self.amplitude}


@dataclass(frozen=True)
class PeakResult:
    peaks: tuple[Peak, ...]
    n_frames: int
    n_freq_bins: int
    sample_count: int
    frame_length: int
    hop_length: int


def _local_maxima_2d(magnitudes: np.ndarray, neighborhood: int, min_freq_bin: int) -> np.ndarray:
    n_frames, n_bins = magnitudes.shape
    candidate = np.zeros_like(magnitudes, dtype=bool)
    candidate[:, min_freq_bin:] = True
    for delta_t in range(-neighborhood, neighborhood + 1):
        for delta_f in range(-neighborhood, neighborhood + 1):
            if delta_t == 0 and delta_f == 0:
                continue
            dst_t0 = max(0, -delta_t)
            dst_t1 = n_frames - max(0, delta_t)
            dst_f0 = max(min_freq_bin, min_freq_bin - delta_f)
            dst_f1 = n_bins - max(0, delta_f)
            if dst_t0 >= dst_t1 or dst_f0 >= dst_f1:
                continue
            src_t0 = dst_t0 + delta_t
            src_t1 = dst_t1 + delta_t
            src_f0 = dst_f0 + delta_f
            src_f1 = dst_f1 + delta_f
            center = magnitudes[dst_t0:dst_t1, dst_f0:dst_f1]
            neighbour = magnitudes[src_t0:src_t1, src_f0:src_f1]
            center_flat = (
                np.arange(dst_t0, dst_t1, dtype=np.int64)[:, None] * n_bins
                + np.arange(dst_f0, dst_f1, dtype=np.int64)[None, :]
            )
            neighbour_flat = center_flat + int(delta_t) * n_bins + int(delta_f)
            # Equal-amplitude neighbours are resolved toward the lexicographically
            # earlier (frame, frequency-bin) index, never toward iteration order.
            candidate[dst_t0:dst_t1, dst_f0:dst_f1] &= (center > neighbour) | (
                (center == neighbour) & (center_flat < neighbour_flat)
            )
    return candidate


def detect_peaks(wav: WavData, config: ShardConfig) -> PeakResult:
    frame_length = config.frame_length
    hop_length = config.hop_length
    if wav.sample_count < frame_length:
        raise FingerprintError(
            code=E_NO_PEAK,
            message="shard is shorter than one analysis frame",
            shard_id=config.shard_id,
            stage="peaks",
            details={"sample_count": wav.sample_count, "frame_length": frame_length},
        )
    n_frames = 1 + (wav.sample_count - frame_length) // hop_length
    n_freq_bins = frame_length // 2 + 1
    frame_offsets = np.arange(n_frames, dtype=np.int64) * hop_length
    indices = frame_offsets[:, None] + np.arange(frame_length, dtype=np.int64)[None, :]
    frames = wav.samples[indices]
    window = np.hanning(frame_length)
    magnitudes = np.abs(np.fft.rfft(frames * window[None, :], axis=1))

    candidates = _local_maxima_2d(magnitudes, config.neighborhood, config.min_freq_bin)
    candidates &= magnitudes >= config.peak_amplitude_floor
    all_positions = np.argwhere(candidates)

    selected: list[Peak] = []
    for frame in range(n_frames):
        frame_rows = all_positions[all_positions[:, 0] == frame]
        if frame_rows.size == 0:
            continue
        frequencies = frame_rows[:, 1]
        amplitudes = magnitudes[frame, frequencies]
        order = np.lexsort((frequencies, -amplitudes))
        for position in order[: config.max_peaks_per_frame]:
            freq = int(frequencies[position])
            selected.append(Peak(frame=frame, freq=freq, amplitude=float(amplitudes[position])))

    if not selected:
        nonzero = int(np.count_nonzero(wav.samples))
        raise FingerprintError(
            code=E_NO_PEAK,
            message="no spectral peak satisfies the configured local-maximum and amplitude rules",
            shard_id=config.shard_id,
            stage="peaks",
            details={
                "sample_count": wav.sample_count,
                "nonzero_samples": nonzero,
                "n_frames": n_frames,
                "neighborhood": config.neighborhood,
                "peak_amplitude_floor": config.peak_amplitude_floor,
            },
        )
    selected.sort(key=lambda peak: (peak.frame, peak.freq))
    return PeakResult(
        peaks=tuple(selected),
        n_frames=n_frames,
        n_freq_bins=n_freq_bins,
        sample_count=wav.sample_count,
        frame_length=frame_length,
        hop_length=hop_length,
    )


def peaks_artifact(
    result: PeakResult,
    config: ShardConfig,
    sample_digest: str,
    stage_version: str,
) -> dict:
    return {
        "schema": "audiofp.peaks.v1",
        "shard_id": config.shard_id,
        "stage_version": stage_version,
        "sample_digest": sample_digest,
        "frame_length": result.frame_length,
        "hop_length": result.hop_length,
        "n_frames": result.n_frames,
        "n_freq_bins": result.n_freq_bins,
        "sample_count": result.sample_count,
        "window": "hanning",
        "spectrum": "amplitude-rfft",
        "local_maximum": "2d-chebyshev",
        "neighborhood": config.neighborhood,
        "min_freq_bin": config.min_freq_bin,
        "peak_amplitude_floor": config.peak_amplitude_floor,
        "max_peaks_per_frame": config.max_peaks_per_frame,
        "peak_count": len(result.peaks),
        "peaks": [peak.as_dict() for peak in result.peaks],
    }

