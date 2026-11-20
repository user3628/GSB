"""Deterministic iterative spectral magnitude-mask separation.

No learned model is involved.  Given K speakers we maintain K complex
estimates of the mixture STFT and iteratively refine soft magnitude masks:

1. Deterministic init: seeded low-frequency spectral basis masks give each
   speaker a distinct but reproducible starting region, and the initial
   complex estimate is ``mask * mixture``.
2. Each iteration re-normalizes the source magnitudes into a soft partition
   (with a fixed floor) and rebuilds complex estimates from the mixture
   phase.  Convergence is therefore a pure function of the input, the
   speaker count, the iteration count and the local content seed.

Separated waveforms are quantized to int16 so that the persisted result is
sample-identical across runs and platforms.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np

from .audio import (FrameClock, frame_signal, hann_window, istft,
                    quantize_int16, stft)


def content_seed(content_fingerprint: str) -> int:
    digest = hashlib.sha256(content_fingerprint.encode("utf-8")).hexdigest()
    return int(digest[: 8], 16)


def _initial_masks(n_frames: int, n_bins: int, n_speakers: int,
                   seed: int) -> np.ndarray:
    """Build deterministic soft masks spanning the frequency axis."""
    rng = np.random.default_rng(seed)
    # Per-speaker frequency basis drawn from a local, seeded RNG.
    basis = rng.uniform(0.35, 1.0, size=(n_speakers, n_bins))
    # Slowly varying deterministic temporal modulation per speaker.
    phase0 = rng.uniform(0.0, 2 * np.pi, size=n_speakers)
    t = np.arange(n_frames, dtype=np.float64)
    modulation = 0.5 + 0.5 * np.cos(
        2 * np.pi * t[None, :] / max(n_frames, 1) + phase0[:, None])
    raw = basis[:, None, :] * modulation[:, :, None]
    raw = raw + 1e-6
    return raw / raw.sum(axis=0, keepdims=True)


def separate_mixture(samples: np.ndarray, n_speakers: int,
                     frame_size: int, hop_size: int,
                     iterations: int, mask_floor: float,
                     content_fingerprint: str
                     ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Run iterative magnitude-mask separation.

    Returns
    -------
    separated_int16 : (K, n_samples) int16 quantized source waveforms
    frame_energy    : (K, n_frames) float64 per-frame RMS energy
    final_masks     : (K, n_frames, n_bins) float64 soft masks
    """
    if n_speakers < 1:
        raise ValueError("n_speakers must be >= 1")
    seed = content_seed(content_fingerprint)
    spectrum = stft(samples, frame_size, hop_size)
    magnitude = np.abs(spectrum)
    n_frames, n_bins = magnitude.shape

    masks = _initial_masks(n_frames, n_bins, n_speakers, seed)
    estimates = masks * spectrum[None, :, :]

    for _ in range(iterations):
        est_mag = np.abs(estimates) + mask_floor * (magnitude[None, :, :] + 1e-12)
        denom = est_mag.sum(axis=0, keepdims=True)
        masks = est_mag / denom
        estimates = masks * spectrum[None, :, :]

    separated = np.empty((n_speakers, len(samples)), dtype=np.float64)
    frame_energy = np.empty((n_speakers, n_frames), dtype=np.float64)
    window = hann_window(frame_size)
    for k in range(n_speakers):
        separated[k] = istft(estimates[k], frame_size, hop_size, len(samples))
        frames, _ = frame_signal(separated[k], frame_size, hop_size)
        frame_energy[k] = np.sqrt(
            np.mean((frames * window[None, :]) ** 2, axis=1))

    separated_int16 = quantize_int16(separated)
    return separated_int16, frame_energy, masks


def separated_bytes(separated_int16: np.ndarray) -> bytes:
    """Canonical little-endian byte payload for the quantized waveforms."""
    return separated_int16.astype("<i2").tobytes()


def write_separated(path: str, separated_int16: np.ndarray) -> None:
    with open(path, "wb") as fh:
        fh.write(separated_bytes(separated_int16))


def read_separated(path: str, n_speakers: int, n_samples: int) -> np.ndarray:
    with open(path, "rb") as fh:
        raw = fh.read()
    arr = np.frombuffer(raw, dtype="<i2")
    return arr.reshape(n_speakers, n_samples).copy()
