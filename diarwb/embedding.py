"""Speaker embeddings from mel-band energy statistics + fixed hash projection.

The embedding is deliberately model-free:

* frame log-mel energies are computed from a channel's waveform;
* per-band mean/std/min/max and band temporal drift are concatenated into a
  statistics vector;
* a fixed, seed-derived hash projection (orthogonalized with deterministic
  QR decomposition) maps the statistics into a compact embedding.

The final vector is L2 normalized and quantized to signed fixed point so the
serialized embedding is byte-identical for identical input and configuration.
"""
from __future__ import annotations

import struct
from typing import Dict, List, Tuple

import numpy as np

from .audio import mel_filterbank, stft

EMBED_Q = 1 << 14  # Q14 fixed point for normalized embedding components


def hash_projection(in_dim: int, out_dim: int, seed: int) -> np.ndarray:
    """Deterministic orthogonal-ish projection from a seeded hash matrix."""
    rng = np.random.default_rng(seed)
    matrix = rng.standard_normal((out_dim, in_dim))
    q, _ = np.linalg.qr(matrix.T, mode="reduced")
    proj = q.T  # (out_dim, in_dim)
    norms = np.sqrt((proj ** 2).sum(axis=1, keepdims=True))
    return proj / np.maximum(norms, 1e-12)


def frame_mel_energies(samples: np.ndarray, sample_rate: int,
                       frame_size: int, hop_size: int, n_mels: int
                       ) -> np.ndarray:
    """Return (n_frames, n_mels) log mel-band energies."""
    spectrum = stft(samples, frame_size, hop_size)
    power = (np.abs(spectrum) ** 2) / frame_size
    bank = mel_filterbank(sample_rate, frame_size, n_mels)
    mel_e = power @ bank.T
    return np.log(mel_e + 1e-8)


def _statistics(mel: np.ndarray) -> np.ndarray:
    active = mel[mel > np.log(1e-8) + 1.0]
    if active.size == 0:
        active = mel
    floor = float(np.percentile(active, 20))
    voiced = mel > floor
    counts = np.maximum(voiced.sum(axis=0), 1)
    mean = (mel * voiced).sum(axis=0) / counts
    var = (((mel - mean[None, :]) ** 2) * voiced).sum(axis=0) / counts
    std = np.sqrt(var)
    band_min = np.where(voiced, mel, np.inf).min(axis=0)
    band_max = np.where(voiced, mel, -np.inf).max(axis=0)
    band_min = np.where(np.isinf(band_min), mean, band_min)
    band_max = np.where(np.isinf(band_max), mean, band_max)
    if mel.shape[0] > 1:
        drift = mel[-1] - mel[0]
    else:
        drift = np.zeros(mel.shape[1])
    return np.concatenate([mean, std, band_min, band_max, drift])


def embed_channel(samples: np.ndarray, sample_rate: int, frame_size: int,
                  hop_size: int, n_mels: int, embedding_dim: int,
                  seed: int) -> np.ndarray:
    """Compute and L2-normalize a float embedding for one channel."""
    mel = frame_mel_energies(samples.astype(np.float64) / 32768.0,
                             sample_rate, frame_size, hop_size, n_mels)
    stats = _statistics(mel)
    proj = hash_projection(stats.shape[0], embedding_dim, seed)
    vec = proj @ stats
    norm = np.sqrt((vec ** 2).sum())
    if norm > 1e-12:
        vec = vec / norm
    return vec


def quantize_embedding(vec: np.ndarray) -> np.ndarray:
    q = np.round(vec * EMBED_Q).astype(np.int16)
    return q


def serialize_embedding(q: np.ndarray) -> bytes:
    return q.astype("<i2").tobytes()


def deserialize_embedding(raw: bytes) -> np.ndarray:
    return np.frombuffer(raw, dtype="<i2").copy()


def embedding_bytes(vec: np.ndarray) -> bytes:
    return serialize_embedding(quantize_embedding(vec))
