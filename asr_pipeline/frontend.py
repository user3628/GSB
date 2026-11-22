"""Deterministic framing, Hanning windowing and band/marker featureization."""

import os

import numpy as np

from .hashing import sha256_bytes
from .vocabulary import VOCABULARY

EPSILON = 1e-10


def frame_signal(samples, frame_config):
    """Build frames with explicit sample offsets and deterministic zero padding."""
    frame_config.validate()
    length = frame_config.frame_length
    hop = frame_config.hop_length
    total = len(samples)
    if total == 0:
        return np.empty((0, length), dtype=np.float64), np.empty(0, dtype=np.int64), []
    frame_count = 1 + int(np.ceil(max(0, total - length) / hop))
    frames = np.zeros((frame_count, length), dtype=np.float64)
    offsets = np.zeros(frame_count, dtype=np.int64)
    padded_flags = []
    for index in range(frame_count):
        start = index * hop
        offsets[index] = start
        end = min(start + length, total)
        frames[index, : end - start] = samples[start:end].astype(np.float64)
        padded_flags.append(end - start < length)
    return frames, offsets, padded_flags


def _band_power(power, count):
    usable = power.shape[1] - 1
    edges = np.linspace(0, usable, count + 1, dtype=np.int64)
    result = np.zeros((power.shape[0], count), dtype=np.float64)
    for index in range(count):
        lo = int(edges[index])
        hi = max(int(edges[index + 1]), lo + 1)
        result[:, index] = np.mean(power[:, lo:hi], axis=1)
    return result


def _marker_power(power, marker_bins):
    """Marker columns align with FFT bins 2..2+marker_bins-1."""
    result = np.zeros((power.shape[0], marker_bins), dtype=np.float64)
    for index in range(marker_bins):
        fft_bin = index + 2
        result[:, index] = power[:, fft_bin]
    return result


def featureize(samples, frame_config):
    """Linear/log band energy, marker-tone energy and spectral flatness."""
    frames, offsets, padded_flags = frame_signal(samples, frame_config)
    columns = frame_config.feature_columns
    if len(frames) == 0:
        return np.empty((0, columns), dtype=np.float64), offsets, padded_flags
    window = np.hanning(frame_config.frame_length).astype(np.float64)
    spectrum = np.fft.rfft(frames * window, axis=1)
    power = (spectrum.real**2 + spectrum.imag**2) / frame_config.frame_length
    band_energy = _band_power(power, frame_config.bands)
    marker_energy = _marker_power(power, frame_config.marker_bins)
    log_energy = 10.0 * np.log10(band_energy + EPSILON)
    geometric = np.exp(np.mean(np.log(power[:, 1:] + EPSILON), axis=1))
    arithmetic = np.mean(power[:, 1:], axis=1)
    flatness = np.clip(geometric / (arithmetic + EPSILON), 0.0, 1.0)
    features = np.concatenate(
        [
            band_energy,
            log_energy,
            marker_energy,
            flatness.reshape(-1, 1),
        ],
        axis=1,
    )
    return features, offsets, padded_flags


def feature_bytes(features):
    return np.ascontiguousarray(features, dtype="<f4").tobytes()


def write_feature_blob(path, features):
    with open(path, "wb") as handle:
        handle.write(feature_bytes(features))


def read_feature_blob(path, frame_config):
    raw = open(path, "rb").read()
    return np.frombuffer(raw, dtype="<f4").reshape(
        -1, frame_config.feature_columns
    ).astype(np.float64)


def build_frontend_artifact(samples, frame_config):
    features, offsets, padded_flags = featureize(samples, frame_config)
    blob = feature_bytes(features)
    log_columns = features[:, frame_config.bands : frame_config.bands * 2]
    band_means = np.round(np.mean(log_columns, axis=0), 6) if len(features) else []
    return {
        "feature_digest": sha256_bytes(blob),
        "frame_count": int(len(features)),
        "bands": frame_config.bands,
        "marker_bins": frame_config.marker_bins,
        "frame_length_samples": frame_config.frame_length,
        "hop_length_samples": frame_config.hop_length,
        "offset_samples": [int(value) for value in offsets],
        "padded_frames": [bool(value) for value in padded_flags],
        "band_mean_log_energy": [float(value) for value in band_means],
        "vocabulary_size": len(VOCABULARY),
    }
