"""Deterministic spectral-envelope and harmonic-structure feature extraction."""

from __future__ import annotations

import math
from typing import Any

import numpy as np

from .config import Config
from .errors import ToneLabError
from .models import Shard
from .util import canonical_json_bytes, sha256_bytes


def _round(value: float, digits: int) -> float:
    return float(round(float(value), digits))


def _pitch_at(shard: Shard, normalized_time: float) -> float:
    times = np.asarray([point[0] for point in shard.pitch_curve], dtype=np.float64)
    values = np.asarray([point[1] for point in shard.pitch_curve], dtype=np.float64)
    if times.size == 1:
        return float(values[0])
    return float(np.interp(normalized_time, times, values))


def extract_features(shard: Shard, config: Config) -> dict[str, Any]:
    settings = config.data["features"]
    frame_length = int(settings["frame_length"])
    hop_length = int(settings["hop_length"])
    fft_size = int(settings["fft_size"])
    envelope_bins = int(settings["envelope_bins"])
    smoothing_bins = int(settings["envelope_smoothing_bins"])
    harmonic_count = int(settings["harmonic_count"])
    digits = int(settings["round_digits"])
    sample_count = shard.sample_count
    if sample_count == 0:
        raise ToneLabError(
            "E_NUMERIC",
            "cannot extract features from an empty shard",
            shard_id=shard.shard_id,
            position={"kind": "samples", "index": 0},
        )
    samples = shard.pcm.astype(np.float64) / 32768.0
    frame_starts = list(range(0, sample_count, hop_length))
    truncated_frames = 0
    frames: list[dict[str, Any]] = []
    nyquist = shard.sample_rate / 2.0
    envelope_freqs = np.geomspace(max(20.0, nyquist / max(envelope_bins, 1)), nyquist, envelope_bins)
    for frame_index, start in enumerate(frame_starts):
        end = min(start + frame_length, sample_count)
        if end <= start:
            break
        length = end - start
        if length < frame_length:
            truncated_frames += 1
        segment = samples[start:end]
        if length == 1:
            window = np.ones(1, dtype=np.float64)
        else:
            window = np.hanning(length)
        spectrum = np.fft.rfft(segment * window, n=fft_size)
        magnitude = np.abs(spectrum)
        if smoothing_bins > 1:
            kernel = np.ones(smoothing_bins, dtype=np.float64) / float(smoothing_bins)
            smoothed = np.convolve(magnitude, kernel, mode="same")
        else:
            smoothed = magnitude
        freqs = np.fft.rfftfreq(fft_size, d=1.0 / shard.sample_rate)
        envelope = np.interp(envelope_freqs, freqs, smoothed)
        envelope_sum = float(np.sum(envelope))
        if envelope_sum > 0:
            envelope_norm = envelope / envelope_sum
        else:
            envelope_norm = np.zeros_like(envelope)

        harmonic_freqs = np.arange(1, harmonic_count + 1, dtype=np.float64) * shard.reference_f0_mean
        valid = harmonic_freqs <= nyquist
        harmonic_values = np.zeros(harmonic_count, dtype=np.float64)
        if np.any(valid):
            harmonic_values[valid] = np.interp(harmonic_freqs[valid], freqs, smoothed)
        harmonic_sum = float(np.sum(harmonic_values))
        if harmonic_sum > 0:
            harmonic_norm = harmonic_values / harmonic_sum
        else:
            harmonic_norm = np.zeros_like(harmonic_values)

        power = magnitude * magnitude
        power_sum = float(np.sum(power))
        if power_sum > 0:
            centroid = float(np.sum(freqs * power) / power_sum)
            cumulative = np.cumsum(power)
            rolloff_index = int(np.searchsorted(cumulative, 0.85 * cumulative[-1]))
            rolloff_index = min(rolloff_index, len(freqs) - 1)
            rolloff_hz = float(freqs[rolloff_index])
        else:
            centroid = 0.0
            rolloff_hz = 0.0
        harmonic_band = np.zeros_like(power, dtype=bool)
        for frequency in harmonic_freqs[valid]:
            harmonic_band |= np.abs(freqs - frequency) <= 20.0
        harmonic_ratio = float(np.sum(power[harmonic_band]) / power_sum) if power_sum > 0 else 0.0
        even_energy = sum(
            float(np.sum(power[np.abs(freqs - k * shard.reference_f0_mean) <= 20.0]))
            for k in range(2, harmonic_count + 1, 2)
            if k * shard.reference_f0_mean <= nyquist
        )
        odd_energy = sum(
            float(np.sum(power[np.abs(freqs - k * shard.reference_f0_mean) <= 20.0]))
            for k in range(1, harmonic_count + 1, 2)
            if k * shard.reference_f0_mean <= nyquist
        )
        odd_even_ratio = float(odd_energy / even_energy) if even_energy > 0 else 0.0
        rms = float(math.sqrt(float(np.mean(segment * segment))))
        center_ms = (start + length / 2.0) * 1000.0 / shard.sample_rate
        frames.append(
            {
                "index": frame_index,
                "start_sample": start,
                "end_sample": end,
                "length": length,
                "truncated": length < frame_length,
                "center_ms": _round(center_ms, 9),
                "rms": _round(rms, digits),
                "spectral_envelope": [_round(value, digits) for value in envelope_norm],
                "harmonic_amplitudes": [_round(value, digits) for value in harmonic_norm],
                "spectral_centroid_hz": _round(centroid, digits),
                "spectral_rolloff_hz": _round(rolloff_hz, digits),
                "harmonic_ratio": _round(harmonic_ratio, digits),
                "odd_even_ratio": _round(odd_even_ratio, digits),
            }
        )
    artifact = {
        "format": "tonelab.features",
        "schema": 1,
        "shard_id": shard.shard_id,
        "timbre_id": shard.timbre_id,
        "structure_version": shard.structure_version,
        "pcm_sha256": shard.pcm_sha256,
        "sample_count": sample_count,
        "sample_rate": shard.sample_rate,
        "reference_f0_mean": _round(shard.reference_f0_mean, digits),
        "frame_length": frame_length,
        "hop_length": hop_length,
        "fft_size": fft_size,
        "requested_frame_count": len(frame_starts),
        "emitted_frame_count": len(frames),
        "truncated_frames": truncated_frames,
        "envelope_freqs_hz": [_round(value, digits) for value in envelope_freqs],
        "frames": frames,
    }
    artifact["artifact_sha256"] = sha256_bytes(canonical_json_bytes(artifact))
    return artifact

