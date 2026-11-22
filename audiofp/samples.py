"""Deterministic PCM WAV and reference-pair sample generation."""

from __future__ import annotations

import json
import math
import struct
import tempfile
from dataclasses import replace
from pathlib import Path
from typing import Any

import numpy as np

from .config import ShardConfig, load_manifest
from .dsp import detect_peaks
from .errors import FingerprintError
from .jsonutil import atomic_write_bytes
from .pairing import generate_fingerprint_pairs
from .wavio import read_wav


SAMPLE_RATE = 16_000


def write_pcm16_wav(path: str | Path, samples: np.ndarray) -> bytes:
    mono = np.asarray(samples, dtype=np.float64)
    pcm = np.clip(np.rint(mono * 32767.0), -32768, 32767).astype("<i2").tobytes()
    header = (
        b"RIFF"
        + struct.pack("<I", 36 + len(pcm))
        + b"WAVE"
        + b"fmt "
        + struct.pack("<IHHIIHH", 16, 1, 1, SAMPLE_RATE, SAMPLE_RATE * 2, 2, 16)
        + b"data"
        + struct.pack("<I", len(pcm))
    )
    data = header + pcm
    atomic_write_bytes(path, data)
    return data


def _fade_envelope(count: int, attack: int = 160, release: int = 320) -> np.ndarray:
    envelope = np.ones(count, dtype=np.float64)
    attack = min(attack, count)
    release = min(release, count)
    if attack:
        envelope[:attack] *= np.linspace(0.0, 1.0, attack, endpoint=False)
    if release:
        envelope[-release:] *= np.linspace(1.0, 0.0, release, endpoint=False)
    return envelope


def _stable_segment(base_freq: float, count: int) -> np.ndarray:
    t = np.arange(count, dtype=np.float64) / SAMPLE_RATE
    envelope = _fade_envelope(count)
    signal = (
        0.24 * np.sin(2.0 * math.pi * base_freq * t)
        + 0.15 * np.sin(2.0 * math.pi * base_freq * 1.5 * t + 0.2)
        + 0.10 * np.sin(2.0 * math.pi * base_freq * 2.0 * t + 0.4)
    )
    return signal * envelope


def _rhythm_segment(base_freq: float, count: int, period_seconds: float) -> np.ndarray:
    t = np.arange(count, dtype=np.float64) / SAMPLE_RATE
    signal = np.zeros(count, dtype=np.float64)
    period = max(1, int(round(period_seconds * SAMPLE_RATE)))
    burst_length = int(0.15 * SAMPLE_RATE)
    burst_t = np.arange(burst_length, dtype=np.float64) / SAMPLE_RATE
    burst_envelope = np.exp(-burst_t * 18.0)
    for start in range(0, count, period):
        length = min(burst_length, count - start)
        if length <= 0:
            continue
        local_t = burst_t[:length]
        burst = (
            0.28 * np.sin(2.0 * math.pi * base_freq * local_t)
            + 0.16 * np.sin(2.0 * math.pi * base_freq * 1.5 * local_t + 0.3)
            + 0.10 * np.sin(2.0 * math.pi * base_freq * 2.0 * local_t + 0.1)
        )
        signal[start : start + length] += burst * burst_envelope[:length]
    return signal


def _synthesize(seed: int, base_freq: float, period_seconds: float, with_noise: bool) -> np.ndarray:
    rng = np.random.default_rng(seed)
    total = int(2.0 * SAMPLE_RATE)
    stable_count = int(0.65 * SAMPLE_RATE)
    noise_count = int(0.60 * SAMPLE_RATE)
    rhythm_count = total - stable_count - noise_count
    signal = np.zeros(total, dtype=np.float64)
    signal[:stable_count] += _stable_segment(base_freq, stable_count)
    if with_noise:
        noise = rng.normal(0.0, 0.010, noise_count)
        # A deterministic low-level tonal bed keeps the noise section a genuine
        # mixed musical segment while remaining below the peak floor.
        t = np.arange(noise_count, dtype=np.float64) / SAMPLE_RATE
        bed = 0.020 * np.sin(2.0 * math.pi * base_freq * 0.5 * t)
        signal[stable_count : stable_count + noise_count] += noise + bed
    signal[stable_count + noise_count :] += _rhythm_segment(base_freq, rhythm_count, period_seconds)
    return np.clip(signal, -0.98, 0.98)


def _reference_entries(clean_path: Path, config: ShardConfig) -> list[dict[str, int]]:
    try:
        wav = read_wav(str(clean_path), config.shard_id)
        peaks = detect_peaks(wav, config)
    except FingerprintError as error:
        if error.code == "E_NO_PEAK":
            return []
        raise
    pairs = generate_fingerprint_pairs(peaks.peaks, config)
    return [
        {
            "anchor_frame": pair.anchor_frame,
            "anchor_freq": pair.anchor_freq,
            "target_frame": pair.target_frame,
            "target_freq": pair.target_freq,
            "dt": pair.dt,
        }
        for pair in pairs.pairs
    ]


def generate_samples(samples_dir: str | Path, seed: int = 20261123) -> dict[str, Any]:
    directory = Path(samples_dir).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    variants = [
        ("music_a", 220.00, 0.100, 1001),
        ("music_b", 261.63, 0.125, 2002),
        ("music_c", 329.63, 0.080, 3003),
    ]
    shard_specs: list[dict[str, Any]] = []
    for shard_id, base_freq, period, offset in variants:
        wav_name = f"{shard_id}.wav"
        signal = _synthesize(seed + offset, base_freq, period, with_noise=True)
        write_pcm16_wav(directory / wav_name, signal)
        shard_specs.append(
            {
                "shard_id": shard_id,
                "wav": wav_name,
                "base_freq": base_freq,
                "period": period,
                "seed": seed + offset,
                "reference": "reference_pairs.json",
            }
        )

    write_pcm16_wav(directory / "silence.wav", np.zeros(2 * SAMPLE_RATE, dtype=np.float64))
    shard_specs.append(
        {
            "shard_id": "silence",
            "wav": "silence.wav",
            "base_freq": None,
            "period": None,
            "seed": seed + 4004,
            "reference": "reference_pairs.json",
        }
    )

    collision_signal = _synthesize(seed + 5005, 293.66, 0.110, with_noise=True)
    write_pcm16_wav(directory / "bucket_collision.wav", collision_signal)
    shard_specs.append(
        {
            "shard_id": "bucket_collision",
            "wav": "bucket_collision.wav",
            "base_freq": 293.66,
            "period": 0.110,
            "seed": seed + 5005,
            "reference": "reference_pairs.json",
            "bucket_width": 1,
            "max_collision_rate": 0.20,
        }
    )

    defaults: dict[str, Any] = {
        "frame_length": 1024,
        "hop_length": 256,
        "neighborhood": 2,
        "min_freq_bin": 2,
        "time_delta_min": 1,
        "time_delta_max": 12,
        "freq_delta_min": 2,
        "freq_delta_max": 80,
        "quantization_bits": 8,
        "max_peaks_per_frame": 4,
        "max_pairs": 3000,
        "targets_per_anchor": 3,
        "bucket_width": 4096,
        "expected_false_match_rate": 0.80,
        "confidence_threshold": 0.70,
        "max_collision_rate": 0.60,
        "peak_amplitude_floor": 0.5,
        "hash_seed": seed,
        "pairing_policy": "bounded_no_wrap",
    }
    manifest = {
        "schema": "audiofp.manifest.v1",
        "seed": seed,
        "reference_pairs": "reference_pairs.json",
        "defaults": defaults,
        "shards": [
            {key: value for key, value in spec.items() if key not in ("base_freq", "period", "seed")}
            for spec in shard_specs
        ],
    }
    manifest_path = directory / "fingerprint_config.json"
    atomic_write_bytes(
        manifest_path,
        (json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8"),
    )

    loaded = load_manifest(manifest_path)
    references: dict[str, list[dict[str, int]]] = {}
    with tempfile.TemporaryDirectory(prefix="audiofp-reference-") as temporary:
        temp_dir = Path(temporary)
        for config, spec in zip(loaded.shards, sorted(shard_specs, key=lambda item: item["shard_id"])):
            if spec["shard_id"] == "silence":
                references[config.shard_id] = []
                continue
            clean_signal = _synthesize(spec["seed"], spec["base_freq"], spec["period"], with_noise=False)
            clean_path = temp_dir / f"{config.shard_id}.wav"
            write_pcm16_wav(clean_path, clean_signal)
            clean_config = replace(config, wav_path=str(clean_path))
            references[config.shard_id] = _reference_entries(clean_path, clean_config)

    reference_payload = {
        "schema": "audiofp.reference.v1",
        "seed": seed,
        "source": "clean stable-tone and rhythm stems; noise stem omitted",
        "shards": references,
    }
    atomic_write_bytes(
        directory / "reference_pairs.json",
        (json.dumps(reference_payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8"),
    )
    return {
        "schema": "audiofp.generated-samples.v1",
        "samples_dir": str(directory),
        "manifest": str(manifest_path),
        "reference_pairs": str(directory / "reference_pairs.json"),
        "seed": seed,
        "shard_ids": [spec["shard_id"] for spec in sorted(shard_specs, key=lambda item: item["shard_id"])],
        "reference_pair_counts": {
            shard_id: len(entries) for shard_id, entries in sorted(references.items())
        },
    }

