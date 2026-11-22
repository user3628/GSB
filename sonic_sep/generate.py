"""Deterministic synthetic source-mixture WAV generator."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import soundfile as sf

from .util import atomic_write_bytes, canonical_json_bytes, sha256_bytes


SAMPLE_RATE = 16_000
SECONDS = (2.5, 3.0, 3.5)
SOURCE_COUNTS = (2, 3, 2)
BASE_F0S = ((180.0, 420.0), (230.0, 315.0, 540.0), (150.0, 390.0))


def _source_wave(
    rng: np.random.Generator,
    n_samples: int,
    f0: float,
    amplitude: float,
    *,
    vibrato_hz: float,
    vibrato_depth: float,
    harmonics: tuple[float, ...],
) -> np.ndarray:
    t = np.arange(n_samples, dtype=np.float64) / SAMPLE_RATE
    vibrato = vibrato_depth * np.sin(2.0 * np.pi * vibrato_hz * t + rng.uniform(0.0, 2.0 * np.pi))
    phase = 2.0 * np.pi * f0 * t + 0.55 * np.sin(2.0 * np.pi * 0.8 * t + rng.uniform(0.0, 2.0 * np.pi))
    phase += 2.0 * np.pi * f0 * np.cumsum(vibrato) / SAMPLE_RATE
    wave = np.zeros(n_samples, dtype=np.float64)
    for harmonic, weight in enumerate(harmonics, start=1):
        wave += weight * np.sin(harmonic * phase + rng.uniform(0.0, 2.0 * np.pi))
    envelope = 0.68 + 0.22 * np.sin(2.0 * np.pi * (0.33 + 0.07 * rng.random()) * t + rng.uniform(0.0, 2 * np.pi))
    gate = np.ones_like(t)
    gate[: int(0.04 * n_samples)] = np.linspace(0.0, 1.0, int(0.04 * n_samples), endpoint=False)
    gate[-int(0.04 * n_samples) :] = np.linspace(1.0, 0.0, int(0.04 * n_samples), endpoint=False)
    wave *= envelope * gate
    peak = float(np.max(np.abs(wave))) or 1.0
    return amplitude * wave / peak


def generate_dataset(
    samples_dir: str | Path,
    *,
    seed: int = 20261123,
    slice_count: int | None = None,
    durations: Sequence[float] = SECONDS,
) -> Path:
    """Generate a fixed, repeatable WAV manifest and return its path."""
    output_dir = Path(samples_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    rng = np.random.Generator(np.random.PCG64(seed))
    requested = len(durations) if slice_count is None else int(slice_count)
    if requested < 3:
        raise ValueError("at least 3 slices are required")
    slices: list[dict[str, Any]] = []
    total_samples = 0
    for index in range(requested):
        duration = float(durations[index % len(durations)])
        n_samples = int(round(duration * SAMPLE_RATE))
        source_count = SOURCE_COUNTS[index % len(SOURCE_COUNTS)]
        source_rows: list[dict[str, Any]] = []
        mixture = np.zeros(n_samples, dtype=np.float64)
        for source_index in range(source_count):
            base = BASE_F0S[index % len(BASE_F0S)][source_index]
            jitter = float(rng.uniform(-2.5, 2.5))
            true_f0 = base + jitter
            half_width = float(rng.uniform(5.0, 14.0))
            visible = not (source_index == 2 and index % 2 == 1)
            amplitude = (0.42 if source_index == 0 else 0.32 if source_index == 1 else 0.20) * (
                1.0 if visible else 0.65
            )
            harmonics = (1.0, 0.32, 0.16) if source_index == 0 else (1.0, 0.22, 0.10)
            mixture += _source_wave(
                rng,
                n_samples,
                true_f0,
                amplitude,
                vibrato_hz=float(rng.uniform(1.7, 4.6)),
                vibrato_depth=float(rng.uniform(0.0015, 0.0045)),
                harmonics=harmonics,
            )
            source_rows.append(
                {
                    "source_id": source_index,
                    "true_f0_hz": round(true_f0, 6),
                    "f0_min_hz": round(true_f0 - half_width, 6),
                    "f0_max_hz": round(true_f0 + half_width, 6),
                    "visible": bool(visible),
                }
            )
        peak = float(np.max(np.abs(mixture))) or 1.0
        mixture = 0.82 * mixture / peak
        pcm = np.clip(np.rint(mixture * 32767.0), -32768, 32767).astype(np.int16)
        file_name = f"slice-{index + 1:03d}.wav"
        wav_path = output_dir / file_name
        sf.write(wav_path, pcm, SAMPLE_RATE, subtype="PCM_16", format="WAV")
        file_bytes = wav_path.read_bytes()
        slices.append(
            {
                "slice_id": f"slice-{index + 1:03d}",
                "file": file_name,
                "n_samples": n_samples,
                "duration_seconds": round(n_samples / SAMPLE_RATE, 6),
                "source_count": source_count,
                "sources": source_rows,
                "sha256": sha256_bytes(file_bytes),
            }
        )
        total_samples += n_samples
    manifest = {
        "schema_version": 1,
        "generator": "sonic_sep.generate",
        "seed": int(seed),
        "sample_rate": SAMPLE_RATE,
        "format": "WAV mono 16-bit PCM",
        "slice_count": len(slices),
        "total_samples": total_samples,
        "slices": slices,
    }
    manifest_path = output_dir / "manifest.json"
    atomic_write_bytes(manifest_path, canonical_json_bytes(manifest) + b"\n")
    return manifest_path


def load_manifest_json(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)

