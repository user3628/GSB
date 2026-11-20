"""Deterministic local WAV shard + JSON manifest generator.

The generator never touches a microphone or network.  Every shard is a sum of
speaker tracks built from seeded RNG streams: amplitude-modulated harmonic
voiced excitation plus colored noise, giving each speaker a distinct,
reproducible spectral signature.  Track envelopes define silence and
overlapping speech; the manifest records millisecond reference segments and
confidence flags derived from the same layout.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import numpy as np

from .audio import SAMPLE_RATE, write_wav

SPEAKER_FREQS = {1: (110.0, 132.0), 2: (165.0, 198.0), 3: (220.0, 264.0)}


def _colored_noise(n: int, rng: np.random.Generator) -> np.ndarray:
    white = rng.standard_normal(n)
    kernel = np.array([1.0, -0.65, 0.35])
    return np.convolve(white, kernel, mode="same")


def _speaker_track(n_samples: int, speaker: int,
                   rng: np.random.Generator) -> np.ndarray:
    t = np.arange(n_samples, dtype=np.float64) / SAMPLE_RATE
    f0, f1 = SPEAKER_FREQS[speaker]
    # Deterministic slow pitch contour per speaker.
    contour = f0 + (f1 - f0) * 0.5 * (1.0 + np.sin(2 * np.pi * t / (1.1 + 0.13 * speaker)))
    phase = 2 * np.pi * np.cumsum(contour) / SAMPLE_RATE
    harmonic = (np.sin(phase) + 0.5 * np.sin(2 * phase)
                + 0.25 * np.sin(3 * phase))
    syllable = 0.55 + 0.45 * np.sin(2 * np.pi * t * (3.2 + 0.3 * speaker)
                                   + rng.uniform(0, 2 * np.pi))
    syllable = np.clip(syllable, 0.05, 1.0)
    noise = 0.25 * _colored_noise(n_samples, rng)
    track = 0.7 * harmonic * syllable + noise
    return track.astype(np.float64)


@dataclass
class Interval:
    start_ms: int
    end_ms: int
    speaker: int


def _layout(n_speakers: int, rng: np.random.Generator,
            duration_ms: int) -> List[Interval]:
    """Fixed family of layouts covering silence and overlap regions."""
    intervals: List[Interval] = []
    if n_speakers == 2:
        intervals = [
            Interval(200, 1600, 1),
            Interval(1200, 2600, 2),   # overlap 1200-1600
            Interval(3000, 4400, 1),
            Interval(4200, 5600, 2),   # overlap 4200-4400
            Interval(6000, 7200, 1),
        ]
    else:
        intervals = [
            Interval(200, 1500, 1),
            Interval(1000, 2400, 2),   # overlap 1000-1500 (1,2)
            Interval(2000, 3200, 3),   # overlap 2000-2400 (2,3)
            Interval(3600, 4800, 1),
            Interval(4500, 5900, 2),   # overlap 4500-4800 (1,2)
            Interval(5400, 6800, 3),   # overlap 5400-5900 (2,3)
            Interval(7200, 8000, 1),
        ]
    jitter = int(rng.integers(-40, 41))
    if intervals:
        intervals = [Interval(max(0, iv.start_ms + jitter),
                              min(duration_ms, iv.end_ms + jitter),
                              iv.speaker) for iv in intervals]
    return [iv for iv in intervals if iv.end_ms > iv.start_ms]


def generate_shard(seed: int, n_speakers: int,
                   duration_ms: int = 8000) -> Tuple[np.ndarray, List[Interval]]:
    rng = np.random.default_rng(seed)
    n_samples = SAMPLE_RATE * duration_ms // 1000
    mix = np.zeros(n_samples, dtype=np.float64)
    intervals = _layout(n_speakers, rng, duration_ms)
    for speaker in range(1, n_speakers + 1):
        track = _speaker_track(n_samples, speaker, rng)
        envelope = np.zeros(n_samples, dtype=np.float64)
        for iv in intervals:
            if iv.speaker != speaker:
                continue
            s = iv.start_ms * SAMPLE_RATE // 1000
            e = iv.end_ms * SAMPLE_RATE // 1000
            ramp = max(1, SAMPLE_RATE // 200)
            env = np.ones(e - s, dtype=np.float64)
            env[:ramp] = np.linspace(0, 1, ramp)
            env[-ramp:] = np.linspace(1, 0, ramp)
            envelope[s:e] = np.maximum(envelope[s:e], env)
        mix += track * envelope
    peak = np.max(np.abs(mix))
    if peak > 0:
        mix = 0.9 * mix / peak
    return mix.astype(np.float64), intervals


def _confidence_for(iv: Interval, overlaps: List[Tuple[int, int]]) -> str:
    for o_start, o_end in overlaps:
        if iv.start_ms < o_end and o_start < iv.end_ms:
            return "low"
    return "high"


def build_demo_dataset(workspace_dir: str, base_seed: int = 20261120
                       ) -> Dict[str, Any]:
    """Create three shards covering 2 and 3 speakers plus their manifest."""
    shard_dir = os.path.join(workspace_dir, "shards")
    os.makedirs(shard_dir, exist_ok=True)
    plan = [("shard-000", 2, 8000), ("shard-001", 3, 8000),
            ("shard-002", 2, 8000)]
    manifest_shards: List[Dict[str, Any]] = []
    for index, (shard_id, n_speakers, duration_ms) in enumerate(plan):
        seed = base_seed + index * 101
        samples, intervals = generate_shard(seed, n_speakers, duration_ms)
        wav_name = f"{shard_id}.wav"
        write_wav(os.path.join(shard_dir, wav_name), samples)
        n_samples = len(samples)
        # Overlap spans: timestamps claimed by more than one speaker.
        marks = np.zeros(duration_ms, dtype=np.int16)
        for iv in intervals:
            marks[iv.start_ms:iv.end_ms] += 1
        overlaps: List[Tuple[int, int]] = []
        in_overlap = False
        start = 0
        for ms, count in enumerate(marks):
            if count > 1 and not in_overlap:
                in_overlap, start = True, ms
            elif count <= 1 and in_overlap:
                in_overlap = False
                overlaps.append((start, ms))
        if in_overlap:
            overlaps.append((start, duration_ms))
        segments = [{
            "seg_id": f"{shard_id}-ref-{i:02d}",
            "start_ms": iv.start_ms,
            "end_ms": iv.end_ms,
            "speaker": f"S{iv.speaker}",
            "confidence": _confidence_for(iv, overlaps),
            "overlap": any(iv.start_ms < oe and os_ < iv.end_ms
                           for os_, oe in overlaps),
        } for i, iv in enumerate(intervals)]
        manifest_shards.append({
            "shard_id": shard_id,
            "order": index,
            "wav": f"shards/{wav_name}",
            "n_samples": n_samples,
            "sample_rate": SAMPLE_RATE,
            "duration_ms": duration_ms,
            "n_speakers": n_speakers,
            "overlap_ms": [list(o) for o in overlaps],
            "segments": segments,
            "seed": seed,
        })
    manifest = {
        "manifest_version": 1,
        "sample_rate": SAMPLE_RATE,
        "sample_width": 2,
        "channels": 1,
        "base_seed": base_seed,
        "shards": manifest_shards,
    }
    manifest_path = os.path.join(workspace_dir, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=True)
    return manifest
