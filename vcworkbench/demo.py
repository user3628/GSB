from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .canonical import atomic_write_json
from .manifest import load_manifest
from .profiles import TARGET_SPEAKERS
from .wavio import write_pcm16_wav
from .workspace import Workspace


DEMO_SEED = 20261122


@dataclass(frozen=True)
class DemoShard:
    shard_id: str
    source_speaker: str
    target_speaker: str
    base_f0_hz: float
    pitch_shift: float
    mel_warp: float
    frame_shift: int
    duration_seconds: float
    transcript: str


DEMO_SHARDS = (
    DemoShard(
        shard_id="zh_en_01",
        source_speaker="S1",
        target_speaker="T1",
        base_f0_hz=180.0,
        pitch_shift=1.06,
        mel_warp=0.93,
        frame_shift=160,
        duration_seconds=1.10,
        transcript="你好，welcome to the streaming voice conversion workbench。第一段中英混排诊断语音。",
    ),
    DemoShard(
        shard_id="zh_en_02",
        source_speaker="S2",
        target_speaker="T2",
        base_f0_hz=230.0,
        pitch_shift=1.05,
        mel_warp=1.07,
        frame_shift=192,
        duration_seconds=1.25,
        transcript="接下来检查 pitch contour 与 mel envelope；please verify byte-identical output。",
    ),
    DemoShard(
        shard_id="zh_en_03",
        source_speaker="S3",
        target_speaker="T1",
        base_f0_hz=160.0,
        pitch_shift=1.12,
        mel_warp=1.00,
        frame_shift=128,
        duration_seconds=1.00,
        transcript="最后一段 short utterance，包含 stop and resume 一致性比对。",
    ),
)


def _synthetic_voice(shard: DemoShard, seed: int, sample_rate: int = 16000) -> np.ndarray:
    rng = np.random.default_rng(seed)
    sample_count = int(round(shard.duration_seconds * sample_rate))
    t = np.arange(sample_count, dtype=np.float64) / float(sample_rate)
    signal = np.zeros(sample_count, dtype=np.float64)
    lead = int(0.10 * sample_rate)
    tail = int(0.08 * sample_rate)
    active_start = lead
    active_end = sample_count - tail
    if active_end <= active_start:
        return np.zeros(sample_count, dtype=np.float64)
    active_len = active_end - active_start
    f0_contour = shard.base_f0_hz * (
        1.0
        + 0.016 * np.sin(2.0 * np.pi * 0.74 * t + 0.31)
        + 0.006 * np.sin(2.0 * np.pi * 2.05 * t + 1.17)
    )
    phase = 2.0 * np.pi * np.cumsum(f0_contour) / float(sample_rate)
    syllable_count = 7
    envelope = np.zeros(sample_count, dtype=np.float64)
    for index in range(syllable_count):
        seg_start = active_start + int(active_len * index / syllable_count)
        seg_end = active_start + int(active_len * (index + 1) / syllable_count)
        length = max(1, seg_end - seg_start)
        local = np.linspace(0.0, np.pi, length, endpoint=False)
        shape = np.sin(local) ** 2
        fade = min(24, length // 4)
        if fade > 0:
            shape[:fade] *= np.linspace(0.0, 1.0, fade, endpoint=False)
            shape[-fade:] *= np.linspace(1.0, 0.0, fade, endpoint=False)
        envelope[seg_start:seg_end] = np.maximum(envelope[seg_start:seg_end], shape)
    formants = (
        (650.0, 110.0, 0.75),
        (1150.0, 150.0, 0.55),
        (2500.0, 260.0, 0.30),
    )
    for harmonic in range(1, 18):
        harmonic_hz = harmonic * f0_contour
        resonance = np.zeros(sample_count, dtype=np.float64)
        for center, bandwidth, strength in formants:
            resonance += strength * np.exp(-0.5 * ((harmonic_hz - center) / bandwidth) ** 2)
        amplitude = (0.42 / harmonic**0.82) * (0.35 + resonance)
        signal += amplitude * np.sin(harmonic * phase + 0.41 * harmonic)
    noisy = rng.standard_normal(sample_count) * 0.0025
    signal = (signal + noisy) * envelope
    peak = float(np.max(np.abs(signal))) if signal.size else 0.0
    if peak > 1.0e-12:
        signal *= 0.58 / peak
    signal[:lead] = 0.0
    signal[active_end:] = 0.0
    return signal


def _reference_for(shard: DemoShard) -> tuple[float, float]:
    factor = TARGET_SPEAKERS[shard.target_speaker].pitch_factor * shard.pitch_shift
    expected = shard.base_f0_hz * factor
    return round(expected * 0.94, 3), round(expected * 1.06, 3)


def demo_manifest() -> dict[str, Any]:
    shards: list[dict[str, Any]] = []
    for shard in DEMO_SHARDS:
        sample_count = int(round(shard.duration_seconds * 16000))
        lo, hi = _reference_for(shard)
        shards.append(
            {
                "shard_id": shard.shard_id,
                "wav": f"samples/{shard.shard_id}.wav",
                "source_speaker": shard.source_speaker,
                "target_speaker": shard.target_speaker,
                "pitch_shift": shard.pitch_shift,
                "mel_warp": shard.mel_warp,
                "frame_shift": shard.frame_shift,
                "quant_bits": 16,
                "expected_output_samples": sample_count,
                "reference_pitch_hz": {"min_hz": lo, "max_hz": hi},
                "reference_tolerance_hz": 4.0,
                "sample_rate": 16000,
                "channels": 1,
                "sample_count": sample_count,
                "transcript": shard.transcript,
            }
        )
    return {
        "version": "vc-manifest-v1",
        "generator": {
            "name": "deterministic-formant-voice-v1",
            "seed": DEMO_SEED,
            "sample_rate": 16000,
            "channels": 1,
            "sample_width_bits": 16,
        },
        "shards": shards,
    }


def generate_demo(workspace: Workspace, *, force: bool = False) -> dict[str, Any]:
    workspace.ensure()
    if workspace.has_manifest() and not force:
        manifest = load_manifest(workspace.manifest_path)
        return {
            "ok": True,
            "status": "already_present",
            "workspace": str(workspace.root),
            "manifest_digest": manifest.digest(),
            "shard_ids": [spec.shard_id for spec in manifest.shards],
        }
    manifest = demo_manifest()
    for index, shard in enumerate(DEMO_SHARDS):
        samples = _synthetic_voice(shard, DEMO_SEED + index * 101)
        pcm = np.rint(np.clip(samples, -1.0, 1.0) * 32767.0).astype("<i2")
        write_pcm16_wav(workspace.samples_dir / f"{shard.shard_id}.wav", pcm, 16000)
    atomic_write_json(workspace.manifest_path, manifest, pretty=True)
    loaded = load_manifest(workspace.manifest_path)
    return {
        "ok": True,
        "status": "generated",
        "workspace": str(workspace.root),
        "manifest_digest": loaded.digest(),
        "seed": DEMO_SEED,
        "shard_ids": [spec.shard_id for spec in loaded.shards],
        "sample_count": sum(spec.sample_count for spec in loaded.shards),
        "target_speakers": sorted({spec.target_speaker for spec in loaded.shards}),
        "languages": ["zh", "en", "zh-en-mixed"],
    }


def demo_paths(workspace: Workspace) -> list[Path]:
    return [workspace.manifest_path, *sorted(workspace.samples_dir.glob("*.wav"))]
