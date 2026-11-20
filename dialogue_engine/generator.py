"""Deterministic synthetic WAV + manifest generator for local demonstrations."""

from __future__ import annotations

import io
import wave
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .serialization import atomic_write_bytes, atomic_write_json, digest_json


SAMPLE_RATE = 16_000


@dataclass(frozen=True)
class TurnSpec:
    speaker_id: str
    emotion: str
    act: str
    duration_ms: int
    gap_before_ms: int
    text: str


@dataclass(frozen=True)
class ShardSpec:
    shard_id: str
    turns: tuple[TurnSpec, ...]


SHARD_SPECS: tuple[ShardSpec, ...] = (
    ShardSpec(
        shard_id="shard_01",
        turns=(
            TurnSpec("S1", "平静", "陈述", 1650, 0, "大家好，今天聊一下本地引擎。Let's begin."),
            TurnSpec("S2", "高兴", "提问", 1480, 240, "真的吗？Can it resume after a stop?"),
            TurnSpec("S1", "愤怒", "打断", 720, 24, "等一下，先别跳过检查点！Wait, hold on."),
            TurnSpec("S3", "高兴", "确认", 620, 170, "好的，没问题。Got it."),
            TurnSpec("S2", "悲伤", "陈述", 1900, 310, "不过我有点担心旧状态被覆盖。I am a bit worried."),
        ),
    ),
    ShardSpec(
        shard_id="shard_02",
        turns=(
            TurnSpec("S3", "平静", "陈述", 1420, 0, "We can inspect every stage, 每一阶段都能查询。"),
            TurnSpec("S1", "愤怒", "提问", 1360, 260, "为什么序号回退？Why did the sequence roll back?"),
            TurnSpec("S2", "高兴", "打断", 700, 20, "No, no——先看指纹！Look at the fingerprint."),
            TurnSpec("S3", "平静", "确认", 560, 210, "明白，确认一致。Confirmed."),
            TurnSpec("S1", "悲伤", "陈述", 1850, 330, "那就继续恢复吧。Let us resume the run."),
        ),
    ),
    ShardSpec(
        shard_id="shard_03",
        turns=(
            TurnSpec("S2", "高兴", "陈述", 1520, 0, "The demo is deterministic，结果逐字节一致。"),
            TurnSpec("S3", "悲伤", "提问", 1780, 280, "Are you sure? 我们真的能从轮次中断处恢复吗？"),
            TurnSpec("S1", "愤怒", "打断", 680, 18, "别猜，用显式前序状态！Do not infer."),
            TurnSpec("S2", "平静", "确认", 590, 230, "Yes, 已核对。Completely sure."),
            TurnSpec("S3", "高兴", "陈述", 1640, 300, "太好了，导出摘要吧。Export the summary, please."),
        ),
    ),
)


_EMOTION_PROFILE = {
    "平静": {"amplitude": 0.095, "f0": 172.0, "brightness": 0.35, "noise": 0.010, "jitter": 0.012},
    "高兴": {"amplitude": 0.235, "f0": 276.0, "brightness": 0.78, "noise": 0.014, "jitter": 0.035},
    "愤怒": {"amplitude": 0.430, "f0": 158.0, "brightness": 0.62, "noise": 0.150, "jitter": 0.020},
    "悲伤": {"amplitude": 0.058, "f0": 112.0, "brightness": 0.16, "noise": 0.008, "jitter": 0.008},
}

_SPEAKER_SCALE = {"S1": 1.00, "S2": 1.06, "S3": 0.94}

_ACT_PROFILE = {
    "陈述": {"pause_ratio": 0.30, "chunks": 3, "factors": (1.00, 1.02, 1.04)},
    "提问": {"pause_ratio": 0.22, "chunks": 3, "factors": (0.58, 0.82, 1.30)},
    "确认": {"pause_ratio": 0.07, "chunks": 2, "factors": (1.00, 0.98)},
    "打断": {"pause_ratio": 0.05, "chunks": 2, "factors": (1.22, 1.28)},
}


def _synthesize_chunk(
    rng: np.random.Generator,
    *,
    duration_samples: int,
    sample_rate: int,
    emotion: str,
    speaker_id: str,
    factor: float,
) -> np.ndarray:
    profile = _EMOTION_PROFILE[emotion]
    time = np.arange(duration_samples, dtype=np.float64) / sample_rate
    vibrato = profile["jitter"] * np.sin(2.0 * np.pi * 5.3 * time)
    f0_curve = profile["f0"] * (1.0 + vibrato)
    phase = 2.0 * np.pi * np.cumsum(f0_curve) / sample_rate
    brightness = profile["brightness"]
    signal = (
        np.sin(phase)
        + brightness * 0.42 * np.sin(2.0 * phase + 0.20)
        + brightness * 0.18 * np.sin(3.0 * phase + 0.55)
        + brightness * 0.08 * np.sin(5.0 * phase + 0.90)
    )
    signal /= 1.0 + brightness * 0.55
    noise = rng.normal(0.0, profile["noise"], size=duration_samples)
    signal = signal + noise

    attack = max(1, int(0.012 * sample_rate))
    release = max(1, int(0.018 * sample_rate))
    envelope = np.ones(duration_samples, dtype=np.float64)
    if duration_samples < attack + release + 4:
        envelope = np.hanning(max(4, duration_samples))
    else:
        envelope[:attack] = 0.5 - 0.5 * np.cos(
            np.linspace(0.0, np.pi, attack, dtype=np.float64)
        )
        envelope[-release:] = 0.5 + 0.5 * np.cos(
            np.linspace(0.0, np.pi, release, dtype=np.float64)
        )
    amplitude = profile["amplitude"] * _SPEAKER_SCALE.get(speaker_id, 1.0)
    return signal * envelope * amplitude * factor


def _render_turn(
    turn: TurnSpec,
    *,
    shard_index: int,
    turn_index: int,
    seed: int,
    sample_rate: int = SAMPLE_RATE,
) -> np.ndarray:
    rng = np.random.default_rng(seed + shard_index * 10_007 + turn_index * 101)
    total_samples = int(round(turn.duration_ms * sample_rate / 1000.0))
    act = _ACT_PROFILE[turn.act]
    chunk_count = int(act["chunks"])
    factors = act["factors"]
    pause_ratio = float(act["pause_ratio"])
    voiced_total = max(1, int(round(total_samples * (1.0 - pause_ratio))))
    silence_total = total_samples - voiced_total
    gap_count = chunk_count + 1
    gap_base = silence_total // gap_count
    remainder = silence_total - gap_base * gap_count
    output = np.zeros(total_samples, dtype=np.float64)
    cursor = 0
    for position in range(chunk_count):
        gap = gap_base + (1 if position < remainder else 0)
        cursor += gap
        remaining_chunks = chunk_count - position
        chunk_samples = voiced_total // remaining_chunks
        voiced_total -= chunk_samples
        if chunk_samples <= 0:
            continue
        chunk = _synthesize_chunk(
            rng,
            duration_samples=chunk_samples,
            sample_rate=sample_rate,
            emotion=turn.emotion,
            speaker_id=turn.speaker_id,
            factor=factors[position % len(factors)],
        )
        end = min(total_samples, cursor + chunk_samples)
        output[cursor:end] = chunk[: end - cursor]
        cursor = end

    if turn.act == "提问" and total_samples > 0:
        # A final rising emphasis makes the question cue visible to the frame slope.
        tail = max(1, min(total_samples, int(0.18 * sample_rate)))
        output[-tail:] *= np.linspace(0.80, 1.45, tail, dtype=np.float64)
    if turn.act == "确认":
        output *= 0.62
    if turn.act == "打断":
        output *= 1.08
    return np.clip(output, -1.0, 1.0)


def _wav_bytes(samples: np.ndarray, sample_rate: int = SAMPLE_RATE) -> bytes:
    pcm = np.rint(samples * 32767.0).astype("<i2", copy=False)
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(sample_rate)
        handle.writeframes(pcm.tobytes(order="C"))
    return buffer.getvalue()


def build_demo_manifest(
    *,
    seed: int = 20_261_121,
    sample_rate: int = SAMPLE_RATE,
    specs: tuple[ShardSpec, ...] = SHARD_SPECS,
) -> tuple[dict[str, Any], list[tuple[str, np.ndarray]]]:
    """Build manifest data and rendered shard arrays without touching disk."""

    shard_entries: list[dict[str, Any]] = []
    arrays: list[tuple[str, np.ndarray]] = []
    for shard_index, shard_spec in enumerate(specs):
        samples: list[np.ndarray] = []
        turns: list[dict[str, Any]] = []
        cursor_ms = 0.0
        for turn_index, turn_spec in enumerate(shard_spec.turns):
            cursor_ms += turn_spec.gap_before_ms
            rendered = _render_turn(
                turn_spec,
                shard_index=shard_index,
                turn_index=turn_index,
                seed=seed,
                sample_rate=sample_rate,
            )
            gap_samples = int(round(turn_spec.gap_before_ms * sample_rate / 1000.0))
            if gap_samples:
                samples.append(np.zeros(gap_samples, dtype=np.float64))
            start_ms = int(round(cursor_ms))
            start_sample = int(round(start_ms * sample_rate / 1000.0))
            samples.append(rendered)
            cursor_ms += turn_spec.duration_ms
            end_ms = int(round(cursor_ms))
            end_sample = int(round(end_ms * sample_rate / 1000.0))
            turns.append(
                {
                    "turn_index": turn_index,
                    "speaker_id": turn_spec.speaker_id,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "start_sample": start_sample,
                    "end_sample": end_sample,
                    "text": turn_spec.text,
                    "ref_emotion": turn_spec.emotion,
                    "ref_dialogue_act": turn_spec.act,
                }
            )
        shard_samples = np.concatenate(samples).astype(np.float64)
        shard_entries.append(
            {
                "shard_id": shard_spec.shard_id,
                "wav_file": f"{shard_spec.shard_id}.wav",
                "sample_count": int(shard_samples.shape[0]),
                "turn_count": len(turns),
                "turns": turns,
            }
        )
        arrays.append((shard_spec.shard_id, shard_samples))

    manifest = {
        "schema_version": 1,
        "generator_version": "deterministic-dialogue-synth-v1",
        "seed": seed,
        "sample_rate": sample_rate,
        "speaker_count": 3,
        "shards": shard_entries,
    }
    return manifest, arrays


def generate_demo_dataset(
    output_dir: str | Path,
    *,
    seed: int = 20_261_121,
    force: bool = False,
) -> dict[str, Any]:
    """Write three deterministic WAV shards plus ``manifest.json``."""

    destination = Path(output_dir).expanduser().resolve()
    manifest_path = destination / "manifest.json"
    if destination.exists() and any(destination.iterdir()) and not force:
        raise FileExistsError(
            f"output directory is not empty: {destination} (pass force=True)"
        )
    destination.mkdir(parents=True, exist_ok=True)
    manifest, arrays = build_demo_manifest(seed=seed)
    written_files: list[str] = []
    for shard_id, samples in arrays:
        filename = f"{shard_id}.wav"
        atomic_write_bytes(destination / filename, _wav_bytes(samples))
        written_files.append(filename)
    atomic_write_json(manifest_path, manifest)
    written_files.append(manifest_path.name)
    return {
        "output_dir": str(destination),
        "manifest": str(manifest_path),
        "seed": seed,
        "shard_count": len(arrays),
        "turn_count": sum(len(entry["turns"]) for entry in manifest["shards"]),
        "files": written_files,
        "manifest_digest": digest_json(manifest),
    }

