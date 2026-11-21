from __future__ import annotations

import json
import math
import wave
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from .canonical import digest_file
from .config import ZONE_LABELS, ZONE_ORDER
from .errors import CabinError


def inspect_wav(path: str | Path) -> dict[str, Any]:
    try:
        with wave.open(str(path), "rb") as handle:
            channels = handle.getnchannels()
            sample_width = handle.getsampwidth()
            sample_rate = handle.getframerate()
            total_samples = handle.getnframes()
            compression = handle.getcomptype()
    except (OSError, wave.Error) as exc:
        raise CabinError(
            "E_IMPORT_INVALID",
            "unable to read WAV shard",
            details={"path": str(path), "reason": str(exc)},
            http_status=422,
        ) from exc
    if channels != 1 or sample_width != 2 or sample_rate != 16000:
        raise CabinError(
            "E_IMPORT_INVALID",
            "WAV must be 16 kHz mono 16-bit PCM",
            details={
                "path": str(path),
                "channels": channels,
                "sample_width": sample_width,
                "sample_rate": sample_rate,
            },
            http_status=422,
        )
    if compression != "NONE":
        raise CabinError(
            "E_IMPORT_INVALID",
            "compressed WAV is not supported",
            details={"path": str(path), "compression": compression},
            http_status=422,
        )
    return {
        "path": str(Path(path).resolve()),
        "channels": channels,
        "sample_width": sample_width,
        "sample_rate": sample_rate,
        "total_samples": total_samples,
        "content_fingerprint": digest_file(path),
    }


def read_zone_signal(path: str | Path, start_sample: int, end_sample: int) -> np.ndarray:
    with wave.open(str(path), "rb") as handle:
        handle.setpos(start_sample)
        raw = handle.readframes(end_sample - start_sample)
    values = np.frombuffer(raw, dtype="<i2").astype(np.float64)
    return values / 32768.0


def write_pcm16_wav(path: str | Path, samples: np.ndarray) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    clipped = np.clip(samples, -1.0, 1.0)
    pcm = np.rint(clipped * 32767.0).astype("<i2")
    with wave.open(str(target), "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(16000)
        handle.writeframes(pcm.tobytes())


def _fade(length_samples: int, sample_rate: int, fade_ms: int = 18) -> np.ndarray:
    fade_len = min(length_samples // 2, int(sample_rate * fade_ms / 1000))
    envelope = np.ones(length_samples, dtype=np.float64)
    if fade_len > 0:
        ramp = np.linspace(0.0, 1.0, fade_len, endpoint=False, dtype=np.float64)
        envelope[:fade_len] = ramp
        envelope[-fade_len:] = ramp[::-1]
    return envelope


def _synthesize_zone(
    rng: np.random.Generator,
    duration_ms: int,
    activity: Iterable[tuple[int, int]],
    *,
    sample_rate: int = 16000,
    noise_amplitude: float = 0.0018,
) -> np.ndarray:
    length = round(duration_ms * sample_rate / 1000)
    signal = rng.normal(0.0, noise_amplitude, length).astype(np.float64)
    for start_ms, end_ms in activity:
        start = round(start_ms * sample_rate / 1000)
        end = round(end_ms * sample_rate / 1000)
        end = min(length, end)
        count = max(0, end - start)
        if count == 0:
            continue
        t = np.arange(count, dtype=np.float64) / sample_rate
        f0 = 148.0 + 34.0 * rng.random()
        tremor = 1.0 + 0.018 * np.sin(2.0 * math.pi * 4.2 * t + rng.random())
        carrier = np.zeros(count, dtype=np.float64)
        for harmonic, gain in ((1, 0.055), (2, 0.031), (3, 0.018), (5, 0.009)):
            phase = rng.random() * 2.0 * math.pi
            carrier += gain * np.sin(2.0 * math.pi * f0 * harmonic * t + phase)
        modulation = 0.78 + 0.22 * np.sin(2.0 * math.pi * 3.1 * t + rng.random() * math.pi)
        speech = carrier * tremor * modulation
        speech += rng.normal(0.0, 0.0045, count)
        signal[start:end] += speech * _fade(count, sample_rate)
    return signal


def _demo_definition() -> dict[str, Any]:
    shards = [
        {
            "shard_id": "shard-001",
            "title": "四音区基础与重叠歧义",
            "priorities": {
                "driver": 1,
                "passenger": 1,
                "rear_left": 2,
                "rear_right": 3,
            },
            "activity": {
                "driver": [(220, 620)],
                "passenger": [(200, 620)],
                "rear_left": [(1540, 1780)],
                "rear_right": [],
            },
            "hits": [
                ("driver", "打开空调", 300, 520, 0.90),
                ("passenger", "打开空调", 315, 540, 0.88),
                ("rear_left", "关闭车窗", 1600, 1730, 0.84),
            ],
        },
        {
            "shard_id": "shard-002",
            "title": "后排双音区与静音间隔",
            "priorities": {
                "driver": 1,
                "passenger": 2,
                "rear_left": 1,
                "rear_right": 1,
            },
            "activity": {
                "driver": [(280, 740)],
                "passenger": [(450, 780)],
                "rear_left": [(1080, 1420)],
                "rear_right": [(1120, 1460)],
            },
            "hits": [
                ("driver", "调高音量", 370, 610, 0.78),
                ("passenger", "调高音量", 470, 690, 0.72),
                ("rear_left", "播放音乐", 1140, 1300, 0.72),
                ("rear_right", "播放音乐", 1180, 1360, 0.88),
            ],
        },
        {
            "shard_id": "shard-003",
            "title": "优先级编辑翻转归属",
            "priorities": {
                "driver": 1,
                "passenger": 2,
                "rear_left": 3,
                "rear_right": 4,
            },
            "activity": {
                "driver": [(140, 470)],
                "passenger": [],
                "rear_left": [(760, 1100)],
                "rear_right": [(790, 1130)],
            },
            "hits": [
                ("driver", "接听电话", 210, 390, 0.86),
                ("rear_left", "打开座椅加热", 810, 1030, 0.80),
                ("rear_right", "打开座椅加热", 825, 1050, 0.78),
            ],
        },
    ]
    return {"zone_duration_ms": 2000, "shards": shards}


def generate_demo_dataset(root: str | Path, *, seed: int = 20261122) -> dict[str, str]:
    target = Path(root)
    wav_dir = target / "wavs"
    wav_dir.mkdir(parents=True, exist_ok=True)
    definition = _demo_definition()
    duration_ms = int(definition["zone_duration_ms"])
    sample_rate = 16000
    samples_per_zone = round(duration_ms * sample_rate / 1000)
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "sample_rate": sample_rate,
        "duration_upper_bound_ms": duration_ms,
        "default_query_budget_ms": 20,
        "generator": {
            "name": "cabin_voice_demo",
            "seed": seed,
        },
        "shards": [],
    }
    for shard_index, definition_shard in enumerate(definition["shards"]):
        shard_id = definition_shard["shard_id"]
        concatenated = np.zeros(samples_per_zone * len(ZONE_ORDER), dtype=np.float64)
        zones: list[dict[str, Any]] = []
        refs: dict[str, list[list[int]]] = {}
        for zone_index, zone_id in enumerate(ZONE_ORDER):
            rng = np.random.default_rng(seed + shard_index * 101 + zone_index)
            activity = definition_shard["activity"][zone_id]
            signal = _synthesize_zone(rng, duration_ms, activity, sample_rate=sample_rate)
            start_sample = zone_index * samples_per_zone
            end_sample = start_sample + samples_per_zone
            concatenated[start_sample:end_sample] = signal
            zones.append(
                {
                    "zone_id": zone_id,
                    "label": ZONE_LABELS[zone_id],
                    "channel": zone_index,
                    "start_sample": start_sample,
                    "end_sample": end_sample,
                }
            )
            refs[zone_id] = [[int(start), int(end)] for start, end in activity]
        wav_path = wav_dir / f"{shard_id}.wav"
        write_pcm16_wav(wav_path, concatenated)
        shard = {
            "shard_id": shard_id,
            "title": definition_shard["title"],
            "wav": str(wav_path.relative_to(target)),
            "total_samples": samples_per_zone * len(ZONE_ORDER),
            "zones": zones,
            "reference_activity_ms": refs,
            "priorities": definition_shard["priorities"],
            "word_hits": [
                {
                    "zone_id": zone_id,
                    "word": word,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "strength": strength,
                }
                for zone_id, word, start_ms, end_ms, strength in definition_shard["hits"]
            ],
        }
        manifest["shards"].append(shard)
    manifest_path = target / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return {
        "manifest_path": str(manifest_path.resolve()),
        "data_root": str(target.resolve()),
    }
