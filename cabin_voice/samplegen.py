"""Deterministic in-cabin WAV and manifest generator (no external data)."""

from __future__ import annotations

import json
import wave
from pathlib import Path
from typing import Any

import numpy as np

from .config import default_config
from .util import atomic_write_bytes, canonical_bytes


ZONES: list[dict[str, Any]] = [
    {"zone_id": "driver", "label": "主驾", "priority": 100, "x_m": -0.42, "y_m": 0.05},
    {"zone_id": "passenger", "label": "副驾", "priority": 90, "x_m": 0.42, "y_m": 0.05},
    {"zone_id": "rear_left", "label": "后排左", "priority": 80, "x_m": -0.38, "y_m": 1.05},
    {"zone_id": "rear_right", "label": "后排右", "priority": 70, "x_m": 0.38, "y_m": 1.05},
]

LEXICON = [
    {"term": "open_ac", "display": "打开空调"},
    {"term": "play_music", "display": "播放音乐"},
    {"term": "open_window", "display": "打开车窗"},
    {"term": "set_temperature", "display": "调节温度"},
    {"term": "answer_call", "display": "接听电话"},
    {"term": "navigate_home", "display": "导航回家"},
    {"term": "nav_destination", "display": "导航到目的地"},
]

CHUNK_SPECS: list[dict[str, Any]] = [
    {
        "chunk_id": "chunk_001",
        "title": "前排双人不同指令",
        "duration_ms": 2400,
        "events": [
            {"zone_id": "driver", "start_ms": 210, "end_ms": 870, "term": "open_ac", "strength": 0.97},
            {"zone_id": "passenger", "start_ms": 700, "end_ms": 1370, "term": "play_music", "strength": 0.90},
            {"zone_id": "rear_left", "start_ms": 1370, "end_ms": 1880, "term": "nav_destination", "strength": 0.84},
        ],
    },
    {
        "chunk_id": "chunk_002",
        "title": "同一指令双音区重叠",
        "duration_ms": 2400,
        "events": [
            {"zone_id": "driver", "start_ms": 320, "end_ms": 1110, "term": "open_window", "strength": 0.915},
            {"zone_id": "passenger", "start_ms": 320, "end_ms": 1120, "term": "open_window", "strength": 0.905},
            {"zone_id": "rear_right", "start_ms": 1500, "end_ms": 2050, "term": "set_temperature", "strength": 0.86},
        ],
    },
    {
        "chunk_id": "chunk_003",
        "title": "后排重叠与后段主驾指令",
        "duration_ms": 2400,
        "events": [
            {"zone_id": "passenger", "start_ms": 180, "end_ms": 820, "term": "answer_call", "strength": 0.94},
            {"zone_id": "rear_left", "start_ms": 650, "end_ms": 1260, "term": "open_ac", "strength": 0.81},
            {"zone_id": "rear_right", "start_ms": 700, "end_ms": 1290, "term": "play_music", "strength": 0.85},
            {"zone_id": "driver", "start_ms": 1640, "end_ms": 2110, "term": "navigate_home", "strength": 0.91},
        ],
    },
]


def _smooth_envelope(length: int, sample_rate: int, attack_ms: float = 35.0, release_ms: float = 55.0) -> np.ndarray:
    attack = min(length, max(1, int(attack_ms * sample_rate / 1000)))
    release = min(length, max(1, int(release_ms * sample_rate / 1000)))
    envelope = np.ones(length, dtype=np.float64)
    if attack > 0:
        envelope[:attack] = np.linspace(0.0, 1.0, attack, endpoint=False) ** 0.7
    if release > 0:
        envelope[-release:] *= np.linspace(1.0, 0.0, release, endpoint=False) ** 1.2
    time = np.arange(length) / sample_rate
    syllable = 0.72 + 0.28 * np.sin(2.0 * np.pi * 4.2 * time + 0.4) ** 2
    return envelope * syllable


def _speech_like(
    length: int,
    sample_rate: int,
    rng: np.random.Generator,
    *,
    fundamental_hz: float,
    amplitude: float,
) -> np.ndarray:
    time = np.arange(length, dtype=np.float64) / sample_rate
    vibrato = 2.0 * np.sin(2.0 * np.pi * 1.7 * time + rng.uniform(0.0, 2.0 * np.pi))
    phase = 2.0 * np.pi * (fundamental_hz + vibrato) * time
    signal = np.zeros(length, dtype=np.float64)
    for harmonic, weight in enumerate((1.0, 0.62, 0.36, 0.22, 0.14, 0.09), start=1):
        signal += weight * np.sin(harmonic * phase + rng.uniform(0.0, 2.0 * np.pi))
    # A small breath-noise component keeps the signal natural while retaining a tonal spectrum.
    signal += 0.075 * rng.standard_normal(length)
    signal /= max(1.0, float(np.max(np.abs(signal))))
    return amplitude * _smooth_envelope(length, sample_rate) * signal


def _zone_waveform(
    zone: dict[str, Any],
    zone_index: int,
    duration_ms: int,
    events: list[dict[str, Any]],
    sample_rate: int,
    rng: np.random.Generator,
) -> tuple[np.ndarray, list[list[int]]]:
    sample_count = int(round(duration_ms * sample_rate / 1000.0))
    waveform = 0.00115 * rng.standard_normal(sample_count)
    time = np.arange(sample_count) / sample_rate
    waveform += 0.00035 * np.sin(2.0 * np.pi * 50.0 * time)
    references: list[list[int]] = []
    for event_index, event in enumerate(events):
        start = int(round(event["start_ms"] * sample_rate / 1000.0))
        end = int(round(event["end_ms"] * sample_rate / 1000.0))
        start = max(0, min(sample_count - 2, start))
        end = max(start + 2, min(sample_count, end))
        fundamental = 128.0 + 21.0 * zone_index + 2.5 * event_index
        speech = _speech_like(
            end - start,
            sample_rate,
            rng,
            fundamental_hz=fundamental,
            amplitude=0.13 + 0.012 * ((event_index + zone_index) % 3),
        )
        waveform[start:end] += speech
        references.append([int(round(start * 1000.0 / sample_rate)), int(round(end * 1000.0 / sample_rate))])
    return waveform, references


def _write_wav(path: Path, waveform: np.ndarray, sample_rate: int) -> None:
    clipped = np.clip(waveform, -0.98, 0.98)
    pcm = np.round(clipped * 32767.0).astype("<i2")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.generating")
    with wave.open(str(temporary), "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(sample_rate)
        handle.writeframes(pcm.tobytes())
    temporary.replace(path)


def generate_demo(output_dir: str | Path, *, seed: int = 20261123) -> dict[str, Any]:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    sample_rate = 16000
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "session_id": f"cabin-demo-{seed}",
        "description": "Deterministic four-zone in-cabin validation session",
        "sample_rate": sample_rate,
        "zones": ZONES,
        "microphone": {"x_m": 0.0, "y_m": 0.38},
        "lexicon": LEXICON,
        "chunks": [],
    }
    for chunk_index, spec in enumerate(CHUNK_SPECS):
        duration_ms = int(spec["duration_ms"])
        zone_waveforms: list[np.ndarray] = []
        segment_entries: list[dict[str, Any]] = []
        offset = 0
        reference_payload: dict[str, Any] = {}
        for zone_index, zone in enumerate(ZONES):
            zone_id = str(zone["zone_id"])
            zone_events = [event for event in spec["events"] if event["zone_id"] == zone_id]
            rng = np.random.default_rng(seed + chunk_index * 1009 + zone_index * 9176)
            waveform, references = _zone_waveform(
                zone, zone_index, duration_ms, zone_events, sample_rate, rng
            )
            sample_count = len(waveform)
            zone_waveforms.append(waveform)
            hits = [
                {
                    "term": event["term"],
                    "strength": round(float(event["strength"]) * (1.0 + 0.0001 * zone_index), 6),
                    "at_ms": int(event["start_ms"] + 35),
                }
                for event in zone_events
            ]
            segment_entries.append(
                {
                    "zone_id": zone_id,
                    "offset_samples": offset,
                    "sample_count": sample_count,
                    "reference_active_ms": references,
                    "lexicon_hits": hits,
                }
            )
            reference_payload[zone_id] = {
                "active_ms": references,
                "lexicon_hits": hits,
            }
            offset += sample_count
        concatenated = np.concatenate(zone_waveforms)
        wav_relative = f"audio/{spec['chunk_id']}.wav"
        _write_wav(output / wav_relative, concatenated, sample_rate)
        manifest["chunks"].append(
            {
                "chunk_id": spec["chunk_id"],
                "title": spec["title"],
                "wav": wav_relative,
                "segments": segment_entries,
                "reference": reference_payload,
                "reference_expected_review_terms": [
                    event["term"]
                    for event in spec["events"]
                    if event["zone_id"] in {"driver", "passenger"}
                    and event["term"] == "open_window"
                ],
            }
        )
    manifest_path = output / "manifest.json"
    atomic_write_bytes(manifest_path, canonical_bytes(manifest))
    config_path = output / "config.json"
    atomic_write_bytes(config_path, canonical_bytes(default_config()))

    overlap_manifest = json.loads(json.dumps(manifest))
    overlap_manifest["session_id"] = f"cabin-overlap-{seed}"
    overlap_manifest["chunks"] = [overlap_manifest["chunks"][1]]
    overlap_path = output / "manifest.overlap.json"
    atomic_write_bytes(overlap_path, canonical_bytes(overlap_manifest))

    invalid_manifest = json.loads(json.dumps(manifest))
    invalid_manifest["session_id"] = f"cabin-invalid-offset-{seed}"
    invalid_manifest["chunks"] = [invalid_manifest["chunks"][0]]
    with wave.open(str(output / invalid_manifest["chunks"][0]["wav"]), "rb") as handle:
        frame_count = handle.getnframes()
    invalid_manifest["chunks"][0]["segments"][0]["offset_samples"] = frame_count + 1
    invalid_manifest["chunks"][0]["segments"][0]["sample_count"] = 4000
    invalid_manifest["chunks"][0]["wav"] = manifest["chunks"][0]["wav"]
    invalid_path = output / "manifest.invalid_offset.json"
    atomic_write_bytes(invalid_path, canonical_bytes(invalid_manifest))

    demo_readme = output / "README.md"
    atomic_write_bytes(
        demo_readme,
        (
            "# Generated deterministic demo\n\n"
            f"Seed: `{seed}`. All WAV files are 16 kHz, mono, 16-bit PCM.\n\n"
            "- `manifest.json`: full three-chunk session.\n"
            "- `manifest.overlap.json`: chunk 2 only; driver and passenger both hit `open_window`.\n"
            "- `manifest.invalid_offset.json`: E_ZONE_OFFSET_OUT_OF_BOUNDS injection.\n"
            "- `config.json`: default engine configuration.\n"
        ).encode("utf-8"),
    )
    return {
        "output": str(output),
        "seed": seed,
        "manifest": str(manifest_path),
        "overlap_manifest": str(overlap_path),
        "invalid_offset_manifest": str(invalid_path),
        "config": str(config_path),
        "chunks": len(manifest["chunks"]),
        "wav_bytes": sum(path.stat().st_size for path in (output / "audio").glob("*.wav")),
    }
