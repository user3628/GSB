#!/usr/bin/env python3
"""Generate deterministic local WAV shards and v2/v1 manifests."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
import soundfile as sf


SEED = 20261122
SAMPLE_RATE = 16000
BIT_DEPTH = 16
CHANNELS = 1
SPEAKER_ID = "speaker_aurora"


UNIT_SPECS = {
    "你": {"kind": "vowel", "formants": [420, 2100, 2900]},
    "好": {"kind": "vowel", "formants": [700, 1350, 2700]},
    "音": {"kind": "vowel", "formants": [390, 2200, 3050]},
    "色": {"kind": "vowel", "formants": [650, 1500, 2550]},
    "语": {"kind": "vowel", "formants": [410, 2150, 3000]},
    "嗯": {"kind": "vowel", "formants": [520, 1100, 2450]},
    "旧": {"kind": "vowel", "formants": [720, 1250, 2500]},
    "版": {"kind": "vowel", "formants": [580, 1650, 2650]},
    "world": {"kind": "vowel", "formants": [500, 900, 2450]},
    "clone": {"kind": "vowel", "formants": [520, 1450, 2550]},
    "lab": {"kind": "consonant", "formants": [480, 1700, 2700]},
    "legacy": {"kind": "consonant", "formants": [550, 1600, 2500]},
    "pause": {"kind": "pause", "formants": [300, 900, 1800]},
    "sil": {"kind": "pause", "formants": [300, 900, 1800]},
    "停顿": {"kind": "pause", "formants": [300, 900, 1800]},
}


def _unit_formants(symbol: str) -> list[float]:
    return [float(x) for x in UNIT_SPECS.get(symbol, {"formants": [500, 1500, 2500]})["formants"]]


def _ranges(formants: list[float]) -> list[list[float]]:
    return [[round(value * 0.92, 3), round(value * 1.08, 3)] for value in formants]


def synthesize(specs: list[tuple[str, int]], seed_offset: int) -> tuple[np.ndarray, list[dict[str, Any]]]:
    total_samples = sum(int(round(duration_ms * SAMPLE_RATE / 1000.0)) for _symbol, duration_ms in specs)
    rng = np.random.default_rng(SEED + seed_offset)
    t = np.arange(total_samples, dtype=np.float64) / SAMPLE_RATE
    signal = np.zeros(total_samples, dtype=np.float64)
    unit_rows: list[dict[str, Any]] = []
    cursor = 0
    for unit_index, (symbol, duration_ms) in enumerate(specs):
        count = int(round(duration_ms * SAMPLE_RATE / 1000.0))
        start = cursor
        end = min(cursor + count, total_samples)
        count = end - start
        local_t = np.arange(count, dtype=np.float64) / SAMPLE_RATE
        spec = UNIT_SPECS.get(symbol, {"kind": "vowel", "formants": [500, 1500, 2500]})
        f0 = 178.0 + 7.0 * math.sin(2.0 * math.pi * (unit_index + 1) / 7.0) + seed_offset * 0.4
        segment = np.zeros(count, dtype=np.float64)
        if spec["kind"] == "pause":
            segment += rng.normal(0.0, 0.0018, count)
        else:
            formants = [float(value) for value in spec["formants"]]
            for harmonic in range(1, 25):
                frequency = harmonic * f0
                if frequency >= SAMPLE_RATE / 2:
                    break
                amplitude = 0.4 / harmonic
                for formant, bandwidth in zip(formants, (90.0, 140.0, 220.0)):
                    amplitude += math.exp(-0.5 * ((frequency - formant) / bandwidth) ** 2) / max(1.0, harmonic ** 0.25)
                phase = (unit_index + 1) * 0.37 + harmonic * 0.11
                segment += amplitude * np.sin(2.0 * math.pi * frequency * local_t + phase)
            if spec["kind"] == "consonant":
                noise = rng.normal(0.0, 0.08, count) * np.exp(-local_t / 0.045)
                segment = 0.65 * segment + noise
            segment += rng.normal(0.0, 0.002, count)
        fade_samples = min(48, max(1, count // 12))
        envelope = np.ones(count, dtype=np.float64)
        envelope[:fade_samples] = np.linspace(0.0, 1.0, fade_samples, endpoint=False)
        envelope[-fade_samples:] *= np.linspace(1.0, 0.0, fade_samples, endpoint=True)
        segment *= envelope
        signal[start:end] += segment
        unit_rows.append(
            {
                "symbol": symbol,
                "start_ms": round(start * 1000.0 / SAMPLE_RATE, 6),
                "end_ms": round(end * 1000.0 / SAMPLE_RATE, 6),
                "formants_hz": _unit_formants(symbol),
                "formant_ranges_hz": _ranges(_unit_formants(symbol)),
            }
        )
        cursor = end
        if cursor >= total_samples:
            break
    peak = float(np.max(np.abs(signal))) if signal.size else 0.0
    if peak > 0:
        signal = signal / peak * 0.72
    pcm = np.clip(np.rint(signal * 32767.0), -32768, 32767).astype(np.int16)
    return pcm, unit_rows


def write_wav(path: Path, pcm: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    sf.write(str(path), pcm, SAMPLE_RATE, subtype="PCM_16", format="WAV")


def v2_shard(
    shard_id: str,
    wav_name: str,
    specs: list[tuple[str, int]],
    seed_offset: int,
    pitch_values: list[float],
) -> tuple[dict[str, Any], np.ndarray]:
    pcm, units = synthesize(specs, seed_offset)
    write_wav(DATA_DIR / wav_name, pcm)
    times = [round(index / (len(pitch_values) - 1), 12) for index in range(len(pitch_values))]
    payload = {
        "shard_id": shard_id,
        "wav": wav_name,
        "sample_count": int(len(pcm)),
        "timbre_id": SPEAKER_ID,
        "reference_f0_mean": 178.0,
        "structure_version": 2,
        "pitch_curve": [[time, value] for time, value in zip(times, pitch_values)],
        "units": units,
    }
    return payload, pcm


def write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    global DATA_DIR
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    args = parser.parse_args()
    DATA_DIR = Path(args.data_dir).resolve()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    v2_shards: list[dict[str, Any]] = []
    shard_records: list[dict[str, Any]] = []
    configs = [
        (
            "shard_001",
            "shard_001.wav",
            [("你", 330), ("好", 300), ("world", 430), ("pause", 150)],
            1,
            [0.97, 1.0, 1.035, 1.015, 0.985],
        ),
        (
            "shard_002",
            "shard_002.wav",
            [("音", 310), ("色", 330), ("clone", 420), ("sil", 160)],
            2,
            [0.96, 1.01, 1.025, 0.995, 1.0],
        ),
        (
            "shard_003",
            "shard_003.wav",
            [("lab", 350), ("语", 310), ("嗯", 270), ("停顿", 250)],
            3,
            [0.98, 1.0, 1.02, 1.04, 0.99],
        ),
    ]
    for shard_id, wav_name, specs, seed_offset, pitch_values in configs:
        shard, pcm = v2_shard(shard_id, wav_name, specs, seed_offset, pitch_values)
        v2_shards.append(shard)
        shard_records.append(
            {
                "order": len(shard_records) + 1,
                "shard_id": shard_id,
                "manifest": "manifest_v2.json",
                "wav": wav_name,
                "sample_count": int(len(pcm)),
                "timbre_id": SPEAKER_ID,
                "unit_symbols": [item["symbol"] for item in shard["units"]],
            }
        )
    v2_total = sum(item["sample_count"] for item in v2_shards)
    manifest_v2 = {
        "format": "tonelab.manifest.v2",
        "structure_version": 2,
        "project_id": "demo-aurora-v2",
        "description": "Deterministic Chinese/English timbre and pronunciation demo (v2)",
        "audio": {"sample_rate": SAMPLE_RATE, "channels": CHANNELS, "bit_depth": BIT_DEPTH},
        "declared_total_samples": v2_total,
        "shards": v2_shards,
    }
    write_manifest(DATA_DIR / "manifest_v2.json", manifest_v2)

    legacy_specs = [("旧", 300), ("版", 290), ("legacy", 390), ("停顿", 300)]
    legacy_pcm, _legacy_units = synthesize(legacy_specs, 4)
    write_wav(DATA_DIR / "shard_004_legacy.wav", legacy_pcm)
    manifest_v1 = {
        "format": "tonelab.manifest.v1",
        "structure_version": 1,
        "project_id": "demo-aurora-v1",
        "description": "Deterministic version 1 compatibility shard (Chinese/English)",
        "audio": {"sample_rate": SAMPLE_RATE, "channels": CHANNELS, "bit_depth": BIT_DEPTH},
        "declared_total_samples": int(len(legacy_pcm)),
        "shards": [
            {
                "wav": "shard_004_legacy.wav",
                "timbre_id": SPEAKER_ID,
                "reference_f0_mean": 178.0,
                "sample_count": int(len(legacy_pcm)),
                "pitch_offsets_percent": [-2, 0, 3, 1, -1],
                "units": "旧|版|legacy|停顿",
            }
        ],
    }
    write_manifest(DATA_DIR / "manifest_v1.json", manifest_v1)
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from tonelab.config import Config
    from tonelab.formats import load_manifest

    generated_v1_id = load_manifest(DATA_DIR / "manifest_v1.json", Config()).shards[0].shard_id
    shard_records.append(
        {
            "order": len(shard_records) + 1,
            "shard_id": generated_v1_id,
            "stable_id_generated_at_import": True,
            "manifest": "manifest_v1.json",
            "wav": "shard_004_legacy.wav",
            "sample_count": int(len(legacy_pcm)),
            "timbre_id": SPEAKER_ID,
            "unit_symbols": ["旧", "版", "legacy", "停顿"],
        }
    )

    anomaly_shard = json.loads(json.dumps(v2_shards[0]))
    anomaly_shard["shard_id"] = "anomaly_timeline"
    duration_ms = anomaly_shard["sample_count"] * 1000.0 / SAMPLE_RATE
    anomaly_shard["units"][0]["end_ms"] = round(duration_ms + 50.0, 6)
    timeline_manifest = {
        "format": "tonelab.manifest.v2",
        "structure_version": 2,
        "project_id": "anomaly-timeline",
        "audio": {"sample_rate": SAMPLE_RATE, "channels": CHANNELS, "bit_depth": BIT_DEPTH},
        "declared_total_samples": int(anomaly_shard["sample_count"]),
        "shards": [anomaly_shard],
    }
    write_manifest(DATA_DIR / "anomaly_timeline.json", timeline_manifest)
    numeric_shard = json.loads(json.dumps(v2_shards[1]))
    numeric_shard["shard_id"] = "anomaly_numeric"
    numeric_shard["pitch_curve"][2][1] = "NaN"
    numeric_manifest = {
        "format": "tonelab.manifest.v2",
        "structure_version": 2,
        "project_id": "anomaly-numeric",
        "audio": {"sample_rate": SAMPLE_RATE, "channels": CHANNELS, "bit_depth": BIT_DEPTH},
        "declared_total_samples": int(numeric_shard["sample_count"]),
        "shards": [numeric_shard],
    }
    write_manifest(DATA_DIR / "anomaly_numeric.json", numeric_manifest)

    index = {
        "format": "tonelab.demo-index",
        "seed": SEED,
        "sample_rate": SAMPLE_RATE,
        "channels": CHANNELS,
        "bit_depth": BIT_DEPTH,
        "manifests": ["manifest_v2.json", "manifest_v1.json"],
        "total_samples": sum(item["sample_count"] for item in shard_records),
        "shard_count": len(shard_records),
        "shard_order": shard_records,
        "anomalies": ["anomaly_timeline.json", "anomaly_numeric.json"],
    }
    write_manifest(DATA_DIR / "index.json", index)
    print(json.dumps(index, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

