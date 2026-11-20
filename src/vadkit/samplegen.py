"""Deterministic local WAV dataset generator (no network or external data)."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Iterable

import numpy as np
from scipy.io import wavfile

from .util import atomic_write_json, sha256_bytes


SAMPLE_RATE = 16_000
CHANNELS = 1
SAMPLE_WIDTH_BYTES = 2
GENERATOR_VERSION = "vadkit.samplegen.v1"
DEFAULT_SEED = 20_261_120


def _samples(ms: float, sample_rate: int = SAMPLE_RATE) -> int:
    return int(round(sample_rate * ms / 1000.0))


def _speech_wave(
    duration_ms: float,
    rng: np.random.Generator,
    *,
    rms: float,
    f0: float,
    sample_rate: int = SAMPLE_RATE,
) -> np.ndarray:
    count = _samples(duration_ms, sample_rate)
    time = np.arange(count, dtype=np.float64) / sample_rate
    vibrato = 1.0 + 0.035 * np.sin(2.0 * np.pi * 5.1 * time + rng.uniform(0, 2 * np.pi))
    f0_track = f0 * (vibrato + 0.06 * np.sin(2.0 * np.pi * 0.7 * time))
    phase = 2.0 * np.pi * np.cumsum(f0_track) / sample_rate
    wave = np.zeros(count, dtype=np.float64)
    for harmonic in range(1, 13):
        formant_gain = 1.0 + 0.65 * np.exp(-0.5 * ((harmonic * f0 - 700.0) / 180.0) ** 2)
        formant_gain += 0.35 * np.exp(-0.5 * ((harmonic * f0 - 1_400.0) / 260.0) ** 2)
        wave += formant_gain * np.sin(harmonic * phase + rng.uniform(-0.18, 0.18)) / harmonic**1.12
    syllable = 0.82 + 0.18 * np.sin(2.0 * np.pi * 3.2 * time - 0.4)
    wave *= syllable

    fade_count = max(1, _samples(12.0, sample_rate))
    envelope = np.ones(count, dtype=np.float64)
    fade = np.linspace(0.0, 1.0, fade_count, endpoint=True)
    envelope[:fade_count] = fade
    envelope[-fade_count:] = fade[::-1]
    wave *= envelope
    current_rms = np.sqrt(np.mean(wave * wave, dtype=np.float64))
    return wave * (rms / max(current_rms, 1e-12))


def _white_noise(
    duration_ms: float, rng: np.random.Generator, *, rms: float, sample_rate: int = SAMPLE_RATE
) -> np.ndarray:
    count = _samples(duration_ms, sample_rate)
    if count == 0:
        return np.zeros(0, dtype=np.float64)
    noise = rng.standard_normal(count)
    current_rms = np.sqrt(np.mean(noise * noise, dtype=np.float64))
    return noise * (rms / max(current_rms, 1e-12))


def _impulse_train(
    count: int,
    rng: np.random.Generator,
    times_ms: Iterable[float],
    *,
    sample_rate: int = SAMPLE_RATE,
) -> np.ndarray:
    output = np.zeros(count, dtype=np.float64)
    length = _samples(9.0, sample_rate)
    decay = np.exp(-np.arange(length, dtype=np.float64) / (0.0014 * sample_rate))
    for time_ms in times_ms:
        start = _samples(time_ms, sample_rate)
        if start >= count:
            continue
        burst = rng.standard_normal(length) * decay
        stop = min(count, start + length)
        output[start:stop] += 0.32 * burst[: stop - start]
    return output


def _mix_at(
    output: np.ndarray,
    signal: np.ndarray,
    start_ms: float,
    *,
    sample_rate: int = SAMPLE_RATE,
) -> None:
    start = _samples(start_ms, sample_rate)
    stop = min(output.shape[0], start + signal.shape[0])
    output[start:stop] += signal[: stop - start]


def _quantize(signal: np.ndarray) -> np.ndarray:
    scaled = np.clip(signal, -0.86, 0.86) * 32767.0
    return np.rint(scaled).astype(np.int16)


def _write_wav(path: Path, samples: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    wavfile.write(path, SAMPLE_RATE, samples.astype(np.int16, copy=False))


def _clear_chunk(seed: int) -> tuple[np.ndarray, list[list[int]], list[str]]:
    rng = np.random.default_rng(seed)
    total_ms = 2_000.0
    output = _white_noise(total_ms, rng, rms=0.00055)
    segments = [(250.0, 620.0, 138.0), (880.0, 1_300.0, 176.0)]
    for start_ms, end_ms, f0 in segments:
        output_part = _speech_wave(end_ms - start_ms, rng, rms=0.062, f0=f0)
        _mix_at(output, output_part, start_ms)
    return (
        _quantize(output),
        [[250, 620], [880, 1_300]],
        ["clear_speech", "silence", "two_utterances"],
    )


def _low_snr_chunk(seed: int) -> tuple[np.ndarray, list[list[int]], list[str]]:
    rng = np.random.default_rng(seed)
    total_ms = 1_900.0
    output = _white_noise(total_ms, rng, rms=0.009)
    speech = _speech_wave(850.0, rng, rms=0.016, f0=128.0)
    _mix_at(output, speech, 300.0)
    return _quantize(output), [[300, 1_150]], ["low_snr", "single_utterance"]


def _pause_chunk(seed: int) -> tuple[np.ndarray, list[list[int]], list[str]]:
    rng = np.random.default_rng(seed)
    total_ms = 2_100.0
    output = _white_noise(total_ms, rng, rms=0.0022)
    first = _speech_wave(430.0, rng, rms=0.055, f0=118.0)
    second = _speech_wave(570.0, rng, rms=0.058, f0=151.0)
    _mix_at(output, first, 320.0)
    _mix_at(output, second, 870.0)
    breath = _white_noise(120.0, rng, rms=0.0035)
    _mix_at(output, breath, 750.0)
    return (
        _quantize(output),
        [[320, 1_440]],
        ["intra_utterance_breath_pause", "pause_bridged_by_min_silence"],
    )


def _impulse_chunk(seed: int) -> tuple[np.ndarray, list[list[int]], list[str]]:
    rng = np.random.default_rng(seed)
    total_ms = 1_900.0
    output = _white_noise(total_ms, rng, rms=0.0018)
    speech = _speech_wave(610.0, rng, rms=0.060, f0=162.0)
    _mix_at(output, speech, 420.0)
    output += _impulse_train(
        output.shape[0], rng, [180.0, 1_250.0, 1_610.0], sample_rate=SAMPLE_RATE
    )
    return _quantize(output), [[420, 1_030]], ["impulse_noise", "clear_speech"]


def _clipped_chunk(seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    total_ms = 1_000.0
    output = _white_noise(total_ms, rng, rms=0.0015)
    speech = _speech_wave(520.0, rng, rms=0.09, f0=145.0)
    _mix_at(output, speech, 220.0)
    samples = _quantize(output)
    start = _samples(500.0)
    stop = start + _samples(48.0)
    samples[start:stop] = np.where(np.arange(stop - start) % 2 == 0, 32767, -32768)
    return samples


def _raw_fingerprint(samples: np.ndarray) -> str:
    return hashlib.sha256(samples.astype("<i2", copy=False).tobytes()).hexdigest()


def _normal_chunks() -> list[tuple[str, str, list[str], np.ndarray, list[list[int]]]]:
    return [
        ("clear_01", "01_clear.wav", *_clear_chunk(DEFAULT_SEED + 1)),
        ("low_snr_02", "02_low_snr.wav", *_low_snr_chunk(DEFAULT_SEED + 2)),
        ("breath_pause_03", "03_breath_pause.wav", *_pause_chunk(DEFAULT_SEED + 3)),
        ("impulse_04", "04_impulse.wav", *_impulse_chunk(DEFAULT_SEED + 4)),
    ]


def generate_dataset(output_dir: str | Path, *, seed: int = DEFAULT_SEED) -> dict[str, Any]:
    """Generate WAV chunks, a truth manifest, and deterministic anomaly files."""

    target = Path(output_dir)
    target.mkdir(parents=True, exist_ok=True)
    audio_dir = target / "audio"
    anomaly_dir = target / "anomalies"
    audio_dir.mkdir(parents=True, exist_ok=True)
    anomaly_dir.mkdir(parents=True, exist_ok=True)

    chunks: list[dict[str, Any]] = []
    total_declared = 0
    for order, (chunk_id, filename, samples, references, conditions) in enumerate(
        _normal_chunks()
    ):
        wav_path = audio_dir / filename
        _write_wav(wav_path, samples)
        sample_count = int(samples.shape[0])
        total_declared += sample_count
        chunks.append(
            {
                "chunk_id": chunk_id,
                "path": f"audio/{filename}",
                "order": order,
                "sample_count": sample_count,
                "duration_ms": round(sample_count * 1000.0 / SAMPLE_RATE, 3),
                "reference_endpoints_ms": references,
                "conditions": conditions,
                "input_sha256": _raw_fingerprint(samples),
            }
        )

    manifest = {
        "schema_version": "vadkit.dataset.v1",
        "generator": {
            "name": GENERATOR_VERSION,
            "seed": seed,
            "sample_rate": SAMPLE_RATE,
            "channels": CHANNELS,
            "sample_width_bytes": SAMPLE_WIDTH_BYTES,
            "encoding": "pcm_s16le",
        },
        "sample_rate": SAMPLE_RATE,
        "channels": CHANNELS,
        "sample_width_bytes": SAMPLE_WIDTH_BYTES,
        "total_declared_samples": total_declared,
        "chunks": chunks,
    }
    atomic_write_json(target / "manifest.json", manifest, pretty=True)

    silent = np.zeros(_samples(900.0), dtype=np.int16)
    clipped = _clipped_chunk(seed + 11)
    length_samples = _clear_chunk(seed + 12)[0]
    mismatch = length_samples[: _samples(1_000.0)]
    _write_wav(anomaly_dir / "silent_zero.wav", silent)
    _write_wav(anomaly_dir / "clipped_peak.wav", clipped)
    _write_wav(anomaly_dir / "length_mismatch.wav", mismatch)
    mismatch_declared = int(mismatch.shape[0]) + 128
    length_manifest = {
        "schema_version": "vadkit.dataset.v1",
        "generator": {
            "name": GENERATOR_VERSION,
            "seed": seed,
            "sample_rate": SAMPLE_RATE,
            "channels": CHANNELS,
            "sample_width_bytes": SAMPLE_WIDTH_BYTES,
            "encoding": "pcm_s16le",
        },
        "sample_rate": SAMPLE_RATE,
        "channels": CHANNELS,
        "sample_width_bytes": SAMPLE_WIDTH_BYTES,
        "total_declared_samples": mismatch_declared,
        "chunks": [
            {
                "chunk_id": "anomaly_length",
                "path": "length_mismatch.wav",
                "order": 0,
                "sample_count": mismatch_declared,
                "duration_ms": round(mismatch_declared * 1000.0 / SAMPLE_RATE, 3),
                "reference_endpoints_ms": [],
                "conditions": ["declared_length_mismatch"],
            }
        ],
    }
    atomic_write_json(anomaly_dir / "length_mismatch_manifest.json", length_manifest, pretty=True)

    anomalies = {
        "schema_version": "vadkit.anomalies.v1",
        "sample_rate": SAMPLE_RATE,
        "channels": CHANNELS,
        "sample_width_bytes": SAMPLE_WIDTH_BYTES,
        "items": [
            {
                "kind": "silent_input",
                "chunk_id": "anomaly_silent",
                "path": "anomalies/silent_zero.wav",
                "sample_count": int(silent.shape[0]),
                "expected_error": "E_SILENT_INPUT",
            },
            {
                "kind": "clipped_peak",
                "chunk_id": "anomaly_clipped",
                "path": "anomalies/clipped_peak.wav",
                "sample_count": int(clipped.shape[0]),
                "expected_error": "E_FEATURE_MISMATCH",
            },
            {
                "kind": "length_mismatch",
                "chunk_id": "anomaly_length",
                "path": "anomalies/length_mismatch.wav",
                "manifest_path": "anomalies/length_mismatch_manifest.json",
                "declared_sample_count": mismatch_declared,
                "sample_count": mismatch_declared,
                "actual_sample_count": int(mismatch.shape[0]),
                "expected_error": "E_INPUT_LENGTH",
            },
            {
                "kind": "invalid_threshold",
                "chunk_id": "anomaly_config",
                "high_threshold": 0.2,
                "low_threshold": 0.7,
                "expected_error": "E_INVALID_CONFIG",
            },
            {
                "kind": "feature_dimension",
                "chunk_id": "anomaly_feature",
                "path": "audio/01_clear.wav",
                "injected_dimensions": 3,
                "expected_error": "E_FEATURE_MISMATCH",
            },
        ],
    }
    atomic_write_json(anomaly_dir / "manifest.json", anomalies, pretty=True)

    return {
        "manifest": str(target / "manifest.json"),
        "anomaly_manifest": str(anomaly_dir / "manifest.json"),
        "chunk_count": len(chunks),
        "total_samples": total_declared,
        "manifest_sha256": sha256_bytes((target / "manifest.json").read_bytes()),
    }

