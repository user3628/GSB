"""Deterministic WAV shard and manifest generation with a fixed seed."""

import json
import os

import numpy as np

from .audio import write_wav
from .config import TARGET_SAMPLE_RATE
from .hashing import canonical_json, rng_from_seed
from .vocabulary import signature_bin, VOCABULARY

SEED = 20261123
SILENCE_GAIN = 40.0
VOICED_GAIN = 9000.0
IMPULSE_GAIN = 26000.0
WORD_FRAMES = 12
SHORT_PAUSE_FRAMES = 3
GAP_FRAMES = 9
LEAD_FRAMES = 4
TRAIL_FRAMES = 6


def _frame_noise(rng, frame_length):
    return rng.standard_normal(frame_length)


def _word_frame(seed, word, word_frame_index, frame_config):
    """Deterministic signature tone at the word's private FFT bin."""
    frame_length = frame_config.frame_length
    sample_rate = frame_config.sample_rate
    marker_bins = frame_config.marker_bins
    center_bin = signature_bin(word, marker_bins)
    frequency = center_bin * sample_rate / frame_length
    time_axis = np.arange(frame_length, dtype=np.float64)
    phase_rng = rng_from_seed(seed, word, word_frame_index, "phase")
    phase = (phase_rng.random() * 2.0 - 1.0) * np.pi
    tone = 0.55 * np.sin(
        2.0 * np.pi * frequency * time_axis / sample_rate + phase
    )
    noise_rng = rng_from_seed(seed, word, word_frame_index, "noise")
    shaped = tone + 0.05 * noise_rng.standard_normal(frame_length)
    return shaped


def _add_impulse(samples, center, rng, frame_length):
    half = frame_length // 8
    lo = max(0, center - half)
    hi = min(len(samples), center + half)
    envelope = np.hanning(hi - lo)
    samples[lo:hi] += IMPULSE_GAIN * envelope * rng.choice([-1.0, 1.0])


def synthesize_shard(words, frame_config, seed, impulse_frames=()):
    """Synthesize one shard and its frame-aligned reference annotations."""
    rng = rng_from_seed(seed)
    hop = frame_config.hop_length
    length = frame_config.frame_length
    total_frames = (
        LEAD_FRAMES
        + len(words) * WORD_FRAMES
        + (len(words) - 1) * (SHORT_PAUSE_FRAMES + GAP_FRAMES)
        + TRAIL_FRAMES
    )
    total_samples = (total_frames - 1) * hop + length
    buffer = np.zeros(total_samples, dtype=np.float64)
    window = np.hanning(length)
    references = []
    cursor = LEAD_FRAMES
    for word in words:
        start_frame = cursor
        for frame_index in range(WORD_FRAMES):
            frame = _word_frame(seed, word, frame_index, frame_config)
            start = (cursor + frame_index) * hop
            buffer[start : start + length] += frame * window * VOICED_GAIN
        end_frame = cursor + WORD_FRAMES - 1
        references.append(
            {
                "word": word,
                "start_ms": int(round(start_frame * frame_config.hop_ms)),
                "end_ms": int(round((end_frame + 1) * frame_config.hop_ms)),
                "start_frame": start_frame,
                "end_frame": end_frame,
            }
        )
        cursor += WORD_FRAMES + SHORT_PAUSE_FRAMES + GAP_FRAMES
    silence_rng = rng_from_seed(seed + 77)
    buffer += silence_rng.standard_normal(total_samples) * SILENCE_GAIN
    tail = silence_rng.standard_normal(hop // 2) * SILENCE_GAIN
    buffer = np.concatenate([buffer, tail])
    for impulse_frame in impulse_frames:
        _add_impulse(buffer, impulse_frame * hop + length // 2, rng, length)
    samples = np.clip(np.rint(buffer), -32768, 32767).astype(np.int16)
    return samples, references, total_frames


def _spectral_declaration(frame_config):
    return {
        "frame_ms": frame_config.frame_ms,
        "hop_ms": frame_config.hop_ms,
        "bands": frame_config.bands,
        "window": "hanning",
        "frame_length_samples": frame_config.frame_length,
        "hop_length_samples": frame_config.hop_length,
    }


def generate_healthy(data_dir, frame_config):
    os.makedirs(data_dir, exist_ok=True)
    shard_plans = [
        ("shard-001", ["hello", "world", "alpha确认", "weather", "report"], (6,)),
        ("shard-002", ["meeting", "check记录", "system", "ready"], (18,)),
        ("shard-003", ["pipeline", "signal同步", "确认", "结束"], (32,)),
    ]
    manifest_shards = []
    for order, (shard_id, words, impulses) in enumerate(shard_plans, start=1):
        samples, references, total_frames = synthesize_shard(
            words, frame_config, stable_seed(shard_id), impulse_frames=impulses
        )
        wav_name = f"{shard_id}.wav"
        write_wav(os.path.join(data_dir, wav_name), TARGET_SAMPLE_RATE, samples)
        manifest_shards.append(
            {
                "shard_id": shard_id,
                "order": order,
                "wav_file": wav_name,
                "declared_samples": int(len(samples)),
                "spectral": _spectral_declaration(frame_config),
                "reference_words": references,
            }
        )
    manifest = {
        "manifest_version": "1.0",
        "sample_rate": TARGET_SAMPLE_RATE,
        "frame_ms": frame_config.frame_ms,
        "hop_ms": frame_config.hop_ms,
        "bands": frame_config.bands,
        "declared_total_samples": int(
            sum(entry["declared_samples"] for entry in manifest_shards)
        ),
        "shards": manifest_shards,
    }
    manifest_path = os.path.join(data_dir, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as handle:
        handle.write(canonical_json(manifest))
    return manifest_path, manifest


def stable_seed(shard_id):
    return SEED + sum(ord(char) for char in shard_id)


def generate_injections(injection_dir, frame_config):
    """Create input-file based injection fixtures; frame params are CLI injected."""
    os.makedirs(injection_dir, exist_ok=True)
    base_words = ["hello", "system", "确认"]
    samples, references, _ = synthesize_shard(
        base_words, frame_config, stable_seed("inject-base")
    )
    fixtures = {}

    length_samples = samples.copy()
    write_wav(
        os.path.join(injection_dir, "length_mismatch.wav"),
        TARGET_SAMPLE_RATE,
        length_samples,
    )
    fixtures["length_mismatch"] = {
        "wav_file": "length_mismatch.wav",
        "declared_samples": int(len(length_samples) + frame_config.hop_length),
        "reference_words": references,
    }

    write_wav(
        os.path.join(injection_dir, "rate_conflict.wav"), 8000, samples
    )
    fixtures["rate_conflict"] = {
        "wav_file": "rate_conflict.wav",
        "declared_samples": int(len(samples)),
        "reference_words": references,
        "actual_sample_rate": 8000,
    }

    silent = np.zeros(len(samples), dtype=np.int16)
    write_wav(os.path.join(injection_dir, "silent.wav"), TARGET_SAMPLE_RATE, silent)
    fixtures["silent"] = {
        "wav_file": "silent.wav",
        "declared_samples": int(len(silent)),
        "reference_words": references,
    }

    rng = rng_from_seed(SEED + 909)
    clipped = np.clip(
        np.rint(samples.astype(np.int32) + rng.standard_normal(len(samples)) * 20000),
        -32768,
        32767,
    ).astype(np.int16)
    write_wav(os.path.join(injection_dir, "clipped.wav"), TARGET_SAMPLE_RATE, clipped)
    fixtures["clipped"] = {
        "wav_file": "clipped.wav",
        "declared_samples": int(len(clipped)),
        "reference_words": references,
    }

    manifest_shards = []
    for order, (name, spec) in enumerate(fixtures.items(), start=1):
        manifest_shards.append(
            {
                "shard_id": f"inject-{name}",
                "order": order,
                "wav_file": spec["wav_file"],
                "declared_samples": spec["declared_samples"],
                "spectral": _spectral_declaration(frame_config),
                "reference_words": spec.get("reference_words", []),
            }
        )
    manifest = {
        "manifest_version": "1.0",
        "sample_rate": TARGET_SAMPLE_RATE,
        "frame_ms": frame_config.frame_ms,
        "hop_ms": frame_config.hop_ms,
        "bands": frame_config.bands,
        "declared_total_samples": int(
            sum(entry["declared_samples"] for entry in manifest_shards)
        ),
        "shards": manifest_shards,
    }
    manifest_path = os.path.join(injection_dir, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as handle:
        handle.write(canonical_json(manifest))
    return manifest_path, manifest

def generate_single_injection(injection_dir, kind, frame_config):
    """Build a one-shard manifest that isolates a single injection kind."""
    full_path, full_manifest = generate_injections(injection_dir, frame_config)
    target = next(
        entry for entry in full_manifest["shards"] if entry["shard_id"] == f"inject-{kind}"
    )
    single = {
        "manifest_version": "1.0",
        "sample_rate": full_manifest["sample_rate"],
        "frame_ms": full_manifest["frame_ms"],
        "hop_ms": full_manifest["hop_ms"],
        "bands": full_manifest["bands"],
        "declared_total_samples": int(target["declared_samples"]),
        "shards": [dict(target, order=1)],
    }
    path = os.path.join(injection_dir, f"manifest_{kind}.json")
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(canonical_json(single))
    return path
