"""16 kHz mono 16-bit PCM WAV IO and deterministic audio quality checks."""

import wave

import numpy as np

from .errors import (
    ClippedAudioError,
    InputLengthError,
    SampleRateError,
    SilentAudioError,
)

SILENCE_ABS_THRESHOLD = 300
SILENCE_RATIO_THRESHOLD = 0.98
CLIP_ABS_VALUE = 32767
CLIP_RATIO_THRESHOLD = 0.002


def read_wav(path):
    with wave.open(str(path), "rb") as wav:
        channels = wav.getnchannels()
        sample_width = wav.getsampwidth()
        sample_rate = wav.getframerate()
        raw = wav.readframes(wav.getnframes())
    if channels != 1:
        raise SampleRateError("only mono WAV is supported", {"channels": channels})
    if sample_width != 2:
        raise SampleRateError(
            "only 16-bit PCM is supported", {"sample_width_bytes": sample_width}
        )
    samples = np.frombuffer(raw, dtype="<i2").astype(np.int16)
    return sample_rate, samples


def write_wav(path, sample_rate, samples):
    samples = np.asarray(samples, dtype=np.int16)
    with wave.open(str(path), "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(sample_rate)
        wav.writeframes(samples.tobytes())


def validate_audio(sample_rate, samples, manifest_entry):
    """Apply manifest-driven and signal-level validation in a fixed order."""
    if sample_rate != manifest_entry["sample_rate"]:
        raise SampleRateError(
            "actual sample rate conflicts with manifest declaration",
            {
                "shard_id": manifest_entry["shard_id"],
                "actual": sample_rate,
                "declared": manifest_entry["sample_rate"],
            },
        )
    if len(samples) != manifest_entry["declared_samples"]:
        raise InputLengthError(
            "actual sample count conflicts with manifest declaration",
            {
                "shard_id": manifest_entry["shard_id"],
                "actual": int(len(samples)),
                "declared": int(manifest_entry["declared_samples"]),
            },
        )
    silent_ratio = float(np.mean(np.abs(samples.astype(np.int32)) <= SILENCE_ABS_THRESHOLD))
    if silent_ratio > SILENCE_RATIO_THRESHOLD:
        raise SilentAudioError(
            "shard is effectively fully silent",
            {"shard_id": manifest_entry["shard_id"], "silent_ratio": round(silent_ratio, 6)},
        )
    clip_ratio = float(np.mean(np.abs(samples.astype(np.int32)) >= CLIP_ABS_VALUE))
    if clip_ratio > CLIP_RATIO_THRESHOLD:
        raise ClippedAudioError(
            "shard exceeds clipping threshold",
            {"shard_id": manifest_entry["shard_id"], "clip_ratio": round(clip_ratio, 6)},
        )
    return {
        "silent_ratio": round(silent_ratio, 6),
        "clip_ratio": round(clip_ratio, 6),
        "peak_abs": int(np.max(np.abs(samples.astype(np.int32)))),
    }
