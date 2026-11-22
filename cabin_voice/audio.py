"""WAV slicing and deterministic frame-level multi-zone VAD."""

from __future__ import annotations

import math
import wave
from pathlib import Path
from typing import Any

import numpy as np

from .errors import CabinVoiceError
from .timeline import overlap_markers
from .util import sha256_json


def read_pcm16_mono(path: str | Path) -> tuple[np.ndarray, int]:
    try:
        with wave.open(str(path), "rb") as handle:
            channels = handle.getnchannels()
            width = handle.getsampwidth()
            rate = handle.getframerate()
            if channels != 1 or width != 2:
                raise CabinVoiceError(
                    "E_INVALID_WAV",
                    "WAV must be mono 16-bit PCM",
                    {"path": str(path), "channels": channels, "sample_width": width},
                )
            raw = handle.readframes(handle.getnframes())
    except FileNotFoundError as exc:
        raise CabinVoiceError("E_NOT_FOUND", "WAV not found", {"path": str(path)}) from exc
    except wave.Error as exc:
        raise CabinVoiceError("E_INVALID_WAV", "unable to parse WAV", {"path": str(path)}) from exc
    samples = np.frombuffer(raw, dtype="<i2")
    return samples, rate


def frame_scores(
    signal: np.ndarray,
    sample_rate: int,
    detection: dict[str, Any],
    *,
    zone_id: str,
) -> dict[str, Any]:
    frame_length = max(2, int(round(float(detection["frame_ms"]) * sample_rate / 1000.0)))
    hop_length = max(1, int(round(float(detection["hop_ms"]) * sample_rate / 1000.0)))
    if len(signal) < frame_length:
        padded = np.zeros(frame_length, dtype=np.float64)
        padded[: len(signal)] = signal.astype(np.float64)
        signal = padded
    window = np.hanning(frame_length)
    samples = signal.astype(np.float64) / 32768.0
    energy_floor = float(detection["energy_floor_db"])
    flatness_weight = float(detection["flatness_weight"])
    epsilon = 1e-12
    scores: list[float] = []
    energies: list[float] = []
    flatness_values: list[float] = []
    starts: list[int] = []
    frame_count = 1 + max(0, (len(samples) - frame_length) // hop_length)
    for index in range(frame_count):
        start = index * hop_length
        frame = samples[start : start + frame_length] * window
        mean_power = float(np.mean(frame * frame))
        energy_db = max(energy_floor, 10.0 * math.log10(mean_power + epsilon))
        spectrum = np.abs(np.fft.rfft(frame)) ** 2
        spectrum = spectrum + epsilon
        geometric = float(np.exp(np.mean(np.log(spectrum))))
        arithmetic = float(np.mean(spectrum))
        flatness = geometric / arithmetic if arithmetic > 0 else 0.0
        flatness_db = 10.0 * math.log10(max(flatness, epsilon))
        score = energy_db - flatness_weight * flatness_db
        if not math.isfinite(score):
            raise CabinVoiceError(
                "E_NONFINITE_SCORE",
                "frame score is not finite",
                {
                    "zone_id": zone_id,
                    "frame_index": index,
                    "energy_db": energy_db,
                    "flatness_db": flatness_db,
                    "score": score,
                },
            )
        starts.append(index)
        energies.append(energy_db)
        flatness_values.append(flatness_db)
        scores.append(score)
    return {
        "frame_length_samples": frame_length,
        "hop_length_samples": hop_length,
        "frame_starts": starts,
        "scores": scores,
        "energy_db": energies,
        "flatness_db": flatness_values,
    }


def _frames_to_intervals(
    scores: list[float],
    detection: dict[str, Any],
    *,
    sample_rate: int,
    hop_length: int,
    frame_length: int,
    duration_ms: int,
    zone_id: str,
) -> list[dict[str, Any]]:
    enter = float(detection["enter_threshold_db"])
    exit_ = float(detection["exit_threshold_db"])
    min_speech = int(detection["min_speech_ms"])
    min_silence = int(detection["min_silence_ms"])
    raw: list[tuple[int, int]] = []
    active = False
    start_index = 0
    for index, score in enumerate(scores):
        if not active and score >= enter:
            active = True
            start_index = index
        elif active and score <= exit_:
            if index > start_index:
                raw.append((start_index, index))
            active = False
    if active and len(scores) > start_index:
        raw.append((start_index, len(scores)))

    intervals: list[dict[str, Any]] = []
    for left, right in raw:
        start_ms = int(round(left * hop_length * 1000 / sample_rate))
        last_frame_end_sample = min(
            (right - 1) * hop_length + frame_length,
            duration_ms * sample_rate // 1000,
        )
        end_ms = int(round(last_frame_end_sample * 1000.0 / sample_rate))
        start_ms = max(0, min(duration_ms, start_ms))
        end_ms = max(0, min(duration_ms, end_ms))
        if end_ms - start_ms >= min_speech:
            intervals.append({"start_ms": start_ms, "end_ms": end_ms, "kind": "speech"})

    # Fill short silences first, then remove short surviving speech fragments.
    merged: list[dict[str, Any]] = []
    for interval in intervals:
        if merged and interval["start_ms"] - merged[-1]["end_ms"] < min_silence:
            merged[-1]["end_ms"] = max(merged[-1]["end_ms"], interval["end_ms"])
        else:
            merged.append(dict(interval))
    return [item for item in merged if item["end_ms"] - item["start_ms"] >= min_speech]


def detect_chunk(
    wav_path: str | Path,
    chunk: dict[str, Any],
    detection: dict[str, Any],
    *,
    sample_rate: int,
    activity_ops: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    from .timeline import apply_activity_operations

    samples, actual_rate = read_pcm16_mono(wav_path)
    if actual_rate != sample_rate:
        raise CabinVoiceError(
            "E_INVALID_WAV",
            "WAV sample rate does not match manifest",
            {"chunk_id": chunk["chunk_id"], "expected": sample_rate, "actual": actual_rate},
        )
    raw_by_zone: dict[str, list[dict[str, Any]]] = {}
    diagnostics: dict[str, Any] = {}
    for segment in chunk["segments"]:
        zone_id = str(segment["zone_id"])
        offset = int(segment["offset_samples"])
        count = int(segment["sample_count"])
        if offset < 0 or count <= 0 or offset + count > len(samples):
            raise CabinVoiceError(
                "E_ZONE_OFFSET_OUT_OF_BOUNDS",
                "zone segment exceeds WAV frame count",
                {
                    "zone_id": zone_id,
                    "chunk_id": chunk["chunk_id"],
                    "parameter": "offset_samples",
                    "offset_samples": offset,
                    "sample_count": count,
                    "wav_frames": int(len(samples)),
                },
            )
        signal = samples[offset : offset + count]
        duration_ms = int(round(count * 1000.0 / sample_rate))
        features = frame_scores(signal, sample_rate, detection, zone_id=zone_id)
        intervals = _frames_to_intervals(
            features["scores"],
            detection,
            sample_rate=sample_rate,
            hop_length=features["hop_length_samples"],
            frame_length=features["frame_length_samples"],
            duration_ms=duration_ms,
            zone_id=zone_id,
        )
        raw_by_zone[zone_id] = intervals
        diagnostics[zone_id] = {
            "duration_ms": duration_ms,
            "frame_count": len(features["scores"]),
            "frame_length_samples": features["frame_length_samples"],
            "hop_length_samples": features["hop_length_samples"],
            "mean_score_db": round(sum(features["scores"]) / len(features["scores"]), 6),
            "active_frame_count": sum(
                1
                for score in features["scores"]
                if score >= float(detection["enter_threshold_db"])
            ),
        }
    effective = apply_activity_operations(raw_by_zone, activity_ops or [], chunk_id=chunk["chunk_id"])
    zones = []
    for segment in chunk["segments"]:
        zone_id = str(segment["zone_id"])
        zones.append(
            {
                "zone_id": zone_id,
                "raw_intervals": raw_by_zone[zone_id],
                "effective_intervals": effective[zone_id],
                **diagnostics[zone_id],
            }
        )
    return {
        "chunk_id": chunk["chunk_id"],
        "zones": zones,
        "overlap_markers": overlap_markers(effective),
        "interval_digest": sha256_json(effective),
    }
