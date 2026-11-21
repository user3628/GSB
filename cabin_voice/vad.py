from __future__ import annotations

import math
from typing import Any, Callable

import numpy as np

from .audio import read_zone_signal
from .canonical import digest_obj
from .config import VadConfig
from .errors import PipelineStopped


def _runs(mask: np.ndarray) -> list[tuple[int, int]]:
    runs: list[tuple[int, int]] = []
    start: int | None = None
    for index, value in enumerate(mask.tolist()):
        if value and start is None:
            start = index
        elif not value and start is not None:
            runs.append((start, index))
            start = None
    if start is not None:
        runs.append((start, len(mask)))
    return runs


def _postprocess(mask: np.ndarray, config: VadConfig) -> np.ndarray:
    result = mask.copy()
    min_speech_frames = math.ceil(config.min_speech_ms / config.hop_ms)
    if min_speech_frames > 0:
        for start, end in _runs(result):
            if end - start < min_speech_frames:
                result[start:end] = False
    if config.min_silence_ms > 0:
        runs = _runs(result)
        for (left_start, left_end), (right_start, right_end) in zip(runs, runs[1:]):
            gap_frames = right_start - left_end
            if gap_frames > 0 and gap_frames * config.hop_ms < config.min_silence_ms:
                result[left_end:right_start] = True
    return result


def _frame_intervals(
    mask: np.ndarray,
    *,
    frame_samples: int,
    hop_samples: int,
    sample_rate: int,
    duration_ms: int,
) -> list[list[int]]:
    intervals: list[list[int]] = []
    for start_frame, end_frame in _runs(mask):
        start_ms = int(round(start_frame * hop_samples * 1000.0 / sample_rate))
        end_ms = int(round((end_frame * hop_samples + frame_samples - hop_samples) * 1000.0 / sample_rate))
        start_ms = max(0, min(duration_ms, start_ms))
        end_ms = max(0, min(duration_ms, end_ms))
        if end_ms <= start_ms:
            continue
        if intervals and start_ms <= intervals[-1][1]:
            intervals[-1][1] = max(intervals[-1][1], end_ms)
        else:
            intervals.append([start_ms, end_ms])
    return intervals


def analyze_zone(
    signal: np.ndarray,
    config: VadConfig,
    *,
    sample_rate: int = 16000,
    duration_ms: int,
) -> dict[str, Any]:
    frame_samples = max(2, round(config.frame_ms * sample_rate / 1000.0))
    hop_samples = max(1, round(config.hop_ms * sample_rate / 1000.0))
    if signal.size == 0:
        frame_count = 0
        padded_frame_count = 0
        padding_samples = 0
        frames = np.zeros((0, frame_samples), dtype=np.float64)
    else:
        if signal.size <= frame_samples:
            frame_count = 1
        else:
            frame_count = 1 + math.ceil((signal.size - frame_samples) / hop_samples)
        padded_frame_count = sum(
            1
            for index in range(frame_count)
            if index * hop_samples + frame_samples > signal.size
        )
        padding_samples = sum(
            max(0, index * hop_samples + frame_samples - signal.size)
            for index in range(frame_count)
        )
        frames = np.zeros((frame_count, frame_samples), dtype=np.float64)
        for index in range(frame_count):
            start = index * hop_samples
            available = max(0, min(frame_samples, signal.size - start))
            if available > 0:
                frames[index, :available] = signal[start : start + available]
    window = np.hanning(frame_samples).astype(np.float64)
    windowed = frames * window
    mean_power = np.mean(windowed * windowed, axis=1) if frame_count else np.zeros(0)
    energy_db = 10.0 * np.log10(mean_power + config.frame_epsilon)
    energy_norm = np.clip(
        (energy_db - config.energy_floor_db)
        / (config.energy_ceiling_db - config.energy_floor_db),
        0.0,
        1.0,
    )
    if frame_count:
        spectra = np.fft.rfft(windowed, axis=1)
        power = np.abs(spectra) ** 2
        usable = power[:, 1:]
        arithmetic = np.mean(usable, axis=1) + config.frame_epsilon
        geometric = np.exp(np.mean(np.log(usable + config.frame_epsilon), axis=1))
        flatness = np.clip(geometric / arithmetic, 0.0, 1.0)
    else:
        flatness = np.zeros(0)
    weight_sum = config.energy_weight + config.flatness_weight
    score = (
        config.energy_weight * energy_norm
        + config.flatness_weight * (1.0 - flatness)
    ) / weight_sum
    raw_mask = np.zeros(frame_count, dtype=np.bool_)
    active = False
    for index, value in enumerate(score.tolist()):
        if not active and value >= config.enter_threshold:
            active = True
        elif active and value < config.exit_threshold:
            active = False
        raw_mask[index] = active
    final_mask = _postprocess(raw_mask, config)
    intervals = _frame_intervals(
        final_mask,
        frame_samples=frame_samples,
        hop_samples=hop_samples,
        sample_rate=sample_rate,
        duration_ms=duration_ms,
    )
    frame_records = [
        {
            "index": index,
            "start_ms": int(round(index * hop_samples * 1000.0 / sample_rate)),
            "energy_db": round(float(energy_db[index]), 9),
            "flatness": round(float(flatness[index]), 9),
            "score": round(float(score[index]), 9),
            "active": bool(final_mask[index]),
        }
        for index in range(frame_count)
    ]
    return {
        "sample_rate": sample_rate,
        "frame_samples": frame_samples,
        "hop_samples": hop_samples,
        "frame_count": frame_count,
        "padded_frame_count": padded_frame_count,
        "padding_samples": padding_samples,
        "active_frame_count": int(np.count_nonzero(final_mask)),
        "frame_mask_digest": digest_obj(final_mask.astype(np.uint8).tolist()),
        "intervals": intervals,
        "frames": frame_records,
    }


def compute_vad(
    manifest: dict[str, Any],
    config: VadConfig,
    *,
    should_stop: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    sample_rate = int(manifest["sample_rate"])
    shard_results: dict[str, Any] = {}
    for shard in manifest["shards"]:
        if should_stop is not None and should_stop():
            raise PipelineStopped()
        zones: dict[str, Any] = {}
        zone_lookup = {zone["zone_id"]: zone for zone in shard["zones"]}
        for zone_id, zone in zone_lookup.items():
            signal = read_zone_signal(
                shard["wav_path"],
                zone["start_sample"],
                zone["end_sample"],
            )
            zone_result = analyze_zone(
                signal,
                config,
                sample_rate=sample_rate,
                duration_ms=zone["duration_ms"],
            )
            reference = shard["reference_activity_ms"].get(zone_id, [])
            overlap = 0
            union = 0
            for start_a, end_a in zone_result["intervals"]:
                for start_b, end_b in reference:
                    overlap += max(0, min(end_a, end_b) - max(start_a, start_b))
            for start, end in zone_result["intervals"]:
                union += end - start
            for start, end in reference:
                union += end - start
            union -= overlap
            zone_result["reference_overlap_ratio"] = round(overlap / union, 9) if union else 1.0
            zones[zone_id] = zone_result
        shard_results[shard["shard_id"]] = {
            "shard_id": shard["shard_id"],
            "content_fingerprint": shard["content_fingerprint"],
            "zones": zones,
        }
    return {
        "sample_rate": sample_rate,
        "config_digest": digest_obj(
            {
                "frame_ms": config.frame_ms,
                "hop_ms": config.hop_ms,
                "energy_floor_db": config.energy_floor_db,
                "energy_ceiling_db": config.energy_ceiling_db,
                "energy_weight": config.energy_weight,
                "flatness_weight": config.flatness_weight,
                "enter_threshold": config.enter_threshold,
                "exit_threshold": config.exit_threshold,
                "min_speech_ms": config.min_speech_ms,
                "min_silence_ms": config.min_silence_ms,
            }
        ),
        "shards": shard_results,
    }
