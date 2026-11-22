"""Stage 2: deterministic resampling with ffill, linear and nearest methods."""

from __future__ import annotations

from collections import Counter
from typing import Any

import numpy as np


def _missing(
    index: int,
    timestamp_ms: int,
    reason: str,
    *,
    source_timestamps_ms: list[int] | None = None,
) -> dict[str, Any]:
    return {
        "index": index,
        "timestamp_ms": timestamp_ms,
        "value": None,
        "method": "missing",
        "requested_method": None,
        "source_timestamps_ms": source_timestamps_ms or [],
        "extrapolated": False,
        "clamped": False,
        "reason": reason,
    }


def resample_channel(
    known_pairs: list[tuple[int, float]],
    grid_points: list[dict[str, int]],
    *,
    method: str,
    max_gap_ms: int,
    allow_extrapolation: bool,
    valid_range: dict[str, float] | None,
) -> list[dict[str, Any]]:
    if not known_pairs:
        return [
            _missing(point["index"], point["timestamp_ms"], "source_has_no_valid_values")
            for point in grid_points
        ]
    known_pairs = sorted(known_pairs, key=lambda pair: pair[0])
    timestamps = np.asarray([pair[0] for pair in known_pairs], dtype=np.int64)
    values = np.asarray([pair[1] for pair in known_pairs], dtype=np.float64)
    output: list[dict[str, Any]] = []

    for point in grid_points:
        target = int(point["timestamp_ms"])
        index = int(np.searchsorted(timestamps, target, side="left"))
        if index < len(timestamps) and int(timestamps[index]) == target:
            output.append(
                {
                    "index": point["index"],
                    "timestamp_ms": target,
                    "value": float(values[index]),
                    "method": "exact",
                    "requested_method": method,
                    "source_timestamps_ms": [target],
                    "extrapolated": False,
                    "clamped": False,
                    "reason": None,
                }
            )
            continue

        left = index - 1
        right = index
        if left >= 0 and right < len(timestamps):
            previous_timestamp = int(timestamps[left])
            next_timestamp = int(timestamps[right])
            gap_ms = next_timestamp - previous_timestamp
            if gap_ms > max_gap_ms:
                output.append(
                    _missing(
                        point["index"],
                        target,
                        "gap_exceeds_max_gap_ms",
                        source_timestamps_ms=[previous_timestamp, next_timestamp],
                    )
                )
                continue
            if method == "ffill":
                output.append(
                    {
                        "index": point["index"],
                        "timestamp_ms": target,
                        "value": float(values[left]),
                        "method": "ffill",
                        "requested_method": method,
                        "source_timestamps_ms": [previous_timestamp],
                        "extrapolated": False,
                        "clamped": False,
                        "reason": None,
                    }
                )
                continue
            if method == "nearest":
                previous_distance = target - previous_timestamp
                next_distance = next_timestamp - target
                chosen = left if previous_distance <= next_distance else right
                output.append(
                    {
                        "index": point["index"],
                        "timestamp_ms": target,
                        "value": float(values[chosen]),
                        "method": "nearest",
                        "requested_method": method,
                        "source_timestamps_ms": [int(timestamps[chosen])],
                        "extrapolated": False,
                        "clamped": False,
                        "reason": None,
                    }
                )
                continue

            # Linear interpolation is performed in the floating-point domain.
            fraction = np.float64(target - previous_timestamp) / np.float64(gap_ms)
            interpolated = np.float64(values[left]) + (
                np.float64(values[right]) - np.float64(values[left])
            ) * fraction
            clamped = False
            if valid_range is not None:
                lower = float(valid_range["min"])
                upper = float(valid_range["max"])
                if interpolated < lower:
                    interpolated = np.float64(lower)
                    clamped = True
                elif interpolated > upper:
                    interpolated = np.float64(upper)
                    clamped = True
            output.append(
                {
                    "index": point["index"],
                    "timestamp_ms": target,
                    "value": float(interpolated),
                    "method": "linear",
                    "requested_method": method,
                    "source_timestamps_ms": [previous_timestamp, next_timestamp],
                    "extrapolated": False,
                    "clamped": clamped,
                    "reason": None,
                }
            )
            continue

        # Outside the observed interval. Only ffill/nearest may extrapolate.
        if not allow_extrapolation:
            output.append(
                _missing(point["index"], target, "outside_observed_coverage")
            )
            continue
        if left < 0:
            next_timestamp = int(timestamps[0])
            distance = next_timestamp - target
            if method == "nearest" and distance <= max_gap_ms:
                output.append(
                    {
                        "index": point["index"],
                        "timestamp_ms": target,
                        "value": float(values[0]),
                        "method": "nearest",
                        "requested_method": method,
                        "source_timestamps_ms": [next_timestamp],
                        "extrapolated": True,
                        "clamped": False,
                        "reason": None,
                    }
                )
            else:
                reason = (
                    "extrapolation_exceeds_max_gap_ms"
                    if method == "nearest"
                    else "method_does_not_extrapolate_backwards"
                )
                output.append(
                    _missing(
                        point["index"],
                        target,
                        reason,
                        source_timestamps_ms=[next_timestamp],
                    )
                )
            continue

        previous_timestamp = int(timestamps[-1])
        distance = target - previous_timestamp
        if method in {"ffill", "nearest"} and distance <= max_gap_ms:
            output.append(
                {
                    "index": point["index"],
                    "timestamp_ms": target,
                    "value": float(values[-1]),
                    "method": method,
                    "requested_method": method,
                    "source_timestamps_ms": [previous_timestamp],
                    "extrapolated": True,
                    "clamped": False,
                    "reason": None,
                }
            )
        else:
            reason = (
                "extrapolation_exceeds_max_gap_ms"
                if method in {"ffill", "nearest"}
                else "linear_does_not_extrapolate"
            )
            output.append(
                _missing(
                    point["index"],
                    target,
                    reason,
                    source_timestamps_ms=[previous_timestamp],
                )
            )
    return output


def resample_source(
    normalized_source: dict[str, Any],
    grid: dict[str, Any],
    config: dict[str, Any],
    source_id: str,
) -> dict[str, Any]:
    source_config = config["sources"][source_id]
    interpolation = config["interpolation"]["per_source"].get(
        source_id, config["interpolation"]["default"]
    )
    max_gap_ms = int(config["resample"]["max_gap_ms"])
    allow_extrapolation = bool(config["resample"]["allow_extrapolation"])
    channels: dict[str, Any] = {}
    method_counter: Counter[str] = Counter()
    missing_counter: Counter[str] = Counter()

    canonical_channels = sorted(set(source_config["channel_mapping"].values()))
    for channel_id in canonical_channels:
        pairs: list[tuple[int, float]] = []
        for record in normalized_source["records"]:
            value = record["channels"].get(channel_id)
            if value is not None:
                pairs.append((int(record["timestamp_ms"]), float(value)))
        channel_points = resample_channel(
            pairs,
            grid["points"],
            method=interpolation,
            max_gap_ms=max_gap_ms,
            allow_extrapolation=allow_extrapolation,
            valid_range=source_config["valid_ranges"].get(channel_id),
        )
        for point in channel_points:
            method_counter[point["method"]] += 1
            if point["reason"]:
                missing_counter[point["reason"]] += 1
        label = config["channels"].get(channel_id, {})
        channels[channel_id] = {
            "label": label.get("label", channel_id),
            "unit": label.get("unit", ""),
            "points": channel_points,
        }

    return {
        "schema_version": 1,
        "source_id": source_id,
        "grid_fingerprint": grid["fingerprint"],
        "interpolation": interpolation,
        "tolerance_ms": int(config["tolerance_ms"]),
        "max_gap_ms": max_gap_ms,
        "allow_extrapolation": allow_extrapolation,
        "channels": channels,
        "summary": {
            "method_counts": {key: method_counter[key] for key in sorted(method_counter)},
            "missing_reason_counts": {
                key: missing_counter[key] for key in sorted(missing_counter)
            },
        },
    }

