"""Stage 1: strict shard validation, cleaning and deterministic grid creation."""

from __future__ import annotations

import json
import math
from bisect import bisect_left
from typing import Any

from .errors import TelemetryError
from .fingerprints import grid_fingerprint


def parse_and_normalize_source(
    sensor_id: str,
    raw_bytes: bytes,
    source_config: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return ``(normalized_artifact, cleaning_report)`` or raise diagnostics."""

    text = raw_bytes.decode("utf-8")
    raw_records: list[dict[str, Any]] = []
    malformed: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            malformed.append(
                {"line": line_number, "reason": "invalid_json", "detail": str(exc)}
            )
            continue
        if not isinstance(record, dict):
            malformed.append({"line": line_number, "reason": "record_not_object"})
            continue
        raw_records.append({"line": line_number, "record": record})
    if malformed:
        raise TelemetryError(
            "E_SOURCE_INVALID",
            "源分片包含不可解析记录 / shard contains malformed records",
            {"sensor_id": sensor_id, "malformed": malformed},
        )
    if not raw_records:
        raise TelemetryError(
            "E_SOURCE_INVALID",
            "源分片为空 / source shard is empty",
            {"sensor_id": sensor_id},
        )

    timestamp_entries: list[dict[str, Any]] = []
    duplicate_issues: list[dict[str, Any]] = []
    non_monotonic_issues: list[dict[str, Any]] = []
    seen_timestamps: dict[int, list[int]] = {}
    previous_timestamp: int | None = None
    for entry in raw_records:
        line_number = int(entry["line"])
        record = entry["record"]
        if record.get("sensor_id") != sensor_id:
            raise TelemetryError(
                "E_SOURCE_INVALID",
                "分片 sensor_id 与文件名/配置不一致 / shard sensor_id mismatch",
                {
                    "sensor_id": sensor_id,
                    "line": line_number,
                    "found": record.get("sensor_id"),
                },
            )
        timestamp = record.get("timestamp_ms")
        if isinstance(timestamp, bool) or not isinstance(timestamp, int):
            raise TelemetryError(
                "E_TIME_INVALID",
                "timestamp_ms 必须是整数 / timestamp_ms must be an integer",
                {"sensor_id": sensor_id, "line": line_number, "timestamp_ms": timestamp},
            )
        timestamp_entries.append(
            {"line": line_number, "timestamp_ms": timestamp, "record": record}
        )
        seen_timestamps.setdefault(timestamp, []).append(line_number)
        if previous_timestamp is not None and timestamp < previous_timestamp:
            non_monotonic_issues.append(
                {
                    "sensor_id": sensor_id,
                    "timestamp_ms": timestamp,
                    "previous_timestamp_ms": previous_timestamp,
                    "line": line_number,
                    "previous_line": timestamp_entries[-2]["line"],
                }
            )
        previous_timestamp = timestamp

    for timestamp, lines in seen_timestamps.items():
        if len(lines) > 1:
            duplicate_issues.append(
                {"sensor_id": sensor_id, "timestamp_ms": timestamp, "lines": lines}
            )

    # Sorting and de-duplication are deliberately performed even on invalid input so
    # diagnostics can expose the exact deterministic canonical preview.
    sorted_entries = sorted(timestamp_entries, key=lambda item: item["timestamp_ms"])
    deduped_preview = []
    last_timestamp = object()
    for entry in sorted_entries:
        timestamp = entry["timestamp_ms"]
        if timestamp == last_timestamp:
            continue
        deduped_preview.append(timestamp)
        last_timestamp = timestamp

    if duplicate_issues or non_monotonic_issues:
        issues = []
        for issue in duplicate_issues:
            issues.append({"kind": "duplicate_timestamp", **issue})
        for issue in non_monotonic_issues:
            issues.append({"kind": "non_monotonic_timestamp", **issue})
        raise TelemetryError(
            "E_TIME_INVALID",
            "时间戳重复或非单调 / duplicate or non-monotonic timestamps",
            {
                "sensor_id": sensor_id,
                "duplicates": duplicate_issues,
                "non_monotonic": non_monotonic_issues,
                "issues": issues,
                "canonical_preview": {
                    "sorted_timestamp_count": len(sorted_entries),
                    "deduped_timestamp_count": len(deduped_preview),
                    "first_timestamps_ms": deduped_preview[:10],
                    "last_timestamps_ms": deduped_preview[-10:],
                },
            },
        )

    accepted_quality = set(source_config["quality_accept"])
    mapping: dict[str, str] = source_config["channel_mapping"]
    valid_ranges: dict[str, dict[str, float]] = source_config["valid_ranges"]
    normalized_records: list[dict[str, Any]] = []
    out_of_range: list[dict[str, Any]] = []
    non_numeric: list[dict[str, Any]] = []
    quality_rejected: list[dict[str, Any]] = []
    unknown_channels: dict[str, int] = {}

    for entry in sorted_entries:
        record = entry["record"]
        timestamp = int(entry["timestamp_ms"])
        quality = record.get("quality", "missing")
        quality_value = quality if isinstance(quality, str) else str(quality)
        raw_values = record.get("channel_values")
        if not isinstance(raw_values, dict):
            raise TelemetryError(
                "E_SOURCE_INVALID",
                "channel_values 必须是映射 / channel_values must be a mapping",
                {"sensor_id": sensor_id, "timestamp_ms": timestamp, "line": entry["line"]},
            )
        for raw_channel in raw_values:
            if raw_channel not in mapping:
                unknown_channels[str(raw_channel)] = unknown_channels.get(str(raw_channel), 0) + 1
        channel_values: dict[str, float | None] = {
            channel_id: None for channel_id in sorted(set(mapping.values()))
        }
        channel_reasons: dict[str, list[str]] = {}
        quality_ok = quality_value in accepted_quality
        if not quality_ok:
            quality_rejected.append(
                {
                    "sensor_id": sensor_id,
                    "timestamp_ms": timestamp,
                    "quality": quality_value,
                    "accepted": sorted(accepted_quality),
                }
            )
            for channel_id in channel_values:
                channel_reasons[channel_id] = ["quality_rejected"]
        else:
            for raw_channel, canonical_channel in mapping.items():
                raw_value = raw_values.get(raw_channel)
                if raw_value is None:
                    channel_reasons.setdefault(canonical_channel, []).append("channel_missing")
                    continue
                if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
                    non_numeric.append(
                        {
                            "sensor_id": sensor_id,
                            "timestamp_ms": timestamp,
                            "channel": canonical_channel,
                            "raw_channel": raw_channel,
                            "value": raw_value,
                        }
                    )
                    channel_reasons.setdefault(canonical_channel, []).append("non_numeric")
                    continue
                numeric_value = float(raw_value)
                if not math.isfinite(numeric_value):
                    non_numeric.append(
                        {
                            "sensor_id": sensor_id,
                            "timestamp_ms": timestamp,
                            "channel": canonical_channel,
                            "raw_channel": raw_channel,
                            "value": str(raw_value),
                        }
                    )
                    channel_reasons.setdefault(canonical_channel, []).append("non_finite")
                    continue
                bounds = valid_ranges.get(canonical_channel)
                if bounds is not None and (
                    numeric_value < bounds["min"] or numeric_value > bounds["max"]
                ):
                    out_of_range.append(
                        {
                            "sensor_id": sensor_id,
                            "timestamp_ms": timestamp,
                            "channel": canonical_channel,
                            "value": numeric_value,
                            "min": bounds["min"],
                            "max": bounds["max"],
                        }
                    )
                    channel_reasons.setdefault(canonical_channel, []).append("out_of_range")
                    continue
                channel_values[canonical_channel] = numeric_value

        valid = quality_ok and any(value is not None for value in channel_values.values())
        normalized_records.append(
            {
                "timestamp_ms": timestamp,
                "quality": quality_value,
                "valid": valid,
                "channels": {key: channel_values[key] for key in sorted(channel_values)},
                "channel_reasons": {
                    key: channel_reasons[key] for key in sorted(channel_reasons)
                },
            }
        )

    cleaning_report = {
        "sensor_id": sensor_id,
        "input_record_count": len(raw_records),
        "sorted_record_count": len(sorted_entries),
        "deduped_timestamp_count": len(deduped_preview),
        "accepted_record_count": sum(1 for record in normalized_records if record["valid"]),
        "quality_rejected_count": len(quality_rejected),
        "out_of_range_count": len(out_of_range),
        "non_numeric_count": len(non_numeric),
        "unknown_channels": {key: unknown_channels[key] for key in sorted(unknown_channels)},
        "quality_rejected": quality_rejected,
        "out_of_range": out_of_range,
        "non_numeric": non_numeric,
        "timestamp_duplicates": [],
        "non_monotonic": [],
    }
    return {
        "sensor_id": sensor_id,
        "records": normalized_records,
        "cleaning_report": cleaning_report,
    }, cleaning_report


def build_grid(config: dict[str, Any], tolerance_ms: int) -> dict[str, Any]:
    grid = config["grid"]
    step_ms = int(grid["step_ms"])
    start_ms = int(grid["start_ms"])
    end_ms = int(grid["end_ms"])
    if step_ms <= 0:
        raise TelemetryError(
            "E_GRID_INVALID",
            "网格步长必须为正 / grid step must be positive",
            {"step_ms": step_ms, "tolerance_ms": tolerance_ms},
        )
    if step_ms > tolerance_ms:
        raise TelemetryError(
            "E_GRID_INVALID",
            "网格步长超过对齐容差 / grid step exceeds tolerance",
            {"step_ms": step_ms, "tolerance_ms": tolerance_ms},
        )
    if end_ms < start_ms:
        raise TelemetryError(
            "E_GRID_INVALID",
            "网格终止时间早于起始时间 / end precedes start",
            {"start_ms": start_ms, "end_ms": end_ms},
        )
    count = ((end_ms - start_ms) // step_ms) + 1
    points = [
        {"index": index, "timestamp_ms": start_ms + index * step_ms}
        for index in range(count)
    ]
    fp = grid_fingerprint(
        {"step_ms": step_ms, "start_ms": start_ms, "end_ms": end_ms, "count": count}
    )
    return {
        "schema_version": 1,
        "step_ms": step_ms,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "count": count,
        "points": points,
        "fingerprint": fp,
    }


def find_exact(known_timestamps: list[int], target: int) -> int | None:
    index = bisect_left(known_timestamps, target)
    if index < len(known_timestamps) and known_timestamps[index] == target:
        return index
    return None

