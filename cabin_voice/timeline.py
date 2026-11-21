from __future__ import annotations

from typing import Any

from .canonical import digest_obj
from .config import ZONE_ORDER
from .errors import invalid_interval


def interval_id(shard_id: str, zone_id: str, start_ms: int, end_ms: int, kind: str = "speech") -> str:
    return "iv-" + digest_obj(
        {
            "shard_id": shard_id,
            "zone_id": zone_id,
            "start_ms": start_ms,
            "end_ms": end_ms,
            "kind": kind,
        }
    )[:16]


def apply_activity_operation(
    intervals: list[list[int]],
    operation: dict[str, Any],
    *,
    shard_id: str,
    zone_id: str,
) -> list[list[int]]:
    kind = operation.get("type")
    at_ms = operation.get("at_ms")
    if not isinstance(at_ms, int):
        raise invalid_interval(
            "activity operation requires integer at_ms",
            shard_id=shard_id,
            zone_id=zone_id,
            start_ms=at_ms,
        )
    ordered = sorted(([int(start), int(end)] for start, end in intervals), key=lambda pair: pair[0])
    if kind == "split":
        for index, (start, end) in enumerate(ordered):
            if start < at_ms < end:
                replacement = [[start, at_ms], [at_ms, end]]
                ordered[index : index + 1] = replacement
                return ordered
        raise invalid_interval(
            "split point must fall strictly inside one active interval",
            shard_id=shard_id,
            zone_id=zone_id,
            start_ms=at_ms,
            end_ms=at_ms,
        )
    if kind == "merge":
        for index in range(len(ordered) - 1):
            left_start, left_end = ordered[index]
            right_start, right_end = ordered[index + 1]
            if left_end == at_ms == right_start:
                ordered[index : index + 2] = [[left_start, right_end]]
                return ordered
        raise invalid_interval(
            "merge point must join two adjacent active intervals",
            shard_id=shard_id,
            zone_id=zone_id,
            start_ms=at_ms,
            end_ms=at_ms,
        )
    raise invalid_interval(
        "unsupported activity operation",
        shard_id=shard_id,
        zone_id=zone_id,
        operation=kind,
    )


def effective_activity_for_shard(
    shard_id: str,
    base_activity: dict[str, list[list[int]]],
    operations: list[dict[str, Any]],
) -> dict[str, list[list[int]]]:
    result = {
        zone_id: [[int(start), int(end)] for start, end in base_activity.get(zone_id, [])]
        for zone_id in ZONE_ORDER
    }
    for operation in sorted(operations, key=lambda item: item["edit_version"]):
        zone_id = operation["zone_id"]
        result[zone_id] = apply_activity_operation(
            result[zone_id],
            operation,
            shard_id=shard_id,
            zone_id=zone_id,
        )
    return result


def compute_overlaps(activity: dict[str, list[list[int]]]) -> list[dict[str, Any]]:
    overlaps: list[dict[str, Any]] = []
    for first_index, first_zone in enumerate(ZONE_ORDER):
        for second_zone in ZONE_ORDER[first_index + 1 :]:
            for first_start, first_end in activity.get(first_zone, []):
                for second_start, second_end in activity.get(second_zone, []):
                    start = max(first_start, second_start)
                    end = min(first_end, second_end)
                    if end > start:
                        overlaps.append(
                            {
                                "zones": [first_zone, second_zone],
                                "start_ms": start,
                                "end_ms": end,
                                "duration_ms": end - start,
                            }
                        )
    overlaps.sort(
        key=lambda item: (
            item["start_ms"],
            item["end_ms"],
            ZONE_ORDER.index(item["zones"][0]),
            ZONE_ORDER.index(item["zones"][1]),
        )
    )
    return overlaps


def build_timeline(
    shards: list[dict[str, Any]],
    effective_activity: dict[str, dict[str, list[list[int]]]],
    priorities: dict[str, dict[str, int]],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for shard in shards:
        shard_id = shard["shard_id"]
        activity = effective_activity[shard_id]
        overlaps = compute_overlaps(activity)
        overlap_zones_by_interval: dict[tuple[str, int, int], set[str]] = {}
        for overlap in overlaps:
            first, second = overlap["zones"]
            for zone in (first, second):
                for start, end in activity.get(zone, []):
                    if max(start, overlap["start_ms"]) < min(end, overlap["end_ms"]):
                        overlap_zones_by_interval.setdefault((zone, start, end), set()).add(
                            second if zone == first else first
                        )
        zones: dict[str, Any] = {}
        for zone_id in ZONE_ORDER:
            intervals = []
            for start, end in activity.get(zone_id, []):
                overlapping_with = sorted(
                    overlap_zones_by_interval.get((zone_id, start, end), set()),
                    key=ZONE_ORDER.index,
                )
                intervals.append(
                    {
                        "interval_id": interval_id(shard_id, zone_id, start, end),
                        "shard_id": shard_id,
                        "zone_id": zone_id,
                        "start_ms": start,
                        "end_ms": end,
                        "duration_ms": end - start,
                        "kind": "speech",
                        "source_zone": zone_id,
                        "priority": priorities[shard_id][zone_id],
                        "overlap": bool(overlapping_with),
                        "overlap_zones": overlapping_with,
                    }
                )
            zones[zone_id] = intervals
        result[shard_id] = {
            "shard_id": shard_id,
            "zones": zones,
            "overlaps": overlaps,
            "has_overlap_activity": bool(overlaps),
            "activity_digest": digest_obj(activity),
        }
    return result
