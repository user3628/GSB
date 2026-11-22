"""Stage 3: deterministic multi-source fusion, conflict and gap records."""

from __future__ import annotations

import math
from collections import Counter
from typing import Any


def _participant_rank(
    source_id: str, priority: int, tie_break: str, source_order: dict[str, int]
) -> tuple[Any, ...]:
    if tie_break == "source_order":
        return (-priority, source_order[source_id], source_id)
    return (-priority, source_id)


def fuse_alignment(
    grid: dict[str, Any],
    resample_artifacts: dict[str, dict[str, Any]],
    config: dict[str, Any],
) -> dict[str, Any]:
    source_ids = sorted(resample_artifacts)
    source_order = {source_id: index for index, source_id in enumerate(config["sources"])}
    weights = config["fusion"]["weights"]
    mode = config["fusion"]["mode"]
    tie_break = config["fusion"]["tie_break"]
    channel_specs = config["channels"]
    channel_ids = sorted(channel_specs)

    indexed_cells: dict[str, dict[str, dict[int, dict[str, Any]]]] = {}
    for source_id in source_ids:
        artifact = resample_artifacts[source_id]
        channel_index: dict[str, dict[int, dict[str, Any]]] = {}
        for channel_id, channel in artifact["channels"].items():
            channel_index[channel_id] = {
                int(point["index"]): point for point in channel["points"]
            }
        indexed_cells[source_id] = channel_index

    points: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    gaps: list[dict[str, Any]] = []
    point_status_counter: Counter[str] = Counter()
    resolution_counter: Counter[str] = Counter()

    for grid_point in grid["points"]:
        point_index = int(grid_point["index"])
        timestamp_ms = int(grid_point["timestamp_ms"])
        channel_results: dict[str, Any] = {}
        missing_channels: list[str] = []
        multi_source_channels = 0
        extrapolated_channels = 0

        for channel_id in channel_ids:
            candidates: list[dict[str, Any]] = []
            missing_sources: dict[str, Any] = {}
            for source_id in source_ids:
                channel_cells = indexed_cells[source_id].get(channel_id)
                if channel_cells is None:
                    missing_sources[source_id] = {"reason": "channel_not_configured"}
                    continue
                cell = channel_cells[point_index]
                if cell["value"] is None or cell["method"] == "missing":
                    missing_sources[source_id] = {
                        "reason": cell.get("reason") or "missing",
                        "source_timestamps_ms": cell.get("source_timestamps_ms", []),
                    }
                    continue
                priority = int(config["sources"][source_id]["priority"])
                candidates.append(
                    {
                        "source_id": source_id,
                        "value": float(cell["value"]),
                        "priority": priority,
                        "weight": float(weights[source_id]),
                        "method": cell["method"],
                        "requested_method": cell.get("requested_method"),
                        "source_timestamps_ms": list(cell.get("source_timestamps_ms", [])),
                        "extrapolated": bool(cell.get("extrapolated", False)),
                        "clamped": bool(cell.get("clamped", False)),
                    }
                )

            candidates.sort(
                key=lambda item: _participant_rank(
                    item["source_id"], item["priority"], tie_break, source_order
                )
            )
            channel_label = channel_specs[channel_id].get("label", channel_id)
            if not candidates:
                missing_channels.append(channel_id)
                channel_results[channel_id] = {
                    "channel_id": channel_id,
                    "label": channel_label,
                    "value": None,
                    "resolution": "missing",
                    "participants": [],
                    "contributions": [],
                    "winner": None,
                    "priority_tie": False,
                    "missing_sources": {
                        source_id: missing_sources[source_id]
                        for source_id in sorted(missing_sources)
                    },
                }
                gaps.append(
                    {
                        "index": point_index,
                        "timestamp_ms": timestamp_ms,
                        "channel": channel_id,
                        "channel_label": channel_label,
                        "kind": "channel_gap",
                        "reason": "no_valid_source",
                        "sources": {
                            source_id: missing_sources[source_id]
                            for source_id in sorted(missing_sources)
                        },
                    }
                )
                resolution_counter["missing"] += 1
                continue

            priority_values = {candidate["priority"] for candidate in candidates}
            priority_tie = len(candidates) > 1 and len(priority_values) == 1
            winner = candidates[0]
            weight_fallback = None

            if len(candidates) == 1:
                fused_value = candidates[0]["value"]
                contributions = [1.0]
                resolution = (
                    "extrapolated" if candidates[0]["extrapolated"] else "single_source"
                )
            else:
                if mode == "priority_override":
                    eligible = [winner]
                    contributions = [
                        1.0 if candidate is winner else 0.0 for candidate in candidates
                    ]
                elif mode == "priority_group_weighted":
                    eligible = [
                        candidate
                        for candidate in candidates
                        if candidate["priority"] == winner["priority"]
                    ]
                    total_weight = math.fsum(
                        candidate["weight"] for candidate in eligible
                    )
                    if total_weight <= 0:
                        share = 1.0 / len(eligible)
                        contributions = [
                            share if candidate in eligible else 0.0
                            for candidate in candidates
                        ]
                        weight_fallback = "equal_weights"
                    else:
                        contributions = [
                            candidate["weight"] / total_weight
                            if candidate in eligible
                            else 0.0
                            for candidate in candidates
                        ]
                else:  # weighted_mean
                    eligible = list(candidates)
                    total_weight = math.fsum(
                        candidate["weight"] for candidate in eligible
                    )
                    if total_weight <= 0:
                        share = 1.0 / len(eligible)
                        contributions = [share for _ in eligible]
                        weight_fallback = "equal_weights"
                    else:
                        contributions = [
                            candidate["weight"] / total_weight for candidate in eligible
                        ]

                if weight_fallback == "equal_weights":
                    fused_value = math.fsum(
                        candidate["value"] for candidate in eligible
                    ) / len(eligible)
                else:
                    contribution_by_source = {
                        candidate["source_id"]: contribution
                        for candidate, contribution in zip(eligible, contributions)
                        if contribution > 0
                    }
                    fused_value = math.fsum(
                        candidate["value"]
                        * contribution_by_source.get(candidate["source_id"], 0.0)
                        for candidate in candidates
                    )
                resolution = "fused"

            participants = []
            for candidate, contribution in zip(candidates, contributions):
                participant = dict(candidate)
                participant["contribution"] = float(contribution)
                participants.append(participant)
            conflict_record = None
            if len(candidates) > 1:
                multi_source_channels += 1
                conflict_record = {
                    "index": point_index,
                    "timestamp_ms": timestamp_ms,
                    "channel": channel_id,
                    "channel_label": channel_label,
                    "mode": mode,
                    "winner": winner["source_id"],
                    "priority_tie": priority_tie,
                    "tie_break": tie_break,
                    "weight_fallback": weight_fallback,
                    "fused_value": float(fused_value),
                    "participants": participants,
                }
                conflicts.append(conflict_record)
            if len(candidates) == 1 and candidates[0]["extrapolated"]:
                extrapolated_channels += 1
            resolution_counter[resolution] += 1
            channel_results[channel_id] = {
                "channel_id": channel_id,
                "label": channel_label,
                "value": float(fused_value),
                "resolution": resolution,
                "participants": participants,
                "contributions": [
                    {
                        "source_id": participant["source_id"],
                        "value": participant["value"],
                        "weight": participant["weight"],
                        "contribution": participant["contribution"],
                    }
                    for participant in participants
                ],
                "winner": winner["source_id"],
                "priority_tie": priority_tie,
                "weight_fallback": weight_fallback,
                "missing_sources": {
                    source_id: missing_sources[source_id]
                    for source_id in sorted(missing_sources)
                },
            }

        valid_channels = len(channel_ids) - len(missing_channels)
        if valid_channels == 0:
            point_status = "gap"
        elif multi_source_channels > 0:
            point_status = "fused"
        elif extrapolated_channels == valid_channels:
            point_status = "single_source_extrapolated"
        else:
            point_status = "single_source"
        point_status_counter[point_status] += 1
        if point_status == "gap":
            for gap in gaps:
                if gap["index"] == point_index:
                    gap["kind"] = "point_gap"
        points.append(
            {
                "index": point_index,
                "timestamp_ms": timestamp_ms,
                "status": point_status,
                "channels": channel_results,
                "missing_channels": missing_channels,
            }
        )

    summary_counts = {
        "grid_points": len(points),
        "point_status_counts": {
            key: point_status_counter[key] for key in sorted(point_status_counter)
        },
        "channel_resolution_counts": {
            key: resolution_counter[key] for key in sorted(resolution_counter)
        },
        "conflict_count": len(conflicts),
        "gap_channel_count": len(gaps),
        "point_gap_count": len(
            {gap["index"] for gap in gaps if gap["kind"] == "point_gap"}
        ),
    }
    return {
        "schema_version": 1,
        "grid_fingerprint": grid["fingerprint"],
        "fusion_mode": mode,
        "tie_break": tie_break,
        "labels": {
            channel_id: channel_specs[channel_id] for channel_id in channel_ids
        },
        "points": points,
        "conflicts": conflicts,
        "gaps": gaps,
        "summary": summary_counts,
    }

