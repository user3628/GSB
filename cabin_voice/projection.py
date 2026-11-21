from __future__ import annotations

from typing import Any

from .config import ZONE_ORDER
from .errors import invalid_stage
from .timeline import build_timeline, effective_activity_for_shard


def effective_activity_map(state: dict[str, Any]) -> dict[str, dict[str, list[list[int]]]]:
    vad_stage = state.get("stages", {}).get("vad")
    if not vad_stage:
        raise invalid_stage(
            "active-zone queries require the VAD stage",
            required_stage="vad",
            available_stages=sorted(state.get("stages", {})),
        )
    result: dict[str, dict[str, list[list[int]]]] = {}
    for shard in state["shards"]:
        shard_id = shard["shard_id"]
        zones = vad_stage["artifact"]["shards"][shard_id]["zones"]
        base_activity = {
            zone_id: zones[zone_id]["intervals"] for zone_id in ZONE_ORDER
        }
        result[shard_id] = effective_activity_for_shard(
            shard_id,
            base_activity,
            state["edit"]["activity_ops"].get(shard_id, []),
        )
    return result


def build_activity_projection(
    state: dict[str, Any],
    activity: dict[str, dict[str, list[list[int]]]] | None = None,
) -> dict[str, Any]:
    activity = activity or effective_activity_map(state)
    return build_timeline(
        state["shards"],
        activity,
        state["edit"]["priorities"],
    )


def build_core_result(state: dict[str, Any]) -> dict[str, Any]:
    arbitration_stage = state.get("stages", {}).get("arbitrate")
    if not arbitration_stage:
        raise invalid_stage(
            "assignment queries require the arbitration stage",
            required_stage="arbitrate",
            available_stages=sorted(state.get("stages", {})),
        )
    activity = effective_activity_map(state)
    timeline = build_activity_projection(state, activity)
    arbitration_shards = arbitration_stage["artifact"]["shards"]
    tdoa: dict[str, Any] = {}
    assignments: dict[str, Any] = {}
    for shard in state["shards"]:
        shard_id = shard["shard_id"]
        shard_assignments = arbitration_shards[shard_id]["assignments"]
        assignments[shard_id] = shard_assignments
        tdoa[shard_id] = [
            {
                "assignment_id": item["assignment_id"],
                "word": item["word"],
                "cluster_start_ms": item["cluster_start_ms"],
                "cluster_end_ms": item["cluster_end_ms"],
                "participating_zones": item["participating_zones"],
                "observed_onset_by_zone": item["observed_onset_by_zone"],
                "pair_delay_details": item["pair_delay_details"],
            }
            for item in shard_assignments
        ]
    return {
        "edit_version": state["edit"]["version"],
        "config_version": state["config_version"],
        "activity": timeline,
        "tdoa": tdoa,
        "assignments": assignments,
    }
