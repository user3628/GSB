from __future__ import annotations

import copy
from typing import Any

from .canonical import digest_obj
from .config import ZONE_ORDER, config_from_dict, patch_config
from .errors import CabinError, invalid_argument, invalid_interval, invalid_stage, stale_version
from .timeline import apply_activity_operation, effective_activity_for_shard


def _shard_lookup(state: dict[str, Any], shard_id: Any) -> dict[str, Any]:
    if not isinstance(shard_id, str):
        raise invalid_argument("shard_id must be a string")
    for shard in state["shards"]:
        if shard["shard_id"] == shard_id:
            return shard
    raise invalid_argument("unknown shard_id", shard_id=shard_id)


def _zone_lookup(state: dict[str, Any], shard_id: str, zone_id: Any) -> dict[str, Any]:
    shard = _shard_lookup(state, shard_id)
    if zone_id not in state["zone_order"]:
        raise invalid_argument("unknown zone_id", shard_id=shard_id, zone_id=zone_id)
    for zone in shard["zones"]:
        if zone["zone_id"] == zone_id:
            return zone
    raise invalid_argument("zone layout is missing", shard_id=shard_id, zone_id=zone_id)


def _validate_version_and_scope(
    state: dict[str, Any],
    expected_version: Any,
    shard_id: Any,
) -> None:
    current_version = state["edit"]["version"]
    if isinstance(expected_version, bool) or not isinstance(expected_version, int):
        raise invalid_argument("expected_version must be an integer")
    if expected_version != current_version:
        raise stale_version(
            "edit version is not current",
            expected_version=expected_version,
            current_version=current_version,
            shard_id=shard_id,
            current_scope_shard_id=state["edit"]["scope_shard_id"],
        )
    if shard_id != state["edit"]["scope_shard_id"]:
        raise stale_version(
            "edit belongs to a different shard than the active edit scope",
            shard_id=shard_id,
            current_scope_shard_id=state["edit"]["scope_shard_id"],
            expected_version=expected_version,
            current_version=current_version,
        )


def _effective_activity(state: dict[str, Any], shard: dict[str, Any]) -> dict[str, list[list[int]]]:
    vad_stage = state.get("stages", {}).get("vad")
    if not vad_stage:
        raise invalid_stage(
            "activity editing requires the VAD stage",
            required_stage="vad",
            available_stages=sorted(state.get("stages", {})),
        )
    shard_id = shard["shard_id"]
    base = vad_stage["artifact"]["shards"][shard_id]["zones"]
    base_activity = {zone_id: base[zone_id]["intervals"] for zone_id in ZONE_ORDER}
    return effective_activity_for_shard(
        shard_id,
        base_activity,
        state["edit"]["activity_ops"].get(shard_id, []),
    )


def _normalize_hit(
    raw: dict[str, Any],
    *,
    shard_id: str,
    zone: dict[str, Any],
) -> dict[str, Any]:
    zone_id = raw.get("zone_id")
    word = raw.get("word")
    start_ms = raw.get("start_ms")
    end_ms = raw.get("end_ms")
    strength_raw = raw.get("strength")
    if zone_id != zone["zone_id"]:
        raise invalid_interval(
            "word hit zone does not match the addressed zone",
            shard_id=shard_id,
            zone_id=str(zone_id),
            start_ms=start_ms,
            end_ms=end_ms,
        )
    if not isinstance(word, str) or not word.strip():
        raise invalid_argument("word hit word must be non-empty", shard_id=shard_id, zone_id=zone_id)
    if (
        isinstance(start_ms, bool)
        or not isinstance(start_ms, int)
        or isinstance(end_ms, bool)
        or not isinstance(end_ms, int)
    ):
        raise invalid_interval(
            "word hit interval endpoints must be integers",
            shard_id=shard_id,
            zone_id=zone_id,
            start_ms=start_ms,
            end_ms=end_ms,
        )
    if start_ms < 0 or end_ms <= start_ms or end_ms > zone["duration_ms"]:
        raise invalid_interval(
            "word hit interval lies outside the zone",
            shard_id=shard_id,
            zone_id=zone_id,
            start_ms=start_ms,
            end_ms=end_ms,
            duration_ms=zone["duration_ms"],
        )
    if isinstance(strength_raw, bool) or not isinstance(strength_raw, (int, float)):
        raise invalid_argument("word hit strength must be numeric", shard_id=shard_id, zone_id=zone_id)
    strength = float(strength_raw)
    if not 0.0 <= strength <= 1.0:
        raise invalid_argument(
            "word hit strength must be within [0, 1]",
            shard_id=shard_id,
            zone_id=zone_id,
            strength=strength,
        )
    identity = {
        "shard_id": shard_id,
        "zone_id": zone_id,
        "word": word.strip(),
        "start_ms": start_ms,
        "end_ms": end_ms,
    }
    return {
        "hit_id": "hit-" + digest_obj(identity)[:16],
        **identity,
        "strength": round(strength, 9),
    }


def _audit(
    state: dict[str, Any],
    *,
    new_version: int,
    operation: str,
    shard_id: str,
    affected: dict[str, Any],
    before: Any,
    after: Any,
) -> None:
    state["edit"]["audit"].append(
        {
            "seq": new_version,
            "edit_version": new_version,
            "operation": operation,
            "shard_id": shard_id,
            "affected": affected,
            "before": before,
            "after": after,
        }
    )


def apply_edit(
    state: dict[str, Any],
    *,
    operation: str,
    parameters: dict[str, Any],
    expected_version: int,
    shard_id: str,
) -> dict[str, Any]:
    state = copy.deepcopy(state)
    if operation == "select_scope":
        if shard_id not in {shard["shard_id"] for shard in state["shards"]}:
            raise invalid_argument("unknown shard_id", shard_id=shard_id)
        current_version = state["edit"]["version"]
        if expected_version != current_version:
            raise stale_version(
                "edit version is not current",
                expected_version=expected_version,
                current_version=current_version,
                shard_id=shard_id,
                current_scope_shard_id=state["edit"]["scope_shard_id"],
            )
        before = state["edit"]["scope_shard_id"]
        new_version = current_version + 1
        state["edit"]["version"] = new_version
        state["edit"]["scope_shard_id"] = shard_id
        _audit(
            state,
            new_version=new_version,
            operation="select_scope",
            shard_id=shard_id,
            affected={"scope_shard_id": shard_id},
            before=before,
            after=shard_id,
        )
        return state
    _validate_version_and_scope(state, expected_version, shard_id)
    shard = _shard_lookup(state, shard_id)
    current_version = state["edit"]["version"]
    new_version = current_version + 1
    if operation == "set_priority":
        zone = _zone_lookup(state, shard_id, parameters.get("zone_id"))
        priority = parameters.get("priority")
        if isinstance(priority, bool) or not isinstance(priority, int) or priority < 1:
            raise invalid_argument("priority must be a positive integer", shard_id=shard_id)
        zone_id = zone["zone_id"]
        before = state["edit"]["priorities"][shard_id][zone_id]
        state["edit"]["priorities"][shard_id][zone_id] = priority
        _audit(
            state,
            new_version=new_version,
            operation=operation,
            shard_id=shard_id,
            affected={"zone_id": zone_id, "field": "priority"},
            before=before,
            after=priority,
        )
    elif operation == "set_priorities":
        priorities = parameters.get("priorities")
        if not isinstance(priorities, dict) or set(priorities) != set(ZONE_ORDER):
            raise invalid_argument(
                "set_priorities requires a complete four-zone priority map",
                shard_id=shard_id,
                zones=list(ZONE_ORDER),
            )
        normalized: dict[str, int] = {}
        for zone_id in ZONE_ORDER:
            priority = priorities[zone_id]
            if isinstance(priority, bool) or not isinstance(priority, int) or priority < 1:
                raise invalid_argument(
                    "priority must be a positive integer",
                    shard_id=shard_id,
                    zone_id=zone_id,
                )
            normalized[zone_id] = priority
        before = copy.deepcopy(state["edit"]["priorities"][shard_id])
        state["edit"]["priorities"][shard_id] = normalized
        _audit(
            state,
            new_version=new_version,
            operation=operation,
            shard_id=shard_id,
            affected={"field": "priorities"},
            before=before,
            after=normalized,
        )
    elif operation == "upsert_word_hit":
        zone = _zone_lookup(state, shard_id, parameters.get("zone_id"))
        hit = _normalize_hit(parameters, shard_id=shard_id, zone=zone)
        hits = state["edit"]["word_hits"][shard_id]
        before = None
        replaced = False
        for index, existing in enumerate(hits):
            if existing["hit_id"] == hit["hit_id"]:
                before = copy.deepcopy(existing)
                hits[index] = hit
                replaced = True
                break
        if not replaced:
            hits.append(hit)
        hits.sort(
            key=lambda item: (
                item["start_ms"],
                item["end_ms"],
                item["zone_id"],
                item["word"],
                item["hit_id"],
            )
        )
        _audit(
            state,
            new_version=new_version,
            operation=operation,
            shard_id=shard_id,
            affected={
                "hit_id": hit["hit_id"],
                "zone_id": hit["zone_id"],
                "start_ms": hit["start_ms"],
                "end_ms": hit["end_ms"],
            },
            before=before,
            after=copy.deepcopy(hit),
        )
    elif operation == "delete_word_hit":
        hit_id = parameters.get("hit_id")
        if not isinstance(hit_id, str):
            raise invalid_argument("hit_id must be a string", shard_id=shard_id)
        hits = state["edit"]["word_hits"][shard_id]
        before = None
        kept = []
        for hit in hits:
            if hit["hit_id"] == hit_id:
                before = copy.deepcopy(hit)
            else:
                kept.append(hit)
        if before is None:
            raise invalid_argument("word hit does not exist", shard_id=shard_id, hit_id=hit_id)
        state["edit"]["word_hits"][shard_id] = kept
        _audit(
            state,
            new_version=new_version,
            operation=operation,
            shard_id=shard_id,
            affected={"hit_id": hit_id},
            before=before,
            after=None,
        )
    elif operation in {"split_activity", "merge_activity"}:
        zone = _zone_lookup(state, shard_id, parameters.get("zone_id"))
        zone_id = zone["zone_id"]
        at_ms = parameters.get("at_ms")
        if isinstance(at_ms, bool) or not isinstance(at_ms, int):
            raise invalid_interval(
                "activity operation requires integer at_ms",
                shard_id=shard_id,
                zone_id=zone_id,
                start_ms=at_ms,
            )
        before_activity = _effective_activity(state, shard)
        op_type = "split" if operation == "split_activity" else "merge"
        candidate = {"type": op_type, "at_ms": at_ms}
        after_zone = apply_activity_operation(
            before_activity[zone_id],
            candidate,
            shard_id=shard_id,
            zone_id=zone_id,
        )
        activity_op = {
            "op_id": "op-" + digest_obj(
                {
                    "shard_id": shard_id,
                    "zone_id": zone_id,
                    "type": op_type,
                    "at_ms": at_ms,
                    "edit_version": new_version,
                }
            )[:16],
            "type": op_type,
            "shard_id": shard_id,
            "zone_id": zone_id,
            "at_ms": at_ms,
            "edit_version": new_version,
        }
        state["edit"]["activity_ops"].setdefault(shard_id, []).append(activity_op)
        _audit(
            state,
            new_version=new_version,
            operation=operation,
            shard_id=shard_id,
            affected={"zone_id": zone_id, "at_ms": at_ms},
            before=copy.deepcopy(before_activity[zone_id]),
            after=copy.deepcopy(after_zone),
        )
    elif operation == "config_patch":
        patch = parameters.get("patch")
        if not isinstance(patch, dict):
            raise invalid_argument("config_patch requires a patch object")
        old_config = config_from_dict(state["config"])
        new_config = patch_config(old_config, patch)
        before = old_config.to_dict()
        after = new_config.to_dict()
        state["config"] = new_config.to_dict()
        state["config_version"] = int(state.get("config_version", 0)) + 1
        invalidated_activity_ops = False
        if old_config.phase_digest("vad") != new_config.phase_digest("vad"):
            invalidated_activity_ops = any(
                bool(ops) for ops in state["edit"]["activity_ops"].values()
            )
            for activity_shard_id in state["edit"]["activity_ops"]:
                state["edit"]["activity_ops"][activity_shard_id] = []
        _audit(
            state,
            new_version=new_version,
            operation=operation,
            shard_id=shard_id,
            affected={
                "config_version": state["config_version"],
                "invalidated_activity_ops": invalidated_activity_ops,
            },
            before=before,
            after=after,
        )
    else:
        raise invalid_argument("unknown edit operation", operation=operation)
    state["edit"]["version"] = new_version
    return state
