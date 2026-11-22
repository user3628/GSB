"""Integer-millisecond interval editing, overlap markers and deterministic edit history."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from .errors import CabinVoiceError
from .util import atomic_write_json, canonical_bytes, read_json, sha256_json


def normalize_intervals(intervals: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    cleaned: list[dict[str, Any]] = []
    for interval in intervals:
        start = int(interval["start_ms"])
        end = int(interval["end_ms"])
        if end <= start:
            continue
        cleaned.append({"start_ms": start, "end_ms": end, "kind": str(interval.get("kind", "speech"))})
    cleaned.sort(key=lambda item: (item["start_ms"], item["end_ms"], item["kind"]))
    merged: list[dict[str, Any]] = []
    for interval in cleaned:
        # Strict overlap merges frame duplication. Touching intervals are preserved: an
        # explicit split must survive until a matching merge edit joins it again.
        if merged and interval["kind"] == merged[-1]["kind"] and interval["start_ms"] < merged[-1]["end_ms"]:
            merged[-1]["end_ms"] = max(merged[-1]["end_ms"], interval["end_ms"])
        else:
            merged.append(dict(interval))
    return merged


def _interval_list(zones: dict[str, list[dict[str, Any]]], zone_id: str, chunk_id: str) -> list[dict[str, Any]]:
    if zone_id not in zones:
        raise CabinVoiceError(
            "E_INVALID_EDIT",
            "activity edit references an unknown zone in the chunk",
            {"chunk_id": chunk_id, "zone_id": zone_id},
        )
    return zones[zone_id]


def apply_activity_operations(
    intervals_by_zone: dict[str, list[dict[str, Any]]],
    operations: Iterable[dict[str, Any]],
    *,
    chunk_id: str,
) -> dict[str, list[dict[str, Any]]]:
    result = {zone: normalize_intervals(intervals) for zone, intervals in intervals_by_zone.items()}
    for operation in operations:
        if operation.get("chunk_id") != chunk_id:
            continue
        zone_id = str(operation.get("zone_id"))
        current = _interval_list(result, zone_id, chunk_id)
        kind = str(operation.get("kind", "speech"))
        if operation.get("op") == "split":
            at_ms = operation.get("at_ms")
            if not isinstance(at_ms, int) or isinstance(at_ms, bool):
                raise CabinVoiceError(
                    "E_INVALID_EDIT", "split edit requires integer at_ms", {"chunk_id": chunk_id, "zone_id": zone_id}
                )
            target = next(
                (item for item in current if item["kind"] == kind and item["start_ms"] < at_ms < item["end_ms"]),
                None,
            )
            if target is None:
                raise CabinVoiceError(
                    "E_INVALID_EDIT",
                    "split point is not strictly inside an active interval",
                    {"chunk_id": chunk_id, "zone_id": zone_id, "at_ms": at_ms},
                )
            index = current.index(target)
            current[index : index + 1] = [
                {"start_ms": target["start_ms"], "end_ms": at_ms, "kind": kind},
                {"start_ms": at_ms, "end_ms": target["end_ms"], "kind": kind},
            ]
        elif operation.get("op") == "merge":
            left_start = operation.get("left_start_ms")
            right_start = operation.get("right_start_ms")
            if not isinstance(left_start, int) or not isinstance(right_start, int):
                raise CabinVoiceError(
                    "E_INVALID_EDIT",
                    "merge edit requires integer left_start_ms and right_start_ms",
                    {"chunk_id": chunk_id, "zone_id": zone_id},
                )
            index = next(
                (
                    position
                    for position, item in enumerate(current[:-1])
                    if item["start_ms"] == left_start
                    and item["kind"] == kind
                    and current[position + 1]["start_ms"] == right_start
                    and current[position + 1]["kind"] == kind
                    and item["end_ms"] == current[position + 1]["start_ms"]
                ),
                None,
            )
            if index is None:
                raise CabinVoiceError(
                    "E_INVALID_EDIT",
                    "merge requires consecutive, touching intervals of the same kind",
                    {
                        "chunk_id": chunk_id,
                        "zone_id": zone_id,
                        "left_start_ms": left_start,
                        "right_start_ms": right_start,
                    },
                )
            current[index : index + 2] = [
                {
                    "start_ms": current[index]["start_ms"],
                    "end_ms": current[index + 1]["end_ms"],
                    "kind": kind,
                }
            ]
        else:
            raise CabinVoiceError(
                "E_INVALID_EDIT",
                "unknown activity edit operation",
                {"chunk_id": chunk_id, "zone_id": zone_id, "op": operation.get("op")},
            )
        result[zone_id] = normalize_intervals(current)
    return result


def overlap_markers(intervals_by_zone: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    events: dict[int, list[tuple[str, int]]] = {}
    for zone_id in sorted(intervals_by_zone):
        for interval in intervals_by_zone[zone_id]:
            events.setdefault(int(interval["start_ms"]), []).append((zone_id, 1))
            events.setdefault(int(interval["end_ms"]), []).append((zone_id, -1))
    boundaries = sorted(events)
    active: dict[str, int] = {}
    regions: list[dict[str, Any]] = []
    for index in range(len(boundaries) - 1):
        point = boundaries[index]
        for zone_id, delta in events[point]:
            active[zone_id] = active.get(zone_id, 0) + delta
            if active[zone_id] <= 0:
                active.pop(zone_id, None)
        end = boundaries[index + 1]
        if end <= point:
            continue
        zones = sorted(active)
        if len(zones) < 2:
            continue
        if regions and regions[-1]["end_ms"] == point and regions[-1]["zones"] == zones:
            regions[-1]["end_ms"] = end
        else:
            regions.append({"start_ms": point, "end_ms": end, "zones": zones})
    return regions


def intervals_digest(intervals_by_zone: dict[str, list[dict[str, Any]]]) -> str:
    return sha256_json(
        {zone: normalize_intervals(values) for zone, values in sorted(intervals_by_zone.items())}
    )


class EditModel:
    """Persistent, monotonically versioned edits around an immutable manifest."""

    def __init__(self, path: str | Path, manifest: dict[str, Any], *, persist: bool = True):
        self.path = Path(path)
        self.manifest = manifest
        self.persist = persist
        self.zones = {str(zone["zone_id"]): zone for zone in manifest["zones"]}
        self.terms = {str(item["term"]) for item in manifest.get("lexicon", [])}
        if self.path.exists():
            try:
                self.state = read_json(self.path)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise CabinVoiceError(
                    "E_INVALID_EDIT", "edit state is corrupt", {"path": str(self.path)}
                ) from exc
        else:
            self.state = {
                "schema_version": 1,
                "edit_version": 0,
                "activity_ops": [],
                "priority_overrides": {},
                "lexicon_overrides": {},
                "trail": [],
            }
        self._normalize_loaded_state()

    def _normalize_loaded_state(self) -> None:
        self.state.setdefault("activity_ops", [])
        self.state.setdefault("priority_overrides", {})
        self.state.setdefault("lexicon_overrides", {})
        self.state.setdefault("trail", [])
        self.state["edit_version"] = int(self.state.get("edit_version", 0))

    def _snapshot(self) -> dict[str, Any]:
        return {
            "edit_version": self.state["edit_version"],
            "activity_ops": self.state["activity_ops"],
            "priority_overrides": self.state["priority_overrides"],
            "lexicon_overrides": self.state["lexicon_overrides"],
            "trail_length": len(self.state["trail"]),
        }

    def _commit(self, operation: str, target: dict[str, Any], mutate) -> dict[str, Any]:
        before = self._snapshot()
        mutate()
        self.state["edit_version"] += 1
        after = self._snapshot()
        entry = {
            "edit_version": self.state["edit_version"],
            "operation": operation,
            "target": target,
            "before_summary": sha256_json(before),
            "after_summary": sha256_json(after),
            "before": before,
            "after": after,
        }
        self.state["trail"].append(entry)
        if self.persist:
            atomic_write_json(self.path, self.state)
        return entry

    @property
    def edit_version(self) -> int:
        return self.state["edit_version"]

    def activity_ops(self, chunk_id: str | None = None) -> list[dict[str, Any]]:
        operations = self.state["activity_ops"]
        if chunk_id is None:
            return list(operations)
        return [dict(item) for item in operations if item.get("chunk_id") == chunk_id]

    def priorities(self) -> dict[str, int]:
        result = {zone_id: int(zone["priority"]) for zone_id, zone in self.zones.items()}
        for zone_id, priority in self.state["priority_overrides"].items():
            if zone_id in result:
                result[zone_id] = int(priority)
        return result

    def hits_for(self, chunk_id: str, zone_id: str) -> list[dict[str, Any]] | None:
        override = self.state["lexicon_overrides"].get(chunk_id, {}).get(zone_id)
        if override is not None:
            return [dict(item) for item in override]
        return None

    def split_activity(
        self, chunk_id: str, zone_id: str, at_ms: int, *, kind: str = "speech"
    ) -> dict[str, Any]:
        if zone_id not in self.zones:
            raise CabinVoiceError(
                "E_INVALID_EDIT", "unknown zone", {"chunk_id": chunk_id, "zone_id": zone_id}
            )
        if not isinstance(at_ms, int) or isinstance(at_ms, bool) or at_ms < 0:
            raise CabinVoiceError(
                "E_INVALID_EDIT", "at_ms must be a non-negative integer", {"chunk_id": chunk_id, "zone_id": zone_id}
            )
        operation = {"op": "split", "chunk_id": chunk_id, "zone_id": zone_id, "at_ms": at_ms, "kind": kind}
        return self._commit(
            "split_activity", {"chunk_id": chunk_id, "zone_id": zone_id, "at_ms": at_ms}, lambda: self.state["activity_ops"].append(operation)
        )

    def merge_activity(
        self, chunk_id: str, zone_id: str, left_start_ms: int, right_start_ms: int, *, kind: str = "speech"
    ) -> dict[str, Any]:
        if zone_id not in self.zones:
            raise CabinVoiceError(
                "E_INVALID_EDIT", "unknown zone", {"chunk_id": chunk_id, "zone_id": zone_id}
            )
        operation = {
            "op": "merge",
            "chunk_id": chunk_id,
            "zone_id": zone_id,
            "left_start_ms": left_start_ms,
            "right_start_ms": right_start_ms,
            "kind": kind,
        }
        return self._commit(
            "merge_activity",
            {
                "chunk_id": chunk_id,
                "zone_id": zone_id,
                "left_start_ms": left_start_ms,
                "right_start_ms": right_start_ms,
            },
            lambda: self.state["activity_ops"].append(operation),
        )

    def set_priority(self, zone_id: str, priority: int) -> dict[str, Any]:
        if zone_id not in self.zones:
            raise CabinVoiceError("E_INVALID_EDIT", "unknown zone", {"zone_id": zone_id})
        if isinstance(priority, bool) or not isinstance(priority, int) or priority < 1:
            raise CabinVoiceError(
                "E_INVALID_EDIT", "priority must be a positive integer", {"zone_id": zone_id, "priority": priority}
            )
        return self._commit(
            "set_priority",
            {"zone_id": zone_id, "priority": priority},
            lambda: self.state["priority_overrides"].__setitem__(zone_id, priority),
        )

    def set_hits(self, chunk_id: str, zone_id: str, hits: list[dict[str, Any]]) -> dict[str, Any]:
        if zone_id not in self.zones:
            raise CabinVoiceError("E_INVALID_EDIT", "unknown zone", {"chunk_id": chunk_id, "zone_id": zone_id})
        cleaned: list[dict[str, Any]] = []
        for hit in hits:
            term = hit.get("term")
            if term not in self.terms:
                raise CabinVoiceError(
                    "E_LEXICON_REFERENCE_MISSING",
                    "edited hit references a missing lexicon term",
                    {"chunk_id": chunk_id, "zone_id": zone_id, "term": term},
                )
            try:
                strength = float(hit.get("strength"))
            except (TypeError, ValueError) as exc:
                raise CabinVoiceError(
                    "E_NONFINITE_SCORE",
                    "edited hit strength must be finite and positive",
                    {"chunk_id": chunk_id, "zone_id": zone_id, "term": term},
                ) from exc
            if not (strength > 0 and strength != float("inf") and strength != float("-inf")):
                raise CabinVoiceError(
                    "E_NONFINITE_SCORE",
                    "edited hit strength must be finite and positive",
                    {"chunk_id": chunk_id, "zone_id": zone_id, "term": term, "strength": strength},
                )
            cleaned.append(
                {
                    "term": term,
                    "strength": strength,
                    **({"at_ms": int(hit["at_ms"])} if "at_ms" in hit else {}),
                }
            )
        cleaned.sort(key=lambda item: (item["term"], item["at_ms"] if "at_ms" in item else -1))
        return self._commit(
            "set_hits",
            {"chunk_id": chunk_id, "zone_id": zone_id, "hits": cleaned},
            lambda: self.state["lexicon_overrides"].setdefault(chunk_id, {}).__setitem__(zone_id, cleaned),
        )

    def state_bytes(self) -> bytes:
        return canonical_bytes(self.state)

    def activity_component(self, chunk_id: str | None = None) -> str:
        return sha256_json(self.activity_ops(chunk_id))

    def priority_component(self) -> str:
        return sha256_json(self.priorities())

    def lexicon_component(self) -> str:
        return sha256_json(self.state["lexicon_overrides"])
