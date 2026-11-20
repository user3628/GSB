from __future__ import annotations

import base64
import copy
import json
import logging
import math
import threading
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
from pathlib import Path
from typing import Any

from .config import EngineConfig, load_config
from .crash import CrashController
from .errors import (
    BusinessError,
    E_CAPACITY_EXCEEDED,
    E_CONFLICTING_BATCH,
    E_CURSOR_EXPIRED,
    E_EVENT_INVALID,
    E_INTERNAL,
    E_INVALID_TRANSITION,
    E_LATE_EVENT,
    E_REQUEST_INVALID,
    E_SNAPSHOT_INVALID,
)
from .persistence import SnapshotStore, canonical_json, sha256_json, utc_now


LOGGER = logging.getLogger(__name__)
STATE_FORMAT = "DWE_STATE_V1"
WINDOW_TYPES = ("tumbling", "sliding", "session")
LEGAL_TRANSITIONS: dict[str, frozenset[str]] = {
    "OPEN": frozenset({"CLOSED"}),
    "CLOSED": frozenset({"EMITTED"}),
    "EMITTED": frozenset(),
}


def _window_key(window_type: str, start_ms: int, end_ms: int, group: str) -> str:
    return canonical_json([window_type, start_ms, end_ms, group])


def _session_key(session_id: int) -> str:
    return canonical_json(["session", session_id])


def _default_state(config: EngineConfig) -> dict[str, Any]:
    return {
        "format": STATE_FORMAT,
        "version": 0,
        "config_fingerprint": config.fingerprint,
        "watermark_ms": None,
        "windows": {},
        "session_index": {},
        "session_tombstones": [],
        "next_session_id": 1,
        "late_queue": [],
        "next_late_seq": 1,
        "batch_registry": {},
        "eviction_log": [],
        "last_intent_id": None,
        "last_batch_id": None,
    }


def _new_window(
    *,
    window_type: str,
    start_ms: int,
    end_ms: int,
    group: str,
    state: str = "OPEN",
    session_id: int | None = None,
) -> dict[str, Any]:
    return {
        "window_type": window_type,
        "window_start_ms": start_ms,
        "window_end_ms": end_ms,
        "group": group,
        "state": state,
        "count": 0,
        "non_null_count": 0,
        "sum": 0.0,
        "compensation": 0.0,
        "min": None,
        "max": None,
        "values": [],
        "first_event_ts_ms": None,
        "last_event_ts_ms": None,
        "emitted_at_version": None,
        "session_id": session_id,
    }


def _neumaier_add(total: float, compensation: float, value: float) -> tuple[float, float]:
    updated = total + value
    if abs(total) >= abs(value):
        compensation += (total - updated) + value
    else:
        compensation += (value - updated) + total
    return updated, compensation


def _add_value_to_window(window: dict[str, Any], ts: int, value: float | None) -> None:
    window["count"] += 1
    first = window["first_event_ts_ms"]
    last = window["last_event_ts_ms"]
    window["first_event_ts_ms"] = ts if first is None else min(first, ts)
    window["last_event_ts_ms"] = ts if last is None else max(last, ts)
    if value is None:
        return
    total, compensation = _neumaier_add(window["sum"], window["compensation"], value)
    window["sum"] = total
    window["compensation"] = compensation
    window["non_null_count"] += 1
    window["values"].append(value)
    current_min = window["min"]
    current_max = window["max"]
    window["min"] = value if current_min is None else min(current_min, value)
    window["max"] = value if current_max is None else max(current_max, value)


def _rebuild_sum(window: dict[str, Any]) -> None:
    total = 0.0
    compensation = 0.0
    for value in window["values"]:
        total, compensation = _neumaier_add(total, compensation, value)
    window["sum"] = total
    window["compensation"] = compensation


def _absorb_window(target: dict[str, Any], source: dict[str, Any]) -> None:
    target["count"] += source["count"]
    target["non_null_count"] += source["non_null_count"]
    target["values"].extend(source["values"])
    if source["min"] is not None:
        target["min"] = source["min"] if target["min"] is None else min(target["min"], source["min"])
    if source["max"] is not None:
        target["max"] = source["max"] if target["max"] is None else max(target["max"], source["max"])
    for field_name, reducer in (
        ("first_event_ts_ms", min),
        ("last_event_ts_ms", max),
    ):
        source_value = source[field_name]
        target_value = target[field_name]
        if source_value is not None:
            target[field_name] = source_value if target_value is None else reducer(target_value, source_value)
    _rebuild_sum(target)


def _ceil_div(numerator: int, denominator: int) -> int:
    return -((-numerator) // denominator)


def _format_sum(total: float, compensation: float, decimals: int) -> str:
    value = total + compensation
    try:
        decimal_value = Decimal(str(value))
        if not decimal_value.is_finite():
            raise ArithmeticError("compensated sum is not finite")
        quantum = Decimal(1).scaleb(-decimals)
        exponent = decimal_value.as_tuple().exponent
        integer_digits = max(1, len(decimal_value.as_tuple().digits) + int(exponent))
        with localcontext() as context:
            context.prec = max(28, integer_digits + decimals + 8)
            rounded = decimal_value.quantize(quantum, rounding=ROUND_HALF_EVEN)
    except (ArithmeticError, ValueError) as exc:
        raise BusinessError(
            E_EVENT_INVALID,
            "Aggregate sum cannot be represented deterministically",
            details={"reason": str(exc)},
        ) from exc
    return f"{rounded:.{decimals}f}"


def _quantile_records(values: list[float], probabilities: list[float]) -> list[dict[str, Any]]:
    ordered = sorted(values)
    sample_size = len(ordered)
    records: list[dict[str, Any]] = []
    for probability in probabilities:
        if sample_size == 0:
            rank = None
            value = None
        else:
            rank = min(max(math.ceil(probability * sample_size), 1), sample_size)
            value = ordered[rank - 1]
        records.append({"p": probability, "value": value, "rank": rank, "sample_size": sample_size})
    return records


def window_sort_key(window: dict[str, Any]) -> tuple[Any, ...]:
    return (
        int(window["window_start_ms"]),
        str(window["group"]),
        str(window["window_type"]),
        int(window["window_end_ms"]),
        int(window.get("session_id") or 0),
    )


class WindowStateMachine:
    @staticmethod
    def validate(current: str, target: str) -> None:
        if target not in LEGAL_TRANSITIONS.get(current, frozenset()):
            raise BusinessError(
                E_INVALID_TRANSITION,
                f"Illegal window transition {current} -> {target}",
                details={
                    "current": current,
                    "target": target,
                    "legal_targets": sorted(LEGAL_TRANSITIONS.get(current, frozenset())),
                },
            )

    @classmethod
    def transition(cls, window: dict[str, Any], target: str, version: int | None = None) -> None:
        current = str(window["state"])
        cls.validate(current, target)
        window["state"] = target
        if target == "EMITTED" and version is not None:
            window["emitted_at_version"] = version


@dataclass(slots=True)
class QueryView:
    view_id: str
    signature: str
    records: list[dict[str, Any]]
    as_of_version: int
    created_monotonic: float


@dataclass(slots=True)
class _InFlightBatch:
    content_hash: str
    event: threading.Event = field(default_factory=threading.Event)
    result: dict[str, Any] | None = None
    error: BusinessError | None = None


class WindowEngine:
    def __init__(
        self,
        *,
        config: EngineConfig | None = None,
        runtime_dir: str | Path = "runtime",
        crash: CrashController | None = None,
        query_view_limit: int = 16,
    ):
        self.config = config or load_config()
        self.runtime_dir = Path(runtime_dir).resolve()
        self.crash = crash or CrashController.from_env()
        self.store = SnapshotStore(self.runtime_dir, self.config, self.crash)
        self._state_lock = threading.RLock()
        self._write_lock = threading.Lock()
        self._inflight: dict[str, _InFlightBatch] = {}
        self._query_views: OrderedDict[str, QueryView] = OrderedDict()
        self._query_view_limit = query_view_limit
        self._latest_seen_version = 0
        self._recovery_report: dict[str, Any] = {}
        self._state: dict[str, Any]
        self._startup_recover()

    @classmethod
    def from_paths(
        cls,
        *,
        config_path: str | Path | None = None,
        runtime_dir: str | Path = "runtime",
        crash: CrashController | None = None,
    ) -> "WindowEngine":
        return cls(config=load_config(config_path), runtime_dir=runtime_dir, crash=crash)

    def _startup_recover(self) -> None:
        scan = self.store.scan_latest()
        if scan.latest is None and scan.had_snapshot_files:
            raise BusinessError(
                E_SNAPSHOT_INVALID,
                "No complete, checksum-valid snapshot can be loaded",
                details={"corrupt_snapshots": scan.corrupt},
            )
        if scan.latest is None:
            state = _default_state(self.config)
        else:
            state = copy.deepcopy(scan.latest.body)
            self._validate_state(state, details={"source": scan.latest.path.name})
        self._state = state
        self._latest_seen_version = max(scan.observed_max_version, int(state["version"]))

        committed: list[dict[str, Any]] = []
        aborted: list[dict[str, Any]] = []
        for intent in self.store.pending_intents():
            batch_id = intent.get("batch_id")
            registered = self._state.get("batch_registry", {}).get(batch_id) if batch_id else None
            hash_matches = bool(
                registered and registered.get("content_hash") == intent.get("content_hash")
            )
            snapshot_matches = self._state.get("last_intent_id") == intent["intent_id"]
            if snapshot_matches or hash_matches:
                self.store.mark_intent_finished(
                    intent["intent_id"], "committed", "recovered: durable snapshot contains intent"
                )
                committed.append(
                    {
                        "intent_id": intent["intent_id"],
                        "batch_id": batch_id,
                        "kind": intent["kind"],
                        "basis": "snapshot_intent" if snapshot_matches else "batch_registry",
                    }
                )
            else:
                self.store.mark_intent_finished(
                    intent["intent_id"], "aborted", "recovered: no committed snapshot contains intent"
                )
                aborted.append(
                    {
                        "intent_id": intent["intent_id"],
                        "batch_id": batch_id,
                        "kind": intent["kind"],
                        "baseline_version": intent["baseline_version"],
                    }
                )

        self._recovery_report = {
            "generated_at": utc_now(),
            "fact_source": "complete snapshot files + SQLite intent journal",
            "loaded_snapshot_version": scan.latest.version if scan.latest else None,
            "loaded_snapshot_file": scan.latest.path.name if scan.latest else None,
            "loaded_snapshot_checksum": scan.latest.checksum if scan.latest else None,
            "state_version_after_recovery": self._state["version"],
            "observed_max_snapshot_version": scan.observed_max_version,
            "complete_snapshots_scanned": len(scan.valid),
            "corrupt_snapshots": scan.corrupt,
            "temporary_snapshot_files_ignored": scan.temporary_files,
            "pending_intents_found": len(committed) + len(aborted),
            "committed_intents": committed,
            "aborted_intents": aborted,
            "rollback_policy": "load exactly one complete snapshot; never merge states",
            "shutdown_report_promise": "none after hard termination; this report contains persisted facts only",
        }
        self.store.write_recovery_report(self._recovery_report)

    def _validate_state(self, state: Any, *, details: dict[str, Any] | None = None) -> None:
        reason: str | None = None
        if not isinstance(state, dict):
            reason = "state root is not an object"
        elif state.get("format") != STATE_FORMAT:
            reason = "unsupported state format"
        elif not isinstance(state.get("version"), int) or isinstance(state.get("version"), bool):
            reason = "state version is invalid"
        elif state["version"] < 0:
            reason = "state version is negative"
        elif state.get("config_fingerprint") != self.config.fingerprint:
            reason = "state config fingerprint mismatch"
        elif not isinstance(state.get("windows"), dict):
            reason = "windows is not an object"
        elif not isinstance(state.get("late_queue"), list):
            reason = "late_queue is not an array"
        elif not isinstance(state.get("batch_registry"), dict):
            reason = "batch_registry is not an object"
        elif not isinstance(state.get("session_index"), dict):
            reason = "session_index is not an object"
        elif not isinstance(state.get("session_tombstones"), list):
            reason = "session_tombstones is not an array"
        elif not isinstance(state.get("eviction_log", []), list):
            reason = "eviction_log is not an array"
        elif not isinstance(state.get("next_late_seq"), int) or state["next_late_seq"] < 1:
            reason = "next_late_seq is invalid"
        elif not isinstance(state.get("next_session_id"), int) or state["next_session_id"] < 1:
            reason = "next_session_id is invalid"
        if reason is None:
            required_window_fields = {
                "window_type",
                "window_start_ms",
                "window_end_ms",
                "group",
                "state",
                "count",
                "non_null_count",
                "sum",
                "compensation",
                "values",
                "emitted_at_version",
            }
            for key, window in state["windows"].items():
                if not isinstance(window, dict):
                    reason = f"window {key!r} is not an object"
                    break
                missing = sorted(required_window_fields - set(window))
                if missing:
                    reason = f"window {key!r} is missing fields: {missing}"
                    break
                if window["state"] not in LEGAL_TRANSITIONS:
                    reason = f"window {key!r} has invalid state {window['state']!r}"
                    break
                if not isinstance(window["values"], list):
                    reason = f"window {key!r} values is not an array"
                    break
        if reason is not None:
            payload = dict(details or {})
            payload["reason"] = reason
            raise BusinessError(E_SNAPSHOT_INVALID, "Snapshot state validation failed", details=payload)

    def recovery_report(self) -> dict[str, Any]:
        with self._state_lock:
            return copy.deepcopy(self._recovery_report)

    def state_summary(self) -> dict[str, Any]:
        with self._state_lock:
            state = self._state
            by_state: dict[str, int] = {name: 0 for name in LEGAL_TRANSITIONS}
            by_type: dict[str, int] = {name: 0 for name in WINDOW_TYPES}
            for window in state["windows"].values():
                by_state[window["state"]] = by_state.get(window["state"], 0) + 1
                by_type[window["window_type"]] = by_type.get(window["window_type"], 0) + 1
            return {
                "state_version": state["version"],
                "watermark_ms": state["watermark_ms"],
                "window_count": len(state["windows"]),
                "windows_by_type": by_type,
                "windows_by_state": by_state,
                "late_queue_length": len(state["late_queue"]),
                "session_groups": len(state["session_index"]),
                "processed_batches": len(state["batch_registry"]),
                "latest_observed_snapshot_version": self._latest_seen_version,
            }

    @staticmethod
    def _normalize_events(events: list[Any]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for index, event in enumerate(events):
            if hasattr(event, "model_dump"):
                raw = event.model_dump()
            elif isinstance(event, dict):
                raw = dict(event)
            else:
                raise BusinessError(E_EVENT_INVALID, "Event must be an object", details={"index": index})
            timestamp = raw.get("timestamp_ms")
            group = raw.get("group")
            value = raw.get("value")
            if not isinstance(timestamp, int) or isinstance(timestamp, bool) or timestamp < 0:
                raise BusinessError(
                    E_EVENT_INVALID,
                    "timestamp_ms must be a non-negative integer",
                    details={"index": index, "timestamp_ms": timestamp},
                )
            if not isinstance(group, str) or not group.strip():
                raise BusinessError(
                    E_EVENT_INVALID,
                    "group must be a non-empty string",
                    details={"index": index},
                )
            if value is not None:
                if not isinstance(value, (int, float)) or isinstance(value, bool):
                    raise BusinessError(
                        E_EVENT_INVALID,
                        "value must be numeric or null",
                        details={"index": index},
                    )
                value = float(value)
                if not math.isfinite(value):
                    raise BusinessError(
                        E_EVENT_INVALID,
                        "value must be finite",
                        details={"index": index},
                    )
            normalized.append({"timestamp_ms": timestamp, "group": group, "value": value})
        return normalized

    @staticmethod
    def _batch_hash(batch_id: str, events: list[dict[str, Any]]) -> str:
        ordered = sorted(
            events,
            key=lambda event: (
                event["timestamp_ms"],
                event["group"],
                event["value"] is not None,
                0.0 if event["value"] is None else event["value"],
            ),
        )
        return sha256_json({"batch_id": batch_id, "events": ordered})

    @staticmethod
    def _window_assignment_starts(config: EngineConfig, timestamp_ms: int) -> list[tuple[str, int, int]]:
        tumbling_start = (timestamp_ms // config.tumbling_size_ms) * config.tumbling_size_ms
        assignments: list[tuple[str, int, int]] = [
            ("tumbling", tumbling_start, tumbling_start + config.tumbling_size_ms)
        ]
        minimum_k = _ceil_div(timestamp_ms - config.sliding_size_ms + 1, config.sliding_step_ms)
        maximum_k = timestamp_ms // config.sliding_step_ms
        for k in range(minimum_k, maximum_k + 1):
            start = k * config.sliding_step_ms
            assignments.append(("sliding", start, start + config.sliding_size_ms))
        return assignments

    def _record_late_assignment(
        self,
        state: dict[str, Any],
        *,
        batch_id: str,
        event_index: int,
        event: dict[str, Any],
        window_type: str,
        window_start_ms: int,
        window_end_ms: int,
        reason: str,
        detected_state: str,
    ) -> None:
        if self.config.late_policy == "reject":
            raise BusinessError(
                E_LATE_EVENT,
                "Event falls into a closed or emitted window and late policy is reject",
                details={
                    "batch_id": batch_id,
                    "event_index": event_index,
                    "window_type": window_type,
                    "window_start_ms": window_start_ms,
                    "window_end_ms": window_end_ms,
                    "group": event["group"],
                    "timestamp_ms": event["timestamp_ms"],
                    "detected_state": detected_state,
                },
            )
        sequence = int(state["next_late_seq"])
        state["next_late_seq"] = sequence + 1
        state["late_queue"].append(
            {
                "seq": sequence,
                "batch_id": batch_id,
                "event_index": event_index,
                "event": copy.deepcopy(event),
                "window_type": window_type,
                "window_start_ms": window_start_ms,
                "window_end_ms": window_end_ms,
                "group": event["group"],
                "reason": reason,
                "detected_state": detected_state,
                "queued_at_version": state["version"],
            }
        )

    def _apply_flat_event(
        self,
        state: dict[str, Any],
        *,
        batch_id: str,
        event_index: int,
        event: dict[str, Any],
    ) -> tuple[int, int, int]:
        late_count = 0
        applied_count = 0
        new_count = 0
        watermark = state["watermark_ms"]
        for window_type, start_ms, end_ms in self._window_assignment_starts(
            self.config, event["timestamp_ms"]
        ):
            key = _window_key(window_type, start_ms, end_ms, event["group"])
            window = state["windows"].get(key)
            if window is not None and window["state"] != "OPEN":
                self._record_late_assignment(
                    state,
                    batch_id=batch_id,
                    event_index=event_index,
                    event=event,
                    window_type=window_type,
                    window_start_ms=start_ms,
                    window_end_ms=end_ms,
                    reason="window_not_open",
                    detected_state=window["state"],
                )
                late_count += 1
                continue
            horizon_closed = (
                watermark is not None
                and int(watermark) >= end_ms + self.config.allowed_lateness_ms
            )
            if window is None and horizon_closed:
                self._record_late_assignment(
                    state,
                    batch_id=batch_id,
                    event_index=event_index,
                    event=event,
                    window_type=window_type,
                    window_start_ms=start_ms,
                    window_end_ms=end_ms,
                    reason="event_horizon_closed",
                    detected_state="ABSENT_CLOSED",
                )
                late_count += 1
                continue
            if window is None:
                window = _new_window(
                    window_type=window_type,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    group=event["group"],
                )
                state["windows"][key] = window
                new_count += 1
            _add_value_to_window(window, event["timestamp_ms"], event["value"])
            applied_count += 1
        return late_count, applied_count, new_count

    def _group_session_keys(self, state: dict[str, Any], group: str) -> list[str]:
        keys = state["session_index"].get(group, [])
        return [key for key in keys if key in state["windows"]]

    def _set_group_session_keys(self, state: dict[str, Any], group: str, keys: list[str]) -> None:
        keys = sorted(
            set(keys),
            key=lambda key: (
                state["windows"][key]["window_start_ms"],
                state["windows"][key].get("session_id") or 0,
            ),
        )
        state["session_index"][group] = keys

    def _apply_session_event(
        self,
        state: dict[str, Any],
        *,
        batch_id: str,
        event_index: int,
        event: dict[str, Any],
    ) -> tuple[int, int, int]:
        group = event["group"]
        timestamp = event["timestamp_ms"]
        gap = self.config.session_gap_ms
        watermark = state["watermark_ms"]
        keys = self._group_session_keys(state, group)
        sessions = [(key, state["windows"][key]) for key in keys]

        closed_affected = [
            (key, window)
            for key, window in sessions
            if window["state"] != "OPEN"
            and int(window["window_start_ms"]) - gap <= timestamp <= int(window["window_end_ms"]) + gap
        ]
        tombstone_affected = [
            tombstone
            for tombstone in state["session_tombstones"]
            if tombstone["group"] == group
            and int(tombstone["window_start_ms"]) - gap <= timestamp <= int(tombstone["window_end_ms"]) + gap
        ]
        if closed_affected or tombstone_affected:
            for key, window in closed_affected:
                self._record_late_assignment(
                    state,
                    batch_id=batch_id,
                    event_index=event_index,
                    event=event,
                    window_type="session",
                    window_start_ms=int(window["window_start_ms"]),
                    window_end_ms=int(window["window_end_ms"]),
                    reason="session_closed_influence",
                    detected_state=window["state"],
                )
            for tombstone in tombstone_affected:
                self._record_late_assignment(
                    state,
                    batch_id=batch_id,
                    event_index=event_index,
                    event=event,
                    window_type="session",
                    window_start_ms=int(tombstone["window_start_ms"]),
                    window_end_ms=int(tombstone["window_end_ms"]),
                    reason="session_evicted_influence",
                    detected_state=tombstone["state"],
                )
            return len(closed_affected) + len(tombstone_affected), 0, 0

        candidates = [
            (key, window)
            for key, window in sessions
            if window["state"] == "OPEN"
            and int(window["window_start_ms"]) - gap <= timestamp <= int(window["window_end_ms"]) + gap
        ]
        if not candidates and watermark is not None and int(watermark) > timestamp + gap:
            self._record_late_assignment(
                state,
                batch_id=batch_id,
                event_index=event_index,
                event=event,
                window_type="session",
                window_start_ms=timestamp,
                window_end_ms=timestamp,
                reason="session_horizon_closed",
                detected_state="ABSENT_CLOSED",
            )
            return 1, 0, 0

        new_count = 0
        if candidates:
            candidates.sort(
                key=lambda item: (
                    int(item[1]["window_start_ms"]),
                    int(item[1].get("session_id") or 0),
                )
            )
            target_key, target = candidates[0]
            affected_keys = [key for key, _ in candidates]
            merged_start = min(int(window["window_start_ms"]) for _, window in candidates)
            merged_end = max(int(window["window_end_ms"]) for _, window in candidates)
            for key, window in candidates[1:]:
                _absorb_window(target, window)
                state["windows"].pop(key, None)
            target["window_start_ms"] = min(merged_start, timestamp)
            target["window_end_ms"] = max(merged_end, timestamp)
            _add_value_to_window(target, timestamp, event["value"])
            remaining = [key for key in keys if key not in affected_keys[1:]]
            self._set_group_session_keys(state, group, remaining)
        else:
            session_id = int(state["next_session_id"])
            state["next_session_id"] = session_id + 1
            key = _session_key(session_id)
            target = _new_window(
                window_type="session",
                start_ms=timestamp,
                end_ms=timestamp,
                group=group,
                session_id=session_id,
            )
            _add_value_to_window(target, timestamp, event["value"])
            state["windows"][key] = target
            keys.append(key)
            self._set_group_session_keys(state, group, keys)
            new_count = 1
        return 0, 1, new_count

    def _finalize_windows(self, state: dict[str, Any]) -> None:
        watermark = state["watermark_ms"]
        if watermark is None:
            return
        eligible: list[str] = []
        for key, window in state["windows"].items():
            if window["state"] != "OPEN":
                continue
            if window["window_type"] == "session":
                should_close = int(watermark) > int(window["window_end_ms"]) + self.config.session_gap_ms
            else:
                should_close = int(watermark) >= int(window["window_end_ms"]) + self.config.allowed_lateness_ms
            if should_close:
                eligible.append(key)
        for key in sorted(eligible, key=lambda item: window_sort_key(state["windows"][item])):
            window = state["windows"][key]
            WindowStateMachine.transition(window, "CLOSED")
            WindowStateMachine.transition(window, "EMITTED", int(state["version"]))

    def _evict_oldest_emitted(self, state: dict[str, Any], count: int, reason: str) -> list[dict[str, Any]]:
        emitted = [
            (key, window)
            for key, window in state["windows"].items()
            if window["state"] == "EMITTED"
        ]
        emitted.sort(
            key=lambda item: (
                int(item[1]["window_end_ms"]),
                int(item[1]["window_start_ms"]),
                str(item[1]["group"]),
                str(item[1]["window_type"]),
                item[0],
            )
        )
        evictions: list[dict[str, Any]] = []
        for key, window in emitted[:count]:
            evictions.append(
                {
                    "window_type": window["window_type"],
                    "window_start_ms": window["window_start_ms"],
                    "window_end_ms": window["window_end_ms"],
                    "group": window["group"],
                    "state": window["state"],
                    "count": window["count"],
                    "sum": _format_sum(window["sum"], window["compensation"], self.config.sum_decimals),
                    "reason": reason,
                    "evicted_at_version": state["version"],
                }
            )
            if window["window_type"] == "session":
                state["session_tombstones"].append(
                    {
                        "group": window["group"],
                        "window_start_ms": int(window["window_start_ms"]),
                        "window_end_ms": int(window["window_end_ms"]),
                        "state": window["state"],
                        "evicted_at_version": state["version"],
                    }
                )
            state["windows"].pop(key, None)
            group = window["group"]
            if window["window_type"] == "session" and group in state["session_index"]:
                state["session_index"][group] = [item for item in state["session_index"][group] if item != key]
                if not state["session_index"][group]:
                    state["session_index"].pop(group, None)
        if evictions:
            state["eviction_log"].extend(evictions)
            if len(state["eviction_log"]) > 200:
                state["eviction_log"] = state["eviction_log"][-200:]
        return evictions

    def _prune_session_tombstones(self, state: dict[str, Any]) -> None:
        watermark = state["watermark_ms"]
        if watermark is None:
            return
        cutoff = int(watermark)
        gap = self.config.session_gap_ms
        state["session_tombstones"] = [
            tombstone
            for tombstone in state["session_tombstones"]
            if cutoff <= int(tombstone["window_end_ms"]) + 2 * gap
        ]

    def _enforce_capacity(self, state: dict[str, Any]) -> list[dict[str, Any]]:
        active_count = len(state["windows"])
        late_count = len(state["late_queue"])
        relief = max(
            active_count - self.config.active_window_soft_limit,
            late_count - self.config.late_queue_soft_limit,
            0,
        )
        evictions = self._evict_oldest_emitted(
            state, relief, "soft_limit_eviction" if relief else "capacity_support"
        ) if relief else []
        active_after = len(state["windows"])
        late_after = len(state["late_queue"])
        if (
            active_after > self.config.active_window_hard_limit
            or late_after > self.config.late_queue_hard_limit
        ):
            raise BusinessError(
                E_CAPACITY_EXCEEDED,
                "Hard capacity limit would be exceeded",
                details={
                    "active_window_count": active_after,
                    "active_window_hard_limit": self.config.active_window_hard_limit,
                    "late_queue_length": late_after,
                    "late_queue_hard_limit": self.config.late_queue_hard_limit,
                    "evictable_emitted_windows": len(evictions),
                },
            )
        self._prune_session_tombstones(state)
        return evictions

    def submit_batch(self, batch_id: str, events: list[Any]) -> dict[str, Any]:
        normalized = self._normalize_events(events)
        if len(normalized) > self.config.batch_event_hard_limit:
            raise BusinessError(
                E_CAPACITY_EXCEEDED,
                "Batch event count exceeds configured limit",
                details={
                    "event_count": len(normalized),
                    "batch_event_hard_limit": self.config.batch_event_hard_limit,
                },
            )
        content_hash = self._batch_hash(batch_id, normalized)

        wait_entry: _InFlightBatch | None = None
        own_entry: _InFlightBatch | None = None
        with self._state_lock:
            existing = self._state["batch_registry"].get(batch_id)
            if existing is not None:
                if existing["content_hash"] != content_hash:
                    raise BusinessError(
                        E_CONFLICTING_BATCH,
                        "batch_id was already committed with different content",
                        details={"batch_id": batch_id},
                    )
                response = copy.deepcopy(existing["response"])
                response["replayed"] = True
                return response
            existing_inflight = self._inflight.get(batch_id)
            if existing_inflight is not None:
                if existing_inflight.content_hash != content_hash:
                    raise BusinessError(
                        E_CONFLICTING_BATCH,
                        "batch_id is in flight with different content",
                        details={"batch_id": batch_id},
                    )
                wait_entry = existing_inflight
            else:
                own_entry = _InFlightBatch(content_hash=content_hash)
                self._inflight[batch_id] = own_entry

        if wait_entry is not None:
            wait_entry.event.wait()
            if wait_entry.error is not None:
                raise wait_entry.error
            if wait_entry.result is None:
                raise BusinessError(E_INTERNAL, "In-flight batch produced no result")
            result = copy.deepcopy(wait_entry.result)
            result["replayed"] = True
            return result

        assert own_entry is not None
        try:
            result = self._process_batch_locked(batch_id, normalized, content_hash)
            own_entry.result = result
            return copy.deepcopy(result)
        except BusinessError as exc:
            own_entry.error = exc
            raise
        except Exception as exc:  # pragma: no cover - defensive translation
            LOGGER.exception("Unexpected batch processing error")
            translated = BusinessError(E_INTERNAL, "Unexpected processing error", details={"reason": str(exc)})
            own_entry.error = translated
            raise translated from exc
        finally:
            own_entry.event.set()
            with self._state_lock:
                self._inflight.pop(batch_id, None)

    def _next_version(self, baseline_version: int) -> int:
        return max(int(baseline_version), self._latest_seen_version) + 1

    def _process_batch_locked(
        self, batch_id: str, events: list[dict[str, Any]], content_hash: str
    ) -> dict[str, Any]:
        with self._write_lock:
            with self._state_lock:
                existing = self._state["batch_registry"].get(batch_id)
                if existing is not None:
                    if existing["content_hash"] != content_hash:
                        raise BusinessError(
                            E_CONFLICTING_BATCH,
                            "batch_id was already committed with different content",
                            details={"batch_id": batch_id},
                        )
                    response = copy.deepcopy(existing["response"])
                    response["replayed"] = True
                    return response
                baseline = self._state
                next_version = self._next_version(baseline["version"])
                intent_id = f"batch:{batch_id}:{uuid.uuid4().hex}"
                self.store.record_intent_start(
                    intent_id=intent_id,
                    batch_id=batch_id,
                    content_hash=content_hash,
                    kind="batch",
                    baseline_version=int(baseline["version"]),
                )
            try:
                self.crash.maybe_crash("after_intent", batch_id=batch_id, kind="batch")
                working = copy.deepcopy(baseline)
                working["version"] = next_version
                working["last_intent_id"] = intent_id
                working["last_batch_id"] = batch_id
                base_watermark = working["watermark_ms"]
                ordered_events = sorted(
                    enumerate(events),
                    key=lambda item: (
                        item[1]["timestamp_ms"],
                        item[1]["group"],
                        item[1]["value"] is not None,
                        0.0 if item[1]["value"] is None else item[1]["value"],
                    ),
                )
                late_assignments = 0
                applied_assignments = 0
                new_windows = 0
                for event_index, event in ordered_events:
                    late, applied, created = self._apply_flat_event(
                        working,
                        batch_id=batch_id,
                        event_index=event_index,
                        event=event,
                    )
                    late_assignments += late
                    applied_assignments += applied
                    new_windows += created
                    late, applied, created = self._apply_session_event(
                        working,
                        batch_id=batch_id,
                        event_index=event_index,
                        event=event,
                    )
                    late_assignments += late
                    applied_assignments += applied
                    new_windows += created
                if events:
                    batch_max = max(event["timestamp_ms"] for event in events)
                    working["watermark_ms"] = batch_max if base_watermark is None else max(base_watermark, batch_max)
                self._finalize_windows(working)
                evictions = self._enforce_capacity(working)
                response = {
                    "ok": True,
                    "batch_id": batch_id,
                    "content_hash": content_hash,
                    "state_version": working["version"],
                    "accepted_events": len(events),
                    "late_assignments": late_assignments,
                    "applied_window_assignments": applied_assignments,
                    "new_windows": new_windows,
                    "evictions": evictions,
                    "watermark_ms": working["watermark_ms"],
                    "window_count": len(working["windows"]),
                    "late_queue_length": len(working["late_queue"]),
                    "replayed": False,
                }
                response["snapshot_version"] = working["version"]
                working["batch_registry"][batch_id] = {
                    "content_hash": content_hash,
                    "response": copy.deepcopy(response),
                }
                snapshot = self.store.write_snapshot(
                    working,
                    intent_id=intent_id,
                    batch_id=batch_id,
                    kind="batch",
                )
                with self._state_lock:
                    self._state = working
                    self._latest_seen_version = max(self._latest_seen_version, snapshot.version)
                try:
                    self.store.mark_intent_finished(intent_id, "committed", f"snapshot_version={snapshot.version}")
                except Exception:  # pragma: no cover - snapshot already proves commit
                    LOGGER.exception("Could not mark committed intent; durable snapshot remains authoritative")
                return response
            except BusinessError as exc:
                try:
                    self.store.mark_intent_finished(intent_id, "aborted", f"{exc.code}: {exc.message}")
                except Exception:  # pragma: no cover - journal already contains pending fact
                    LOGGER.exception("Could not mark intent aborted")
                raise
            except Exception as exc:
                try:
                    self.store.mark_intent_finished(intent_id, "aborted", f"internal error: {exc}")
                except Exception:  # pragma: no cover
                    LOGGER.exception("Could not mark intent aborted")
                raise

    def trigger_snapshot(self, label: str | None = None) -> dict[str, Any]:
        with self._write_lock:
            with self._state_lock:
                baseline = self._state
                next_version = self._next_version(baseline["version"])
                intent_id = f"snapshot:{uuid.uuid4().hex}"
                self.store.record_intent_start(
                    intent_id=intent_id,
                    batch_id=None,
                    content_hash=None,
                    kind="snapshot",
                    baseline_version=int(baseline["version"]),
                )
            try:
                working = copy.deepcopy(baseline)
                working["version"] = next_version
                working["last_intent_id"] = intent_id
                snapshot = self.store.write_snapshot(
                    working,
                    intent_id=intent_id,
                    batch_id=None,
                    kind="snapshot",
                )
                with self._state_lock:
                    self._state = working
                    self._latest_seen_version = max(self._latest_seen_version, snapshot.version)
                try:
                    self.store.mark_intent_finished(intent_id, "committed", f"snapshot_version={snapshot.version}")
                except Exception:  # pragma: no cover - snapshot already proves commit
                    LOGGER.exception("Could not mark snapshot intent; durable snapshot remains authoritative")
                return {
                    "ok": True,
                    "label": label,
                    "snapshot": snapshot.public_dict(),
                    "state_version": working["version"],
                }
            except Exception as exc:
                try:
                    self.store.mark_intent_finished(intent_id, "aborted", f"snapshot error: {exc}")
                except Exception:  # pragma: no cover
                    LOGGER.exception("Could not mark snapshot intent aborted")
                raise

    def load_snapshot(self, *, version: int | None = None, path: str | Path | None = None) -> dict[str, Any]:
        if (version is None) == (path is None):
            raise BusinessError(E_REQUEST_INVALID, "Provide exactly one of version or path")
        if version is not None:
            source = self.store.load_version(version)
        else:
            source = self.store.load_external(Path(str(path)))
        self._validate_state(source.body, details={"source_snapshot_version": source.version})
        loaded_state = copy.deepcopy(source.body)

        with self._write_lock:
            with self._state_lock:
                current_version = int(self._state["version"])
                if source.version <= current_version:
                    raise BusinessError(
                        E_SNAPSHOT_INVALID,
                        "Snapshot version is not monotonic",
                        details={"source_version": source.version, "current_version": current_version},
                    )
                baseline_version = current_version
                next_version = max(source.version, current_version, self._latest_seen_version) + 1
                intent_id = f"load:{source.version}:{uuid.uuid4().hex}"
                self.store.record_intent_start(
                    intent_id=intent_id,
                    batch_id=None,
                    content_hash=source.checksum,
                    kind="load",
                    baseline_version=baseline_version,
                )
            try:
                loaded_state["version"] = next_version
                loaded_state["config_fingerprint"] = self.config.fingerprint
                loaded_state["last_intent_id"] = intent_id
                # Replace the entire state. Nothing from the prior in-memory state is merged.
                snapshot = self.store.write_snapshot(
                    loaded_state,
                    intent_id=intent_id,
                    batch_id=None,
                    kind="load",
                )
                with self._state_lock:
                    old_version = self._state["version"]
                    self._state = loaded_state
                    self._latest_seen_version = max(self._latest_seen_version, snapshot.version)
                try:
                    self.store.mark_intent_finished(intent_id, "committed", f"loaded_from={source.version}")
                except Exception:  # pragma: no cover - snapshot already proves commit
                    LOGGER.exception("Could not mark load intent; durable snapshot remains authoritative")
                return {
                    "ok": True,
                    "whole_state_replaced": True,
                    "loaded_from_version": source.version,
                    "previous_state_version": old_version,
                    "new_state_version": loaded_state["version"],
                    "snapshot": snapshot.public_dict(),
                    "window_count": len(loaded_state["windows"]),
                    "late_queue_length": len(loaded_state["late_queue"]),
                }
            except Exception as exc:
                try:
                    self.store.mark_intent_finished(intent_id, "aborted", f"load error: {exc}")
                except Exception:  # pragma: no cover
                    LOGGER.exception("Could not mark load intent aborted")
                raise

    def list_snapshots(self) -> list[dict[str, Any]]:
        return self.store.list_snapshots()

    def _public_window(self, window: dict[str, Any], probabilities: list[float]) -> dict[str, Any]:
        is_session = window["window_type"] == "session"
        return {
            "window_type": window["window_type"],
            "window_start_ms": int(window["window_start_ms"]),
            "window_end_ms": int(window["window_end_ms"]),
            "interval": (
                "inclusive_event_span" if is_session else "half_open"
            ),
            "group": window["group"],
            "state": window["state"],
            "count": int(window["count"]),
            "non_null_count": int(window["non_null_count"]),
            "aggregate_status": "ok" if int(window["non_null_count"]) > 0 else "empty",
            "sum": _format_sum(window["sum"], window["compensation"], self.config.sum_decimals),
            "min": window["min"],
            "max": window["max"],
            "quantiles": _quantile_records(window["values"], probabilities),
            "first_event_ts_ms": window["first_event_ts_ms"],
            "last_event_ts_ms": window["last_event_ts_ms"],
            "emitted_at_version": window["emitted_at_version"],
            "session_id": window.get("session_id"),
        }

    def _query_signature(
        self,
        *,
        start_ms: int,
        end_ms: int,
        group: str | None,
        window_types: list[str],
        quantiles: list[float],
    ) -> str:
        return sha256_json(
            {
                "start_ms": start_ms,
                "end_ms": end_ms,
                "group": group,
                "window_types": sorted(window_types),
                "quantiles": quantiles,
            }
        )

    def _decode_cursor(self, cursor: str) -> tuple[str, int]:
        try:
            padding = "=" * (-len(cursor) % 4)
            raw = base64.urlsafe_b64decode((cursor + padding).encode("ascii"))
            payload = json.loads(raw.decode("utf-8"))
            return str(payload["view"]), int(payload["offset"])
        except (ValueError, KeyError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise BusinessError(E_REQUEST_INVALID, "Malformed query cursor") from exc

    def _encode_cursor(self, view_id: str, offset: int) -> str:
        payload = canonical_json({"v": 1, "view": view_id, "offset": offset}).encode("utf-8")
        return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")

    def query_windows(
        self,
        *,
        start_ms: int,
        end_ms: int,
        group: str | None,
        window_types: list[str],
        quantiles: list[float] | None,
        limit: int,
        cursor: str | None,
    ) -> dict[str, Any]:
        if end_ms <= start_ms:
            raise BusinessError(E_REQUEST_INVALID, "end_ms must be greater than start_ms")
        for window_type in window_types:
            if window_type not in WINDOW_TYPES:
                raise BusinessError(E_REQUEST_INVALID, f"Unknown window type: {window_type}")
        window_types = list(dict.fromkeys(window_types))
        probabilities = list(dict.fromkeys(quantiles)) if quantiles is not None else list(self.config.default_quantiles)
        for probability in probabilities:
            if not 0.0 < probability <= 1.0:
                raise BusinessError(E_REQUEST_INVALID, "Quantiles must satisfy 0 < p <= 1")
        signature = self._query_signature(
            start_ms=start_ms,
            end_ms=end_ms,
            group=group,
            window_types=window_types,
            quantiles=probabilities,
        )

        if cursor:
            view_id, offset = self._decode_cursor(cursor)
            with self._state_lock:
                view = self._query_views.get(view_id)
            if view is None or view.signature != signature:
                raise BusinessError(
                    E_CURSOR_EXPIRED,
                    "Query view expired or cursor filters do not match the original request",
                )
        else:
            with self._state_lock:
                state = self._state
                records: list[dict[str, Any]] = []
                for window in state["windows"].values():
                    if window["window_type"] not in window_types:
                        continue
                    if group is not None and window["group"] != group:
                        continue
                    if window["window_type"] == "session":
                        intersects = int(window["window_start_ms"]) <= end_ms and int(window["window_end_ms"]) >= start_ms
                    else:
                        intersects = int(window["window_start_ms"]) < end_ms and int(window["window_end_ms"]) > start_ms
                    if not intersects:
                        continue
                    records.append(self._public_window(window, probabilities))
                records.sort(
                    key=lambda item: (
                        item["window_start_ms"],
                        item["group"],
                        item["window_type"],
                        item["window_end_ms"],
                        item.get("session_id") or 0,
                    )
                )
                view_id = uuid.uuid4().hex
                view = QueryView(
                    view_id=view_id,
                    signature=signature,
                    records=records,
                    as_of_version=int(state["version"]),
                    created_monotonic=time.monotonic(),
                )
                self._query_views[view_id] = view
                while len(self._query_views) > self._query_view_limit:
                    self._query_views.popitem(last=False)
            offset = 0

        page = view.records[offset : offset + limit]
        next_offset = offset + len(page)
        next_cursor = self._encode_cursor(view.view_id, next_offset) if next_offset < len(view.records) else None
        return {
            "ok": True,
            "as_of_version": view.as_of_version,
            "query_view_id": view.view_id,
            "range_match": "window interval overlaps requested range",
            "sort_order": ["window_start_ms", "group", "window_type", "window_end_ms", "session_id"],
            "total": len(view.records),
            "offset": offset,
            "limit": limit,
            "items": copy.deepcopy(page),
            "next_cursor": next_cursor,
            "sum_rounding": {
                "method": "Neumaier compensated sum, converted to Decimal and ROUND_HALF_EVEN",
                "decimals": self.config.sum_decimals,
                "json_representation": "fixed-width decimal string",
            },
            "quantile_method": {
                "name": "nearest-rank",
                "rank_formula": "clamp(ceil(p * sample_size), 1, sample_size)",
                "empty_sample": "value=null, rank=null, sample_size=0",
            },
        }

    def late_events(self, *, limit: int = 100, offset: int = 0, group: str | None = None) -> dict[str, Any]:
        if limit < 1 or limit > 1000:
            raise BusinessError(E_REQUEST_INVALID, "limit must be between 1 and 1000")
        if offset < 0:
            raise BusinessError(E_REQUEST_INVALID, "offset must be non-negative")
        with self._state_lock:
            state = self._state
            entries = [
                copy.deepcopy(entry)
                for entry in state["late_queue"]
                if group is None or entry["group"] == group
            ]
            entries.sort(key=lambda entry: entry["seq"])
            total = len(entries)
            page = entries[offset : offset + limit]
            return {
                "ok": True,
                "as_of_version": state["version"],
                "late_policy": self.config.late_policy,
                "queue_length": len(state["late_queue"]),
                "total_matching": total,
                "offset": offset,
                "limit": limit,
                "items": page,
            }

    def legal_transitions(self) -> dict[str, list[str]]:
        return {state: sorted(targets) for state, targets in LEGAL_TRANSITIONS.items()}
