"""Domain engine: ingestion, versioned windows, quota, breaker, and recovery."""

from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .breaker import (
    BreakerState,
    evaluate_automatic,
    manual_transition,
)
from .config import GuardConfig
from .errors import BusinessError, validation_error
from .numerics import (
    canonical_json,
    compute_window_stats,
    derive_quota,
    primary_quantile_us,
    propagate_metrics,
    sample_digest,
    samples_payload,
    sha256_json,
)
from .storage import Storage, fetch_all_dicts, row_dict, utc_now


FaultHook = Callable[[str, dict[str, Any]], None]


@dataclass(slots=True)
class NormalizedEvent:
    event_index: int
    timestamp_ms: int
    route: str
    success: bool
    latency_us: int

    def canonical(self) -> dict[str, Any]:
        return {
            "timestamp_ms": self.timestamp_ms,
            "route": self.route,
            "success": self.success,
            "latency_us": self.latency_us,
        }


@dataclass(slots=True)
class NormalizedBatch:
    batch_id: str
    sequence: int
    events: list[NormalizedEvent]
    content_hash: str
    route_summary: dict[str, int]

    def intent_payload(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "sequence": self.sequence,
            "content_hash": self.content_hash,
            "event_count": len(self.events),
            "route_summary": self.route_summary,
        }


def _require_int(value: Any, field: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise validation_error(f"{field} must be an integer")
    if minimum is not None and value < minimum:
        raise validation_error(f"{field} must be >= {minimum}")
    return value


def _normalize_batch(payload: Mapping[str, Any]) -> NormalizedBatch:
    batch_id = payload.get("batch_id")
    if not isinstance(batch_id, str) or not batch_id.strip():
        raise validation_error("batch_id must be a non-empty string")
    if len(batch_id) > 128:
        raise validation_error("batch_id must be at most 128 characters")
    sequence = _require_int(payload.get("sequence", 1), "sequence", 1)
    events_payload = payload.get("events")
    if not isinstance(events_payload, list) or not events_payload:
        raise validation_error("events must be a non-empty list")
    if len(events_payload) > 100_000:
        raise validation_error("events exceeds the 100000 event limit")

    events: list[NormalizedEvent] = []
    route_summary: dict[str, int] = {}
    for index, raw in enumerate(events_payload):
        if not isinstance(raw, Mapping):
            raise validation_error(f"events[{index}] must be an object")
        route = raw.get("route", raw.get("route_id"))
        if not isinstance(route, str) or not route:
            raise validation_error(f"events[{index}].route must be a non-empty string")
        if len(route) > 512:
            raise validation_error(f"events[{index}].route is too long")
        success = raw.get("success")
        if isinstance(success, int) and not isinstance(success, bool) and success in (0, 1):
            success = bool(success)
        if not isinstance(success, bool):
            raise validation_error(f"events[{index}].success must be boolean")
        timestamp_ms = _require_int(
            raw.get("timestamp_ms", raw.get("ts_ms")),
            f"events[{index}].timestamp_ms",
            0,
        )
        latency_us = _require_int(raw.get("latency_us"), f"events[{index}].latency_us", 0)
        events.append(
            NormalizedEvent(
                event_index=index,
                timestamp_ms=timestamp_ms,
                route=route,
                success=success,
                latency_us=latency_us,
            )
        )
        route_summary[route] = route_summary.get(route, 0) + 1

    canonical_events = [event.canonical() for event in events]
    content_hash = sha256_json(
        {"batch_id": batch_id, "sequence": sequence, "events": canonical_events}
    )
    return NormalizedBatch(
        batch_id=batch_id,
        sequence=sequence,
        events=events,
        content_hash=content_hash,
        route_summary=route_summary,
    )


class Engine:
    def __init__(
        self,
        database: str | Path,
        config: GuardConfig | None = None,
        *,
        fault_hook: FaultHook | None = None,
        auto_recover: bool = True,
    ) -> None:
        self.config = config or GuardConfig()
        self.config.validate()
        self.storage = Storage(database, self.config)
        self.storage.initialize()
        self._lock = threading.RLock()
        self._fault_hook = fault_hook
        self.startup_recovery = self.storage.recover() if auto_recover else None

    def _fault(self, stage: str, context: dict[str, Any]) -> None:
        if self._fault_hook is not None:
            self._fault_hook(stage, context)

    # ------------------------------------------------------------------
    # Ingestion and version publication
    # ------------------------------------------------------------------

    def ingest_batch(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        batch = _normalize_batch(payload)
        with self._lock:
            with self.storage.read_connection() as check:
                existing = check.execute(
                    "SELECT * FROM batches WHERE batch_id=? AND sequence=?",
                    (batch.batch_id, batch.sequence),
                ).fetchone()
            if existing is not None:
                if existing["content_hash"] != batch.content_hash:
                    raise BusinessError(
                        "E_CONFLICTING_REPORT",
                        "batch_id and sequence already exist with different content",
                        409,
                        {
                            "batch_id": batch.batch_id,
                            "sequence": batch.sequence,
                            "retained_content_hash": existing["content_hash"],
                            "submitted_content_hash": batch.content_hash,
                            "retained_status": existing["status"],
                        },
                    )
                if existing["result_json"]:
                    result = json.loads(existing["result_json"])
                else:
                    result = {
                        "ok": True,
                        "batch": {
                            "batch_id": existing["batch_id"],
                            "sequence": existing["sequence"],
                            "content_hash": existing["content_hash"],
                            "status": existing["status"],
                            "event_count": existing["event_count"],
                            "abort_reason": existing["abort_reason"],
                        },
                        "routes": {},
                    }
                result["duplicate"] = True
                result["duplicate_policy"] = "RETURNED_FIRST_COMMITTED_REPORT"
                return result

            intent_id = self.storage.prepared_intent(
                "INGEST",
                f"{batch.batch_id}:{batch.sequence}",
                batch.intent_payload(),
            )
            try:
                self._fault("after_intent", {"intent_id": intent_id, "batch": batch.intent_payload()})
                with self.storage.transaction() as connection:
                    self._ensure_routes(connection, batch)
                    self._insert_events(connection, batch)
                    self._update_watermarks(connection, batch)
                    changed = self._recompute_closed_windows(connection, batch)
                    result = self._build_batch_result(connection, batch, changed, duplicate=False)
                    result_json = canonical_json(result)
                    now = utc_now()
                    connection.execute(
                        "INSERT INTO batches(batch_id, sequence, content_hash, status, event_count, "
                        "route_summary_json, result_json, abort_reason, created_at, completed_at) "
                        "VALUES(?, ?, ?, 'COMMITTED', ?, ?, ?, NULL, ?, ?)",
                        (
                            batch.batch_id,
                            batch.sequence,
                            batch.content_hash,
                            len(batch.events),
                            canonical_json(batch.route_summary),
                            result_json,
                            now,
                            now,
                        ),
                    )
                    self.storage.complete_intent(connection, intent_id)
                return result
            except Exception:
                self.storage.abort_intent(intent_id)
                raise

    def _ensure_routes(self, connection: sqlite3.Connection, batch: NormalizedBatch) -> None:
        now = utc_now()
        for route in batch.route_summary:
            connection.execute(
                "INSERT INTO routes(route, watermark_ms, current_composite_version, created_at) "
                "VALUES(?, 0, NULL, ?) ON CONFLICT(route) DO NOTHING",
                (route, now),
            )
            connection.execute(
                "INSERT INTO route_runtime(route, current_concurrency, budget_watermark, "
                "target_latency_us, updated_at) VALUES(?, 0, '1.0', ?, ?) "
                "ON CONFLICT(route) DO NOTHING",
                (route, self.config.target_latency_us, now),
            )

    def _insert_events(self, connection: sqlite3.Connection, batch: NormalizedBatch) -> None:
        rows = [
            (
                batch.batch_id,
                batch.sequence,
                event.event_index,
                event.timestamp_ms,
                event.route,
                int(event.success),
                event.latency_us,
            )
            for event in batch.events
        ]
        connection.executemany(
            "INSERT INTO events(batch_id, sequence, event_index, timestamp_ms, route, success, latency_us) "
            "VALUES(?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        now = utc_now()
        candidate_rows: set[tuple[str, int, int]] = set()
        for event in batch.events:
            for start_ms in self.config.containing_window_starts(event.timestamp_ms):
                candidate_rows.add(
                    (event.route, start_ms, start_ms + self.config.window_length_ms)
                )
        connection.executemany(
            "INSERT INTO window_candidates(route, start_ms, end_ms, first_seen_at) "
            "VALUES(?, ?, ?, ?) ON CONFLICT(route, start_ms) DO UPDATE SET end_ms=excluded.end_ms",
            [(route, start_ms, end_ms, now) for route, start_ms, end_ms in sorted(candidate_rows)],
        )

    def _update_watermarks(self, connection: sqlite3.Connection, batch: NormalizedBatch) -> None:
        maxima: dict[str, int] = {}
        for event in batch.events:
            maxima[event.route] = max(maxima.get(event.route, -1), event.timestamp_ms)
        for route, watermark in maxima.items():
            connection.execute(
                "UPDATE routes SET watermark_ms=MAX(watermark_ms, ?) WHERE route=?",
                (watermark, route),
            )

    def _recompute_closed_windows(
        self,
        connection: sqlite3.Connection,
        batch: NormalizedBatch,
    ) -> list[dict[str, Any]]:
        changed: list[dict[str, Any]] = []
        routes = sorted(batch.route_summary)
        for route in routes:
            watermark_row = connection.execute(
                "SELECT watermark_ms FROM routes WHERE route=?", (route,)
            ).fetchone()
            watermark = int(watermark_row["watermark_ms"])
            candidates = connection.execute(
                "SELECT start_ms, end_ms FROM window_candidates "
                "WHERE route=? AND end_ms<=? ORDER BY end_ms, start_ms",
                (route, watermark),
            ).fetchall()
            last_published_window_version_id: int | None = None
            route_changed = False
            for candidate in candidates:
                start_ms = int(candidate["start_ms"])
                end_ms = int(candidate["end_ms"])
                samples = fetch_all_dicts(
                    connection,
                    "SELECT timestamp_ms, batch_id, sequence, event_index, success, latency_us "
                    "FROM events WHERE route=? AND timestamp_ms>=? AND timestamp_ms<? "
                    "ORDER BY timestamp_ms, batch_id, sequence, event_index",
                    (route, start_ms, end_ms),
                )
                if not samples:
                    continue
                digest = sample_digest(samples)
                head = connection.execute(
                    "SELECT wv.* FROM window_heads wh JOIN window_versions wv "
                    "ON wv.id=wh.current_version_id WHERE wh.route=? AND wh.start_ms=?",
                    (route, start_ms),
                ).fetchone()
                if head is not None and head["sample_digest"] == digest and int(head["closed"]) == 1:
                    continue

                stats = compute_window_stats(
                    route=route,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    samples=samples,
                    config=self.config,
                )
                version = 1 if head is None else int(head["version"]) + 1
                now = utc_now()
                cursor = connection.execute(
                    "INSERT INTO window_versions(route, start_ms, end_ms, version, closed, sample_count, "
                    "success_count, failure_count, consecutive_failures, sample_digest, stats_json, "
                    "samples_json, source_batch_id, source_sequence, created_at) "
                    "VALUES(?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        route,
                        start_ms,
                        end_ms,
                        version,
                        stats["samples"]["sample_count"],
                        stats["samples"]["success_count"],
                        stats["samples"]["failure_count"],
                        stats["samples"]["consecutive_failures"],
                        digest,
                        canonical_json(stats),
                        canonical_json(samples_payload(samples)),
                        batch.batch_id,
                        batch.sequence,
                        now,
                    ),
                )
                window_version_id = int(cursor.lastrowid)
                connection.execute(
                    "INSERT INTO window_heads(route, start_ms, end_ms, current_version_id) "
                    "VALUES(?, ?, ?, ?) ON CONFLICT(route, start_ms) DO UPDATE SET "
                    "end_ms=excluded.end_ms, current_version_id=excluded.current_version_id",
                    (route, start_ms, end_ms, window_version_id),
                )
                self._fault(
                    "after_window_version",
                    {
                        "route": route,
                        "window_version_id": window_version_id,
                        "version": version,
                        "batch_id": batch.batch_id,
                    },
                )
                composite = self._publish_composite(
                    connection,
                    route=route,
                    window_version_id=window_version_id,
                    window_version=version,
                    stats=stats,
                    batch_id=batch.batch_id,
                    sequence=batch.sequence,
                )
                changed.append(composite)
                last_published_window_version_id = window_version_id
                route_changed = True

            # A late event can create a new version of an older window. Never let
            # that update move the route's current composite backwards. Re-publish
            # the latest closed window so every reader still sees one complete,
            # newest composite.
            latest_head = connection.execute(
                "SELECT wv.* FROM window_heads wh JOIN window_versions wv "
                "ON wv.id=wh.current_version_id WHERE wh.route=? "
                "ORDER BY wv.end_ms DESC, wv.start_ms DESC LIMIT 1",
                (route,),
            ).fetchone()
            if (
                route_changed
                and latest_head is not None
                and int(latest_head["id"]) != last_published_window_version_id
            ):
                latest_stats = json.loads(latest_head["stats_json"])
                composite = self._publish_composite(
                    connection,
                    route=route,
                    window_version_id=int(latest_head["id"]),
                    window_version=int(latest_head["version"]),
                    stats=latest_stats,
                    batch_id=batch.batch_id,
                    sequence=batch.sequence,
                )
                changed.append(composite)
        return changed

    def _publish_composite(
        self,
        connection: sqlite3.Connection,
        *,
        route: str,
        window_version_id: int,
        window_version: int,
        stats: Mapping[str, Any],
        batch_id: str,
        sequence: int,
    ) -> dict[str, Any]:
        # 1. Breaker evaluation against this exact window version.
        breaker_row = connection.execute(
            "SELECT bv.* FROM breaker_heads bh JOIN breaker_versions bv "
            "ON bv.id=bh.current_version_id WHERE bh.route=?",
            (route,),
        ).fetchone()
        if breaker_row is None:
            current_breaker = BreakerState(state="CLOSED", version=0)
        else:
            current_breaker = BreakerState(
                state=breaker_row["state"],
                version=int(breaker_row["version"]),
                opened_at_ms=breaker_row["opened_at_ms"],
                next_probe_at_ms=breaker_row["next_probe_at_ms"],
                half_open_since_ms=breaker_row["half_open_since_ms"],
                last_reason=breaker_row["last_reason"],
            )
        decision = evaluate_automatic(
            current_breaker,
            stats,
            int(stats["window"]["end_ms"]),
            self.config,
        )
        next_breaker_version = current_breaker.version + 1
        breaker_details = dict(decision.details)
        breaker_details.update(
            {
                "from_version": current_breaker.version,
                "to_version": next_breaker_version,
                "window_version": window_version,
                "window_version_id": window_version_id,
            }
        )
        cursor = connection.execute(
            "INSERT INTO breaker_versions(route, version, window_version_id, state, opened_at_ms, "
            "next_probe_at_ms, half_open_since_ms, last_reason, details_json, created_at) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                route,
                next_breaker_version,
                window_version_id,
                decision.state,
                decision.opened_at_ms,
                decision.next_probe_at_ms,
                decision.half_open_since_ms,
                decision.reason,
                canonical_json(breaker_details),
                utc_now(),
            ),
        )
        breaker_version_id = int(cursor.lastrowid)
        connection.execute(
            "INSERT INTO breaker_heads(route, current_version_id, state) VALUES(?, ?, ?) "
            "ON CONFLICT(route) DO UPDATE SET current_version_id=excluded.current_version_id, "
            "state=excluded.state",
            (route, breaker_version_id, decision.state),
        )
        self._fault(
            "after_breaker_version",
            {
                "route": route,
                "breaker_version_id": breaker_version_id,
                "version": next_breaker_version,
            },
        )

        # 2. Quota propagation across newest closed windows, with failure-safe
        # insufficient-sample handling that still leaves a complete composite.
        runtime = connection.execute(
            "SELECT * FROM route_runtime WHERE route=?", (route,)
        ).fetchone()
        current_concurrency = int(runtime["current_concurrency"]) if runtime else 0
        budget = Decimal(runtime["budget_watermark"]) if runtime else Decimal(1)
        target_latency = (
            int(runtime["target_latency_us"]) if runtime else self.config.target_latency_us
        )
        quota_head = connection.execute(
            "SELECT final_quota FROM quota_heads WHERE route=?", (route,)
        ).fetchone()
        previous_quota = int(quota_head["final_quota"]) if quota_head else None
        try:
            views = self._propagation_views(
                connection,
                route,
                current=stats,
                current_window_version=window_version,
                current_window_version_id=window_version_id,
            )
            propagation = propagate_metrics(views, self.config.propagation_weights)
            quota_details = derive_quota(
                propagation=propagation,
                breaker_state=decision.state,
                current_concurrency=current_concurrency,
                previous_quota=previous_quota,
                budget_watermark=budget,
                target_latency_us=target_latency,
                config=self.config,
            )
            final_quota = int(quota_details["final_quota"])
            quota_status = "OK"
            business_code = None
        except ValueError as exc:
            p95 = primary_quantile_us(stats, "P95")
            if p95 is not None:
                raise
            final_quota = self.config.min_quota
            quota_status = "INSUFFICIENT_SAMPLES"
            business_code = "E_INSUFFICIENT_SAMPLES"
            quota_details = {
                "status": quota_status,
                "business_code": business_code,
                "reason": "P95_INSUFFICIENT_SAMPLES",
                "required_samples": self.config.min_samples_p95,
                "sample_count": stats["samples"]["sample_count"],
                "final_quota": final_quota,
                "previous_quota": previous_quota,
                "breaker_state": decision.state,
                "message": str(exc),
                "window": {
                    "start_ms": stats["window"]["start_ms"],
                    "end_ms": stats["window"]["end_ms"],
                    "window_version": window_version,
                },
                "method_comparison": stats["method_comparison"],
                "error_propagation": stats["error_propagation"],
                "steps": [],
            }
        quota_details["status"] = quota_status
        if business_code:
            quota_details["business_code"] = business_code
        if breaker_row is None:
            next_quota_version = 1
        else:
            latest_quota = connection.execute(
                "SELECT version FROM quota_versions WHERE route=? ORDER BY version DESC LIMIT 1",
                (route,),
            ).fetchone()
            next_quota_version = 1 if latest_quota is None else int(latest_quota["version"]) + 1
        cursor = connection.execute(
            "INSERT INTO quota_versions(route, version, window_version_id, breaker_version_id, "
            "previous_quota, final_quota, details_json, created_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
            (
                route,
                next_quota_version,
                window_version_id,
                breaker_version_id,
                previous_quota,
                final_quota,
                canonical_json(quota_details),
                utc_now(),
            ),
        )
        quota_version_id = int(cursor.lastrowid)
        connection.execute(
            "INSERT INTO quota_heads(route, current_version_id, final_quota, window_version_id) "
            "VALUES(?, ?, ?, ?) ON CONFLICT(route) DO UPDATE SET "
            "current_version_id=excluded.current_version_id, final_quota=excluded.final_quota, "
            "window_version_id=excluded.window_version_id",
            (route, quota_version_id, final_quota, window_version_id),
        )

        # 3. One immutable composite version links both paths to one window.
        head = connection.execute(
            "SELECT current_version FROM composite_heads WHERE route=?", (route,)
        ).fetchone()
        next_composite_version = 1 if head is None else int(head["current_version"]) + 1
        committed_at = utc_now()
        connection.execute(
            "INSERT INTO composite_snapshots(route, version, window_version_id, quota_version_id, "
            "breaker_version_id, committed_at) VALUES(?, ?, ?, ?, ?, ?)",
            (
                route,
                next_composite_version,
                window_version_id,
                quota_version_id,
                breaker_version_id,
                committed_at,
            ),
        )
        connection.execute(
            "INSERT INTO composite_heads(route, current_version, window_version_id, "
            "quota_version_id, breaker_version_id) VALUES(?, ?, ?, ?, ?) "
            "ON CONFLICT(route) DO UPDATE SET current_version=excluded.current_version, "
            "window_version_id=excluded.window_version_id, quota_version_id=excluded.quota_version_id, "
            "breaker_version_id=excluded.breaker_version_id",
            (
                route,
                next_composite_version,
                window_version_id,
                quota_version_id,
                breaker_version_id,
            ),
        )
        connection.execute(
            "UPDATE routes SET current_composite_version=? WHERE route=?",
            (next_composite_version, route),
        )
        if decision.changed:
            transition_details = dict(breaker_details)
            transition_details.update(
                {
                    "composite_version": next_composite_version,
                    "source": "SYSTEM",
                }
            )
            connection.execute(
                "INSERT INTO breaker_transitions(route, from_state, to_state, reason, source, "
                "occurred_at_ms, composite_version, details_json, created_at) "
                "VALUES(?, ?, ?, ?, 'SYSTEM', ?, ?, ?, ?)",
                (
                    route,
                    current_breaker.state,
                    decision.state,
                    decision.reason,
                    int(stats["window"]["end_ms"]),
                    next_composite_version,
                    canonical_json(transition_details),
                    utc_now(),
                ),
            )

        return {
            "route": route,
            "composite_version": next_composite_version,
            "window_version": window_version,
            "window_version_id": window_version_id,
            "window_start_ms": int(stats["window"]["start_ms"]),
            "window_end_ms": int(stats["window"]["end_ms"]),
            "breaker_version": next_breaker_version,
            "breaker_version_id": breaker_version_id,
            "breaker_state": decision.state,
            "breaker_changed": decision.changed,
            "breaker_reason": decision.reason,
            "quota_version": next_quota_version,
            "quota_version_id": quota_version_id,
            "quota_status": quota_status,
            "business_code": business_code,
            "final_quota": final_quota,
            "sample_count": int(stats["samples"]["sample_count"]),
            "success_rate": stats["success_rate"]["value"],
            "p95_us": (
                stats["quantiles"][stats["quantiles"]["primary_method"]]["P95"]["value_us"]
            ),
            "max_method_deviation_us": stats["method_comparison"]["max_abs_deviation_us"],
            "committed_at": committed_at,
        }

    def _propagation_views(
        self,
        connection: sqlite3.Connection,
        route: str,
        *,
        current: Mapping[str, Any],
        current_window_version: int,
        current_window_version_id: int,
    ) -> list[dict[str, Any]]:
        def view(stats: Mapping[str, Any], version: int, version_id: int) -> dict[str, Any] | None:
            p95 = primary_quantile_us(stats, "P95")
            if p95 is None:
                return None
            item = dict(stats)
            item["window_version"] = version
            item["window_version_id"] = version_id
            low = Decimal(stats["success_rate"]["confidence_95"]["low"])
            high = Decimal(stats["success_rate"]["confidence_95"]["high"])
            item["error_contract"] = {
                "success_rate_ci_half_width": str((high - low) / Decimal(2)),
                "method_spread_us": stats["method_comparison"]["max_abs_deviation_us"],
            }
            return item

        current_view = view(current, current_window_version, current_window_version_id)
        if current_view is None:
            raise ValueError("current window P95 has insufficient samples")
        result = [current_view]
        rows = fetch_all_dicts(
            connection,
            "SELECT wv.* FROM window_heads wh JOIN window_versions wv ON wv.id=wh.current_version_id "
            "WHERE wh.route=? AND NOT (wh.start_ms=?) AND wv.end_ms<=? "
            "ORDER BY wv.end_ms DESC, wv.start_ms DESC",
            (route, int(current["window"]["start_ms"]), int(current["window"]["end_ms"])),
        )
        for row in rows:
            stats = json.loads(row["stats_json"])
            item = view(stats, int(row["version"]), int(row["id"]))
            if item is not None:
                result.append(item)
            if len(result) >= len(self.config.propagation_weights):
                break
        return result

    def _build_batch_result(
        self,
        connection: sqlite3.Connection,
        batch: NormalizedBatch,
        changed: Sequence[Mapping[str, Any]],
        *,
        duplicate: bool,
    ) -> dict[str, Any]:
        latest_by_route: dict[str, dict[str, Any]] = {}
        for item in changed:
            latest_by_route[str(item["route"])] = dict(item)
        routes: dict[str, Any] = {}
        for route, count in sorted(batch.route_summary.items()):
            composite = self._load_composite_by_head(connection, route)
            if composite is None:
                routes[route] = {
                    "status": "AWAITING_CLOSED_WINDOW",
                    "ingested_events": count,
                    "message": "No [start,end) window has closed at the route watermark.",
                }
            else:
                summary = self._composite_summary(composite)
                summary["ingested_events"] = count
                routes[route] = summary
        return {
            "ok": True,
            "duplicate": duplicate,
            "batch": {
                "batch_id": batch.batch_id,
                "sequence": batch.sequence,
                "content_hash": batch.content_hash,
                "event_count": len(batch.events),
                "route_summary": batch.route_summary,
                "status": "COMMITTED",
            },
            "routes": routes,
            "consistency_contract": (
                "Every route result is committed under one composite version referencing the same "
                "window, breaker, and quota versions."
            ),
        }

    # ------------------------------------------------------------------
    # Read-side composite access
    # ------------------------------------------------------------------

    def _load_composite_by_head(
        self, connection: sqlite3.Connection, route: str
    ) -> dict[str, Any] | None:
        row = connection.execute(
            "SELECT cs.route, cs.version AS composite_version, cs.committed_at, "
            "wv.id AS window_version_id, wv.version AS window_version, wv.start_ms, wv.end_ms, "
            "wv.sample_count, wv.success_count, wv.failure_count, wv.stats_json, "
            "bv.id AS breaker_version_id, bv.version AS breaker_version, bv.state AS breaker_state, "
            "bv.opened_at_ms, bv.next_probe_at_ms, bv.half_open_since_ms, bv.last_reason, "
            "bv.details_json AS breaker_details_json, "
            "qv.id AS quota_version_id, qv.version AS quota_version, qv.previous_quota, "
            "qv.final_quota, qv.details_json AS quota_details_json "
            "FROM composite_heads ch "
            "JOIN composite_snapshots cs ON cs.route=ch.route AND cs.version=ch.current_version "
            "JOIN window_versions wv ON wv.id=cs.window_version_id "
            "JOIN breaker_versions bv ON bv.id=cs.breaker_version_id "
            "JOIN quota_versions qv ON qv.id=cs.quota_version_id "
            "WHERE ch.route=?",
            (route,),
        ).fetchone()
        if row is None:
            return None
        data = row_dict(row)
        assert data is not None
        data["stats"] = json.loads(data.pop("stats_json"))
        data["breaker_details"] = json.loads(data.pop("breaker_details_json"))
        data["quota_details"] = json.loads(data.pop("quota_details_json"))
        return data

    def _composite_summary(self, composite: Mapping[str, Any]) -> dict[str, Any]:
        stats = composite["stats"]
        primary = stats["quantiles"]["primary_method"]
        return {
            "status": "COMMITTED",
            "composite_version": composite["composite_version"],
            "committed_at": composite["committed_at"],
            "window": {
                "start_ms": composite["start_ms"],
                "end_ms": composite["end_ms"],
                "version": composite["window_version"],
                "version_id": composite["window_version_id"],
                "sample_count": composite["sample_count"],
                "success_rate": stats["success_rate"]["value"],
                "p95_us": stats["quantiles"][primary]["P95"]["value_us"],
                "max_method_deviation_us": stats["method_comparison"]["max_abs_deviation_us"],
            },
            "breaker": {
                "state": composite["breaker_state"],
                "version": composite["breaker_version"],
                "version_id": composite["breaker_version_id"],
                "opened_at_ms": composite["opened_at_ms"],
                "next_probe_at_ms": composite["next_probe_at_ms"],
                "reason": composite["last_reason"],
            },
            "quota": {
                "version": composite["quota_version"],
                "version_id": composite["quota_version_id"],
                "final_quota": composite["final_quota"],
                "previous_quota": composite["previous_quota"],
                "status": composite["quota_details"].get("status", "OK"),
                "business_code": composite["quota_details"].get("business_code"),
                "reason": composite["quota_details"].get("reason"),
            },
            "consistency": {
                "same_window_version_in_limiter_and_breaker": True,
                "window_version": composite["window_version"],
                "breaker_window_version_id": composite["window_version_id"],
                "quota_window_version_id": composite["window_version_id"],
            },
        }

    # ------------------------------------------------------------------
    # Admission decision
    # ------------------------------------------------------------------

    def decide(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        route = payload.get("route")
        if not isinstance(route, str) or not route:
            raise validation_error("route must be a non-empty string")
        current_concurrency = _require_int(
            payload.get("current_concurrency", 0), "current_concurrency", 0
        )
        target_latency_us = _require_int(
            payload.get("target_latency_us", self.config.target_latency_us),
            "target_latency_us",
            1,
        )
        try:
            budget = Decimal(str(payload.get("budget_watermark", 1)))
        except (InvalidOperation, ValueError) as exc:
            raise validation_error("budget_watermark must be a decimal") from exc
        if not budget.is_finite() or budget < 0 or budget > 1:
            raise validation_error("budget_watermark must be within 0..1")
        as_of_ms = payload.get("as_of_ms")
        as_of = None if as_of_ms is None else _require_int(as_of_ms, "as_of_ms", 0)
        requested_start = payload.get("window_start_ms")
        requested_window = payload.get("window_version")
        requested_composite = payload.get("composite_version")

        with self._lock:
            intent_id = self.storage.prepared_intent(
                "DECISION",
                route,
                {
                    "route": route,
                    "current_concurrency": current_concurrency,
                    "budget_watermark": str(budget),
                    "target_latency_us": target_latency_us,
                    "as_of_ms": as_of,
                },
            )
            try:
                self._fault("decision_after_intent", {"intent_id": intent_id, "route": route})
                with self.storage.transaction() as connection:
                    composite = self._load_composite_by_head(connection, route)
                    if composite is None:
                        raise BusinessError(
                            "E_STALE_WINDOW",
                            "route has no committed closed-window composite",
                            422,
                            {"route": route, "reason": "NO_COMMITTED_WINDOW"},
                        )
                    if requested_start is not None and int(requested_start) != int(composite["start_ms"]):
                        raise BusinessError(
                            "E_STALE_WINDOW",
                            "requested window_start_ms is not the current committed window",
                            409,
                            {
                                "requested_window_start_ms": int(requested_start),
                                "current_window_start_ms": composite["start_ms"],
                                "current_window_version": composite["window_version"],
                            },
                        )
                    if requested_window is not None and int(requested_window) != int(
                        composite["window_version"]
                    ):
                        raise BusinessError(
                            "E_STALE_WINDOW",
                            "requested window version is not current",
                            409,
                            {
                                "requested_window_version": int(requested_window),
                                "current_window_version": composite["window_version"],
                            },
                        )
                    if requested_composite is not None and int(requested_composite) != int(
                        composite["composite_version"]
                    ):
                        raise BusinessError(
                            "E_STALE_WINDOW",
                            "requested composite version is not current",
                            409,
                            {
                                "requested_composite_version": int(requested_composite),
                                "current_composite_version": composite["composite_version"],
                            },
                        )
                    evaluation_time = int(composite["end_ms"]) if as_of is None else as_of
                    if evaluation_time > int(composite["end_ms"]) + self.config.stale_grace_ms:
                        raise BusinessError(
                            "E_STALE_WINDOW",
                            "the current window is older than the configured stale grace",
                            422,
                            {
                                "as_of_ms": evaluation_time,
                                "window_end_ms": composite["end_ms"],
                                "stale_grace_ms": self.config.stale_grace_ms,
                            },
                        )

                    connection.execute(
                        "UPDATE route_runtime SET current_concurrency=?, budget_watermark=?, "
                        "target_latency_us=?, updated_at=? WHERE route=?",
                        (
                            current_concurrency,
                            str(budget),
                            target_latency_us,
                            utc_now(),
                            route,
                        ),
                    )

                    breaker_changed = False
                    breaker_transition = None
                    if (
                        composite["breaker_state"] == "OPEN"
                        and composite["next_probe_at_ms"] is not None
                        and evaluation_time >= int(composite["next_probe_at_ms"])
                    ):
                        state = BreakerState(
                            state=composite["breaker_state"],
                            version=int(composite["breaker_version"]),
                            opened_at_ms=composite["opened_at_ms"],
                            next_probe_at_ms=composite["next_probe_at_ms"],
                            half_open_since_ms=composite["half_open_since_ms"],
                            last_reason=composite["last_reason"],
                        )
                        decision = manual_transition(
                            state,
                            "HALF_OPEN",
                            evaluation_time,
                            "COOLDOWN_ELAPSED",
                            self.config,
                        )
                        breaker_changed = True
                        breaker_transition = decision
                        new_breaker_id = self._insert_breaker_version(
                            connection,
                            route=route,
                            window_version_id=int(composite["window_version_id"]),
                            previous_version=int(composite["breaker_version"]),
                            decision=decision,
                            source="SYSTEM",
                        )
                    else:
                        new_breaker_id = int(composite["breaker_version_id"])

                    quota_details = self._recompute_quota_for_current(
                        connection,
                        route=route,
                        composite=composite,
                        breaker_version_id=new_breaker_id,
                        breaker_state=(
                            breaker_transition.state
                            if breaker_transition is not None
                            else composite["breaker_state"]
                        ),
                        current_concurrency=current_concurrency,
                        budget=budget,
                        target_latency_us=target_latency_us,
                    )
                    if quota_details.get("status") == "INSUFFICIENT_SAMPLES":
                        raise BusinessError(
                            "E_INSUFFICIENT_SAMPLES",
                            "the current window does not have enough samples for P95 admission",
                            422,
                            {
                                "route": route,
                                "sample_count": composite["sample_count"],
                                "required_samples": self.config.min_samples_p95,
                                "window_version": composite["window_version"],
                            },
                        )
                    composite_version = self._insert_composite(
                        connection,
                        route=route,
                        window_version_id=int(composite["window_version_id"]),
                        quota_version_id=int(quota_details["_quota_version_id"]),
                        breaker_version_id=new_breaker_id,
                    )
                    quota_details.pop("_quota_version_id", None)
                    if breaker_transition is not None:
                        self._insert_transition(
                            connection,
                            route=route,
                            from_state=breaker_transition.details["from_state"],
                            to_state=breaker_transition.state,
                            reason=breaker_transition.reason,
                            source="SYSTEM",
                            occurred_at_ms=evaluation_time,
                            composite_version=composite_version,
                            details=breaker_transition.details,
                        )

                    current = self._load_composite_by_head(connection, route)
                    assert current is not None
                    allowed, decision_code, admission_reason = self._admission_result(
                        current=current,
                        quota_details=quota_details,
                        current_concurrency=current_concurrency,
                    )
                    response = {
                        "ok": True,
                        "route": route,
                        "as_of_ms": evaluation_time,
                        "decision": {
                            "allowed": allowed,
                            "code": decision_code,
                            "reason": admission_reason,
                            "current_concurrency": current_concurrency,
                            "current_limit": int(quota_details["final_quota"]),
                            "headroom": max(0, int(quota_details["final_quota"]) - current_concurrency),
                        },
                        "composite_version": current["composite_version"],
                        "window": {
                            "version": current["window_version"],
                            "version_id": current["window_version_id"],
                            "start_ms": current["start_ms"],
                            "end_ms": current["end_ms"],
                            "sample_count": current["sample_count"],
                            "success_rate": current["stats"]["success_rate"]["value"],
                            "p95_us": current["stats"]["quantiles"][
                                current["stats"]["quantiles"]["primary_method"]
                            ]["P95"]["value_us"],
                            "max_method_deviation_us": current["stats"]["method_comparison"][
                                "max_abs_deviation_us"
                            ],
                        },
                        "breaker": {
                            "version": current["breaker_version"],
                            "version_id": current["breaker_version_id"],
                            "state": current["breaker_state"],
                            "opened_at_ms": current["opened_at_ms"],
                            "next_probe_at_ms": current["next_probe_at_ms"],
                            "recorded_transition": breaker_changed,
                        },
                        "quota": {
                            "version": current["quota_version"],
                            "version_id": current["quota_version_id"],
                            "previous_quota": quota_details.get("previous_quota"),
                            "target_before_damping": quota_details.get("target_before_damping"),
                            "final_quota": quota_details["final_quota"],
                            "status": quota_details.get("status"),
                            "reason": quota_details.get("reason"),
                            "error_upper_bound_units": quota_details.get("error_propagation", {}).get(
                                "total_error_upper_bound_units"
                            ),
                            "derivation": quota_details,
                        },
                        "consistency": {
                            "same_window_version_in_limiter_and_breaker": True,
                            "limiter_window_version": current["window_version"],
                            "breaker_window_version": current["window_version"],
                            "window_version_id": current["window_version_id"],
                            "numeric_explanation": (
                                "Both paths read one immutable composite snapshot. Interpolation "
                                "spread and confidence bounds are attached to its statistics; any "
                                "version change requires a new composite version."
                            ),
                        },
                    }
                    connection.execute(
                        "INSERT INTO decision_logs(route, request_json, response_json, composite_version, "
                        "created_at) VALUES(?, ?, ?, ?, ?)",
                        (
                            route,
                            canonical_json(payload),
                            canonical_json(response),
                            composite_version,
                            utc_now(),
                        ),
                    )
                    self.storage.complete_intent(connection, intent_id)
                return response
            except Exception:
                self.storage.abort_intent(intent_id)
                raise

    def _recompute_quota_for_current(
        self,
        connection: sqlite3.Connection,
        *,
        route: str,
        composite: Mapping[str, Any],
        breaker_version_id: int,
        breaker_state: str,
        current_concurrency: int,
        budget: Decimal,
        target_latency_us: int,
    ) -> dict[str, Any]:
        stats = composite["stats"]
        try:
            views = self._propagation_views(
                connection,
                route,
                current=stats,
                current_window_version=int(composite["window_version"]),
                current_window_version_id=int(composite["window_version_id"]),
            )
            propagation = propagate_metrics(views, self.config.propagation_weights)
            quota_details = derive_quota(
                propagation=propagation,
                breaker_state=breaker_state,
                current_concurrency=current_concurrency,
                previous_quota=(
                    None if composite["previous_quota"] is None else int(composite["final_quota"])
                ),
                budget_watermark=budget,
                target_latency_us=target_latency_us,
                config=self.config,
            )
            quota_details["status"] = "OK"
            quota_details["business_code"] = None
        except ValueError as exc:
            if primary_quantile_us(stats, "P95") is not None:
                raise
            quota_details = {
                "status": "INSUFFICIENT_SAMPLES",
                "business_code": "E_INSUFFICIENT_SAMPLES",
                "reason": "P95_INSUFFICIENT_SAMPLES",
                "required_samples": self.config.min_samples_p95,
                "sample_count": int(stats["samples"]["sample_count"]),
                "final_quota": self.config.min_quota,
                "previous_quota": composite["final_quota"],
                "breaker_state": breaker_state,
                "message": str(exc),
                "window": {
                    "start_ms": stats["window"]["start_ms"],
                    "end_ms": stats["window"]["end_ms"],
                    "window_version": composite["window_version"],
                },
                "method_comparison": stats["method_comparison"],
                "error_propagation": stats["error_propagation"],
                "steps": [],
            }
        head = connection.execute(
            "SELECT current_version_id FROM quota_heads WHERE route=?", (route,)
        ).fetchone()
        previous_version = 0
        if head is not None:
            previous_version = int(
                connection.execute(
                    "SELECT version FROM quota_versions WHERE id=?",
                    (head["current_version_id"],),
                ).fetchone()["version"]
            )
        next_version = previous_version + 1
        cursor = connection.execute(
            "INSERT INTO quota_versions(route, version, window_version_id, breaker_version_id, "
            "previous_quota, final_quota, details_json, created_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
            (
                route,
                next_version,
                int(composite["window_version_id"]),
                breaker_version_id,
                int(composite["final_quota"]),
                int(quota_details["final_quota"]),
                canonical_json(quota_details),
                utc_now(),
            ),
        )
        quota_version_id = int(cursor.lastrowid)
        connection.execute(
            "INSERT INTO quota_heads(route, current_version_id, final_quota, window_version_id) "
            "VALUES(?, ?, ?, ?) ON CONFLICT(route) DO UPDATE SET "
            "current_version_id=excluded.current_version_id, final_quota=excluded.final_quota, "
            "window_version_id=excluded.window_version_id",
            (
                route,
                quota_version_id,
                int(quota_details["final_quota"]),
                int(composite["window_version_id"]),
            ),
        )
        quota_details["quota_version"] = next_version
        quota_details["_quota_version_id"] = quota_version_id
        return quota_details

    def _insert_breaker_version(
        self,
        connection: sqlite3.Connection,
        *,
        route: str,
        window_version_id: int,
        previous_version: int,
        decision: Any,
        source: str,
    ) -> int:
        next_version = previous_version + 1
        details = dict(decision.details)
        details.update(
            {
                "from_version": previous_version,
                "to_version": next_version,
                "window_version_id": window_version_id,
                "source": source,
            }
        )
        cursor = connection.execute(
            "INSERT INTO breaker_versions(route, version, window_version_id, state, opened_at_ms, "
            "next_probe_at_ms, half_open_since_ms, last_reason, details_json, created_at) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                route,
                next_version,
                window_version_id,
                decision.state,
                decision.opened_at_ms,
                decision.next_probe_at_ms,
                decision.half_open_since_ms,
                decision.reason,
                canonical_json(details),
                utc_now(),
            ),
        )
        breaker_version_id = int(cursor.lastrowid)
        connection.execute(
            "INSERT INTO breaker_heads(route, current_version_id, state) VALUES(?, ?, ?) "
            "ON CONFLICT(route) DO UPDATE SET current_version_id=excluded.current_version_id, "
            "state=excluded.state",
            (route, breaker_version_id, decision.state),
        )
        return breaker_version_id

    def _insert_composite(
        self,
        connection: sqlite3.Connection,
        *,
        route: str,
        window_version_id: int,
        quota_version_id: int,
        breaker_version_id: int,
    ) -> int:
        head = connection.execute(
            "SELECT current_version FROM composite_heads WHERE route=?", (route,)
        ).fetchone()
        next_version = 1 if head is None else int(head["current_version"]) + 1
        connection.execute(
            "INSERT INTO composite_snapshots(route, version, window_version_id, quota_version_id, "
            "breaker_version_id, committed_at) VALUES(?, ?, ?, ?, ?, ?)",
            (
                route,
                next_version,
                window_version_id,
                quota_version_id,
                breaker_version_id,
                utc_now(),
            ),
        )
        connection.execute(
            "INSERT INTO composite_heads(route, current_version, window_version_id, "
            "quota_version_id, breaker_version_id) VALUES(?, ?, ?, ?, ?) "
            "ON CONFLICT(route) DO UPDATE SET current_version=excluded.current_version, "
            "window_version_id=excluded.window_version_id, quota_version_id=excluded.quota_version_id, "
            "breaker_version_id=excluded.breaker_version_id",
            (route, next_version, window_version_id, quota_version_id, breaker_version_id),
        )
        connection.execute(
            "UPDATE routes SET current_composite_version=? WHERE route=?",
            (next_version, route),
        )
        return next_version

    def _insert_transition(
        self,
        connection: sqlite3.Connection,
        *,
        route: str,
        from_state: str,
        to_state: str,
        reason: str,
        source: str,
        occurred_at_ms: int,
        composite_version: int,
        details: Mapping[str, Any],
    ) -> None:
        payload = dict(details)
        payload["composite_version"] = composite_version
        payload["source"] = source
        connection.execute(
            "INSERT INTO breaker_transitions(route, from_state, to_state, reason, source, "
            "occurred_at_ms, composite_version, details_json, created_at) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                route,
                from_state,
                to_state,
                reason,
                source,
                occurred_at_ms,
                composite_version,
                canonical_json(payload),
                utc_now(),
            ),
        )

    def _admission_result(
        self,
        *,
        current: Mapping[str, Any],
        quota_details: Mapping[str, Any],
        current_concurrency: int,
    ) -> tuple[bool, str, str]:
        state = current["breaker_state"]
        limit = int(quota_details["final_quota"])
        if state == "OPEN":
            return False, "DENIED_BY_BREAKER", "breaker is OPEN; quota is advisory until recovery"
        if limit <= current_concurrency:
            return False, "DENIED_BY_QUOTA", "current_concurrency is at or above the adaptive limit"
        if state == "HALF_OPEN":
            return True, "ALLOWED_HALF_OPEN_PROBE", "inside the derated half-open probe allowance"
        return True, "ALLOWED", "inside the adaptive limit"

    # ------------------------------------------------------------------
    # Manual state transitions and queries
    # ------------------------------------------------------------------

    def manual_breaker_transition(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        route = payload.get("route")
        if not isinstance(route, str) or not route:
            raise validation_error("route must be a non-empty string")
        to_state = payload.get("to_state")
        if not isinstance(to_state, str):
            raise validation_error("to_state must be a string")
        occurred_at_ms = _require_int(payload.get("occurred_at_ms"), "occurred_at_ms", 0)
        reason = payload.get("reason", "MANUAL_TRANSITION")
        if not isinstance(reason, str) or not reason:
            raise validation_error("reason must be a non-empty string")

        with self._lock:
            intent_id = self.storage.prepared_intent(
                "TRANSITION",
                route,
                {
                    "route": route,
                    "to_state": to_state,
                    "occurred_at_ms": occurred_at_ms,
                    "reason": reason,
                },
            )
            try:
                self._fault(
                    "transition_after_intent",
                    {"intent_id": intent_id, "route": route, "to_state": to_state},
                )
                with self.storage.transaction() as connection:
                    composite = self._load_composite_by_head(connection, route)
                    if composite is None:
                        raise BusinessError(
                            "E_STALE_WINDOW",
                            "route has no committed composite to attach a breaker version to",
                            422,
                            {"route": route},
                        )
                    state = BreakerState(
                        state=composite["breaker_state"],
                        version=int(composite["breaker_version"]),
                        opened_at_ms=composite["opened_at_ms"],
                        next_probe_at_ms=composite["next_probe_at_ms"],
                        half_open_since_ms=composite["half_open_since_ms"],
                        last_reason=composite["last_reason"],
                    )
                    decision = manual_transition(
                        state, to_state, occurred_at_ms, reason, self.config
                    )
                    if not decision.changed:
                        response = {
                            "ok": True,
                            "changed": False,
                            "route": route,
                            "state": composite["breaker_state"],
                            "breaker_version": composite["breaker_version"],
                            "composite_version": composite["composite_version"],
                            "reason": decision.reason,
                            "message": "same-state transition is idempotent and persists no new version",
                        }
                        self.storage.complete_intent(connection, intent_id)
                        return response

                    breaker_version_id = self._insert_breaker_version(
                        connection,
                        route=route,
                        window_version_id=int(composite["window_version_id"]),
                        previous_version=int(composite["breaker_version"]),
                        decision=decision,
                        source="MANUAL",
                    )
                    quota_details = self._recompute_quota_for_current(
                        connection,
                        route=route,
                        composite=composite,
                        breaker_version_id=breaker_version_id,
                        breaker_state=decision.state,
                        current_concurrency=int(
                            connection.execute(
                                "SELECT current_concurrency FROM route_runtime WHERE route=?",
                                (route,),
                            ).fetchone()["current_concurrency"]
                        ),
                        budget=Decimal(
                            connection.execute(
                                "SELECT budget_watermark FROM route_runtime WHERE route=?",
                                (route,),
                            ).fetchone()["budget_watermark"]
                        ),
                        target_latency_us=int(
                            connection.execute(
                                "SELECT target_latency_us FROM route_runtime WHERE route=?",
                                (route,),
                            ).fetchone()["target_latency_us"]
                        ),
                    )
                    composite_version = self._insert_composite(
                        connection,
                        route=route,
                        window_version_id=int(composite["window_version_id"]),
                        quota_version_id=int(quota_details["_quota_version_id"]),
                        breaker_version_id=breaker_version_id,
                    )
                    self._insert_transition(
                        connection,
                        route=route,
                        from_state=state.state,
                        to_state=decision.state,
                        reason=decision.reason,
                        source="MANUAL",
                        occurred_at_ms=occurred_at_ms,
                        composite_version=composite_version,
                        details=decision.details,
                    )
                    self.storage.complete_intent(connection, intent_id)
                return {
                    "ok": True,
                    "changed": True,
                    "route": route,
                    "from_state": state.state,
                    "state": decision.state,
                    "reason": decision.reason,
                    "breaker_version": decision.details.get("to_version"),
                    "composite_version": composite_version,
                    "quota_version": quota_details["quota_version"],
                    "quota": quota_details["final_quota"],
                    "occurred_at_ms": occurred_at_ms,
                }
            except Exception:
                self.storage.abort_intent(intent_id)
                raise

    def get_circuit_state(self, route: str, as_of_ms: int | None = None) -> dict[str, Any]:
        with self.storage.read_connection() as connection:
            composite = self._load_composite_by_head(connection, route)
        if composite is None:
            raise BusinessError(
                "E_ROUTE_NOT_FOUND",
                "route has no committed composite",
                404,
                {"route": route},
            )
        evaluation = int(composite["end_ms"]) if as_of_ms is None else as_of_ms
        cooldown_elapsed = (
            composite["breaker_state"] == "OPEN"
            and composite["next_probe_at_ms"] is not None
            and evaluation >= int(composite["next_probe_at_ms"])
        )
        effective = "HALF_OPEN" if cooldown_elapsed else composite["breaker_state"]
        return {
            "ok": True,
            "route": route,
            "composite_version": composite["composite_version"],
            "window_version": composite["window_version"],
            "window_version_id": composite["window_version_id"],
            "window_start_ms": composite["start_ms"],
            "window_end_ms": composite["end_ms"],
            "state": composite["breaker_state"],
            "effective_state_on_next_evaluation": effective,
            "breaker_version": composite["breaker_version"],
            "opened_at_ms": composite["opened_at_ms"],
            "next_probe_at_ms": composite["next_probe_at_ms"],
            "half_open_since_ms": composite["half_open_since_ms"],
            "cooldown_elapsed": cooldown_elapsed,
            "last_reason": composite["last_reason"],
            "quota": {
                "version": composite["quota_version"],
                "final_quota": composite["final_quota"],
                "status": composite["quota_details"].get("status", "OK"),
            },
            "consistency": {
                "same_window_version_in_limiter_and_breaker": True,
                "breaker_window_version_id": composite["window_version_id"],
                "quota_window_version_id": composite["window_version_id"],
            },
        }

    def get_window_stats(
        self,
        route: str,
        *,
        window_start_ms: int | None = None,
        version: int | None = None,
        strict: str | bool | None = None,
    ) -> dict[str, Any]:
        with self.storage.read_connection() as connection:
            if window_start_ms is None:
                composite = self._load_composite_by_head(connection, route)
                if composite is None:
                    raise BusinessError(
                        "E_STALE_WINDOW",
                        "route has no committed closed window",
                        422,
                        {"route": route},
                    )
                stats = composite["stats"]
                window_version = composite["window_version"]
                window_version_id = composite["window_version_id"]
                start_ms = composite["start_ms"]
                end_ms = composite["end_ms"]
                composite_version = composite["composite_version"]
            else:
                row = connection.execute(
                    "SELECT wv.* FROM window_heads wh JOIN window_versions wv "
                    "ON wv.id=wh.current_version_id WHERE wh.route=? AND wh.start_ms=?",
                    (route, window_start_ms),
                ).fetchone()
                if row is None:
                    raise BusinessError(
                        "E_STALE_WINDOW",
                        "requested window is not the current committed version",
                        409,
                        {"route": route, "window_start_ms": window_start_ms},
                    )
                if version is not None and int(version) != int(row["version"]):
                    raise BusinessError(
                        "E_STALE_WINDOW",
                        "requested window version is no longer current",
                        409,
                        {
                            "requested_version": int(version),
                            "current_version": int(row["version"]),
                        },
                    )
                stats = json.loads(row["stats_json"])
                window_version = int(row["version"])
                window_version_id = int(row["id"])
                start_ms = int(row["start_ms"])
                end_ms = int(row["end_ms"])
                head = connection.execute(
                    "SELECT current_version FROM composite_heads WHERE route=?", (route,)
                ).fetchone()
                composite_version = int(head["current_version"]) if head else None

        if strict not in (None, False, "false", "0"):
            levels = ["P50", "P95", "P99"] if strict in (True, "true", "1", "all") else [str(strict).upper()]
            insufficient = []
            for level in levels:
                if level not in ("P50", "P95", "P99"):
                    raise validation_error("strict must be true, all, P50, P95, or P99")
                if not stats["quantiles"][stats["quantiles"]["primary_method"]][level]["sufficient"]:
                    insufficient.append(
                        {
                            "quantile": level,
                            "required_samples": stats["quantiles"][
                                stats["quantiles"]["primary_method"]
                            ][level]["required_samples"],
                        }
                    )
            if insufficient:
                raise BusinessError(
                    "E_INSUFFICIENT_SAMPLES",
                    "one or more requested quantiles do not meet the declared sample floor",
                    422,
                    {
                        "route": route,
                        "window_start_ms": start_ms,
                        "sample_count": stats["samples"]["sample_count"],
                        "insufficient": insufficient,
                    },
                )
        return {
            "ok": True,
            "route": route,
            "window_start_ms": start_ms,
            "window_end_ms": end_ms,
            "window_version": window_version,
            "window_version_id": window_version_id,
            "composite_version": composite_version,
            "stats": stats,
        }

    def list_windows(self, route: str, limit: int = 20) -> dict[str, Any]:
        if limit < 1 or limit > 500:
            raise validation_error("limit must be within 1..500")
        with self.storage.read_connection() as connection:
            rows = fetch_all_dicts(
                connection,
                "SELECT wv.* FROM window_heads wh JOIN window_versions wv "
                "ON wv.id=wh.current_version_id WHERE wh.route=? "
                "ORDER BY wv.end_ms DESC, wv.start_ms DESC LIMIT ?",
                (route, limit),
            )
        if not rows:
            raise BusinessError(
                "E_ROUTE_NOT_FOUND", "route has no committed windows", 404, {"route": route}
            )
        windows = []
        for row in rows:
            stats = json.loads(row["stats_json"])
            primary = stats["quantiles"]["primary_method"]
            windows.append(
                {
                    "window_version": row["version"],
                    "window_version_id": row["id"],
                    "start_ms": row["start_ms"],
                    "end_ms": row["end_ms"],
                    "sample_count": row["sample_count"],
                    "success_count": row["success_count"],
                    "success_rate": stats["success_rate"]["value"],
                    "p50_nearest_rank_us": stats["quantiles"]["nearest_rank"]["P50"]["value_us"],
                    "p50_linear_us": stats["quantiles"]["linear"]["P50"]["value_us"],
                    "p95_nearest_rank_us": stats["quantiles"]["nearest_rank"]["P95"]["value_us"],
                    "p95_linear_us": stats["quantiles"]["linear"]["P95"]["value_us"],
                    "p99_nearest_rank_us": stats["quantiles"]["nearest_rank"]["P99"]["value_us"],
                    "p99_linear_us": stats["quantiles"]["linear"]["P99"]["value_us"],
                    "max_method_deviation_us": stats["method_comparison"]["max_abs_deviation_us"],
                    "p95_sufficient": stats["quantiles"][primary]["P95"]["sufficient"],
                    "p99_sufficient": stats["quantiles"][primary]["P99"]["sufficient"],
                    "error_propagation": stats["error_propagation"],
                }
            )
        return {"ok": True, "route": route, "count": len(windows), "windows": windows}

    def get_quota_derivation(
        self,
        route: str,
        *,
        composite_version: int | None = None,
    ) -> dict[str, Any]:
        with self.storage.read_connection() as connection:
            composite = self._load_composite_by_head(connection, route)
        if composite is None:
            raise BusinessError("E_ROUTE_NOT_FOUND", "route not found", 404, {"route": route})
        if composite_version is not None and int(composite_version) != int(
            composite["composite_version"]
        ):
            raise BusinessError(
                "E_STALE_WINDOW",
                "requested composite version is not current",
                409,
                {
                    "requested_composite_version": int(composite_version),
                    "current_composite_version": composite["composite_version"],
                },
            )
        return {
            "ok": True,
            "route": route,
            "composite_version": composite["composite_version"],
            "window_version": composite["window_version"],
            "window_version_id": composite["window_version_id"],
            "breaker_version": composite["breaker_version"],
            "breaker_state": composite["breaker_state"],
            "quota_version": composite["quota_version"],
            "final_quota": composite["final_quota"],
            "status": composite["quota_details"].get("status", "OK"),
            "derivation": composite["quota_details"],
            "consistency": {
                "same_window_version_in_limiter_and_breaker": True,
                "breaker_window_version_id": composite["window_version_id"],
                "quota_window_version_id": composite["window_version_id"],
            },
        }

    def get_batch(self, batch_id: str, sequence: int) -> dict[str, Any]:
        with self.storage.read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM batches WHERE batch_id=? AND sequence=?",
                (batch_id, sequence),
            ).fetchone()
        if row is None:
            raise BusinessError(
                "E_BATCH_NOT_FOUND",
                "batch was not found",
                404,
                {"batch_id": batch_id, "sequence": sequence},
            )
        result = json.loads(row["result_json"]) if row["result_json"] else None
        return {
            "ok": True,
            "batch": {
                "batch_id": row["batch_id"],
                "sequence": row["sequence"],
                "content_hash": row["content_hash"],
                "status": row["status"],
                "event_count": row["event_count"],
                "route_summary": json.loads(row["route_summary_json"]),
                "abort_reason": row["abort_reason"],
                "created_at": row["created_at"],
                "completed_at": row["completed_at"],
            },
            "committed_result": result,
        }

    def list_transitions(self, route: str, limit: int = 100) -> dict[str, Any]:
        if limit < 1 or limit > 1000:
            raise validation_error("limit must be within 1..1000")
        with self.storage.read_connection() as connection:
            rows = connection.execute(
                "SELECT * FROM breaker_transitions WHERE route=? ORDER BY id LIMIT ?",
                (route, limit),
            ).fetchall()
        transitions = []
        for row in rows:
            transitions.append(
                {
                    "from_state": row["from_state"],
                    "to_state": row["to_state"],
                    "reason": row["reason"],
                    "source": row["source"],
                    "occurred_at_ms": row["occurred_at_ms"],
                    "composite_version": row["composite_version"],
                    "details": json.loads(row["details_json"]),
                }
            )
        return {"ok": True, "route": route, "count": len(transitions), "transitions": transitions}

    def recovery_report(self, run_id: int | None = None) -> dict[str, Any]:
        report = (
            self.storage.recovery_by_id(run_id)
            if run_id is not None
            else self.storage.latest_recovery_report()
        )
        if report is None:
            raise BusinessError(
                "E_NOT_FOUND", "no recovery run has been recorded", 404, {"run_id": run_id}
            )
        return {"ok": True, "report": report}

    def health(self) -> dict[str, Any]:
        with self.storage.read_connection() as connection:
            row = connection.execute("SELECT COUNT(*) AS n FROM routes").fetchone()
            quick = str(connection.execute("PRAGMA quick_check").fetchone()[0])
        return {
            "ok": True,
            "service": "rateguard",
            "database": self.storage.database,
            "routes": int(row["n"]),
            "sqlite_quick_check": quick,
            "numeric_contract": {
                "arithmetic": "decimal",
                "rounding": self.config.rounding_mode,
                "window_boundary": "[start_ms,end_ms)",
            },
        }

