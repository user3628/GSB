from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator

from .auth import authorize
from .config import Settings
from .errors import (
    E_AUTH_DENIED,
    E_CONFLICTING_REQUEST,
    E_INVALID_INPUT,
    E_INVALID_TRANSITION,
    E_RATE_LIMITED,
    E_SNAPSHOT_INVALID,
    E_TOKEN_EXPIRED,
    DomainError,
)
from .fixedpoint import SCALE, apply_scale
from .models import (
    AuthResult,
    BatchInput,
    BatchResult,
    BucketDelta,
    Decision,
    EventInput,
    canonical_json,
    sha256_text,
)
from .quota import QuotaEngine
from .store import Store
from .windows import WindowSeries, build_window_state

AUTH_OK = "AUTH_OK"


class _ReadWriteGate:
    """Writer-preferring gate: many concurrent readers, one serialized writer."""

    def __init__(self) -> None:
        import threading

        self._condition = threading.Condition()
        self._readers = 0
        self._writer = False
        self._waiting_writers = 0

    @contextmanager
    def read(self) -> Iterator[None]:
        with self._condition:
            while self._writer or self._waiting_writers:
                self._condition.wait()
            self._readers += 1
        try:
            yield
        finally:
            with self._condition:
                self._readers -= 1
                if self._readers == 0:
                    self._condition.notify_all()

    @contextmanager
    def write(self) -> Iterator[None]:
        with self._condition:
            self._waiting_writers += 1
            try:
                while self._writer or self._readers:
                    self._condition.wait()
            finally:
                self._waiting_writers -= 1
            self._writer = True
        try:
            yield
        finally:
            with self._condition:
                self._writer = False
                self._condition.notify_all()


@dataclass(slots=True)
class _NormalizedBatch:
    batch_id: str
    fingerprint: str
    events: list[EventInput]
    primary_events: dict[str, EventInput]
    fingerprints: dict[str, str]
    batch_event_rows: list[tuple[int, str, str]]


class GatewayService:
    """Serialized batch adjudicator with concurrent read-side SQLite snapshots."""

    def __init__(
        self,
        settings: Settings,
        *,
        store: Store | None = None,
        crash_hook: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.settings = settings
        self.store = store or Store(settings)
        self.store.initialize()
        self._gate = _ReadWriteGate()
        self._crash_hook = crash_hook
        self.quota_engine = QuotaEngine(settings)

    def recover(self):
        with self._gate.write():
            return self.store.recover_pending()

    def submit_batch(self, batch: BatchInput) -> BatchResult:
        if len(batch.events) > self.settings.api_max_events:
            raise DomainError(
                E_INVALID_INPUT,
                "batch exceeds configured API event limit",
                {"event_count": len(batch.events), "limit": self.settings.api_max_events},
            )
        normalized = self._normalize_batch(batch)
        wait_started = time.perf_counter()
        with self._gate.write():
            wait_ms = int((time.perf_counter() - wait_started) * 1000)
            result = self._submit_locked(normalized)
            result.writer_wait_ms = wait_ms
            return result

    def _normalize_batch(self, batch: BatchInput) -> _NormalizedBatch:
        seen_sequences: set[int] = set()
        primary_events: dict[str, EventInput] = {}
        fingerprints: dict[str, str] = {}
        batch_event_rows: list[tuple[int, str, str]] = []
        for event in batch.events:
            if event.event_seq in seen_sequences:
                raise DomainError(
                    E_CONFLICTING_REQUEST,
                    "duplicate event_seq within a batch",
                    {"batch_id": batch.batch_id, "event_seq": event.event_seq},
                )
            seen_sequences.add(event.event_seq)
            fingerprint = event.fingerprint()
            previous = primary_events.get(event.request_id)
            if previous is not None and fingerprints[event.request_id] != fingerprint:
                raise DomainError(
                    E_CONFLICTING_REQUEST,
                    "request_id appears with different content in the same batch",
                    {
                        "request_id": event.request_id,
                        "batch_id": batch.batch_id,
                        "first_event_seq": previous.event_seq,
                        "conflicting_event_seq": event.event_seq,
                    },
                )
            if previous is None:
                primary_events[event.request_id] = event
                fingerprints[event.request_id] = fingerprint
            batch_event_rows.append((event.event_seq, event.request_id, fingerprint))

        batch_event_rows.sort(key=lambda item: item[0])
        fingerprint = sha256_text(canonical_json(batch_event_rows))
        return _NormalizedBatch(
            batch_id=batch.batch_id,
            fingerprint=fingerprint,
            events=list(batch.events),
            primary_events=primary_events,
            fingerprints=fingerprints,
            batch_event_rows=batch_event_rows,
        )

    def _submit_locked(self, normalized: _NormalizedBatch) -> BatchResult:
        existing_batch = self.store.get_batch_row(normalized.batch_id)
        if existing_batch is not None:
            if existing_batch["fingerprint"] != normalized.fingerprint:
                raise DomainError(
                    E_CONFLICTING_REQUEST,
                    "batch_id was already used with different content",
                    {
                        "batch_id": normalized.batch_id,
                        "existing_status": existing_batch["status"],
                    },
                )
            if existing_batch["status"] == "COMMITTED":
                return self.store.get_batch_result(normalized.batch_id, replayed=True)
            if existing_batch["status"] == "PENDING":
                self.store.recover_pending()
                raise DomainError(
                    E_INVALID_TRANSITION,
                    "batch was pending after a crash; recovery marked it aborted",
                    {"batch_id": normalized.batch_id},
                )
            raise DomainError(
                E_INVALID_TRANSITION,
                "aborted batch cannot be resubmitted",
                {"batch_id": normalized.batch_id},
            )

        base_version = self.store.validate_snapshot()
        registry = self.store.load_auth_registry(base_version)
        existing_requests = self.store.get_existing_requests(list(normalized.primary_events))

        conflicting: list[dict[str, Any]] = []
        reused_request_ids: set[str] = set()
        new_events: list[EventInput] = []
        for request_id, event in normalized.primary_events.items():
            existing = existing_requests.get(request_id)
            if existing is None:
                new_events.append(event)
            elif existing["event_fingerprint"] != normalized.fingerprints[request_id]:
                conflicting.append(
                    {
                        "request_id": request_id,
                        "existing_fingerprint": existing["event_fingerprint"],
                        "submitted_fingerprint": normalized.fingerprints[request_id],
                    }
                )
            else:
                reused_request_ids.add(request_id)
        if conflicting:
            raise DomainError(
                E_CONFLICTING_REQUEST,
                "one or more request identifiers were previously decided with different content",
                {"conflicts": conflicting[:20], "conflict_count": len(conflicting)},
            )

        target_version = base_version + 1
        intent_id = self.store.create_intent(
            batch_id=normalized.batch_id,
            fingerprint=normalized.fingerprint,
            base_version=base_version,
            target_version=target_version,
            event_count=len(normalized.events),
            payload={
                "batch_id": normalized.batch_id,
                "event_count": len(normalized.events),
                "unique_request_count": len(normalized.primary_events),
                "new_request_count": len(new_events),
                "reused_request_count": len(reused_request_ids),
                "writer_queue_policy": "queue_as_next_batch",
            },
        )

        if self._crash_hook is not None:
            self._crash_hook(
                {
                    "phase": "after_intent_commit",
                    "batch_id": normalized.batch_id,
                    "intent_id": intent_id,
                    "base_version": base_version,
                    "target_version": target_version,
                }
            )

        try:
            computed = self._compute_decisions(
                normalized=normalized,
                base_version=base_version,
                target_version=target_version,
                registry=registry,
                new_events=new_events,
            )
            self.store.persist_commit(
                batch_id=normalized.batch_id,
                intent_id=intent_id,
                base_version=base_version,
                target_version=target_version,
                decisions=computed["decisions"],
                window_events=computed["window_events"],
                bucket_deltas=computed["bucket_deltas"],
                quota_states=computed["quota_states"],
                batch_event_rows=normalized.batch_event_rows,
                summary=computed["summary"],
            )
        except Exception as exc:
            self.store.abort_intent(
                batch_id=normalized.batch_id,
                intent_id=intent_id,
                reason=f"DECISION_FAILED:{type(exc).__name__}:{exc}",
            )
            raise

        # Release the in-memory write buffers before loading the committed view.
        computed["decisions"].clear()
        computed["window_events"].clear()
        return self.store.get_batch_result(normalized.batch_id, replayed=False)

    def _compute_decisions(
        self,
        *,
        normalized: _NormalizedBatch,
        base_version: int,
        target_version: int,
        registry,
        new_events: list[EventInput],
    ) -> dict[str, Any]:
        now_ms = int(time.time() * 1000)
        auth_results: dict[str, AuthResult] = {}
        for event in sorted(new_events, key=lambda item: (item.timestamp_ms, item.event_seq, item.request_id)):
            auth_results[event.request_id] = authorize(
                event,
                registry,
                token_version=base_version,
            )

        admitted_auth_events = [
            event for event in new_events if auth_results[event.request_id].ok
        ]
        existing_rows_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
        if admitted_auth_events:
            min_ts = min(event.timestamp_ms for event in admitted_auth_events)
            max_ts = max(event.timestamp_ms for event in admitted_auth_events)
            keys = sorted({(event.route_key, event.client_id) for event in admitted_auth_events})
            existing_rows_by_key = self.store.load_window_events(
                keys,
                min_timestamp_ms=min_ts - self.settings.long_length_ms,
                max_timestamp_ms=max_ts,
                version=base_version,
            )
        else:
            keys = []

        window_state = build_window_state(
            existing_rows_by_key=existing_rows_by_key,
            new_events=admitted_auth_events,
            settings=self.settings,
            secret=self.settings.local_secret,
        )
        quota_state_rows = self.store.load_quota_states(keys)
        key_scales: dict[tuple[str, str], int] = {}
        key_groups: dict[tuple[str, str], str] = {}
        for key in keys:
            row = quota_state_rows.get(key)
            key_scales[key] = int(row["scale_ppm"]) if row else SCALE
        for event in admitted_auth_events:
            key_groups[(event.route_key, event.client_id)] = auth_results[
                event.request_id
            ].quota_group  # type: ignore[assignment]

        decisions_by_request: dict[str, Decision] = {}
        window_events: list[dict[str, Any]] = []
        bucket_accumulator: dict[tuple[str, str, str, int], dict[str, Any]] = {}

        for event in sorted(
            admitted_auth_events,
            key=lambda item: (item.timestamp_ms, item.event_seq, item.request_id),
        ):
            auth = auth_results[event.request_id]
            key = (event.route_key, event.client_id)
            series = window_state.get(*key)
            short = series.estimate(event.request_id, "short")
            long = series.estimate(event.request_id, "long")
            admitted_short = series.admitted_weight(event.request_id, "short")
            group = registry.quota_groups[auth.quota_group]  # type: ignore[index]
            quota = self.quota_engine.decide(
                group=group,
                previous_scale_ppm=key_scales[key],
                short=short,
                long=long,
                admitted_short_weight=admitted_short,
            )
            key_scales[key] = quota.scale_ppm
            trigger_windows = []
            if short.estimated_weight > quota.short_quota:
                trigger_windows.append("short")
            if long.estimated_weight > quota.long_quota:
                trigger_windows.append("long")
            allowed = not trigger_windows
            trigger_window = "+".join(trigger_windows) if trigger_windows else None
            if allowed:
                series.mark_admitted(event.request_id)
                decision_code = "ALLOW"
                reasons = (AUTH_OK, "ALLOW")
            else:
                decision_code = E_RATE_LIMITED
                reasons = (AUTH_OK, E_RATE_LIMITED, f"TRIGGER:{trigger_window}")

            decisions_by_request[event.request_id] = Decision(
                request_id=event.request_id,
                batch_id=normalized.batch_id,
                event_seq=event.event_seq,
                event_fingerprint=normalized.fingerprints[event.request_id],
                timestamp_ms=event.timestamp_ms,
                route_key=event.route_key,
                client_id=event.client_id,
                quota_group=auth.quota_group,
                weight=event.weight,
                auth_ok=True,
                auth_code=AUTH_OK,
                auth_reason=auth.reason,
                allowed=allowed,
                decision_code=decision_code,
                decision_reasons=reasons,
                rate_evaluated=True,
                trigger_window=trigger_window,
                short=short,
                long=long,
                short_quota=quota.short_quota,
                long_quota=quota.long_quota,
                quota_scale_ppm=quota.scale_ppm,
                read_version=base_version,
                window_version=base_version,
                token_version=base_version,
                commit_version=target_version,
                created_at_ms=now_ms,
            )
            window_events.append(
                (
                    normalized.batch_id,
                    event.event_seq,
                    event.request_id,
                    event.timestamp_ms,
                    event.route_key,
                    event.client_id,
                    event.weight,
                    allowed,
                )
            )
            for window_name, spec in (("short", series.short_spec), ("long", series.long_spec)):
                start = (event.timestamp_ms // spec.step_ms) * spec.step_ms
                bucket_key = (event.route_key, event.client_id, window_name, start)
                bucket = bucket_accumulator.get(bucket_key)
                if bucket is None:
                    bucket = {
                        "route_key": event.route_key,
                        "client_id": event.client_id,
                        "window_name": window_name,
                        "bucket_start_ms": start,
                        "bucket_end_ms": start + spec.step_ms,
                        "event_count": 0,
                        "weight_sum": 0,
                        "admitted_count": 0,
                        "admitted_weight_sum": 0,
                        "updated_at_ms": event.timestamp_ms,
                    }
                    bucket_accumulator[bucket_key] = bucket
                bucket["event_count"] += 1
                bucket["weight_sum"] += event.weight
                if allowed:
                    bucket["admitted_count"] += 1
                    bucket["admitted_weight_sum"] += event.weight
                bucket["updated_at_ms"] = max(bucket["updated_at_ms"], event.timestamp_ms)

        for event in sorted(new_events, key=lambda item: (item.timestamp_ms, item.event_seq, item.request_id)):
            if event.request_id in decisions_by_request:
                continue
            auth = auth_results[event.request_id]
            decisions_by_request[event.request_id] = Decision(
                request_id=event.request_id,
                batch_id=normalized.batch_id,
                event_seq=event.event_seq,
                event_fingerprint=normalized.fingerprints[event.request_id],
                timestamp_ms=event.timestamp_ms,
                route_key=event.route_key,
                client_id=event.client_id,
                quota_group=auth.quota_group,
                weight=event.weight,
                auth_ok=False,
                auth_code=auth.code,
                auth_reason=auth.reason,
                allowed=False,
                decision_code=auth.code,
                decision_reasons=(auth.code, auth.reason),
                rate_evaluated=False,
                trigger_window=None,
                short=None,
                long=None,
                short_quota=None,
                long_quota=None,
                quota_scale_ppm=None,
                read_version=base_version,
                window_version=base_version,
                token_version=base_version,
                commit_version=target_version,
                created_at_ms=now_ms,
            )

        all_decisions: list[Decision] = []
        reused_event_count = 0
        auth_denied_count = 0
        token_expired_count = 0
        rate_limited_count = 0
        allowed_count = 0
        decided_event_count = 0
        for event in sorted(normalized.events, key=lambda item: item.event_seq):
            primary = normalized.primary_events[event.request_id]
            decision = decisions_by_request.get(event.request_id)
            if decision is None:
                # Existing request decision is joined from SQLite by batch_events.
                reused_event_count += 1
                continue
            if event.event_seq != primary.event_seq:
                reused_event_count += 1
                continue
            decided_event_count += 1
            if decision.decision_code == E_AUTH_DENIED:
                auth_denied_count += 1
            elif decision.decision_code == E_TOKEN_EXPIRED:
                token_expired_count += 1
            elif decision.decision_code == E_RATE_LIMITED:
                rate_limited_count += 1
            elif decision.allowed:
                allowed_count += 1
            all_decisions.append(decision)

        # Ensure one decision object per unique newly-decided request.
        unique_decisions: list[Decision] = []
        seen_decisions: set[str] = set()
        for decision in all_decisions:
            if decision.request_id not in seen_decisions:
                seen_decisions.add(decision.request_id)
                unique_decisions.append(decision)

        bucket_deltas = [
            BucketDelta(
                route_key=value["route_key"],
                client_id=value["client_id"],
                window_name=value["window_name"],
                bucket_start_ms=value["bucket_start_ms"],
                bucket_end_ms=value["bucket_end_ms"],
                event_count=value["event_count"],
                weight_sum=value["weight_sum"],
                admitted_count=value["admitted_count"],
                admitted_weight_sum=value["admitted_weight_sum"],
                updated_at_ms=value["updated_at_ms"],
            )
            for value in sorted(
                bucket_accumulator.values(),
                key=lambda item: (
                    item["route_key"],
                    item["client_id"],
                    item["window_name"],
                    item["bucket_start_ms"],
                ),
            )
        ]
        quota_states = []
        for key, scale in sorted(key_scales.items()):
            group_name = key_groups.get(key)
            if group_name is None:
                continue
            group = registry.quota_groups[group_name]
            quota_states.append(
                {
                    "route_key": key[0],
                    "client_id": key[1],
                    "quota_group": group_name,
                    "short_limit_weight": group.short_limit_weight,
                    "long_limit_weight": group.long_limit_weight,
                    "scale_ppm": scale,
                    "updated_at_ms": now_ms,
                }
            )

        summary = {
            "decided_event_count": decided_event_count,
            "reused_event_count": reused_event_count,
            "auth_denied_count": auth_denied_count,
            "token_expired_count": token_expired_count,
            "rate_limited_count": rate_limited_count,
            "allowed_count": allowed_count,
        }
        return {
            "decisions": unique_decisions,
            "window_events": window_events,
            "bucket_deltas": bucket_deltas,
            "quota_states": quota_states,
            "summary": summary,
        }

    def get_decision(self, request_id: str) -> Decision:
        with self._gate.read():
            return self.store.get_decision(request_id)

    def get_batch(self, batch_id: str) -> BatchResult:
        with self._gate.read():
            return self.store.get_batch_result(batch_id)

    def query_windows(
        self,
        *,
        route_key: str,
        client_id: str,
        version: int | None = None,
        at_ms: int | None = None,
    ) -> dict[str, Any]:
        with self._gate.read():
            return self._query_windows_locked(
                route_key=route_key,
                client_id=client_id,
                version=version,
                at_ms=at_ms,
            )

    def _query_windows_locked(
        self,
        *,
        route_key: str,
        client_id: str,
        version: int | None = None,
        at_ms: int | None = None,
    ) -> dict[str, Any]:
        selected = self.store.validate_snapshot(version)
        if at_ms is None:
            at_ms = self.store.max_event_timestamp(selected)
        if at_ms is None:
            at_ms = 0
        min_ts = at_ms - self.settings.long_length_ms
        rows = self.store.load_events_for_key(
            route_key,
            client_id,
            min_timestamp_ms=min_ts,
            max_timestamp_ms=at_ms,
            version=selected,
        )
        series = WindowSeries(
            route_key=route_key,
            client_id=client_id,
            existing_rows=rows,
            new_events=(),
            settings=self.settings,
            secret=self.settings.local_secret,
        )
        short = series.estimate_at(at_ms, "short")
        long = series.estimate_at(at_ms, "long")
        admitted_short = series.admitted_at(at_ms, "short")
        admitted_long = series.admitted_at(at_ms, "long")
        registry = self.store.load_auth_registry(selected)
        quota_group_name = registry.client_groups.get(client_id)
        if quota_group_name is None:
            raise DomainError(
                E_SNAPSHOT_INVALID,
                "client has no committed quota-group membership at this snapshot",
                {"client_id": client_id, "version": selected},
            )
        group = registry.quota_groups[quota_group_name]
        quota_state = self.store.get_quota_state(route_key, client_id, version=selected)
        scale_ppm = int(quota_state["scale_ppm"]) if quota_state else SCALE
        short_quota = apply_scale(
            group.short_limit_weight,
            scale_ppm,
            min_scale=self.settings.quota_min_scale_ppm,
            max_scale=self.settings.quota_max_scale_ppm,
        )
        long_quota = apply_scale(
            group.long_limit_weight,
            scale_ppm,
            min_scale=self.settings.quota_min_scale_ppm,
            max_scale=self.settings.quota_max_scale_ppm,
        )
        buckets = self.store.load_bucket_rows(
            route_key,
            client_id,
            min_timestamp_ms=min_ts,
            max_timestamp_ms=at_ms,
            version=selected,
        )
        return {
            "ok": True,
            "route_key": route_key,
            "client_id": client_id,
            "quota_group": quota_group_name,
            "snapshot_version": selected,
            "at_ms": at_ms,
            "window_boundary": "left_open_right_closed:(at_ms-length_ms, at_ms]",
            "bucket_boundary": "left_closed_right_open:[bucket_start, bucket_start+step_ms)",
            "short": short.to_dict(),
            "long": long.to_dict(),
            "admitted_weight": {"short": admitted_short, "long": admitted_long},
            "quota": {
                "short": short_quota,
                "long": long_quota,
                "scale_ppm": scale_ppm,
                "short_base": group.short_limit_weight,
                "long_base": group.long_limit_weight,
            },
            "buckets": buckets,
        }

    def sampling_explanation(
        self,
        *,
        route_key: str,
        client_id: str,
        version: int | None = None,
        at_ms: int | None = None,
    ) -> dict[str, Any]:
        windows = self.query_windows(
            route_key=route_key,
            client_id=client_id,
            version=version,
            at_ms=at_ms,
        )
        return {
            "ok": True,
            "route_key": route_key,
            "client_id": client_id,
            "snapshot_version": windows["snapshot_version"],
            "at_ms": windows["at_ms"],
            "confidence_level": 0.99,
            "estimator": "Horvitz-Thompson with deterministic Bernoulli hash reservoir",
            "short": {
                "policy": windows["short"],
                "boundary": windows["window_boundary"],
            },
            "long": {
                "policy": windows["long"],
                "boundary": windows["window_boundary"],
            },
            "explanation": (
                "The exact path is used when the actual event count in the queried "
                "window is <= exact_limit. Above that threshold the deterministic hash "
                "sample rate is selected up-front for the route/client/window so the "
                "expected sample size is bounded. The interval is an approximate 99% "
                "normal interval over positive integer request weights."
            ),
        }

    def latest_recovery_report(self):
        with self._gate.read():
            return self.store.latest_recovery_report()

    def snapshot_info(self, version: int | None = None) -> dict[str, Any]:
        with self._gate.read():
            return self.store.snapshot_info(version)
