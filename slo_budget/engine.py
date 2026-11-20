"""Versioned SLI/SLO engine backed by SQLite."""

from __future__ import annotations

import hashlib
import heapq
import json
import math
import os
import re
import sqlite3
import threading
import time
import uuid
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from .config import (
    Settings,
    align_down,
    align_up,
    format_timestamp_ms,
    parse_window,
    parse_timestamp_ms,
    window_label,
)
from .db import (
    begin_read,
    connect,
    dumps,
    initialize_database,
    latest_version,
    loads,
)
from .errors import BusinessError, invalid_request, late_event
from .timeutil import wilson_interval, z_for_confidence

_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_TIMESTAMP_KEYS = ("timestamp", "ts", "time")
_GOOD_KEYS = ("good", "is_good", "good_event")
_SLI_KEYS = ("sli_type", "sli")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


@dataclass(frozen=True)
class EventRecord:
    service: str
    sli_type: str
    timestamp_ms: int
    good: bool
    event_id: str | None = None

    def canonical(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "good": self.good,
            "service": self.service,
            "sli_type": self.sli_type,
            "timestamp_ms": self.timestamp_ms,
        }


class CalculationCoordinator:
    """Single-flight and version-aware result cache for window calculations."""

    def __init__(self, max_entries: int = 512) -> None:
        self._condition = threading.Condition()
        self._inflight: set[tuple[Any, ...]] = set()
        self._errors: dict[tuple[Any, ...], BaseException] = {}
        self._cache: OrderedDict[tuple[Any, ...], dict[str, Any]] = OrderedDict()
        self._max_entries = max_entries
        self.runs = 0
        self.waits = 0
        self.cache_hits = 0

    def get_or_compute(
        self,
        key: tuple[Any, ...],
        compute: Callable[[], dict[str, Any]],
    ) -> dict[str, Any]:
        with self._condition:
            if key in self._cache:
                self.cache_hits += 1
                self._cache.move_to_end(key)
                return json.loads(json.dumps(self._cache[key]))
            if key in self._inflight:
                self.waits += 1
                while key in self._inflight:
                    self._condition.wait()
                error = self._errors.pop(key, None)
                if error is not None:
                    raise error
                if key in self._cache:
                    self._cache.move_to_end(key)
                    return json.loads(json.dumps(self._cache[key]))
                raise RuntimeError("single-flight calculation disappeared")
            self._inflight.add(key)

        try:
            result = compute()
        except BaseException as exc:
            with self._condition:
                self._errors[key] = exc
                self._inflight.discard(key)
                self._condition.notify_all()
            raise
        with self._condition:
            self.runs += 1
            self._cache[key] = json.loads(json.dumps(result))
            self._cache.move_to_end(key)
            while len(self._cache) > self._max_entries:
                self._cache.popitem(last=False)
            self._inflight.discard(key)
            self._condition.notify_all()
        return json.loads(json.dumps(result))

    def stats(self) -> dict[str, int]:
        with self._condition:
            return {
                "runs": self.runs,
                "waits": self.waits,
                "cache_hits": self.cache_hits,
                "cached_results": len(self._cache),
            }


class SLIEngine:
    """Single-writer, multi-reader SLI/SLO service."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.settings.validate()
        self._write_lock = threading.RLock()
        self.calculations = CalculationCoordinator(settings.query_cache_size)
        self._z = z_for_confidence(settings.confidence_level)
        initialize_database(settings)

    def normalize_events(
        self, raw_events: Sequence[Mapping[str, Any] | EventRecord]
    ) -> list[EventRecord]:
        if not raw_events:
            raise invalid_request("At least one event is required.")
        if len(raw_events) > self.settings.max_batch_events:
            raise invalid_request(
                "The event batch exceeds the configured maximum.",
                max_batch_events=self.settings.max_batch_events,
            )
        records: list[EventRecord] = []
        for index, raw in enumerate(raw_events):
            if isinstance(raw, EventRecord):
                records.append(raw)
                continue
            if not isinstance(raw, Mapping):
                raise invalid_request("Each event must be an object.", index=index)
            timestamp = _first_present(raw, _TIMESTAMP_KEYS)
            good = _first_present(raw, _GOOD_KEYS)
            sli_type = _first_present(raw, _SLI_KEYS)
            service = raw.get("service")
            event_id = raw.get("event_id")
            if timestamp is None:
                raise invalid_request("Missing event timestamp.", index=index)
            if good is None:
                raise invalid_request("Missing good-event flag.", index=index)
            if not isinstance(good, bool):
                raise invalid_request("good must be a JSON boolean.", index=index)
            if not isinstance(service, str) or not _NAME_RE.fullmatch(service):
                raise invalid_request("Invalid service name.", index=index)
            if not isinstance(sli_type, str) or not _NAME_RE.fullmatch(sli_type):
                raise invalid_request("Invalid sli_type.", index=index)
            if event_id is not None:
                if not isinstance(event_id, str) or not event_id or len(event_id) > 256:
                    raise invalid_request("event_id must be a non-empty string <=256 chars.", index=index)
            records.append(
                EventRecord(
                    service=service,
                    sli_type=sli_type,
                    timestamp_ms=parse_timestamp_ms(timestamp),
                    good=good,
                    event_id=event_id,
                )
            )
        seen: set[str] = set()
        duplicates: list[str] = []
        for record in records:
            if record.event_id is not None:
                if record.event_id in seen:
                    duplicates.append(record.event_id)
                seen.add(record.event_id)
        if duplicates:
            raise BusinessError(
                "E_DUPLICATE_EVENT",
                "The batch contains duplicate event_id values.",
                http_status=409,
                details={"event_ids": duplicates[:20], "duplicate_count": len(duplicates)},
            )
        return records

    def ingest(
        self,
        raw_events: Sequence[Mapping[str, Any] | EventRecord],
        *,
        batch_id: str | None = None,
        source: str | None = None,
    ) -> dict[str, Any]:
        records = self.normalize_events(raw_events)
        payload_hash = _payload_hash(records)
        if batch_id is not None and (not isinstance(batch_id, str) or not batch_id):
            raise invalid_request("batch_id must be a non-empty string")

        with self._write_lock:
            existing = self._get_batch(batch_id) if batch_id else None
            if existing is not None:
                if existing["payload_sha256"] != payload_hash:
                    raise BusinessError(
                        "E_CONFLICT",
                        "batch_id was already used with a different payload.",
                        http_status=409,
                        details={"batch_id": batch_id},
                    )
                response = loads(existing["response_json"], {})
                response["replayed"] = True
                return response

            duplicate_ids = self._existing_event_ids(
                [record.event_id for record in records if record.event_id is not None]
            )
            if duplicate_ids:
                raise BusinessError(
                    "E_DUPLICATE_EVENT",
                    "One or more event_id values already exist.",
                    http_status=409,
                    details={
                        "event_ids": duplicate_ids[:20],
                        "duplicate_count": len(duplicate_ids),
                    },
                )

            watermarks = self._validate_acceptance(records, batch_id=batch_id)
            intent_id = uuid.uuid4().hex
            created_at = utc_now()
            target_version = self._latest_version() + 1
            service_names = sorted({record.service for record in records})
            intent_details = {
                "services": service_names,
                "event_count": len(records),
                "source": source,
                "late_tolerance_seconds": self.settings.late_tolerance_seconds,
            }
            self._start_intent(
                intent_id=intent_id,
                kind="INGEST",
                service=",".join(service_names),
                batch_id=batch_id,
                payload_sha256=payload_hash,
                target_version=target_version,
                accepted_events=len(records),
                created_at=created_at,
                details=intent_details,
            )

            if self.settings.stall_after_intent_ms > 0:
                time.sleep(self.settings.stall_after_intent_ms / 1000.0)

            try:
                return self._commit_ingest(
                    records=records,
                    watermarks=watermarks,
                    batch_id=batch_id,
                    source=source,
                    payload_hash=payload_hash,
                    intent_id=intent_id,
                    target_version=target_version,
                    committed_at=created_at,
                )
            except BaseException as exc:
                try:
                    self._finish_intent_failed(intent_id, exc)
                except Exception:
                    pass
                raise

    def _commit_ingest(
        self,
        *,
        records: list[EventRecord],
        watermarks: dict[str, int],
        batch_id: str | None,
        source: str | None,
        payload_hash: str,
        intent_id: str,
        target_version: int,
        committed_at: str,
    ) -> dict[str, Any]:
        connection = connect(self.settings)
        try:
            connection.execute("BEGIN IMMEDIATE")
            if latest_version(connection) != target_version - 1:
                raise BusinessError(
                    "E_CONFLICT",
                    "Another writer committed a newer version; retry the batch.",
                    http_status=409,
                )
            if batch_id:
                row = connection.execute(
                    "SELECT payload_sha256, response_json FROM ingest_batches WHERE batch_id=?",
                    (batch_id,),
                ).fetchone()
                if row is not None:
                    if row["payload_sha256"] != payload_hash:
                        raise BusinessError(
                            "E_CONFLICT",
                            "batch_id was already used with a different payload.",
                            http_status=409,
                        )
                    response = loads(row["response_json"], {})
                    response["replayed"] = True
                    connection.commit()
                    return response

            existing_by_service: dict[str, dict[tuple[str, int], dict[str, Any]]] = {}
            for service in watermarks:
                existing_by_service[service] = self._load_latest_service_buckets(
                    connection, service, target_version - 1
                )

            additions: dict[tuple[str, str, int], list[int]] = defaultdict(lambda: [0, 0])
            for record in records:
                bucket_start = align_down(record.timestamp_ms, self.settings.bucket_ms)
                key = (record.service, record.sli_type, bucket_start)
                old = existing_by_service.setdefault(record.service, {}).get(
                    (record.sli_type, bucket_start)
                )
                if old is not None and old["state"] == "CLOSED":
                    raise late_event(
                        "The target bucket is already closed.",
                        accepted_events=0,
                        bucket_counts_unchanged=True,
                        service=record.service,
                        bucket_start=format_timestamp_ms(bucket_start),
                    )
                additions[key][0] += 1 if record.good else 0
                additions[key][1] += 1

            for index, record in enumerate(records):
                connection.execute(
                    """
                    INSERT INTO events(event_id, service, sli_type, timestamp_ms, good,
                                       bucket_start_ms, batch_id, version)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.event_id,
                        record.service,
                        record.sli_type,
                        record.timestamp_ms,
                        1 if record.good else 0,
                        align_down(record.timestamp_ms, self.settings.bucket_ms),
                        batch_id,
                        target_version,
                    ),
                )

            touched_buckets: list[dict[str, Any]] = []
            closed_now: list[dict[str, Any]] = []
            for service, service_watermark in watermarks.items():
                existing = existing_by_service.get(service, {})
                affected_keys = {
                    (sli, start)
                    for (svc, sli, start) in additions
                    if svc == service
                }
                for sli_type, bucket_start in sorted(affected_keys):
                    key = (service, sli_type, bucket_start)
                    old = existing.get((sli_type, bucket_start))
                    added_good, added_total = additions[key]
                    good_count = (old["good_count"] if old else 0) + added_good
                    total_count = (old["total_count"] if old else 0) + added_total
                    closed = self._bucket_is_closed(bucket_start, service_watermark)
                    row = {
                        "service": service,
                        "sli_type": sli_type,
                        "bucket_start_ms": bucket_start,
                        "good_count": good_count,
                        "total_count": total_count,
                        "state": "CLOSED" if closed else "OPEN",
                    }
                    self._insert_bucket_version(connection, target_version, row, committed_at)
                    touched_buckets.append(row)
                    if closed and (old is None or old["state"] != "CLOSED"):
                        closed_now.append(row)

                for (sli_type, bucket_start), old in existing.items():
                    if (sli_type, bucket_start) in affected_keys:
                        continue
                    if old["state"] == "OPEN" and self._bucket_is_closed(
                        bucket_start, service_watermark
                    ):
                        row = {
                            "service": service,
                            "sli_type": sli_type,
                            "bucket_start_ms": bucket_start,
                            "good_count": old["good_count"],
                            "total_count": old["total_count"],
                            "state": "CLOSED",
                        }
                        self._insert_bucket_version(
                            connection, target_version, row, committed_at
                        )
                        closed_now.append(row)

            for row in closed_now:
                allowed = row["total_count"] * (1.0 - self.settings.slo_target)
                bad = row["total_count"] - row["good_count"]
                ratio = bad / allowed if allowed else 0.0
                connection.execute(
                    """
                    INSERT OR IGNORE INTO closed_budget_snapshots(
                        service, sli_type, bucket_start_ms, closed_version,
                        good_count, total_count, slo_target, allowed_bad_count,
                        bad_count, consumption_ratio, closed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["service"],
                        row["sli_type"],
                        row["bucket_start_ms"],
                        target_version,
                        row["good_count"],
                        row["total_count"],
                        self.settings.slo_target,
                        allowed,
                        bad,
                        ratio,
                        committed_at,
                    ),
                )

            for service, service_watermark in watermarks.items():
                connection.execute(
                    """
                    INSERT INTO service_state(service, watermark_ms, last_version, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(service) DO UPDATE SET
                        watermark_ms=excluded.watermark_ms,
                        last_version=excluded.last_version,
                        updated_at=excluded.updated_at
                    """,
                    (service, service_watermark, target_version, committed_at),
                )
                connection.execute(
                    """
                    INSERT INTO service_state_versions(version, service, watermark_ms)
                    VALUES (?, ?, ?)
                    """,
                    (target_version, service, service_watermark),
                )

            connection.execute(
                """
                INSERT INTO versions(version, committed_at, batch_id, accepted_events, source)
                VALUES (?, ?, ?, ?, ?)
                """,
                (target_version, committed_at, batch_id, len(records), source),
            )

            alerts: list[dict[str, Any]] = []
            touched_pairs = sorted({(record.service, record.sli_type) for record in records})
            for service, sli_type in touched_pairs:
                alert = self._compute_and_store_alert(
                    connection=connection,
                    version=target_version,
                    service=service,
                    sli_type=sli_type,
                    watermark_ms=watermarks[service],
                )
                alerts.append(alert)

            response = {
                "version": target_version,
                "accepted_events": len(records),
                "rejected_events": 0,
                "batch_id": batch_id,
                "services": sorted(watermarks),
                "buckets_touched": len(touched_buckets),
                "buckets_closed": len(closed_now),
                "alerts": alerts,
                "replayed": False,
            }
            if batch_id:
                connection.execute(
                    """
                    INSERT INTO ingest_batches(batch_id, payload_sha256, version,
                                               accepted_events, response_json, committed_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        batch_id,
                        payload_hash,
                        target_version,
                        len(records),
                        dumps(response),
                        committed_at,
                    ),
                )
            connection.execute(
                """
                UPDATE write_intents
                SET state='COMPLETE', finished_at=?, target_version=?,
                    details_json=?
                WHERE intent_id=? AND state='RUNNING'
                """,
                (
                    utc_now(),
                    target_version,
                    dumps({**intent_safe_context(records), "alerts": len(alerts)}),
                    intent_id,
                ),
            )
            connection.commit()
            return response
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _validate_acceptance(
        self, records: list[EventRecord], *, batch_id: str | None
    ) -> dict[str, int]:
        """Validate lateness using a fixed view of the current watermark.

        All events in the batch are checked against the batch's final watermark. This
        makes validation independent of JSON event order and rejects the whole batch
        atomically if any event is too old.
        """

        connection = connect(self.settings, readonly=True)
        try:
            begin_read(connection)
            batch_max: dict[str, int] = {}
            for record in records:
                batch_max[record.service] = max(
                    batch_max.get(record.service, record.timestamp_ms),
                    record.timestamp_ms,
                )
            final_watermarks: dict[str, int] = {}
            for service, maximum in batch_max.items():
                existing = self._service_watermark(connection, service, None)
                final_watermarks[service] = max(existing, maximum)
            violations: list[dict[str, Any]] = []
            for record in records:
                final_watermark = final_watermarks[record.service]
                bucket_start = align_down(record.timestamp_ms, self.settings.bucket_ms)
                reason: str | None = None
                if record.timestamp_ms < final_watermark - self.settings.late_tolerance_ms:
                    reason = "OUT_OF_ORDER_BEYOND_TOLERANCE"
                elif (
                    bucket_start
                    + self.settings.bucket_ms
                    + self.settings.late_tolerance_ms
                    <= final_watermark
                ):
                    reason = "WINDOW_CLOSED"
                if reason is not None:
                    violations.append(
                        {
                            "event_id": record.event_id,
                            "service": record.service,
                            "sli_type": record.sli_type,
                            "timestamp": format_timestamp_ms(record.timestamp_ms),
                            "bucket_start": format_timestamp_ms(bucket_start),
                            "reason": reason,
                        }
                    )
            connection.commit()
        finally:
            connection.close()
        if violations:
            self._record_rejections(records, violations, batch_id=batch_id)
            raise late_event(
                "The batch was rejected atomically because one or more events are late or out of order.",
                rejection_count=len(violations),
                late_tolerance_seconds=self.settings.late_tolerance_seconds,
                violations=violations[:20],
                accepted_events=0,
                bucket_counts_unchanged=True,
            )
        return final_watermarks

    def window_metrics(
        self,
        service: str,
        sli_type: str,
        window: str | int,
        *,
        as_of: object | None = None,
        version: int | None = None,
    ) -> dict[str, Any]:
        return self.windows_metrics(
            service,
            sli_type,
            [window],
            as_of=as_of,
            version=version,
        )[0]

    def windows_metrics(
        self,
        service: str,
        sli_type: str,
        windows: Sequence[str | int],
        *,
        as_of: object | None = None,
        version: int | None = None,
    ) -> list[dict[str, Any]]:
        durations = [
            parse_window(item, self.settings.window_seconds_set()) for item in windows
        ]
        connection = connect(self.settings, readonly=True)
        try:
            begin_read(connection)
            snapshot = self._resolve_version(connection, version)
            watermark = self._service_watermark(connection, service, snapshot)
            as_of_ms = self._resolve_as_of(connection, as_of, watermark)
            connection.commit()
        finally:
            connection.close()
        results = []
        for duration in durations:
            key = (
                "window",
                snapshot,
                service,
                sli_type,
                duration,
                as_of_ms,
                self.settings.exact_event_limit,
                self.settings.slo_target,
            )
            results.append(
                self.calculations.get_or_compute(
                    key,
                    lambda duration=duration: self._compute_window_snapshot(
                        snapshot,
                        service,
                        sli_type,
                        duration,
                        as_of_ms,
                    ),
                )
            )
        return results

    def _compute_window_snapshot(
        self,
        version: int,
        service: str,
        sli_type: str,
        duration_seconds: int,
        as_of_ms: int,
    ) -> dict[str, Any]:
        connection = connect(self.settings, readonly=True)
        try:
            begin_read(connection)
            result = self._window_metrics_connection(
                connection,
                version=version,
                service=service,
                sli_type=sli_type,
                duration_seconds=duration_seconds,
                as_of_ms=as_of_ms,
                allow_sampling=True,
            )
            connection.commit()
            return result
        finally:
            connection.close()

    def _window_metrics_connection(
        self,
        connection: sqlite3.Connection,
        *,
        version: int,
        service: str,
        sli_type: str,
        duration_seconds: int,
        as_of_ms: int,
        allow_sampling: bool,
    ) -> dict[str, Any]:
        duration_ms = duration_seconds * 1000
        start_ms = as_of_ms - duration_ms
        rows = self._load_window_buckets(
            connection,
            version=version,
            service=service,
            sli_type=sli_type,
            start_ms=start_ms,
            end_ms=as_of_ms,
        )
        good_count = sum(row["good_count"] for row in rows)
        total_count = sum(row["total_count"] for row in rows)
        base = {
            "version": version,
            "service": service,
            "sli_type": sli_type,
            "window": window_label(duration_seconds),
            "window_seconds": duration_seconds,
            "window_start": format_timestamp_ms(start_ms),
            "window_end": format_timestamp_ms(as_of_ms),
            "as_of_ms": as_of_ms,
            "slo_target": self.settings.slo_target,
            "minimum_sample_count": self.settings.minimum_samples,
            "exact_event_limit": self.settings.exact_event_limit,
            "base_bucket_seconds": self.settings.bucket_seconds,
            "boundary_aligned": start_ms % self.settings.bucket_ms == 0
            and as_of_ms % self.settings.bucket_ms == 0,
            "bucket_count": len(rows),
            "closed_bucket_count": sum(1 for row in rows if row["state"] == "CLOSED"),
        }
        if total_count < self.settings.minimum_samples:
            return {
                **base,
                "status": "E_INSUFFICIENT_DATA",
                "sample_count": total_count,
                "minimum_sample_count": self.settings.minimum_samples,
                "sli": None,
                "error_budget": None,
            }

        if allow_sampling and total_count > self.settings.exact_event_limit:
            sample_good, sample_size, population = self._deterministic_reservoir(
                connection,
                version=version,
                service=service,
                sli_type=sli_type,
                start_ms=start_ms,
                end_ms=as_of_ms,
                limit=self.settings.exact_event_limit,
            )
            if population != total_count:
                raise BusinessError(
                    "E_PERSISTENCE",
                    "Bucket counts and persisted events disagree for this snapshot.",
                    http_status=500,
                    details={
                        "bucket_event_count": total_count,
                        "persisted_event_count": population,
                    },
                )
            if sample_size < self.settings.minimum_samples:
                return {
                    **base,
                    "status": "E_INSUFFICIENT_DATA",
                    "sample_count": sample_size,
                    "population_sample_count": total_count,
                    "minimum_sample_count": self.settings.minimum_samples,
                    "sli": None,
                    "error_budget": None,
                }
            sli = sample_good / sample_size
            lower, upper = wilson_interval(sample_good, sample_size, self._z)
            method = "deterministic_reservoir_blake2b_256"
            sample_rate = _round(sample_size / population)
            bad_fraction = 1.0 - sli
            allowed_bad_fraction = _round(1.0 - self.settings.slo_target, 15)
            observed_bad = bad_fraction * total_count
            allowed_bad = total_count * allowed_bad_fraction
            consumed = observed_bad / allowed_bad
            bad_upper = 1.0 - lower
            bad_lower = 1.0 - upper
            budget = {
                "allowed_bad_fraction": allowed_bad_fraction,
                "allowed_bad_events": _round(allowed_bad),
                "observed_bad_events": _round(observed_bad),
                "consumed_fraction": _round(consumed),
                "remaining_fraction": _round(1.0 - consumed),
                "exhausted": consumed >= 1.0,
                "consumed_fraction_confidence_interval": [
                    _round((bad_lower * total_count) / allowed_bad),
                    _round((bad_upper * total_count) / allowed_bad),
                ],
            }
            return {
                **base,
                "status": "OK",
                "sli": _round(sli),
                "sli_confidence_interval": [_round(lower), _round(upper)],
                "confidence_level": self.settings.confidence_level,
                "total_events": total_count,
                "sampled_good_events": sample_good,
                "sampled_events": sample_size,
                "estimated_good_events": _round(sli * total_count),
                "estimated_bad_events": _round(observed_bad),
                "sampling": {
                    "used": True,
                    "method": method,
                    "sample_size": sample_size,
                    "population_size": population,
                    "sample_rate": sample_rate,
                    "exact_event_limit": self.settings.exact_event_limit,
                    "deterministic": True,
                    "confidence_interval_method": "wilson_score",
                },
                "error_budget": budget,
            }

        sli = good_count / total_count
        allowed_bad_fraction = _round(1.0 - self.settings.slo_target, 15)
        allowed_bad = total_count * allowed_bad_fraction
        bad_count = total_count - good_count
        consumed = bad_count / allowed_bad
        return {
            **base,
            "status": "OK",
            "sli": _round(sli),
            "sli_confidence_interval": None,
            "confidence_level": None,
            "total_events": total_count,
            "good_events": good_count,
            "bad_events": bad_count,
            "sampling": {
                "used": False,
                "method": "exact",
                "sample_size": total_count,
                "population_size": total_count,
                "sample_rate": 1.0,
                "exact_event_limit": self.settings.exact_event_limit,
            },
            "error_budget": {
                "allowed_bad_fraction": allowed_bad_fraction,
                "allowed_bad_events": _round(allowed_bad),
                "observed_bad_events": bad_count,
                "consumed_fraction": _round(consumed),
                "remaining_fraction": _round(1.0 - consumed),
                "exhausted": consumed >= 1.0,
            },
        }

    def budget_summary(
        self,
        service: str,
        *,
        sli_type: str | None = None,
        start: object | None = None,
        end: object | None = None,
        version: int | None = None,
        include_rolling: bool = True,
    ) -> dict[str, Any]:
        connection = connect(self.settings, readonly=True)
        try:
            begin_read(connection)
            snapshot = self._resolve_version(connection, version)
            watermark = self._service_watermark(connection, service, snapshot)
            scope_sli = None if sli_type in (None, "", "all") else sli_type
            rows = self._load_service_buckets(connection, snapshot, service, scope_sli)
            if not rows:
                raise BusinessError(
                    "E_SERVICE_NOT_FOUND",
                    "No committed events exist for the requested service/SLI.",
                    http_status=404,
                    details={"service": service, "sli_type": scope_sli, "version": snapshot},
                )
            as_of_ms = self._resolve_as_of(connection, end, watermark)
            max_window_ms = max(self.settings.windows) * 1000
            if start is None:
                earliest = min(row["bucket_start_ms"] for row in rows)
                range_start = align_down(earliest, max_window_ms)
            else:
                range_start = align_down(
                    self._parse_boundary(start, "start"), max_window_ms
                )
            latest_bucket_end = max(
                row["bucket_start_ms"] + self.settings.bucket_ms for row in rows
            )
            if end is None:
                range_end = align_up(max(as_of_ms, latest_bucket_end), max_window_ms)
            else:
                range_end = align_up(as_of_ms, max_window_ms)
            if range_end <= range_start:
                raise invalid_request(
                    "The budget range end must be after the range start.",
                    start=format_timestamp_ms(range_start),
                    end=format_timestamp_ms(range_end),
                )
            scoped_rows = [
                row
                for row in rows
                if range_start <= row["bucket_start_ms"] < range_end
            ]
            ledger_map: dict[int, dict[str, int]] = {}
            for row in scoped_rows:
                entry = ledger_map.setdefault(
                    row["bucket_start_ms"], {"good": 0, "total": 0, "buckets": 0}
                )
                entry["good"] += row["good_count"]
                entry["total"] += row["total_count"]
                entry["buckets"] += 1
            ledger = [
                {
                    "bucket_start": format_timestamp_ms(bucket_start),
                    "bucket_start_ms": bucket_start,
                    "bucket_end": format_timestamp_ms(bucket_start + self.settings.bucket_ms),
                    "good_events": entry["good"],
                    "total_events": entry["total"],
                    "bad_events": entry["total"] - entry["good"],
                    "contribution_buckets": entry["buckets"],
                }
                for bucket_start, entry in sorted(ledger_map.items())
            ]
            total_good = sum(item["good_events"] for item in ledger)
            total_count = sum(item["total_events"] for item in ledger)
            total_bad = total_count - total_good
            allowed_fraction = _round(1.0 - self.settings.slo_target, 15)
            total_allowed = total_count * allowed_fraction
            service_total = self._budget_totals(total_good, total_count)

            tumbling: dict[str, Any] = {}
            max_slices = 0
            for duration in sorted(self.settings.windows):
                duration_ms = duration * 1000
                number = (range_end - range_start) // duration_ms
                if (range_end - range_start) % duration_ms != 0:
                    raise invalid_request(
                        "Aligned budget range is not divisible by the requested window.",
                        window=window_label(duration),
                    )
                max_slices = max(max_slices, number)
                if max_slices > 5000:
                    raise BusinessError(
                        "E_RANGE_TOO_LARGE",
                        "The requested budget range produces too many tumbling windows.",
                        http_status=422,
                        details={"max_slices": max_slices, "limit": 5000},
                    )
                slices = []
                ledger_index = 0
                sum_good = 0
                sum_total = 0
                for index in range(number):
                    chunk_start = range_start + index * duration_ms
                    chunk_end = chunk_start + duration_ms
                    good = 0
                    total = 0
                    while ledger_index < len(ledger) and ledger[ledger_index]["bucket_start_ms"] < chunk_end:
                        item = ledger[ledger_index]
                        if item["bucket_start_ms"] >= chunk_start:
                            good += item["good_events"]
                            total += item["total_events"]
                        ledger_index += 1
                    sum_good += good
                    sum_total += total
                    slices.append(
                        {
                            "window_start": format_timestamp_ms(chunk_start),
                            "window_end": format_timestamp_ms(chunk_end),
                            "good_events": good,
                            "total_events": total,
                            "bad_events": total - good,
                            **self._budget_totals(good, total),
                        }
                    )
                tumbling[window_label(duration)] = {
                    "window_seconds": duration,
                    "slice_count": number,
                    "sum_good_events": sum_good,
                    "sum_total_events": sum_total,
                    "sum_bad_events": sum_total - sum_good,
                    "sum_consumed_error_budget": sum_total - sum_good,
                    "sum_error_budget_units": _round(sum_total * allowed_fraction),
                    "equals_service_total": sum_good == total_good
                    and sum_total == total_count,
                    "slices": slices,
                }
            connection.commit()
        finally:
            connection.close()

        rolling_windows = []
        if include_rolling:
            if scope_sli is None:
                rolling_windows = self._aggregate_all_sli_windows(
                    connection=None,
                    version=snapshot,
                    service=service,
                    as_of_ms=as_of_ms,
                )
            else:
                rolling_windows = self.windows_metrics(
                    service,
                    scope_sli,
                    list(self.settings.windows),
                    as_of=as_of_ms,
                    version=snapshot,
                )

        all_tumbling_equal = all(
            item["equals_service_total"] for item in tumbling.values()
        )
        return {
            "version": snapshot,
            "service": service,
            "sli_type": scope_sli or "all",
            "range_start": format_timestamp_ms(range_start),
            "range_end": format_timestamp_ms(range_end),
            "slo_target": self.settings.slo_target,
            "service_total": service_total,
            "ledger": ledger,
            "tumbling_windows": tumbling,
            "rolling_windows": rolling_windows,
            "consistency_checks": {
                "ledger_good_events_equal_total": total_good
                == service_total["good_events"],
                "ledger_bad_events_equal_total": total_bad
                == service_total["consumed_error_budget"],
                "ledger_error_budget_units_equal_total": math.isclose(
                    total_allowed,
                    service_total["error_budget_units"],
                    rel_tol=1e-9,
                    abs_tol=1e-9,
                ),
                "all_tumbling_windows_equal_service_total": all_tumbling_equal,
                "overlapping_rolling_windows_are_not_additive": True,
            },
            "additivity_note": (
                "Use ledger or tumbling_windows for budget totals. Rolling windows overlap; "
                "their budget values must not be summed across 5m/30m/1h."
            ),
        }

    def _aggregate_all_sli_windows(
        self,
        *,
        connection: sqlite3.Connection | None,
        version: int,
        service: str,
        as_of_ms: int,
    ) -> list[dict[str, Any]]:
        del connection
        results = []
        for duration in self.settings.windows:
            duration_ms = duration * 1000
            start_ms = as_of_ms - duration_ms
            con = connect(self.settings, readonly=True)
            try:
                begin_read(con)
                rows = self._load_window_buckets(
                    con,
                    version=version,
                    service=service,
                    sli_type=None,
                    start_ms=start_ms,
                    end_ms=as_of_ms,
                )
                con.commit()
            finally:
                con.close()
            good = sum(row["good_count"] for row in rows)
            total = sum(row["total_count"] for row in rows)
            item: dict[str, Any] = {
                "version": version,
                "service": service,
                "sli_type": "all",
                "window": window_label(duration),
                "window_seconds": duration,
                "window_start": format_timestamp_ms(start_ms),
                "window_end": format_timestamp_ms(as_of_ms),
                "slo_target": self.settings.slo_target,
                "total_events": total,
                "sampling": {
                    "used": False,
                    "method": "exact_aggregate_ledger",
                    "sample_size": total,
                    "population_size": total,
                    "sample_rate": 1.0,
                    "exact_event_limit": self.settings.exact_event_limit,
                },
            }
            if total < self.settings.minimum_samples:
                item.update(
                    {
                        "status": "E_INSUFFICIENT_DATA",
                        "sample_count": total,
                        "minimum_sample_count": self.settings.minimum_samples,
                        "sli": None,
                        "error_budget": None,
                    }
                )
            else:
                item.update(
                    {
                        "status": "OK",
                        "sli": _round(good / total),
                        "sli_confidence_interval": None,
                        "good_events": good,
                        "bad_events": total - good,
                        "error_budget": self._budget_totals(good, total),
                    }
                )
            results.append(item)
        return results

    def alerts(
        self,
        *,
        service: str | None = None,
        version: int | None = None,
    ) -> dict[str, Any]:
        connection = connect(self.settings, readonly=True)
        try:
            begin_read(connection)
            snapshot = self._resolve_version(connection, version)
            params: list[Any] = [snapshot]
            where = "a.version=(" \
                "SELECT MAX(b.version) FROM alerts_history b " \
                "WHERE b.service=a.service AND b.sli_type=a.sli_type AND b.version<=? )"
            if service is not None:
                where += " AND a.service=?"
                params.append(service)
            rows = connection.execute(
                f"""
                SELECT a.* FROM alerts_history a
                WHERE {where}
                ORDER BY a.service, a.sli_type
                """,
                params,
            ).fetchall()
            alerts = [self._alert_row_to_dict(row) for row in rows]
            connection.commit()
        finally:
            connection.close()
        return {
            "version": snapshot,
            "alerts": alerts,
            "status_counts": {
                status: sum(1 for item in alerts if item["status"] == status)
                for status in ("OK", "WARNING", "CRITICAL")
            },
        }

    def rejections(
        self,
        *,
        batch_id: str | None = None,
        service: str | None = None,
        limit: int = 100,
        version: int | None = None,
    ) -> dict[str, Any]:
        if not 1 <= limit <= 1000:
            raise invalid_request("limit must be between 1 and 1000")
        connection = connect(self.settings, readonly=True)
        try:
            begin_read(connection)
            snapshot = self._resolve_version(connection, version)
            clauses = []
            params: list[Any] = []
            if batch_id is not None:
                clauses.append("batch_id=?")
                params.append(batch_id)
            if service is not None:
                clauses.append("service=?")
                params.append(service)
            where = " AND ".join(clauses) if clauses else "1=1"
            rows = connection.execute(
                f"""
                SELECT id, event_id, service, sli_type, timestamp_ms, bucket_start_ms,
                       reason, watermark_ms, batch_id, rejected_at
                FROM rejected_events
                WHERE {where}
                ORDER BY id DESC LIMIT ?
                """,
                [*params, limit],
            ).fetchall()
            connection.commit()
        finally:
            connection.close()
        return {
            "version": snapshot,
            "count": len(rows),
            "rejections": [
                {
                    "rejection_id": row["id"],
                    "event_id": row["event_id"],
                    "service": row["service"],
                    "sli_type": row["sli_type"],
                    "timestamp": format_timestamp_ms(row["timestamp_ms"]),
                    "bucket_start": format_timestamp_ms(row["bucket_start_ms"]),
                    "reason": row["reason"],
                    "watermark": format_timestamp_ms(row["watermark_ms"])
                    if row["watermark_ms"] is not None
                    else None,
                    "batch_id": row["batch_id"],
                    "rejected_at": row["rejected_at"],
                }
                for row in rows
            ],
        }

    def health(self) -> dict[str, Any]:
        connection = connect(self.settings, readonly=True)
        try:
            begin_read(connection)
            version = self._resolve_version(connection, None)
            row = connection.execute(
                "SELECT COUNT(*) AS events FROM events WHERE version<=?", (version,)
            ).fetchone()
            services = connection.execute(
                "SELECT COUNT(DISTINCT service) AS services FROM events WHERE version<=?",
                (version,),
            ).fetchone()
            connection.commit()
        finally:
            connection.close()
        return {
            "status": "ok",
            "version": version,
            "event_count": int(row["events"]),
            "service_count": int(services["services"]),
            "database": str(self.settings.db_path),
            "calculation_stats": self.calculations.stats(),
        }

    def recover(self, *, reason: str = "startup") -> dict[str, Any]:
        started_at = utc_now()
        with self._write_lock:
            connection = connect(self.settings)
            try:
                connection.execute("BEGIN IMMEDIATE")
                complete_version = latest_version(connection)
                rows = connection.execute(
                    "SELECT * FROM write_intents WHERE state='RUNNING' ORDER BY created_at"
                ).fetchall()
                actions = []
                for row in rows:
                    intent = dict(row)
                    target = intent.get("target_version")
                    action = (
                        "ROLLED_BACK_TO_LAST_COMPLETE_VERSION"
                        if target is None or target > complete_version
                        else "MARKED_ABANDONED_AFTER_COMMITTED_VERSION_CHECK"
                    )
                    connection.execute(
                        "UPDATE write_intents SET state='ABANDONED', finished_at=?, details_json=? "
                        "WHERE intent_id=? AND state='RUNNING'",
                        (
                            utc_now(),
                            dumps(
                                {
                                    **loads(intent.get("details_json"), {}),
                                    "recovery_action": action,
                                    "last_complete_version": complete_version,
                                }
                            ),
                            intent["intent_id"],
                        ),
                    )
                    actions.append(
                        {
                            "intent_id": intent["intent_id"],
                            "kind": intent["kind"],
                            "batch_id": intent.get("batch_id"),
                            "target_version": target,
                            "state": "ABANDONED",
                            "action": action,
                        }
                    )
                service_rows = connection.execute(
                    """
                    SELECT s.service, s.watermark_ms, s.last_version
                    FROM service_state s
                    WHERE s.last_version <= ?
                    ORDER BY s.service
                    """,
                    (complete_version,),
                ).fetchall()
                recovered_services = [
                    {
                        "service": row["service"],
                        "watermark": format_timestamp_ms(row["watermark_ms"]),
                        "last_version": row["last_version"],
                    }
                    for row in service_rows
                ]
                finished_at = utc_now()
                report_id = uuid.uuid4().hex
                report = {
                    "report_id": report_id,
                    "reason": reason,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "database": str(self.settings.db_path),
                    "last_complete_version": complete_version,
                    "intents_scanned": len(rows),
                    "intents_abandoned": len(rows),
                    "unfinished_intents": actions,
                    "recovered_services": recovered_services,
                    "consistency": {
                        "events_and_budgets_at_last_complete_version": True,
                        "partial_in_memory_state_is_not_persisted": True,
                        "requires_window_recompute": any(
                            action["action"]
                            == "MARKED_ABANDONED_AFTER_COMMITTED_VERSION_CHECK"
                            for action in actions
                        ),
                    },
                    "note": (
                        "This report is reconstructed only from durable SQLite facts after startup. "
                        "A forced termination cannot produce a graceful shutdown report or promise "
                        "anything about work that was never committed."
                    ),
                }
                connection.execute(
                    """
                    INSERT INTO recovery_reports(report_id, started_at, finished_at, db_path,
                        last_complete_version, intents_scanned, intents_abandoned, report_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        report_id,
                        started_at,
                        finished_at,
                        str(self.settings.db_path),
                        complete_version,
                        len(rows),
                        len(rows),
                        dumps(report),
                    ),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
            finally:
                connection.close()
            self._write_recovery_artifact(report)
            return report

    def recovery_report(self) -> dict[str, Any] | None:
        connection = connect(self.settings, readonly=True)
        try:
            begin_read(connection)
            row = connection.execute(
                "SELECT report_json FROM recovery_reports ORDER BY finished_at DESC LIMIT 1"
            ).fetchone()
            connection.commit()
        finally:
            connection.close()
        return loads(row["report_json"], None) if row else None

    def _write_recovery_artifact(self, report: dict[str, Any]) -> None:
        path = Path(self.settings.data_dir) / "recovery_report.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)

    def _get_batch(self, batch_id: str) -> sqlite3.Row | None:
        connection = connect(self.settings, readonly=True)
        try:
            begin_read(connection)
            row = connection.execute(
                "SELECT * FROM ingest_batches WHERE batch_id=?", (batch_id,)
            ).fetchone()
            connection.commit()
            return row
        finally:
            connection.close()

    def _existing_event_ids(self, event_ids: list[str]) -> list[str]:
        if not event_ids:
            return []
        found: list[str] = []
        connection = connect(self.settings, readonly=True)
        try:
            begin_read(connection)
            for offset in range(0, len(event_ids), 500):
                chunk = event_ids[offset : offset + 500]
                placeholders = ",".join("?" for _ in chunk)
                rows = connection.execute(
                    f"SELECT event_id FROM events WHERE event_id IN ({placeholders})",
                    chunk,
                ).fetchall()
                found.extend(row["event_id"] for row in rows)
            connection.commit()
        finally:
            connection.close()
        return found

    def _start_intent(
        self,
        *,
        intent_id: str,
        kind: str,
        service: str | None,
        batch_id: str | None,
        payload_sha256: str,
        target_version: int,
        accepted_events: int,
        created_at: str,
        details: dict[str, Any],
    ) -> None:
        connection = connect(self.settings)
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                INSERT INTO write_intents(intent_id, kind, service, batch_id,
                    payload_sha256, target_version, accepted_events, state,
                    created_at, details_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'RUNNING', ?, ?)
                """,
                (
                    intent_id,
                    kind,
                    service,
                    batch_id,
                    payload_sha256,
                    target_version,
                    accepted_events,
                    created_at,
                    dumps(details),
                ),
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _finish_intent_failed(self, intent_id: str, error: BaseException) -> None:
        connection = connect(self.settings)
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "UPDATE write_intents SET state='FAILED', finished_at=?, details_json=? "
                "WHERE intent_id=? AND state='RUNNING'",
                (
                    utc_now(),
                    dumps({"error": type(error).__name__, "message": str(error)}),
                    intent_id,
                ),
            )
            connection.commit()
        except BaseException:
            connection.rollback()
        finally:
            connection.close()

    def _record_rejections(
        self,
        records: list[EventRecord],
        violations: list[dict[str, Any]],
        *,
        batch_id: str | None,
    ) -> None:
        violation_keys = {
            (item.get("event_id"), item["timestamp"], item["sli_type"], item["service"])
            for item in violations
        }
        connection = connect(self.settings)
        try:
            connection.execute("BEGIN IMMEDIATE")
            rejected_at = utc_now()
            for record in records:
                key = (
                    record.event_id,
                    format_timestamp_ms(record.timestamp_ms),
                    record.sli_type,
                    record.service,
                )
                if key not in violation_keys:
                    continue
                matching = next(
                    item
                    for item in violations
                    if item.get("event_id") == record.event_id
                    and item["timestamp"] == format_timestamp_ms(record.timestamp_ms)
                    and item["sli_type"] == record.sli_type
                    and item["service"] == record.service
                )
                watermark = connection.execute(
                    "SELECT watermark_ms FROM service_state WHERE service=?",
                    (record.service,),
                ).fetchone()
                connection.execute(
                    """
                    INSERT INTO rejected_events(event_id, service, sli_type, timestamp_ms,
                        bucket_start_ms, reason, watermark_ms, batch_id, rejected_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.event_id,
                        record.service,
                        record.sli_type,
                        record.timestamp_ms,
                        align_down(record.timestamp_ms, self.settings.bucket_ms),
                        matching["reason"],
                        watermark["watermark_ms"] if watermark else None,
                        batch_id,
                        rejected_at,
                    ),
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _insert_bucket_version(
        self,
        connection: sqlite3.Connection,
        version: int,
        row: Mapping[str, Any],
        updated_at: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO bucket_versions(version, service, sli_type, bucket_start_ms,
                good_count, total_count, state, slo_target, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                version,
                row["service"],
                row["sli_type"],
                row["bucket_start_ms"],
                row["good_count"],
                row["total_count"],
                row["state"],
                self.settings.slo_target,
                updated_at,
            ),
        )

    def _load_latest_service_buckets(
        self,
        connection: sqlite3.Connection,
        service: str,
        version: int,
    ) -> dict[tuple[str, int], dict[str, Any]]:
        rows = connection.execute(
            """
            WITH ranked AS (
                SELECT b.*, ROW_NUMBER() OVER (
                    PARTITION BY b.sli_type, b.bucket_start_ms
                    ORDER BY b.version DESC
                ) AS rank
                FROM bucket_versions b
                WHERE b.service=? AND b.version<=?
            )
            SELECT * FROM ranked WHERE rank=1
            """,
            (service, version),
        ).fetchall()
        return {
            (row["sli_type"], row["bucket_start_ms"]): dict(row) for row in rows
        }

    def _load_window_buckets(
        self,
        connection: sqlite3.Connection,
        *,
        version: int,
        service: str,
        sli_type: str | None,
        start_ms: int,
        end_ms: int,
    ) -> list[sqlite3.Row]:
        sli_clause = "" if sli_type is None else " AND b.sli_type=?"
        params: list[Any] = [version, service]
        if sli_type is not None:
            params.append(sli_type)
        params.extend([start_ms, end_ms])
        return list(
            connection.execute(
                f"""
                WITH ranked AS (
                    SELECT b.*, ROW_NUMBER() OVER (
                        PARTITION BY b.sli_type, b.bucket_start_ms
                        ORDER BY b.version DESC
                    ) AS rank
                    FROM bucket_versions b
                    WHERE b.version<=? AND b.service=?{sli_clause}
                      AND b.bucket_start_ms>=? AND b.bucket_start_ms<?
                )
                SELECT * FROM ranked WHERE rank=1
                ORDER BY bucket_start_ms, sli_type
                """,
                params,
            ).fetchall()
        )

    def _load_service_buckets(
        self,
        connection: sqlite3.Connection,
        version: int,
        service: str,
        sli_type: str | None,
    ) -> list[sqlite3.Row]:
        sli_clause = "" if sli_type is None else " AND b.sli_type=?"
        params: list[Any] = [version, service]
        if sli_type is not None:
            params.append(sli_type)
        return list(
            connection.execute(
                f"""
                WITH ranked AS (
                    SELECT b.*, ROW_NUMBER() OVER (
                        PARTITION BY b.sli_type, b.bucket_start_ms
                        ORDER BY b.version DESC
                    ) AS rank
                    FROM bucket_versions b
                    WHERE b.version<=? AND b.service=?{sli_clause}
                )
                SELECT * FROM ranked WHERE rank=1
                ORDER BY bucket_start_ms, sli_type
                """,
                params,
            ).fetchall()
        )

    def _deterministic_reservoir(
        self,
        connection: sqlite3.Connection,
        *,
        version: int,
        service: str,
        sli_type: str,
        start_ms: int,
        end_ms: int,
        limit: int,
    ) -> tuple[int, int, int]:
        rows = connection.execute(
            """
            SELECT id, timestamp_ms, good
            FROM events
            WHERE version<=? AND service=? AND sli_type=?
              AND timestamp_ms>=? AND timestamp_ms<?
            """,
            (version, service, sli_type, start_ms, end_ms),
        ).fetchall()
        population = len(rows)
        if population <= limit:
            return sum(1 for row in rows if row["good"]), population, population
        heap: list[tuple[int, int, int]] = []
        for row in rows:
            payload = f"{row['id']}|{row['timestamp_ms']}|{row['good']}".encode("ascii")
            digest = int.from_bytes(
                hashlib.blake2b(payload, digest_size=16).digest(), "big"
            )
            composite = (digest << 32) | (int(row["id"]) & 0xFFFFFFFF)
            item = (-composite, int(row["id"]), int(row["good"]))
            if len(heap) < limit:
                heapq.heappush(heap, item)
            elif composite < -heap[0][0]:
                heapq.heapreplace(heap, item)
        sampled_good = sum(item[2] for item in heap)
        return sampled_good, len(heap), population

    def _compute_and_store_alert(
        self,
        *,
        connection: sqlite3.Connection,
        version: int,
        service: str,
        sli_type: str,
        watermark_ms: int,
    ) -> dict[str, Any]:
        as_of_ms = align_up(watermark_ms + 1, self.settings.bucket_ms)
        metrics: dict[int, dict[str, Any]] = {}
        for duration in self.settings.windows:
            metrics[duration] = self._window_metrics_connection(
                connection,
                version=version,
                service=service,
                sli_type=sli_type,
                duration_seconds=duration,
                as_of_ms=as_of_ms,
                allow_sampling=False,
            )
        short_duration = self.settings.alert_short_window
        long_duration = self.settings.alert_long_window
        short = metrics[short_duration]
        long = metrics[long_duration]
        required = [short, long]
        insufficient = [item for item in required if item["status"] != "OK"]
        if insufficient:
            status = "OK"
            evaluation_status = "INSUFFICIENT_DATA"
            short_burn = None
            long_burn = None
        else:
            short_burn = self._burn_rate(short["sli"])
            long_burn = self._burn_rate(long["sli"])
            if (
                short_burn >= self.settings.critical_burn_rate
                and long_burn >= self.settings.critical_burn_rate
            ):
                status = "CRITICAL"
            elif (
                short_burn >= self.settings.warning_burn_rate
                and long_burn >= self.settings.warning_burn_rate
            ):
                status = "WARNING"
            else:
                status = "OK"
            evaluation_status = status
        details = {
            "windows": {
                window_label(duration): {
                    "sli": metric.get("sli"),
                    "burn_rate": self._burn_rate(metric["sli"])
                    if metric["status"] == "OK"
                    else None,
                    "total_events": metric.get("total_events", metric.get("sample_count", 0)),
                    "status": metric["status"],
                }
                for duration, metric in sorted(metrics.items())
            },
            "rule": (
                "CRITICAL requires short and long burn rates >= critical threshold; "
                "WARNING requires short and long burn rates >= warning threshold."
            ),
            "warning_threshold": self.settings.warning_burn_rate,
            "critical_threshold": self.settings.critical_burn_rate,
            "insufficient_windows": [
                {
                    "window": item["window"],
                    "sample_count": item.get("sample_count", item.get("total_events", 0)),
                }
                for item in insufficient
            ],
        }
        stored = {
            "version": version,
            "service": service,
            "sli_type": sli_type,
            "status": status,
            "evaluation_status": evaluation_status,
            "as_of": format_timestamp_ms(as_of_ms),
            "as_of_ms": as_of_ms,
            "short_window": window_label(short_duration),
            "long_window": window_label(long_duration),
            "short_sli": short.get("sli"),
            "long_sli": long.get("sli"),
            "short_burn_rate": short_burn,
            "long_burn_rate": long_burn,
            "short_sample_count": short.get("total_events", short.get("sample_count", 0)),
            "long_sample_count": long.get("total_events", long.get("sample_count", 0)),
            "slo_target": self.settings.slo_target,
            "details": details,
        }
        connection.execute(
            """
            INSERT INTO alerts_history(version, service, sli_type, status, evaluation_status,
                as_of_ms, short_window_seconds, long_window_seconds, short_sli, long_sli,
                short_burn_rate, long_burn_rate, short_sample_count, long_sample_count,
                details_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                version,
                service,
                sli_type,
                status,
                evaluation_status,
                as_of_ms,
                short_duration,
                long_duration,
                short.get("sli"),
                long.get("sli"),
                short_burn,
                long_burn,
                stored["short_sample_count"],
                stored["long_sample_count"],
                dumps(details),
            ),
        )
        return stored

    def _alert_row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "version": row["version"],
            "service": row["service"],
            "sli_type": row["sli_type"],
            "status": row["status"],
            "evaluation_status": row["evaluation_status"],
            "as_of": format_timestamp_ms(row["as_of_ms"]),
            "short_window": window_label(row["short_window_seconds"]),
            "long_window": window_label(row["long_window_seconds"]),
            "short_sli": row["short_sli"],
            "long_sli": row["long_sli"],
            "short_burn_rate": row["short_burn_rate"],
            "long_burn_rate": row["long_burn_rate"],
            "short_sample_count": row["short_sample_count"],
            "long_sample_count": row["long_sample_count"],
            "slo_target": self.settings.slo_target,
            "details": loads(row["details_json"], {}),
        }

    def _burn_rate(self, sli: float) -> float:
        return _round((1.0 - sli) / (1.0 - self.settings.slo_target))

    def _budget_totals(self, good: int, total: int) -> dict[str, Any]:
        allowed_fraction = _round(1.0 - self.settings.slo_target, 15)
        allowed = total * allowed_fraction
        bad = total - good
        consumed = bad / allowed if allowed else 0.0
        return {
            "good_events": good,
            "total_events": total,
            "bad_events": bad,
            "sli": _round(good / total) if total else None,
            "error_budget_units": _round(allowed),
            "consumed_error_budget": bad,
            "budget_consumption_ratio": _round(consumed),
            "budget_remaining_ratio": _round(1.0 - consumed),
            "error_budget_exhausted": consumed >= 1.0,
        }

    def _bucket_is_closed(self, bucket_start_ms: int, watermark_ms: int) -> bool:
        return (
            bucket_start_ms
            + self.settings.bucket_ms
            + self.settings.late_tolerance_ms
            <= watermark_ms
        )

    def _service_watermark(
        self,
        connection: sqlite3.Connection,
        service: str,
        version: int | None,
    ) -> int:
        if version is None:
            row = connection.execute(
                "SELECT watermark_ms FROM service_state WHERE service=?", (service,)
            ).fetchone()
        else:
            row = connection.execute(
                """
                SELECT watermark_ms FROM service_state_versions
                WHERE service=? AND version<=?
                ORDER BY version DESC LIMIT 1
                """,
                (service, version),
            ).fetchone()
            if row is None:
                row = connection.execute(
                    "SELECT watermark_ms FROM service_state WHERE service=? AND last_version<=?",
                    (service, version),
                ).fetchone()
        return int(row["watermark_ms"]) if row is not None else 0

    def _resolve_version(
        self, connection: sqlite3.Connection, requested: int | None
    ) -> int:
        if requested is None:
            return latest_version(connection)
        if not isinstance(requested, int) or requested < 0:
            raise invalid_request("version must be a non-negative integer")
        row = connection.execute(
            "SELECT version FROM versions WHERE version=?", (requested,)
        ).fetchone()
        if row is None:
            raise BusinessError(
                "E_VERSION_NOT_FOUND",
                "The requested committed version does not exist.",
                http_status=404,
                details={"version": requested},
            )
        return requested

    def _resolve_as_of(
        self,
        connection: sqlite3.Connection,
        as_of: object | None,
        watermark_ms: int,
    ) -> int:
        del connection
        if as_of is None:
            if watermark_ms <= 0:
                return align_down(int(time.time() * 1000), self.settings.bucket_ms)
            return align_up(watermark_ms + 1, self.settings.bucket_ms)
        parsed = parse_timestamp_ms(as_of)
        return align_down(parsed, self.settings.bucket_ms)

    def _parse_boundary(self, value: object, name: str) -> int:
        try:
            return parse_timestamp_ms(value)
        except BusinessError as exc:
            raise invalid_request(f"Invalid {name} boundary.", error=exc.message) from exc

    def _latest_version(self) -> int:
        connection = connect(self.settings, readonly=True)
        try:
            begin_read(connection)
            version = latest_version(connection)
            connection.commit()
            return version
        finally:
            connection.close()


def _first_present(mapping: Mapping[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _round(value: float, digits: int = 9) -> float:
    return round(float(value), digits)


def _payload_hash(records: list[EventRecord]) -> str:
    canonical = sorted(
        (record.canonical() for record in records),
        key=lambda item: (
            item["timestamp_ms"],
            item["service"],
            item["sli_type"],
            item["event_id"] or "",
            item["good"],
        ),
    )
    return hashlib.sha256(
        json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def intent_safe_context(records: list[EventRecord]) -> dict[str, Any]:
    return {
        "event_count": len(records),
        "services": sorted({record.service for record in records}),
    }
