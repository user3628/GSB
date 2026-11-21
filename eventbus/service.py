from __future__ import annotations

import json
import math
import os
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .aggregation import NeumaierSum
from .errors import BusinessError
from .hashing import (
    body_digest,
    canonical_json,
    event_fingerprint,
    normalize_event_time,
    parse_measure,
    partition_for,
)
from .storage import Database


ACCEPTED = "ACCEPTED"
DISPATCHED = "DISPATCHED"
ACKED = "ACKED"
REDELIVERED = "REDELIVERED"
DEAD_LETTERED = "DEAD_LETTERED"
EVENT_STATES = (ACCEPTED, DISPATCHED, ACKED, REDELIVERED, DEAD_LETTERED)

LEGAL_TRANSITIONS: dict[str | None, frozenset[str]] = {
    None: frozenset({ACCEPTED}),
    ACCEPTED: frozenset({DISPATCHED, DEAD_LETTERED}),
    DISPATCHED: frozenset({ACKED, REDELIVERED, DEAD_LETTERED}),
    REDELIVERED: frozenset({DISPATCHED, DEAD_LETTERED}),
    # ACKED -> REDELIVERED is reserved for an explicit, transactional offset rewind.
    ACKED: frozenset({REDELIVERED}),
    DEAD_LETTERED: frozenset(),
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def epoch_iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat(timespec="microseconds")


_MISSING = object()


def _value_from(raw: dict[str, Any], body: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in raw:
            return raw[name]
    for name in names:
        if name in body:
            return body[name]
    return _MISSING


class EventBus:
    """Single-process, single-writer event bus backed by one SQLite database."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        auto_recover: bool = True,
        test_hooks: bool = False,
        crash_hook: Callable[[], None] | None = None,
    ) -> None:
        self.db = Database(db_path)
        self.db.initialize()
        self.test_hooks = test_hooks
        self.crash_hook = crash_hook or (lambda: os._exit(86))
        # A global writer lock is stronger than the required per-partition writer:
        # it also makes multi-partition publish batches atomic in this process.
        self._write_lock = threading.RLock()
        self._recovery_report: dict[str, Any] | None = None
        if auto_recover:
            self.recover()

    # ------------------------------------------------------------------
    # Topic and event ingestion
    # ------------------------------------------------------------------
    def create_topic(
        self,
        topic_id: str,
        *,
        partitions: int,
        group_id: str,
        min_retained_seq: int = 1,
    ) -> dict[str, Any]:
        if not isinstance(topic_id, str) or not topic_id.strip():
            raise BusinessError("E_REQUEST_INVALID", "topic 不能为空。", {"field": "topic"})
        if isinstance(partitions, bool) or not isinstance(partitions, int) or partitions < 1:
            raise BusinessError(
                "E_REQUEST_INVALID",
                "partitions 必须是正整数。",
                {"field": "partitions", "value": repr(partitions)},
            )
        if not isinstance(group_id, str) or not group_id.strip():
            raise BusinessError(
                "E_REQUEST_INVALID", "group_id 不能为空。", {"field": "group_id"}
            )
        if not isinstance(min_retained_seq, int) or min_retained_seq < 1:
            raise BusinessError(
                "E_REQUEST_INVALID",
                "min_retained_seq 必须是正整数。",
                {"field": "min_retained_seq", "value": repr(min_retained_seq)},
            )

        now = utc_now()
        with self._write_lock, self.db.write_transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM topics WHERE topic_id = ?", (topic_id,)
            ).fetchone()
            if existing is not None:
                same = (
                    existing["partitions"] == partitions
                    and existing["group_id"] == group_id
                    and existing["min_retained_seq"] == min_retained_seq
                )
                if not same:
                    raise BusinessError(
                        "E_CONFLICTING_TOPIC",
                        "同名主题已以不同配置声明。",
                        {
                            "topic": topic_id,
                            "existing": {
                                "partitions": existing["partitions"],
                                "group_id": existing["group_id"],
                                "min_retained_seq": existing["min_retained_seq"],
                            },
                            "requested": {
                                "partitions": partitions,
                                "group_id": group_id,
                                "min_retained_seq": min_retained_seq,
                            },
                        },
                    )
                return self._topic_dict(connection, existing, created=False)

            connection.execute(
                "INSERT INTO topics(topic_id, partitions, group_id, min_retained_seq, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (topic_id, partitions, group_id, min_retained_seq, now),
            )
            for partition in range(partitions):
                connection.execute(
                    "INSERT INTO partitions(topic_id, partition, next_seq, min_retained_seq) "
                    "VALUES (?, ?, ?, ?)",
                    (topic_id, partition, min_retained_seq, min_retained_seq),
                )
                connection.execute(
                    "INSERT INTO consumer_offsets("
                    "topic_id, group_id, partition, next_seq, replaying, version, updated_at"
                    ") VALUES (?, ?, ?, ?, 0, 1, ?)",
                    (topic_id, group_id, partition, min_retained_seq, now),
                )
                connection.execute(
                    "INSERT INTO offset_history("
                    "topic_id, group_id, partition, old_seq, new_seq, kind, reason, created_at"
                    ") VALUES (?, ?, ?, ?, ?, 'INITIAL', 'topic_created', ?)",
                    (
                        topic_id,
                        group_id,
                        partition,
                        min_retained_seq,
                        min_retained_seq,
                        now,
                    ),
                )
            row = connection.execute(
                "SELECT * FROM topics WHERE topic_id = ?", (topic_id,)
            ).fetchone()
            return self._topic_dict(connection, row, created=True)

    def get_topic(self, topic_id: str) -> dict[str, Any]:
        with self.db.read_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM topics WHERE topic_id = ?", (topic_id,)
            ).fetchone()
            if row is None:
                raise BusinessError(
                    "E_TOPIC_NOT_FOUND", "主题不存在。", {"topic": topic_id}
                )
            return self._topic_dict(connection, row, created=False)

    def list_topics(self) -> list[dict[str, Any]]:
        with self.db.read_transaction() as connection:
            rows = connection.execute("SELECT * FROM topics ORDER BY topic_id").fetchall()
            return [self._topic_dict(connection, row, created=False) for row in rows]

    def publish(
        self,
        topic_id: str,
        batch_id: str | None,
        raw_events: list[dict[str, Any]],
        *,
        crash_after_intent: bool = False,
    ) -> dict[str, Any]:
        if not isinstance(raw_events, list) or not raw_events:
            raise BusinessError(
                "E_EVENT_INVALID", "events 必须是非空数组。", {"field": "events"}
            )
        if len(raw_events) > 10_000:
            raise BusinessError(
                "E_EVENT_INVALID",
                "单批事件数超过 10000。",
                {"field": "events", "count": len(raw_events)},
            )
        self._validate_test_hook(crash_after_intent)

        parsed: list[dict[str, Any]] = []
        seen: dict[str, str] = {}
        for index, raw in enumerate(raw_events):
            event = self._parse_event(topic_id, raw, batch_id, index)
            previous = seen.get(event["event_id"])
            if previous is not None and previous != event["fingerprint"]:
                raise BusinessError(
                    "E_CONFLICTING_EVENT",
                    "同一批次内同一事件标识对应不同内容。",
                    {"event_id": event["event_id"], "indexes": [index]},
                )
            seen[event["event_id"]] = event["fingerprint"]
            parsed.append(event)

        resolved_batch_id = parsed[0]["batch_id"]
        intent_payload = {
            "topic": topic_id,
            "batch_id": resolved_batch_id,
            "events": parsed,
        }
        intent_id = self._create_intent(
            "INGEST",
            topic_id=topic_id,
            partition=None,
            group_id=None,
            payload=intent_payload,
            precondition={"event_ids": [event["event_id"] for event in parsed]},
        )
        self._after_intent_commit(crash_after_intent)
        try:
            with self._write_lock, self.db.write_transaction() as connection:
                topic = self._topic_row(connection, topic_id)
                now = utc_now()
                response_events: list[dict[str, Any]] = []
                assigned_in_batch: dict[str, sqlite3.Row] = {}
                accepted = 0
                duplicates = 0

                for event in parsed:
                    existing = connection.execute(
                        "SELECT * FROM events WHERE event_id = ?", (event["event_id"],)
                    ).fetchone()
                    if existing is None:
                        existing = assigned_in_batch.get(event["event_id"])
                    if existing is not None:
                        if existing["fingerprint"] != event["fingerprint"]:
                            raise BusinessError(
                                "E_CONFLICTING_EVENT",
                                "事件标识已被不同内容占用；首次内容保持不变。",
                                {
                                    "event_id": event["event_id"],
                                    "existing_topic": existing["topic_id"],
                                    "existing_body_digest": existing["body_digest"],
                                    "incoming_body_digest": event["body_digest"],
                                },
                            )
                        duplicates += 1
                        response_events.append(self._event_summary(existing, duplicate=True))
                        continue

                    partition = partition_for(
                        topic_id, event["partition_key"], topic["partitions"]
                    )
                    partition_row = connection.execute(
                        "SELECT * FROM partitions WHERE topic_id = ? AND partition = ?",
                        (topic_id, partition),
                    ).fetchone()
                    sequence = int(partition_row["next_seq"])
                    connection.execute(
                        "UPDATE partitions SET next_seq = ? "
                        "WHERE topic_id = ? AND partition = ?",
                        (sequence + 1, topic_id, partition),
                    )
                    connection.execute(
                        "INSERT INTO events("
                        "event_id, topic_id, partition, partition_key, production_seq, "
                        "event_time, body_digest, batch_id, event_key, measure, payload_json, "
                        "fingerprint, state, created_at, updated_at"
                        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            event["event_id"],
                            topic_id,
                            partition,
                            event["partition_key"],
                            sequence,
                            event["event_time"],
                            event["body_digest"],
                            event["batch_id"],
                            event["key"],
                            event["measure"],
                            event["payload_json"],
                            event["fingerprint"],
                            ACCEPTED,
                            now,
                            now,
                        ),
                    )
                    self._trace(
                        connection,
                        event["event_id"],
                        None,
                        ACCEPTED,
                        "ingested",
                        None,
                        now,
                    )
                    inserted = connection.execute(
                        "SELECT * FROM events WHERE event_id = ?", (event["event_id"],)
                    ).fetchone()
                    assigned_in_batch[event["event_id"]] = inserted
                    accepted += 1
                    response_events.append(self._event_summary(inserted, duplicate=False))

                connection.execute(
                    "INSERT INTO ingest_batches("
                    "internal_id, topic_id, batch_id, intent_id, accepted_count, "
                    "duplicate_count, committed_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        str(uuid.uuid4()),
                        topic_id,
                        resolved_batch_id,
                        intent_id,
                        accepted,
                        duplicates,
                        now,
                    ),
                )
                self._complete_intent(connection, intent_id)
            return {
                "batch_id": resolved_batch_id,
                "accepted": accepted,
                "duplicates": duplicates,
                "events": response_events,
                "intent_id": intent_id,
            }
        except BusinessError as exc:
            self._abort_intent(intent_id, exc)
            raise
        except Exception as exc:
            self._abort_intent(intent_id, exc)
            raise

    def _parse_event(
        self,
        topic_id: str,
        raw: Any,
        default_batch_id: str | None,
        index: int,
    ) -> dict[str, Any]:
        if not isinstance(raw, dict):
            raise BusinessError(
                "E_EVENT_INVALID",
                "事件必须是 JSON 对象。",
                {"index": index, "reason": "not_an_object"},
            )
        body = raw.get("body", {})
        if body is None:
            body = {}
        if not isinstance(body, dict):
            raise BusinessError(
                "E_EVENT_INVALID",
                "body 必须是 JSON 对象。",
                {"index": index, "field": "body"},
            )

        event_id = raw.get("event_id", _MISSING)
        if not isinstance(event_id, str) or not event_id.strip():
            raise BusinessError(
                "E_EVENT_INVALID",
                "event_id 必须是非空字符串。",
                {"index": index, "field": "event_id"},
            )
        partition_key = raw.get("partition_key", _MISSING)
        if not isinstance(partition_key, str) or not partition_key.strip():
            raise BusinessError(
                "E_EVENT_INVALID",
                "partition_key 必须是非空字符串。",
                {"index": index, "event_id": event_id, "field": "partition_key"},
            )

        time_value = _value_from(raw, body, "event_time", "timestamp")
        if time_value is _MISSING:
            raise BusinessError(
                "E_EVENT_INVALID",
                "缺少 event_time。",
                {"index": index, "event_id": event_id, "field": "event_time"},
            )
        event_time = normalize_event_time(time_value)

        key_value = _value_from(raw, body, "key", "event_key")
        if not isinstance(key_value, str) or not key_value.strip():
            raise BusinessError(
                "E_EVENT_INVALID",
                "事件 key 必须是非空字符串。",
                {"index": index, "event_id": event_id, "field": "key"},
            )
        key = key_value.strip()

        measure_value = _value_from(raw, body, "measure", "metric", "value")
        if measure_value is _MISSING:
            measure_value = None
        measure, measure_canonical = parse_measure(measure_value)

        payload_value = _value_from(raw, body, "payload", "data")
        if payload_value is _MISSING or payload_value is None:
            payload_value = {}
        if not isinstance(payload_value, dict):
            raise BusinessError(
                "E_EVENT_INVALID",
                "payload 必须是 JSON 对象。",
                {"index": index, "event_id": event_id, "field": "payload"},
            )
        try:
            payload_json = canonical_json(payload_value)
        except (TypeError, ValueError) as exc:
            raise BusinessError(
                "E_EVENT_INVALID",
                "payload 不是可规范化的 JSON。",
                {"index": index, "event_id": event_id, "reason": str(exc)},
            ) from exc

        batch_value = raw.get("batch_id", default_batch_id)
        if not isinstance(batch_value, str) or not batch_value.strip():
            raise BusinessError(
                "E_EVENT_INVALID",
                "缺少 epoch batch_id。",
                {"index": index, "event_id": event_id, "field": "batch_id"},
            )
        event_batch_id = batch_value.strip()

        digest = body_digest(key, measure_canonical, payload_value)
        supplied_digest = raw.get("body_digest")
        if supplied_digest is not None and supplied_digest != digest:
            raise BusinessError(
                "E_EVENT_INVALID",
                "body_digest 与规范化事件体不一致。",
                {
                    "index": index,
                    "event_id": event_id,
                    "supplied": supplied_digest,
                    "calculated": digest,
                },
            )
        fingerprint = event_fingerprint(
            topic_id=topic_id,
            partition_key=partition_key,
            event_time=event_time,
            key=key,
            measure_canonical=measure_canonical,
            payload=payload_value,
            digest=digest,
        )
        return {
            "event_id": event_id,
            "partition_key": partition_key,
            "event_time": event_time,
            "body_digest": digest,
            "batch_id": event_batch_id,
            "key": key,
            "measure": measure,
            "measure_canonical": measure_canonical,
            "payload_json": payload_json,
            "fingerprint": fingerprint,
        }

    # ------------------------------------------------------------------
    # Poll, acknowledge and replay
    # ------------------------------------------------------------------
    def poll(
        self,
        topic_id: str,
        group_id: str,
        partition: int,
        consumer_id: str,
        *,
        lease_seconds: float = 30.0,
        crash_after_intent: bool = False,
    ) -> dict[str, Any]:
        self._validate_scope(topic_id, group_id, partition, consumer_id)
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, (int, float))
            or not math.isfinite(float(lease_seconds))
            or not 0.05 <= float(lease_seconds) <= 3600.0
        ):
            raise BusinessError(
                "E_REQUEST_INVALID",
                "lease_seconds 必须在 0.05 到 3600 之间。",
                {"lease_seconds": repr(lease_seconds)},
            )
        self._validate_test_hook(crash_after_intent)

        intent_id = self._create_intent(
            "DISPATCH",
            topic_id=topic_id,
            group_id=group_id,
            partition=partition,
            payload={"consumer_id": consumer_id, "lease_seconds": float(lease_seconds)},
            precondition={"no_active_lease": True},
        )
        self._after_intent_commit(crash_after_intent)
        try:
            with self._write_lock, self.db.write_transaction() as connection:
                topic = self._topic_row(connection, topic_id)
                if group_id != topic["group_id"]:
                    raise BusinessError(
                        "E_GROUP_MISMATCH",
                        "消费者组与主题声明不一致。",
                        {
                            "topic": topic_id,
                            "declared_group": topic["group_id"],
                            "requested_group": group_id,
                        },
                    )
                self._partition_row(connection, topic_id, partition)
                offset = self._offset_row(connection, topic_id, group_id, partition)
                now_epoch = time.time()
                now = epoch_iso(now_epoch)

                active = connection.execute(
                    "SELECT * FROM deliveries WHERE topic_id = ? AND group_id = ? "
                    "AND partition = ? AND status = 'ACTIVE'",
                    (topic_id, group_id, partition),
                ).fetchone()
                if active is not None:
                    if float(active["lease_expires_epoch"]) > now_epoch:
                        raise BusinessError(
                            "E_LEASE_HELD",
                            "该分区已有未过期投递租约。",
                            {
                                "topic": topic_id,
                                "group_id": group_id,
                                "partition": partition,
                                "holder_consumer_id": active["consumer_id"],
                                "event_id": active["event_id"],
                                "lease_expires_at": active["lease_expires_at"],
                                "remaining_seconds": round(
                                    float(active["lease_expires_epoch"]) - now_epoch, 3
                                ),
                            },
                        )
                    connection.execute(
                        "UPDATE deliveries SET status = 'EXPIRED', ended_at = ?, "
                        "end_reason = 'lease_timeout' WHERE id = ?",
                        (now, active["id"]),
                    )
                    event = connection.execute(
                        "SELECT * FROM events WHERE event_id = ?", (active["event_id"],)
                    ).fetchone()
                    if event is not None and event["state"] == DISPATCHED:
                        self._transition(
                            connection,
                            event,
                            REDELIVERED,
                            "lease_timeout",
                            active["id"],
                            now,
                        )

                replaying = bool(offset["replaying"])
                replay_end = offset["replay_end"]
                if replaying and int(offset["next_seq"]) >= int(replay_end):
                    connection.execute(
                        "UPDATE consumer_offsets SET replaying = 0, replay_target = NULL, "
                        "replay_end = NULL, version = version + 1, updated_at = ? "
                        "WHERE topic_id = ? AND group_id = ? AND partition = ?",
                        (now, topic_id, group_id, partition),
                    )
                    replaying = False
                    replay_end = None

                event = None
                while True:
                    candidate = connection.execute(
                        "SELECT * FROM events WHERE topic_id = ? AND partition = ? "
                        "AND production_seq >= ? ORDER BY production_seq LIMIT 1",
                        (topic_id, partition, offset["next_seq"]),
                    ).fetchone()
                    if candidate is None:
                        break
                    if replaying and int(candidate["production_seq"]) >= int(replay_end):
                        break
                    if candidate["state"] == DEAD_LETTERED:
                        old_seq = int(offset["next_seq"])
                        new_seq = int(candidate["production_seq"]) + 1
                        self._set_offset(
                            connection,
                            topic_id,
                            group_id,
                            partition,
                            new_seq,
                            replaying,
                            offset["replay_target"],
                            replay_end,
                            now,
                        )
                        connection.execute(
                            "INSERT INTO offset_history("
                            "topic_id, group_id, partition, old_seq, new_seq, kind, reason, created_at"
                            ") VALUES (?, ?, ?, ?, ?, 'ADVANCE', 'dead_letter_terminal', ?)",
                            (topic_id, group_id, partition, old_seq, new_seq, now),
                        )
                        offset = self._offset_row(connection, topic_id, group_id, partition)
                        if replaying and new_seq >= int(replay_end):
                            connection.execute(
                                "UPDATE consumer_offsets SET replaying = 0, replay_target = NULL, "
                                "replay_end = NULL, version = version + 1, updated_at = ? "
                                "WHERE topic_id = ? AND group_id = ? AND partition = ?",
                                (now, topic_id, group_id, partition),
                            )
                            replaying = False
                            replay_end = None
                        continue
                    event = candidate
                    break

                if event is None:
                    self._complete_intent(connection, intent_id)
                    return {
                        "event": None,
                        "reason": "REPLAY_BARRIER" if replaying else "NO_EVENT",
                        "offset": int(offset["next_seq"]),
                        "replaying": replaying,
                        "replay_end": replay_end,
                        "lease": None,
                        "intent_id": intent_id,
                    }

                if event["state"] not in {ACCEPTED, REDELIVERED, DISPATCHED}:
                    raise BusinessError(
                        "E_INVALID_TRANSITION",
                        "当前事件状态不能被投递。",
                        {
                            "event_id": event["event_id"],
                            "state": event["state"],
                            "offset": int(offset["next_seq"]),
                        },
                    )
                if event["state"] == DISPATCHED:
                    # This is only reachable after externally damaged state. Mark the
                    # orphaned attempt as a real redelivery before granting a new lease.
                    self._transition(
                        connection,
                        event,
                        REDELIVERED,
                        "orphaned_dispatch_recovered",
                        None,
                        now,
                    )
                    event = connection.execute(
                        "SELECT * FROM events WHERE event_id = ?", (event["event_id"],)
                    ).fetchone()

                self._transition(
                    connection, event, DISPATCHED, "poll_lease_granted", None, now
                )
                event = connection.execute(
                    "SELECT * FROM events WHERE event_id = ?", (event["event_id"],)
                ).fetchone()
                expires_epoch = now_epoch + float(lease_seconds)
                lease_token = str(uuid.uuid4())
                attempt = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM deliveries WHERE event_id = ?",
                        (event["event_id"],),
                    ).fetchone()[0]
                ) + 1
                cursor = connection.execute(
                    "INSERT INTO deliveries("
                    "topic_id, group_id, partition, event_id, consumer_id, lease_token, "
                    "status, attempt, dispatched_at, lease_expires_epoch, lease_expires_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?, ?)",
                    (
                        topic_id,
                        group_id,
                        partition,
                        event["event_id"],
                        consumer_id,
                        lease_token,
                        attempt,
                        now,
                        expires_epoch,
                        epoch_iso(expires_epoch),
                    ),
                )
                delivery_id = int(cursor.lastrowid)
                self._complete_intent(connection, intent_id)
                return {
                    "event": self._event_dict(event),
                    "reason": "DELIVERED",
                    "offset": int(offset["next_seq"]),
                    "replaying": replaying,
                    "replay_end": replay_end if replaying else None,
                    "lease": {
                        "delivery_id": delivery_id,
                        "lease_token": lease_token,
                        "consumer_id": consumer_id,
                        "attempt": attempt,
                        "lease_expires_epoch": expires_epoch,
                        "lease_expires_at": epoch_iso(expires_epoch),
                    },
                    "intent_id": intent_id,
                }
        except BusinessError as exc:
            self._abort_intent(intent_id, exc)
            raise
        except Exception as exc:
            self._abort_intent(intent_id, exc)
            raise

    def ack(
        self,
        topic_id: str,
        group_id: str,
        partition: int,
        consumer_id: str,
        event_id: str,
        lease_token: str,
        *,
        crash_after_intent: bool = False,
    ) -> dict[str, Any]:
        self._validate_scope(topic_id, group_id, partition, consumer_id)
        if not isinstance(event_id, str) or not event_id:
            raise BusinessError("E_REQUEST_INVALID", "event_id 不能为空。", {})
        if not isinstance(lease_token, str) or not lease_token:
            raise BusinessError("E_REQUEST_INVALID", "lease_token 不能为空。", {})
        self._validate_test_hook(crash_after_intent)

        intent_id = self._create_intent(
            "ACK",
            topic_id=topic_id,
            group_id=group_id,
            partition=partition,
            payload={
                "consumer_id": consumer_id,
                "event_id": event_id,
                "lease_token": lease_token,
            },
            precondition={"event_not_acked": True},
        )
        self._after_intent_commit(crash_after_intent)
        try:
            with self._write_lock, self.db.write_transaction() as connection:
                topic = self._topic_row(connection, topic_id)
                if group_id != topic["group_id"]:
                    raise BusinessError(
                        "E_GROUP_MISMATCH",
                        "消费者组与主题声明不一致。",
                        {"declared_group": topic["group_id"], "requested_group": group_id},
                    )
                self._partition_row(connection, topic_id, partition)
                event = connection.execute(
                    "SELECT * FROM events WHERE event_id = ?", (event_id,)
                ).fetchone()
                if event is None or event["topic_id"] != topic_id:
                    raise BusinessError(
                        "E_NOT_DISPATCHED",
                        "确认目标不存在或不属于该主题。",
                        {"event_id": event_id, "topic": topic_id},
                    )
                if int(event["partition"]) != partition:
                    raise BusinessError(
                        "E_NOT_DISPATCHED",
                        "确认目标不属于指定分区。",
                        {"event_id": event_id, "event_partition": int(event["partition"])},
                    )

                now = utc_now()
                if event["state"] == ACKED:
                    confirmation = connection.execute(
                        "SELECT * FROM confirmations WHERE event_id = ? AND status = 'ACTIVE'",
                        (event_id,),
                    ).fetchone()
                    if confirmation is None:
                        raise BusinessError(
                            "E_INVALID_TRANSITION",
                            "事件为 ACKED 但缺少有效确认记录。",
                            {"event_id": event_id},
                        )
                    self._complete_intent(connection, intent_id)
                    return {
                        "idempotent": True,
                        "event_id": event_id,
                        "production_seq": int(event["production_seq"]),
                        "offset": int(self._offset_row(
                            connection, topic_id, group_id, partition
                        )["next_seq"]),
                        "confirmation_id": int(confirmation["id"]),
                        "intent_id": intent_id,
                    }

                if event["state"] != DISPATCHED:
                    raise BusinessError(
                        "E_NOT_DISPATCHED",
                        "事件尚未处于有效投递状态，不能确认。",
                        {"event_id": event_id, "state": event["state"]},
                    )
                delivery = connection.execute(
                    "SELECT * FROM deliveries WHERE topic_id = ? AND group_id = ? "
                    "AND partition = ? AND event_id = ? AND status = 'ACTIVE'",
                    (topic_id, group_id, partition, event_id),
                ).fetchone()
                if delivery is None:
                    raise BusinessError(
                        "E_NOT_DISPATCHED",
                        "没有匹配的当前投递租约。",
                        {"event_id": event_id},
                    )
                if (
                    delivery["lease_token"] != lease_token
                    or delivery["consumer_id"] != consumer_id
                ):
                    raise BusinessError(
                        "E_LEASE_HELD",
                        "投递租约令牌或消费者不匹配。",
                        {
                            "event_id": event_id,
                            "holder_consumer_id": delivery["consumer_id"],
                            "lease_expires_at": delivery["lease_expires_at"],
                        },
                    )
                if float(delivery["lease_expires_epoch"]) <= time.time():
                    raise BusinessError(
                        "E_NOT_DISPATCHED",
                        "投递租约已超时，确认未生效。",
                        {"event_id": event_id, "lease_expires_at": delivery["lease_expires_at"]},
                    )

                offset = self._offset_row(connection, topic_id, group_id, partition)
                seq = int(event["production_seq"])
                if int(offset["next_seq"]) != seq:
                    raise BusinessError(
                        "E_INVALID_TRANSITION",
                        "只能确认当前位点处的事件。",
                        {
                            "event_id": event_id,
                            "event_seq": seq,
                            "offset": int(offset["next_seq"]),
                        },
                    )

                self._transition(
                    connection,
                    event,
                    ACKED,
                    "consumer_ack",
                    delivery["id"],
                    now,
                )
                connection.execute(
                    "UPDATE deliveries SET status = 'ACKED', ended_at = ?, "
                    "end_reason = 'consumer_ack' WHERE id = ?",
                    (now, delivery["id"]),
                )
                cursor = connection.execute(
                    "INSERT INTO confirmations("
                    "event_id, delivery_id, topic_id, group_id, partition, production_seq, "
                    "consumer_id, status, acked_at, intent_id"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?)",
                    (
                        event_id,
                        delivery["id"],
                        topic_id,
                        group_id,
                        partition,
                        seq,
                        consumer_id,
                        now,
                        intent_id,
                    ),
                )
                confirmation_id = int(cursor.lastrowid)
                old_offset = int(offset["next_seq"])
                new_offset = seq + 1
                replaying = bool(offset["replaying"])
                replay_target = offset["replay_target"]
                replay_end = offset["replay_end"]
                if replaying and new_offset >= int(replay_end):
                    replaying = False
                    replay_target = None
                    replay_end = None
                self._set_offset(
                    connection,
                    topic_id,
                    group_id,
                    partition,
                    new_offset,
                    replaying,
                    replay_target,
                    replay_end,
                    now,
                )
                connection.execute(
                    "INSERT INTO offset_history("
                    "topic_id, group_id, partition, old_seq, new_seq, kind, reason, created_at"
                    ") VALUES (?, ?, ?, ?, ?, 'ADVANCE', 'consumer_ack', ?)",
                    (topic_id, group_id, partition, old_offset, new_offset, now),
                )
                aggregate = self._recompute_key(
                    connection, topic_id, group_id, event["event_key"], now
                )
                self._complete_intent(connection, intent_id)
                return {
                    "idempotent": False,
                    "event_id": event_id,
                    "production_seq": seq,
                    "offset": new_offset,
                    "replaying": replaying,
                    "replay_end": replay_end,
                    "confirmation_id": confirmation_id,
                    "aggregate": aggregate,
                    "intent_id": intent_id,
                }
        except BusinessError as exc:
            self._abort_intent(intent_id, exc)
            raise
        except Exception as exc:
            self._abort_intent(intent_id, exc)
            raise

    def rewind_offset(
        self,
        topic_id: str,
        group_id: str,
        partition: int,
        target_seq: int,
        *,
        reason: str = "operator_rewind",
        crash_after_intent: bool = False,
    ) -> dict[str, Any]:
        self._validate_scope(topic_id, group_id, partition, "operator")
        if isinstance(target_seq, bool) or not isinstance(target_seq, int):
            raise BusinessError(
                "E_OFFSET_NOT_FOUND",
                "回拨目标必须是整数生产序号。",
                {"target_seq": repr(target_seq)},
            )
        self._validate_test_hook(crash_after_intent)
        intent_id = self._create_intent(
            "OFFSET_REWIND",
            topic_id=topic_id,
            group_id=group_id,
            partition=partition,
            payload={"target_seq": target_seq, "reason": reason},
            precondition={"target_is_committed_seq": True},
        )
        self._after_intent_commit(crash_after_intent)
        try:
            with self._write_lock, self.db.write_transaction() as connection:
                topic = self._topic_row(connection, topic_id)
                if group_id != topic["group_id"]:
                    raise BusinessError(
                        "E_GROUP_MISMATCH",
                        "消费者组与主题声明不一致。",
                        {"declared_group": topic["group_id"], "requested_group": group_id},
                    )
                partition_row = self._partition_row(connection, topic_id, partition)
                offset = self._offset_row(connection, topic_id, group_id, partition)
                target_event = connection.execute(
                    "SELECT * FROM events WHERE topic_id = ? AND partition = ? "
                    "AND production_seq = ?",
                    (topic_id, partition, target_seq),
                ).fetchone()
                if target_seq < int(partition_row["min_retained_seq"]):
                    raise BusinessError(
                        "E_OFFSET_EXPIRED",
                        "回拨目标早于分区最早保留序号。",
                        {
                            "target_seq": target_seq,
                            "min_retained_seq": int(partition_row["min_retained_seq"]),
                        },
                    )
                if target_event is None:
                    raise BusinessError(
                        "E_OFFSET_NOT_FOUND",
                        "回拨目标不是已提交生产序号。",
                        {"target_seq": target_seq, "partition": partition},
                    )
                current = int(offset["next_seq"])
                if target_seq > current:
                    raise BusinessError(
                        "E_OFFSET_NOT_FOUND",
                        "位点不能通过回拨操作向前跳跃。",
                        {"target_seq": target_seq, "current_offset": current},
                    )
                if target_seq == current and not bool(offset["replaying"]):
                    self._complete_intent(connection, intent_id)
                    return {
                        "changed": False,
                        "old_offset": current,
                        "offset": current,
                        "replaying": False,
                        "replay_target": None,
                        "replay_end": None,
                        "rolled_back_confirmations": 0,
                        "rolled_back_events": 0,
                        "intent_id": intent_id,
                    }

                now = utc_now()
                released_delivery: int | None = None
                active = connection.execute(
                    "SELECT * FROM deliveries WHERE topic_id = ? AND group_id = ? "
                    "AND partition = ? AND status = 'ACTIVE'",
                    (topic_id, group_id, partition),
                ).fetchone()
                if active is not None:
                    connection.execute(
                        "UPDATE deliveries SET status = 'RELEASED', ended_at = ?, "
                        "end_reason = 'offset_rewind' WHERE id = ?",
                        (now, active["id"]),
                    )
                    released_delivery = int(active["id"])
                    active_event = connection.execute(
                        "SELECT * FROM events WHERE event_id = ?", (active["event_id"],)
                    ).fetchone()
                    if active_event is not None and active_event["state"] == DISPATCHED:
                        self._transition(
                            connection,
                            active_event,
                            REDELIVERED,
                            "offset_rewind",
                            active["id"],
                            now,
                        )

                rolled_back_confirmations = 0
                cursor = connection.execute(
                    "UPDATE confirmations SET status = 'ROLLED_BACK', rolled_back_at = ? "
                    "WHERE topic_id = ? AND group_id = ? AND partition = ? "
                    "AND production_seq >= ? AND status = 'ACTIVE'",
                    (now, topic_id, group_id, partition, target_seq),
                )
                rolled_back_confirmations += int(cursor.rowcount)

                rolled_back_events = 0
                replay_events = connection.execute(
                    "SELECT * FROM events WHERE topic_id = ? AND partition = ? "
                    "AND production_seq >= ? ORDER BY production_seq",
                    (topic_id, partition, target_seq),
                ).fetchall()
                for replay_event in replay_events:
                    if replay_event["state"] == ACKED:
                        self._transition(
                            connection,
                            replay_event,
                            REDELIVERED,
                            "offset_rewind_replay",
                            None,
                            now,
                        )
                        rolled_back_events += 1
                    elif replay_event["state"] == DISPATCHED:
                        self._transition(
                            connection,
                            replay_event,
                            REDELIVERED,
                            "offset_rewind_replay",
                            None,
                            now,
                        )
                        rolled_back_events += 1

                previous_replay_end = (
                    int(offset["replay_end"]) if bool(offset["replaying"]) else current
                )
                replay_end = max(current, previous_replay_end)
                replaying = target_seq < replay_end
                self._set_offset(
                    connection,
                    topic_id,
                    group_id,
                    partition,
                    target_seq,
                    replaying,
                    target_seq if replaying else None,
                    replay_end if replaying else None,
                    now,
                )
                connection.execute(
                    "INSERT INTO offset_history("
                    "topic_id, group_id, partition, old_seq, new_seq, kind, reason, created_at"
                    ") VALUES (?, ?, ?, ?, ?, 'ROLLBACK', ?, ?)",
                    (topic_id, group_id, partition, current, target_seq, reason, now),
                )
                rebuilt = self._rebuild_all_aggregates(connection, now)
                self._complete_intent(connection, intent_id)
                return {
                    "changed": True,
                    "old_offset": current,
                    "offset": target_seq,
                    "replaying": replaying,
                    "replay_target": target_seq if replaying else None,
                    "replay_end": replay_end if replaying else None,
                    "rolled_back_confirmations": rolled_back_confirmations,
                    "rolled_back_events": rolled_back_events,
                    "released_delivery_id": released_delivery,
                    "aggregate_keys_rebuilt": rebuilt,
                    "intent_id": intent_id,
                }
        except BusinessError as exc:
            self._abort_intent(intent_id, exc)
            raise
        except Exception as exc:
            self._abort_intent(intent_id, exc)
            raise

    def dead_letter(
        self,
        topic_id: str,
        group_id: str,
        partition: int,
        event_id: str,
        *,
        reason: str = "operator_dead_letter",
    ) -> dict[str, Any]:
        self._validate_scope(topic_id, group_id, partition, "operator")
        intent_id = self._create_intent(
            "DEAD_LETTER",
            topic_id=topic_id,
            group_id=group_id,
            partition=partition,
            payload={"event_id": event_id, "reason": reason},
            precondition={"state_terminal_not_acked": True},
        )
        try:
            with self._write_lock, self.db.write_transaction() as connection:
                topic = self._topic_row(connection, topic_id)
                if group_id != topic["group_id"]:
                    raise BusinessError(
                        "E_GROUP_MISMATCH",
                        "消费者组与主题声明不一致。",
                        {"declared_group": topic["group_id"], "requested_group": group_id},
                    )
                self._partition_row(connection, topic_id, partition)
                event = connection.execute(
                    "SELECT * FROM events WHERE event_id = ?", (event_id,)
                ).fetchone()
                if event is None or int(event["partition"]) != partition:
                    raise BusinessError(
                        "E_EVENT_NOT_FOUND",
                        "事件不存在于指定主题分区。",
                        {"event_id": event_id, "topic": topic_id, "partition": partition},
                    )
                now = utc_now()
                if event["state"] not in {ACCEPTED, DISPATCHED, REDELIVERED}:
                    raise BusinessError(
                        "E_INVALID_TRANSITION",
                        f"事件状态 {event['state']} 不能迁移到 {DEAD_LETTERED}。",
                        {
                            "event_id": event_id,
                            "from_state": event["state"],
                            "to_state": DEAD_LETTERED,
                        },
                    )
                active = connection.execute(
                    "SELECT * FROM deliveries WHERE event_id = ? AND status = 'ACTIVE'",
                    (event_id,),
                ).fetchone()
                if active is not None:
                    connection.execute(
                        "UPDATE deliveries SET status = 'RELEASED', ended_at = ?, "
                        "end_reason = 'dead_lettered' WHERE id = ?",
                        (now, active["id"]),
                    )
                self._transition(
                    connection,
                    event,
                    DEAD_LETTERED,
                    reason,
                    active["id"] if active is not None else None,
                    now,
                )
                offset = self._offset_row(connection, topic_id, group_id, partition)
                seq = int(event["production_seq"])
                new_offset = int(offset["next_seq"])
                if new_offset == seq:
                    new_offset = seq + 1
                    replaying = bool(offset["replaying"])
                    replay_target = offset["replay_target"]
                    replay_end = offset["replay_end"]
                    if replaying and new_offset >= int(replay_end):
                        replaying = False
                        replay_target = None
                        replay_end = None
                    self._set_offset(
                        connection,
                        topic_id,
                        group_id,
                        partition,
                        new_offset,
                        replaying,
                        replay_target,
                        replay_end,
                        now,
                    )
                    connection.execute(
                        "INSERT INTO offset_history("
                        "topic_id, group_id, partition, old_seq, new_seq, kind, reason, created_at"
                        ") VALUES (?, ?, ?, ?, ?, 'ADVANCE', 'dead_letter_terminal', ?)",
                        (topic_id, group_id, partition, seq, new_offset, now),
                    )
                self._complete_intent(connection, intent_id)
                return {
                    "event_id": event_id,
                    "state": DEAD_LETTERED,
                    "offset": new_offset,
                    "intent_id": intent_id,
                }
        except BusinessError as exc:
            self._abort_intent(intent_id, exc)
            raise
        except Exception as exc:
            self._abort_intent(intent_id, exc)
            raise

    # ------------------------------------------------------------------
    # Read model
    # ------------------------------------------------------------------
    def get_event(self, event_id: str) -> dict[str, Any]:
        with self.db.read_transaction() as connection:
            event = connection.execute(
                "SELECT * FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
            if event is None:
                raise BusinessError(
                    "E_EVENT_NOT_FOUND", "事件不存在。", {"event_id": event_id}
                )
            result = self._event_dict(event)
            result["state_trace"] = [
                dict(row)
                for row in connection.execute(
                    "SELECT id, from_state, to_state, reason, delivery_id, created_at "
                    "FROM event_state_trace WHERE event_id = ? ORDER BY id",
                    (event_id,),
                ).fetchall()
            ]
            result["deliveries"] = [
                dict(row)
                for row in connection.execute(
                    "SELECT id, consumer_id, status, attempt, dispatched_at, "
                    "lease_expires_at, ended_at, end_reason FROM deliveries "
                    "WHERE event_id = ? ORDER BY id",
                    (event_id,),
                ).fetchall()
            ]
            result["confirmations"] = [
                dict(row)
                for row in connection.execute(
                    "SELECT id, consumer_id, status, acked_at, rolled_back_at "
                    "FROM confirmations WHERE event_id = ? ORDER BY id",
                    (event_id,),
                ).fetchall()
            ]
            return result

    def list_events(
        self,
        *,
        topic_id: str | None = None,
        partition: int | None = None,
        state: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 5000))
        clauses: list[str] = []
        params: list[Any] = []
        if topic_id is not None:
            clauses.append("topic_id = ?")
            params.append(topic_id)
        if partition is not None:
            clauses.append("partition = ?")
            params.append(partition)
        if state is not None:
            if state not in EVENT_STATES:
                raise BusinessError(
                    "E_REQUEST_INVALID", "未知事件状态。", {"state": state}
                )
            clauses.append("state = ?")
            params.append(state)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        with self.db.read_transaction() as connection:
            rows = connection.execute(
                f"SELECT * FROM events {where} ORDER BY topic_id, partition, production_seq LIMIT ?",
                params,
            ).fetchall()
            return [self._event_dict(row) for row in rows]

    def get_offset(self, topic_id: str, group_id: str, partition: int) -> dict[str, Any]:
        with self.db.read_transaction() as connection:
            self._topic_row(connection, topic_id)
            self._partition_row(connection, topic_id, partition)
            row = self._offset_row(connection, topic_id, group_id, partition)
            return self._offset_dict(row)

    def get_aggregates(self, topic_id: str, group_id: str) -> dict[str, Any]:
        with self.db.read_transaction() as connection:
            topic = self._topic_row(connection, topic_id)
            if group_id != topic["group_id"]:
                raise BusinessError(
                    "E_GROUP_MISMATCH",
                    "消费者组与主题声明不一致。",
                    {"declared_group": topic["group_id"], "requested_group": group_id},
                )
            rows = connection.execute(
                "SELECT * FROM aggregate_state WHERE topic_id = ? AND group_id = ? "
                "ORDER BY event_key",
                (topic_id, group_id),
            ).fetchall()
            return {
                "topic": topic_id,
                "group_id": group_id,
                "groups": [
                    {
                        "key": row["event_key"],
                        "count": int(row["event_count"]),
                        "sum": float(row["measure_sum"] + row["compensation"]),
                        "compensation": float(row["compensation"]),
                    }
                    for row in rows
                ],
            }

    def snapshot(self, topic_id: str, group_id: str) -> dict[str, Any]:
        """Read all consistency-sensitive facts from one SQLite read snapshot."""

        with self.db.read_transaction() as connection:
            topic = self._topic_row(connection, topic_id)
            if group_id != topic["group_id"]:
                raise BusinessError(
                    "E_GROUP_MISMATCH",
                    "消费者组与主题声明不一致。",
                    {"declared_group": topic["group_id"], "requested_group": group_id},
                )
            events = [
                self._event_dict(row)
                for row in connection.execute(
                    "SELECT * FROM events WHERE topic_id = ? "
                    "ORDER BY partition, production_seq",
                    (topic_id,),
                ).fetchall()
            ]
            offsets = [
                self._offset_dict(row)
                for row in connection.execute(
                    "SELECT * FROM consumer_offsets WHERE topic_id = ? AND group_id = ? "
                    "ORDER BY partition",
                    (topic_id, group_id),
                ).fetchall()
            ]
            aggregates = [
                {
                    "key": row["event_key"],
                    "count": int(row["event_count"]),
                    "sum": float(row["measure_sum"] + row["compensation"]),
                    "compensation": float(row["compensation"]),
                }
                for row in connection.execute(
                    "SELECT * FROM aggregate_state WHERE topic_id = ? AND group_id = ? "
                    "ORDER BY event_key",
                    (topic_id, group_id),
                ).fetchall()
            ]
            return {
                "topic": topic_id,
                "group_id": group_id,
                "events": events,
                "offsets": offsets,
                "aggregates": aggregates,
            }

    def get_latest_recovery_report(self) -> dict[str, Any] | None:
        with self.db.read_transaction() as connection:
            row = connection.execute(
                "SELECT report_json FROM recovery_reports ORDER BY created_at DESC, rowid DESC LIMIT 1"
            ).fetchone()
            return json.loads(row["report_json"]) if row is not None else None

    def recover(self) -> dict[str, Any]:
        """Abort incomplete intents and reconcile to the last complete boundary."""

        with self._write_lock, self.db.write_transaction() as connection:
            started = utc_now()
            report_id = str(uuid.uuid4())
            pending = connection.execute(
                "SELECT * FROM intents WHERE status = 'PENDING' ORDER BY created_at, rowid"
            ).fetchall()
            aborted_intents: list[dict[str, Any]] = []
            for intent in pending:
                connection.execute(
                    "UPDATE intents SET status = 'ABORTED', completed_at = ?, "
                    "error = 'startup_recovery_aborted_incomplete_intent' WHERE intent_id = ?",
                    (started, intent["intent_id"]),
                )
                aborted_intents.append(
                    {
                        "intent_id": intent["intent_id"],
                        "kind": intent["kind"],
                        "topic": intent["topic_id"],
                        "group_id": intent["group_id"],
                        "partition": intent["partition"],
                    }
                )

            released_leases: list[dict[str, Any]] = []
            active_deliveries = connection.execute(
                "SELECT * FROM deliveries WHERE status = 'ACTIVE' ORDER BY id"
            ).fetchall()
            for delivery in active_deliveries:
                connection.execute(
                    "UPDATE deliveries SET status = 'RECOVERED', ended_at = ?, "
                    "end_reason = 'process_restart' WHERE id = ?",
                    (started, delivery["id"]),
                )
                event = connection.execute(
                    "SELECT * FROM events WHERE event_id = ?", (delivery["event_id"],)
                ).fetchone()
                if event is not None and event["state"] == DISPATCHED:
                    self._transition(
                        connection,
                        event,
                        REDELIVERED,
                        "process_restart_release",
                        delivery["id"],
                        started,
                    )
                released_leases.append(
                    {
                        "delivery_id": int(delivery["id"]),
                        "event_id": delivery["event_id"],
                        "consumer_id": delivery["consumer_id"],
                    }
                )

            offset_checks: list[dict[str, Any]] = []
            rolled_back_events = 0
            rolled_back_confirmations = 0
            topics = connection.execute("SELECT * FROM topics ORDER BY topic_id").fetchall()
            for topic in topics:
                for partition_row in connection.execute(
                    "SELECT * FROM partitions WHERE topic_id = ? ORDER BY partition",
                    (topic["topic_id"],),
                ).fetchall():
                    topic_id = topic["topic_id"]
                    group_id = topic["group_id"]
                    partition = int(partition_row["partition"])
                    offset = self._offset_row(connection, topic_id, group_id, partition)
                    boundary = self._complete_boundary(connection, topic_id, partition)
                    current = int(offset["next_seq"])
                    target = min(current, boundary)
                    high_water = max(current, boundary)

                    reset_events = connection.execute(
                        "SELECT * FROM events WHERE topic_id = ? AND partition = ? "
                        "AND production_seq >= ? AND state = 'ACKED' ORDER BY production_seq",
                        (topic_id, partition, target),
                    ).fetchall()
                    for event in reset_events:
                        self._transition(
                            connection,
                            event,
                            REDELIVERED,
                            "recovery_rollback_to_complete_boundary",
                            None,
                            started,
                        )
                        rolled_back_events += 1
                    cursor = connection.execute(
                        "UPDATE confirmations SET status = 'ROLLED_BACK', rolled_back_at = ? "
                        "WHERE topic_id = ? AND group_id = ? AND partition = ? "
                        "AND production_seq >= ? AND status = 'ACTIVE'",
                        (started, topic_id, group_id, partition, target),
                    )
                    rolled_back_confirmations += int(cursor.rowcount)

                    changed = current != target
                    replaying = high_water > target
                    was_replaying = bool(offset["replaying"])
                    if changed or replaying or was_replaying:
                        self._set_offset(
                            connection,
                            topic_id,
                            group_id,
                            partition,
                            target,
                            replaying,
                            target if replaying else None,
                            high_water if replaying else None,
                            started,
                        )
                        connection.execute(
                            "INSERT INTO offset_history("
                            "topic_id, group_id, partition, old_seq, new_seq, kind, reason, created_at"
                            ") VALUES (?, ?, ?, ?, ?, 'RECOVERY_ROLLBACK', "
                            "'recovery_last_complete_boundary', ?)",
                            (topic_id, group_id, partition, current, target, started),
                        )
                    offset_checks.append(
                        {
                            "topic": topic_id,
                            "group_id": group_id,
                            "partition": partition,
                            "persisted_offset_before": current,
                            "complete_boundary_from_facts": boundary,
                            "offset_after": target,
                            "replaying": replaying,
                            "replay_end": high_water if replaying else None,
                            "changed": changed,
                        }
                    )

            aggregate_keys_rebuilt = self._rebuild_all_aggregates(connection, started)
            report = {
                "report_id": report_id,
                "started_at": started,
                "completed_at": utc_now(),
                "source": "persisted_sqlite_facts_only",
                "pending_intents_found": len(pending),
                "pending_intents_aborted": aborted_intents,
                "released_leases": released_leases,
                "offset_checks": offset_checks,
                "rolled_back_events": rolled_back_events,
                "rolled_back_confirmations": rolled_back_confirmations,
                "aggregate_keys_rebuilt": aggregate_keys_rebuilt,
                "notes": [
                    "A forcibly killed process does not emit a graceful shutdown report.",
                    "This report is generated only after restart from durable intents and state.",
                ],
            }
            connection.execute(
                "INSERT INTO recovery_reports(report_id, created_at, report_json) "
                "VALUES (?, ?, ?)",
                (report_id, report["completed_at"], canonical_json(report)),
            )
            self._recovery_report = report
            return report

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _validate_scope(
        self, topic_id: str, group_id: str, partition: int, consumer_id: str
    ) -> None:
        for field, value in (
            ("topic", topic_id),
            ("group_id", group_id),
            ("consumer_id", consumer_id),
        ):
            if not isinstance(value, str) or not value.strip():
                raise BusinessError(
                    "E_REQUEST_INVALID", f"{field} 不能为空。", {"field": field}
                )
        if isinstance(partition, bool) or not isinstance(partition, int) or partition < 0:
            raise BusinessError(
                "E_REQUEST_INVALID",
                "partition 必须是非负整数。",
                {"field": "partition", "value": repr(partition)},
            )

    def _topic_row(
        self, connection: sqlite3.Connection, topic_id: str
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM topics WHERE topic_id = ?", (topic_id,)
        ).fetchone()
        if row is None:
            raise BusinessError("E_TOPIC_NOT_FOUND", "主题不存在。", {"topic": topic_id})
        return row

    def _partition_row(
        self, connection: sqlite3.Connection, topic_id: str, partition: int
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM partitions WHERE topic_id = ? AND partition = ?",
            (topic_id, partition),
        ).fetchone()
        if row is None:
            raise BusinessError(
                "E_PARTITION_NOT_FOUND",
                "分区不存在。",
                {"topic": topic_id, "partition": partition},
            )
        return row

    def _offset_row(
        self, connection: sqlite3.Connection, topic_id: str, group_id: str, partition: int
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM consumer_offsets WHERE topic_id = ? AND group_id = ? "
            "AND partition = ?",
            (topic_id, group_id, partition),
        ).fetchone()
        if row is None:
            raise BusinessError(
                "E_GROUP_MISMATCH",
                "消费者组在该主题没有位点。",
                {"topic": topic_id, "group_id": group_id, "partition": partition},
            )
        return row

    def _set_offset(
        self,
        connection: sqlite3.Connection,
        topic_id: str,
        group_id: str,
        partition: int,
        next_seq: int,
        replaying: bool,
        replay_target: int | None,
        replay_end: int | None,
        now: str,
    ) -> None:
        connection.execute(
            "UPDATE consumer_offsets SET next_seq = ?, replaying = ?, replay_target = ?, "
            "replay_end = ?, version = version + 1, updated_at = ? "
            "WHERE topic_id = ? AND group_id = ? AND partition = ?",
            (
                next_seq,
                1 if replaying else 0,
                replay_target if replaying else None,
                replay_end if replaying else None,
                now,
                topic_id,
                group_id,
                partition,
            ),
        )

    def _trace(
        self,
        connection: sqlite3.Connection,
        event_id: str,
        from_state: str | None,
        to_state: str,
        reason: str,
        delivery_id: int | None,
        now: str,
    ) -> None:
        connection.execute(
            "INSERT INTO event_state_trace("
            "event_id, from_state, to_state, reason, delivery_id, created_at"
            ") VALUES (?, ?, ?, ?, ?, ?)",
            (event_id, from_state, to_state, reason, delivery_id, now),
        )

    def _transition(
        self,
        connection: sqlite3.Connection,
        event: sqlite3.Row,
        to_state: str,
        reason: str,
        delivery_id: int | None,
        now: str,
    ) -> None:
        from_state = event["state"]
        if to_state not in LEGAL_TRANSITIONS.get(from_state, frozenset()):
            raise BusinessError(
                "E_INVALID_TRANSITION",
                f"事件状态不能从 {from_state} 迁移到 {to_state}。",
                {
                    "event_id": event["event_id"],
                    "from_state": from_state,
                    "to_state": to_state,
                    "reason": reason,
                },
            )
        if to_state == REDELIVERED and from_state == ACKED and reason in {
            "consumer_ack",
        }:
            raise BusinessError(
                "E_INVALID_TRANSITION",
                "ACKED 只能通过显式位点回拨回到 REDELIVERED。",
                {"event_id": event["event_id"], "reason": reason},
            )
        connection.execute(
            "UPDATE events SET state = ?, updated_at = ? WHERE event_id = ?",
            (to_state, now, event["event_id"]),
        )
        self._trace(connection, event["event_id"], from_state, to_state, reason, delivery_id, now)

    def _complete_boundary(
        self, connection: sqlite3.Connection, topic_id: str, partition: int
    ) -> int:
        partition_row = connection.execute(
            "SELECT * FROM partitions WHERE topic_id = ? AND partition = ?",
            (topic_id, partition),
        ).fetchone()
        boundary = int(partition_row["min_retained_seq"])
        while True:
            event = connection.execute(
                "SELECT * FROM events WHERE topic_id = ? AND partition = ? "
                "AND production_seq = ?",
                (topic_id, partition, boundary),
            ).fetchone()
            if event is None:
                break
            if event["state"] == DEAD_LETTERED:
                boundary += 1
                continue
            if event["state"] == ACKED and connection.execute(
                "SELECT 1 FROM confirmations WHERE event_id = ? AND status = 'ACTIVE'",
                (event["event_id"],),
            ).fetchone() is not None:
                boundary += 1
                continue
            break
        return boundary

    def _recompute_key(
        self,
        connection: sqlite3.Connection,
        topic_id: str,
        group_id: str,
        key: str,
        now: str,
    ) -> dict[str, Any]:
        rows = connection.execute(
            "SELECT e.event_key, e.measure FROM confirmations c "
            "JOIN events e ON e.event_id = c.event_id "
            "WHERE c.topic_id = ? AND c.group_id = ? AND c.status = 'ACTIVE' "
            "AND e.event_key = ? ORDER BY e.partition, e.production_seq",
            (topic_id, group_id, key),
        ).fetchall()
        accumulator = NeumaierSum()
        count = 0
        for row in rows:
            accumulator.add(row["measure"])
            count += 1
        connection.execute(
            "INSERT INTO aggregate_state("
            "topic_id, group_id, event_key, event_count, measure_sum, compensation, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(topic_id, group_id, event_key) DO UPDATE SET "
            "event_count = excluded.event_count, measure_sum = excluded.measure_sum, "
            "compensation = excluded.compensation, updated_at = excluded.updated_at",
            (
                topic_id,
                group_id,
                key,
                count,
                accumulator.total,
                accumulator.compensation,
                now,
            ),
        )
        return {
            "key": key,
            "count": count,
            "sum": accumulator.value,
            "compensation": accumulator.compensation,
        }

    def _rebuild_all_aggregates(
        self, connection: sqlite3.Connection, now: str
    ) -> int:
        topics = connection.execute("SELECT * FROM topics ORDER BY topic_id").fetchall()
        keys = 0
        for topic in topics:
            topic_id = topic["topic_id"]
            group_id = topic["group_id"]
            rows = connection.execute(
                "SELECT e.event_key, e.measure FROM confirmations c "
                "JOIN events e ON e.event_id = c.event_id "
                "WHERE c.topic_id = ? AND c.group_id = ? AND c.status = 'ACTIVE' "
                "ORDER BY e.event_key, e.partition, e.production_seq",
                (topic_id, group_id),
            ).fetchall()
            aggregated: dict[str, tuple[int, NeumaierSum]] = {}
            for row in rows:
                if row["event_key"] not in aggregated:
                    aggregated[row["event_key"]] = (0, NeumaierSum())
                count, accumulator = aggregated[row["event_key"]]
                accumulator.add(row["measure"])
                aggregated[row["event_key"]] = (count + 1, accumulator)
            connection.execute(
                "DELETE FROM aggregate_state WHERE topic_id = ? AND group_id = ?",
                (topic_id, group_id),
            )
            for key in sorted(aggregated):
                count, accumulator = aggregated[key]
                connection.execute(
                    "INSERT INTO aggregate_state("
                    "topic_id, group_id, event_key, event_count, measure_sum, "
                    "compensation, updated_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        topic_id,
                        group_id,
                        key,
                        count,
                        accumulator.total,
                        accumulator.compensation,
                        now,
                    ),
                )
                keys += 1
        return keys

    def _create_intent(
        self,
        kind: str,
        *,
        topic_id: str | None,
        group_id: str | None,
        partition: int | None,
        payload: dict[str, Any],
        precondition: dict[str, Any],
    ) -> str:
        intent_id = str(uuid.uuid4())
        now = utc_now()
        with self.db.write_transaction() as connection:
            connection.execute(
                "INSERT INTO intents("
                "intent_id, kind, topic_id, group_id, partition, payload_json, "
                "precondition_json, status, created_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)",
                (
                    intent_id,
                    kind,
                    topic_id,
                    group_id,
                    partition,
                    canonical_json(payload),
                    canonical_json(precondition),
                    now,
                ),
            )
        return intent_id

    def _complete_intent(self, connection: sqlite3.Connection, intent_id: str) -> None:
        connection.execute(
            "UPDATE intents SET status = 'COMMITTED', completed_at = ? "
            "WHERE intent_id = ? AND status = 'PENDING'",
            (utc_now(), intent_id),
        )

    def _abort_intent(self, intent_id: str, exc: BaseException) -> None:
        try:
            with self.db.write_transaction() as connection:
                connection.execute(
                    "UPDATE intents SET status = 'ABORTED', completed_at = ?, error = ? "
                    "WHERE intent_id = ? AND status = 'PENDING'",
                    (utc_now(), f"{type(exc).__name__}: {str(exc)[:500]}", intent_id),
                )
        except Exception:
            # Recovery will safely abort a still-pending intent on next startup.
            pass

    def _after_intent_commit(self, requested: bool) -> None:
        if not requested:
            return
        self._validate_test_hook(True)
        self.crash_hook()

    def _validate_test_hook(self, requested: bool) -> None:
        if requested and not self.test_hooks:
            raise BusinessError(
                "E_TEST_HOOK_DISABLED",
                "未启用测试用强制终止钩子。",
                {"hint": "set EVENTBUS_ENABLE_TEST_HOOKS=1"},
            )

    def _topic_dict(
        self, connection: sqlite3.Connection, row: sqlite3.Row, *, created: bool
    ) -> dict[str, Any]:
        partitions = connection.execute(
            "SELECT * FROM partitions WHERE topic_id = ? ORDER BY partition",
            (row["topic_id"],),
        ).fetchall()
        event_counts = {
            int(item["partition"]): int(item["count"])
            for item in connection.execute(
                "SELECT partition, COUNT(*) AS count FROM events WHERE topic_id = ? "
                "GROUP BY partition",
                (row["topic_id"],),
            ).fetchall()
        }
        return {
            "topic": row["topic_id"],
            "partitions": int(row["partitions"]),
            "group_id": row["group_id"],
            "min_retained_seq": int(row["min_retained_seq"]),
            "created_at": row["created_at"],
            "created": created,
            "partition_state": [
                {
                    "partition": int(partition["partition"]),
                    "next_seq": int(partition["next_seq"]),
                    "min_retained_seq": int(partition["min_retained_seq"]),
                    "event_count": event_counts.get(int(partition["partition"]), 0),
                }
                for partition in partitions
            ],
        }

    def _event_summary(self, row: sqlite3.Row, *, duplicate: bool) -> dict[str, Any]:
        return {
            "event_id": row["event_id"],
            "partition": int(row["partition"]),
            "production_seq": int(row["production_seq"]),
            "state": row["state"],
            "body_digest": row["body_digest"],
            "batch_id": row["batch_id"],
            "duplicate": duplicate,
        }

    def _event_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "event_id": row["event_id"],
            "topic": row["topic_id"],
            "partition": int(row["partition"]),
            "partition_key": row["partition_key"],
            "production_seq": int(row["production_seq"]),
            "event_time": row["event_time"],
            "body_digest": row["body_digest"],
            "batch_id": row["batch_id"],
            "key": row["event_key"],
            "measure": row["measure"],
            "payload": json.loads(row["payload_json"]),
            "state": row["state"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _offset_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "topic": row["topic_id"],
            "group_id": row["group_id"],
            "partition": int(row["partition"]),
            "offset": int(row["next_seq"]),
            "replaying": bool(row["replaying"]),
            "replay_target": int(row["replay_target"])
            if row["replay_target"] is not None
            else None,
            "replay_end": int(row["replay_end"])
            if row["replay_end"] is not None
            else None,
            "version": int(row["version"]),
            "updated_at": row["updated_at"],
        }


def state_machine_document() -> dict[str, Any]:
    return {
        "states": list(EVENT_STATES),
        "transitions": {
            (source if source is not None else "INITIAL"): sorted(targets)
            for source, targets in LEGAL_TRANSITIONS.items()
        },
        "trigger_rules": {
            "ACCEPTED": "ingest",
            "DISPATCHED": "poll grants a lease",
            "REDELIVERED": "lease timeout, explicit offset rewind, or restart recovery",
            "ACKED": "consumer ack; repeated ack is idempotent and adds no transition",
            "DEAD_LETTERED": "explicit terminal transition only; not counted as ACKED aggregate input",
        },
    }
