"""Core ordered-delivery and transaction-decision state machine."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from . import codes
from .codes import BusinessError
from .coordinator_client import CoordinatorClient, CoordinatorFailure
from .db import immediate, initialize, json_text, read_conn


INJECTIONS = {
    "commit",
    "rollback",
    "refuse",
    "hang",
    "unknown",
    "duplicate_same",
    "duplicate_conflict",
}


def _row_to_dict(row: sqlite3.Row | None) -> dict | None:
    return dict(row) if row is not None else None


def _loads(value: str | None) -> Any:
    if value is None:
        return None
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return value


class MessageController:
    def __init__(
        self,
        db_path: str | Path,
        coordinator_url: str,
        unreachable_url: str | None = None,
        query_timeout_seconds: float = 0.5,
        clock_ms: Callable[[], int] | None = None,
        auto_renegotiate: bool = True,
    ) -> None:
        self.db_path = str(db_path)
        self.coordinator = CoordinatorClient(
            coordinator_url,
            unreachable_url=unreachable_url,
            default_timeout_seconds=query_timeout_seconds,
        )
        self.query_timeout_seconds = float(query_timeout_seconds)
        self.clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.auto_renegotiate = auto_renegotiate
        self._locks_guard = threading.Lock()
        self._locks: dict[str, threading.RLock] = {}

    # ------------------------------------------------------------------
    # Infrastructure
    # ------------------------------------------------------------------
    def initialize(self) -> None:
        initialize(self.db_path)

    def _now(self) -> int:
        return int(self.clock_ms())

    def _lock(self, key: str) -> threading.RLock:
        with self._locks_guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.RLock()
                self._locks[key] = lock
            return lock

    def _partition_lock(self, topic: str, partition_no: int) -> threading.RLock:
        return self._lock(f"partition:{topic}:{partition_no}")

    def _txn_lock(self, txn_id: str) -> threading.RLock:
        return self._lock(f"txn:{txn_id}")

    def _maybe_crash(self, point: str) -> None:
        if os.environ.get("SEQ_TXN_CRASH_AT") != point:
            return
        marker = os.environ.get("SEQ_TXN_CRASH_MARKER")
        if marker:
            path = Path(marker)
            if path.exists():
                return
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"crashed-at={point}\n", encoding="utf-8")
        os._exit(97)

    def _audit(
        self,
        conn: sqlite3.Connection,
        event_type: str,
        *,
        code: str = codes.OK,
        topic: str | None = None,
        partition_no: int | None = None,
        sequence: int | None = None,
        txn_id: str | None = None,
        consumer_id: str | None = None,
        detail: dict | None = None,
    ) -> None:
        conn.execute(
            "INSERT INTO audit_events(event_type, topic, partition_no, sequence, txn_id, "
            "consumer_id, code, detail_json, created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                event_type,
                topic,
                partition_no,
                sequence,
                txn_id,
                consumer_id,
                code,
                json_text(detail or {}),
                self._now(),
            ),
        )

    def _new_intent(self, kind: str, scope_key: str, payload: dict) -> str:
        intent_id = f"intent-{uuid.uuid4().hex}"
        now = self._now()
        with immediate(self.db_path) as conn:
            conn.execute(
                "INSERT INTO intents(intent_id, kind, scope_key, state, payload_json, "
                "created_at, updated_at) VALUES(?,?,?,'STARTED',?,?,?)",
                (intent_id, kind, scope_key, json_text(payload), now, now),
            )
        return intent_id

    def _finish_intent(
        self, intent_id: str, state: str, note: str | None = None, conn=None
    ) -> None:
        now = self._now()
        sql = (
            "UPDATE intents SET state=?, updated_at=?, completed_at=?, note=COALESCE(?, note) "
            "WHERE intent_id=?"
        )
        params = (state, now, now, note, intent_id)
        if conn is not None:
            conn.execute(sql, params)
            return
        with immediate(self.db_path) as own_conn:
            own_conn.execute(sql, params)

    def _abort_intent(self, intent_id: str, note: str) -> None:
        try:
            self._finish_intent(intent_id, "ABORTED", note)
        except sqlite3.Error:
            pass

    # ------------------------------------------------------------------
    # Topics and publishing
    # ------------------------------------------------------------------
    def create_topic(
        self,
        name: str,
        partition_count: int,
        routing_mode: str,
        routing_salt: str | None = None,
        config: dict | None = None,
    ) -> dict:
        if not name or not name.strip():
            raise BusinessError(codes.E_BAD_REQUEST, "topic name must not be empty")
        if routing_mode not in {"hash", "explicit"}:
            raise BusinessError(
                codes.E_PARTITION_INVALID,
                "routing mode must be 'hash' or 'explicit'",
                {"routing_mode": routing_mode},
            )
        if isinstance(partition_count, bool) or not isinstance(partition_count, int):
            raise BusinessError(codes.E_PARTITION_INVALID, "partition_count must be an integer")
        if partition_count < 1 or partition_count > 4096:
            raise BusinessError(
                codes.E_PARTITION_INVALID,
                "partition_count must be in [1, 4096]",
                {"partition_count": partition_count},
            )
        salt = routing_salt or hashlib.sha256(f"seqtxn:{name}".encode()).hexdigest()[:16]
        topic_config = {
            "name": name,
            "partition_count": partition_count,
            "routing_mode": routing_mode,
            "routing_salt": salt,
            **(config or {}),
        }
        now = self._now()
        with immediate(self.db_path) as conn:
            existing = conn.execute("SELECT * FROM topics WHERE name=?", (name,)).fetchone()
            if existing:
                comparison = (
                    existing["partition_count"],
                    existing["routing_mode"],
                    existing["routing_salt"],
                )
                if comparison == (partition_count, routing_mode, salt):
                    return {**dict(existing), "config": _loads(existing["config_json"]), "duplicate": True}
                raise BusinessError(
                    codes.E_TOPIC_CONFLICT,
                    "topic already exists with a different partition/routing configuration",
                    {"topic": name},
                )
            conn.execute(
                "INSERT INTO topics(name, partition_count, routing_mode, routing_salt, "
                "config_json, created_at) VALUES(?,?,?,?,?,?)",
                (name, partition_count, routing_mode, salt, json_text(topic_config), now),
            )
            for partition_no in range(partition_count):
                conn.execute(
                    "INSERT INTO partitions(topic, partition_no) VALUES(?,?)",
                    (name, partition_no),
                )
            self._audit(
                conn,
                "TOPIC_CREATED",
                topic=name,
                detail=topic_config,
            )
        return {**topic_config, "created_at": now, "duplicate": False}

    def _topic_or_error(self, conn, topic: str) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM topics WHERE name=?", (topic,)).fetchone()
        if not row:
            raise BusinessError(codes.E_NOT_FOUND, "topic not found", {"topic": topic})
        return row

    def _route(self, conn, topic: str, partition: Any, partition_key: Any) -> int:
        topic_row = self._topic_or_error(conn, topic)
        count = int(topic_row["partition_count"])
        mode = topic_row["routing_mode"]
        if mode == "explicit":
            if partition is None or isinstance(partition, bool) or not isinstance(partition, int):
                raise BusinessError(
                    codes.E_PARTITION_INVALID,
                    "explicit-routing topic requires an integer partition",
                    {"topic": topic, "partition": partition},
                )
            if partition < 0 or partition >= count:
                raise BusinessError(
                    codes.E_PARTITION_INVALID,
                    "partition is outside the topic range",
                    {"topic": topic, "partition": partition, "partition_count": count},
                )
            return partition
        if not isinstance(partition_key, str) or not partition_key:
            raise BusinessError(
                codes.E_PARTITION_INVALID,
                "hash-routing topic requires a non-empty partition_key",
                {"topic": topic, "partition_key": partition_key},
            )
        salted = f"{topic_row['routing_salt']}\0{partition_key}".encode("utf-8")
        digest = hashlib.sha256(salted).digest()
        return int.from_bytes(digest[:8], "big") % count

    def _canonical_body(self, body: Any) -> str:
        return json_text(body)

    def publish(
        self,
        topic: str,
        producer_id: str,
        client_sequence: Any,
        body: Any,
        partition: int | None = None,
        partition_key: str | None = None,
        txn_id: str | None = None,
    ) -> dict:
        if not producer_id:
            raise BusinessError(codes.E_BAD_REQUEST, "producer_id must not be empty")
        if client_sequence is None or str(client_sequence) == "":
            raise BusinessError(codes.E_BAD_REQUEST, "client_sequence must not be empty")
        client_sequence = str(client_sequence)
        if txn_id is not None and not str(txn_id):
            raise BusinessError(codes.E_BAD_REQUEST, "txn_id must not be empty")
        txn_id = str(txn_id) if txn_id is not None else None
        body_text = self._canonical_body(body)
        body_digest = hashlib.sha256(body_text.encode("utf-8")).hexdigest()

        with read_conn(self.db_path) as conn:
            partition_no = self._route(conn, topic, partition, partition_key)

        intent_id = self._new_intent(
            "PRODUCE",
            f"producer:{producer_id}:{client_sequence}",
            {
                "topic": topic,
                "partition": partition_no,
                "producer_id": producer_id,
                "client_sequence": client_sequence,
                "txn_id": txn_id,
                "body_digest": body_digest,
            },
        )
        self._maybe_crash("produce_after_intent")
        try:
            with immediate(self.db_path) as conn:
                self._topic_or_error(conn, topic)
                existing = conn.execute(
                    "SELECT * FROM messages WHERE producer_id=? AND client_sequence=?",
                    (producer_id, client_sequence),
                ).fetchone()
                if existing:
                    same = (
                        existing["topic"],
                        existing["partition_no"],
                        existing["txn_id"],
                        existing["body_digest"],
                    ) == (topic, partition_no, txn_id, body_digest)
                    self._finish_intent(intent_id, "ABORTED", "idempotent replay", conn=conn)
                    if not same:
                        raise BusinessError(
                            codes.E_IDEMPOTENCY_CONFLICT,
                            "producer/client_sequence was already used with different content",
                            {
                                "producer_id": producer_id,
                                "client_sequence": client_sequence,
                                "existing_message": self._message_dict(existing),
                            },
                        )
                    return {
                        **self._message_dict(existing),
                        "duplicate": True,
                    }

                if txn_id:
                    txn = conn.execute(
                        "SELECT state FROM transactions WHERE txn_id=?", (txn_id,)
                    ).fetchone()
                    if txn and txn["state"] != "PENDING":
                        self._finish_intent(intent_id, "ABORTED", "transaction finalized", conn=conn)
                        raise BusinessError(
                            codes.E_TXN_FINALIZED,
                            "cannot append a message to a finalized transaction",
                            {"txn_id": txn_id, "state": txn["state"]},
                        )
                    if not txn:
                        conn.execute(
                            "INSERT INTO transactions(txn_id, state, idempotency_key, created_at) "
                            "VALUES(?,'PENDING',?,?)",
                            (txn_id, f"txn:{txn_id}", self._now()),
                        )

                conn.execute(
                    "UPDATE partitions SET last_sequence=last_sequence+1 WHERE topic=? AND partition_no=?",
                    (topic, partition_no),
                )
                partition_row = conn.execute(
                    "SELECT last_sequence FROM partitions WHERE topic=? AND partition_no=?",
                    (topic, partition_no),
                ).fetchone()
                sequence = int(partition_row["last_sequence"])
                now = self._now()
                state = "PENDING" if txn_id else "COMMITTED"
                cursor = conn.execute(
                    "INSERT INTO messages(topic, partition_no, sequence, producer_id, "
                    "client_sequence, txn_id, body, body_digest, produced_at, state, visible_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        topic,
                        partition_no,
                        sequence,
                        producer_id,
                        client_sequence,
                        txn_id,
                        body_text,
                        body_digest,
                        now,
                        state,
                        now if state == "COMMITTED" else None,
                    ),
                )
                message_id = int(cursor.lastrowid)
                if txn_id:
                    conn.execute(
                        "INSERT INTO pending_messages(message_id, txn_id, topic, partition_no, "
                        "sequence, created_at) VALUES(?,?,?,?,?,?)",
                        (message_id, txn_id, topic, partition_no, sequence, now),
                    )
                state_row = self._refresh_partition_state(conn, topic, partition_no)
                self._audit(
                    conn,
                    "MESSAGE_PRODUCED",
                    topic=topic,
                    partition_no=partition_no,
                    sequence=sequence,
                    txn_id=txn_id,
                    detail={
                        "producer_id": producer_id,
                        "client_sequence": client_sequence,
                        "state": state,
                        "body_digest": body_digest,
                    },
                )
                self._finish_intent(intent_id, "COMMITTED", None, conn=conn)
                message = conn.execute(
                    "SELECT * FROM messages WHERE message_id=?", (message_id,)
                ).fetchone()
                return {
                    **self._message_dict(message),
                    "partition_state": state_row,
                    "duplicate": False,
                }
        except BusinessError:
            self._abort_intent(intent_id, "business validation rejected publish")
            raise
        except BaseException:
            self._abort_intent(intent_id, "publish failed")
            raise

    # ------------------------------------------------------------------
    # Lease / acknowledgement
    # ------------------------------------------------------------------
    def _effective_lease(self, lease: sqlite3.Row | None, now: int) -> dict | None:
        if not lease:
            return None
        data = dict(lease)
        if data["state"] == "ACTIVE" and data["expires_at"] <= now:
            data["effective_state"] = "EXPIRED"
            data["active"] = False
        else:
            data["effective_state"] = data["state"]
            data["active"] = data["state"] == "ACTIVE"
        return data

    def _refresh_partition_state(self, conn, topic: str, partition_no: int) -> dict:
        row = conn.execute(
            "SELECT * FROM partitions WHERE topic=? AND partition_no=?",
            (topic, partition_no),
        ).fetchone()
        if not row:
            raise BusinessError(
                codes.E_PARTITION_INVALID,
                "partition does not exist",
                {"topic": topic, "partition": partition_no},
            )
        visibility = conn.execute(
            "SELECT COALESCE(MAX(sequence), 0) AS seq FROM messages "
            "WHERE topic=? AND partition_no=? AND state!='PENDING'",
            (topic, partition_no),
        ).fetchone()["seq"]
        messages = conn.execute(
            "SELECT sequence, state, acked_at FROM messages "
            "WHERE topic=? AND partition_no=? ORDER BY sequence",
            (topic, partition_no),
        ).fetchall()
        acked_through = 0
        for message in messages:
            if message["state"] == "ROLLED_BACK" or message["acked_at"] is not None:
                acked_through = int(message["sequence"])
            else:
                break
        head = conn.execute(
            "SELECT sequence, state FROM messages WHERE topic=? AND partition_no=? "
            "AND state!='ROLLED_BACK' AND acked_at IS NULL ORDER BY sequence LIMIT 1",
            (topic, partition_no),
        ).fetchone()
        head_sequence = int(head["sequence"]) if head else None
        delivery_ready = bool(head and head["state"] == "COMMITTED")
        delivery_sequence = head_sequence if delivery_ready else None
        conn.execute(
            "UPDATE partitions SET visibility_sequence=MAX(visibility_sequence, ?), "
            "head_sequence=?, delivery_sequence=?, delivery_ready=?, acked_through=? "
            "WHERE topic=? AND partition_no=?",
            (
                int(visibility or 0),
                head_sequence,
                delivery_sequence,
                1 if delivery_ready else 0,
                acked_through,
                topic,
                partition_no,
            ),
        )
        updated = conn.execute(
            "SELECT * FROM partitions WHERE topic=? AND partition_no=?",
            (topic, partition_no),
        ).fetchone()
        return dict(updated)

    def claim(
        self,
        topic: str,
        partition_no: int,
        consumer_id: str,
        visibility_timeout_ms: int = 30_000,
    ) -> dict:
        if not consumer_id:
            raise BusinessError(codes.E_BAD_REQUEST, "consumer_id must not be empty")
        if (
            isinstance(visibility_timeout_ms, bool)
            or not isinstance(visibility_timeout_ms, int)
            or visibility_timeout_ms <= 0
        ):
            raise BusinessError(codes.E_BAD_REQUEST, "visibility_timeout_ms must be positive")
        with read_conn(self.db_path) as conn:
            self._topic_or_error(conn, topic)
            partition = conn.execute(
                "SELECT * FROM partitions WHERE topic=? AND partition_no=?",
                (topic, partition_no),
            ).fetchone()
            if not partition:
                raise BusinessError(
                    codes.E_PARTITION_INVALID,
                    "partition is outside the topic range",
                    {"topic": topic, "partition": partition_no},
                )

        key = f"partition:{topic}:{partition_no}"
        with self._lock(key):
            intent_id = self._new_intent(
                "LEASE_ACQUIRE",
                key,
                {
                    "topic": topic,
                    "partition": partition_no,
                    "consumer_id": consumer_id,
                    "visibility_timeout_ms": visibility_timeout_ms,
                },
            )
            self._maybe_crash("lease_after_intent")
            lease_id = f"lease-{uuid.uuid4().hex}"
            lease_token = uuid.uuid4().hex
            result: dict
            try:
                with immediate(self.db_path) as conn:
                    now = self._now()
                    active = conn.execute(
                        "SELECT * FROM leases WHERE topic=? AND partition_no=? AND state='ACTIVE'",
                        (topic, partition_no),
                    ).fetchone()
                    if active and int(active["expires_at"]) <= now:
                        conn.execute(
                            "UPDATE leases SET state='EXPIRED', ended_at=?, "
                            "end_reason='visibility_timeout_materialized' WHERE lease_id=?",
                            (now, active["lease_id"]),
                        )
                        self._audit(
                            conn,
                            "LEASE_EXPIRED",
                            topic=topic,
                            partition_no=partition_no,
                            sequence=int(active["sequence"]),
                            consumer_id=active["consumer_id"],
                            detail={"lease_id": active["lease_id"], "attempt": active["attempt"]},
                        )
                        self._refresh_partition_state(conn, topic, partition_no)
                        active = None
                    if active:
                        self._finish_intent(
                            intent_id, "ABORTED", "partition lease is held", conn=conn
                        )
                        result = {
                            "message": None,
                            "reason": "LEASE_HELD",
                            "code": codes.E_LEASE_HELD,
                            "lease": self._effective_lease(active, now),
                        }
                    else:
                        head = conn.execute(
                            "SELECT * FROM messages WHERE topic=? AND partition_no=? "
                            "AND state!='ROLLED_BACK' AND acked_at IS NULL "
                            "ORDER BY sequence LIMIT 1",
                            (topic, partition_no),
                        ).fetchone()
                        if not head:
                            self._finish_intent(intent_id, "ABORTED", "empty partition", conn=conn)
                            result = {"message": None, "reason": "EMPTY"}
                        elif head["state"] == "PENDING":
                            self._finish_intent(
                                intent_id, "ABORTED", "head transaction pending", conn=conn
                            )
                            state = self._refresh_partition_state(conn, topic, partition_no)
                            result = {
                                "message": None,
                                "reason": "HEAD_PENDING_TRANSACTION",
                                "blocked_sequence": int(head["sequence"]),
                                "txn_id": head["txn_id"],
                                "partition_state": state,
                            }
                        else:
                            attempt = conn.execute(
                                "SELECT COUNT(*) AS n FROM leases WHERE topic=? AND "
                                "partition_no=? AND sequence=?",
                                (topic, partition_no, head["sequence"]),
                            ).fetchone()["n"] + 1
                            lease_id = f"lease-{uuid.uuid4().hex}"
                            conn.execute(
                                "INSERT INTO leases(lease_id, intent_id, topic, partition_no, "
                                "sequence, consumer_id, lease_token, attempt, acquired_at, "
                                "expires_at, state) VALUES(?,?,?,?,?,?,?,?,?,?,'ACTIVE')",
                                (
                                    lease_id,
                                    intent_id,
                                    topic,
                                    partition_no,
                                    head["sequence"],
                                    consumer_id,
                                    lease_token,
                                    attempt,
                                    now,
                                    now + int(visibility_timeout_ms),
                                ),
                            )
                            conn.execute(
                                "UPDATE partitions SET lease_epoch=lease_epoch+1 "
                                "WHERE topic=? AND partition_no=?",
                                (topic, partition_no),
                            )
                            self._audit(
                                conn,
                                "LEASE_GRANTED",
                                topic=topic,
                                partition_no=partition_no,
                                sequence=int(head["sequence"]),
                                consumer_id=consumer_id,
                                detail={
                                    "lease_id": lease_id,
                                    "attempt": attempt,
                                    "expires_at": now + int(visibility_timeout_ms),
                                },
                            )
                            state = self._refresh_partition_state(conn, topic, partition_no)
                            result = {
                                "message": self._message_dict(head),
                                "reason": None,
                                "lease_token": lease_token,
                                "lease_id": lease_id,
                                "consumer_id": consumer_id,
                                "attempt": attempt,
                                "acquired_at": now,
                                "expires_at": now + int(visibility_timeout_ms),
                                "partition_state": state,
                            }
                if result.get("message") is not None:
                    self._maybe_crash("lease_after_effect")
                    self._finish_intent(intent_id, "COMMITTED", None)
                else:
                    self._finish_intent(intent_id, "ABORTED", "no lease granted")
                if result.get("code") == codes.E_LEASE_HELD:
                    raise BusinessError(
                        codes.E_LEASE_HELD,
                        "another consumer holds the partition lease",
                        result.get("lease"),
                    )
                return result
            except BusinessError:
                self._abort_intent(intent_id, "lease rejected")
                raise
            except BaseException:
                self._abort_intent(intent_id, "lease operation failed")
                raise

    def _lease_partition_lookup(self, token: str) -> sqlite3.Row:
        with read_conn(self.db_path) as conn:
            lease = conn.execute(
                "SELECT * FROM leases WHERE lease_token=?", (token,)
            ).fetchone()
        if not lease:
            raise BusinessError(codes.E_LEASE_INVALID, "lease token is unknown")
        return lease

    def _materialize_due_lease_expiry(self, lease_token: str, topic: str, partition_no: int) -> None:
        """Persist an expired lease before an ack/release decision.

        Claim already performs this transition atomically.  Doing it here as a
        separate short write means a late ack cannot leave a stale ACTIVE row
        after returning E_LEASE_EXPIRED.
        """
        with immediate(self.db_path) as conn:
            now = self._now()
            lease = conn.execute(
                "SELECT * FROM leases WHERE lease_token=? AND state='ACTIVE'",
                (lease_token,),
            ).fetchone()
            if lease and int(lease["expires_at"]) <= now:
                conn.execute(
                    "UPDATE leases SET state='EXPIRED', ended_at=?, "
                    "end_reason='visibility_timeout_materialized' WHERE lease_id=?",
                    (now, lease["lease_id"]),
                )
                self._refresh_partition_state(conn, topic, partition_no)
                self._audit(
                    conn,
                    "LEASE_EXPIRED",
                    topic=topic,
                    partition_no=partition_no,
                    sequence=int(lease["sequence"]),
                    consumer_id=lease["consumer_id"],
                    detail={"lease_id": lease["lease_id"], "attempt": lease["attempt"]},
                )

    def ack(
        self, lease_token: str, sequence: int | None = None, consumer_id: str | None = None
    ) -> dict:
        lease_lookup = self._lease_partition_lookup(lease_token)
        topic = lease_lookup["topic"]
        partition_no = int(lease_lookup["partition_no"])
        key = f"partition:{topic}:{partition_no}"
        with self._lock(key):
            self._materialize_due_lease_expiry(lease_token, topic, partition_no)
            intent_id = self._new_intent(
                "ACK",
                key,
                {
                    "lease_token": lease_token,
                    "sequence": sequence,
                    "consumer_id": consumer_id,
                },
            )
            self._maybe_crash("ack_after_intent")
            try:
                with immediate(self.db_path) as conn:
                    now = self._now()
                    lease = conn.execute(
                        "SELECT * FROM leases WHERE lease_token=?", (lease_token,)
                    ).fetchone()
                    if not lease:
                        self._finish_intent(intent_id, "ABORTED", "unknown lease", conn=conn)
                        raise BusinessError(codes.E_LEASE_INVALID, "lease token is unknown")
                    message = conn.execute(
                        "SELECT * FROM messages WHERE topic=? AND partition_no=? AND sequence=?",
                        (topic, partition_no, lease["sequence"]),
                    ).fetchone()
                    if lease["state"] == "ACKED" or (message and message["acked_at"] is not None):
                        self._finish_intent(intent_id, "ABORTED", "duplicate ack", conn=conn)
                        raise BusinessError(
                            codes.E_DUPLICATE_ACK,
                            "message was already acknowledged",
                            {
                                "topic": topic,
                                "partition": partition_no,
                                "sequence": int(lease["sequence"]),
                            },
                        )
                    if consumer_id and consumer_id != lease["consumer_id"]:
                        self._finish_intent(intent_id, "ABORTED", "consumer mismatch", conn=conn)
                        raise BusinessError(
                            codes.E_LEASE_INVALID,
                            "lease is owned by another consumer",
                            {"owner": lease["consumer_id"]},
                        )
                    if sequence is not None and int(sequence) != int(lease["sequence"]):
                        self._finish_intent(intent_id, "ABORTED", "sequence mismatch", conn=conn)
                        raise BusinessError(
                            codes.E_SEQUENCE_MISMATCH,
                            "ack sequence does not match the leased head",
                            {"expected": int(lease["sequence"]), "received": sequence},
                        )
                    if lease["state"] == "ACTIVE" and int(lease["expires_at"]) <= now:
                        conn.execute(
                            "UPDATE leases SET state='EXPIRED', ended_at=?, "
                            "end_reason='expired_before_ack' WHERE lease_id=?",
                            (now, lease["lease_id"]),
                        )
                        self._refresh_partition_state(conn, topic, partition_no)
                        self._finish_intent(intent_id, "ABORTED", "lease expired", conn=conn)
                        raise BusinessError(
                            codes.E_LEASE_EXPIRED,
                            "lease expired; message will be redelivered",
                            {"sequence": int(lease["sequence"])},
                        )
                    if lease["state"] != "ACTIVE":
                        self._finish_intent(
                            intent_id, "ABORTED", f"lease state {lease['state']}", conn=conn
                        )
                        raise BusinessError(
                            codes.E_LEASE_EXPIRED,
                            f"lease is not active ({lease['state']})",
                            {"sequence": int(lease["sequence"])},
                        )
                    conn.execute(
                        "UPDATE leases SET state='ACKED', ended_at=?, end_reason='ack' "
                        "WHERE lease_id=?",
                        (now, lease["lease_id"]),
                    )
                    conn.execute(
                        "UPDATE messages SET acked_at=?, ack_lease_id=? "
                        "WHERE topic=? AND partition_no=? AND sequence=?",
                        (now, lease["lease_id"], topic, partition_no, lease["sequence"]),
                    )
                    state = self._refresh_partition_state(conn, topic, partition_no)
                    self._audit(
                        conn,
                        "MESSAGE_ACKED",
                        topic=topic,
                        partition_no=partition_no,
                        sequence=int(lease["sequence"]),
                        consumer_id=lease["consumer_id"],
                        detail={"lease_id": lease["lease_id"], "attempt": lease["attempt"]},
                    )
                    self._finish_intent(intent_id, "COMMITTED", None, conn=conn)
                    return {
                        "topic": topic,
                        "partition": partition_no,
                        "sequence": int(lease["sequence"]),
                        "partition_state": state,
                    }
            except BusinessError:
                self._abort_intent(intent_id, "ack rejected")
                raise
            except BaseException:
                self._abort_intent(intent_id, "ack failed")
                raise

    def release(self, lease_token: str, reason: str = "consumer_release") -> dict:
        lease_lookup = self._lease_partition_lookup(lease_token)
        topic = lease_lookup["topic"]
        partition_no = int(lease_lookup["partition_no"])
        key = f"partition:{topic}:{partition_no}"
        with self._lock(key):
            self._materialize_due_lease_expiry(lease_token, topic, partition_no)
            intent_id = self._new_intent(
                "LEASE_RELEASE", key, {"lease_token": lease_token, "reason": reason}
            )
            try:
                with immediate(self.db_path) as conn:
                    now = self._now()
                    lease = conn.execute(
                        "SELECT * FROM leases WHERE lease_token=?", (lease_token,)
                    ).fetchone()
                    if not lease or lease["state"] != "ACTIVE":
                        self._finish_intent(intent_id, "ABORTED", "lease not active", conn=conn)
                        raise BusinessError(
                            codes.E_LEASE_INVALID, "lease is not active", {"sequence": lease["sequence"] if lease else None}
                        )
                    conn.execute(
                        "UPDATE leases SET state='RELEASED', ended_at=?, end_reason=? "
                        "WHERE lease_id=?",
                        (now, reason, lease["lease_id"]),
                    )
                    state = self._refresh_partition_state(conn, topic, partition_no)
                    self._audit(
                        conn,
                        "LEASE_RELEASED",
                        topic=topic,
                        partition_no=partition_no,
                        sequence=int(lease["sequence"]),
                        consumer_id=lease["consumer_id"],
                        detail={"lease_id": lease["lease_id"], "reason": reason},
                    )
                    self._finish_intent(intent_id, "COMMITTED", None, conn=conn)
                    return {
                        "topic": topic,
                        "partition": partition_no,
                        "sequence": int(lease["sequence"]),
                        "partition_state": state,
                    }
            except BusinessError:
                self._abort_intent(intent_id, "release rejected")
                raise
            except BaseException:
                self._abort_intent(intent_id, "release failed")
                raise

    # ------------------------------------------------------------------
    # Transaction decision
    # ------------------------------------------------------------------
    def _txn_row(self, conn, txn_id: str) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM transactions WHERE txn_id=?", (txn_id,)).fetchone()
        if not row:
            raise BusinessError(codes.E_TXN_NOT_FOUND, "transaction not found", {"txn_id": txn_id})
        return row

    def _query_attempt(self, conn, txn_id: str, key: str) -> int:
        return int(
            conn.execute(
                "SELECT COUNT(*) AS n FROM txn_queries WHERE txn_id=? AND idempotency_key=?",
                (txn_id, key),
            ).fetchone()["n"]
        ) + 1

    def _record_query(
        self,
        txn_id: str,
        key: str,
        attempt: int,
        started_at: int,
        code: str,
        source: str,
        detail: dict,
        outcome: str | None = None,
    ) -> None:
        with immediate(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO txn_queries(query_id, txn_id, idempotency_key, "
                "attempt, requested_at, finished_at, outcome, result_code, source, detail_json) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    f"query-{uuid.uuid4().hex}",
                    txn_id,
                    key,
                    attempt,
                    started_at,
                    self._now(),
                    outcome,
                    code,
                    source,
                    json_text(detail),
                ),
            )

    def _record_fault(
        self,
        txn_id: str | None,
        injection: str,
        conditions: dict,
        observed_code: str,
        observed: dict,
    ) -> None:
        with immediate(self.db_path) as conn:
            conn.execute(
                "INSERT INTO fault_events(txn_id, injection, conditions_json, observed_code, "
                "observed_json, created_at) VALUES(?,?,?,?,?,?)",
                (
                    txn_id,
                    injection,
                    json_text(conditions),
                    observed_code,
                    json_text(observed),
                    self._now(),
                ),
            )

    def _record_conflict(
        self,
        txn_id: str,
        existing: str,
        incoming: str,
        key: str,
        source: str,
        detail: dict,
        conn=None,
    ) -> None:
        sql = (
            "INSERT INTO txn_conflicts(txn_id, existing_outcome, incoming_outcome, "
            "idempotency_key, source, detected_at, detail_json) VALUES(?,?,?,?,?,?,?)"
        )
        params = (txn_id, existing, incoming, key, source, self._now(), json_text(detail))
        if conn is not None:
            conn.execute(sql, params)
        else:
            with immediate(self.db_path) as own_conn:
                own_conn.execute(sql, params)

    def _force_injection_override(self, injection: str | None, existing: str) -> tuple[str | None, str | None]:
        if injection == "duplicate_same":
            return "commit" if existing == "COMMITTED" else "rollback", "duplicate_same"
        if injection == "duplicate_conflict":
            return "rollback" if existing == "COMMITTED" else "commit", "duplicate_conflict"
        return injection, injection

    def resolve_transaction(
        self,
        txn_id: str,
        idempotency_key: str | None = None,
        timeout_ms: int | None = None,
        injection: str | None = None,
        force: bool = False,
        source: str = "api",
    ) -> dict:
        if injection is not None and injection not in INJECTIONS:
            raise BusinessError(
                codes.E_BAD_REQUEST,
                "unknown injection",
                {"allowed": sorted(INJECTIONS)},
            )
        if timeout_ms is not None and (
            isinstance(timeout_ms, bool) or not isinstance(timeout_ms, int) or timeout_ms <= 0
        ):
            raise BusinessError(codes.E_BAD_REQUEST, "timeout_ms must be positive")
        key = idempotency_key or f"txn:{txn_id}"
        timeout_seconds = (
            float(timeout_ms) / 1000.0 if timeout_ms is not None else self.query_timeout_seconds
        )

        with self._txn_lock(txn_id):
            with read_conn(self.db_path) as conn:
                txn = self._txn_row(conn, txn_id)
                existing_state = txn["state"]
                existing_outcome = (
                    "COMMIT" if existing_state == "COMMITTED" else "ROLLBACK"
                    if existing_state == "ROLLED_BACK"
                    else None
                )
                conditions = {
                    "txn_state": existing_state,
                    "idempotency_key": key,
                    "force": force,
                    "injection": injection,
                }

            if existing_outcome is not None and not force:
                result = self.transaction_detail(txn_id)
                result["replayed"] = True
                result["message"] = "transaction already has a persisted conclusion"
                if injection:
                    self._record_fault(
                        txn_id,
                        injection,
                        conditions,
                        codes.E_TXN_REPLAY,
                        {"replayed": True, "state": existing_state},
                    )
                return result

            if existing_outcome is not None and injection in {"duplicate_same", "duplicate_conflict"}:
                override, effective_injection = self._force_injection_override(injection, existing_state)
                try:
                    coordinator_result = self.coordinator.query(
                        txn_id,
                        key,
                        timeout_seconds=timeout_seconds,
                        injection=override,
                        force=True,
                    )
                except CoordinatorFailure as failure:
                    self._record_fault(
                        txn_id,
                        injection,
                        conditions,
                        failure.code,
                        {"message": failure.message, "details": failure.details},
                    )
                    raise BusinessError(failure.code, failure.message, failure.details) from failure
                if coordinator_result.outcome == existing_outcome:
                    self._record_fault(
                        txn_id,
                        injection,
                        conditions,
                        codes.E_DUPLICATE_ACK,
                        {
                            "existing": existing_outcome,
                            "incoming": coordinator_result.outcome,
                            "unchanged": True,
                        },
                    )
                    raise BusinessError(
                        codes.E_DUPLICATE_ACK,
                        "duplicate coordinator conclusion ignored; first conclusion retained",
                        {
                            "txn_id": txn_id,
                            "outcome": existing_outcome,
                            "injection": effective_injection,
                        },
                    )
                self._record_conflict(
                    txn_id,
                    existing_outcome,
                    coordinator_result.outcome,
                    key,
                    coordinator_result.source,
                    {"injection": injection, "query_response": coordinator_result.response},
                )
                self._record_fault(
                    txn_id,
                    injection,
                    conditions,
                    codes.E_TXN_CONFLICT,
                    {
                        "existing": existing_outcome,
                        "incoming": coordinator_result.outcome,
                        "retained": existing_outcome,
                    },
                )
                raise BusinessError(
                    codes.E_TXN_CONFLICT,
                    "coordinator conclusion conflicts with the persisted first conclusion",
                    {
                        "txn_id": txn_id,
                        "existing": existing_outcome,
                        "incoming": coordinator_result.outcome,
                    },
                )

            if existing_outcome is not None and force:
                override = None
                if injection in {"commit", "rollback", "unknown", "hang", "refuse"}:
                    override = injection
                try:
                    coordinator_result = self.coordinator.query(
                        txn_id,
                        key,
                        timeout_seconds=timeout_seconds,
                        injection=override,
                        force=True,
                    )
                except CoordinatorFailure as failure:
                    self._record_fault(
                        txn_id,
                        injection or "force",
                        conditions,
                        failure.code,
                        {"message": failure.message, "details": failure.details},
                    )
                    raise BusinessError(failure.code, failure.message, failure.details) from failure
                if coordinator_result.outcome != existing_outcome:
                    self._record_conflict(
                        txn_id,
                        existing_outcome,
                        coordinator_result.outcome,
                        key,
                        coordinator_result.source,
                        {"injection": injection, "query_response": coordinator_result.response},
                    )
                    if injection:
                        self._record_fault(
                            txn_id,
                            injection,
                            conditions,
                            codes.E_TXN_CONFLICT,
                            {"existing": existing_outcome, "incoming": coordinator_result.outcome},
                        )
                    raise BusinessError(
                        codes.E_TXN_CONFLICT,
                        "coordinator conclusion conflicts with the persisted first conclusion",
                        {
                            "txn_id": txn_id,
                            "existing": existing_outcome,
                            "incoming": coordinator_result.outcome,
                        },
                    )
                if injection:
                    self._record_fault(
                        txn_id,
                        injection,
                        conditions,
                        codes.E_DUPLICATE_ACK,
                        {"existing": existing_outcome, "incoming": coordinator_result.outcome},
                    )
                    raise BusinessError(
                        codes.E_DUPLICATE_ACK,
                        "duplicate coordinator conclusion ignored; first conclusion retained",
                        {"txn_id": txn_id, "outcome": existing_outcome},
                    )
                result = self.transaction_detail(txn_id)
                result["replayed"] = True
                return result

            if injection in {"duplicate_same", "duplicate_conflict"}:
                self._record_fault(
                    txn_id,
                    injection,
                    conditions,
                    codes.E_FAULT_PRECONDITION,
                    {"reason": "a final conclusion must already exist"},
                )
                raise BusinessError(
                    codes.E_FAULT_PRECONDITION,
                    "duplicate-conclusion injection requires an existing final decision",
                    {"txn_id": txn_id, "state": existing_state},
                )

            intent_id = self._new_intent(
                "TXN_RESOLVE",
                f"txn:{txn_id}",
                {
                    "txn_id": txn_id,
                    "idempotency_key": key,
                    "injection": injection,
                    "source": source,
                },
            )
            self._maybe_crash("txn_after_intent")
            started_at = self._now()
            attempt = 0
            concurrent_conflict: dict | None = None
            try:
                with read_conn(self.db_path) as conn:
                    attempt = self._query_attempt(conn, txn_id, key)
                try:
                    coordinator_result = self.coordinator.query(
                        txn_id,
                        key,
                        timeout_seconds=timeout_seconds,
                        injection=injection if injection not in {"duplicate_same", "duplicate_conflict"} else None,
                        force=force,
                    )
                except CoordinatorFailure as failure:
                    self._record_query(
                        txn_id,
                        key,
                        attempt,
                        started_at,
                        failure.code,
                        source,
                        {"message": failure.message, "details": failure.details},
                    )
                    self._finish_intent(intent_id, "ABORTED", f"coordinator {failure.code}")
                    if injection:
                        self._record_fault(
                            txn_id,
                            injection,
                            conditions,
                            failure.code,
                            {"message": failure.message, "details": failure.details},
                        )
                    raise BusinessError(failure.code, failure.message, failure.details) from failure

                self._maybe_crash("txn_after_remote")
                decision_source = f"coordinator:{self.coordinator.base_url}:{coordinator_result.source}"
                try:
                    with immediate(self.db_path) as conn:
                        current = self._txn_row(conn, txn_id)
                        now = self._now()
                        if current["state"] != "PENDING":
                            current_outcome = (
                                "COMMIT" if current["state"] == "COMMITTED" else "ROLLBACK"
                            )
                            if current_outcome != coordinator_result.outcome:
                                concurrent_conflict = {
                                    "existing": current_outcome,
                                    "incoming": coordinator_result.outcome,
                                    "source": coordinator_result.source,
                                    "response": coordinator_result.response,
                                }
                                self._record_conflict(
                                    txn_id,
                                    current_outcome,
                                    coordinator_result.outcome,
                                    key,
                                    coordinator_result.source,
                                    {"concurrent_decision": True},
                                    conn=conn,
                                )
                                self._finish_intent(
                                    intent_id, "ABORTED", "concurrent conclusion conflict", conn=conn
                                )
                                raise BusinessError(
                                    codes.E_TXN_CONFLICT,
                                    "concurrent coordinator conclusion conflicts with first decision",
                                    {
                                        "existing": current_outcome,
                                        "incoming": coordinator_result.outcome,
                                    },
                                )
                            self._finish_intent(
                                intent_id, "COMMITTED", "concurrent same decision", conn=conn
                            )
                            result = dict(current)
                            result["replayed"] = True
                            return result

                        message_rows = conn.execute(
                            "SELECT topic, partition_no, sequence FROM messages WHERE txn_id=? "
                            "ORDER BY topic, partition_no, sequence",
                            (txn_id,),
                        ).fetchall()
                        final_state = (
                            "COMMITTED" if coordinator_result.outcome == "COMMIT" else "ROLLED_BACK"
                        )
                        conn.execute(
                            "UPDATE transactions SET state=?, decision_source=?, decided_at=?, "
                            "query_count=query_count+1, last_outcome=? WHERE txn_id=?",
                            (final_state, decision_source, now, coordinator_result.outcome, txn_id),
                        )
                        conn.execute(
                            "UPDATE messages SET state=?, visible_at=?, decision_source=? "
                            "WHERE txn_id=?",
                            (
                                final_state,
                                now if final_state == "COMMITTED" else None,
                                decision_source,
                                txn_id,
                            ),
                        )
                        conn.execute("DELETE FROM pending_messages WHERE txn_id=?", (txn_id,))
                        conn.execute(
                            "INSERT INTO coordinator_cache(txn_id, idempotency_key, outcome, "
                            "source, response_json, stored_at) VALUES(?,?,?,?,?,?) "
                            "ON CONFLICT(txn_id, idempotency_key) DO NOTHING",
                            (
                                txn_id,
                                key,
                                coordinator_result.outcome,
                                decision_source,
                                json_text(coordinator_result.response),
                                now,
                            ),
                        )
                        conn.execute(
                            "INSERT OR REPLACE INTO txn_queries(query_id, txn_id, idempotency_key, "
                            "attempt, requested_at, finished_at, outcome, result_code, source, "
                            "detail_json) VALUES(?,?,?,?,?,?,?,?,?,?)",
                            (
                                f"query-{uuid.uuid4().hex}",
                                txn_id,
                                key,
                                attempt,
                                started_at,
                                now,
                                coordinator_result.outcome,
                                codes.OK,
                                decision_source,
                                json_text(coordinator_result.response),
                            ),
                        )
                        partitions = sorted(
                            {(row["topic"], int(row["partition_no"])) for row in message_rows}
                        )
                        partition_states = []
                        for topic, partition_no in partitions:
                            state = self._refresh_partition_state(conn, topic, partition_no)
                            partition_states.append(state)
                            self._audit(
                                conn,
                                "PARTITION_STATE_UPDATED",
                                topic=topic,
                                partition_no=partition_no,
                                txn_id=txn_id,
                                detail={"state": state, "reason": "transaction_decision"},
                            )
                        self._audit(
                            conn,
                            "TXN_DECIDED",
                            txn_id=txn_id,
                            detail={
                                "outcome": coordinator_result.outcome,
                                "state": final_state,
                                "decision_source": decision_source,
                                "message_count": len(message_rows),
                                "partitions": [
                                    {"topic": topic, "partition": partition_no}
                                    for topic, partition_no in partitions
                                ],
                            },
                        )
                        self._finish_intent(intent_id, "COMMITTED", None, conn=conn)
                        result = self.transaction_detail(txn_id, conn=conn)
                except BusinessError:
                    self._abort_intent(intent_id, "decision rejected")
                    raise
                if injection:
                    self._record_fault(
                        txn_id,
                        injection,
                        conditions,
                        codes.OK,
                        {
                            "outcome": coordinator_result.outcome,
                            "decision_source": decision_source,
                        },
                    )
                result["replayed"] = False
                return result
            except BusinessError:
                self._abort_intent(intent_id, "transaction resolution rejected")
                if concurrent_conflict is not None:
                    self._record_conflict(
                        txn_id,
                        concurrent_conflict["existing"],
                        concurrent_conflict["incoming"],
                        key,
                        concurrent_conflict["source"],
                        {
                            "concurrent_decision": True,
                            "query_response": concurrent_conflict["response"],
                        },
                    )
                raise
            except BaseException:
                self._abort_intent(intent_id, "transaction resolution failed")
                raise

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------
    def _append_recovery_event(
        self,
        run_id: str,
        kind: str,
        action: str,
        before: dict,
        after: dict,
        intent_id: str | None = None,
    ) -> None:
        with immediate(self.db_path) as conn:
            conn.execute(
                "INSERT INTO recovery_events(run_id, intent_id, kind, action, before_json, "
                "after_json, created_at) VALUES(?,?,?,?,?,?,?)",
                (run_id, intent_id, kind, action, json_text(before), json_text(after), self._now()),
            )

    def recover(self, auto_renegotiate: bool | None = None) -> dict:
        run_id = f"recovery-{uuid.uuid4().hex}"
        started = self._now()
        candidates: list[str] = []
        counts = {
            "unfinished_intents": 0,
            "leases_released": 0,
            "transactions_marked_pending": 0,
            "renegotiated": 0,
            "renegotiation_errors": 0,
        }
        with immediate(self.db_path) as conn:
            conn.execute(
                "INSERT INTO recovery_runs(run_id, started_at, starting_process, facts_json, note) "
                "VALUES(?,?,?,?,?)",
                (
                    run_id,
                    started,
                    str(os.getpid()),
                    "{}",
                    "Recovery is reconstructed only from SQLite facts; a killed process cannot write a complete exit report.",
                ),
            )
            unfinished = conn.execute(
                "SELECT * FROM intents WHERE state='STARTED' ORDER BY created_at, intent_id"
            ).fetchall()
            counts["unfinished_intents"] = len(unfinished)
            touched_partitions: set[tuple[str, int]] = set()
            for intent in unfinished:
                intent_id = intent["intent_id"]
                kind = intent["kind"]
                payload = _loads(intent["payload_json"]) or {}
                before: dict = {"intent": dict(intent)}
                after: dict = {}
                if kind == "LEASE_ACQUIRE":
                    leases = conn.execute(
                        "SELECT * FROM leases WHERE intent_id=? AND state='ACTIVE'",
                        (intent_id,),
                    ).fetchall()
                    before["leases"] = [dict(row) for row in leases]
                    for lease in leases:
                        conn.execute(
                            "UPDATE leases SET state='RECOVERED', ended_at=?, "
                            "end_reason='unfinished_lease_intent' WHERE lease_id=?",
                            (self._now(), lease["lease_id"]),
                        )
                        counts["leases_released"] += 1
                        touched_partitions.add((lease["topic"], int(lease["partition_no"])))
                    after = {
                        "released_lease_ids": [row["lease_id"] for row in leases],
                        "message_redeliverable": True,
                    }
                    action = "RELEASE_UNCOMMITTED_LEASE" if leases else "NO_EFFECT"
                elif kind == "TXN_RESOLVE":
                    txn_id = payload.get("txn_id") or intent["scope_key"].removeprefix("txn:")
                    txn = conn.execute(
                        "SELECT * FROM transactions WHERE txn_id=?", (txn_id,)
                    ).fetchone()
                    before["transaction"] = dict(txn) if txn else None
                    if txn and txn["state"] == "PENDING" and txn_id not in candidates:
                        candidates.append(txn_id)
                    if txn and txn["state"] == "PENDING":
                        counts["transactions_marked_pending"] += 1
                    after = {
                        "txn_id": txn_id,
                        "persisted_state": txn["state"] if txn else None,
                        "renegotiate": bool(txn and txn["state"] == "PENDING"),
                    }
                    action = "KEEP_TXN_PENDING_AND_RENEGOTIATE"
                elif kind == "ACK":
                    action = "DISCARD_UNCOMMITTED_ACK"
                    after = {"ack_applied": False}
                elif kind in {"PRODUCE", "LEASE_RELEASE"}:
                    action = "DISCARD_UNCOMMITTED_WRITE"
                    after = {"write_applied": False}
                else:
                    action = "DISCARD_UNKNOWN_INCOMPLETE_WRITE"
                    after = {}
                conn.execute(
                    "UPDATE intents SET state='ABORTED', updated_at=?, completed_at=?, "
                    "note='recovered incomplete intent' WHERE intent_id=?",
                    (self._now(), self._now(), intent_id),
                )
                conn.execute(
                    "INSERT INTO recovery_events(run_id, intent_id, kind, action, before_json, "
                    "after_json, created_at) VALUES(?,?,?,?,?,?,?)",
                    (
                        run_id,
                        intent_id,
                        kind,
                        action,
                        json_text(before),
                        json_text(after),
                        self._now(),
                    ),
                )
                self._audit(
                    conn,
                    "RECOVERY_INTENT_ABORTED",
                    topic=payload.get("topic"),
                    partition_no=payload.get("partition"),
                    txn_id=payload.get("txn_id"),
                    detail={"intent_id": intent_id, "kind": kind, "action": action},
                )
            for topic, partition_no in sorted(touched_partitions):
                self._refresh_partition_state(conn, topic, partition_no)
            facts = {
                **counts,
                "candidate_transactions": candidates,
                "recovered_partitions": [
                    {"topic": topic, "partition": partition_no}
                    for topic, partition_no in sorted(touched_partitions)
                ],
                "report_basis": "persisted_sqlite_facts",
                "complete_exit_report_possible_after_kill": False,
            }
            conn.execute(
                "UPDATE recovery_runs SET facts_json=? WHERE run_id=?",
                (json_text(facts), run_id),
            )

        should_renegotiate = (
            self.auto_renegotiate if auto_renegotiate is None else auto_renegotiate
        )
        if should_renegotiate:
            for txn_id in candidates:
                try:
                    result = self.resolve_transaction(txn_id, source="recovery")
                    counts["renegotiated"] += 1
                    self._append_recovery_event(
                        run_id,
                        "TXN_RENEGOTIATE",
                        "RENEGOTIATED_FROM_PENDING",
                        {"txn_id": txn_id, "state": "PENDING"},
                        {"txn_id": txn_id, "state": result.get("state"), "outcome": result.get("outcome")},
                    )
                except BusinessError as error:
                    counts["renegotiation_errors"] += 1
                    self._append_recovery_event(
                        run_id,
                        "TXN_RENEGOTIATE",
                        "RENEGOTIATION_DEFERRED",
                        {"txn_id": txn_id, "state": "PENDING"},
                        {"txn_id": txn_id, "error": error.code},
                    )
        with immediate(self.db_path) as conn:
            row = conn.execute(
                "SELECT facts_json FROM recovery_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            persisted_facts = _loads(row["facts_json"]) if row else {}
            persisted_facts.update(
                {
                    "renegotiated": counts["renegotiated"],
                    "renegotiation_errors": counts["renegotiation_errors"],
                }
            )
            conn.execute(
                "UPDATE recovery_runs SET finished_at=?, facts_json=? WHERE run_id=?",
                (self._now(), json_text(persisted_facts), run_id),
            )
        return self.recovery_report(run_id=run_id)

    # ------------------------------------------------------------------
    # Read models
    # ------------------------------------------------------------------
    def _message_dict(self, row: sqlite3.Row, include_body: bool = True) -> dict:
        data = {
            "message_id": int(row["message_id"]),
            "topic": row["topic"],
            "partition": int(row["partition_no"]),
            "sequence": int(row["sequence"]),
            "producer_id": row["producer_id"],
            "client_sequence": row["client_sequence"],
            "txn_id": row["txn_id"],
            "body_digest": row["body_digest"],
            "produced_at": int(row["produced_at"]),
            "state": row["state"],
            "visible_at": row["visible_at"],
            "decision_source": row["decision_source"],
            "acked_at": row["acked_at"],
        }
        if include_body:
            data["body"] = _loads(row["body"])
        return data

    def list_topics(self) -> list[dict]:
        with read_conn(self.db_path) as conn:
            rows = conn.execute("SELECT * FROM topics ORDER BY name").fetchall()
            result = []
            for row in rows:
                partitions = conn.execute(
                    "SELECT * FROM partitions WHERE topic=? ORDER BY partition_no",
                    (row["name"],),
                ).fetchall()
                item = dict(row)
                item["config"] = _loads(item.pop("config_json"))
                item["partitions"] = [self._partition_summary(part) for part in partitions]
                result.append(item)
            return result

    def _partition_summary(self, row: sqlite3.Row) -> dict:
        data = dict(row)
        data["delivery_ready"] = bool(data["delivery_ready"])
        return data

    def topic_detail(self, topic: str) -> dict:
        with read_conn(self.db_path) as conn:
            row = self._topic_or_error(conn, topic)
            partitions = conn.execute(
                "SELECT * FROM partitions WHERE topic=? ORDER BY partition_no", (topic,)
            ).fetchall()
            result = dict(row)
            result["config"] = _loads(result.pop("config_json"))
            result["partitions"] = [self._partition_summary(part) for part in partitions]
            return result

    def partition_detail(self, topic: str, partition_no: int) -> dict:
        with read_conn(self.db_path) as conn:
            self._topic_or_error(conn, topic)
            row = conn.execute(
                "SELECT * FROM partitions WHERE topic=? AND partition_no=?",
                (topic, partition_no),
            ).fetchone()
            if not row:
                raise BusinessError(
                    codes.E_PARTITION_INVALID,
                    "partition is outside the topic range",
                    {"topic": topic, "partition": partition_no},
                )
            data = self._partition_summary(row)
            lease = conn.execute(
                "SELECT * FROM leases WHERE topic=? AND partition_no=? AND state='ACTIVE'",
                (topic, partition_no),
            ).fetchone()
            data["active_lease"] = self._effective_lease(lease, self._now())
            data["message_counts"] = {
                row["state"]: int(row["n"])
                for row in conn.execute(
                    "SELECT state, COUNT(*) AS n FROM messages WHERE topic=? AND "
                    "partition_no=? GROUP BY state",
                    (topic, partition_no),
                ).fetchall()
            }
            counts = conn.execute(
                "SELECT COUNT(*) AS total, SUM(CASE WHEN acked_at IS NOT NULL THEN 1 ELSE 0 END) "
                "AS acked FROM messages WHERE topic=? AND partition_no=?",
                (topic, partition_no),
            ).fetchone()
            data["message_counts"]["TOTAL"] = int(counts["total"] or 0)
            data["message_counts"]["ACKED"] = int(counts["acked"] or 0)
            return data

    def list_messages(
        self,
        topic: str,
        partition_no: int,
        state: str | None = None,
        limit: int = 1_000,
    ) -> list[dict]:
        if limit < 1 or limit > 10_000:
            raise BusinessError(codes.E_BAD_REQUEST, "limit must be in [1, 10000]")
        if state is not None and state not in {"PENDING", "COMMITTED", "ROLLED_BACK"}:
            raise BusinessError(codes.E_BAD_REQUEST, "invalid message state")
        with read_conn(self.db_path) as conn:
            self._topic_or_error(conn, topic)
            sql = (
                "SELECT * FROM messages WHERE topic=? AND partition_no=? "
                + ("AND state=? " if state else "")
                + "ORDER BY sequence LIMIT ?"
            )
            params: list[Any] = [topic, partition_no]
            if state:
                params.append(state)
            params.append(limit)
            return [self._message_dict(row) for row in conn.execute(sql, params).fetchall()]

    def transaction_detail(self, txn_id: str, conn=None) -> dict:
        owns = conn is None
        if owns:
            context = read_conn(self.db_path)
            conn = context.__enter__()
        try:
            txn = self._txn_row(conn, txn_id)
            data = dict(txn)
            data["outcome"] = (
                "COMMIT"
                if txn["state"] == "COMMITTED"
                else "ROLLBACK"
                if txn["state"] == "ROLLED_BACK"
                else None
            )
            messages = conn.execute(
                "SELECT * FROM messages WHERE txn_id=? ORDER BY topic, partition_no, sequence",
                (txn_id,),
            ).fetchall()
            data["messages"] = [self._message_dict(row) for row in messages]
            data["pending_count"] = int(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM pending_messages WHERE txn_id=?", (txn_id,)
                ).fetchone()["n"]
            )
            data["queries"] = [
                {
                    **dict(row),
                    "detail": _loads(row["detail_json"]),
                }
                for row in conn.execute(
                    "SELECT * FROM txn_queries WHERE txn_id=? ORDER BY requested_at, attempt",
                    (txn_id,),
                ).fetchall()
            ]
            data["conflicts"] = [
                {**dict(row), "detail": _loads(row["detail_json"])}
                for row in conn.execute(
                    "SELECT * FROM txn_conflicts WHERE txn_id=? ORDER BY conflict_id",
                    (txn_id,),
                ).fetchall()
            ]
            return data
        finally:
            if owns:
                context.__exit__(None, None, None)

    def list_audit_events(
        self,
        topic: str | None = None,
        txn_id: str | None = None,
        after_event_id: int = 0,
        limit: int = 5_000,
    ) -> list[dict]:
        clauses = ["event_id>?"]
        params: list[Any] = [after_event_id]
        if topic:
            clauses.append("topic=?")
            params.append(topic)
        if txn_id:
            clauses.append("txn_id=?")
            params.append(txn_id)
        params.append(limit)
        with read_conn(self.db_path) as conn:
            rows = conn.execute(
                "SELECT * FROM audit_events WHERE "
                + " AND ".join(clauses)
                + " ORDER BY event_id LIMIT ?",
                params,
            ).fetchall()
            return [{**dict(row), "detail": _loads(row["detail_json"])} for row in rows]

    def list_fault_events(self) -> list[dict]:
        with read_conn(self.db_path) as conn:
            rows = conn.execute("SELECT * FROM fault_events ORDER BY fault_id").fetchall()
            return [
                {
                    **dict(row),
                    "conditions": _loads(row["conditions_json"]),
                    "observed": _loads(row["observed_json"]),
                }
                for row in rows
            ]

    def recovery_report(self, run_id: str | None = None) -> dict:
        with read_conn(self.db_path) as conn:
            if run_id is None:
                run = conn.execute(
                    "SELECT * FROM recovery_runs ORDER BY rowid DESC LIMIT 1"
                ).fetchone()
            else:
                run = conn.execute(
                    "SELECT * FROM recovery_runs WHERE run_id=?", (run_id,)
                ).fetchone()
            if not run:
                return {
                    "run_id": None,
                    "events": [],
                    "report_basis": "persisted_sqlite_facts",
                    "complete_exit_report_possible_after_kill": False,
                }
            events = conn.execute(
                "SELECT * FROM recovery_events WHERE run_id=? ORDER BY event_id",
                (run["run_id"],),
            ).fetchall()
            data = dict(run)
            data["facts"] = _loads(data.pop("facts_json"))
            data["events"] = [
                {
                    **dict(row),
                    "before": _loads(row["before_json"]),
                    "after": _loads(row["after_json"]),
                }
                for row in events
            ]
            data["report_basis"] = "persisted_sqlite_facts"
            data["complete_exit_report_possible_after_kill"] = False
            return data

    def partition_delivery_sequence(self, topic: str, partition_no: int) -> list[int]:
        with read_conn(self.db_path) as conn:
            rows = conn.execute(
                "SELECT sequence FROM audit_events WHERE topic=? AND partition_no=? "
                "AND event_type='MESSAGE_ACKED' ORDER BY event_id",
                (topic, partition_no),
            ).fetchall()
            return [int(row["sequence"]) for row in rows]

