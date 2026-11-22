"""Durable store and state machine for the local middleware."""

from __future__ import annotations

import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Iterator, TypeVar

from .constants import (
    ACKED,
    DEAD_LETTERED,
    DELIVERY_ACKED,
    DELIVERY_ADMIN_REDELIVERED,
    DELIVERY_DISPATCHED,
    DELIVERY_ERROR,
    DELIVERY_EXPIRED,
    DELIVERY_MODES,
    DELIVERY_REFUSED,
    DELIVERY_ROLLED_BACK,
    DELIVERY_TIMED_OUT,
    DISPATCHED,
    INTENT_APPLIED,
    INTENT_COMMITTED,
    INTENT_ROLLED_BACK,
    INTENT_STARTED,
    MODE_AT_LEAST_ONCE,
    MODE_EXACTLY_ONCE,
    PENDING,
    REDELIVERED,
    REQUEST_ABORTED,
    REQUEST_DONE,
    REQUEST_FAILED,
    REQUEST_RUNNING,
)
from .db import Database
from .errors import (
    E_ALREADY_ACKED,
    E_DUPLICATE_ACK,
    E_INTERNAL,
    E_INVALID_ARGUMENT,
    E_LEASE_EXPIRED,
    E_LEASE_LOST,
    E_NOT_FOUND,
    E_REQUEST_CONFLICT,
    E_REQUEST_TIMEOUT,
    MWError,
)
from .models import validate_transition
from .utils import (
    body_digest,
    canonical_json,
    dumps,
    loads,
    maybe_fault,
    new_id,
    now_ms,
    require_int,
)

T = TypeVar("T")


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


def _message_dict(row: sqlite3.Row, include_body: bool = True) -> dict[str, Any]:
    item = dict(row)
    if include_body:
        item["body"] = loads(item.pop("body_json", None), None)
    else:
        item.pop("body_json", None)
    item["body_digest"] = item.get("body_digest")
    return item


class SQLiteStore:
    """SQLite persistence with queue-local writer locks.

    A connection is created per operation. WAL lets read transactions continue
    against a stable committed snapshot while a writer commits a newer revision.
    """

    def __init__(self, path: str) -> None:
        self.db = Database(path)
        self.db.initialize()
        self._request_condition = threading.Condition()

    # ------------------------------------------------------------------
    # Intent and revision primitives
    # ------------------------------------------------------------------
    @staticmethod
    def _bump_revision(conn: sqlite3.Connection) -> int:
        conn.execute("UPDATE revision SET value = value + 1 WHERE id = 1")
        row = conn.execute("SELECT value FROM revision WHERE id = 1").fetchone()
        return int(row["value"])

    def revision(self) -> int:
        with self.db.read_tx() as conn:
            row = conn.execute("SELECT value FROM revision WHERE id = 1").fetchone()
            return int(row["value"])

    def _start_intent(
        self,
        kind: str,
        *,
        request_id: str | None = None,
        queue_name: str | None = None,
        message_id: str | None = None,
        delivery_id: int | None = None,
        consumer_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> str:
        intent_id = new_id("int_")
        ts = now_ms()
        with self.db.write_tx() as conn:
            conn.execute(
                """
                INSERT INTO intents(
                    intent_id, request_id, kind, queue_name, message_id,
                    delivery_id, consumer_id, state, payload_json, created_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    intent_id,
                    request_id,
                    kind,
                    queue_name,
                    message_id,
                    delivery_id,
                    consumer_id,
                    INTENT_STARTED,
                    dumps(payload or {}),
                    ts,
                ),
            )
            self._bump_revision(conn)
        return intent_id

    @staticmethod
    def _finish_intent_in_tx(
        conn: sqlite3.Connection,
        intent_id: str,
        state: str,
        *,
        result: dict[str, Any] | None = None,
    ) -> None:
        ts = now_ms()
        conn.execute(
            """
            UPDATE intents
               SET state = ?, result_json = ?, finished_at_ms = ?
             WHERE intent_id = ?
            """,
            (state, dumps(result or {}), ts, intent_id),
        )

    def _finish_intent(
        self, intent_id: str, state: str, *, result: dict[str, Any] | None = None
    ) -> None:
        with self.db.write_tx() as conn:
            self._finish_intent_in_tx(conn, intent_id, state, result=result)
            self._bump_revision(conn)

    @staticmethod
    def _mark_intent_applied_in_tx(
        conn: sqlite3.Connection,
        intent_id: str,
        *,
        result: dict[str, Any] | None = None,
    ) -> None:
        conn.execute(
            """
            UPDATE intents
               SET state = ?, applied_at_ms = ?, result_json = ?
             WHERE intent_id = ?
            """,
            (INTENT_APPLIED, now_ms(), dumps(result or {}), intent_id),
        )

    # ------------------------------------------------------------------
    # Idempotent request execution
    # ------------------------------------------------------------------
    def execute_request(
        self,
        request_id: str,
        operation: str,
        fn: Callable[[], T],
        *,
        wait_timeout_ms: int = 30_000,
    ) -> tuple[T, bool]:
        """Execute an operation at most once for a request id.

        The first caller owns execution. Concurrent callers poll the durable
        request row and then reuse the exact stored result/error. Across a
        process crash, recovery marks RUNNING rows ABORTED so a retry is safe.
        """

        if not request_id or not isinstance(request_id, str):
            raise MWError(E_INVALID_ARGUMENT, "request_id must be a non-empty string")
        owner = new_id("owner_")
        ts = now_ms()
        first = False
        with self.db.write_tx() as conn:
            row = conn.execute(
                "SELECT * FROM request_results WHERE request_id = ?", (request_id,)
            ).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO request_results(
                        request_id, operation, status, owner_token,
                        created_at_ms, updated_at_ms
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (request_id, operation, REQUEST_RUNNING, owner, ts, ts),
                )
                self._bump_revision(conn)
                first = True
            else:
                if row["operation"] != operation:
                    raise MWError(
                        E_REQUEST_CONFLICT,
                        f"request_id {request_id!r} was already used for {row['operation']}",
                        details={"existing_operation": row["operation"], "operation": operation},
                    )
                if row["status"] == REQUEST_DONE:
                    return loads(row["result_json"], None), True
                if row["status"] == REQUEST_FAILED:
                    error = loads(row["error_json"], {}) or {}
                    raise MWError(
                        error.get("code", E_INTERNAL),
                        error.get("message", "request previously failed"),
                        http_status=int(error.get("http_status", 400)),
                        details=error.get("details"),
                    )
                if row["status"] == REQUEST_ABORTED:
                    conn.execute(
                        """
                        UPDATE request_results
                           SET status = ?, owner_token = ?, updated_at_ms = ?,
                               error_json = NULL, result_json = NULL
                         WHERE request_id = ?
                        """,
                        (REQUEST_RUNNING, owner, ts, request_id),
                    )
                    self._bump_revision(conn)
                    first = True

        if not first:
            deadline = time.monotonic() + (wait_timeout_ms / 1000.0)
            while time.monotonic() < deadline:
                with self.db.read_tx() as conn:
                    row = conn.execute(
                        "SELECT * FROM request_results WHERE request_id = ?", (request_id,)
                    ).fetchone()
                if row is None:
                    break
                if row["status"] == REQUEST_DONE:
                    return loads(row["result_json"], None), True
                if row["status"] == REQUEST_FAILED:
                    error = loads(row["error_json"], {}) or {}
                    raise MWError(
                        error.get("code", E_INTERNAL),
                        error.get("message", "request previously failed"),
                        http_status=int(error.get("http_status", 400)),
                        details=error.get("details"),
                    )
                if row["status"] == REQUEST_ABORTED:
                    # The owner died before a result was stored. Retry using the
                    # same request id; business operations are idempotent.
                    return self.execute_request(
                        request_id, operation, fn, wait_timeout_ms=wait_timeout_ms
                    )
                with self._request_condition:
                    self._request_condition.wait(timeout=0.01)
            raise MWError(
                E_REQUEST_TIMEOUT,
                f"timed out waiting for request {request_id!r}",
                http_status=409,
            )

        try:
            result = fn()
        except MWError as exc:
            error_payload = {
                "code": exc.code,
                "message": exc.message,
                "http_status": exc.http_status,
                "details": exc.details,
            }
            with self.db.write_tx() as conn:
                conn.execute(
                    """
                    UPDATE request_results
                       SET status = ?, error_json = ?, updated_at_ms = ?
                     WHERE request_id = ?
                    """,
                    (REQUEST_FAILED, dumps(error_payload), now_ms(), request_id),
                )
                self._bump_revision(conn)
            with self._request_condition:
                self._request_condition.notify_all()
            raise
        except BaseException:
            with self.db.write_tx() as conn:
                conn.execute(
                    """
                    UPDATE request_results
                       SET status = ?, error_json = ?, updated_at_ms = ?
                     WHERE request_id = ?
                    """,
                    (
                        REQUEST_FAILED,
                        dumps(
                            {
                                "code": E_INTERNAL,
                                "message": "internal operation failure",
                                "http_status": 500,
                            }
                        ),
                        now_ms(),
                        request_id,
                    ),
                )
                self._bump_revision(conn)
            with self._request_condition:
                self._request_condition.notify_all()
            raise

        with self.db.write_tx() as conn:
            conn.execute(
                """
                UPDATE request_results
                   SET status = ?, result_json = ?, updated_at_ms = ?
                 WHERE request_id = ?
                """,
                (REQUEST_DONE, dumps(result), now_ms(), request_id),
            )
            self._bump_revision(conn)
        with self._request_condition:
            self._request_condition.notify_all()
        return result, False

    # ------------------------------------------------------------------
    # Queue, consumer and enqueue operations
    # ------------------------------------------------------------------
    def register_queue(
        self,
        queue_name: str,
        priority: int = 100,
        *,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        if not queue_name:
            raise MWError(E_INVALID_ARGUMENT, "queue_name is required")
        priority = require_int(priority, "priority", minimum=0)
        with self.db.queue_write(queue_name):
            intent_id = self._start_intent(
                "REGISTER_QUEUE",
                request_id=request_id,
                queue_name=queue_name,
                payload={"priority": priority},
            )
            with self.db.write_tx() as conn:
                existing = conn.execute(
                    "SELECT * FROM queues WHERE queue_name = ?", (queue_name,)
                ).fetchone()
                ts = now_ms()
                if existing is None:
                    conn.execute(
                        """
                        INSERT INTO queues(queue_name, priority, next_seq, rotation, created_at_ms)
                        VALUES (?, ?, 1, 0, ?)
                        """,
                        (queue_name, priority, ts),
                    )
                else:
                    conn.execute(
                        "UPDATE queues SET priority = ? WHERE queue_name = ?",
                        (priority, queue_name),
                    )
                self._finish_intent_in_tx(
                    conn,
                    intent_id,
                    INTENT_COMMITTED,
                    result={"queue_name": queue_name, "priority": priority},
                )
                self._bump_revision(conn)
        return self.get_queue_summary(queue_name)

    def register_consumer(
        self,
        consumer_id: str,
        group_name: str,
        endpoint: str,
        queues: list[str],
        *,
        enabled: bool = True,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        if not consumer_id or not group_name or not endpoint:
            raise MWError(E_INVALID_ARGUMENT, "consumer_id, group_name and endpoint are required")
        if not isinstance(queues, list) or not queues:
            raise MWError(E_INVALID_ARGUMENT, "queues must be a non-empty list")
        with self.db.read_tx() as conn:
            missing = [
                q
                for q in queues
                if conn.execute(
                    "SELECT 1 FROM queues WHERE queue_name = ?", (q,)
                ).fetchone()
                is None
            ]
        if missing:
            raise MWError(
                E_NOT_FOUND,
                f"queue(s) not registered: {', '.join(missing)}",
                http_status=404,
            )
        intent_id = self._start_intent(
            "REGISTER_CONSUMER",
            request_id=request_id,
            consumer_id=consumer_id,
            payload={"group_name": group_name, "endpoint": endpoint, "queues": queues},
        )
        with self.db.write_tx() as conn:
            ts = now_ms()
            conn.execute(
                """
                INSERT INTO consumers(
                    consumer_id, group_name, endpoint, enabled, created_at_ms, updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(consumer_id) DO UPDATE SET
                    group_name = excluded.group_name,
                    endpoint = excluded.endpoint,
                    enabled = excluded.enabled,
                    updated_at_ms = excluded.updated_at_ms
                """,
                (consumer_id, group_name, endpoint, int(enabled), ts, ts),
            )
            for queue_name in queues:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO consumer_queues(consumer_id, queue_name)
                    VALUES (?, ?)
                    """,
                    (consumer_id, queue_name),
                )
            conn.execute(
                "DELETE FROM consumer_queues WHERE consumer_id = ? AND queue_name NOT IN (%s)"
                % ",".join("?" for _ in queues),
                (consumer_id, *queues),
            )
            self._finish_intent_in_tx(
                conn,
                intent_id,
                INTENT_COMMITTED,
                result={"consumer_id": consumer_id, "queues": queues},
            )
            self._bump_revision(conn)
        return self.get_consumer(consumer_id) or {}

    def _validate_message_item(self, item: dict[str, Any]) -> dict[str, Any]:
        message_id = item.get("message_id")
        if not isinstance(message_id, str) or not message_id:
            raise MWError(E_INVALID_ARGUMENT, "each message needs a non-empty message_id")
        visibility_timeout_ms = require_int(
            item.get("visibility_timeout_ms", 30_000),
            "visibility_timeout_ms",
            minimum=1,
        )
        max_retries = require_int(item.get("max_retries", 3), "max_retries", minimum=0)
        body = item.get("body")
        digest = item.get("body_digest")
        computed = body_digest(body)
        if digest is not None and digest != computed:
            raise MWError(
                E_INVALID_ARGUMENT,
                f"body_digest mismatch for message {message_id!r}",
                details={"supplied": digest, "computed": computed},
            )
        return {
            "message_id": message_id,
            "body": body,
            "body_json": dumps(body),
            "body_digest": computed,
            "visibility_timeout_ms": visibility_timeout_ms,
            "max_retries": max_retries,
        }

    def enqueue(
        self,
        queue_name: str,
        messages: list[dict[str, Any]],
        *,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(messages, list) or not messages:
            raise MWError(E_INVALID_ARGUMENT, "messages must be a non-empty list")
        validated = [self._validate_message_item(item) for item in messages]
        with self.db.read_tx() as conn:
            queue_exists = conn.execute(
                "SELECT 1 FROM queues WHERE queue_name = ?", (queue_name,)
            ).fetchone()
        if queue_exists is None:
            raise MWError(
                E_NOT_FOUND,
                f"queue {queue_name!r} is not registered",
                http_status=404,
            )
        with self.db.queue_write(queue_name):
            intent_id = self._start_intent(
                "ENQUEUE",
                request_id=request_id,
                queue_name=queue_name,
                payload={"message_ids": [m["message_id"] for m in validated]},
            )
            with self.db.write_tx() as conn:
                queue = conn.execute(
                    "SELECT * FROM queues WHERE queue_name = ?", (queue_name,)
                ).fetchone()
                if queue is None:  # defensive: queues are not deleted by the API
                    raise MWError(
                        E_NOT_FOUND,
                        f"queue {queue_name!r} is not registered",
                        http_status=404,
                    )
                next_seq = int(queue["next_seq"])
                ts = now_ms()
                produced: list[dict[str, Any]] = []
                for item in validated:
                    existing = conn.execute(
                        """
                        SELECT seq, body_digest FROM messages
                         WHERE queue_name = ? AND message_id = ?
                        """,
                        (queue_name, item["message_id"]),
                    ).fetchone()
                    if existing is not None:
                        produced.append(
                            {
                                "message_id": item["message_id"],
                                "seq": int(existing["seq"]),
                                "duplicate": True,
                                "body_digest": existing["body_digest"],
                            }
                        )
                        continue
                    seq = next_seq
                    conn.execute(
                        """
                        INSERT INTO messages(
                            queue_name, message_id, seq, body_digest, body_json,
                            visibility_timeout_ms, max_retries, status,
                            attempt_count, redelivery_count, available_at_ms,
                            created_at_ms, updated_at_ms
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?)
                        """,
                        (
                            queue_name,
                            item["message_id"],
                            seq,
                            item["body_digest"],
                            item["body_json"],
                            item["visibility_timeout_ms"],
                            item["max_retries"],
                            PENDING,
                            ts,
                            ts,
                            ts,
                        ),
                    )
                    produced.append(
                        {
                            "message_id": item["message_id"],
                            "seq": seq,
                            "duplicate": False,
                            "body_digest": item["body_digest"],
                        }
                    )
                    next_seq += 1
                conn.execute(
                    "UPDATE queues SET next_seq = ? WHERE queue_name = ?",
                    (next_seq, queue_name),
                )
                result = {
                    "ok": True,
                    "queue_name": queue_name,
                    "messages": produced,
                    "next_seq": next_seq,
                }
                self._finish_intent_in_tx(
                    conn, intent_id, INTENT_COMMITTED, result={"produced": produced}
                )
                self._bump_revision(conn)
        return result

    # ------------------------------------------------------------------
    # Claim / dispatch / failure
    # ------------------------------------------------------------------
    def _get_consumer_for_queue_locked(
        self, conn: sqlite3.Connection, queue_name: str, consumer_id: str | None
    ) -> sqlite3.Row:
        if consumer_id:
            row = conn.execute(
                """
                SELECT c.* FROM consumers c
                  JOIN consumer_queues cq ON cq.consumer_id = c.consumer_id
                 WHERE c.consumer_id = ? AND cq.queue_name = ? AND c.enabled = 1
                """,
                (consumer_id, queue_name),
            ).fetchone()
            if row is None:
                raise MWError(
                    E_NOT_FOUND,
                    f"consumer {consumer_id!r} is not enabled for queue {queue_name!r}",
                    http_status=404,
                )
            return row
        row = conn.execute(
            """
            SELECT c.* FROM consumers c
              JOIN consumer_queues cq ON cq.consumer_id = c.consumer_id
             WHERE cq.queue_name = ? AND c.enabled = 1
             ORDER BY c.consumer_id
             LIMIT 1
            """,
            (queue_name,),
        ).fetchone()
        if row is None:
            raise MWError(
                E_NOT_FOUND,
                f"no enabled consumer subscribed to queue {queue_name!r}",
                http_status=404,
            )
        return row

    def _eligible_message_locked(
        self, conn: sqlite3.Connection, queue_name: str, ts: int
    ) -> sqlite3.Row | None:
        return conn.execute(
            """
            SELECT * FROM messages
             WHERE queue_name = ?
               AND status IN (?, ?)
               AND available_at_ms <= ?
               AND active_delivery_id IS NULL
             ORDER BY seq ASC
             LIMIT 1
            """,
            (queue_name, PENDING, REDELIVERED, ts),
        ).fetchone()

    def claim(
        self,
        queue_name: str,
        consumer_id: str | None,
        mode: str,
        *,
        round_id: str | None = None,
        defer_commit: bool = False,
        request_id: str | None = None,
    ) -> dict[str, Any] | None:
        if mode not in DELIVERY_MODES:
            raise MWError(E_INVALID_ARGUMENT, f"mode must be one of {sorted(DELIVERY_MODES)}")
        with self.db.queue_write(queue_name):
            with self.db.read_tx() as conn:
                consumer = self._get_consumer_for_queue_locked(conn, queue_name, consumer_id)
                ts = now_ms()
                candidate = self._eligible_message_locked(conn, queue_name, ts)
            if candidate is None:
                return None

            intent_id = self._start_intent(
                "DISPATCH",
                request_id=request_id,
                queue_name=queue_name,
                message_id=candidate["message_id"],
                consumer_id=consumer["consumer_id"],
                payload={"round_id": round_id, "mode": mode},
            )
            with self.db.write_tx() as conn:
                consumer = self._get_consumer_for_queue_locked(conn, queue_name, consumer_id)
                ts = now_ms()
                row = self._eligible_message_locked(conn, queue_name, ts)
                if row is None:
                    self._finish_intent_in_tx(
                        conn, intent_id, INTENT_ROLLED_BACK, result={"reason": "already_leased"}
                    )
                    self._bump_revision(conn)
                    return None
                attempt = int(row["attempt_count"]) + 1
                lease_token = new_id("lease_")
                deadline = ts + int(row["visibility_timeout_ms"])
                cur = conn.execute(
                    """
                    INSERT INTO deliveries(
                        queue_name, message_id, consumer_id, group_name, attempt,
                        delivery_mode, round_id, lease_token, state,
                        dispatched_at_ms, deadline_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        queue_name,
                        row["message_id"],
                        consumer["consumer_id"],
                        consumer["group_name"],
                        attempt,
                        mode,
                        round_id,
                        lease_token,
                        DELIVERY_DISPATCHED,
                        ts,
                        deadline,
                    ),
                )
                delivery_id = int(cur.lastrowid)
                conn.execute(
                    """
                    UPDATE messages
                       SET status = ?, attempt_count = ?, active_delivery_id = ?,
                           available_at_ms = ?, updated_at_ms = ?
                     WHERE queue_name = ? AND message_id = ?
                    """,
                    (
                        DISPATCHED,
                        attempt,
                        delivery_id,
                        deadline,
                        ts,
                        queue_name,
                        row["message_id"],
                    ),
                )
                self._trace_in_tx(
                    conn,
                    round_id,
                    "CLAIMED",
                    queue_name=queue_name,
                    message_id=row["message_id"],
                    seq=int(row["seq"]),
                    delivery_id=delivery_id,
                    consumer_id=consumer["consumer_id"],
                    details={
                        "attempt": attempt,
                        "mode": mode,
                        "lease_deadline_ms": deadline,
                    },
                )
                result = {
                    "delivery_id": delivery_id,
                    "queue_name": queue_name,
                    "message_id": row["message_id"],
                    "seq": int(row["seq"]),
                    "consumer_id": consumer["consumer_id"],
                    "consumer_group": consumer["group_name"],
                    "consumer_endpoint": consumer["endpoint"],
                    "lease_token": lease_token,
                    "attempt": attempt,
                    "mode": mode,
                    "round_id": round_id,
                    "body": loads(row["body_json"], None),
                    "body_digest": row["body_digest"],
                    "visibility_timeout_ms": int(row["visibility_timeout_ms"]),
                    "max_retries": int(row["max_retries"]),
                    "deadline_ms": deadline,
                    "intent_id": intent_id,
                }
                conn.execute(
                    "UPDATE intents SET delivery_id = ?, message_id = ? WHERE intent_id = ?",
                    (delivery_id, row["message_id"], intent_id),
                )
                self._mark_intent_applied_in_tx(conn, intent_id, result=result)
                self._bump_revision(conn)
            maybe_fault("dispatch_after_applied")
            if not defer_commit:
                self._finish_intent(
                    intent_id,
                    INTENT_COMMITTED,
                    result={"delivery_id": delivery_id, "state": "leased"},
                )
            return result

    def complete_dispatch(
        self,
        queue_name: str,
        intent_id: str,
        delivery_id: int,
        *,
        http_status: int | None,
        elapsed_ms: int,
    ) -> None:
        with self.db.queue_write(queue_name):
            with self.db.write_tx() as conn:
                row = conn.execute(
                    "SELECT state FROM intents WHERE intent_id = ?", (intent_id,)
                ).fetchone()
                if row is None or row["state"] == INTENT_ROLLED_BACK:
                    return
                delivery = conn.execute(
                    "SELECT * FROM deliveries WHERE delivery_id = ?", (delivery_id,)
                ).fetchone()
                conn.execute(
                    """
                    UPDATE deliveries
                       SET dispatch_outcome = 'ACCEPTED', http_status = ?, elapsed_ms = ?
                     WHERE delivery_id = ?
                    """,
                    (http_status, elapsed_ms, delivery_id),
                )
                if delivery is not None:
                    self._trace_in_tx(
                        conn,
                        delivery["round_id"],
                        "HTTP_ACCEPTED",
                        queue_name=queue_name,
                        message_id=delivery["message_id"],
                        delivery_id=delivery_id,
                        consumer_id=delivery["consumer_id"],
                        details={"http_status": http_status, "elapsed_ms": elapsed_ms},
                    )
                self._finish_intent_in_tx(
                    conn,
                    intent_id,
                    INTENT_COMMITTED,
                    result={"delivery_id": delivery_id, "outcome": "ACCEPTED"},
                )
                self._bump_revision(conn)

    def fail_dispatch(
        self,
        queue_name: str,
        intent_id: str,
        delivery_id: int,
        *,
        outcome: str,
        error_code: str,
        http_status: int | None,
        elapsed_ms: int,
    ) -> dict[str, Any]:
        with self.db.queue_write(queue_name):
            with self.db.write_tx() as conn:
                delivery = conn.execute(
                    "SELECT * FROM deliveries WHERE delivery_id = ?", (delivery_id,)
                ).fetchone()
                if delivery is None:
                    raise MWError(E_NOT_FOUND, "delivery not found", http_status=404)
                message = conn.execute(
                    "SELECT * FROM messages WHERE queue_name = ? AND message_id = ?",
                    (queue_name, delivery["message_id"]),
                ).fetchone()
                if message is None:
                    raise MWError(E_NOT_FOUND, "message not found", http_status=404)
                ts = now_ms()
                if delivery["state"] == DELIVERY_ACKED or message["status"] == ACKED:
                    self._finish_intent_in_tx(
                        conn,
                        intent_id,
                        INTENT_COMMITTED,
                        result={
                            "delivery_id": delivery_id,
                            "outcome": "ACKED_BEFORE_TRANSPORT_RESULT",
                            "message_state": message["status"],
                            "error_code": error_code,
                        },
                    )
                    self._bump_revision(conn)
                    return {
                        "delivery_id": delivery_id,
                        "outcome": "ACKED_BEFORE_TRANSPORT_RESULT",
                        "message_state": message["status"],
                        "error_code": error_code,
                    }
                conn.execute(
                    """
                    UPDATE deliveries
                       SET state = ?, finished_at_ms = ?, dispatch_outcome = ?,
                           error_code = ?, http_status = ?, elapsed_ms = ?
                     WHERE delivery_id = ?
                    """,
                    (outcome, ts, outcome, error_code, http_status, elapsed_ms, delivery_id),
                )
                message_state = message["status"]
                if (
                    message_state == DISPATCHED
                    and message["active_delivery_id"] == delivery_id
                ):
                    attempt = int(message["attempt_count"])
                    max_retries = int(message["max_retries"])
                    if attempt <= max_retries:
                        new_state = REDELIVERED
                        conn.execute(
                            """
                            UPDATE messages
                               SET status = ?, active_delivery_id = NULL,
                                   available_at_ms = ?, updated_at_ms = ?,
                                   redelivery_count = redelivery_count + 1,
                                   last_error = ?
                             WHERE queue_name = ? AND message_id = ?
                            """,
                            (
                                new_state,
                                ts,
                                ts,
                                error_code,
                                queue_name,
                                message["message_id"],
                            ),
                        )
                    else:
                        new_state = DEAD_LETTERED
                        conn.execute(
                            """
                            UPDATE messages
                               SET status = ?, active_delivery_id = NULL,
                                   updated_at_ms = ?, last_error = ?
                             WHERE queue_name = ? AND message_id = ?
                            """,
                            (new_state, ts, error_code, queue_name, message["message_id"]),
                        )
                        self._recompute_group_offset(
                            conn, delivery["group_name"], queue_name, ts
                        )
                else:
                    new_state = message_state
                self._trace_in_tx(
                    conn,
                    delivery["round_id"],
                    "DISPATCH_TIMEOUT" if outcome == DELIVERY_TIMED_OUT else "DISPATCH_FAILED",
                    queue_name=queue_name,
                    message_id=delivery["message_id"],
                    delivery_id=delivery_id,
                    consumer_id=delivery["consumer_id"],
                    details={
                        "outcome": outcome,
                        "error_code": error_code,
                        "http_status": http_status,
                        "elapsed_ms": elapsed_ms,
                        "message_state": new_state,
                    },
                )
                self._finish_intent_in_tx(
                    conn,
                    intent_id,
                    INTENT_COMMITTED,
                    result={
                        "delivery_id": delivery_id,
                        "outcome": outcome,
                        "message_state": new_state,
                        "error_code": error_code,
                    },
                )
                self._bump_revision(conn)
        return {
            "delivery_id": delivery_id,
            "outcome": outcome,
            "message_state": new_state,
            "error_code": error_code,
        }

    # ------------------------------------------------------------------
    # ACK
    # ------------------------------------------------------------------
    @staticmethod
    def _idempotency_key(queue_name: str, message_id: str, consumer_id: str) -> str:
        return f"{queue_name}\x1f{message_id}\x1f{consumer_id}"

    def _recompute_group_offset(
        self,
        conn: sqlite3.Connection,
        group_name: str,
        queue_name: str,
        ts: int,
    ) -> int:
        next_seq = 1
        while True:
            row = conn.execute(
                "SELECT status FROM messages WHERE queue_name = ? AND seq = ?",
                (queue_name, next_seq),
            ).fetchone()
            if row is None or row["status"] not in (ACKED, DEAD_LETTERED):
                break
            next_seq += 1
        conn.execute(
            """
            INSERT INTO group_offsets(group_name, queue_name, next_seq, updated_at_ms)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(group_name, queue_name) DO UPDATE SET
                next_seq = excluded.next_seq,
                updated_at_ms = excluded.updated_at_ms
            """,
            (group_name, queue_name, next_seq, ts),
        )
        return next_seq

    def ack(
        self,
        delivery_id: int,
        lease_token: str,
        consumer_id: str,
        mode: str | None = None,
        *,
        request_id: str | None = None,
        fault_point: str | None = "ack_after_applied",
    ) -> dict[str, Any]:
        with self.db.read_tx() as conn:
            initial = conn.execute(
                "SELECT * FROM deliveries WHERE delivery_id = ?", (delivery_id,)
            ).fetchone()
        if initial is None:
            raise MWError(E_NOT_FOUND, "delivery not found", http_status=404)
        queue_name = initial["queue_name"]
        with self.db.queue_write(queue_name):
            with self.db.read_tx() as conn:
                delivery = conn.execute(
                    "SELECT * FROM deliveries WHERE delivery_id = ?", (delivery_id,)
                ).fetchone()
                message = conn.execute(
                    "SELECT * FROM messages WHERE queue_name = ? AND message_id = ?",
                    (queue_name, initial["message_id"]),
                ).fetchone()
                existing_ack = conn.execute(
                    "SELECT * FROM acks WHERE delivery_id = ?", (delivery_id,)
                ).fetchone()
                requested_mode = mode or delivery["delivery_mode"]
                if requested_mode != delivery["delivery_mode"]:
                    raise MWError(
                        E_INVALID_ARGUMENT,
                        "ack mode does not match the delivery mode",
                        details={"delivery_mode": delivery["delivery_mode"], "mode": requested_mode},
                    )
                if message["status"] == ACKED:
                    if delivery["delivery_mode"] == MODE_EXACTLY_ONCE:
                        raise MWError(
                            E_DUPLICATE_ACK,
                            "exactly-once acknowledgement was already committed",
                            http_status=409,
                            details={
                                "message_id": message["message_id"],
                                "consumer_id": consumer_id,
                                "idempotency_key": self._idempotency_key(
                                    queue_name, message["message_id"], consumer_id
                                ),
                            },
                        )
                    raise MWError(
                        E_ALREADY_ACKED,
                        "message was already acknowledged",
                        http_status=409,
                    )
                if delivery["state"] != DELIVERY_DISPATCHED:
                    raise MWError(
                        E_LEASE_LOST,
                        f"delivery is in state {delivery['state']}, not DISPATCHED",
                        http_status=409,
                    )
                if delivery["lease_token"] != lease_token or delivery["consumer_id"] != consumer_id:
                    raise MWError(
                        E_LEASE_LOST,
                        "lease token or consumer does not own this delivery",
                        http_status=409,
                    )
                if message["active_delivery_id"] != delivery_id:
                    raise MWError(
                        E_LEASE_LOST,
                        "delivery is not the active lease for the message",
                        http_status=409,
                    )
                if int(delivery["deadline_ms"]) <= now_ms():
                    raise MWError(
                        E_LEASE_EXPIRED,
                        "visibility timeout has expired; acknowledgement is no longer valid",
                        http_status=409,
                    )
                if existing_ack is not None and existing_ack["state"] != DELIVERY_ROLLED_BACK:
                    if delivery["delivery_mode"] == MODE_EXACTLY_ONCE:
                        raise MWError(
                            E_DUPLICATE_ACK,
                            "duplicate exactly-once acknowledgement",
                            http_status=409,
                        )
                    raise MWError(E_ALREADY_ACKED, "delivery was already acknowledged", http_status=409)
                previous_offset_row = conn.execute(
                    """
                    SELECT next_seq FROM group_offsets
                     WHERE group_name = ? AND queue_name = ?
                    """,
                    (delivery["group_name"], queue_name),
                ).fetchone()
                previous_offset = int(previous_offset_row["next_seq"]) if previous_offset_row else 1

            intent_id = self._start_intent(
                "ACK",
                request_id=request_id,
                queue_name=queue_name,
                message_id=delivery["message_id"],
                delivery_id=delivery_id,
                consumer_id=consumer_id,
                payload={
                    "lease_token": lease_token,
                    "delivery_mode": delivery["delivery_mode"],
                    "previous_group_offset": previous_offset,
                },
            )
            ts = now_ms()
            idem_key = self._idempotency_key(queue_name, delivery["message_id"], consumer_id)
            try:
                with self.db.write_tx() as conn:
                    # Re-read under the durable writer transaction.
                    delivery = conn.execute(
                        "SELECT * FROM deliveries WHERE delivery_id = ?", (delivery_id,)
                    ).fetchone()
                    message = conn.execute(
                        "SELECT * FROM messages WHERE queue_name = ? AND message_id = ?",
                        (queue_name, delivery["message_id"]),
                    ).fetchone()
                    if delivery["state"] != DELIVERY_DISPATCHED or message["status"] != DISPATCHED:
                        raise MWError(E_LEASE_LOST, "lease changed before ack commit", http_status=409)
                    if int(delivery["deadline_ms"]) <= ts:
                        raise MWError(E_LEASE_EXPIRED, "lease expired before ack commit", http_status=409)
                    conn.execute(
                        """
                        INSERT INTO acks(
                            queue_name, message_id, consumer_id, group_name, delivery_id,
                            idempotency_key, request_id, state, acked_at_ms
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(queue_name, message_id, consumer_id) DO UPDATE SET
                            delivery_id = excluded.delivery_id,
                            idempotency_key = excluded.idempotency_key,
                            request_id = excluded.request_id,
                            state = excluded.state,
                            acked_at_ms = excluded.acked_at_ms
                        """,
                        (
                            queue_name,
                            delivery["message_id"],
                            consumer_id,
                            delivery["group_name"],
                            delivery_id,
                            idem_key,
                            request_id,
                            INTENT_APPLIED,
                            ts,
                        ),
                    )
                    conn.execute(
                        "UPDATE deliveries SET state = ?, finished_at_ms = ? WHERE delivery_id = ?",
                        (DELIVERY_ACKED, ts, delivery_id),
                    )
                    conn.execute(
                        """
                        UPDATE messages
                           SET status = ?, active_delivery_id = NULL, updated_at_ms = ?,
                               last_error = NULL
                         WHERE queue_name = ? AND message_id = ?
                        """,
                        (ACKED, ts, queue_name, delivery["message_id"]),
                    )
                    next_offset = self._recompute_group_offset(
                        conn, delivery["group_name"], queue_name, ts
                    )
                    self._trace_in_tx(
                        conn,
                        delivery["round_id"],
                        "ACKED",
                        queue_name=queue_name,
                        message_id=delivery["message_id"],
                        delivery_id=delivery_id,
                        consumer_id=consumer_id,
                        details={
                            "delivery_mode": delivery["delivery_mode"],
                            "next_group_offset": next_offset,
                        },
                    )
                    self._mark_intent_applied_in_tx(
                        conn,
                        intent_id,
                        result={
                            "delivery_id": delivery_id,
                            "next_group_offset": next_offset,
                        },
                    )
                    self._bump_revision(conn)
            except MWError as exc:
                if exc.code in (E_LEASE_LOST, E_LEASE_EXPIRED):
                    self._finish_intent(
                        intent_id,
                        INTENT_ROLLED_BACK,
                        result={"reason": "lease_state_changed"},
                    )
                raise
            if fault_point:
                maybe_fault(fault_point)
            with self.db.write_tx() as conn:
                conn.execute(
                    "UPDATE acks SET state = ? WHERE delivery_id = ? AND state = ?",
                    (INTENT_COMMITTED, delivery_id, INTENT_APPLIED),
                )
                self._finish_intent_in_tx(
                    conn,
                    intent_id,
                    INTENT_COMMITTED,
                    result={
                        "delivery_id": delivery_id,
                        "idempotency_key": idem_key,
                        "next_group_offset": next_offset,
                    },
                )
                self._bump_revision(conn)
        return {
            "ok": True,
            "queue_name": queue_name,
            "message_id": delivery["message_id"],
            "consumer_id": consumer_id,
            "delivery_id": delivery_id,
            "delivery_mode": delivery["delivery_mode"],
            "idempotency_key": idem_key,
            "message_state": ACKED,
            "group_name": delivery["group_name"],
            "next_group_offset": next_offset,
            "ack_at_ms": ts,
        }

    # ------------------------------------------------------------------
    # Queries and read snapshots
    # ------------------------------------------------------------------
    def get_consumer(self, consumer_id: str) -> dict[str, Any] | None:
        with self.db.read_tx() as conn:
            row = conn.execute(
                "SELECT * FROM consumers WHERE consumer_id = ?", (consumer_id,)
            ).fetchone()
            if row is None:
                return None
            queues = [
                r["queue_name"]
                for r in conn.execute(
                    "SELECT queue_name FROM consumer_queues WHERE consumer_id = ? ORDER BY queue_name",
                    (consumer_id,),
                ).fetchall()
            ]
            result = dict(row)
            result["enabled"] = bool(result["enabled"])
            result["queues"] = queues
            return result

    def list_consumers(self) -> list[dict[str, Any]]:
        with self.db.read_tx() as conn:
            rows = conn.execute("SELECT consumer_id FROM consumers ORDER BY consumer_id").fetchall()
        return [self.get_consumer(row["consumer_id"]) for row in rows]  # type: ignore[list-item]

    def get_queue_summary(self, queue_name: str) -> dict[str, Any]:
        with self.db.read_tx() as conn:
            queue = conn.execute(
                "SELECT * FROM queues WHERE queue_name = ?", (queue_name,)
            ).fetchone()
            if queue is None:
                raise MWError(E_NOT_FOUND, f"queue {queue_name!r} not found", http_status=404)
            status_rows = conn.execute(
                """
                SELECT status, COUNT(*) AS n FROM messages
                 WHERE queue_name = ? GROUP BY status
                """,
                (queue_name,),
            ).fetchall()
            counts = {row["status"]: int(row["n"]) for row in status_rows}
            active = conn.execute(
                "SELECT COUNT(*) AS n FROM deliveries WHERE queue_name = ? AND state = ?",
                (queue_name, DELIVERY_DISPATCHED),
            ).fetchone()["n"]
            oldest = conn.execute(
                """
                SELECT seq FROM messages WHERE queue_name = ?
                   AND status IN (?, ?) ORDER BY seq LIMIT 1
                """,
                (queue_name, PENDING, REDELIVERED),
            ).fetchone()
            offsets = [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM group_offsets WHERE queue_name = ? ORDER BY group_name",
                    (queue_name,),
                ).fetchall()
            ]
            revision = conn.execute("SELECT value FROM revision WHERE id = 1").fetchone()["value"]
        return {
            "ok": True,
            "queue_name": queue_name,
            "priority": int(queue["priority"]),
            "next_seq": int(queue["next_seq"]),
            "status_counts": {state: counts.get(state, 0) for state in (
                PENDING, DISPATCHED, ACKED, REDELIVERED, DEAD_LETTERED
            )},
            "active_deliveries": int(active),
            "oldest_eligible_seq": int(oldest["seq"]) if oldest else None,
            "group_offsets": offsets,
            "revision": int(revision),
        }

    def active_delivery_count(self, queue_name: str) -> int:
        with self.db.read_tx() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM deliveries WHERE queue_name = ? AND state = ?",
                (queue_name, DELIVERY_DISPATCHED),
            ).fetchone()
            return int(row["n"])

    def get_message(self, queue_name: str, message_id: str) -> dict[str, Any]:
        with self.db.read_tx() as conn:
            row = conn.execute(
                "SELECT * FROM messages WHERE queue_name = ? AND message_id = ?",
                (queue_name, message_id),
            ).fetchone()
            if row is None:
                raise MWError(E_NOT_FOUND, "message not found", http_status=404)
            message = _message_dict(row)
            message["deliveries"] = [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM deliveries WHERE queue_name = ? AND message_id = ? ORDER BY attempt",
                    (queue_name, message_id),
                ).fetchall()
            ]
            message["acks"] = [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM acks WHERE queue_name = ? AND message_id = ? ORDER BY ack_id",
                    (queue_name, message_id),
                ).fetchall()
            ]
            message["intents"] = [
                dict(r)
                for r in conn.execute(
                    """
                    SELECT * FROM intents
                     WHERE queue_name = ? AND message_id = ? ORDER BY created_at_ms, intent_id
                    """,
                    (queue_name, message_id),
                ).fetchall()
            ]
            message["revision"] = int(
                conn.execute("SELECT value FROM revision WHERE id = 1").fetchone()["value"]
            )
        return message

    def list_intents(
        self, *, state: str | None = None, limit: int = 200
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 10_000))
        with self.db.read_tx() as conn:
            if state:
                rows = conn.execute(
                    "SELECT * FROM intents WHERE state = ? ORDER BY created_at_ms DESC LIMIT ?",
                    (state, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM intents ORDER BY created_at_ms DESC LIMIT ?", (limit,)
                ).fetchall()
        return [dict(r) for r in rows]

    def snapshot(self, queue_name: str | None = None) -> dict[str, Any]:
        """Read all returned facts from one SQLite read transaction."""
        with self.db.read_tx() as conn:
            revision = int(
                conn.execute("SELECT value FROM revision WHERE id = 1").fetchone()["value"]
            )
            queues = [
                dict(r)
                for r in conn.execute("SELECT * FROM queues ORDER BY queue_name").fetchall()
            ]
            result: dict[str, Any] = {
                "ok": True,
                "revision": revision,
                "queues": queues,
            }
            params: tuple[Any, ...] = ()
            where = ""
            if queue_name:
                where = " WHERE queue_name = ?"
                params = (queue_name,)
            messages = [
                _message_dict(r)
                for r in conn.execute(
                    f"SELECT * FROM messages{where} ORDER BY queue_name, seq", params
                ).fetchall()
            ]
            deliveries = [
                dict(r)
                for r in conn.execute(
                    f"SELECT * FROM deliveries{where} ORDER BY delivery_id", params
                ).fetchall()
            ]
            acks = [
                dict(r)
                for r in conn.execute(
                    f"SELECT * FROM acks{where} ORDER BY ack_id", params
                ).fetchall()
            ]
            offsets = [
                dict(r)
                for r in conn.execute(
                    f"SELECT * FROM group_offsets{where} ORDER BY group_name, queue_name", params
                ).fetchall()
            ]
            result.update(
                {
                    "messages": messages,
                    "deliveries": deliveries,
                    "acks": acks,
                    "group_offsets": offsets,
                }
            )
            return result

    # ------------------------------------------------------------------
    # Round and trace records
    # ------------------------------------------------------------------
    @staticmethod
    def _trace_in_tx(
        conn: sqlite3.Connection,
        round_id: str | None,
        event: str,
        *,
        queue_name: str | None = None,
        message_id: str | None = None,
        seq: int | None = None,
        delivery_id: int | None = None,
        consumer_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        if not round_id:
            return
        conn.execute(
            """
            INSERT INTO round_traces(
                round_id, ts_ms, event, queue_name, message_id, seq,
                delivery_id, consumer_id, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                round_id,
                now_ms(),
                event,
                queue_name,
                message_id,
                seq,
                delivery_id,
                consumer_id,
                dumps(details or {}),
            ),
        )

    def create_round(
        self, round_id: str, request_id: str, mode: str, config: dict[str, Any]
    ) -> None:
        ts = now_ms()
        with self.db.write_tx() as conn:
            conn.execute(
                """
                INSERT INTO rounds(
                    round_id, request_id, mode, status, config_json, started_at_ms
                ) VALUES (?, ?, ?, 'RUNNING', ?, ?)
                """,
                (round_id, request_id, mode, dumps(config), ts),
            )
            self._trace_in_tx(
                conn, round_id, "ROUND_START", details={"mode": mode, "config": config}
            )
            self._bump_revision(conn)

    def finish_round(
        self,
        round_id: str,
        status: str,
        result: dict[str, Any],
        extra_traces: list[dict[str, Any]] | None = None,
        rotate_queues: list[str] | None = None,
    ) -> None:
        ts = now_ms()
        with self.db.write_tx() as conn:
            for queue_name in rotate_queues or []:
                conn.execute(
                    "UPDATE queues SET rotation = rotation + 1 WHERE queue_name = ?",
                    (queue_name,),
                )
            for trace in extra_traces or []:
                self._trace_in_tx(
                    conn,
                    round_id,
                    trace.get("event", "TRACE"),
                    queue_name=trace.get("queue_name"),
                    message_id=trace.get("message_id"),
                    seq=trace.get("seq"),
                    delivery_id=trace.get("delivery_id"),
                    consumer_id=trace.get("consumer_id"),
                    details=trace.get("details"),
                )
            self._trace_in_tx(conn, round_id, "ROUND_END", details={"status": status})
            conn.execute(
                """
                UPDATE rounds
                   SET status = ?, finished_at_ms = ?, result_json = ?
                 WHERE round_id = ?
                """,
                (status, ts, dumps(result), round_id),
            )
            self._bump_revision(conn)

    def get_round(self, round_id: str, *, include_traces: bool = True) -> dict[str, Any]:
        with self.db.read_tx() as conn:
            row = conn.execute("SELECT * FROM rounds WHERE round_id = ?", (round_id,)).fetchone()
            if row is None:
                raise MWError(E_NOT_FOUND, "round not found", http_status=404)
            result = dict(row)
            result["config"] = loads(result.pop("config_json"), {})
            result["result"] = loads(result.pop("result_json"), None)
            if include_traces:
                result["traces"] = [
                    {
                        **dict(t),
                        "details": loads(t["details_json"], {}),
                    }
                    for t in conn.execute(
                        "SELECT * FROM round_traces WHERE round_id = ? ORDER BY trace_id",
                        (round_id,),
                    ).fetchall()
                ]
                for trace in result["traces"]:
                    trace.pop("details_json", None)
            return result

    # ------------------------------------------------------------------
    # Visibility timeout reaper
    # ------------------------------------------------------------------
    def reap_expired(self, limit: int = 1000) -> dict[str, Any]:
        limit = max(1, min(int(limit), 10_000))
        with self.db.read_tx() as conn:
            rows = conn.execute(
                """
                SELECT delivery_id, queue_name FROM deliveries
                 WHERE state = ? AND deadline_ms <= ?
                 ORDER BY deadline_ms, delivery_id LIMIT ?
                """,
                (DELIVERY_DISPATCHED, now_ms(), limit),
            ).fetchall()
        actions: list[dict[str, Any]] = []
        for candidate in rows:
            queue_name = candidate["queue_name"]
            with self.db.queue_write(queue_name):
                with self.db.read_tx() as conn:
                    delivery = conn.execute(
                        "SELECT * FROM deliveries WHERE delivery_id = ?",
                        (candidate["delivery_id"],),
                    ).fetchone()
                    if delivery is None or delivery["state"] != DELIVERY_DISPATCHED:
                        continue
                    ts = now_ms()
                    if int(delivery["deadline_ms"]) > ts:
                        continue
                intent_id = self._start_intent(
                    "VISIBILITY_TIMEOUT",
                    queue_name=queue_name,
                    message_id=delivery["message_id"],
                    delivery_id=int(delivery["delivery_id"]),
                    consumer_id=delivery["consumer_id"],
                    payload={"deadline_ms": int(delivery["deadline_ms"])},
                )
                with self.db.write_tx() as conn:
                    delivery = conn.execute(
                        "SELECT * FROM deliveries WHERE delivery_id = ?",
                        (candidate["delivery_id"],),
                    ).fetchone()
                    message = conn.execute(
                        "SELECT * FROM messages WHERE queue_name = ? AND message_id = ?",
                        (queue_name, delivery["message_id"]),
                    ).fetchone()
                    ts = now_ms()
                    if (
                        delivery["state"] != DELIVERY_DISPATCHED
                        or int(delivery["deadline_ms"]) > ts
                        or message["status"] != DISPATCHED
                    ):
                        self._finish_intent_in_tx(
                            conn,
                            intent_id,
                            INTENT_ROLLED_BACK,
                            result={"reason": "lease_changed"},
                        )
                        self._bump_revision(conn)
                        continue
                    conn.execute(
                        """
                        UPDATE deliveries
                           SET state = ?, finished_at_ms = ?, dispatch_outcome = 'VISIBILITY_TIMEOUT',
                               error_code = 'E_VISIBILITY_TIMEOUT'
                         WHERE delivery_id = ?
                        """,
                        (DELIVERY_EXPIRED, ts, delivery["delivery_id"]),
                    )
                    attempt = int(message["attempt_count"])
                    max_retries = int(message["max_retries"])
                    if attempt <= max_retries:
                        new_state = REDELIVERED
                        conn.execute(
                            """
                            UPDATE messages
                               SET status = ?, active_delivery_id = NULL,
                                   available_at_ms = ?, updated_at_ms = ?,
                                   redelivery_count = redelivery_count + 1,
                                   last_error = 'E_VISIBILITY_TIMEOUT'
                             WHERE queue_name = ? AND message_id = ?
                            """,
                            (new_state, ts, ts, queue_name, message["message_id"]),
                        )
                    else:
                        new_state = DEAD_LETTERED
                        conn.execute(
                            """
                            UPDATE messages
                               SET status = ?, active_delivery_id = NULL,
                                   updated_at_ms = ?, last_error = 'E_VISIBILITY_TIMEOUT'
                             WHERE queue_name = ? AND message_id = ?
                            """,
                            (new_state, ts, queue_name, message["message_id"]),
                        )
                        self._recompute_group_offset(
                            conn, delivery["group_name"], queue_name, ts
                        )
                    action = {
                        "delivery_id": int(delivery["delivery_id"]),
                        "queue_name": queue_name,
                        "message_id": message["message_id"],
                        "attempt": attempt,
                        "new_state": new_state,
                        "reason": "VISIBILITY_TIMEOUT",
                    }
                    actions.append(action)
                    self._trace_in_tx(
                        conn,
                        delivery["round_id"],
                        "VISIBILITY_TIMEOUT",
                        queue_name=queue_name,
                        message_id=message["message_id"],
                        seq=int(message["seq"]),
                        delivery_id=int(delivery["delivery_id"]),
                        consumer_id=delivery["consumer_id"],
                        details={"new_state": new_state, "attempt": attempt},
                    )
                    self._finish_intent_in_tx(
                        conn, intent_id, INTENT_COMMITTED, result=action
                    )
                    self._bump_revision(conn)
        return {
            "ok": True,
            "scanned": len(rows),
            "redelivered": sum(1 for a in actions if a["new_state"] == REDELIVERED),
            "dead_lettered": sum(1 for a in actions if a["new_state"] == DEAD_LETTERED),
            "actions": actions,
        }

    # ------------------------------------------------------------------
    # Administrative transition endpoint support
    # ------------------------------------------------------------------
    def transition_state(
        self,
        queue_name: str,
        message_id: str,
        target_state: str,
        *,
        consumer_id: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        with self.db.queue_write(queue_name):
            with self.db.read_tx() as conn:
                message = conn.execute(
                    "SELECT * FROM messages WHERE queue_name = ? AND message_id = ?",
                    (queue_name, message_id),
                ).fetchone()
                if message is None:
                    raise MWError(E_NOT_FOUND, "message not found", http_status=404)
                current = message["status"]
                validate_transition(current, target_state)
            intent_id = self._start_intent(
                "ADMIN_TRANSITION",
                request_id=request_id,
                queue_name=queue_name,
                message_id=message_id,
                payload={"from": current, "to": target_state},
            )
            with self.db.write_tx() as conn:
                message = conn.execute(
                    "SELECT * FROM messages WHERE queue_name = ? AND message_id = ?",
                    (queue_name, message_id),
                ).fetchone()
                ts = now_ms()
                consumer = consumer_id or "state-admin"
                group = "state-admin"
                if target_state == DISPATCHED:
                    attempt = int(message["attempt_count"]) + 1
                    token = new_id("admin_lease_")
                    cur = conn.execute(
                        """
                        INSERT INTO deliveries(
                            queue_name, message_id, consumer_id, group_name, attempt,
                            delivery_mode, round_id, lease_token, state,
                            dispatched_at_ms, deadline_ms
                        ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
                        """,
                        (
                            queue_name,
                            message_id,
                            consumer,
                            group,
                            attempt,
                            MODE_AT_LEAST_ONCE,
                            token,
                            DELIVERY_DISPATCHED,
                            ts,
                            ts + int(message["visibility_timeout_ms"]),
                        ),
                    )
                    delivery_id = int(cur.lastrowid)
                    conn.execute(
                        """
                        UPDATE messages
                           SET status = ?, attempt_count = ?, active_delivery_id = ?,
                               updated_at_ms = ?
                         WHERE queue_name = ? AND message_id = ?
                        """,
                        (DISPATCHED, attempt, delivery_id, ts, queue_name, message_id),
                    )
                elif target_state == ACKED:
                    delivery = conn.execute(
                        """
                        SELECT * FROM deliveries WHERE queue_name = ? AND message_id = ?
                           AND state = ? ORDER BY delivery_id DESC LIMIT 1
                        """,
                        (queue_name, message_id, DELIVERY_DISPATCHED),
                    ).fetchone()
                    if delivery is None:
                        raise MWError(E_LEASE_LOST, "ACKED requires an active delivery", http_status=409)
                    delivery_id = int(delivery["delivery_id"])
                    group = delivery["group_name"]
                    conn.execute(
                        "UPDATE deliveries SET state = ?, finished_at_ms = ? WHERE delivery_id = ?",
                        (DELIVERY_ACKED, ts, delivery_id),
                    )
                    conn.execute(
                        """
                        INSERT INTO acks(
                            queue_name, message_id, consumer_id, group_name, delivery_id,
                            idempotency_key, request_id, state, acked_at_ms
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(delivery_id) DO UPDATE SET
                            state = excluded.state, acked_at_ms = excluded.acked_at_ms
                        """,
                        (
                            queue_name,
                            message_id,
                            delivery["consumer_id"],
                            group,
                            delivery_id,
                            self._idempotency_key(queue_name, message_id, delivery["consumer_id"]),
                            request_id,
                            INTENT_COMMITTED,
                            ts,
                        ),
                    )
                    conn.execute(
                        """
                        UPDATE messages SET status = ?, active_delivery_id = NULL, updated_at_ms = ?
                         WHERE queue_name = ? AND message_id = ?
                        """,
                        (ACKED, ts, queue_name, message_id),
                    )
                    self._recompute_group_offset(conn, group, queue_name, ts)
                elif target_state == REDELIVERED:
                    delivery = conn.execute(
                        "SELECT * FROM deliveries WHERE delivery_id = ?",
                        (message["active_delivery_id"],),
                    ).fetchone()
                    if delivery is not None:
                        conn.execute(
                            "UPDATE deliveries SET state = ?, finished_at_ms = ? WHERE delivery_id = ?",
                            (DELIVERY_ADMIN_REDELIVERED, ts, delivery["delivery_id"]),
                        )
                    conn.execute(
                        """
                        UPDATE messages
                           SET status = ?, active_delivery_id = NULL, available_at_ms = ?,
                               updated_at_ms = ?, redelivery_count = redelivery_count + 1
                         WHERE queue_name = ? AND message_id = ?
                        """,
                        (REDELIVERED, ts, ts, queue_name, message_id),
                    )
                elif target_state == DEAD_LETTERED:
                    delivery = conn.execute(
                        "SELECT * FROM deliveries WHERE delivery_id = ?",
                        (message["active_delivery_id"],),
                    ).fetchone()
                    if delivery is not None:
                        conn.execute(
                            "UPDATE deliveries SET state = ?, finished_at_ms = ? WHERE delivery_id = ?",
                            (DELIVERY_EXPIRED, ts, delivery["delivery_id"]),
                        )
                        group = delivery["group_name"]
                    conn.execute(
                        """
                        UPDATE messages SET status = ?, active_delivery_id = NULL, updated_at_ms = ?
                         WHERE queue_name = ? AND message_id = ?
                        """,
                        (DEAD_LETTERED, ts, queue_name, message_id),
                    )
                    self._recompute_group_offset(conn, group, queue_name, ts)
                self._finish_intent_in_tx(
                    conn,
                    intent_id,
                    INTENT_COMMITTED,
                    result={"from": current, "to": target_state},
                )
                self._bump_revision(conn)
        result = self.get_message(queue_name, message_id)
        return {
            "ok": True,
            "queue_name": queue_name,
            "message_id": message_id,
            "from": current,
            "to": target_state,
            "message": result,
        }

    # ------------------------------------------------------------------
    # Crash recovery
    # ------------------------------------------------------------------
    def recover(self, *, run_expired: bool = True) -> dict[str, Any]:
        """Roll back unfinished intents using durable DB facts only."""
        report_id = new_id("recovery_")
        started = now_ms()
        actions: list[dict[str, Any]] = []
        with self.db.read_tx() as conn:
            unfinished = [
                dict(r)
                for r in conn.execute(
                    """
                    SELECT * FROM intents
                     WHERE state IN (?, ?) ORDER BY created_at_ms, intent_id
                    """,
                    (INTENT_STARTED, INTENT_APPLIED),
                ).fetchall()
            ]
        for initial_intent in unfinished:
            queue_name = initial_intent.get("queue_name")
            if not queue_name:
                with self.db.write_tx() as conn:
                    self._finish_intent_in_tx(
                        conn,
                        initial_intent["intent_id"],
                        INTENT_ROLLED_BACK,
                        result={"reason": "RECOVERY_NO_QUEUE_FACT"},
                    )
                    self._bump_revision(conn)
                actions.append(
                    {
                        "intent_id": initial_intent["intent_id"],
                        "kind": initial_intent["kind"],
                        "action": "intent_rolled_back",
                        "reason": "RECOVERY_NO_QUEUE_FACT",
                    }
                )
                continue
            with self.db.queue_write(queue_name):
                with self.db.write_tx() as conn:
                    intent = conn.execute(
                        "SELECT * FROM intents WHERE intent_id = ?",
                        (initial_intent["intent_id"],),
                    ).fetchone()
                    if intent is None or intent["state"] not in (INTENT_STARTED, INTENT_APPLIED):
                        continue
                    kind = intent["kind"]
                    ts = now_ms()
                    if kind == "DISPATCH":
                        message = conn.execute(
                            "SELECT * FROM messages WHERE queue_name = ? AND message_id = ?",
                            (queue_name, intent["message_id"]),
                        ).fetchone()
                        delivery = (
                            conn.execute(
                                "SELECT * FROM deliveries WHERE delivery_id = ?",
                                (intent["delivery_id"],),
                            ).fetchone()
                            if intent["delivery_id"] is not None
                            else None
                        )
                        from_state = message["status"] if message else None
                        to_state = from_state
                        if message is not None and message["status"] == DISPATCHED:
                            if delivery is not None and delivery["state"] == DELIVERY_DISPATCHED:
                                conn.execute(
                                    "UPDATE deliveries SET state = ?, finished_at_ms = ? WHERE delivery_id = ?",
                                    (DELIVERY_ROLLED_BACK, ts, delivery["delivery_id"]),
                                )
                            conn.execute(
                                """
                                UPDATE messages
                                   SET status = ?, active_delivery_id = NULL,
                                       available_at_ms = ?, updated_at_ms = ?,
                                       last_error = 'E_RECOVERED_UNCOMMITTED_DISPATCH'
                                 WHERE queue_name = ? AND message_id = ?
                                """,
                                (PENDING, ts, ts, queue_name, intent["message_id"]),
                            )
                            to_state = PENDING
                            action = "dispatch_rolled_back_to_pending"
                        elif message is not None and message["status"] in (PENDING, REDELIVERED):
                            if delivery is not None and delivery["state"] == DELIVERY_DISPATCHED:
                                conn.execute(
                                    "UPDATE deliveries SET state = ?, finished_at_ms = ? WHERE delivery_id = ?",
                                    (DELIVERY_ROLLED_BACK, ts, delivery["delivery_id"]),
                                )
                            action = "dispatch_intent_cleared_no_visible_effect"
                        else:
                            action = "dispatch_preserved_superseded_state"
                        action_row = {
                            "intent_id": intent["intent_id"],
                            "kind": kind,
                            "queue_name": queue_name,
                            "message_id": intent["message_id"],
                            "delivery_id": intent["delivery_id"],
                            "from_state": from_state,
                            "to_state": to_state,
                            "action": action,
                        }
                    elif kind == "ACK":
                        delivery = (
                            conn.execute(
                                "SELECT * FROM deliveries WHERE delivery_id = ?",
                                (intent["delivery_id"],),
                            ).fetchone()
                            if intent["delivery_id"] is not None
                            else None
                        )
                        message = conn.execute(
                            "SELECT * FROM messages WHERE queue_name = ? AND message_id = ?",
                            (queue_name, intent["message_id"]),
                        ).fetchone()
                        ack = (
                            conn.execute(
                                "SELECT * FROM acks WHERE delivery_id = ?",
                                (intent["delivery_id"],),
                            ).fetchone()
                            if intent["delivery_id"] is not None
                            else None
                        )
                        from_state = message["status"] if message else None
                        to_state = from_state
                        if ack is not None and ack["state"] != DELIVERY_ROLLED_BACK:
                            conn.execute(
                                "UPDATE acks SET state = ? WHERE delivery_id = ?",
                                (DELIVERY_ROLLED_BACK, intent["delivery_id"]),
                            )
                        if delivery is not None and delivery["state"] == DELIVERY_ACKED:
                            conn.execute(
                                """
                                UPDATE deliveries
                                   SET state = ?, finished_at_ms = NULL,
                                       dispatch_outcome = 'ACK_ROLLED_BACK'
                                 WHERE delivery_id = ?
                                """,
                                (DELIVERY_DISPATCHED, intent["delivery_id"]),
                            )
                        if message is not None and message["status"] == ACKED:
                            conn.execute(
                                """
                                UPDATE messages
                                   SET status = ?, active_delivery_id = ?, updated_at_ms = ?,
                                       last_error = 'E_RECOVERED_UNCOMMITTED_ACK'
                                 WHERE queue_name = ? AND message_id = ?
                                """,
                                (DISPATCHED, intent["delivery_id"], ts, queue_name, intent["message_id"]),
                            )
                            to_state = DISPATCHED
                        group_name = (
                            delivery["group_name"]
                            if delivery is not None
                            else loads(intent["payload_json"], {}).get("group_name", "unknown")
                        )
                        self._recompute_group_offset(conn, group_name, queue_name, ts)
                        action_row = {
                            "intent_id": intent["intent_id"],
                            "kind": kind,
                            "queue_name": queue_name,
                            "message_id": intent["message_id"],
                            "delivery_id": intent["delivery_id"],
                            "from_state": from_state,
                            "to_state": to_state,
                            "action": "ack_rolled_back_to_unconfirmed",
                        }
                    else:
                        action_row = {
                            "intent_id": intent["intent_id"],
                            "kind": kind,
                            "queue_name": queue_name,
                            "action": "non_business_intent_rolled_back",
                        }
                    self._finish_intent_in_tx(
                        conn,
                        intent["intent_id"],
                        INTENT_ROLLED_BACK,
                        result=action_row,
                    )
                    self._bump_revision(conn)
                    actions.append(action_row)

        aborted_requests: list[str] = []
        with self.db.read_tx() as conn:
            running_requests = [
                r["request_id"]
                for r in conn.execute(
                    "SELECT request_id FROM request_results WHERE status = ?",
                    (REQUEST_RUNNING,),
                ).fetchall()
            ]
        if running_requests:
            with self.db.write_tx() as conn:
                for request_id in running_requests:
                    conn.execute(
                        """
                        UPDATE request_results
                           SET status = ?, error_json = ?, updated_at_ms = ?
                         WHERE request_id = ? AND status = ?
                        """,
                        (
                            REQUEST_ABORTED,
                            dumps({"code": "E_RECOVERED", "message": "owner terminated"}),
                            now_ms(),
                            request_id,
                            REQUEST_RUNNING,
                        ),
                    )
                    aborted_requests.append(request_id)
                conn.execute(
                    "UPDATE rounds SET status = 'ABORTED' WHERE status = 'RUNNING'"
                )
                self._bump_revision(conn)

        reap_result = self.reap_expired() if run_expired else None
        finished = now_ms()
        with self.db.read_tx() as conn:
            intent_counts = {
                row["state"]: int(row["n"])
                for row in conn.execute(
                    "SELECT state, COUNT(*) AS n FROM intents GROUP BY state"
                ).fetchall()
            }
            status_counts = {
                row["status"]: int(row["n"])
                for row in conn.execute(
                    "SELECT status, COUNT(*) AS n FROM messages GROUP BY status"
                ).fetchall()
            }
            revision = int(
                conn.execute("SELECT value FROM revision WHERE id = 1").fetchone()["value"]
            )
        report = {
            "ok": True,
            "report_id": report_id,
            "started_at_ms": started,
            "finished_at_ms": finished,
            "elapsed_ms": finished - started,
            "unfinished_intents_scanned": len(unfinished),
            "actions": actions,
            "dispatch_rollbacks": [
                a for a in actions if a.get("action") == "dispatch_rolled_back_to_pending"
            ],
            "ack_rollbacks": [
                a for a in actions if a.get("action") == "ack_rolled_back_to_unconfirmed"
            ],
            "preserved": [
                a for a in actions if "preserved" in a.get("action", "")
            ],
            "aborted_request_ids": aborted_requests,
            "post_recovery_reap": reap_result,
            "durable_facts_after_recovery": {
                "revision": revision,
                "intent_counts": intent_counts,
                "message_status_counts": status_counts,
            },
        }
        with self.db.write_tx() as conn:
            conn.execute(
                """
                INSERT INTO recovery_reports(report_id, created_at_ms, report_json)
                VALUES (?, ?, ?)
                """,
                (report_id, finished, dumps(report)),
            )
            self._bump_revision(conn)
        report["durable_facts_after_recovery"]["revision"] = self.revision()
        return report

    def get_latest_recovery_report(self) -> dict[str, Any] | None:
        with self.db.read_tx() as conn:
            row = conn.execute(
                "SELECT * FROM recovery_reports ORDER BY created_at_ms DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return loads(row["report_json"], None)
