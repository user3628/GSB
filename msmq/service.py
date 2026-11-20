from __future__ import annotations

import copy
import json
import os
import sys
import uuid
from typing import Any, Iterable

from .compat import COMPATIBILITY_MATRIX, FORMAT_COMPACT, FORMAT_JSONL
from .db import Database
from .errors import (
    E_ALREADY_ACKED,
    E_BAD_REQUEST,
    E_CONFLICT_AMBIGUOUS,
    E_DUPLICATE_ACK,
    E_FORMAT_INVALID,
    E_INTERNAL,
    E_LEASE_MISMATCH,
    E_MESSAGE_EXISTS,
    E_MESSAGE_NOT_FOUND,
    E_NOT_FOUND,
    E_QUEUE_BUSY,
    E_QUEUE_EXISTS,
    E_QUEUE_MODE_MISMATCH,
    E_QUEUE_NOT_FOUND,
    E_RECORD_TOO_LARGE,
    MQError,
)
from .formats import (
    COMPACT_MAX_BODY_BYTES,
    decode_auto,
    encode_compact,
    encode_jsonl,
    sha256_bytes,
)
from .models import (
    DELIVERY_STATE_PRIORITY,
    MessageRecord,
    ParsedBatch,
    QueueMeta,
    message_payload_equal,
)
from .timeutil import now_us

ALLOWED_SEMANTICS = {"at_least_once", "exactly_once"}


def _queue_name(value: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or len(value) > 128:
        raise MQError(
            E_BAD_REQUEST,
            "queue name must be a trimmed non-empty string of at most 128 characters",
            http_status=422,
        )
    if any(ord(char) < 32 for char in value):
        raise MQError(E_BAD_REQUEST, "queue name may not contain control characters", http_status=422)
    return value


def _message_id(value: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or len(value) > 256:
        raise MQError(
            E_BAD_REQUEST,
            "message id must be a trimmed non-empty string of at most 256 characters",
            http_status=422,
        )
    if any(ord(char) < 32 for char in value):
        raise MQError(E_BAD_REQUEST, "message id may not contain control characters", http_status=422)
    return value


def _consumer_id(value: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or len(value) > 256:
        raise MQError(
            E_BAD_REQUEST,
            "consumer id must be a trimmed non-empty string of at most 256 characters",
            http_status=422,
        )
    return value


def _validate_body(body: str) -> bytes:
    if not isinstance(body, str):
        raise MQError(E_BAD_REQUEST, "message body must be a string", http_status=422)
    body_bytes = body.encode("utf-8")
    if len(body_bytes) > COMPACT_MAX_BODY_BYTES:
        raise MQError(
            E_RECORD_TOO_LARGE,
            "message body exceeds the largest supported format capacity",
            http_status=413,
            details={
                "body_bytes": len(body_bytes),
                "max_body_bytes": COMPACT_MAX_BODY_BYTES,
                "format": FORMAT_COMPACT,
            },
        )
    return body_bytes


def _validate_extensions(extensions: dict[str, Any] | None) -> dict[str, Any]:
    if extensions is None:
        return {}
    if not isinstance(extensions, dict):
        raise MQError(E_BAD_REQUEST, "extensions must be a JSON object", http_status=422)
    encoded = json.dumps(extensions, ensure_ascii=False, separators=(",", ":"))
    if len(encoded.encode("utf-8")) > 8192:
        raise MQError(
            E_RECORD_TOO_LARGE,
            "extensions exceed the supported optional-field capacity",
            http_status=413,
            details={"max_extension_bytes": 8192},
        )
    return extensions


def _queue_from_row(row: Any) -> QueueMeta:
    return QueueMeta(
        name=row["name"],
        semantics=row["semantics"],
        visibility_timeout_ms=row["visibility_timeout_ms"],
        version=row["version"],
        extensions=json.loads(row["extensions_json"]),
    )


def _message_from_row(row: Any) -> MessageRecord:
    return MessageRecord(
        message_id=row["id"],
        queue=row["queue_name"],
        body=row["body"],
        produced_at_us=row["produced_at_us"],
        expires_at_us=row["expires_at_us"],
        content_type=row["content_type"],
        state=row["state"],
        record_version=row["record_version"],
        last_write_at_us=row["last_write_at_us"],
        delivery_count=row["delivery_count"],
        extensions=json.loads(row["extensions_json"]),
        lease_owner=row["lease_owner"],
        lease_token=row["lease_token"],
        lease_expires_at_us=row["lease_expires_at_us"],
        source_format=row["source_format"],
        source_version=row["source_version"],
    )


class MQService:
    def __init__(self, db: Database, *, enable_failpoints: bool = False) -> None:
        self.db = db
        self.enable_failpoints = enable_failpoints

    def _maybe_crash(self, stage: str, requested: str | None) -> None:
        if not self.enable_failpoints or requested is None:
            return
        if requested in {stage, "all"}:
            print(f"MSMQ_FAILPOINT_TRIGGERED stage={stage}", file=sys.stderr, flush=True)
            os._exit(97)

    # ------------------------------------------------------------------ queues
    def create_queue(
        self,
        *,
        name: str,
        semantics: str,
        visibility_timeout_ms: int = 30_000,
        extensions: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        name = _queue_name(name)
        if semantics not in ALLOWED_SEMANTICS:
            raise MQError(
                E_BAD_REQUEST,
                "semantics must be at_least_once or exactly_once",
                http_status=422,
            )
        if not isinstance(visibility_timeout_ms, int) or not (1 <= visibility_timeout_ms <= 3_600_000):
            raise MQError(
                E_BAD_REQUEST,
                "visibility_timeout_ms must be within 1..3600000",
                http_status=422,
            )
        extensions = _validate_extensions(extensions)
        timestamp = now_us()
        with self.db.transaction() as connection:
            existing = connection.execute("SELECT name FROM queues WHERE name = ?", (name,)).fetchone()
            if existing is not None:
                raise MQError(
                    E_QUEUE_EXISTS,
                    "queue already exists",
                    http_status=409,
                    details={"queue": name},
                )
            connection.execute(
                """
                INSERT INTO queues(name, semantics, visibility_timeout_ms, version, extensions_json,
                                   created_at_us, updated_at_us)
                VALUES (?, ?, ?, 1, ?, ?, ?)
                """,
                (name, semantics, visibility_timeout_ms, json.dumps(extensions), timestamp, timestamp),
            )
            self.db.write_queue_snapshot(connection, name, timestamp=timestamp)
        return self.get_queue(name)

    def get_queue(self, name: str) -> dict[str, Any]:
        name = _queue_name(name)
        connection = self.db.connect()
        try:
            row = connection.execute("SELECT * FROM queues WHERE name = ?", (name,)).fetchone()
            if row is None:
                raise MQError(
                    E_QUEUE_NOT_FOUND,
                    "queue does not exist",
                    http_status=404,
                    details={"queue": name},
                )
            return _queue_from_row(row).to_dict()
        finally:
            connection.close()

    def migrate_queue(
        self,
        *,
        name: str,
        semantics: str,
        visibility_timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        name = _queue_name(name)
        if semantics not in ALLOWED_SEMANTICS:
            raise MQError(E_BAD_REQUEST, "invalid target semantics", http_status=422)
        if visibility_timeout_ms is not None and not (1 <= visibility_timeout_ms <= 3_600_000):
            raise MQError(E_BAD_REQUEST, "invalid visibility timeout", http_status=422)
        timestamp = now_us()
        with self.db.transaction() as connection:
            row = connection.execute("SELECT * FROM queues WHERE name = ?", (name,)).fetchone()
            if row is None:
                raise MQError(
                    E_QUEUE_NOT_FOUND,
                    "queue does not exist",
                    http_status=404,
                    details={"queue": name},
                )
            in_flight = connection.execute(
                """
                SELECT COUNT(*) AS n FROM messages
                WHERE queue_name = ? AND state = 'leased' AND lease_expires_at_us > ?
                """,
                (name, timestamp),
            ).fetchone()["n"]
            if in_flight:
                raise MQError(
                    E_QUEUE_BUSY,
                    "semantics migration is blocked while leases are active",
                    http_status=409,
                    details={"queue": name, "active_leases": in_flight},
                )
            prior = row["semantics"]
            connection.execute(
                """
                UPDATE queues SET semantics = ?, visibility_timeout_ms = ?, updated_at_us = ?
                WHERE name = ?
                """,
                (
                    semantics,
                    visibility_timeout_ms if visibility_timeout_ms is not None else row["visibility_timeout_ms"],
                    timestamp,
                    name,
                ),
            )
            version = self.db.bump_queue_version_and_snapshot(connection, name, timestamp=timestamp)
        return {
            "queue": name,
            "previous_semantics": prior,
            "semantics": semantics,
            "explicit_migration": True,
            "queue_version": version,
        }

    # ---------------------------------------------------------------- enqueue
    def enqueue(
        self,
        *,
        queue: str,
        body: str,
        message_id: str | None = None,
        content_type: str = "text/plain",
        ttl_ms: int = 60_000,
        produced_at_ms: int | None = None,
        extensions: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        queue = _queue_name(queue)
        _validate_body(body)
        message_id = _message_id(message_id or f"msg-{uuid.uuid4().hex}")
        if not isinstance(ttl_ms, int):
            raise MQError(E_BAD_REQUEST, "ttl_ms must be an integer", http_status=422)
        if produced_at_ms is not None and (not isinstance(produced_at_ms, int) or produced_at_ms < 0):
            raise MQError(E_BAD_REQUEST, "produced_at_ms must be a non-negative integer", http_status=422)
        if not isinstance(content_type, str) or not content_type or len(content_type) > 128:
            raise MQError(E_BAD_REQUEST, "content_type must be a non-empty string", http_status=422)
        extensions = _validate_extensions(extensions)
        produced_at_us = produced_at_ms * 1_000 if produced_at_ms is not None else now_us()
        expires_at_us = produced_at_us + ttl_ms * 1_000
        timestamp = now_us()
        intent_id = self.db.begin_intent(
            "enqueue", {"queue": queue, "message_id": message_id, "size": len(body.encode("utf-8"))}
        )
        try:
            with self.db.transaction() as connection:
                queue_row = connection.execute("SELECT * FROM queues WHERE name = ?", (queue,)).fetchone()
                if queue_row is None:
                    raise MQError(
                        E_QUEUE_NOT_FOUND,
                        "queue does not exist",
                        http_status=404,
                        details={"queue": queue},
                    )
                existing = connection.execute("SELECT id FROM messages WHERE id = ?", (message_id,)).fetchone()
                if existing is not None:
                    raise MQError(
                        E_MESSAGE_EXISTS,
                        "message id already exists",
                        http_status=409,
                        details={"message_id": message_id},
                    )
                connection.execute(
                    """
                    INSERT INTO messages(id, queue_name, body, content_type, produced_at_us, expires_at_us,
                                         state, record_version, last_write_at_us, delivery_count,
                                         lease_owner, lease_token, lease_expires_at_us, lease_intent_id,
                                         extensions_json, source_format, source_version,
                                         created_at_us, updated_at_us)
                    VALUES (?, ?, ?, ?, ?, ?, 'pending', 1, ?, 0, NULL, NULL, NULL, NULL, ?, 'api', 1, ?, ?)
                    """,
                    (
                        message_id,
                        queue,
                        body,
                        content_type,
                        produced_at_us,
                        expires_at_us,
                        timestamp,
                        json.dumps(extensions),
                        timestamp,
                        timestamp,
                    ),
                )
                version = self.db.bump_queue_version_and_snapshot(connection, queue, timestamp=timestamp)
                self.db.mark_intent_committed(connection, intent_id, {"message_id": message_id})
        except BaseException as exc:
            self.db.abort_intent(
                intent_id,
                {"code": getattr(exc, "code", E_INTERNAL), "message": str(exc)},
            )
            raise
        return {
            "message_id": message_id,
            "queue": queue,
            "state": "pending",
            "produced_at_us": produced_at_us,
            "expires_at_us": expires_at_us,
            "queue_version": version,
        }

    # ------------------------------------------------------------------ claim
    def claim(
        self,
        *,
        queue: str,
        consumer_id: str,
        visibility_timeout_ms: int | None = None,
        failpoint: str | None = None,
    ) -> dict[str, Any]:
        queue = _queue_name(queue)
        consumer_id = _consumer_id(consumer_id)
        self.get_queue(queue)
        intent_id = self.db.begin_intent(
            "claim",
            {"queue": queue, "consumer_id": consumer_id, "visibility_timeout_ms": visibility_timeout_ms},
        )
        try:
            self._maybe_crash("claim_after_intent", failpoint)
            with self.db.transaction() as connection:
                queue_row = connection.execute("SELECT * FROM queues WHERE name = ?", (queue,)).fetchone()
                if queue_row is None:
                    raise MQError(E_QUEUE_NOT_FOUND, "queue does not exist", http_status=404)
                timeout_ms = visibility_timeout_ms or queue_row["visibility_timeout_ms"]
                if not (1 <= timeout_ms <= 3_600_000):
                    raise MQError(E_BAD_REQUEST, "invalid visibility timeout", http_status=422)
                timestamp = now_us()
                candidate = connection.execute(
                    """
                    SELECT * FROM messages
                    WHERE queue_name = ? AND expires_at_us > ?
                      AND (state = 'pending' OR (state = 'leased' AND lease_expires_at_us <= ?))
                    ORDER BY produced_at_us, id
                    LIMIT 1
                    """,
                    (queue, timestamp, timestamp),
                ).fetchone()
                if candidate is None:
                    self.db.mark_intent_committed(connection, intent_id, {"message_id": None})
                    return {"message": None, "consumer_id": consumer_id}
                lease_token = uuid.uuid4().hex
                attempt = candidate["delivery_count"] + 1
                lease_expires_at_us = timestamp + timeout_ms * 1_000
                connection.execute(
                    """
                    UPDATE messages SET state = 'leased', delivery_count = ?, lease_owner = ?,
                        lease_token = ?, lease_expires_at_us = ?, lease_intent_id = ?,
                        last_write_at_us = ?, updated_at_us = ?
                    WHERE id = ?
                    """,
                    (
                        attempt,
                        consumer_id,
                        lease_token,
                        lease_expires_at_us,
                        intent_id,
                        timestamp,
                        timestamp,
                        candidate["id"],
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO deliveries(message_id, queue_name, consumer_id, lease_token, attempt,
                                           claimed_at_us, lease_expires_at_us, status, intent_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'leased', ?)
                    """,
                    (
                        candidate["id"],
                        queue,
                        consumer_id,
                        lease_token,
                        attempt,
                        timestamp,
                        lease_expires_at_us,
                        intent_id,
                    ),
                )
                version = self.db.bump_queue_version_and_snapshot(connection, queue, timestamp=timestamp)
                self._maybe_crash("claim_mid_transaction", failpoint)
                self.db.mark_intent_committed(
                    connection,
                    intent_id,
                    {"message_id": candidate["id"], "attempt": attempt, "lease_token": lease_token},
                )
        except BaseException as exc:
            self.db.abort_intent(
                intent_id,
                {"code": getattr(exc, "code", E_INTERNAL), "message": str(exc)},
            )
            raise
        return {
            "message": {
                "message_id": candidate["id"],
                "queue": queue,
                "body": candidate["body"],
                "content_type": candidate["content_type"],
                "produced_at_us": candidate["produced_at_us"],
                "expires_at_us": candidate["expires_at_us"],
                "delivery_count": attempt,
                "lease_token": lease_token,
                "lease_expires_at_us": lease_expires_at_us,
                "state": "leased",
                "record_version": candidate["record_version"],
                "extensions": json.loads(candidate["extensions_json"]),
            },
            "consumer_id": consumer_id,
            "queue_version": version,
        }

    # ------------------------------------------------------------------- acks
    def _ack_one(
        self,
        *,
        queue: str,
        consumer_id: str,
        message_id: str,
        lease_token: str,
        failpoint: str | None,
    ) -> dict[str, Any]:
        connection = self.db.connect()
        try:
            message_row = connection.execute(
                "SELECT * FROM messages WHERE id = ? AND queue_name = ?", (message_id, queue)
            ).fetchone()
            queue_row = connection.execute("SELECT * FROM queues WHERE name = ?", (queue,)).fetchone()
        finally:
            connection.close()
        if queue_row is None:
            raise MQError(E_QUEUE_NOT_FOUND, "queue does not exist", http_status=404)
        if message_row is None:
            raise MQError(
                E_MESSAGE_NOT_FOUND,
                "message does not exist in the queue",
                http_status=404,
                details={"message_id": message_id, "queue": queue},
            )
        pre_state = {
            "message_id": message_id,
            "state": message_row["state"],
            "lease_owner": message_row["lease_owner"],
            "lease_token": message_row["lease_token"],
            "lease_expires_at_us": message_row["lease_expires_at_us"],
            "delivery_count": message_row["delivery_count"],
            "last_write_at_us": message_row["last_write_at_us"],
        }
        intent_id = self.db.begin_intent(
            "ack",
            {
                "queue": queue,
                "consumer_id": consumer_id,
                "message_id": message_id,
                "lease_token": lease_token,
                "pre_state": pre_state,
            },
        )
        try:
            with self.db.transaction() as connection:
                queue_row = connection.execute("SELECT * FROM queues WHERE name = ?", (queue,)).fetchone()
                if queue_row is None:
                    raise MQError(E_QUEUE_NOT_FOUND, "queue does not exist", http_status=404)
                row = connection.execute("SELECT * FROM messages WHERE id = ?", (message_id,)).fetchone()
                if row is None or row["queue_name"] != queue:
                    raise MQError(
                        E_MESSAGE_NOT_FOUND,
                        "message does not exist in the queue",
                        http_status=404,
                        details={"message_id": message_id, "queue": queue},
                    )
                duplicate = connection.execute(
                    "SELECT * FROM acks WHERE message_id = ? AND consumer_id = ?",
                    (message_id, consumer_id),
                ).fetchone()
                if duplicate is not None:
                    code = E_DUPLICATE_ACK if queue_row["semantics"] == "exactly_once" else E_ALREADY_ACKED
                    raise MQError(
                        code,
                        "message was already acknowledged by this consumer",
                        http_status=409,
                        details={
                            "message_id": message_id,
                            "consumer_id": consumer_id,
                            "delivery_count": duplicate["delivery_count"],
                            "idempotency_key": f"{message_id}:{consumer_id}",
                        },
                    )
                if row["state"] == "acked":
                    raise MQError(
                        E_ALREADY_ACKED,
                        "message is already acknowledged",
                        http_status=409,
                        details={"message_id": message_id},
                    )
                if not lease_token or row["lease_token"] != lease_token:
                    raise MQError(
                        E_LEASE_MISMATCH,
                        "lease token does not match the current delivery",
                        http_status=409,
                        details={"message_id": message_id},
                    )
                timestamp = now_us()
                connection.execute(
                    """
                    INSERT INTO acks(message_id, consumer_id, acked_at_us, delivery_count, intent_id)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (message_id, consumer_id, timestamp, row["delivery_count"], intent_id),
                )
                connection.execute(
                    """
                    UPDATE messages SET state = 'acked', lease_owner = NULL, lease_token = NULL,
                        lease_expires_at_us = NULL, lease_intent_id = NULL,
                        last_write_at_us = ?, updated_at_us = ?
                    WHERE id = ?
                    """,
                    (timestamp, timestamp, message_id),
                )
                connection.execute(
                    """
                    UPDATE deliveries SET status = 'acked'
                    WHERE message_id = ? AND lease_token = ? AND status = 'leased'
                    """,
                    (message_id, lease_token),
                )
                version = self.db.bump_queue_version_and_snapshot(connection, queue, timestamp=timestamp)
                self._maybe_crash("ack_mid_transaction", failpoint)
                self.db.mark_intent_committed(
                    connection,
                    intent_id,
                    {"message_id": message_id, "consumer_id": consumer_id, "acked_at_us": timestamp},
                )
        except BaseException as exc:
            self.db.abort_intent(
                intent_id,
                {"code": getattr(exc, "code", E_INTERNAL), "message": str(exc)},
            )
            raise
        return {
            "message_id": message_id,
            "consumer_id": consumer_id,
            "acked_at_us": timestamp,
            "delivery_count": row["delivery_count"],
            "queue_version": version,
        }

    def ack_batch(
        self,
        *,
        queue: str,
        consumer_id: str,
        acks: Iterable[dict[str, Any]],
        failpoint: str | None = None,
    ) -> dict[str, Any]:
        queue = _queue_name(queue)
        consumer_id = _consumer_id(consumer_id)
        self.get_queue(queue)
        results: list[dict[str, Any]] = []
        for index, item in enumerate(acks):
            message_id = item.get("message_id")
            lease_token = item.get("lease_token", "")
            try:
                message_id = _message_id(message_id)
                acknowledged = self._ack_one(
                    queue=queue,
                    consumer_id=consumer_id,
                    message_id=message_id,
                    lease_token=lease_token,
                    failpoint=failpoint,
                )
                results.append({"index": index, "ok": True, **acknowledged})
            except MQError as exc:
                results.append(
                    {
                        "index": index,
                        "ok": False,
                        "message_id": message_id,
                        "error": {"code": exc.code, "message": exc.message, "details": exc.details},
                    }
                )
        succeeded = sum(1 for item in results if item["ok"])
        return {
            "queue": queue,
            "consumer_id": consumer_id,
            "results": results,
            "succeeded": succeeded,
            "failed": len(results) - succeeded,
            "partial_success": bool(succeeded and succeeded != len(results)),
        }

    # --------------------------------------------------------------- snapshot
    def snapshot(self, queue: str) -> dict[str, Any]:
        queue = _queue_name(queue)
        connection = self.db.connect()
        try:
            connection.execute("BEGIN")
            queue_row = connection.execute("SELECT * FROM queues WHERE name = ?", (queue,)).fetchone()
            if queue_row is None:
                raise MQError(
                    E_QUEUE_NOT_FOUND,
                    "queue does not exist",
                    http_status=404,
                    details={"queue": queue},
                )
            rows = connection.execute(
                "SELECT * FROM messages WHERE queue_name = ? ORDER BY id", (queue,)
            ).fetchall()
            timestamp = now_us()
            counts = {"pending": 0, "leased": 0, "acked": 0, "expired": 0}
            messages: list[dict[str, Any]] = []
            for row in rows:
                effective_state = row["state"]
                if effective_state in {"pending", "leased"} and row["expires_at_us"] <= timestamp:
                    effective_state = "expired"
                counts[effective_state] += 1
                messages.append(
                    {
                        "message_id": row["id"],
                        "queue": row["queue_name"],
                        "body": row["body"],
                        "content_type": row["content_type"],
                        "produced_at_us": row["produced_at_us"],
                        "expires_at_us": row["expires_at_us"],
                        "state": effective_state,
                        "stored_state": row["state"],
                        "record_version": row["record_version"],
                        "last_write_at_us": row["last_write_at_us"],
                        "delivery_count": row["delivery_count"],
                        "lease_owner": row["lease_owner"],
                        "lease_token": row["lease_token"],
                        "lease_expires_at_us": row["lease_expires_at_us"],
                        "extensions": json.loads(row["extensions_json"]),
                        "source_format": row["source_format"],
                        "source_version": row["source_version"],
                    }
                )
            ack_count = connection.execute(
                "SELECT COUNT(*) AS n FROM acks WHERE message_id IN (SELECT id FROM messages WHERE queue_name = ?)",
                (queue,),
            ).fetchone()["n"]
            delivery_count = connection.execute(
                "SELECT COUNT(*) AS n FROM deliveries WHERE queue_name = ?", (queue,)
            ).fetchone()["n"]
            snapshot_row = connection.execute(
                """
                SELECT queue_version, snapshot_json, created_at_us FROM queue_snapshots
                WHERE queue_name = ? ORDER BY queue_version DESC, id DESC LIMIT 1
                """,
                (queue,),
            ).fetchone()
            connection.execute("COMMIT")
        except Exception:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        return {
            "queue": _queue_from_row(queue_row).to_dict(),
            "counts": counts,
            "message_count": len(messages),
            "ack_count": ack_count,
            "delivery_record_count": delivery_count,
            "messages": messages,
            "latest_version_snapshot": (
                {
                    "queue_version": snapshot_row["queue_version"],
                    "created_at_us": snapshot_row["created_at_us"],
                    "snapshot": json.loads(snapshot_row["snapshot_json"]),
                }
                if snapshot_row
                else None
            ),
            "latest_import_report": self.db.latest_import_report(),
        }

    def version_snapshots(self, queue: str, *, limit: int = 50) -> list[dict[str, Any]]:
        queue = _queue_name(queue)
        if not (1 <= limit <= 500):
            raise MQError(E_BAD_REQUEST, "limit must be within 1..500", http_status=422)
        connection = self.db.connect()
        try:
            queue_row = connection.execute("SELECT name FROM queues WHERE name = ?", (queue,)).fetchone()
            if queue_row is None:
                raise MQError(
                    E_QUEUE_NOT_FOUND,
                    "queue does not exist",
                    http_status=404,
                    details={"queue": queue},
                )
            rows = connection.execute(
                """
                SELECT queue_version, snapshot_json, created_at_us FROM queue_snapshots
                WHERE queue_name = ? ORDER BY queue_version DESC, id DESC LIMIT ?
                """,
                (queue, limit),
            ).fetchall()
            return [
                {
                    "queue_version": row["queue_version"],
                    "created_at_us": row["created_at_us"],
                    "snapshot": json.loads(row["snapshot_json"]),
                }
                for row in rows
            ]
        finally:
            connection.close()

    # ---------------------------------------------------------------- exports
    def _read_export_batch(self, queue: str | None) -> tuple[list[QueueMeta], list[MessageRecord]]:
        connection = self.db.connect()
        try:
            connection.execute("BEGIN")
            if queue is not None:
                queue = _queue_name(queue)
                queue_rows = connection.execute("SELECT * FROM queues WHERE name = ?", (queue,)).fetchall()
                if not queue_rows:
                    raise MQError(
                        E_QUEUE_NOT_FOUND,
                        "queue does not exist",
                        http_status=404,
                        details={"queue": queue},
                    )
                message_rows = connection.execute(
                    "SELECT * FROM messages WHERE queue_name = ? ORDER BY produced_at_us, id", (queue,)
                ).fetchall()
            else:
                queue_rows = connection.execute("SELECT * FROM queues ORDER BY name").fetchall()
                message_rows = connection.execute(
                    "SELECT * FROM messages ORDER BY queue_name, produced_at_us, id"
                ).fetchall()
            queues = [_queue_from_row(row) for row in queue_rows]
            messages = [_message_from_row(row) for row in message_rows]
            connection.execute("COMMIT")
            return queues, messages
        except Exception:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    def export_compact(self, queue: str | None = None) -> bytes:
        queues, messages = self._read_export_batch(queue)
        return encode_compact(messages, queues)

    def export_jsonl(self, queue: str | None = None) -> bytes:
        queues, messages = self._read_export_batch(queue)
        return encode_jsonl(messages, queues)

    # ----------------------------------------------------------------- import
    def _resolve_conflict(
        self, existing: MessageRecord, incoming: MessageRecord
    ) -> tuple[str, str]:
        existing_priority = DELIVERY_STATE_PRIORITY[existing.state]
        incoming_priority = DELIVERY_STATE_PRIORITY[incoming.state]
        if incoming_priority != existing_priority:
            return (
                ("overwrite", "higher_delivery_state_priority")
                if incoming_priority > existing_priority
                else ("skip", "lower_delivery_state_priority")
            )
        if incoming.record_version != existing.record_version:
            return (
                ("overwrite", "higher_record_version")
                if incoming.record_version > existing.record_version
                else ("skip", "lower_record_version")
            )
        incoming_write = int(incoming.last_write_at_us or 0)
        existing_write = int(existing.last_write_at_us or 0)
        if incoming_write != existing_write:
            return (
                ("overwrite", "later_last_write")
                if incoming_write > existing_write
                else ("skip", "earlier_last_write")
            )
        if message_payload_equal(existing, incoming):
            return "skip", "identical"
        return "reject", "same_priority_version_and_write_time_with_different_payload"

    def _apply_import(self, parsed: ParsedBatch, intent_id: str, failpoint: str | None) -> dict[str, Any]:
        report = copy.deepcopy(parsed.decode_report)
        report.update(
            {
                "format": parsed.format,
                "format_version": parsed.version,
                "decision": parsed.decision,
                "accepted": True,
                "committed": False,
                "atomic": True,
                "records_total": len(parsed.messages),
                "queues_total": len(parsed.queues),
                "inserted": 0,
                "overwritten": 0,
                "skipped": 0,
                "rejected": 0,
                "conflicts": [],
                "queue_metadata_conflicts": [],
                "degradations": report.get("degraded_fields", []),
                "preserved_optional_fields": report.get("unknown_optional_keys", []),
                "state_changed": False,
            }
        )
        duplicate_ids: dict[str, list[int]] = {}
        for index, message in enumerate(parsed.messages):
            duplicate_ids.setdefault(message.message_id, []).append(index)
        duplicate = next(((mid, indexes) for mid, indexes in duplicate_ids.items() if len(indexes) > 1), None)
        if duplicate is not None:
            report["rejected"] = 1
            report["rejected_index"] = duplicate[1][1]
            report["reject_code"] = E_FORMAT_INVALID
            report["conflicts"].append(
                {
                    "index": duplicate[1][1],
                    "message_id": duplicate[0],
                    "outcome": "reject",
                    "reason": "duplicate_message_id_within_batch",
                    "duplicate_indices": duplicate[1],
                }
            )
            raise MQError(
                E_FORMAT_INVALID,
                "batch contains duplicate message identifiers",
                http_status=422,
                details={
                    "rejected_index": duplicate[1][1],
                    "message_id": duplicate[0],
                    "state_changed": False,
                    "report": report,
                },
            )
        queue_map = {queue.name: queue for queue in parsed.queues}
        missing_queue = next((i for i, m in enumerate(parsed.messages) if m.queue not in queue_map), None)
        if missing_queue is not None:
            report["rejected"] = 1
            report["rejected_index"] = missing_queue
            report["reject_code"] = E_FORMAT_INVALID
            raise MQError(
                E_FORMAT_INVALID,
                "batch record references queue metadata that is absent",
                http_status=422,
                details={
                    "rejected_index": missing_queue,
                    "message_id": parsed.messages[missing_queue].message_id,
                    "state_changed": False,
                    "report": report,
                },
            )

        timestamp = now_us()
        modified_queues: set[str] = set()
        try:
            with self.db.transaction() as connection:
                for queue_meta in parsed.queues:
                    existing = connection.execute(
                        "SELECT * FROM queues WHERE name = ?", (queue_meta.name,)
                    ).fetchone()
                    if existing is None:
                        connection.execute(
                            """
                            INSERT INTO queues(name, semantics, visibility_timeout_ms, version,
                                               extensions_json, created_at_us, updated_at_us)
                            VALUES (?, ?, ?, 1, ?, ?, ?)
                            """,
                            (
                                queue_meta.name,
                                queue_meta.semantics,
                                queue_meta.visibility_timeout_ms,
                                json.dumps(queue_meta.extensions),
                                timestamp,
                                timestamp,
                            ),
                        )
                        modified_queues.add(queue_meta.name)
                    else:
                        if existing["semantics"] != queue_meta.semantics:
                            indexes = [
                                i for i, message in enumerate(parsed.messages) if message.queue == queue_meta.name
                            ]
                            rejected_index = indexes[0] if indexes else -1
                            report["rejected"] = 1
                            report["rejected_index"] = rejected_index
                            report["reject_code"] = E_QUEUE_MODE_MISMATCH
                            report["state_changed"] = False
                            report["queue_metadata_conflicts"].append(
                                {
                                    "queue": queue_meta.name,
                                    "outcome": "reject",
                                    "reason": "existing_queue_mode_differs_without_explicit_migration",
                                    "existing_semantics": existing["semantics"],
                                    "incoming_semantics": queue_meta.semantics,
                                }
                            )
                            raise MQError(
                                E_QUEUE_MODE_MISMATCH,
                                "import may not switch an existing queue's delivery semantics",
                                http_status=409,
                                details={
                                    "rejected_index": rejected_index,
                                    "queue": queue_meta.name,
                                    "state_changed": False,
                                    "report": report,
                                },
                            )
                        existing_extensions = json.loads(existing["extensions_json"])
                        merged_extensions = {**existing_extensions, **queue_meta.extensions}
                        if merged_extensions != existing_extensions:
                            connection.execute(
                                """
                                UPDATE queues SET extensions_json = ?, updated_at_us = ?
                                WHERE name = ?
                                """,
                                (json.dumps(merged_extensions), timestamp, queue_meta.name),
                            )
                            modified_queues.add(queue_meta.name)
                        if existing["visibility_timeout_ms"] != queue_meta.visibility_timeout_ms:
                            report["queue_metadata_conflicts"].append(
                                {
                                    "queue": queue_meta.name,
                                    "outcome": "skip",
                                    "reason": "existing_visibility_timeout_preserved",
                                    "existing_visibility_timeout_ms": existing["visibility_timeout_ms"],
                                    "incoming_visibility_timeout_ms": queue_meta.visibility_timeout_ms,
                                }
                            )

                for index, incoming in enumerate(parsed.messages):
                    existing_row = connection.execute(
                        "SELECT * FROM messages WHERE id = ?", (incoming.message_id,)
                    ).fetchone()
                    if existing_row is None:
                        connection.execute(
                            """
                            INSERT INTO messages(id, queue_name, body, content_type, produced_at_us,
                                expires_at_us, state, record_version, last_write_at_us, delivery_count,
                                lease_owner, lease_token, lease_expires_at_us, lease_intent_id,
                                extensions_json, source_format, source_version, created_at_us, updated_at_us)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)
                            """,
                            (
                                incoming.message_id,
                                incoming.queue,
                                incoming.body,
                                incoming.content_type,
                                incoming.produced_at_us,
                                incoming.expires_at_us,
                                incoming.state,
                                incoming.record_version,
                                incoming.last_write_at_us,
                                incoming.delivery_count,
                                incoming.lease_owner,
                                incoming.lease_token,
                                incoming.lease_expires_at_us,
                                json.dumps(incoming.extensions),
                                incoming.source_format,
                                incoming.source_version,
                                timestamp,
                                timestamp,
                            ),
                        )
                        report["inserted"] += 1
                        modified_queues.add(incoming.queue)
                        continue
                    existing = _message_from_row(existing_row)
                    if existing.queue != incoming.queue:
                        outcome, reason = "reject", "message_id_already_owned_by_another_queue"
                    else:
                        outcome, reason = self._resolve_conflict(existing, incoming)
                    report["conflicts"].append(
                        {
                            "index": index,
                            "message_id": incoming.message_id,
                            "outcome": outcome,
                            "reason": reason,
                            "existing": {
                                "state": existing.state,
                                "record_version": existing.record_version,
                                "last_write_at_us": existing.last_write_at_us,
                            },
                            "incoming": {
                                "state": incoming.state,
                                "record_version": incoming.record_version,
                                "last_write_at_us": incoming.last_write_at_us,
                            },
                        }
                    )
                    if outcome == "reject":
                        report["rejected"] = 1
                        report["rejected_index"] = index
                        report["reject_code"] = E_CONFLICT_AMBIGUOUS
                        report["state_changed"] = False
                        raise MQError(
                            E_CONFLICT_AMBIGUOUS,
                            "import conflict cannot be resolved deterministically",
                            http_status=409,
                            details={
                                "rejected_index": index,
                                "message_id": incoming.message_id,
                                "reason": reason,
                                "state_changed": False,
                                "report": report,
                            },
                        )
                    if outcome == "skip":
                        report["skipped"] += 1
                        continue
                    connection.execute(
                        """
                        UPDATE messages SET queue_name = ?, body = ?, content_type = ?,
                            produced_at_us = ?, expires_at_us = ?, state = ?, record_version = ?,
                            last_write_at_us = ?, delivery_count = ?, lease_owner = ?, lease_token = ?,
                            lease_expires_at_us = ?, lease_intent_id = NULL, extensions_json = ?,
                            source_format = ?, source_version = ?, updated_at_us = ?
                        WHERE id = ?
                        """,
                        (
                            incoming.queue,
                            incoming.body,
                            incoming.content_type,
                            incoming.produced_at_us,
                            incoming.expires_at_us,
                            incoming.state,
                            incoming.record_version,
                            incoming.last_write_at_us,
                            incoming.delivery_count,
                            incoming.lease_owner,
                            incoming.lease_token,
                            incoming.lease_expires_at_us,
                            json.dumps(incoming.extensions),
                            incoming.source_format,
                            incoming.source_version,
                            timestamp,
                            incoming.message_id,
                        ),
                    )
                    report["overwritten"] += 1
                    modified_queues.add(incoming.queue)

                for queue_name in sorted(modified_queues):
                    self.db.bump_queue_version_and_snapshot(connection, queue_name, timestamp=timestamp)
                report["committed"] = True
                report["state_changed"] = bool(modified_queues)
                report["committed_at_us"] = timestamp
                report["sha256"] = parsed.sha256
                report["raw_size"] = parsed.raw_size
                self._maybe_crash("import_mid_transaction", failpoint)
                connection.execute(
                    """
                    INSERT INTO import_reports(created_at_us, format, format_version, decision, committed, report_json)
                    VALUES (?, ?, ?, ?, 1, ?)
                    """,
                    (
                        timestamp,
                        parsed.format,
                        parsed.version,
                        parsed.decision,
                        json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                    ),
                )
                self.db.mark_intent_committed(
                    connection,
                    intent_id,
                    {
                        "records_total": len(parsed.messages),
                        "inserted": report["inserted"],
                        "overwritten": report["overwritten"],
                        "skipped": report["skipped"],
                    },
                )
        except MQError:
            raise
        return report

    def import_data(
        self,
        data: bytes,
        *,
        expected_format: str,
        sub_ms_policy: str = "truncate",
        failpoint: str | None = None,
    ) -> dict[str, Any]:
        intent_id = self.db.begin_intent(
            "import",
            {
                "expected_format": expected_format,
                "size": len(data),
                "sha256": sha256_bytes(data),
                "sub_ms_policy": sub_ms_policy,
            },
        )
        parsed: ParsedBatch | None = None
        try:
            parsed = decode_auto(data, expected_format=expected_format, sub_ms_policy=sub_ms_policy)
            return self._apply_import(parsed, intent_id, failpoint)
        except MQError as exc:
            exc.details.setdefault(
                "rejected_index", exc.details.get("index", -1)
            )
            self.db.abort_intent(
                intent_id,
                {
                    "code": exc.code,
                    "message": exc.message,
                    "rejected_index": exc.details.get("rejected_index"),
                    "state_changed": False,
                },
            )
            report = exc.details.get("report")
            if report is None:
                safe_error_details = {
                    key: value for key, value in exc.details.items() if key != "report"
                }
                report = {
                    "format": expected_format,
                    "format_version": parsed.version if parsed else None,
                    "decision": parsed.decision if parsed else "reject",
                    "accepted": False,
                    "committed": False,
                    "atomic": True,
                    "records_total": len(parsed.messages) if parsed else None,
                    "rejected": 1,
                    "rejected_index": exc.details.get("index", exc.details.get("rejected_index", -1)),
                    "reject_code": exc.code,
                    "state_changed": False,
                    "error": {"code": exc.code, "message": exc.message, "details": safe_error_details},
                    "sha256": sha256_bytes(data),
                    "raw_size": len(data),
                }
            else:
                report = copy.deepcopy(report)
                report["accepted"] = False
                report["committed"] = False
                report["atomic"] = True
                report["rejected"] = max(1, report.get("rejected", 1))
                report["reject_code"] = exc.code
                report["state_changed"] = False
                report["sha256"] = sha256_bytes(data)
                report["raw_size"] = len(data)
            self.db.insert_import_report(
                timestamp=now_us(),
                format_name=parsed.format if parsed else expected_format,
                format_version=parsed.version if parsed else 0,
                decision=parsed.decision if parsed else "reject",
                committed=False,
                report=report,
            )
            exc.details.setdefault("report", report)
            exc.details.setdefault("state_changed", False)
            raise
        except BaseException as exc:
            self.db.abort_intent(
                intent_id,
                {"code": E_INTERNAL, "message": str(exc), "state_changed": False},
            )
            raise

    # ------------------------------------------------------------- static data
    def compatibility_matrix(self) -> dict[str, Any]:
        return copy.deepcopy(COMPATIBILITY_MATRIX)
