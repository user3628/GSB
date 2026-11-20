from __future__ import annotations

import base64
import hashlib
import json
import os
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from app.config import Settings
from app.db import Database, sha256_bytes
from app.errors import BusinessError, ERROR_HTTP_STATUS, error


CREATING = "CREATING"
COMMITTED = "COMMITTED"
MERGING = "MERGING"
MERGED = "MERGED"
DELETED = "DELETED"

SNAPSHOT_STATES = {CREATING, COMMITTED, MERGING, MERGED, DELETED}
ACTIVE_SNAPSHOT_STATES = {CREATING, COMMITTED, MERGING}

LEGAL_TRANSITIONS: dict[str, set[str]] = {
    CREATING: {COMMITTED, DELETED},
    COMMITTED: {MERGING, DELETED},
    MERGING: {MERGED, COMMITTED},
    MERGED: set(),
    DELETED: set(),
}


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _row(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


def _bool(value: Any) -> bool:
    return bool(int(value)) if isinstance(value, (int, bool)) else bool(value)


class CowService:
    def __init__(self, db: Database, settings: Settings):
        self.db = db
        self.settings = settings
        self._locks_guard = threading.Lock()
        self._volume_locks: dict[str, threading.RLock] = {}
        self._request_locks: dict[str, threading.Lock] = {}

    # ------------------------------------------------------------------
    # Initialization and locking
    # ------------------------------------------------------------------
    def initialize(self) -> dict[str, Any]:
        self.db.initialize(self.settings, utcnow())
        return self.recover()

    def _volume_lock(self, volume_id: str) -> threading.RLock:
        with self._locks_guard:
            return self._volume_locks.setdefault(volume_id, threading.RLock())

    def _request_lock(self, request_id: str) -> threading.Lock:
        with self._locks_guard:
            return self._request_locks.setdefault(request_id, threading.Lock())

    # ------------------------------------------------------------------
    # Schema-backed helpers
    # ------------------------------------------------------------------
    def _volume(self, conn: sqlite3.Connection, volume_id: str) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM volumes WHERE volume_id = ?", (volume_id,)
        ).fetchone()
        if row is None:
            raise error(
                "E_VOLUME_NOT_FOUND",
                "The requested volume does not exist.",
                404,
                volume_id=volume_id,
            )
        return row

    def _snapshot(self, conn: sqlite3.Connection, volume_id: str, snapshot_id: str) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM snapshots WHERE volume_id = ? AND snapshot_id = ?",
            (volume_id, snapshot_id),
        ).fetchone()
        if row is None:
            raise error(
                "E_SNAPSHOT_NOT_FOUND",
                "The requested snapshot does not exist.",
                404,
                snapshot_id=snapshot_id,
            )
        return row

    def _assert_transition(self, from_state: str, to_state: str, snapshot_id: str) -> None:
        if to_state not in LEGAL_TRANSITIONS.get(from_state, set()):
            raise error(
                "E_INVALID_TRANSITION",
                f"Illegal snapshot state transition: {from_state} -> {to_state}.",
                409,
                snapshot_id=snapshot_id,
                from_state=from_state,
                to_state=to_state,
            )

    def _transition(
        self,
        conn: sqlite3.Connection,
        volume_id: str,
        snapshot_id: str,
        from_state: str,
        to_state: str,
        *,
        event_type: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self._assert_transition(from_state, to_state, snapshot_id)
        now = utcnow()
        updated = conn.execute(
            """
            UPDATE snapshots
               SET state = ?, updated_at = ?
             WHERE volume_id = ? AND snapshot_id = ? AND state = ?
            """,
            (to_state, now, volume_id, snapshot_id, from_state),
        )
        if updated.rowcount != 1:
            raise error(
                "E_INVALID_TRANSITION",
                "Snapshot state changed before the transition could be persisted.",
                409,
                snapshot_id=snapshot_id,
                expected_state=from_state,
                to_state=to_state,
            )
        self._event(
            conn,
            volume_id,
            snapshot_id,
            event_type or "STATE_TRANSITION",
            from_state,
            to_state,
            details or {},
        )

    def _event(
        self,
        conn: sqlite3.Connection,
        volume_id: str,
        snapshot_id: str,
        event_type: str,
        from_state: str | None,
        to_state: str | None,
        details: dict[str, Any] | None = None,
    ) -> str:
        event_id = new_id("event")
        conn.execute(
            """
            INSERT INTO snapshot_events(
                event_id, volume_id, snapshot_id, event_type,
                from_state, to_state, details_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                volume_id,
                snapshot_id,
                event_type,
                from_state,
                to_state,
                canonical_json(details or {}),
                utcnow(),
            ),
        )
        return event_id

    def _insert_intent(
        self,
        conn: sqlite3.Connection,
        volume_id: str,
        request_id: str,
        operation: str,
        payload: dict[str, Any],
    ) -> str:
        intent_id = new_id("intent")
        now = utcnow()
        conn.execute(
            """
            INSERT INTO intents(
                intent_id, volume_id, request_id, operation, state,
                payload_json, details_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'PENDING', ?, NULL, ?, ?)
            """,
            (intent_id, volume_id, request_id, operation, canonical_json(payload), now, now),
        )
        return intent_id

    def _finish_intent(
        self,
        conn: sqlite3.Connection,
        intent_id: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        conn.execute(
            """
            UPDATE intents
               SET state = 'COMMITTED', details_json = ?, updated_at = ?
             WHERE intent_id = ? AND state = 'PENDING'
            """,
            (canonical_json(details or {}), utcnow(), intent_id),
        )

    def _finish_request_tx(
        self,
        conn: sqlite3.Connection,
        request_id: str,
        response: dict[str, Any],
    ) -> None:
        conn.execute(
            """
            UPDATE requests
               SET state = 'COMPLETED', response_json = ?, error_code = NULL, updated_at = ?
             WHERE request_id = ? AND state = 'IN_PROGRESS'
            """,
            (canonical_json(response), utcnow(), request_id),
        )

    def _current_map(self, conn: sqlite3.Connection, volume_id: str) -> dict[int, dict[str, Any]]:
        rows = conn.execute(
            """
            SELECT cb.block_no, cb.block_version AS version,
                   bv.digest, bv.written_at
              FROM current_blocks cb
              JOIN block_versions bv
                ON bv.volume_id = cb.volume_id
               AND bv.block_no = cb.block_no
               AND bv.version = cb.block_version
             WHERE cb.volume_id = ?
             ORDER BY cb.block_no
            """,
            (volume_id,),
        ).fetchall()
        return {int(row["block_no"]): dict(row) for row in rows}

    def _active_version_ref_count(
        self,
        conn: sqlite3.Connection,
        volume_id: str,
        block_no: int,
        version: int,
    ) -> int:
        placeholders = ",".join("?" for _ in ACTIVE_SNAPSHOT_STATES)
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS c
              FROM snapshot_refs sr
              JOIN snapshots s
                ON s.volume_id = sr.volume_id
               AND s.snapshot_id = sr.snapshot_id
             WHERE sr.volume_id = ? AND sr.block_no = ? AND sr.block_version = ?
               AND s.state IN ({placeholders})
            """,
            (volume_id, block_no, version, *sorted(ACTIVE_SNAPSHOT_STATES)),
        ).fetchone()
        return int(row["c"])

    def _resolve_snapshot(
        self,
        conn: sqlite3.Connection,
        volume_id: str,
        snapshot_id: str,
    ) -> tuple[dict[int, dict[str, Any]], list[dict[str, Any]]]:
        resolved: dict[int, dict[str, Any]] = {}
        lineage: list[dict[str, Any]] = []
        seen: set[str] = set()
        current: str | None = snapshot_id
        while current is not None:
            if current in seen:
                raise error(
                    "E_SNAPSHOT_CORRUPT",
                    "The snapshot parent chain contains a cycle.",
                    409,
                    snapshot_id=snapshot_id,
                    cycle_at=current,
                )
            seen.add(current)
            snap = self._snapshot(conn, volume_id, current)
            lineage.append(
                {
                    "snapshot_id": snap["snapshot_id"],
                    "state": snap["state"],
                    "mode": snap["mode"],
                    "consistency_seq": int(snap["consistency_seq"]),
                }
            )
            refs = conn.execute(
                """
                SELECT block_no, block_version, digest
                  FROM snapshot_refs
                 WHERE volume_id = ? AND snapshot_id = ?
                 ORDER BY block_no
                """,
                (volume_id, current),
            ).fetchall()
            for ref in refs:
                block_no = int(ref["block_no"])
                if block_no not in resolved:
                    resolved[block_no] = {
                        "block_no": block_no,
                        "version": int(ref["block_version"]),
                        "digest": ref["digest"],
                        "source_snapshot_id": current,
                    }
            current = snap["parent_id"]
        return resolved, lineage

    def _validate_resolved_snapshot(
        self,
        conn: sqlite3.Connection,
        volume_id: str,
        mapping: dict[int, dict[str, Any]],
        block_count: int,
        snapshot_id: str,
    ) -> None:
        missing = [block_no for block_no in range(block_count) if block_no not in mapping]
        extra = sorted(block_no for block_no in mapping if block_no < 0 or block_no >= block_count)
        if missing or extra:
            raise error(
                "E_SNAPSHOT_CORRUPT",
                "The snapshot does not resolve every block in the volume.",
                409,
                snapshot_id=snapshot_id,
                missing_blocks=missing,
                out_of_range_blocks=extra,
            )

        bad: list[dict[str, Any]] = []
        missing_content: list[int] = []
        for block_no in range(block_count):
            ref = mapping[block_no]
            version_row = conn.execute(
                """
                SELECT digest, content FROM block_versions
                 WHERE volume_id = ? AND block_no = ? AND version = ?
                """,
                (volume_id, block_no, ref["version"]),
            ).fetchone()
            if version_row is None:
                missing_content.append(block_no)
                continue
            actual = sha256_bytes(bytes(version_row["content"]))
            expected_ref = str(ref["digest"]).lower()
            expected_version = str(version_row["digest"]).lower()
            if actual != expected_ref or actual != expected_version:
                bad.append(
                    {
                        "block_no": block_no,
                        "version": ref["version"],
                        "snapshot_digest": expected_ref,
                        "version_digest": expected_version,
                        "actual_digest": actual,
                    }
                )
        if missing_content or bad:
            raise error(
                "E_SNAPSHOT_CORRUPT",
                "Snapshot block content is missing or failed digest verification.",
                409,
                snapshot_id=snapshot_id,
                missing_content_blocks=missing_content,
                digest_mismatches=bad,
            )

    def _stats(self, conn: sqlite3.Connection, volume_id: str) -> dict[str, Any]:
        volume = self._volume(conn, volume_id)
        snapshots = [dict(row) for row in conn.execute(
            "SELECT * FROM snapshots WHERE volume_id = ? ORDER BY created_at, snapshot_id",
            (volume_id,),
        ).fetchall()]
        placeholders = ",".join("?" for _ in ACTIVE_SNAPSHOT_STATES)
        active_rows = conn.execute(
            f"""
            SELECT volume_id, snapshot_id, parent_id, state, consistency_seq, ref_count
              FROM snapshots
             WHERE volume_id = ? AND state IN ({placeholders})
            """,
            (volume_id, *sorted(ACTIVE_SNAPSHOT_STATES)),
        ).fetchall()
        active = {str(row["snapshot_id"]): dict(row) for row in active_rows}

        depth_cache: dict[str, int] = {}

        def depth(snapshot_id: str, trail: set[str]) -> int:
            if snapshot_id in depth_cache:
                return depth_cache[snapshot_id]
            if snapshot_id in trail:
                raise error(
                    "E_SNAPSHOT_CORRUPT",
                    "Active snapshot parent chain contains a cycle.",
                    409,
                    snapshot_id=snapshot_id,
                )
            row = active.get(snapshot_id)
            if row is None:
                return 0
            parent = row.get("parent_id")
            value = 1
            if parent in active:
                value = 1 + depth(str(parent), trail | {snapshot_id})
            depth_cache[snapshot_id] = value
            return value

        chain_depth = max((depth(snapshot_id, set()) for snapshot_id in active), default=0)
        ref_sum = conn.execute(
            f"""
            SELECT COALESCE(SUM(ref_count), 0) AS total
              FROM snapshots
             WHERE volume_id = ? AND state IN ({placeholders})
            """,
            (volume_id, *sorted(ACTIVE_SNAPSHOT_STATES)),
        ).fetchone()
        distinct_refs = conn.execute(
            f"""
            SELECT COUNT(DISTINCT sr.block_no || ':' || sr.block_version) AS total
              FROM snapshot_refs sr
              JOIN snapshots s
                ON s.volume_id = sr.volume_id AND s.snapshot_id = sr.snapshot_id
             WHERE sr.volume_id = ? AND s.state IN ({placeholders})
            """,
            (volume_id, *sorted(ACTIVE_SNAPSHOT_STATES)),
        ).fetchone()
        active_children = {
            str(row["parent_id"])
            for row in active_rows
            if row["parent_id"] is not None
        }
        heads = sorted(set(active) - active_children)
        state_counts: dict[str, int] = {state: 0 for state in sorted(SNAPSHOT_STATES)}
        for snap in snapshots:
            state_counts[str(snap["state"])] = state_counts.get(str(snap["state"]), 0) + 1
        return {
            "volume_id": volume_id,
            "consistency_seq": int(volume["consistency_seq"]),
            "block_count": int(volume["block_count"]),
            "snapshot_count": len(snapshots),
            "active_snapshot_count": len(active),
            "state_counts": state_counts,
            "chain_depth": chain_depth,
            "active_heads": heads,
            "reference_block_count": int(ref_sum["total"]),
            "referenced_version_count": int(distinct_refs["total"]),
        }

    def _snapshot_view(
        self,
        conn: sqlite3.Connection,
        volume_id: str,
        snap: sqlite3.Row | dict[str, Any],
        *,
        include_resolution: bool = True,
    ) -> dict[str, Any]:
        item = dict(snap)
        item["consistency_seq"] = int(item["consistency_seq"])
        item["ref_count"] = int(item["ref_count"])
        item["active"] = item["state"] in ACTIVE_SNAPSHOT_STATES
        if include_resolution:
            try:
                mapping, lineage = self._resolve_snapshot(conn, volume_id, str(item["snapshot_id"]))
                item["resolved_block_count"] = len(mapping)
                item["resolution_depth"] = len(lineage)
            except BusinessError as exc:
                item["resolved_block_count"] = None
                item["resolution_error"] = exc.code
        return item

    def _snapshot_chain_locked(
        self,
        conn: sqlite3.Connection,
        volume_id: str,
    ) -> dict[str, Any]:
        snapshots = conn.execute(
            "SELECT * FROM snapshots WHERE volume_id = ? ORDER BY created_at, snapshot_id",
            (volume_id,),
        ).fetchall()
        items = [self._snapshot_view(conn, volume_id, row) for row in snapshots]
        stats = self._stats(conn, volume_id)
        return {"ok": True, **stats, "snapshots": items}

    # ------------------------------------------------------------------
    # Request idempotency wrapper
    # ------------------------------------------------------------------
    def _request_record(self, request_id: str) -> dict[str, Any] | None:
        conn = self.db.connect()
        try:
            return _row(conn.execute(
                "SELECT * FROM requests WHERE request_id = ?", (request_id,)
            ).fetchone())
        finally:
            conn.close()

    def _request_hash(
        self,
        operation: str,
        volume_id: str,
        payload: dict[str, Any],
    ) -> str:
        return hashlib.sha256(
            canonical_json(
                {"operation": operation, "volume_id": volume_id, "payload": payload}
            ).encode("utf-8")
        ).hexdigest()

    def _reuse_response(self, record: dict[str, Any], request_hash: str) -> dict[str, Any]:
        request_id = str(record["request_id"])
        if record["request_hash"] != request_hash:
            raise error(
                "E_CONFLICTING_REQUEST",
                "The request_id was already used with a different request payload.",
                409,
                request_id=request_id,
                existing_operation=record["operation"],
            )
        state = record["state"]
        if state == "ABORTED":
            raise error(
                "E_OPERATION_ABORTED",
                "The original request was interrupted and has been durably marked aborted.",
                409,
                request_id=request_id,
            )
        if state == "IN_PROGRESS":
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                time.sleep(0.05)
                refreshed = self._request_record(request_id)
                if refreshed is not None and refreshed["state"] != "IN_PROGRESS":
                    return self._reuse_response(refreshed, request_hash)
            raise error(
                "E_OPERATION_ACTIVE",
                "The original request is still in progress.",
                409,
                request_id=request_id,
            )
        response_raw = record.get("response_json")
        if response_raw is None:
            raise error(
                "E_INTERNAL",
                "A completed request is missing its durable response.",
                500,
                request_id=request_id,
            )
        response = json.loads(response_raw)
        if response.get("ok") is True:
            return response
        code = response.get("code", "E_INTERNAL")
        raise BusinessError(
            code=code,
            message=response.get("message", "The original request failed."),
            http_status=ERROR_HTTP_STATUS.get(code, 409),
            details=response.get("details", {}),
        )

    def _claim_request(
        self,
        request_id: str,
        volume_id: str,
        operation: str,
        request_hash: str,
    ) -> bool:
        now = utcnow()
        with self.db.transaction() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO requests(
                        request_id, volume_id, operation, request_hash,
                        state, response_json, error_code, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'IN_PROGRESS', NULL, NULL, ?, ?)
                    """,
                    (request_id, volume_id, operation, request_hash, now, now),
                )
                return True
            except sqlite3.IntegrityError:
                existing = conn.execute(
                    "SELECT request_hash FROM requests WHERE request_id = ?",
                    (request_id,),
                ).fetchone()
                if existing is not None and existing["request_hash"] == request_hash:
                    return False
                raise error(
                    "E_CONFLICTING_REQUEST",
                    "The request_id was claimed concurrently with different content.",
                    409,
                    request_id=request_id,
                )

    def _store_error_response(self, request_id: str, exc: BusinessError) -> None:
        response = exc.body(request_id)
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE requests
                   SET state = 'COMPLETED', response_json = ?, error_code = ?, updated_at = ?
                 WHERE request_id = ? AND state = 'IN_PROGRESS'
                """,
                (canonical_json(response), exc.code, utcnow(), request_id),
            )

    def _run_request(
        self,
        operation: str,
        volume_id: str,
        request_id: str | None,
        payload: dict[str, Any],
        action: Callable[[str], dict[str, Any]],
        *,
        nonblocking_volume: bool = False,
    ) -> dict[str, Any]:
        rid = request_id or new_id("req")
        if not rid.strip():
            raise error("E_INVALID_REQUEST", "request_id must not be empty.", 422)
        request_hash = self._request_hash(operation, volume_id, payload)
        with self._request_lock(rid):
            existing = self._request_record(rid)
            if existing is not None:
                return self._reuse_response(existing, request_hash)
            volume_lock = self._volume_lock(volume_id)
            if nonblocking_volume and not volume_lock.acquire(blocking=False):
                raise error(
                    "E_OPERATION_ACTIVE",
                    "Another mutation is active on this volume.",
                    409,
                    volume_id=volume_id,
                )
            if not nonblocking_volume:
                volume_lock.acquire()
            try:
                claimed = self._claim_request(rid, volume_id, operation, request_hash)
                if not claimed:
                    existing = self._request_record(rid)
                    if existing is None:
                        raise error(
                            "E_OPERATION_ABORTED",
                            "The request claim disappeared before it could be reused.",
                            409,
                            request_id=rid,
                        )
                    return self._reuse_response(existing, request_hash)
                return action(rid)
            except BusinessError as exc:
                if exc.code != "E_CONFLICTING_REQUEST":
                    self._store_error_response(rid, exc)
                raise
            except Exception as exc:
                internal = error(
                    "E_INTERNAL",
                    f"Unexpected internal error: {type(exc).__name__}",
                    500,
                )
                self._store_error_response(rid, internal)
                raise BusinessError(
                    code=internal.code,
                    message=internal.message,
                    http_status=500,
                    details={"error_type": type(exc).__name__},
                ) from exc
            finally:
                volume_lock.release()

    # ------------------------------------------------------------------
    # Block writes
    # ------------------------------------------------------------------
    def write_block(
        self,
        volume_id: str,
        block_no: int,
        content: bytes,
        expected_digest: str,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        payload = {
            "block_no": block_no,
            "content_b64": base64.b64encode(content).decode("ascii"),
            "sha256": expected_digest.lower(),
        }
        return self._run_request(
            "write_block",
            volume_id,
            request_id,
            payload,
            lambda rid: self._write_block_locked(volume_id, block_no, content, expected_digest, rid),
        )

    def _write_block_locked(
        self,
        volume_id: str,
        block_no: int,
        content: bytes,
        expected_digest: str,
        request_id: str,
    ) -> dict[str, Any]:
        with self.db.read_view() as conn:
            volume = self._volume(conn, volume_id)
            block_count = int(volume["block_count"])
            if block_no < 0 or block_no >= block_count:
                raise error(
                    "E_BLOCK_NOT_FOUND",
                    "The block number is outside the fixed volume.",
                    404,
                    volume_id=volume_id,
                    block_no=block_no,
                    block_count=block_count,
                )
            current = conn.execute(
                """
                SELECT block_version FROM current_blocks
                 WHERE volume_id = ? AND block_no = ?
                """,
                (volume_id, block_no),
            ).fetchone()
            if current is None:
                raise error(
                    "E_BLOCK_NOT_FOUND",
                    "The volume has no current mapping for the requested block.",
                    404,
                    volume_id=volume_id,
                    block_no=block_no,
                )
            previous_version = int(current["block_version"])

        actual_digest = sha256_bytes(content)
        normalized_expected = expected_digest.strip().lower()
        if normalized_expected != actual_digest:
            raise error(
                "E_BLOCK_CORRUPT",
                "The supplied SHA-256 does not match the submitted block content.",
                409,
                volume_id=volume_id,
                block_no=block_no,
                expected_sha256=normalized_expected,
                actual_sha256=actual_digest,
            )

        intent_payload = {
            "volume_id": volume_id,
            "block_no": block_no,
            "digest": actual_digest,
            "previous_version": previous_version,
        }
        with self.db.transaction() as conn:
            intent_id = self._insert_intent(
                conn, volume_id, request_id, "BLOCK_WRITE", intent_payload
            )

        with self.db.transaction() as conn:
            self._volume(conn, volume_id)
            current = conn.execute(
                "SELECT block_version FROM current_blocks WHERE volume_id = ? AND block_no = ?",
                (volume_id, block_no),
            ).fetchone()
            if current is None:
                raise error("E_BLOCK_NOT_FOUND", "Current block mapping disappeared.", 404)
            old_version = int(current["block_version"])
            max_row = conn.execute(
                "SELECT COALESCE(MAX(version), 0) AS v FROM block_versions WHERE volume_id = ? AND block_no = ?",
                (volume_id, block_no),
            ).fetchone()
            new_version = int(max_row["v"]) + 1
            old_ref_count = self._active_version_ref_count(
                conn, volume_id, block_no, old_version
            )
            written_at = utcnow()
            conn.execute(
                """
                INSERT INTO block_versions(
                    volume_id, block_no, version, digest, content, written_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (volume_id, block_no, new_version, actual_digest, content, written_at),
            )
            conn.execute(
                """
                UPDATE current_blocks SET block_version = ?
                 WHERE volume_id = ? AND block_no = ?
                """,
                (new_version, volume_id, block_no),
            )
            seq_row = conn.execute(
                "SELECT consistency_seq FROM volumes WHERE volume_id = ?", (volume_id,)
            ).fetchone()
            new_seq = int(seq_row["consistency_seq"]) + 1
            conn.execute(
                "UPDATE volumes SET consistency_seq = ?, updated_at = ? WHERE volume_id = ?",
                (new_seq, written_at, volume_id),
            )
            cow_performed = old_ref_count > 0
            cow_event_id = None
            if cow_performed:
                cow_event_id = new_id("cow")
                conn.execute(
                    """
                    INSERT INTO cow_events(
                        event_id, volume_id, block_no, old_version,
                        new_version, old_ref_count, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        cow_event_id,
                        volume_id,
                        block_no,
                        old_version,
                        new_version,
                        old_ref_count,
                        written_at,
                    ),
                )
            response = {
                "ok": True,
                "request_id": request_id,
                "volume_id": volume_id,
                "block_no": block_no,
                "previous_version": old_version,
                "version": new_version,
                "digest": actual_digest,
                "written_at": written_at,
                "consistency_seq": new_seq,
                "cow_performed": cow_performed,
                "previous_version_ref_count": old_ref_count,
                "cow_event_id": cow_event_id,
            }
            self._finish_intent(
                conn,
                intent_id,
                {
                    "new_version": new_version,
                    "consistency_seq": new_seq,
                    "cow_performed": cow_performed,
                },
            )
            self._finish_request_tx(conn, request_id, response)
        return response

    # ------------------------------------------------------------------
    # Snapshot creation
    # ------------------------------------------------------------------
    def create_snapshot(
        self,
        volume_id: str,
        mode: str,
        parent_id: str | None,
        snapshot_id: str | None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        if mode not in {"full", "incremental"}:
            raise error("E_INVALID_REQUEST", "mode must be full or incremental.", 422)
        if mode == "full" and parent_id is not None:
            raise error("E_INVALID_REQUEST", "A full snapshot cannot have a parent.", 422)
        if mode == "incremental" and not parent_id:
            raise error("E_INVALID_REQUEST", "An incremental snapshot requires parent_id.", 422)
        payload = {"mode": mode, "parent_id": parent_id, "snapshot_id": snapshot_id}
        return self._run_request(
            "create_snapshot",
            volume_id,
            request_id,
            payload,
            lambda rid: self._create_snapshot_locked(
                volume_id, mode, parent_id, snapshot_id, rid
            ),
        )

    def _create_snapshot_locked(
        self,
        volume_id: str,
        mode: str,
        parent_id: str | None,
        snapshot_id: str | None,
        request_id: str,
    ) -> dict[str, Any]:
        sid = snapshot_id or new_id("snap")
        created_at = utcnow()
        with self.db.transaction() as conn:
            volume = self._volume(conn, volume_id)
            if conn.execute(
                "SELECT 1 FROM snapshots WHERE volume_id = ? AND snapshot_id = ?",
                (volume_id, sid),
            ).fetchone():
                raise error(
                    "E_SNAPSHOT_ALREADY_EXISTS",
                    "A snapshot with the requested identifier already exists.",
                    409,
                    snapshot_id=sid,
                )
            current = self._current_map(conn, volume_id)
            block_count = int(volume["block_count"])
            if len(current) != block_count:
                raise error(
                    "E_VOLUME_CORRUPT",
                    "The current volume does not contain every configured block.",
                    409,
                    volume_id=volume_id,
                    expected_blocks=block_count,
                    actual_blocks=len(current),
                )
            if mode == "incremental":
                parent = self._snapshot(conn, volume_id, str(parent_id))
                if parent["state"] != COMMITTED:
                    raise error(
                        "E_INVALID_TRANSITION",
                        "Only a COMMITTED snapshot can be used as an incremental parent.",
                        409,
                        snapshot_id=parent_id,
                        state=parent["state"],
                    )
                parent_map, _ = self._resolve_snapshot(conn, volume_id, str(parent_id))
                self._validate_resolved_snapshot(
                    conn, volume_id, parent_map, block_count, str(parent_id)
                )
                refs = [
                    current[block_no]
                    for block_no in range(block_count)
                    if parent_map[block_no]["version"] != current[block_no]["version"]
                ]
            else:
                refs = [current[block_no] for block_no in range(block_count)]

            intent_id = self._insert_intent(
                conn,
                volume_id,
                request_id,
                "SNAPSHOT_CREATE",
                {
                    "snapshot_id": sid,
                    "mode": mode,
                    "parent_id": parent_id,
                    "block_refs": [
                        {
                            "block_no": ref["block_no"],
                            "version": ref["version"],
                            "digest": ref["digest"],
                        }
                        for ref in refs
                    ],
                },
            )
            conn.execute(
                """
                INSERT INTO snapshots(
                    volume_id, snapshot_id, parent_id, mode, state,
                    consistency_seq, ref_count, merged_into, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
                """,
                (
                    volume_id,
                    sid,
                    parent_id if mode == "incremental" else None,
                    mode,
                    CREATING,
                    int(volume["consistency_seq"]),
                    len(refs),
                    created_at,
                    created_at,
                ),
            )
            for ref in refs:
                conn.execute(
                    """
                    INSERT INTO snapshot_refs(
                        volume_id, snapshot_id, block_no, block_version, digest
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (volume_id, sid, ref["block_no"], ref["version"], ref["digest"]),
                )
            self._event(
                conn,
                volume_id,
                sid,
                "SNAPSHOT_CREATING",
                None,
                CREATING,
                {"delta_ref_count": len(refs), "mode": mode, "parent_id": parent_id},
            )

        with self.db.transaction() as conn:
            snap = self._snapshot(conn, volume_id, sid)
            if snap["state"] != CREATING:
                raise error(
                    "E_INVALID_TRANSITION",
                    "The snapshot is no longer in CREATING state.",
                    409,
                    snapshot_id=sid,
                    state=snap["state"],
                )
            self._transition(
                conn,
                volume_id,
                sid,
                CREATING,
                COMMITTED,
                event_type="SNAPSHOT_COMMITTED",
                details={"consistency_seq": int(snap["consistency_seq"])},
            )
            stats = self._stats(conn, volume_id)
            snap_view = self._snapshot_view(conn, volume_id, self._snapshot(conn, volume_id, sid))
            response = {
                "ok": True,
                "request_id": request_id,
                "snapshot": snap_view,
                "chain_depth": stats["chain_depth"],
                "active_snapshot_count": stats["active_snapshot_count"],
                "reference_block_count": stats["reference_block_count"],
                "referenced_version_count": stats["referenced_version_count"],
            }
            self._finish_intent(
                conn,
                intent_id,
                {
                    "snapshot_id": sid,
                    "state": COMMITTED,
                    "delta_ref_count": int(snap["ref_count"]),
                },
            )
            self._finish_request_tx(conn, request_id, response)
        return response

    # ------------------------------------------------------------------
    # Snapshot merge
    # ------------------------------------------------------------------
    def merge_snapshots(
        self,
        volume_id: str,
        source_snapshot_id: str,
        target_snapshot_id: str,
        request_id: str | None = None,
        crash_after_begin: bool = False,
    ) -> dict[str, Any]:
        if crash_after_begin and not self.settings.fault_injection:
            raise error(
                "E_FAULT_INJECTION_DISABLED",
                "Fault injection is disabled for this process.",
                409,
            )
        payload = {
            "source_snapshot_id": source_snapshot_id,
            "target_snapshot_id": target_snapshot_id,
            "crash_after_begin": crash_after_begin,
        }
        return self._run_request(
            "merge_snapshots",
            volume_id,
            request_id,
            payload,
            lambda rid: self._merge_locked(
                volume_id,
                source_snapshot_id,
                target_snapshot_id,
                rid,
                crash_after_begin,
            ),
            nonblocking_volume=True,
        )

    def _merge_locked(
        self,
        volume_id: str,
        source_snapshot_id: str,
        target_snapshot_id: str,
        request_id: str,
        crash_after_begin: bool,
    ) -> dict[str, Any]:
        with self.db.transaction() as conn:
            volume = self._volume(conn, volume_id)
            source = self._snapshot(conn, volume_id, source_snapshot_id)
            target = self._snapshot(conn, volume_id, target_snapshot_id)
            if source["state"] == MERGING:
                raise error(
                    "E_OPERATION_ACTIVE",
                    "The source snapshot is already being merged.",
                    409,
                    snapshot_id=source_snapshot_id,
                )
            if source["state"] != COMMITTED or target["state"] != COMMITTED:
                raise error(
                    "E_INVALID_TRANSITION",
                    "Both snapshots must be COMMITTED before merging.",
                    409,
                    source_state=source["state"],
                    target_state=target["state"],
                )
            if source["parent_id"] != target_snapshot_id:
                raise error(
                    "E_SNAPSHOT_NOT_ADJACENT",
                    "The source snapshot is not the direct child of the target snapshot.",
                    409,
                    source_snapshot_id=source_snapshot_id,
                    target_snapshot_id=target_snapshot_id,
                    source_parent_id=source["parent_id"],
                )
            other_children = conn.execute(
                """
                SELECT snapshot_id FROM snapshots
                 WHERE volume_id = ? AND parent_id = ? AND snapshot_id != ?
                   AND state IN ('CREATING', 'COMMITTED', 'MERGING')
                """,
                (volume_id, target_snapshot_id, source_snapshot_id),
            ).fetchall()
            if other_children:
                raise error(
                    "E_SNAPSHOT_REFERENCED",
                    "The target snapshot has another active child; merge the linear chain first.",
                    409,
                    target_snapshot_id=target_snapshot_id,
                    other_children=[row["snapshot_id"] for row in other_children],
                )
            source_map, _ = self._resolve_snapshot(conn, volume_id, source_snapshot_id)
            self._validate_resolved_snapshot(
                conn,
                volume_id,
                source_map,
                int(volume["block_count"]),
                source_snapshot_id,
            )
            children = conn.execute(
                """
                SELECT snapshot_id FROM snapshots
                 WHERE volume_id = ? AND parent_id = ?
                   AND state IN ('CREATING', 'COMMITTED', 'MERGING')
                """,
                (volume_id, source_snapshot_id),
            ).fetchall()
            stats_before = self._stats(conn, volume_id)
            intent_id = self._insert_intent(
                conn,
                volume_id,
                request_id,
                "SNAPSHOT_MERGE",
                {
                    "source_snapshot_id": source_snapshot_id,
                    "target_snapshot_id": target_snapshot_id,
                    "source_map": [
                        {
                            "block_no": source_map[block_no]["block_no"],
                            "version": source_map[block_no]["version"],
                            "digest": source_map[block_no]["digest"],
                        }
                        for block_no in sorted(source_map)
                    ],
                    "children_to_reparent": [row["snapshot_id"] for row in children],
                },
            )
            self._transition(
                conn,
                volume_id,
                source_snapshot_id,
                COMMITTED,
                MERGING,
                event_type="SNAPSHOT_MERGE_BEGIN",
                details={"target_snapshot_id": target_snapshot_id},
            )

        if crash_after_begin:
            os._exit(86)

        with self.db.transaction() as conn:
            source = self._snapshot(conn, volume_id, source_snapshot_id)
            target = self._snapshot(conn, volume_id, target_snapshot_id)
            if source["state"] != MERGING:
                raise error(
                    "E_INVALID_TRANSITION",
                    "Merge source is not in MERGING state.",
                    409,
                    snapshot_id=source_snapshot_id,
                    state=source["state"],
                )
            source_map, _ = self._resolve_snapshot(conn, volume_id, source_snapshot_id)
            volume = self._volume(conn, volume_id)
            self._validate_resolved_snapshot(
                conn,
                volume_id,
                source_map,
                int(volume["block_count"]),
                source_snapshot_id,
            )

            old_target_refs = conn.execute(
                """
                SELECT block_no, block_version, digest FROM snapshot_refs
                 WHERE volume_id = ? AND snapshot_id = ?
                """,
                (volume_id, target_snapshot_id),
            ).fetchall()
            captured_at = utcnow()
            for ref in old_target_refs:
                conn.execute(
                    """
                    INSERT INTO snapshot_ref_history(
                        history_id, volume_id, snapshot_id, block_no,
                        block_version, digest, reason, captured_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        new_id("hist"),
                        volume_id,
                        target_snapshot_id,
                        int(ref["block_no"]),
                        int(ref["block_version"]),
                        ref["digest"],
                        "REPLACED_BY_MERGE",
                        captured_at,
                    ),
                )
            conn.execute(
                "DELETE FROM snapshot_refs WHERE volume_id = ? AND snapshot_id = ?",
                (volume_id, target_snapshot_id),
            )
            for block_no in range(int(volume["block_count"])):
                ref = source_map[block_no]
                conn.execute(
                    """
                    INSERT INTO snapshot_refs(
                        volume_id, snapshot_id, block_no, block_version, digest
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        volume_id,
                        target_snapshot_id,
                        block_no,
                        ref["version"],
                        ref["digest"],
                    ),
                )
            children = conn.execute(
                """
                SELECT snapshot_id FROM snapshots
                 WHERE volume_id = ? AND parent_id = ?
                   AND state IN ('CREATING', 'COMMITTED', 'MERGING')
                """,
                (volume_id, source_snapshot_id),
            ).fetchall()
            for child in children:
                conn.execute(
                    "UPDATE snapshots SET parent_id = ?, updated_at = ? WHERE volume_id = ? AND snapshot_id = ?",
                    (target_snapshot_id, utcnow(), volume_id, child["snapshot_id"]),
                )
            conn.execute(
                """
                UPDATE snapshots
                   SET ref_count = ?, consistency_seq = ?, updated_at = ?
                 WHERE volume_id = ? AND snapshot_id = ?
                """,
                (
                    int(volume["block_count"]),
                    int(source["consistency_seq"]),
                    utcnow(),
                    volume_id,
                    target_snapshot_id,
                ),
            )
            source_before = COMMITTED
            self._transition(
                conn,
                volume_id,
                source_snapshot_id,
                MERGING,
                MERGED,
                event_type="SNAPSHOT_MERGED",
                details={"target_snapshot_id": target_snapshot_id},
            )
            conn.execute(
                "UPDATE snapshots SET merged_into = ?, updated_at = ? WHERE volume_id = ? AND snapshot_id = ?",
                (target_snapshot_id, utcnow(), volume_id, source_snapshot_id),
            )
            self._event(
                conn,
                volume_id,
                target_snapshot_id,
                "SNAPSHOT_REFS_REWRITTEN",
                None,
                None,
                {
                    "source_snapshot_id": source_snapshot_id,
                    "old_ref_count": len(old_target_refs),
                    "new_ref_count": int(volume["block_count"]),
                    "children_reparented": [row["snapshot_id"] for row in children],
                },
            )
            stats_after = self._stats(conn, volume_id)
            target_view = self._snapshot_view(
                conn, volume_id, self._snapshot(conn, volume_id, target_snapshot_id)
            )
            source_view = self._snapshot_view(
                conn, volume_id, self._snapshot(conn, volume_id, source_snapshot_id)
            )
            response = {
                "ok": True,
                "request_id": request_id,
                "source": source_view,
                "target": target_view,
                "source_state_before": source_before,
                "source_state_after": MERGED,
                "refs_rewritten": int(volume["block_count"]),
                "children_reparented": [row["snapshot_id"] for row in children],
                "chain_depth_before": stats_before["chain_depth"],
                "chain_depth_after": stats_after["chain_depth"],
                "active_snapshot_count_before": stats_before["active_snapshot_count"],
                "active_snapshot_count_after": stats_after["active_snapshot_count"],
                "reference_block_count_before": stats_before["reference_block_count"],
                "reference_block_count_after": stats_after["reference_block_count"],
                "referenced_version_count_after": stats_after["referenced_version_count"],
                "target_ref_count_after": int(volume["block_count"]),
            }
            self._finish_intent(
                conn,
                intent_id,
                {
                    "source_snapshot_id": source_snapshot_id,
                    "target_snapshot_id": target_snapshot_id,
                    "source_state": MERGED,
                    "target_ref_count": int(volume["block_count"]),
                },
            )
            self._finish_request_tx(conn, request_id, response)
        return response

    # ------------------------------------------------------------------
    # Snapshot deletion
    # ------------------------------------------------------------------
    def delete_snapshot(
        self,
        volume_id: str,
        snapshot_id: str,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        payload = {"snapshot_id": snapshot_id}
        return self._run_request(
            "delete_snapshot",
            volume_id,
            request_id,
            payload,
            lambda rid: self._delete_snapshot_locked(volume_id, snapshot_id, rid),
        )

    def _delete_snapshot_locked(
        self,
        volume_id: str,
        snapshot_id: str,
        request_id: str,
    ) -> dict[str, Any]:
        with self.db.read_view() as conn:
            snap = self._snapshot(conn, volume_id, snapshot_id)
            if snap["state"] != COMMITTED:
                raise error(
                    "E_INVALID_TRANSITION",
                    "Only a COMMITTED snapshot can be deleted.",
                    409,
                    snapshot_id=snapshot_id,
                    state=snap["state"],
                )
            children = conn.execute(
                """
                SELECT snapshot_id FROM snapshots
                 WHERE volume_id = ? AND parent_id = ?
                   AND state IN ('CREATING', 'COMMITTED', 'MERGING')
                """,
                (volume_id, snapshot_id),
            ).fetchall()
            if children:
                raise error(
                    "E_SNAPSHOT_REFERENCED",
                    "The snapshot still has active child snapshots.",
                    409,
                    snapshot_id=snapshot_id,
                    child_snapshot_ids=[row["snapshot_id"] for row in children],
                )

        with self.db.transaction() as conn:
            intent_id = self._insert_intent(
                conn,
                volume_id,
                request_id,
                "SNAPSHOT_DELETE",
                {"snapshot_id": snapshot_id},
            )

        with self.db.transaction() as conn:
            snap = self._snapshot(conn, volume_id, snapshot_id)
            if snap["state"] != COMMITTED:
                raise error(
                    "E_INVALID_TRANSITION",
                    "Snapshot state changed before deletion.",
                    409,
                    snapshot_id=snapshot_id,
                    state=snap["state"],
                )
            children = conn.execute(
                """
                SELECT snapshot_id FROM snapshots
                 WHERE volume_id = ? AND parent_id = ?
                   AND state IN ('CREATING', 'COMMITTED', 'MERGING')
                """,
                (volume_id, snapshot_id),
            ).fetchall()
            if children:
                raise error(
                    "E_SNAPSHOT_REFERENCED",
                    "The snapshot still has active child snapshots.",
                    409,
                    snapshot_id=snapshot_id,
                    child_snapshot_ids=[row["snapshot_id"] for row in children],
                )
            self._transition(
                conn,
                volume_id,
                snapshot_id,
                COMMITTED,
                DELETED,
                event_type="SNAPSHOT_DELETED",
                details={"refs_retained_for_history": int(snap["ref_count"])},
            )
            stats = self._stats(conn, volume_id)
            response = {
                "ok": True,
                "request_id": request_id,
                "snapshot_id": snapshot_id,
                "state": DELETED,
                "refs_retained_for_history": int(snap["ref_count"]),
                "chain_depth": stats["chain_depth"],
                "active_snapshot_count": stats["active_snapshot_count"],
                "reference_block_count": stats["reference_block_count"],
            }
            self._finish_intent(
                conn, intent_id, {"snapshot_id": snapshot_id, "state": DELETED}
            )
            self._finish_request_tx(conn, request_id, response)
        return response

    # ------------------------------------------------------------------
    # Rollback and diffs
    # ------------------------------------------------------------------
    def _rollback_diff(
        self,
        conn: sqlite3.Connection,
        volume_id: str,
        snapshot_id: str,
    ) -> dict[str, Any]:
        volume = self._volume(conn, volume_id)
        target = self._snapshot(conn, volume_id, snapshot_id)
        if target["state"] != COMMITTED:
            raise error(
                "E_INVALID_TRANSITION",
                "A rollback target must be COMMITTED.",
                409,
                snapshot_id=snapshot_id,
                state=target["state"],
            )
        target_map, lineage = self._resolve_snapshot(conn, volume_id, snapshot_id)
        block_count = int(volume["block_count"])
        self._validate_resolved_snapshot(
            conn, volume_id, target_map, block_count, snapshot_id
        )
        current = self._current_map(conn, volume_id)
        if len(current) != block_count:
            raise error(
                "E_VOLUME_CORRUPT",
                "The current volume is incomplete; rollback was not started.",
                409,
                volume_id=volume_id,
                expected_blocks=block_count,
                actual_blocks=len(current),
            )
        overwritten: list[dict[str, Any]] = []
        for block_no in range(block_count):
            cur = current[block_no]
            dst = target_map[block_no]
            if int(cur["version"]) != int(dst["version"]):
                overwritten.append(
                    {
                        "block_no": block_no,
                        "current_version": int(cur["version"]),
                        "target_version": int(dst["version"]),
                        "current_digest": cur["digest"],
                        "target_digest": dst["digest"],
                    }
                )
        return {
            "target_snapshot_id": snapshot_id,
            "target_consistency_seq": int(target["consistency_seq"]),
            "current_consistency_seq": int(volume["consistency_seq"]),
            "write_back_blocks": len(overwritten),
            "unchanged_blocks": block_count - len(overwritten),
            "overwritten_current_versions": overwritten,
            "lineage_depth": len(lineage),
        }

    def rollback_snapshot(
        self,
        volume_id: str,
        snapshot_id: str,
        request_id: str | None = None,
        crash_before_commit: bool = False,
    ) -> dict[str, Any]:
        if crash_before_commit and not self.settings.fault_injection:
            raise error(
                "E_FAULT_INJECTION_DISABLED",
                "Fault injection is disabled for this process.",
                409,
            )
        payload = {
            "snapshot_id": snapshot_id,
            "crash_before_commit": crash_before_commit,
        }
        return self._run_request(
            "rollback_snapshot",
            volume_id,
            request_id,
            payload,
            lambda rid: self._rollback_locked(
                volume_id, snapshot_id, rid, crash_before_commit
            ),
        )

    def _rollback_locked(
        self,
        volume_id: str,
        snapshot_id: str,
        request_id: str,
        crash_before_commit: bool,
    ) -> dict[str, Any]:
        with self.db.read_view() as conn:
            diff = self._rollback_diff(conn, volume_id, snapshot_id)

        with self.db.transaction() as conn:
            intent_id = self._insert_intent(
                conn,
                volume_id,
                request_id,
                "SNAPSHOT_ROLLBACK",
                {
                    "snapshot_id": snapshot_id,
                    "write_back_blocks": diff["write_back_blocks"],
                    "target_consistency_seq": diff["target_consistency_seq"],
                },
            )

        with self.db.transaction() as conn:
            diff = self._rollback_diff(conn, volume_id, snapshot_id)
            target_map, _ = self._resolve_snapshot(conn, volume_id, snapshot_id)
            volume = self._volume(conn, volume_id)
            for item in diff["overwritten_current_versions"]:
                conn.execute(
                    """
                    UPDATE current_blocks SET block_version = ?
                     WHERE volume_id = ? AND block_no = ?
                    """,
                    (
                        int(item["target_version"]),
                        volume_id,
                        int(item["block_no"]),
                    ),
                )
            new_seq = int(volume["consistency_seq"]) + 1
            now = utcnow()
            conn.execute(
                "UPDATE volumes SET consistency_seq = ?, updated_at = ? WHERE volume_id = ?",
                (new_seq, now, volume_id),
            )
            self._event(
                conn,
                volume_id,
                snapshot_id,
                "SNAPSHOT_ROLLBACK",
                None,
                None,
                {
                    "write_back_blocks": diff["write_back_blocks"],
                    "consistency_seq_before": int(volume["consistency_seq"]),
                    "consistency_seq_after": new_seq,
                },
            )
            response = {
                "ok": True,
                "request_id": request_id,
                "target_snapshot_id": snapshot_id,
                "write_back_blocks": diff["write_back_blocks"],
                "unchanged_blocks": diff["unchanged_blocks"],
                "overwritten_current_versions": diff["overwritten_current_versions"],
                "target_consistency_seq": diff["target_consistency_seq"],
                "current_consistency_seq_before": diff["current_consistency_seq"],
                "current_consistency_seq_after": new_seq,
                "restored_versions": {
                    str(block_no): int(target_map[block_no]["version"])
                    for block_no in sorted(target_map)
                },
            }
            if crash_before_commit:
                os._exit(87)
            self._finish_intent(
                conn,
                intent_id,
                {
                    "snapshot_id": snapshot_id,
                    "write_back_blocks": diff["write_back_blocks"],
                    "consistency_seq_after": new_seq,
                },
            )
            self._finish_request_tx(conn, request_id, response)
        return response

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------
    def get_chain(self, volume_id: str) -> dict[str, Any]:
        with self.db.read_view() as conn:
            return self._snapshot_chain_locked(conn, volume_id)

    def get_rollback_diff(self, volume_id: str, snapshot_id: str) -> dict[str, Any]:
        with self.db.read_view() as conn:
            diff = self._rollback_diff(conn, volume_id, snapshot_id)
            return {"ok": True, **diff}

    def get_current_view(self, volume_id: str) -> dict[str, Any]:
        with self.db.read_view() as conn:
            volume = self._volume(conn, volume_id)
            current = self._current_map(conn, volume_id)
            rows = []
            for block_no in range(int(volume["block_count"])):
                if block_no not in current:
                    raise error(
                        "E_VOLUME_CORRUPT",
                        "The current volume is missing a block.",
                        409,
                        block_no=block_no,
                    )
                item = current[block_no]
                rows.append(
                    {
                        "block_no": block_no,
                        "version": int(item["version"]),
                        "digest": item["digest"],
                        "last_written_at": item["written_at"],
                    }
                )
            return {
                "ok": True,
                "volume_id": volume_id,
                "block_count": int(volume["block_count"]),
                "consistency_seq": int(volume["consistency_seq"]),
                "blocks": rows,
            }

    def get_snapshot_blocks(
        self,
        volume_id: str,
        snapshot_id: str,
        include_content: bool = False,
    ) -> dict[str, Any]:
        with self.db.read_view() as conn:
            volume = self._volume(conn, volume_id)
            snap = self._snapshot(conn, volume_id, snapshot_id)
            mapping, lineage = self._resolve_snapshot(conn, volume_id, snapshot_id)
            self._validate_resolved_snapshot(
                conn,
                volume_id,
                mapping,
                int(volume["block_count"]),
                snapshot_id,
            )
            blocks = []
            for block_no in range(int(volume["block_count"])):
                ref = mapping[block_no]
                bv = conn.execute(
                    "SELECT content, written_at FROM block_versions WHERE volume_id = ? AND block_no = ? AND version = ?",
                    (volume_id, block_no, ref["version"]),
                ).fetchone()
                item: dict[str, Any] = {
                    "block_no": block_no,
                    "version": int(ref["version"]),
                    "digest": ref["digest"],
                    "source_snapshot_id": ref["source_snapshot_id"],
                    "version_snapshot_ref_count": self._active_version_ref_count(
                        conn, volume_id, block_no, int(ref["version"])
                    ),
                    "written_at": bv["written_at"] if bv else None,
                }
                if include_content:
                    item["content_b64"] = base64.b64encode(bytes(bv["content"])).decode("ascii") if bv else None
                blocks.append(item)
            return {
                "ok": True,
                "snapshot": self._snapshot_view(conn, volume_id, snap, include_resolution=False),
                "lineage": lineage,
                "resolved_block_count": len(mapping),
                "blocks": blocks,
            }

    def get_block_versions(self, volume_id: str, block_no: int) -> dict[str, Any]:
        with self.db.read_view() as conn:
            volume = self._volume(conn, volume_id)
            if block_no < 0 or block_no >= int(volume["block_count"]):
                raise error(
                    "E_BLOCK_NOT_FOUND",
                    "The block number is outside the fixed volume.",
                    404,
                    block_no=block_no,
                )
            placeholders = ",".join("?" for _ in ACTIVE_SNAPSHOT_STATES)
            rows = conn.execute(
                f"""
                SELECT bv.version, bv.digest, bv.written_at,
                       cb.block_version AS current_version,
                       CASE WHEN cb.block_version = bv.version THEN 1 ELSE 0 END AS is_current,
                       (
                           SELECT COUNT(*)
                             FROM snapshot_refs sr
                             JOIN snapshots s
                               ON s.volume_id = sr.volume_id AND s.snapshot_id = sr.snapshot_id
                            WHERE sr.volume_id = bv.volume_id
                              AND sr.block_no = bv.block_no
                              AND sr.block_version = bv.version
                              AND s.state IN ({placeholders})
                       ) AS active_snapshot_ref_count
                  FROM block_versions bv
                  LEFT JOIN current_blocks cb
                    ON cb.volume_id = bv.volume_id AND cb.block_no = bv.block_no
                 WHERE bv.volume_id = ? AND bv.block_no = ?
                 ORDER BY bv.version
                """,
                (*sorted(ACTIVE_SNAPSHOT_STATES), volume_id, block_no),
            ).fetchall()
            return {
                "ok": True,
                "volume_id": volume_id,
                "block_no": block_no,
                "versions": [
                    {
                        "version": int(row["version"]),
                        "digest": row["digest"],
                        "written_at": row["written_at"],
                        "is_current": _bool(row["is_current"]),
                        "active_snapshot_ref_count": int(row["active_snapshot_ref_count"]),
                    }
                    for row in rows
                ],
            }

    def get_recovery_report(self, volume_id: str) -> dict[str, Any]:
        conn = self.db.connect()
        try:
            row = conn.execute(
                """
                SELECT * FROM recovery_reports
                 WHERE volume_id = ?
                 ORDER BY created_at DESC, report_id DESC
                 LIMIT 1
                """,
                (volume_id,),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise error(
                "E_RECOVERY_REPORT_NOT_FOUND",
                "No recovery report has been persisted for this volume.",
                404,
                volume_id=volume_id,
            )
        return {"ok": True, **json.loads(row["report_json"])}

    # ------------------------------------------------------------------
    # Restart recovery
    # ------------------------------------------------------------------
    def recover(self) -> dict[str, Any]:
        started_at = utcnow()
        aborted_intents: list[dict[str, Any]] = []
        repaired_snapshots: list[dict[str, Any]] = []
        aborted_requests: list[str] = []
        recovery_errors: list[str] = []

        try:
            with self.db.transaction() as conn:
                volume = self._volume(conn, self.settings.volume_id)
                pending = conn.execute(
                    """
                    SELECT * FROM intents
                     WHERE volume_id = ? AND state = 'PENDING'
                     ORDER BY created_at, intent_id
                    """,
                    (self.settings.volume_id,),
                ).fetchall()
                for intent in pending:
                    payload = json.loads(intent["payload_json"] or "{}")
                    operation = str(intent["operation"])
                    action = "no persisted mutation had committed"
                    snapshot_id = payload.get("snapshot_id") or payload.get("source_snapshot_id")
                    repair: dict[str, Any] | None = None
                    try:
                        if operation == "SNAPSHOT_CREATE" and snapshot_id:
                            snap = conn.execute(
                                "SELECT state FROM snapshots WHERE volume_id = ? AND snapshot_id = ?",
                                (volume["volume_id"], snapshot_id),
                            ).fetchone()
                            if snap is not None and snap["state"] == CREATING:
                                conn.execute(
                                    "DELETE FROM snapshot_refs WHERE volume_id = ? AND snapshot_id = ?",
                                    (volume["volume_id"], snapshot_id),
                                )
                                conn.execute(
                                    "UPDATE snapshots SET ref_count = 0, updated_at = ? WHERE volume_id = ? AND snapshot_id = ?",
                                    (utcnow(), volume["volume_id"], snapshot_id),
                                )
                                self._transition(
                                    conn,
                                    str(volume["volume_id"]),
                                    str(snapshot_id),
                                    CREATING,
                                    DELETED,
                                    event_type="RECOVERY_ABORT_CREATING",
                                    details={"intent_id": intent["intent_id"]},
                                )
                                action = "CREATING snapshot marked DELETED and staged refs removed"
                                repair = {
                                    "snapshot_id": snapshot_id,
                                    "from_state": CREATING,
                                    "to_state": DELETED,
                                }
                        elif operation == "SNAPSHOT_MERGE" and snapshot_id:
                            snap = conn.execute(
                                "SELECT state FROM snapshots WHERE volume_id = ? AND snapshot_id = ?",
                                (volume["volume_id"], snapshot_id),
                            ).fetchone()
                            if snap is not None and snap["state"] == MERGING:
                                self._transition(
                                    conn,
                                    str(volume["volume_id"]),
                                    str(snapshot_id),
                                    MERGING,
                                    COMMITTED,
                                    event_type="RECOVERY_ABORT_MERGE",
                                    details={"intent_id": intent["intent_id"]},
                                )
                                action = "MERGING source restored to COMMITTED; target refs untouched"
                                repair = {
                                    "snapshot_id": snapshot_id,
                                    "from_state": MERGING,
                                    "to_state": COMMITTED,
                                }
                        elif operation in {"SNAPSHOT_DELETE", "SNAPSHOT_ROLLBACK", "BLOCK_WRITE"}:
                            action = "operation transaction was not committed; durable state unchanged"
                        conn.execute(
                            """
                            UPDATE intents
                               SET state = 'ABORTED', details_json = ?, updated_at = ?
                             WHERE intent_id = ? AND state = 'PENDING'
                            """,
                            (
                                canonical_json(
                                    {
                                        "recovery_action": action,
                                        "recovered_at": utcnow(),
                                        "repair": repair,
                                    }
                                ),
                                utcnow(),
                                intent["intent_id"],
                            ),
                        )
                    except Exception as exc:
                        recovery_errors.append(
                            f"intent {intent['intent_id']} recovery failed: {type(exc).__name__}: {exc}"
                        )
                    aborted_intents.append(
                        {
                            "intent_id": intent["intent_id"],
                            "operation": operation,
                            "snapshot_id": snapshot_id,
                            "action": action,
                            "repair": repair,
                        }
                    )
                    if repair is not None:
                        repaired_snapshots.append(repair)

                in_progress = conn.execute(
                    "SELECT request_id FROM requests WHERE state = 'IN_PROGRESS' ORDER BY created_at"
                ).fetchall()
                for request in in_progress:
                    request_id = str(request["request_id"])
                    response = {
                        "ok": False,
                        "code": "E_OPERATION_ABORTED",
                        "message": "The request was interrupted before a durable completion marker existed.",
                        "request_id": request_id,
                    }
                    conn.execute(
                        """
                        UPDATE requests
                           SET state = 'ABORTED', response_json = ?, error_code = 'E_OPERATION_ABORTED',
                               updated_at = ?
                         WHERE request_id = ? AND state = 'IN_PROGRESS'
                        """,
                        (canonical_json(response), utcnow(), request_id),
                    )
                    aborted_requests.append(request_id)
        except Exception as exc:
            recovery_errors.append(f"recovery transaction failed: {type(exc).__name__}: {exc}")

        checks = self._consistency_checks()
        if recovery_errors or not checks["ok"]:
            status = "ATTENTION"
        elif aborted_intents or aborted_requests or repaired_snapshots:
            status = "RECOVERED"
        else:
            status = "CONSISTENT"

        finished_at = utcnow()
        with self.db.transaction() as conn:
            volume = self._volume(conn, self.settings.volume_id)
            stats = self._stats(conn, self.settings.volume_id)
            report = {
                "report_id": new_id("recovery"),
                "generated_at": finished_at,
                "started_at": started_at,
                "finished_at": finished_at,
                "durable_only": True,
                "status": status,
                "summary": {
                    "pending_intents_scanned": len(aborted_intents),
                    "intents_aborted": len(aborted_intents),
                    "requests_aborted": len(aborted_requests),
                    "snapshots_repaired": len(repaired_snapshots),
                    "consistency_checks_ok": checks["ok"],
                },
                "intents_aborted": aborted_intents,
                "requests_aborted": aborted_requests,
                "snapshot_repairs": repaired_snapshots,
                "recovery_errors": recovery_errors,
                "consistency": checks,
                "durable_state": {
                    "volume_id": self.settings.volume_id,
                    "consistency_seq": int(volume["consistency_seq"]),
                    "block_count": int(volume["block_count"]),
                    "chain_depth": stats["chain_depth"],
                    "active_snapshot_count": stats["active_snapshot_count"],
                    "reference_block_count": stats["reference_block_count"],
                },
                "note": (
                    "This report is derived only from committed SQLite facts observed at restart. "
                    "It does not claim that a process generated an exit report before termination."
                ),
            }
            conn.execute(
                """
                INSERT INTO recovery_reports(report_id, volume_id, status, created_at, report_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    report["report_id"],
                    self.settings.volume_id,
                    status,
                    finished_at,
                    canonical_json(report),
                ),
            )
        return {"ok": True, **report}

    def _consistency_checks(self) -> dict[str, Any]:
        conn = self.db.connect()
        try:
            volume = self._volume(conn, self.settings.volume_id)
            block_count = int(volume["block_count"])
            fk_violations = list(conn.execute("PRAGMA foreign_key_check").fetchall())
            current = self._current_map(conn, self.settings.volume_id)
            missing_blocks = [b for b in range(block_count) if b not in current]
            extra_blocks = [b for b in current if b < 0 or b >= block_count]
            digest_mismatches: list[dict[str, Any]] = []
            for block_no in range(block_count):
                if block_no not in current:
                    continue
                version = int(current[block_no]["version"])
                bv = conn.execute(
                    "SELECT digest, content FROM block_versions WHERE volume_id = ? AND block_no = ? AND version = ?",
                    (self.settings.volume_id, block_no, version),
                ).fetchone()
                if bv is None:
                    digest_mismatches.append({"block_no": block_no, "reason": "missing_content"})
                    continue
                actual = sha256_bytes(bytes(bv["content"]))
                if actual != str(current[block_no]["digest"]).lower() or actual != str(bv["digest"]).lower():
                    digest_mismatches.append(
                        {"block_no": block_no, "reason": "digest_mismatch", "actual": actual}
                    )
            transient = conn.execute(
                "SELECT snapshot_id, state FROM snapshots WHERE volume_id = ? AND state IN ('CREATING', 'MERGING')",
                (self.settings.volume_id,),
            ).fetchall()
            pending = conn.execute(
                "SELECT COUNT(*) AS c FROM intents WHERE volume_id = ? AND state = 'PENDING'",
                (self.settings.volume_id,),
            ).fetchone()
            in_progress_requests = conn.execute(
                "SELECT COUNT(*) AS c FROM requests WHERE volume_id = ? AND state = 'IN_PROGRESS'",
                (self.settings.volume_id,),
            ).fetchone()
            active_refs = conn.execute(
                """
                SELECT sr.snapshot_id, sr.block_no, sr.block_version, sr.digest,
                       bv.digest AS version_digest, bv.content
                  FROM snapshot_refs sr
                  JOIN snapshots s
                    ON s.volume_id = sr.volume_id AND s.snapshot_id = sr.snapshot_id
                  JOIN block_versions bv
                    ON bv.volume_id = sr.volume_id AND bv.block_no = sr.block_no
                   AND bv.version = sr.block_version
                 WHERE sr.volume_id = ? AND s.state IN ('CREATING', 'COMMITTED', 'MERGING')
                """,
                (self.settings.volume_id,),
            ).fetchall()
            ref_mismatches: list[dict[str, Any]] = []
            for ref in active_refs:
                actual = sha256_bytes(bytes(ref["content"]))
                if actual != str(ref["digest"]).lower() or actual != str(ref["version_digest"]).lower():
                    ref_mismatches.append(
                        {
                            "snapshot_id": ref["snapshot_id"],
                            "block_no": int(ref["block_no"]),
                            "version": int(ref["block_version"]),
                        }
                    )
            resolved_snapshot_checks = 0
            snapshot_resolution_errors: list[dict[str, Any]] = []
            committed_snapshots = conn.execute(
                """
                SELECT snapshot_id FROM snapshots
                 WHERE volume_id = ? AND state = 'COMMITTED'
                 ORDER BY snapshot_id
                """,
                (self.settings.volume_id,),
            ).fetchall()
            for snap in committed_snapshots:
                snapshot_id = str(snap["snapshot_id"])
                try:
                    mapping, lineage = self._resolve_snapshot(
                        conn, self.settings.volume_id, snapshot_id
                    )
                    self._validate_resolved_snapshot(
                        conn,
                        self.settings.volume_id,
                        mapping,
                        block_count,
                        snapshot_id,
                    )
                    resolved_snapshot_checks += 1
                    lineage_states = [item["state"] for item in lineage]
                    if not lineage_states or any(state != COMMITTED for state in lineage_states):
                        snapshot_resolution_errors.append(
                            {
                                "snapshot_id": snapshot_id,
                                "reason": "active_lineage_not_fully_committed",
                                "lineage_states": lineage_states,
                            }
                        )
                except BusinessError as exc:
                    snapshot_resolution_errors.append(
                        {
                            "snapshot_id": snapshot_id,
                            "reason": exc.code,
                            "details": exc.details,
                        }
                    )
                except Exception as exc:
                    snapshot_resolution_errors.append(
                        {
                            "snapshot_id": snapshot_id,
                            "reason": "internal_error",
                            "error_type": type(exc).__name__,
                        }
                    )
            ok = not (
                fk_violations
                or missing_blocks
                or extra_blocks
                or digest_mismatches
                or transient
                or int(pending["c"])
                or ref_mismatches
                or int(in_progress_requests["c"])
                or snapshot_resolution_errors
            )
            return {
                "ok": ok,
                "foreign_key_violations": [dict(row) for row in fk_violations],
                "current_blocks_verified": block_count - len(missing_blocks),
                "missing_current_blocks": missing_blocks,
                "out_of_range_current_blocks": extra_blocks,
                "current_digest_mismatches": digest_mismatches,
                "active_snapshot_refs_verified": len(active_refs) - len(ref_mismatches),
                "active_snapshot_ref_mismatches": ref_mismatches,
                "resolved_snapshots_verified": resolved_snapshot_checks,
                "snapshot_resolution_errors": snapshot_resolution_errors,
                "transient_snapshots": [dict(row) for row in transient],
                "pending_intents": int(pending["c"]),
                "requests_in_progress": int(in_progress_requests["c"]),
            }
        finally:
            conn.close()

