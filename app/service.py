from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import uuid
from typing import Any, Awaitable, Callable

from app.db import Database
from app.errors import BusinessError, business_error
from app.manifest import canonical_object_digest, is_complete, manifest_from_parts
from app.models import CreateSessionRequest, FinalizeSessionRequest, SubmitPartRequest, utc_now


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _session_public(row: dict[str, Any], parts: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "session_id": row["id"],
        "object_key": row["object_key"],
        "backend_id": row["backend_id"],
        "max_parts": row["max_parts"],
        "total_parts": row["total_parts"],
        "priority": row["priority"],
        "status": row["status"],
        "version": row["version"],
        "conflict_policy": row["conflict_policy"],
        "submitted_parts": row["submitted_parts"],
        "completed_parts": row["completed_parts"],
        "object_digest": row["object_digest"],
        "is_complete": row["status"] == "COMPLETED" and row["completed_parts"] == row["total_parts"],
        "metadata": json.loads(row["metadata_json"] or "{}"),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _part_public(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "seq": row["seq"],
        "size_bytes": row["size_bytes"],
        "sha256": row["sha256"],
        "arrived_at": row["arrived_at"],
        "status": row["status"],
        "round_id": row["round_id"],
        "attempt_count": row["attempt_count"],
        "last_error_code": row["last_error_code"],
        "last_error_context": json.loads(row["last_error_context_json"] or "{}"),
        "verified_at": row["verified_at"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


class IdempotencyService:
    def __init__(self, database: Database, wait_seconds: float = 8.0) -> None:
        self.database = database
        self.wait_seconds = wait_seconds
        self._locks: dict[str, asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()

    async def _lock_for(self, request_id: str) -> asyncio.Lock:
        async with self._locks_guard:
            return self._locks.setdefault(request_id, asyncio.Lock())

    async def execute(
        self,
        request_id: str,
        operation: str,
        payload: dict[str, Any],
        action: Callable[[], Awaitable[tuple[int, dict[str, Any]]]],
    ) -> tuple[int, dict[str, Any]]:
        request_hash = hashlib.sha256(
            _json({"operation": operation, "payload": payload}).encode("utf-8")
        ).hexdigest()
        lock = await self._lock_for(request_id)
        async with lock:
            state, row = self.database.claim_idempotency(request_id, operation, request_hash)
            if state == "CONFLICT":
                raise business_error(
                    "E_CONFLICTING_REQUEST",
                    "request_id was already used with a different request body",
                    request_id=request_id,
                    existing_operation=row["operation"] if row else None,
                )
            if state == "COMPLETED" and row:
                return int(row["http_status"]), json.loads(row["response_json"])
            if state == "IN_PROGRESS":
                deadline = asyncio.get_running_loop().time() + self.wait_seconds
                while asyncio.get_running_loop().time() < deadline:
                    await asyncio.sleep(0.025)
                    existing = self.database.get_idempotency(request_id)
                    if existing is None:
                        break
                    if existing["state"] == "COMPLETED":
                        return int(existing["http_status"]), json.loads(existing["response_json"])
                    if existing["state"] == "ABORTED":
                        state, row = self.database.claim_idempotency(
                            request_id, operation, request_hash
                        )
                        if state == "CLAIMED":
                            break
                        if state == "CONFLICT":
                            raise business_error(
                                "E_CONFLICTING_REQUEST",
                                "request_id was already used with a different request body",
                                request_id=request_id,
                            )
                        continue
                else:
                    raise business_error(
                        "E_REQUEST_IN_PROGRESS",
                        "an identical request is still in progress",
                        request_id=request_id,
                    )

            try:
                status_code, response = await action()
            except BusinessError as error:
                status_code, response = error.status_code, error.as_dict()
            except Exception as error:  # pragma: no cover - defensive boundary
                status_code, response = 500, {
                    "code": "E_INTERNAL",
                    "message": "unexpected coordinator failure",
                    "context": {"error_type": type(error).__name__},
                }
            self.database.finish_idempotency(request_id, status_code, response)
            return status_code, response


class SessionService:
    def __init__(self, database: Database) -> None:
        self.database = database

    def _detail_from_snapshot(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        session = dict(snapshot["session"])
        parts = [dict(row) for row in snapshot["parts"]]
        metadata_row = snapshot["object_metadata"]
        quota_row = snapshot["backend_quota"]
        metadata = None
        if metadata_row:
            metadata = {
                "object_key": metadata_row["object_key"],
                "status": metadata_row["status"],
                "manifest": json.loads(metadata_row["manifest_json"] or "[]"),
                "object_digest": metadata_row["object_digest"],
                "version": metadata_row["version"],
            }
        quota = None
        if quota_row:
            quota = {
                "backend_id": quota_row["backend_id"],
                "capacity_bytes": quota_row["capacity_bytes"],
                "committed_bytes": quota_row["committed_bytes"],
                "reserved_bytes": quota_row["reserved_bytes"],
                "remaining_bytes": quota_row["capacity_bytes"]
                - quota_row["committed_bytes"]
                - quota_row["reserved_bytes"],
                "version": quota_row["version"],
            }
        result = {
            "version": session["version"],
            "read_version": snapshot["read_version"],
            "session": _session_public(session, parts),
            "parts": [_part_public(part) for part in parts],
            "object_metadata": metadata,
            "backend_quota": quota,
        }
        return result

    def get_session(self, session_id: str) -> dict[str, Any]:
        snapshot = self.database.read_session_snapshot(session_id)
        if snapshot is None:
            raise business_error("E_NOT_FOUND", "upload session not found", session_id=session_id)
        return self._detail_from_snapshot(snapshot)

    def create_session(self, request: CreateSessionRequest) -> dict[str, Any]:
        now = utc_now()
        session_id = request.session_id or f"sess-{uuid.uuid4().hex[:16]}"
        total_parts = request.total_parts or request.max_parts
        if total_parts > request.max_parts:
            raise business_error(
                "E_PART_SEQ_INVALID",
                "total_parts exceeds max_parts",
                total_parts=total_parts,
                max_parts=request.max_parts,
            )
        try:
            with self.database.transaction() as connection:
                backend = connection.execute(
                    "SELECT * FROM backends WHERE id=? AND enabled=1", (request.backend_id,)
                ).fetchone()
                if backend is None:
                    raise business_error(
                        "E_NOT_FOUND",
                        "backend is not declared or is disabled",
                        backend_id=request.backend_id,
                    )
                if request.max_parts > backend["max_parts_per_session"]:
                    raise business_error(
                        "E_PART_SEQ_INVALID",
                        "session max_parts exceeds backend per-session part limit",
                        max_parts=request.max_parts,
                        backend_max_parts_per_session=backend["max_parts_per_session"],
                    )
                existing = connection.execute(
                    "SELECT id FROM sessions WHERE object_key=? AND status='ACTIVE'",
                    (request.object_key,),
                ).fetchone()
                if existing is not None:
                    raise business_error(
                        "E_SESSION_CONFLICT",
                        "an active upload session already exists for this object key",
                        object_key=request.object_key,
                        existing_session_id=existing["id"],
                        conflict_policy=request.conflict_policy,
                    )
                connection.execute(
                    """
                    INSERT INTO sessions(id, object_key, backend_id, max_parts, total_parts,
                                         priority, status, version, conflict_policy, metadata_json,
                                         completed_parts, submitted_parts, object_digest,
                                         created_at, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, 'ACTIVE', 1, ?, ?, 0, 0, NULL, ?, ?)
                    """,
                    (
                        session_id,
                        request.object_key,
                        request.backend_id,
                        request.max_parts,
                        total_parts,
                        request.priority,
                        request.conflict_policy,
                        _json(request.metadata),
                        now,
                        now,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO object_metadata(session_id, object_key, status, manifest_json,
                                                object_digest, version, updated_at)
                    VALUES(?, ?, 'DRAFT', '[]', NULL, 1, ?)
                    """,
                    (session_id, request.object_key, now),
                )
                connection.execute(
                    """
                    INSERT INTO audit_events(session_id, event_type, detail_json, created_at)
                    VALUES(?, 'SESSION_CREATED', ?, ?)
                    """,
                    (
                        session_id,
                        _json(
                            {
                                "object_key": request.object_key,
                                "backend_id": request.backend_id,
                                "priority": request.priority,
                                "total_parts": total_parts,
                            }
                        ),
                        now,
                    ),
                )
        except sqlite3.IntegrityError as error:
            with self.database.transaction(immediate=False) as connection:
                existing = connection.execute(
                    "SELECT id FROM sessions WHERE object_key=? AND status='ACTIVE'",
                    (request.object_key,),
                ).fetchone()
                duplicate_id = connection.execute(
                    "SELECT id FROM sessions WHERE id=?", (session_id,)
                ).fetchone()
            if existing is not None or duplicate_id is not None:
                raise business_error(
                    "E_SESSION_CONFLICT",
                    "session_id already exists or object key has an active session",
                    existing_session_id=existing["id"] if existing is not None else session_id,
                    object_key=request.object_key,
                ) from error
            raise
        return self.get_session(session_id)

    def submit_part(self, session_id: str, request: SubmitPartRequest) -> dict[str, Any]:
        now = utc_now()
        arrived_at = request.arrived_at or now
        duplicate = False
        with self.database.transaction() as connection:
            session = connection.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
            if session is None:
                raise business_error("E_NOT_FOUND", "upload session not found", session_id=session_id)
            if request.seq < 1 or request.seq > session["total_parts"] or request.seq > session["max_parts"]:
                raise business_error(
                    "E_PART_SEQ_INVALID",
                    "part sequence is outside the declared session range",
                    session_id=session_id,
                    seq=request.seq,
                    min_seq=1,
                    max_seq=min(session["total_parts"], session["max_parts"]),
                )
            existing = connection.execute(
                "SELECT * FROM parts WHERE session_id=? AND seq=?", (session_id, request.seq)
            ).fetchone()
            if existing is not None:
                if existing["sha256"].lower() != request.sha256.lower():
                    raise business_error(
                        "E_PART_MISMATCH",
                        "part sequence was already submitted with a different digest",
                        session_id=session_id,
                        seq=request.seq,
                        existing_sha256=existing["sha256"],
                        submitted_sha256=request.sha256,
                    )
                duplicate = True
            else:
                if session["status"] != "ACTIVE":
                    raise business_error(
                        "E_SESSION_NOT_ACTIVE",
                        "upload session is not accepting new parts",
                        session_id=session_id,
                        status=session["status"],
                    )
                connection.execute(
                    """
                    INSERT INTO parts(session_id, seq, size_bytes, sha256, arrived_at, status,
                                      attempt_count, created_at, updated_at)
                    VALUES(?, ?, ?, ?, ?, 'PENDING', 0, ?, ?)
                    """,
                    (
                        session_id,
                        request.seq,
                        request.size_bytes,
                        request.sha256,
                        arrived_at,
                        now,
                        now,
                    ),
                )
                connection.execute(
                    """
                    UPDATE sessions SET submitted_parts=submitted_parts+1,
                        version=version+1, updated_at=? WHERE id=?
                    """,
                    (now, session_id),
                )
                connection.execute(
                    "UPDATE object_metadata SET version=version+1, updated_at=? WHERE session_id=?",
                    (now, session_id),
                )
                connection.execute(
                    """
                    INSERT INTO audit_events(session_id, event_type, detail_json, created_at)
                    VALUES(?, 'PART_SUBMITTED', ?, ?)
                    """,
                    (
                        session_id,
                        _json(
                            {
                                "seq": request.seq,
                                "size_bytes": request.size_bytes,
                                "sha256": request.sha256,
                            }
                        ),
                        now,
                    ),
                )
        result = self.get_session(session_id)
        result["duplicate"] = duplicate
        return result

    def finalize_session(
        self, session_id: str, request: FinalizeSessionRequest | None = None
    ) -> dict[str, Any]:
        now = utc_now()
        with self.database.transaction() as connection:
            session = connection.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
            if session is None:
                raise business_error("E_NOT_FOUND", "upload session not found", session_id=session_id)
            parts = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM parts WHERE session_id=? ORDER BY seq", (session_id,)
                ).fetchall()
            ]
            seqs = {int(part["seq"]) for part in parts}
            expected = set(range(1, session["total_parts"] + 1))
            missing = sorted(expected - seqs)
            if missing:
                raise business_error(
                    "E_PART_SEQ_INVALID",
                    "part sequence is not continuous; assembly was not attempted",
                    session_id=session_id,
                    missing_seqs=missing,
                    total_parts=session["total_parts"],
                )
            not_verified = sorted(
                int(part["seq"]) for part in parts if part["status"] != "VERIFIED"
            )
            if not_verified:
                raise business_error(
                    "E_PARTS_INCOMPLETE",
                    "some submitted parts have not passed readback verification",
                    session_id=session_id,
                    not_verified_seqs=not_verified,
                )
            if session["status"] == "COMPLETED":
                return self.get_session(session_id)
            manifest = manifest_from_parts(parts)
            object_digest = canonical_object_digest(parts)
            connection.execute(
                """
                UPDATE sessions SET status='COMPLETED', completed_parts=?,
                    object_digest=?, version=version+1, updated_at=? WHERE id=?
                """,
                (len(parts), object_digest, now, session_id),
            )
            connection.execute(
                """
                UPDATE object_metadata SET status='READY', manifest_json=?, object_digest=?,
                    version=version+1, updated_at=? WHERE session_id=?
                """,
                (_json(manifest), object_digest, now, session_id),
            )
            connection.execute(
                """
                INSERT INTO audit_events(session_id, event_type, detail_json, created_at)
                VALUES(?, 'OBJECT_FINALIZED', ?, ?)
                """,
                (session_id, _json({"object_digest": object_digest}), now),
            )
        return self.get_session(session_id)

    def list_backend_quotas(self) -> dict[str, Any]:
        with self.database.transaction(immediate=False) as connection:
            rows = connection.execute(
                """
                SELECT q.backend_id, q.capacity_bytes, q.committed_bytes, q.reserved_bytes,
                       q.capacity_bytes - q.committed_bytes - q.reserved_bytes AS remaining_bytes,
                       q.version, b.url, b.max_concurrency, b.max_parts_per_session
                FROM backend_quota q JOIN backends b ON b.id=q.backend_id
                ORDER BY q.backend_id
                """
            ).fetchall()
        return {"backends": [dict(row) for row in rows], "version": max((r["version"] for r in rows), default=0)}

    def backend_quota(self, backend_id: str) -> dict[str, Any]:
        with self.database.transaction(immediate=False) as connection:
            row = connection.execute(
                """
                SELECT q.backend_id, q.capacity_bytes, q.committed_bytes, q.reserved_bytes,
                       q.capacity_bytes - q.committed_bytes - q.reserved_bytes AS remaining_bytes,
                       q.version, b.url, b.max_concurrency, b.max_parts_per_session
                FROM backend_quota q JOIN backends b ON b.id=q.backend_id
                WHERE q.backend_id=?
                """,
                (backend_id,),
            ).fetchone()
        if row is None:
            raise business_error("E_NOT_FOUND", "backend not found", backend_id=backend_id)
        return dict(row)
