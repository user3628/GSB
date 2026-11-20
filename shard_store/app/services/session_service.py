"""Upload-session lifecycle: creation, conflict policy, cancellation."""
from __future__ import annotations

import sqlite3

from app.config import Settings
from app.db.connection import transaction
from app.errors import BusinessError
from app.repositories import SessionRepository
from app.services.locking import registry
from app.utils import new_session_id


def row_to_dict(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


class SessionService:
    def __init__(self, settings: Settings):
        self.settings = settings

    def create_session(
        self,
        object_key: str,
        min_part_size: int | None = None,
        max_part_size: int | None = None,
        expected_total_size: int | None = None,
        merge_strategy: str = "fast",
        conflict_policy: str = "reject",
    ) -> dict:
        if not object_key or not isinstance(object_key, str):
            raise BusinessError("E_PART_INVALID", "object_key must be a non-empty string", 400)
        min_size = self.settings.min_part_size_default if min_part_size is None else min_part_size
        max_size = self.settings.max_part_size_default if max_part_size is None else max_part_size
        if min_size <= 0 or max_size < min_size:
            raise BusinessError(
                "E_PART_INVALID",
                "size bounds invalid: require 0 < min_part_size <= max_part_size",
                400,
            )
        if merge_strategy not in ("fast", "full"):
            raise BusinessError("E_PART_INVALID", "merge_strategy must be 'fast' or 'full'", 400)
        if conflict_policy not in ("reject", "allow"):
            raise BusinessError("E_PART_INVALID", "conflict_policy must be 'reject' or 'allow'", 400)
        if expected_total_size is not None and expected_total_size < 0:
            raise BusinessError("E_PART_INVALID", "expected_total_size must be >= 0", 400)

        session_id = new_session_id()
        with _connect(self.settings) as conn, transaction(conn, immediate=True):
            repo = SessionRepository(conn)
            open_sessions = repo.list_open_for_key(object_key)
            if open_sessions and conflict_policy == "reject":
                raise BusinessError(
                    "E_SESSION_CONFLICT",
                    f"object key '{object_key}' already has an in-progress session",
                    409,
                    details={
                        "object_key": object_key,
                        "existing_session_ids": [r["session_id"] for r in open_sessions],
                        "rule": "reject policy: only one open/merging session per key; "
                                "retry with conflict_policy='allow' to coexist",
                    },
                )
            repo.create(
                session_id, object_key, min_size, max_size,
                expected_total_size, merge_strategy, conflict_policy,
            )
            row = repo.get(session_id)
            result = row_to_dict(row)
            result["coexistence"] = bool(open_sessions)
            if open_sessions:
                # Explicit coexistence rule: sessions may proceed in parallel,
                # but their merges serialise on the object key and each takes
                # the next monotonically increasing version number.
                result["coexisting_session_ids"] = [r["session_id"] for r in open_sessions]
            return result

    def get_session(self, session_id: str) -> dict:
        with _connect(self.settings) as conn:
            row = SessionRepository(conn).get(session_id)
            if row is None:
                raise BusinessError("E_SESSION_NOT_FOUND", f"session '{session_id}' not found", 404)
            return row_to_dict(row)

    def list_sessions(self) -> list[dict]:
        with _connect(self.settings) as conn:
            return [row_to_dict(r) for r in SessionRepository(conn).list_all()]

    def cancel_session(self, session_id: str) -> dict:
        lock = registry.session_lock(session_id)
        with lock:
            with _connect(self.settings) as conn, transaction(conn, immediate=True):
                repo = SessionRepository(conn)
                row = repo.get(session_id)
                if row is None:
                    raise BusinessError("E_SESSION_NOT_FOUND", f"session '{session_id}' not found", 404)
                status = row["status"]
                if status == "completed":
                    raise BusinessError(
                        "E_SESSION_COMPLETED",
                        "a completed session cannot be cancelled",
                        409,
                        details={"session_id": session_id},
                    )
                if status == "merging":
                    raise BusinessError(
                        "E_SESSION_CONFLICT",
                        "session is merging; wait for merge or run crash recovery",
                        409,
                    )
                if status == "cancelled":
                    # Idempotent cancellation: second call reports current state.
                    return row_to_dict(row)
                repo.update_status(session_id, "cancelled")
                return row_to_dict(repo.get(session_id))


def _connect(settings: Settings):
    from app.db.connection import connect
    return connect(settings)
