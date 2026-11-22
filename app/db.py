from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from app.compiler import CompiledSnapshot, canonical_json


SCHEMA = """
CREATE TABLE IF NOT EXISTS intents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    base_version INTEGER NOT NULL,
    target_version INTEGER NOT NULL,
    payload_digest TEXT,
    fingerprint TEXT,
    state TEXT NOT NULL,
    error_code TEXT,
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    version INTEGER PRIMARY KEY,
    digest TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    route_count INTEGER NOT NULL,
    intent_id INTEGER REFERENCES intents(id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS route_budgets (
    version INTEGER NOT NULL REFERENCES snapshots(version) ON DELETE CASCADE,
    route_id TEXT NOT NULL,
    route_json TEXT NOT NULL,
    derivation_json TEXT NOT NULL,
    timeout_q6 INTEGER NOT NULL,
    connect_budget_q6 INTEGER NOT NULL,
    total_wait_q6 INTEGER NOT NULL,
    health_penalty_q6 INTEGER NOT NULL,
    PRIMARY KEY (version, route_id)
);

CREATE TABLE IF NOT EXISTS artifacts (
    version INTEGER NOT NULL REFERENCES snapshots(version) ON DELETE CASCADE,
    artifact_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    route_id TEXT,
    payload_json TEXT NOT NULL,
    digest TEXT NOT NULL,
    PRIMARY KEY (version, artifact_key)
);

CREATE TABLE IF NOT EXISTS artifact_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL REFERENCES snapshots(version) ON DELETE CASCADE,
    route_id TEXT,
    artifact_kind TEXT NOT NULL,
    action TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    last_complete_version INTEGER NOT NULL,
    last_complete_digest TEXT,
    orphan_intent_ids_json TEXT NOT NULL,
    report_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS idempotency (
    request_key TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    state TEXT NOT NULL,
    status_code INTEGER,
    headers_json TEXT,
    body BLOB,
    error_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_artifact_events_version ON artifact_events(version, id);
CREATE INDEX IF NOT EXISTS idx_intents_state ON intents(state);
"""


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


class SQLiteStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self.db_path), timeout=15.0, isolation_level=None
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 15000")
        return connection

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(SCHEMA)

    def create_intent(
        self,
        *,
        kind: str,
        base_version: int,
        target_version: int,
        payload_digest: str | None,
        fingerprint: str | None,
        detail: Mapping[str, Any] | None = None,
    ) -> int:
        now = utc_now()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO intents(
                    kind, base_version, target_version, payload_digest, fingerprint,
                    state, detail_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'PREPARED', ?, ?, ?)
                """,
                (
                    kind,
                    base_version,
                    target_version,
                    payload_digest,
                    fingerprint,
                    canonical_json(dict(detail or {})),
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def update_intent(
        self,
        intent_id: int,
        state: str,
        *,
        error_code: str | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> None:
        with self._connect() as connection:
            if detail is None:
                connection.execute(
                    "UPDATE intents SET state=?, error_code=?, updated_at=? WHERE id=?",
                    (state, error_code, utc_now(), intent_id),
                )
            else:
                row = connection.execute(
                    "SELECT detail_json FROM intents WHERE id=?", (intent_id,)
                ).fetchone()
                current = json.loads(row["detail_json"]) if row else {}
                current.update(detail)
                connection.execute(
                    """
                    UPDATE intents
                    SET state=?, error_code=?, detail_json=?, updated_at=?
                    WHERE id=?
                    """,
                    (state, error_code, canonical_json(current), utc_now(), intent_id),
                )

    def current_version(self) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COALESCE(MAX(version), 0) AS version FROM snapshots"
            ).fetchone()
            return int(row["version"])

    def latest_snapshot(self) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM snapshots ORDER BY version DESC LIMIT 1"
            ).fetchone()
            return dict(row) if row else None

    def snapshot(self, version: int) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM snapshots WHERE version=?", (version,)
            ).fetchone()
            return dict(row) if row else None

    def list_snapshots(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT version, digest, route_count, intent_id, created_at
                FROM snapshots ORDER BY version
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def commit_compiled(
        self,
        snapshot: CompiledSnapshot,
        *,
        intent_id: int,
        expected_base_version: int,
        payload_json: str,
    ) -> bool:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT COALESCE(MAX(version), 0) AS version FROM snapshots"
            ).fetchone()
            current_version = int(row["version"])
            if current_version != expected_base_version:
                connection.rollback()
                return False

            now = utc_now()
            connection.execute(
                """
                INSERT INTO snapshots(version, digest, payload_json, route_count, intent_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.version,
                    snapshot.digest,
                    payload_json,
                    snapshot.route_count,
                    intent_id,
                    now,
                ),
            )
            for plan in snapshot.routes:
                connection.execute(
                    """
                    INSERT INTO route_budgets(
                        version, route_id, route_json, derivation_json,
                        timeout_q6, connect_budget_q6, total_wait_q6, health_penalty_q6
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot.version,
                        plan.route.route_id,
                        canonical_json(plan.route.normalized_payload()),
                        canonical_json(plan.budget.payload()),
                        plan.budget.timeout_q6,
                        plan.budget.connect_budget_q6,
                        plan.budget.total_wait_q6,
                        plan.budget.health_penalty_q6,
                    ),
                )
            for artifact in snapshot.artifacts:
                connection.execute(
                    """
                    INSERT INTO artifacts(version, artifact_key, kind, route_id, payload_json, digest)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot.version,
                        artifact.key,
                        artifact.kind,
                        artifact.route_id,
                        canonical_json(artifact.payload),
                        artifact.digest,
                    ),
                )
            for event in snapshot.events:
                connection.execute(
                    """
                    INSERT INTO artifact_events(version, route_id, artifact_kind, action, reason, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot.version,
                        event.get("route_id"),
                        event["artifact_kind"],
                        event["action"],
                        event["reason"],
                        now,
                    ),
                )
            detail = {
                "committed_at": now,
                "route_count": snapshot.route_count,
                "artifact_count": len(snapshot.artifacts),
                "event_count": len(snapshot.events),
                "digest": snapshot.digest,
            }
            connection.execute(
                """
                UPDATE intents
                SET state='COMMITTED', detail_json=?, updated_at=?
                WHERE id=?
                """,
                (canonical_json(detail), now, intent_id),
            )
            connection.commit()
            return True
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def artifact_events(self, version: int) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, version, route_id, artifact_kind, action, reason, created_at
                FROM artifact_events WHERE version=? ORDER BY id
                """,
                (version,),
            ).fetchall()
            return [dict(row) for row in rows]

    def artifacts(self, version: int) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT artifact_key, kind, route_id, payload_json, digest
                FROM artifacts WHERE version=? ORDER BY artifact_key
                """,
                (version,),
            ).fetchall()
            return [dict(row) for row in rows]

    def budgets(self, version: int) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT route_id, route_json, derivation_json, timeout_q6,
                       connect_budget_q6, total_wait_q6, health_penalty_q6
                FROM route_budgets WHERE version=? ORDER BY route_id
                """,
                (version,),
            ).fetchall()
            return [dict(row) for row in rows]

    def list_intents(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM intents ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            result: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                item["detail"] = json.loads(item.pop("detail_json") or "{}")
                result.append(item)
            return result

    def recover(self) -> dict[str, Any]:
        started = utc_now()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT * FROM intents
                WHERE state IN ('PREPARED', 'COMPILING')
                ORDER BY id
                """
            ).fetchall()
            orphan_ids: list[int] = []
            orphan_details: list[dict[str, Any]] = []
            for row in rows:
                intent = dict(row)
                orphan_ids.append(int(intent["id"]))
                detail = json.loads(intent.get("detail_json") or "{}")
                detail["recovery"] = {
                    "action": "ROLLED_BACK",
                    "reason": "process_stopped_before_atomic_commit",
                    "observed_state": intent["state"],
                }
                connection.execute(
                    """
                    UPDATE intents
                    SET state='ROLLED_BACK', detail_json=?, updated_at=?
                    WHERE id=?
                    """,
                    (canonical_json(detail), started, intent["id"]),
                )
                orphan_details.append(
                    {
                        "intent_id": intent["id"],
                        "kind": intent["kind"],
                        "base_version": intent["base_version"],
                        "target_version": intent["target_version"],
                        "previous_state": intent["state"],
                    }
                )

            snapshot_row = connection.execute(
                "SELECT * FROM snapshots ORDER BY version DESC LIMIT 1"
            ).fetchone()
            last_version = int(snapshot_row["version"]) if snapshot_row else 0
            last_digest = snapshot_row["digest"] if snapshot_row else None

            abandoned = connection.execute(
                "SELECT request_key FROM idempotency WHERE state='IN_PROGRESS'"
            ).fetchall()
            abandoned_keys = [row["request_key"] for row in abandoned]
            connection.execute(
                "UPDATE idempotency SET state='ABANDONED', updated_at=? WHERE state='IN_PROGRESS'",
                (started,),
            )

            finished = utc_now()
            report = {
                "report_basis": "persisted_sqlite_facts_only",
                "started_at": started,
                "finished_at": finished,
                "orphan_intent_ids": orphan_ids,
                "orphan_intents": orphan_details,
                "abandoned_idempotency_keys": abandoned_keys,
                "last_complete_version": last_version,
                "last_complete_digest": last_digest,
                "rolled_back_intent_count": len(orphan_ids),
                "configured_version_after_recovery": last_version,
            }
            connection.execute(
                """
                INSERT INTO recovery_runs(
                    started_at, finished_at, last_complete_version,
                    last_complete_digest, orphan_intent_ids_json, report_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    started,
                    finished,
                    last_version,
                    last_digest,
                    canonical_json(orphan_ids),
                    canonical_json(report),
                ),
            )
            connection.commit()
            return report
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def recovery_reports(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM recovery_runs ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["report"] = json.loads(item.pop("report_json"))
                item["orphan_intent_ids"] = json.loads(item.pop("orphan_intent_ids_json"))
                result.append(item)
            return result

    def get_idempotency(self, request_key: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM idempotency WHERE request_key=?", (request_key,)
            ).fetchone()
            return dict(row) if row else None

    def begin_idempotency(self, request_key: str, fingerprint: str) -> dict[str, Any]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM idempotency WHERE request_key=?", (request_key,)
            ).fetchone()
            if row:
                connection.commit()
                return dict(row)
            now = utc_now()
            connection.execute(
                """
                INSERT INTO idempotency(
                    request_key, fingerprint, state, created_at, updated_at
                ) VALUES (?, ?, 'IN_PROGRESS', ?, ?)
                """,
                (request_key, fingerprint, now, now),
            )
            connection.commit()
            return {
                "request_key": request_key,
                "fingerprint": fingerprint,
                "state": "IN_PROGRESS",
            }
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def complete_idempotency(
        self,
        request_key: str,
        *,
        status_code: int,
        headers: Mapping[str, str],
        body: bytes,
        error_json: str | None = None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE idempotency
                SET state='COMPLETED', status_code=?, headers_json=?, body=?,
                    error_json=?, updated_at=?
                WHERE request_key=? AND state='IN_PROGRESS'
                """,
                (
                    status_code,
                    canonical_json(dict(headers)),
                    sqlite3.Binary(body),
                    error_json,
                    utc_now(),
                    request_key,
                ),
            )

    def abandon_idempotency(self, request_key: str, reason: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE idempotency
                SET state='ABANDONED', error_json=?, updated_at=?
                WHERE request_key=? AND state='IN_PROGRESS'
                """,
                (canonical_json({"reason": reason}), utc_now(), request_key),
            )

    def recent_commits(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT s.version, s.digest, s.route_count, s.created_at, i.kind, i.id AS intent_id
                FROM snapshots s LEFT JOIN intents i ON i.id=s.intent_id
                ORDER BY s.version DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
