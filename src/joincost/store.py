from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .errors import (
    BusinessError,
    E_CONFLICTING_REQUEST,
    E_INTERNAL,
    E_NOT_FOUND,
    E_REQUEST_ABORTED,
    E_SNAPSHOT_CONFLICT,
    E_VERSION_RETIRED,
)
from .model import StatsSnapshot, canonical_json, coerce_snapshot_from_dict
from .fixed import MAX_ROWS


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshot_versions (
    version INTEGER PRIMARY KEY,
    digest TEXT NOT NULL,
    format TEXT NOT NULL,
    format_version INTEGER NOT NULL,
    decision TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    report_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS current_snapshot (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    version INTEGER NOT NULL,
    digest TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS import_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT,
    detected_format TEXT,
    format_version INTEGER,
    decision TEXT,
    code TEXT,
    digest TEXT,
    report_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS intents (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    reference_version INTEGER,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    detail_json TEXT
);
CREATE TABLE IF NOT EXISTS plan_requests (
    request_id TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    snapshot_version INTEGER,
    snapshot_digest TEXT,
    response_json TEXT,
    error_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS recovery_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    report_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);
CREATE INDEX IF NOT EXISTS idx_plan_status ON plan_requests(status);
"""


class Store:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(SCHEMA)
            connection.commit()
        finally:
            connection.close()

    @contextmanager
    def _write_tx(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    def current_snapshot_binding(self) -> dict[str, Any] | None:
        with self._read() as connection:
            row = connection.execute(
                "SELECT version, digest FROM current_snapshot WHERE singleton = 1"
            ).fetchone()
        return dict(row) if row else None

    def current_snapshot_view(self) -> dict[str, Any]:
        with self._read() as connection:
            connection.execute("BEGIN")
            binding = connection.execute(
                "SELECT version, digest FROM current_snapshot WHERE singleton = 1"
            ).fetchone()
            if binding is None:
                versions = connection.execute(
                    "SELECT version, digest, format, format_version, decision, created_at "
                    "FROM snapshot_versions ORDER BY version"
                ).fetchall()
                connection.execute("COMMIT")
                return {
                    "current_snapshot": None,
                    "snapshots": [
                        {**dict(item), "current": False} for item in versions
                    ],
                }
            row = connection.execute(
                "SELECT snapshot_json FROM snapshot_versions WHERE version = ?",
                (binding["version"],),
            ).fetchone()
            versions = connection.execute(
                "SELECT version, digest, format, format_version, decision, created_at "
                "FROM snapshot_versions ORDER BY version"
            ).fetchall()
            connection.execute("COMMIT")
        if row is None:
            raise BusinessError(
                E_INTERNAL,
                "current snapshot pointer references a missing version",
                {"version": binding["version"]},
            )
        snapshot = coerce_snapshot_from_dict(json.loads(row["snapshot_json"]))
        if snapshot.digest != binding["digest"]:
            raise BusinessError(
                E_INTERNAL,
                "current snapshot digest does not match its pointer",
                {"version": binding["version"]},
            )
        return {
            "current_snapshot": snapshot.public_dict(),
            "snapshots": [
                {**dict(item), "current": item["version"] == binding["version"]}
                for item in versions
            ],
        }

    def get_snapshot(self, version: int) -> StatsSnapshot:
        if version < 0 or version > MAX_ROWS:
            raise BusinessError(
                E_VERSION_RETIRED,
                "snapshot version is outside the retained signed 64-bit range",
                {"requested_version": version},
            )
        with self._read() as connection:
            row = connection.execute(
                "SELECT snapshot_json, digest FROM snapshot_versions WHERE version = ?",
                (version,),
            ).fetchone()
        if row is None:
            current = self.current_snapshot_binding()
            raise BusinessError(
                E_VERSION_RETIRED,
                "the requested statistics snapshot version is not retained",
                {"requested_version": version, "current": current},
            )
        raw = json.loads(row["snapshot_json"])
        snapshot = coerce_snapshot_from_dict(raw)
        if snapshot.digest != row["digest"]:
            raise BusinessError(
                E_INTERNAL,
                "stored snapshot digest does not match its row",
                {"version": version},
            )
        return snapshot

    def list_snapshots(self) -> list[dict[str, Any]]:
        with self._read() as connection:
            rows = connection.execute(
                "SELECT version, digest, format, format_version, decision, created_at "
                "FROM snapshot_versions ORDER BY version"
            ).fetchall()
            current = connection.execute(
                "SELECT version FROM current_snapshot WHERE singleton = 1"
            ).fetchone()
        current_version = current["version"] if current else None
        return [
            {
                **dict(row),
                "current": row["version"] == current_version,
            }
            for row in rows
        ]

    def record_import_report(
        self,
        *,
        source: str | None,
        report: dict[str, Any],
        code: str | None = None,
    ) -> int:
        with self._write_tx() as connection:
            cursor = connection.execute(
                "INSERT INTO import_reports("
                "source, detected_format, format_version, decision, code, digest, "
                "report_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    source,
                    report.get("detected_format"),
                    report.get("format_version"),
                    report.get("decision"),
                    code,
                    report.get("digest"),
                    canonical_json(report),
                    utc_now(),
                ),
            )
            return int(cursor.lastrowid)

    def list_import_reports(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._read() as connection:
            rows = connection.execute(
                "SELECT * FROM import_reports ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [
            {
                **dict(row),
                "report_json": json.loads(row["report_json"]),
            }
            for row in rows
        ]

    def write_intent(
        self,
        *,
        intent_id: str,
        kind: str,
        reference_version: int | None,
        payload: dict[str, Any],
    ) -> None:
        now = utc_now()
        with self._write_tx() as connection:
            try:
                connection.execute(
                    "INSERT INTO intents(id, kind, reference_version, payload_json, "
                    "status, created_at, updated_at) VALUES (?, ?, ?, ?, 'PENDING', ?, ?)",
                    (intent_id, kind, reference_version, canonical_json(payload), now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise BusinessError(
                    E_CONFLICTING_REQUEST,
                    "intent id already exists",
                    {"intent_id": intent_id},
                ) from exc

    def mark_intent(
        self, intent_id: str, status: str, detail: dict[str, Any] | None = None
    ) -> None:
        with self._write_tx() as connection:
            connection.execute(
                "UPDATE intents SET status = ?, updated_at = ?, detail_json = ? "
                "WHERE id = ?",
                (status, utc_now(), canonical_json(detail) if detail is not None else None, intent_id),
            )

    def commit_snapshot(
        self,
        snapshot: StatsSnapshot,
        report: dict[str, Any],
        *,
        source: str | None,
        intent_id: str | None = None,
    ) -> dict[str, Any]:
        with self._write_tx() as connection:
            current = connection.execute(
                "SELECT version, digest FROM current_snapshot WHERE singleton = 1"
            ).fetchone()
            current_version = current["version"] if current else None
            current_digest = current["digest"] if current else None
            duplicate = False
            if current is not None:
                if snapshot.version < current_version:
                    raise BusinessError(
                        E_VERSION_RETIRED,
                        "snapshot version is older than the current version",
                        {
                            "incoming_version": snapshot.version,
                            "current_version": current_version,
                        },
                    )
                if snapshot.version == current_version:
                    if snapshot.digest != current_digest:
                        raise BusinessError(
                            E_SNAPSHOT_CONFLICT,
                            "snapshot version already exists with a different digest",
                            {
                                "version": snapshot.version,
                                "incoming_digest": snapshot.digest,
                                "current_digest": current_digest,
                            },
                        )
                    duplicate = True
            existing_version = connection.execute(
                "SELECT digest FROM snapshot_versions WHERE version = ?",
                (snapshot.version,),
            ).fetchone()
            if existing_version is not None:
                if existing_version["digest"] != snapshot.digest:
                    raise BusinessError(
                        E_SNAPSHOT_CONFLICT,
                        "snapshot version already exists with a different digest",
                        {
                            "version": snapshot.version,
                            "incoming_digest": snapshot.digest,
                            "stored_digest": existing_version["digest"],
                        },
                    )
                duplicate = True
                connection.execute(
                    "INSERT INTO current_snapshot(singleton, version, digest) "
                    "VALUES (1, ?, ?) ON CONFLICT(singleton) DO UPDATE SET "
                    "version = excluded.version, digest = excluded.digest",
                    (snapshot.version, snapshot.digest),
                )
            if not duplicate:
                connection.execute(
                    "INSERT INTO snapshot_versions(version, digest, format, format_version, "
                    "decision, snapshot_json, report_json, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        snapshot.version,
                        snapshot.digest,
                        snapshot.format_name,
                        snapshot.format_version,
                        snapshot.decision,
                        canonical_json(snapshot.storage_dict()),
                        canonical_json(report),
                        utc_now(),
                    ),
                )
                connection.execute(
                    "INSERT INTO current_snapshot(singleton, version, digest) "
                    "VALUES (1, ?, ?) ON CONFLICT(singleton) DO UPDATE SET "
                    "version = excluded.version, digest = excluded.digest",
                    (snapshot.version, snapshot.digest),
                )
            connection.execute(
                "INSERT INTO import_reports("
                "source, detected_format, format_version, decision, code, digest, "
                "report_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    source,
                    report.get("detected_format"),
                    report.get("format_version"),
                    report.get("decision"),
                    None,
                    snapshot.digest,
                    canonical_json(report),
                    utc_now(),
                ),
            )
            if intent_id:
                connection.execute(
                    "UPDATE intents SET status = 'COMPLETED', updated_at = ?, "
                    "detail_json = ? WHERE id = ?",
                    (
                        utc_now(),
                        canonical_json(
                            {
                                "snapshot_version": snapshot.version,
                                "digest": snapshot.digest,
                                "duplicate": duplicate,
                            }
                        ),
                        intent_id,
                    ),
                )
        return {
            "duplicate": duplicate,
            "previous_version": current_version,
            "current_version": snapshot.version,
            "digest": snapshot.digest,
        }

    def begin_plan_request(
        self,
        *,
        request_id: str,
        request_hash: str,
        snapshot_version: int,
        snapshot_digest: str,
    ) -> dict[str, Any]:
        with self._write_tx() as connection:
            existing = connection.execute(
                "SELECT * FROM plan_requests WHERE request_id = ?", (request_id,)
            ).fetchone()
            if existing is not None:
                if existing["request_hash"] != request_hash:
                    raise BusinessError(
                        E_CONFLICTING_REQUEST,
                        "request id was previously submitted with different content",
                        {
                            "request_id": request_id,
                            "existing_hash": existing["request_hash"],
                            "incoming_hash": request_hash,
                        },
                    )
                return {
                    "state": existing["status"],
                    "existing": True,
                    "response": json.loads(existing["response_json"])
                    if existing["response_json"]
                    else None,
                    "error": json.loads(existing["error_json"])
                    if existing["error_json"]
                    else None,
                }
            now = utc_now()
            connection.execute(
                "INSERT INTO plan_requests(request_id, request_hash, status, "
                "snapshot_version, snapshot_digest, created_at, updated_at) "
                "VALUES (?, ?, 'RUNNING', ?, ?, ?, ?)",
                (request_id, request_hash, snapshot_version, snapshot_digest, now, now),
            )
            connection.execute(
                "INSERT INTO intents(id, kind, reference_version, payload_json, status, "
                "created_at, updated_at) VALUES (?, 'plan_build', ?, ?, 'PENDING', ?, ?)",
                (
                    f"plan:{request_id}",
                    snapshot_version,
                    canonical_json(
                        {
                            "request_id": request_id,
                            "snapshot_version": snapshot_version,
                            "snapshot_digest": snapshot_digest,
                        }
                    ),
                    now,
                    now,
                ),
            )
            return {"state": "NEW", "existing": False, "response": None, "error": None}

    def complete_plan_request(
        self, request_id: str, response: dict[str, Any]
    ) -> None:
        with self._write_tx() as connection:
            now = utc_now()
            cursor = connection.execute(
                "UPDATE plan_requests SET status = 'COMPLETED', response_json = ?, "
                "error_json = NULL, updated_at = ? WHERE request_id = ?",
                (canonical_json(response), now, request_id),
            )
            if cursor.rowcount != 1:
                raise BusinessError(
                    E_NOT_FOUND, "plan request disappeared while completing", {"request_id": request_id}
                )
            connection.execute(
                "UPDATE intents SET status = 'COMPLETED', updated_at = ?, detail_json = ? "
                "WHERE id = ?",
                (
                    now,
                    canonical_json(
                        {
                            "snapshot_version": response.get("snapshot_version"),
                            "snapshot_digest": response.get("snapshot_digest"),
                        }
                    ),
                    f"plan:{request_id}",
                ),
            )

    def fail_plan_request(
        self, request_id: str, error: dict[str, Any], status: str = "FAILED"
    ) -> None:
        with self._write_tx() as connection:
            now = utc_now()
            connection.execute(
                "UPDATE plan_requests SET status = ?, error_json = ?, updated_at = ? "
                "WHERE request_id = ?",
                (status, canonical_json(error), now, request_id),
            )
            connection.execute(
                "UPDATE intents SET status = ?, updated_at = ?, detail_json = ? WHERE id = ?",
                (status, now, canonical_json(error), f"plan:{request_id}"),
            )

    def get_plan_request(self, request_id: str) -> dict[str, Any]:
        with self._read() as connection:
            row = connection.execute(
                "SELECT * FROM plan_requests WHERE request_id = ?", (request_id,)
            ).fetchone()
        if row is None:
            raise BusinessError(
                E_NOT_FOUND, "plan request was not found", {"request_id": request_id}
            )
        return {
            "request_id": row["request_id"],
            "request_hash": row["request_hash"],
            "status": row["status"],
            "snapshot_version": row["snapshot_version"],
            "snapshot_digest": row["snapshot_digest"],
            "response": json.loads(row["response_json"]) if row["response_json"] else None,
            "error": json.loads(row["error_json"]) if row["error_json"] else None,
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def pending_intents(self) -> list[dict[str, Any]]:
        with self._read() as connection:
            rows = connection.execute(
                "SELECT * FROM intents WHERE status IN ('PENDING', 'STARTED') "
                "ORDER BY created_at, id"
            ).fetchall()
        return [
            {
                **dict(row),
                "payload": json.loads(row["payload_json"]),
            }
            for row in rows
        ]

    def recover(self) -> dict[str, Any]:
        started_at = utc_now()
        differences: list[dict[str, Any]] = []
        aborted_plans: list[dict[str, Any]] = []
        recovered_commits: list[dict[str, Any]] = []
        pending_count = 0
        with self._write_tx() as connection:
            before = connection.execute(
                "SELECT version, digest FROM current_snapshot WHERE singleton = 1"
            ).fetchone()
            before_version = before["version"] if before else None
            pending = connection.execute(
                "SELECT * FROM intents WHERE status IN ('PENDING', 'STARTED') "
                "ORDER BY created_at, id"
            ).fetchall()
            pending_count = len(pending)
            for intent in pending:
                payload = json.loads(intent["payload_json"])
                if intent["kind"] == "plan_build":
                    request_id = payload.get("request_id")
                    request = connection.execute(
                        "SELECT * FROM plan_requests WHERE request_id = ?", (request_id,)
                    ).fetchone()
                    if request is not None and request["status"] == "COMPLETED":
                        connection.execute(
                            "UPDATE intents SET status = 'RECOVERED_COMMITTED', updated_at = ?, "
                            "detail_json = ? WHERE id = ?",
                            (
                                utc_now(),
                                canonical_json({"result": "plan was already complete"}),
                                intent["id"],
                            ),
                        )
                    else:
                        error = {
                            "ok": False,
                            "error": {
                                "code": E_REQUEST_ABORTED,
                                "message": "process terminated before plan build committed",
                                "details": {
                                    "request_id": request_id,
                                    "snapshot_version": payload.get("snapshot_version"),
                                    "recovery": True,
                                },
                            },
                        }
                        if request is not None:
                            connection.execute(
                                "UPDATE plan_requests SET status = 'ABORTED', error_json = ?, "
                                "updated_at = ? WHERE request_id = ?",
                                (canonical_json(error), utc_now(), request_id),
                            )
                        connection.execute(
                            "UPDATE intents SET status = 'ABORTED', updated_at = ?, "
                            "detail_json = ? WHERE id = ?",
                            (utc_now(), canonical_json(error), intent["id"]),
                        )
                        aborted_plans.append(
                            {
                                "request_id": request_id,
                                "snapshot_version": payload.get("snapshot_version"),
                                "status": "ABORTED",
                            }
                        )
                elif intent["kind"] == "snapshot_commit":
                    snapshot_version = payload.get("snapshot_version")
                    snapshot_digest = payload.get("digest")
                    committed = connection.execute(
                        "SELECT version, digest FROM snapshot_versions WHERE version = ?",
                        (snapshot_version,),
                    ).fetchone()
                    if committed is not None and committed["digest"] == snapshot_digest:
                        connection.execute(
                            "UPDATE intents SET status = 'RECOVERED_COMMITTED', updated_at = ?, "
                            "detail_json = ? WHERE id = ?",
                            (
                                utc_now(),
                                canonical_json({"result": "snapshot transaction committed atomically"}),
                                intent["id"],
                            ),
                        )
                        recovered_commits.append(
                            {
                                "snapshot_version": snapshot_version,
                                "digest": snapshot_digest,
                            }
                        )
                    else:
                        connection.execute(
                            "UPDATE intents SET status = 'ABORTED', updated_at = ?, "
                            "detail_json = ? WHERE id = ?",
                            (
                                utc_now(),
                                canonical_json({"result": "snapshot was not committed"}),
                                intent["id"],
                            ),
                        )
                        differences.append(
                            {
                                "kind": "snapshot_intent_aborted",
                                "snapshot_version": snapshot_version,
                                "message": "old snapshot remains current",
                            }
                        )
            running_requests = connection.execute(
                "SELECT request_id, snapshot_version FROM plan_requests WHERE status = 'RUNNING'"
            ).fetchall()
            for request in running_requests:
                if not any(item["request_id"] == request["request_id"] for item in aborted_plans):
                    error = {
                        "ok": False,
                        "error": {
                            "code": E_REQUEST_ABORTED,
                            "message": "orphaned running plan was aborted during recovery",
                            "details": {"request_id": request["request_id"], "recovery": True},
                        },
                    }
                    connection.execute(
                        "UPDATE plan_requests SET status = 'ABORTED', error_json = ?, "
                        "updated_at = ? WHERE request_id = ?",
                        (canonical_json(error), utc_now(), request["request_id"]),
                    )
                    aborted_plans.append(
                        {
                            "request_id": request["request_id"],
                            "snapshot_version": request["snapshot_version"],
                            "status": "ABORTED",
                        }
                    )
            current = connection.execute(
                "SELECT version, digest FROM current_snapshot WHERE singleton = 1"
            ).fetchone()
            if current is not None:
                valid = connection.execute(
                    "SELECT version, digest FROM snapshot_versions WHERE version = ?",
                    (current["version"],),
                ).fetchone()
                if valid is None or valid["digest"] != current["digest"]:
                    latest = connection.execute(
                        "SELECT version, digest FROM snapshot_versions ORDER BY version DESC LIMIT 1"
                    ).fetchone()
                    if latest is None:
                        connection.execute("DELETE FROM current_snapshot WHERE singleton = 1")
                        differences.append(
                            {
                                "kind": "current_snapshot_cleared",
                                "reason": "no complete snapshot version remained",
                            }
                        )
                    else:
                        connection.execute(
                            "UPDATE current_snapshot SET version = ?, digest = ? WHERE singleton = 1",
                            (latest["version"], latest["digest"]),
                        )
                        differences.append(
                            {
                                "kind": "current_snapshot_rolled_back",
                                "from_version": current["version"],
                                "to_version": latest["version"],
                            }
                        )
            else:
                latest = connection.execute(
                    "SELECT version, digest FROM snapshot_versions ORDER BY version DESC LIMIT 1"
                ).fetchone()
                if latest is not None:
                    connection.execute(
                        "INSERT INTO current_snapshot(singleton, version, digest) "
                        "VALUES (1, ?, ?)",
                        (latest["version"], latest["digest"]),
                    )
                    differences.append(
                        {
                            "kind": "current_snapshot_recovered",
                            "to_version": latest["version"],
                            "reason": "current pointer was missing",
                        }
                    )
            after = connection.execute(
                "SELECT version, digest FROM current_snapshot WHERE singleton = 1"
            ).fetchone()
            after_version = after["version"] if after else None
            report = {
                "ok": True,
                "started_at": started_at,
                "completed_at": utc_now(),
                "current_version_before": before_version,
                "current_version_after": after_version,
                "snapshot_changed": before_version != after_version,
                "scanned_pending_intents": pending_count,
                "recovered_snapshot_commits": recovered_commits,
                "aborted_plans": aborted_plans,
                "differences": differences,
                "recovery_policy": (
                    "SQLite transactions provide atomic snapshot replacement; pending "
                    "plan intents become E_REQUEST_ABORTED and the last complete snapshot "
                    "stays current."
                ),
            }
            connection.execute(
                "INSERT INTO recovery_reports(started_at, completed_at, report_json) "
                "VALUES (?, ?, ?)",
                (started_at, utc_now(), canonical_json(report)),
            )
        return report

    def latest_recovery_report(self) -> dict[str, Any] | None:
        with self._read() as connection:
            row = connection.execute(
                "SELECT report_json FROM recovery_reports ORDER BY id DESC LIMIT 1"
            ).fetchone()
        return json.loads(row["report_json"]) if row else None

