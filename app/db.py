from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .domain import PolicySnapshot, parse_policy
from .errors import AppError, conflicting_round, stale_snapshot
from .state import NodeUsage, Placement, RoundRecord, ServiceStatus, StateDraft, StateSnapshot
from .state_machine import ServiceState
from .units import canonical_json


SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS policies (
    version INTEGER PRIMARY KEY,
    generated_at TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    committed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS policy_services (
    policy_version INTEGER NOT NULL,
    service_id TEXT NOT NULL,
    model TEXT NOT NULL,
    current_replicas INTEGER NOT NULL,
    min_replicas INTEGER NOT NULL,
    max_replicas INTEGER NOT NULL,
    replica_memory_bytes INTEGER NOT NULL,
    replica_compute_milli INTEGER NOT NULL,
    target_concurrency INTEGER NOT NULL,
    latency_budget_ms INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    max_replicas_per_fault_domain INTEGER NOT NULL,
    PRIMARY KEY (policy_version, service_id)
);

CREATE TABLE IF NOT EXISTS policy_nodes (
    policy_version INTEGER NOT NULL,
    node_id TEXT NOT NULL,
    fault_domain TEXT NOT NULL,
    memory_bytes INTEGER NOT NULL,
    compute_milli INTEGER NOT NULL,
    healthy INTEGER NOT NULL,
    reserved_memory_bytes INTEGER NOT NULL,
    reserved_compute_milli INTEGER NOT NULL,
    PRIMARY KEY (policy_version, node_id)
);

CREATE TABLE IF NOT EXISTS policy_initial_placements (
    policy_version INTEGER NOT NULL,
    service_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    count INTEGER NOT NULL,
    PRIMARY KEY (policy_version, service_id, node_id)
);

CREATE TABLE IF NOT EXISTS revisions (
    revision INTEGER PRIMARY KEY,
    policy_version INTEGER NOT NULL,
    kind TEXT NOT NULL,
    parent_revision INTEGER,
    round_id TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS placements (
    revision INTEGER NOT NULL,
    replica_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    memory_bytes INTEGER NOT NULL,
    compute_milli INTEGER NOT NULL,
    PRIMARY KEY (revision, replica_id)
);
CREATE INDEX IF NOT EXISTS idx_placements_revision_service
    ON placements(revision, service_id);
CREATE INDEX IF NOT EXISTS idx_placements_revision_node
    ON placements(revision, node_id);

CREATE TABLE IF NOT EXISTS node_states (
    revision INTEGER NOT NULL,
    node_id TEXT NOT NULL,
    healthy INTEGER NOT NULL,
    used_memory_bytes INTEGER NOT NULL,
    used_compute_milli INTEGER NOT NULL,
    PRIMARY KEY (revision, node_id)
);

CREATE TABLE IF NOT EXISTS service_states (
    revision INTEGER NOT NULL,
    service_id TEXT NOT NULL,
    state TEXT NOT NULL,
    current_replicas INTEGER NOT NULL,
    desired_replicas INTEGER NOT NULL,
    reason TEXT NOT NULL,
    degraded_json TEXT NOT NULL,
    PRIMARY KEY (revision, service_id)
);

CREATE TABLE IF NOT EXISTS rounds (
    round_id TEXT PRIMARY KEY,
    policy_version INTEGER NOT NULL,
    request_hash TEXT NOT NULL,
    request_json TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    result_json TEXT,
    error_json TEXT,
    revision INTEGER
);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    round_id TEXT,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);

CREATE TABLE IF NOT EXISTS recovery_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id TEXT,
    intent_id TEXT,
    detected_at TEXT NOT NULL,
    action TEXT NOT NULL,
    detail_json TEXT NOT NULL
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class Database:
    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def connect(self, *, query_only: bool = False) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        if query_only:
            connection.execute("PRAGMA query_only = ON")
        return connection

    def initialize(self) -> None:
        connection = sqlite3.connect(self.path, timeout=30.0)
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(SCHEMA)
            connection.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', '1')"
            )
            connection.commit()
        finally:
            connection.close()

    @staticmethod
    def _meta_int(connection: sqlite3.Connection, key: str, default: int = 0) -> int:
        row = connection.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return int(row["value"]) if row is not None else default

    @staticmethod
    def _set_meta(connection: sqlite3.Connection, key: str, value: int | str) -> None:
        connection.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )

    def current_policy_version(self) -> int:
        connection = self.connect(query_only=True)
        try:
            return self._meta_int(connection, "latest_policy_version")
        finally:
            connection.close()

    def latest_revision(self) -> int:
        connection = self.connect(query_only=True)
        try:
            return self._meta_int(connection, "latest_revision")
        finally:
            connection.close()

    def get_policy(self, version: int | None = None) -> PolicySnapshot | None:
        connection = self.connect(query_only=True)
        try:
            if version is None:
                version = self._meta_int(connection, "latest_policy_version")
                if version == 0:
                    return None
            row = connection.execute(
                "SELECT snapshot_json FROM policies WHERE version = ?", (version,)
            ).fetchone()
            if row is None:
                return None
            return parse_policy(json.loads(row["snapshot_json"]))
        finally:
            connection.close()

    def get_policy_row(self, version: int | None = None) -> dict[str, Any] | None:
        connection = self.connect(query_only=True)
        try:
            if version is None:
                version = self._meta_int(connection, "latest_policy_version")
                if version == 0:
                    return None
            row = connection.execute(
                "SELECT version, generated_at, snapshot_json, committed_at "
                "FROM policies WHERE version = ?",
                (version,),
            ).fetchone()
            if row is None:
                return None
            return {
                "version": row["version"],
                "generated_at": row["generated_at"],
                "snapshot": json.loads(row["snapshot_json"]),
                "committed_at": row["committed_at"],
            }
        finally:
            connection.close()

    def list_policy_versions(self) -> list[dict[str, Any]]:
        connection = self.connect(query_only=True)
        try:
            rows = connection.execute(
                "SELECT version, generated_at, committed_at FROM policies ORDER BY version"
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            connection.close()

    def begin_policy_intent(self, version: int, policy_hash: str) -> str:
        intent_id = f"policy:{version}:{uuid.uuid4().hex}"
        now = utc_now()
        connection = self.connect()
        try:
            connection.execute(
                "INSERT INTO intents(intent_id, round_id, kind, status, created_at, "
                "updated_at, payload_json) VALUES(?, NULL, 'POLICY_COMMIT', 'PENDING', ?, ?, ?)",
                (intent_id, now, now, canonical_json({"version": version, "hash": policy_hash})),
            )
            connection.commit()
            return intent_id
        finally:
            connection.close()

    def abort_intent(self, intent_id: str, reason: str) -> None:
        now = utc_now()
        connection = self.connect()
        try:
            connection.execute(
                "UPDATE intents SET status='ABORTED', updated_at=?, "
                "payload_json=json_set(payload_json, '$.abort_reason', ?) WHERE intent_id=?",
                (now, reason, intent_id),
            )
            connection.commit()
        finally:
            connection.close()

    def commit_policy(
        self,
        policy: PolicySnapshot,
        raw_spec: dict[str, Any],
        draft: StateDraft,
        intent_id: str,
    ) -> int:
        now = utc_now()
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            current_version = self._meta_int(connection, "latest_policy_version")
            if policy.version <= current_version:
                raise stale_snapshot(
                    "policy version must be strictly greater than the current version",
                    submitted_version=policy.version,
                    current_version=current_version,
                )
            base_revision = self._meta_int(connection, "latest_revision")
            revision = base_revision + 1
            snapshot_json = canonical_json(raw_spec)
            connection.execute(
                "INSERT INTO policies(version, generated_at, snapshot_json, committed_at) "
                "VALUES(?, ?, ?, ?)",
                (policy.version, policy.generated_at, snapshot_json, now),
            )
            self._insert_policy_rows(connection, policy)
            connection.execute(
                "INSERT INTO revisions(revision, policy_version, kind, parent_revision, "
                "round_id, created_at) VALUES(?, ?, 'POLICY', ?, NULL, ?)",
                (revision, policy.version, base_revision or None, now),
            )
            self._insert_state(connection, revision, draft)
            self._set_meta(connection, "latest_policy_version", policy.version)
            self._set_meta(connection, "latest_revision", revision)
            connection.execute(
                "UPDATE intents SET status='COMMITTED', updated_at=? WHERE intent_id=?",
                (now, intent_id),
            )
            connection.commit()
            return revision
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _insert_policy_rows(connection: sqlite3.Connection, policy: PolicySnapshot) -> None:
        for service in policy.services:
            connection.execute(
                "INSERT INTO policy_services(policy_version, service_id, model, "
                "current_replicas, min_replicas, max_replicas, replica_memory_bytes, "
                "replica_compute_milli, target_concurrency, latency_budget_ms, priority, "
                "max_replicas_per_fault_domain) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    policy.version,
                    service.id,
                    service.model,
                    service.current_replicas,
                    service.min_replicas,
                    service.max_replicas,
                    service.replica_memory_bytes,
                    service.replica_compute_milli,
                    service.target_concurrency,
                    service.latency_budget_ms,
                    service.priority,
                    service.max_replicas_per_fault_domain,
                ),
            )
        for node in policy.nodes:
            connection.execute(
                "INSERT INTO policy_nodes(policy_version, node_id, fault_domain, "
                "memory_bytes, compute_milli, healthy, reserved_memory_bytes, "
                "reserved_compute_milli) VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    policy.version,
                    node.id,
                    node.fault_domain,
                    node.memory_bytes,
                    node.compute_milli,
                    int(node.healthy),
                    node.reserved_memory_bytes,
                    node.reserved_compute_milli,
                ),
            )
        for placement in policy.initial_placements:
            connection.execute(
                "INSERT INTO policy_initial_placements(policy_version, service_id, "
                "node_id, count) VALUES(?, ?, ?, ?)",
                (
                    policy.version,
                    placement.service_id,
                    placement.node_id,
                    placement.count,
                ),
            )

    @staticmethod
    def _insert_state(
        connection: sqlite3.Connection, revision: int, draft: StateDraft
    ) -> None:
        for placement in draft.placements:
            connection.execute(
                "INSERT INTO placements(revision, replica_id, service_id, node_id, "
                "memory_bytes, compute_milli) VALUES(?, ?, ?, ?, ?, ?)",
                (
                    revision,
                    placement.replica_id,
                    placement.service_id,
                    placement.node_id,
                    placement.memory_bytes,
                    placement.compute_milli,
                ),
            )
        for node in draft.nodes:
            connection.execute(
                "INSERT INTO node_states(revision, node_id, healthy, used_memory_bytes, "
                "used_compute_milli) VALUES(?, ?, ?, ?, ?)",
                (
                    revision,
                    node.node_id,
                    int(node.healthy),
                    node.used_memory_bytes,
                    node.used_compute_milli,
                ),
            )
        for service in draft.services:
            connection.execute(
                "INSERT INTO service_states(revision, service_id, state, current_replicas, "
                "desired_replicas, reason, degraded_json) VALUES(?, ?, ?, ?, ?, ?, ?)",
                (
                    revision,
                    service.service_id,
                    service.state.value,
                    service.current_replicas,
                    service.desired_replicas,
                    service.reason,
                    canonical_json(list(service.degraded_domains)),
                ),
            )

    def start_round(
        self,
        round_id: str,
        policy_version: int,
        request_hash: str,
        request_json: str,
        base_revision: int,
    ) -> str:
        intent_id = f"round:{round_id}:{uuid.uuid4().hex}"
        now = utc_now()
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    "INSERT INTO rounds(round_id, policy_version, request_hash, request_json, "
                    "status, started_at) VALUES(?, ?, ?, ?, 'RUNNING', ?)",
                    (round_id, policy_version, request_hash, request_json, now),
                )
            except sqlite3.IntegrityError as exc:
                raise conflicting_round(round_id, reason="concurrent round id insertion") from exc
            connection.execute(
                "INSERT INTO intents(intent_id, round_id, kind, status, created_at, "
                "updated_at, payload_json) VALUES(?, ?, 'ROUND_COMMIT', 'PENDING', ?, ?, ?)",
                (
                    intent_id,
                    round_id,
                    now,
                    now,
                    canonical_json(
                        {
                            "round_id": round_id,
                            "policy_version": policy_version,
                            "base_revision": base_revision,
                            "request_hash": request_hash,
                        }
                    ),
                ),
            )
            connection.commit()
            return intent_id
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def commit_manual_state(
        self,
        policy: PolicySnapshot,
        draft: StateDraft,
        description: str,
        payload: dict[str, Any],
    ) -> int:
        intent_id = f"manual:{uuid.uuid4().hex}"
        now = utc_now()
        connection = self.connect()
        try:
            connection.execute(
                "INSERT INTO intents(intent_id, round_id, kind, status, created_at, "
                "updated_at, payload_json) VALUES(?, NULL, 'MANUAL_STATE', 'PENDING', ?, ?, ?)",
                (intent_id, now, now, canonical_json(payload)),
            )
            connection.commit()
            connection.execute("BEGIN IMMEDIATE")
            base_revision = self._meta_int(connection, "latest_revision")
            revision = base_revision + 1
            connection.execute(
                "INSERT INTO revisions(revision, policy_version, kind, parent_revision, "
                "round_id, created_at) VALUES(?, ?, 'MANUAL', ?, NULL, ?)",
                (revision, policy.version, base_revision or None, now),
            )
            self._insert_state(connection, revision, draft)
            connection.execute(
                "UPDATE intents SET status='COMMITTED', updated_at=?, "
                "payload_json=json_set(payload_json, '$.description', ?) WHERE intent_id=?",
                (now, description, intent_id),
            )
            self._set_meta(connection, "latest_revision", revision)
            connection.commit()
            return revision
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def commit_round(
        self,
        round_id: str,
        intent_id: str,
        policy: PolicySnapshot,
        draft: StateDraft,
        result: dict[str, Any],
        base_revision: int,
    ) -> tuple[int, dict[str, Any]]:
        now = utc_now()
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            latest = self._meta_int(connection, "latest_revision")
            if latest != base_revision:
                raise AppError(
                    "E_CONFLICTING_ROUND",
                    "the committed revision changed while the round was being planned",
                    {"base_revision": base_revision, "latest_revision": latest},
                    409,
                )
            revision = latest + 1
            connection.execute(
                "INSERT INTO revisions(revision, policy_version, kind, parent_revision, "
                "round_id, created_at) VALUES(?, ?, 'ROUND', ?, ?, ?)",
                (revision, policy.version, base_revision, round_id, now),
            )
            self._insert_state(connection, revision, draft)
            stored_result = dict(result)
            stored_result["revision"] = revision
            stored_result["completed_at"] = now
            connection.execute(
                "UPDATE rounds SET status='COMMITTED', completed_at=?, result_json=?, "
                "error_json=NULL, revision=? WHERE round_id=?",
                (now, canonical_json(stored_result), revision, round_id),
            )
            connection.execute(
                "UPDATE intents SET status='COMMITTED', updated_at=? WHERE intent_id=?",
                (now, intent_id),
            )
            self._set_meta(connection, "latest_revision", revision)
            connection.commit()
            return revision, stored_result
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def abort_round(
        self, round_id: str, intent_id: str, error: dict[str, Any]
    ) -> None:
        now = utc_now()
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "UPDATE rounds SET status='ABORTED', completed_at=?, error_json=? "
                "WHERE round_id=? AND status='RUNNING'",
                (now, canonical_json(error), round_id),
            )
            connection.execute(
                "UPDATE intents SET status='ABORTED', updated_at=? WHERE intent_id=?",
                (now, intent_id),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def get_round(self, round_id: str) -> RoundRecord | None:
        connection = self.connect(query_only=True)
        try:
            row = connection.execute(
                "SELECT round_id, policy_version, request_hash, status, started_at, "
                "completed_at, result_json, error_json, revision FROM rounds "
                "WHERE round_id=?",
                (round_id,),
            ).fetchone()
            if row is None:
                return None
            return RoundRecord(
                round_id=row["round_id"],
                policy_version=row["policy_version"],
                request_hash=row["request_hash"],
                status=row["status"],
                started_at=row["started_at"],
                completed_at=row["completed_at"],
                result=json.loads(row["result_json"]) if row["result_json"] else None,
                error=json.loads(row["error_json"]) if row["error_json"] else None,
                revision=row["revision"],
            )
        finally:
            connection.close()

    def list_rounds(self, limit: int = 50) -> list[dict[str, Any]]:
        connection = self.connect(query_only=True)
        try:
            rows = connection.execute(
                "SELECT round_id, policy_version, status, started_at, completed_at, revision "
                "FROM rounds ORDER BY started_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            connection.close()

    def latest_state(self) -> StateSnapshot | None:
        revision = self.latest_revision()
        if revision == 0:
            return None
        return self.get_state(revision)

    def get_state(self, revision: int | None = None) -> StateSnapshot | None:
        connection = self.connect(query_only=True)
        try:
            connection.execute("BEGIN")
            if revision is None:
                revision = self._meta_int(connection, "latest_revision")
                if revision == 0:
                    connection.rollback()
                    return None
            revision_row = connection.execute(
                "SELECT revision, policy_version, kind, parent_revision, round_id, created_at "
                "FROM revisions WHERE revision=?",
                (revision,),
            ).fetchone()
            if revision_row is None:
                connection.rollback()
                return None
            placement_rows = connection.execute(
                "SELECT replica_id, service_id, node_id, memory_bytes, compute_milli "
                "FROM placements WHERE revision=? ORDER BY replica_id",
                (revision,),
            ).fetchall()
            node_rows = connection.execute(
                "SELECT node_id, healthy, used_memory_bytes, used_compute_milli "
                "FROM node_states WHERE revision=? ORDER BY node_id",
                (revision,),
            ).fetchall()
            service_rows = connection.execute(
                "SELECT service_id, state, current_replicas, desired_replicas, reason, "
                "degraded_json FROM service_states WHERE revision=? ORDER BY service_id",
                (revision,),
            ).fetchall()
            snapshot = StateSnapshot(
                revision=revision_row["revision"],
                policy_version=revision_row["policy_version"],
                kind=revision_row["kind"],
                parent_revision=revision_row["parent_revision"],
                round_id=revision_row["round_id"],
                created_at=revision_row["created_at"],
                placements=tuple(
                    Placement(
                        replica_id=row["replica_id"],
                        service_id=row["service_id"],
                        node_id=row["node_id"],
                        memory_bytes=row["memory_bytes"],
                        compute_milli=row["compute_milli"],
                    )
                    for row in placement_rows
                ),
                nodes=tuple(
                    NodeUsage(
                        node_id=row["node_id"],
                        healthy=bool(row["healthy"]),
                        used_memory_bytes=row["used_memory_bytes"],
                        used_compute_milli=row["used_compute_milli"],
                    )
                    for row in node_rows
                ),
                services=tuple(
                    ServiceStatus(
                        service_id=row["service_id"],
                        state=ServiceState(row["state"]),
                        current_replicas=row["current_replicas"],
                        desired_replicas=row["desired_replicas"],
                        reason=row["reason"],
                        degraded_domains=tuple(json.loads(row["degraded_json"])),
                    )
                    for row in service_rows
                ),
            )
            connection.commit()
            return snapshot
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def list_revisions(self, limit: int = 100) -> list[dict[str, Any]]:
        connection = self.connect(query_only=True)
        try:
            rows = connection.execute(
                "SELECT revision, policy_version, kind, parent_revision, round_id, created_at "
                "FROM revisions ORDER BY revision DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            connection.close()

    def recover_incomplete(self) -> list[dict[str, Any]]:
        """Mark persisted, unfinished intents/rounds as aborted.

        State itself cannot be partially visible because every revision is written
        in one SQLite transaction.  The report therefore names the last committed
        revision discovered from persistent facts, never an in-memory promise.
        """

        now = utc_now()
        connection = self.connect()
        reports: list[dict[str, Any]] = []
        try:
            connection.execute("BEGIN IMMEDIATE")
            latest_revision = self._meta_int(connection, "latest_revision")
            running_rounds = connection.execute(
                "SELECT r.round_id, r.policy_version, i.intent_id FROM rounds r "
                "LEFT JOIN intents i ON i.round_id = r.round_id AND i.status = 'PENDING' "
                "WHERE r.status = 'RUNNING' ORDER BY r.started_at, r.round_id"
            ).fetchall()
            handled_intents: set[str] = set()
            for row in running_rounds:
                intent_id = row["intent_id"]
                error = {
                    "error": {
                        "code": "E_ROUND_ABORTED",
                        "message": "unfinished round was marked aborted during startup recovery",
                        "details": {
                            "round_id": row["round_id"],
                            "last_committed_revision": latest_revision,
                        },
                    }
                }
                connection.execute(
                    "UPDATE rounds SET status='ABORTED', completed_at=?, error_json=? "
                    "WHERE round_id=? AND status='RUNNING'",
                    (now, canonical_json(error), row["round_id"]),
                )
                if intent_id:
                    connection.execute(
                        "UPDATE intents SET status='ABORTED', updated_at=? WHERE intent_id=?",
                        (now, intent_id),
                    )
                    handled_intents.add(intent_id)
                detail = {
                    "round_id": row["round_id"],
                    "intent_id": intent_id,
                    "policy_version": row["policy_version"],
                    "last_committed_revision": latest_revision,
                    "partial_revision_visible": False,
                }
                connection.execute(
                    "INSERT INTO recovery_reports(round_id, intent_id, detected_at, action, "
                    "detail_json) VALUES(?, ?, ?, 'ROUND_MARKED_ABORTED', ?)",
                    (row["round_id"], intent_id, now, canonical_json(detail)),
                )
                reports.append(
                    {
                        "round_id": row["round_id"],
                        "intent_id": intent_id,
                        "action": "ROUND_MARKED_ABORTED",
                        "detected_at": now,
                        "detail": detail,
                    }
                )

            orphan_intents = connection.execute(
                "SELECT intent_id, round_id, kind FROM intents WHERE status='PENDING' "
                "ORDER BY created_at, intent_id"
            ).fetchall()
            for row in orphan_intents:
                if row["intent_id"] in handled_intents:
                    continue
                connection.execute(
                    "UPDATE intents SET status='ABORTED', updated_at=? WHERE intent_id=?",
                    (now, row["intent_id"]),
                )
                detail = {
                    "round_id": row["round_id"],
                    "intent_id": row["intent_id"],
                    "last_committed_revision": latest_revision,
                    "partial_revision_visible": False,
                }
                connection.execute(
                    "INSERT INTO recovery_reports(round_id, intent_id, detected_at, action, "
                    "detail_json) VALUES(?, ?, ?, 'INTENT_MARKED_ABORTED', ?)",
                    (row["round_id"], row["intent_id"], now, canonical_json(detail)),
                )
                reports.append(
                    {
                        "round_id": row["round_id"],
                        "intent_id": row["intent_id"],
                        "action": "INTENT_MARKED_ABORTED",
                        "detected_at": now,
                        "detail": detail,
                    }
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return reports

    def recovery_report(self, limit: int = 100) -> dict[str, Any]:
        connection = self.connect(query_only=True)
        try:
            rows = connection.execute(
                "SELECT id, round_id, intent_id, detected_at, action, detail_json "
                "FROM recovery_reports ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            items = [
                {
                    "id": row["id"],
                    "round_id": row["round_id"],
                    "intent_id": row["intent_id"],
                    "detected_at": row["detected_at"],
                    "action": row["action"],
                    "detail": json.loads(row["detail_json"]),
                }
                for row in rows
            ]
            return {
                "source": "sqlite_persisted_facts",
                "latest_committed_revision": self._meta_int(connection, "latest_revision"),
                "items": items,
                "note": (
                    "A forced termination cannot produce a clean process-exit report; "
                    "this report is reconstructed only from committed SQLite facts."
                ),
            }
        finally:
            connection.close()

    def pending_intent_count(self) -> int:
        connection = self.connect(query_only=True)
        try:
            row = connection.execute(
                "SELECT COUNT(*) AS n FROM intents WHERE status='PENDING'"
            ).fetchone()
            return int(row["n"])
        finally:
            connection.close()

    def state_summary_for_revision(self, revision: int) -> dict[str, Any] | None:
        snapshot = self.get_state(revision)
        if snapshot is None:
            return None
        return {
            "revision": snapshot.revision,
            "policy_version": snapshot.policy_version,
            "kind": snapshot.kind,
            "round_id": snapshot.round_id,
            "created_at": snapshot.created_at,
            "placements": [item.as_dict() for item in snapshot.placements],
            "nodes": [item.as_dict() for item in snapshot.nodes],
            "services": [item.as_dict() for item in snapshot.services],
        }
