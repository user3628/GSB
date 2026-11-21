from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from app.domain import (
    assert_acyclic,
    build_diagnostics,
    build_revision_artifacts,
    canonical_json,
    content_hash,
    reverse_dependency_closure,
    state_hash,
    unit_fingerprint,
    validate_dependency_references,
)
from app.errors import DomainError
from app.models import FullSnapshotInput, IncrementalSnapshotInput


SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dataset_revisions (
    version INTEGER PRIMARY KEY,
    commit_id TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS environments (
    name TEXT PRIMARY KEY,
    env_version INTEGER NOT NULL,
    is_baseline INTEGER NOT NULL,
    version_lag_tolerance INTEGER NOT NULL,
    active_revision INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS environment_specs (
    revision INTEGER NOT NULL,
    environment TEXT NOT NULL,
    env_version INTEGER NOT NULL,
    is_baseline INTEGER NOT NULL,
    version_lag_tolerance INTEGER NOT NULL,
    state_hash TEXT NOT NULL,
    unit_count INTEGER NOT NULL,
    PRIMARY KEY (revision, environment)
);

CREATE TABLE IF NOT EXISTS deployment_units (
    revision INTEGER NOT NULL,
    environment TEXT NOT NULL,
    service TEXT NOT NULL,
    image_digest TEXT NOT NULL,
    config_json TEXT NOT NULL,
    dependencies_json TEXT NOT NULL,
    expected_version TEXT NOT NULL,
    row_hash TEXT NOT NULL,
    PRIMARY KEY (revision, environment, service)
);

CREATE TABLE IF NOT EXISTS derived_artifacts (
    revision INTEGER NOT NULL,
    environment TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    artifact_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    PRIMARY KEY (revision, environment, artifact_type, artifact_key)
);

CREATE TABLE IF NOT EXISTS artifact_refs (
    revision INTEGER NOT NULL,
    environment TEXT NOT NULL,
    subject_type TEXT NOT NULL,
    subject_key TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    artifact_key TEXT NOT NULL,
    PRIMARY KEY (
        revision, environment, subject_type, subject_key, artifact_type, artifact_key
    )
);

CREATE TABLE IF NOT EXISTS commits (
    commit_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    request_json TEXT NOT NULL,
    status TEXT NOT NULL,
    revision INTEGER,
    result_json TEXT,
    created_at TEXT NOT NULL,
    finished_at TEXT
);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    commit_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    environment TEXT,
    base_version INTEGER,
    target_version INTEGER,
    status TEXT NOT NULL,
    phase TEXT NOT NULL,
    facts_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    finished_at TEXT,
    error_code TEXT
);

CREATE TABLE IF NOT EXISTS cascade_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    revision INTEGER NOT NULL,
    commit_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    environment TEXT NOT NULL,
    subject_type TEXT NOT NULL,
    subject_key TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    artifact_key TEXT NOT NULL,
    reason TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recovered_at TEXT NOT NULL,
    intent_id TEXT NOT NULL,
    commit_id TEXT NOT NULL,
    status TEXT NOT NULL,
    last_complete_version INTEGER NOT NULL,
    facts_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_units_env ON deployment_units(revision, environment);
CREATE INDEX IF NOT EXISTS idx_artifacts_env
    ON derived_artifacts(revision, environment, artifact_type);
CREATE INDEX IF NOT EXISTS idx_refs_subject
    ON artifact_refs(revision, environment, subject_type, subject_key);
CREATE INDEX IF NOT EXISTS idx_cascade_commit ON cascade_events(commit_id, seq);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class Store:
    """SQLite-backed single-writer, immutable-revision repository."""

    def __init__(self, db_path: str | Path, *, fault_point: str | None = None) -> None:
        self.db_path = Path(db_path)
        self._write_lock = threading.RLock()
        self.fault_point = fault_point

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(SCHEMA)
            conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES('current_revision', '0')"
            )
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _read(self, operation: Callable[[sqlite3.Connection], Any]) -> Any:
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            return operation(conn)
        finally:
            if conn.in_transaction:
                conn.rollback()
            conn.close()

    def _maybe_fault(self, point: str) -> None:
        if self.fault_point == point:
            os._exit(137)

    # ------------------------------------------------------------------
    # Submission
    # ------------------------------------------------------------------
    def submit_full(self, model: FullSnapshotInput) -> dict[str, Any]:
        request = model.model_dump(mode="json")
        request_hash = content_hash(request)

        with self._write_lock:
            existing = self._find_commit(model.commit_id)
            if existing is not None:
                return self._reuse_commit(existing, request_hash)

            environments = {
                environment.name: self._environment_from_model(environment)
                for environment in model.environments
            }
            for environment in environments.values():
                validate_dependency_references(environment)
                assert_acyclic(environment)

            current_version = self.current_version()
            if model.snapshot_version <= current_version:
                raise DomainError(
                    "E_STALE_BASE",
                    "a full snapshot must use a monotonic snapshot version",
                    details={
                        "current_version": current_version,
                        "submitted_version": model.snapshot_version,
                    },
                )

            intent_id = self._create_intent(
                commit_id=model.commit_id,
                kind="full",
                environment=None,
                base_version=current_version,
                target_version=model.snapshot_version,
                facts={"environment_names": sorted(environments)},
            )
            self._maybe_fault("after_intent")

            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._assert_current_revision(conn, current_version)
                revision = model.snapshot_version
                self._insert_revision(
                    conn, revision, model.commit_id, "full", utc_now()
                )
                artifacts = build_revision_artifacts(environments)
                affected = self._full_affected(environments, artifacts)
                result = self._commit_state(
                    conn=conn,
                    commit_id=model.commit_id,
                    kind="full",
                    request_hash=request_hash,
                    request_json=request,
                    revision=revision,
                    environments=environments,
                    artifacts=artifacts,
                    affected=affected,
                    replace_environments=True,
                    intent_id=intent_id,
                )
                self._maybe_fault("mid_rebuild")
                conn.commit()
                return result
            except DomainError:
                conn.rollback()
                self._abort_intent(intent_id, "E_STALE_BASE")
                raise
            except BaseException:
                conn.rollback()
                self._abort_intent(intent_id, "E_INTERNAL")
                raise
            finally:
                conn.close()

    def submit_incremental(self, model: IncrementalSnapshotInput) -> dict[str, Any]:
        request = model.model_dump(mode="json")
        request_hash = content_hash(request)

        with self._write_lock:
            existing = self._find_commit(model.commit_id)
            if existing is not None:
                return self._reuse_commit(existing, request_hash)

            current_version = self.current_version()
            if model.base_version != current_version:
                raise DomainError(
                    "E_STALE_BASE",
                    "incremental fragment base_version is not the current snapshot",
                    details={
                        "current_version": current_version,
                        "base_version": model.base_version,
                    },
                )

            old_environments = self._load_environments_at(current_version)
            if model.environment not in old_environments:
                raise DomainError(
                    "E_ENV_NOT_FOUND",
                    "environment does not exist in the current snapshot",
                    status_code=404,
                    details={"environment": model.environment},
                )

            old_environment = old_environments[model.environment]
            new_environment = json.loads(canonical_json(old_environment))
            new_environment["version"] = old_environment["version"] + 1
            for unit_model in model.units:
                unit = self._unit_from_model(unit_model)
                new_environment["units"][unit["name"]] = unit
            validate_dependency_references(new_environment)
            assert_acyclic(new_environment)

            environments = json.loads(canonical_json(old_environments))
            environments[model.environment] = new_environment

            changed_services, field_changes = self._changed_units(
                old_environment["units"], new_environment["units"], model
            )
            if not changed_services:
                raise DomainError(
                    "E_NO_CHANGES",
                    "incremental fragment does not change any deployment unit",
                    status_code=400,
                    details={"environment": model.environment},
                )
            affected_services = sorted(
                set(
                    reverse_dependency_closure(
                        old_environment["units"], changed_services
                    )
                )
                | set(
                    reverse_dependency_closure(
                        new_environment["units"], changed_services
                    )
                )
            )

            target_version = current_version + 1
            intent_id = self._create_intent(
                commit_id=model.commit_id,
                kind="incremental",
                environment=model.environment,
                base_version=model.base_version,
                target_version=target_version,
                facts={
                    "changed_services": changed_services,
                    "affected_services": affected_services,
                },
            )
            self._maybe_fault("after_intent")

            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._assert_current_revision(conn, model.base_version)
                duplicate = self._find_commit_in_tx(conn, model.commit_id)
                if duplicate is not None:
                    conn.rollback()
                    if duplicate["request_hash"] != request_hash:
                        self._abort_intent(intent_id, "E_CONFLICTING_COMMIT")
                        raise DomainError(
                            "E_CONFLICTING_COMMIT",
                            "commit_id already exists with different content",
                            details={"commit_id": model.commit_id},
                        )
                    self._complete_intent(intent_id, duplicate["revision"])
                    return self._reuse_commit(duplicate, request_hash)

                self._insert_revision(
                    conn, target_version, model.commit_id, "incremental", utc_now()
                )
                artifacts = build_revision_artifacts(environments)
                affected = self._incremental_affected(
                    environment=model.environment,
                    old_units=old_environment["units"],
                    new_units=new_environment["units"],
                    changed_services=changed_services,
                    affected_services=affected_services,
                    field_changes=field_changes,
                    artifacts=artifacts,
                )
                result = self._commit_state(
                    conn=conn,
                    commit_id=model.commit_id,
                    kind="incremental",
                    request_hash=request_hash,
                    request_json=request,
                    revision=target_version,
                    environments=environments,
                    artifacts=artifacts,
                    affected=affected,
                    replace_environments=False,
                    intent_id=intent_id,
                )
                self._maybe_fault("mid_rebuild")
                conn.commit()
                return result
            except DomainError:
                conn.rollback()
                self._abort_intent(intent_id, "E_STALE_BASE")
                raise
            except BaseException:
                conn.rollback()
                self._abort_intent(intent_id, "E_INTERNAL")
                raise
            finally:
                conn.close()

    # ------------------------------------------------------------------
    # Commit state writers
    # ------------------------------------------------------------------
    def _commit_state(
        self,
        *,
        conn: sqlite3.Connection,
        commit_id: str,
        kind: str,
        request_hash: str,
        request_json: dict[str, Any],
        revision: int,
        environments: dict[str, dict[str, Any]],
        artifacts: dict[str, Any],
        affected: dict[str, Any],
        replace_environments: bool,
        intent_id: str,
    ) -> dict[str, Any]:
        self._write_environment_specs(conn, revision, environments)
        self._write_units(conn, revision, environments)
        self._write_current_environments(
            conn, revision, environments, replace_environments
        )
        self._write_artifacts(conn, revision, artifacts)
        cascade = self._write_cascade_events(
            conn, revision, commit_id, affected, artifacts
        )
        conn.execute(
            "UPDATE meta SET value=? WHERE key='current_revision'", (str(revision),)
        )

        result = {
            "commit_id": commit_id,
            "kind": kind,
            "snapshot_version": revision,
            "environments": {
                name: {
                    "environment_version": environment["version"],
                    "service_count": len(environment["units"]),
                    "state_hash": state_hash(environment),
                }
                for name, environment in sorted(environments.items())
            },
            "affected": affected,
            "cascade_trace": cascade,
            "artifacts_rebuilt": len(artifacts["artifacts"]),
            "references_written": len(artifacts["references"]),
            "scores": {
                name: {
                    "score": score["score"],
                    "version_lag": score["version_lag"],
                    "lag_exceeded": score["lag_exceeded"],
                }
                for name, score in sorted(artifacts["scores"].items())
            },
        }
        finished_at = utc_now()
        conn.execute(
            """
            INSERT INTO commits(
                commit_id, kind, request_hash, request_json, status, revision,
                result_json, created_at, finished_at
            ) VALUES(?,?,?,?, 'success', ?,?,?,?)
            """,
            (
                commit_id,
                kind,
                request_hash,
                canonical_json(request_json),
                revision,
                canonical_json(result),
                finished_at,
                finished_at,
            ),
        )
        conn.execute(
            """
            UPDATE intents
               SET status='completed', phase='committed', updated_at=?, finished_at=?
             WHERE intent_id=?
            """,
            (finished_at, finished_at, intent_id),
        )
        return result

    def _insert_revision(
        self, conn: sqlite3.Connection, version: int, commit_id: str, kind: str, now: str
    ) -> None:
        conn.execute(
            "INSERT INTO dataset_revisions(version, commit_id, kind, created_at) VALUES(?,?,?,?)",
            (version, commit_id, kind, now),
        )

    def _write_environment_specs(
        self,
        conn: sqlite3.Connection,
        revision: int,
        environments: dict[str, dict[str, Any]],
    ) -> None:
        conn.executemany(
            """
            INSERT INTO environment_specs(
                revision, environment, env_version, is_baseline,
                version_lag_tolerance, state_hash, unit_count
            ) VALUES(?,?,?,?,?,?,?)
            """,
            [
                (
                    revision,
                    environment["name"],
                    environment["version"],
                    int(environment["is_baseline"]),
                    environment["version_lag_tolerance"],
                    state_hash(environment),
                    len(environment["units"]),
                )
                for _, environment in sorted(environments.items())
            ],
        )

    def _write_units(
        self,
        conn: sqlite3.Connection,
        revision: int,
        environments: dict[str, dict[str, Any]],
    ) -> None:
        rows = []
        for environment_name, environment in sorted(environments.items()):
            for service, unit in sorted(environment["units"].items()):
                rows.append(
                    (
                        revision,
                        environment_name,
                        service,
                        unit["image_digest"],
                        canonical_json(unit["config"]),
                        canonical_json(sorted(unit["dependencies"])),
                        unit["expected_version"],
                        unit_fingerprint(unit),
                    )
                )
        conn.executemany(
            """
            INSERT INTO deployment_units(
                revision, environment, service, image_digest, config_json,
                dependencies_json, expected_version, row_hash
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            rows,
        )

    def _write_current_environments(
        self,
        conn: sqlite3.Connection,
        revision: int,
        environments: dict[str, dict[str, Any]],
        replace_environments: bool,
    ) -> None:
        if replace_environments:
            placeholders = ",".join("?" for _ in environments)
            conn.execute(
                f"DELETE FROM environments WHERE name NOT IN ({placeholders})",
                tuple(sorted(environments)),
            )
        conn.executemany(
            """
            INSERT INTO environments(
                name, env_version, is_baseline, version_lag_tolerance, active_revision
            ) VALUES(?,?,?,?,?)
            ON CONFLICT(name) DO UPDATE SET
                env_version=excluded.env_version,
                is_baseline=excluded.is_baseline,
                version_lag_tolerance=excluded.version_lag_tolerance,
                active_revision=excluded.active_revision
            """,
            [
                (
                    name,
                    environment["version"],
                    int(environment["is_baseline"]),
                    environment["version_lag_tolerance"],
                    revision,
                )
                for name, environment in sorted(environments.items())
            ],
        )

    def _write_artifacts(
        self,
        conn: sqlite3.Connection,
        revision: int,
        artifacts: dict[str, Any],
    ) -> None:
        conn.executemany(
            """
            INSERT INTO derived_artifacts(
                revision, environment, artifact_type, artifact_key,
                payload_json, content_hash
            ) VALUES(?,?,?,?,?,?)
            """,
            [
                (
                    revision,
                    artifact["environment"],
                    artifact["artifact_type"],
                    artifact["artifact_key"],
                    canonical_json(artifact["payload"]),
                    artifact["content_hash"],
                )
                for artifact in artifacts["artifacts"]
            ],
        )
        conn.executemany(
            """
            INSERT INTO artifact_refs(
                revision, environment, subject_type, subject_key,
                artifact_type, artifact_key
            ) VALUES(?,?,?,?,?,?)
            """,
            [
                (
                    revision,
                    reference["environment"],
                    reference["subject_type"],
                    reference["subject_key"],
                    reference["artifact_type"],
                    reference["artifact_key"],
                )
                for reference in artifacts["references"]
            ],
        )

    def _write_cascade_events(
        self,
        conn: sqlite3.Connection,
        revision: int,
        commit_id: str,
        affected: dict[str, Any],
        artifacts: dict[str, Any],
    ) -> list[dict[str, Any]]:
        events = [
            {**event, "seq": index}
            for index, event in enumerate(affected["cascade"], start=1)
        ]
        conn.executemany(
            """
            INSERT INTO cascade_events(
                revision, commit_id, seq, environment, subject_type,
                subject_key, artifact_type, artifact_key, reason
            ) VALUES(?,?,?,?,?,?,?,?,?)
            """,
            [
                (
                    revision,
                    commit_id,
                    event["seq"],
                    event["environment"],
                    event["subject_type"],
                    event["subject_key"],
                    event["artifact_type"],
                    event["artifact_key"],
                    event["reason"],
                )
                for event in events
            ],
        )
        return events

    # ------------------------------------------------------------------
    # Affected-artifact calculations and traces
    # ------------------------------------------------------------------
    def _full_affected(
        self,
        environments: dict[str, dict[str, Any]],
        artifacts: dict[str, Any],
    ) -> dict[str, Any]:
        changed_services = {
            environment: sorted(spec["units"])
            for environment, spec in sorted(environments.items())
        }
        cascade: list[dict[str, Any]] = []
        for environment_name, spec in sorted(environments.items()):
            for service in sorted(spec["units"]):
                cascade.append(
                    self._event(
                        environment_name,
                        "unit",
                        service,
                        "deployment_plan",
                        f"service:{service}",
                        "full_snapshot_replace",
                    )
                )
            cascade.extend(
                [
                    self._event(
                        environment_name,
                        "environment",
                        environment_name,
                        "topology",
                        "environment",
                        "full_snapshot_replace",
                    ),
                    self._event(
                        environment_name,
                        "environment",
                        environment_name,
                        "expected_state",
                        "environment",
                        "full_snapshot_replace",
                    ),
                    self._event(
                        environment_name,
                        "environment",
                        environment_name,
                        "consistency_score",
                        "environment",
                        "cross_environment_rebuild",
                    ),
                ]
            )
        return {
            "mode": "full_replace",
            "changed_services": changed_services,
            "dependent_services": {},
            "affected_services": changed_services,
            "field_changes": {},
            "invalidated_artifacts": self._invalidated_artifacts(cascade),
            "cascade": cascade,
            "artifact_summary": self._artifact_summary(artifacts),
        }

    def _incremental_affected(
        self,
        *,
        environment: str,
        old_units: dict[str, dict[str, Any]],
        new_units: dict[str, dict[str, Any]],
        changed_services: list[str],
        affected_services: list[str],
        field_changes: dict[str, list[str]],
        artifacts: dict[str, Any],
    ) -> dict[str, Any]:
        cascade: list[dict[str, Any]] = []
        dependent_services = sorted(set(affected_services) - set(changed_services))
        for service in changed_services:
            cascade.append(
                self._event(
                    environment,
                    "unit",
                    service,
                    "deployment_plan",
                    f"service:{service}",
                    "direct_change:" + "+".join(field_changes.get(service, [])),
                )
            )
            old_config = old_units.get(service, {}).get("config", {})
            new_config = new_units[service]["config"]
            for key in sorted(set(old_config) | set(new_config)):
                if old_config.get(key) != new_config.get(key):
                    config_subject = f"{service}:{key}"
                    for artifact_type, artifact_key in (
                        ("deployment_plan", f"service:{service}"),
                        ("topology", "environment"),
                        ("consistency_score", "environment"),
                    ):
                        cascade.append(
                            self._event(
                                environment,
                                "config_key",
                                config_subject,
                                artifact_type,
                                artifact_key,
                                "config_key_changed",
                            )
                        )
        for service in dependent_services:
            cascade.append(
                self._event(
                    environment,
                    "unit",
                    service,
                    "deployment_plan",
                    f"service:{service}",
                    "downstream_dependency",
                )
            )
        cascade.extend(
            [
                self._event(
                    environment,
                    "environment",
                    environment,
                    "topology",
                    "environment",
                    "dependency_or_unit_change",
                ),
                self._event(
                    environment,
                    "environment",
                    environment,
                    "expected_state",
                    "environment",
                    "unit_state_change",
                ),
            ]
        )
        for score_environment in sorted(
            artifact["environment"]
            for artifact in artifacts["artifacts"]
            if artifact["artifact_type"] == "consistency_score"
        ):
            cascade.append(
                self._event(
                    score_environment,
                    "environment",
                    score_environment,
                    "consistency_score",
                    "environment",
                    "cross_environment_dependency",
                )
            )
        return {
            "mode": "incremental",
            "environment": environment,
            "changed_services": changed_services,
            "dependent_services": dependent_services,
            "affected_services": affected_services,
            "field_changes": field_changes,
            "invalidated_artifacts": self._invalidated_artifacts(cascade),
            "cascade": cascade,
            "artifact_summary": self._artifact_summary(artifacts),
        }

    @staticmethod
    def _event(
        environment: str,
        subject_type: str,
        subject_key: str,
        artifact_type: str,
        artifact_key: str,
        reason: str,
    ) -> dict[str, str]:
        return {
            "environment": environment,
            "subject_type": subject_type,
            "subject_key": subject_key,
            "artifact_type": artifact_type,
            "artifact_key": artifact_key,
            "reason": reason,
        }

    @staticmethod
    def _invalidated_artifacts(events: list[dict[str, Any]]) -> list[dict[str, str]]:
        seen: set[tuple[str, str, str]] = set()
        values: list[dict[str, str]] = []
        for event in events:
            key = (
                event["environment"],
                event["artifact_type"],
                event["artifact_key"],
            )
            if key in seen:
                continue
            seen.add(key)
            values.append(
                {
                    "environment": event["environment"],
                    "artifact_type": event["artifact_type"],
                    "artifact_key": event["artifact_key"],
                }
            )
        return values

    @staticmethod
    def _artifact_summary(artifacts: dict[str, Any]) -> dict[str, int]:
        summary: dict[str, int] = {}
        for artifact in artifacts["artifacts"]:
            summary[artifact["artifact_type"]] = (
                summary.get(artifact["artifact_type"], 0) + 1
            )
        return dict(sorted(summary.items()))

    # ------------------------------------------------------------------
    # Intent, commit, and recovery helpers
    # ------------------------------------------------------------------
    def _find_commit(self, commit_id: str) -> dict[str, Any] | None:
        def operation(conn: sqlite3.Connection) -> dict[str, Any] | None:
            row = conn.execute(
                "SELECT * FROM commits WHERE commit_id=?", (commit_id,)
            ).fetchone()
            return dict(row) if row else None

        return self._read(operation)

    @staticmethod
    def _find_commit_in_tx(
        conn: sqlite3.Connection, commit_id: str
    ) -> dict[str, Any] | None:
        row = conn.execute(
            "SELECT * FROM commits WHERE commit_id=?", (commit_id,)
        ).fetchone()
        return dict(row) if row else None

    @staticmethod
    def _reuse_commit(existing: dict[str, Any], request_hash: str) -> dict[str, Any]:
        if existing["request_hash"] != request_hash:
            raise DomainError(
                "E_CONFLICTING_COMMIT",
                "commit_id already exists with different content",
                details={"commit_id": existing["commit_id"]},
            )
        result = json.loads(existing["result_json"])
        result["idempotent_replay"] = True
        return result

    def _create_intent(
        self,
        *,
        commit_id: str,
        kind: str,
        environment: str | None,
        base_version: int,
        target_version: int,
        facts: dict[str, Any],
    ) -> str:
        intent_id = uuid.uuid4().hex
        now = utc_now()
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO intents(
                    intent_id, commit_id, kind, environment, base_version,
                    target_version, status, phase, facts_json, created_at, updated_at
                ) VALUES(?,?,?,?,?,?, 'pending', 'prepared', ?,?,?)
                """,
                (
                    intent_id,
                    commit_id,
                    kind,
                    environment,
                    base_version,
                    target_version,
                    canonical_json(facts),
                    now,
                    now,
                ),
            )
        finally:
            conn.close()
        return intent_id

    def _abort_intent(self, intent_id: str, error_code: str) -> None:
        now = utc_now()
        conn = self._connect()
        try:
            conn.execute(
                """
                UPDATE intents
                   SET status='aborted', phase='abandoned', error_code=?,
                       updated_at=?, finished_at=?
                 WHERE intent_id=? AND status='pending'
                """,
                (error_code, now, now, intent_id),
            )
        finally:
            conn.close()

    def _complete_intent(self, intent_id: str, revision: int) -> None:
        now = utc_now()
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT facts_json FROM intents WHERE intent_id=?", (intent_id,)
            ).fetchone()
            facts = json.loads(row["facts_json"]) if row else {}
            facts["completed_revision"] = revision
            conn.execute(
                """
                UPDATE intents
                   SET status='completed', phase='committed', facts_json=?,
                       updated_at=?, finished_at=?
                 WHERE intent_id=? AND status='pending'
                """,
                (canonical_json(facts), now, now, intent_id),
            )
        finally:
            conn.close()

    @staticmethod
    def _assert_current_revision(conn: sqlite3.Connection, expected: int) -> None:
        row = conn.execute(
            "SELECT value FROM meta WHERE key='current_revision'"
        ).fetchone()
        actual = int(row["value"]) if row else 0
        if actual != expected:
            raise DomainError(
                "E_STALE_BASE",
                "snapshot changed before the write transaction acquired the writer lock",
                details={"expected_version": expected, "current_version": actual},
            )

    # ------------------------------------------------------------------
    # Queries (each call is one read transaction over one revision)
    # ------------------------------------------------------------------
    def current_version(self) -> int:
        def operation(conn: sqlite3.Connection) -> int:
            row = conn.execute(
                "SELECT value FROM meta WHERE key='current_revision'"
            ).fetchone()
            return int(row["value"]) if row else 0

        return self._read(operation)

    def _resolve_revision(self, conn: sqlite3.Connection, version: int | None) -> int:
        if version is None:
            row = conn.execute(
                "SELECT value FROM meta WHERE key='current_revision'"
            ).fetchone()
            revision = int(row["value"]) if row else 0
        else:
            revision = version
        row = conn.execute(
            "SELECT version FROM dataset_revisions WHERE version=?", (revision,)
        ).fetchone()
        if row is None:
            raise DomainError(
                "E_VERSION_RETIRED",
                "the bound snapshot version is not retained by this workbench",
                status_code=410,
                details={"requested_version": version},
            )
        return revision

    def list_environments(self, version: int | None = None) -> dict[str, Any]:
        def operation(conn: sqlite3.Connection) -> dict[str, Any]:
            if version is None:
                current = int(
                    conn.execute(
                        "SELECT value FROM meta WHERE key='current_revision'"
                    ).fetchone()["value"]
                )
                if current == 0:
                    return {"snapshot_version": 0, "environments": []}
            revision = self._resolve_revision(conn, version)
            rows = conn.execute(
                """
                SELECT s.environment, s.env_version, s.is_baseline,
                       s.version_lag_tolerance, s.state_hash, s.unit_count,
                       a.payload_json AS score_json
                  FROM environment_specs s
                  LEFT JOIN derived_artifacts a
                    ON a.revision=s.revision AND a.environment=s.environment
                   AND a.artifact_type='consistency_score' AND a.artifact_key='environment'
                 WHERE s.revision=?
                 ORDER BY s.environment
                """,
                (revision,),
            ).fetchall()
            environments = []
            for row in rows:
                item = dict(row)
                item["is_baseline"] = bool(item["is_baseline"])
                score = json.loads(item.pop("score_json")) if item["score_json"] else None
                item["consistency"] = score
                environments.append(item)
            return {"snapshot_version": revision, "environments": environments}

        return self._read(operation)

    def environment_bundle(
        self,
        environment: str,
        *,
        version: int | None = None,
    ) -> dict[str, Any]:
        def operation(conn: sqlite3.Connection) -> dict[str, Any]:
            revision = self._resolve_revision(conn, version)
            spec = conn.execute(
                "SELECT * FROM environment_specs WHERE revision=? AND environment=?",
                (revision, environment),
            ).fetchone()
            if spec is None:
                raise DomainError(
                    "E_ENV_NOT_FOUND",
                    "environment is not present in the bound snapshot",
                    status_code=404,
                    details={"environment": environment, "snapshot_version": revision},
                )
            artifacts = conn.execute(
                """
                SELECT artifact_type, artifact_key, payload_json, content_hash
                  FROM derived_artifacts
                 WHERE revision=? AND environment=?
                 ORDER BY CASE artifact_type
                            WHEN 'deployment_plan' THEN 1
                            WHEN 'topology' THEN 2
                            WHEN 'expected_state' THEN 3
                            ELSE 4 END,
                          artifact_key
                """,
                (revision, environment),
            ).fetchall()
            plan = []
            topology = None
            expected_state = None
            score = None
            artifact_hashes: dict[str, str] = {}
            for row in artifacts:
                payload = json.loads(row["payload_json"])
                artifact_hashes[
                    f"{row['artifact_type']}:{row['artifact_key']}"
                ] = row["content_hash"]
                if row["artifact_type"] == "deployment_plan":
                    plan.append(payload)
                elif row["artifact_type"] == "topology":
                    topology = payload
                elif row["artifact_type"] == "expected_state":
                    expected_state = payload
                elif row["artifact_type"] == "consistency_score":
                    score = payload
            plan.sort(key=lambda item: item["order"])
            return {
                "snapshot_version": revision,
                "environment": environment,
                "environment_version": spec["env_version"],
                "is_baseline": bool(spec["is_baseline"]),
                "version_lag_tolerance": spec["version_lag_tolerance"],
                "state_hash": spec["state_hash"],
                "plan": plan,
                "topology": topology,
                "expected_state": expected_state,
                "consistency": score,
                "artifact_hashes": artifact_hashes,
            }

        return self._read(operation)

    def deployment_plan(
        self,
        environment: str,
        *,
        version: int | None = None,
        service: str | None = None,
    ) -> dict[str, Any]:
        bundle = self.environment_bundle(environment, version=version)
        if service is None:
            items = bundle["plan"]
        else:
            items = [item for item in bundle["plan"] if item["service"] == service]
            if not items:
                raise DomainError(
                    "E_SERVICE_NOT_FOUND",
                    "service is not present in the bound deployment plan",
                    status_code=404,
                    details={
                        "environment": environment,
                        "service": service,
                        "snapshot_version": bundle["snapshot_version"],
                    },
                )
        return {
            "snapshot_version": bundle["snapshot_version"],
            "environment": environment,
            "environment_version": bundle["environment_version"],
            "plan": items,
            "state_hash": bundle["state_hash"],
        }

    def topology(
        self, environment: str, *, version: int | None = None
    ) -> dict[str, Any]:
        bundle = self.environment_bundle(environment, version=version)
        return {
            "snapshot_version": bundle["snapshot_version"],
            "environment": environment,
            "environment_version": bundle["environment_version"],
            "topology": bundle["topology"],
        }

    def application_view(
        self,
        environment: str,
        application: str,
        *,
        version: int | None = None,
    ) -> dict[str, Any]:
        bundle = self.environment_bundle(environment, version=version)
        plan_item = next(
            (item for item in bundle["plan"] if item["service"] == application), None
        )
        if plan_item is None:
            raise DomainError(
                "E_SERVICE_NOT_FOUND",
                "application is not present in the bound environment",
                status_code=404,
                details={
                    "environment": environment,
                    "application": application,
                    "snapshot_version": bundle["snapshot_version"],
                },
            )
        dependencies = sorted(plan_item["dependency_fingerprints"])
        dependents = sorted(
            item["service"]
            for item in bundle["plan"]
            if item["service"] != application
            and application in item["dependency_fingerprints"]
        )
        return {
            "snapshot_version": bundle["snapshot_version"],
            "environment": environment,
            "application": application,
            "environment_version": bundle["environment_version"],
            "plan": plan_item,
            "dependencies": dependencies,
            "dependents": dependents,
            "topology": bundle["topology"],
            "consistency": bundle["consistency"],
        }

    def references(
        self,
        environment: str,
        *,
        version: int | None = None,
        subject: str | None = None,
    ) -> dict[str, Any]:
        def operation(conn: sqlite3.Connection) -> dict[str, Any]:
            revision = self._resolve_revision(conn, version)
            exists = conn.execute(
                "SELECT 1 FROM environment_specs WHERE revision=? AND environment=?",
                (revision, environment),
            ).fetchone()
            if exists is None:
                raise DomainError(
                    "E_ENV_NOT_FOUND",
                    "environment is not present in the bound snapshot",
                    status_code=404,
                    details={"environment": environment, "snapshot_version": revision},
                )
            if subject is None:
                rows = conn.execute(
                    """
                    SELECT subject_type, subject_key, artifact_type, artifact_key
                      FROM artifact_refs
                     WHERE revision=? AND environment=?
                     ORDER BY subject_type, subject_key, artifact_type, artifact_key
                    """,
                    (revision, environment),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT subject_type, subject_key, artifact_type, artifact_key
                      FROM artifact_refs
                     WHERE revision=? AND environment=?
                       AND (subject_key=? OR subject_key LIKE ?)
                     ORDER BY subject_type, subject_key, artifact_type, artifact_key
                    """,
                    (revision, environment, subject, f"{subject}:%"),
                ).fetchall()
            return {
                "snapshot_version": revision,
                "environment": environment,
                "references": [dict(row) for row in rows],
            }

        return self._read(operation)

    def diagnostics(
        self,
        *,
        version: int | None = None,
        baseline: str | None = None,
    ) -> dict[str, Any]:
        def operation(conn: sqlite3.Connection) -> dict[str, Any]:
            if version is None:
                current = int(
                    conn.execute(
                        "SELECT value FROM meta WHERE key='current_revision'"
                    ).fetchone()["value"]
                )
                if current == 0:
                    return {
                        "snapshot_version": 0,
                        "baseline_environment": None,
                        "baseline_version": 0,
                        "items": [],
                        "environment_lag_markers": [],
                        "counts": {
                            "critical": 0,
                            "high": 0,
                            "medium": 0,
                            "low": 0,
                        },
                        "scores": {},
                    }
            revision = self._resolve_revision(conn, version)
            environments = self._load_environments_conn(conn, revision)
            declared_baseline = next(
                name
                for name, environment in environments.items()
                if environment["is_baseline"]
            )
            if baseline is not None and baseline != declared_baseline:
                raise DomainError(
                    "E_SPEC_INVALID",
                    "baseline query must match the snapshot's declared baseline",
                    status_code=400,
                    details={
                        "declared_baseline": declared_baseline,
                        "requested_baseline": baseline,
                    },
                )
            scores = {}
            rows = conn.execute(
                """
                SELECT environment, payload_json FROM derived_artifacts
                 WHERE revision=? AND artifact_type='consistency_score'
                   AND artifact_key='environment'
                """,
                (revision,),
            ).fetchall()
            for row in rows:
                scores[row["environment"]] = json.loads(row["payload_json"])
            diagnostics = build_diagnostics(environments, scores)
            diagnostics["snapshot_version"] = revision
            return diagnostics

        return self._read(operation)

    def commit_record(self, commit_id: str) -> dict[str, Any]:
        record = self._find_commit(commit_id)
        if record is None:
            raise DomainError(
                "E_COMMIT_NOT_FOUND",
                "commit identifier does not exist",
                status_code=404,
                details={"commit_id": commit_id},
            )
        result = dict(record)
        result["request"] = json.loads(result.pop("request_json"))
        result["result"] = json.loads(result.pop("result_json"))
        return result

    def cascade_trace(self, commit_id: str) -> dict[str, Any]:
        def operation(conn: sqlite3.Connection) -> dict[str, Any]:
            commit = conn.execute(
                "SELECT revision FROM commits WHERE commit_id=?", (commit_id,)
            ).fetchone()
            if commit is None:
                raise DomainError(
                    "E_COMMIT_NOT_FOUND",
                    "commit identifier does not exist",
                    status_code=404,
                    details={"commit_id": commit_id},
                )
            rows = conn.execute(
                """
                SELECT seq, environment, subject_type, subject_key,
                       artifact_type, artifact_key, reason
                  FROM cascade_events WHERE commit_id=? ORDER BY seq
                """,
                (commit_id,),
            ).fetchall()
            return {
                "commit_id": commit_id,
                "snapshot_version": commit["revision"],
                "events": [dict(row) for row in rows],
            }

        return self._read(operation)

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------
    def recover_pending_intents(self) -> dict[str, Any]:
        with self._write_lock:
            conn = self._connect()
            recovered: list[dict[str, Any]] = []
            try:
                conn.execute("BEGIN IMMEDIATE")
                current = int(
                    conn.execute(
                        "SELECT value FROM meta WHERE key='current_revision'"
                    ).fetchone()["value"]
                )
                last_complete_row = conn.execute(
                    """
                    SELECT MAX(r.version) AS version
                      FROM dataset_revisions r
                      JOIN commits c ON c.revision=r.version AND c.status='success'
                    """
                ).fetchone()
                last_complete = int(last_complete_row["version"] or 0)
                pending = conn.execute(
                    "SELECT * FROM intents WHERE status='pending' ORDER BY created_at, intent_id"
                ).fetchall()
                for row in pending:
                    facts = json.loads(row["facts_json"])
                    facts.update(
                        {
                            "last_complete_version": last_complete,
                            "facts_source": "sqlite_committed_rows",
                            "recovery_action": "intent_aborted_no_uncommitted_rows_visible",
                            "exit_report_available": False,
                        }
                    )
                    now = utc_now()
                    conn.execute(
                        """
                        UPDATE intents
                           SET status='aborted', phase='recovered', error_code='E_RECOVERED_ABORTED',
                               facts_json=?, updated_at=?, finished_at=?
                         WHERE intent_id=? AND status='pending'
                        """,
                        (canonical_json(facts), now, now, row["intent_id"]),
                    )
                    conn.execute(
                        """
                        INSERT INTO recovery_events(
                            recovered_at, intent_id, commit_id, status,
                            last_complete_version, facts_json
                        ) VALUES(?,?,?,?,?,?)
                        """,
                        (
                            now,
                            row["intent_id"],
                            row["commit_id"],
                            "aborted",
                            last_complete,
                            canonical_json(facts),
                        ),
                    )
                    recovered.append(
                        {
                            "intent_id": row["intent_id"],
                            "commit_id": row["commit_id"],
                            "kind": row["kind"],
                            "environment": row["environment"],
                            "base_version": row["base_version"],
                            "target_version": row["target_version"],
                            "status": "aborted",
                            **facts,
                        }
                    )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()
            return {
                "current_version": current,
                "last_complete_version": last_complete,
                "scanned_pending": len(pending),
                "recovered": recovered,
            }

    def recovery_report(self) -> dict[str, Any]:
        def operation(conn: sqlite3.Connection) -> dict[str, Any]:
            current = int(
                conn.execute(
                    "SELECT value FROM meta WHERE key='current_revision'"
                ).fetchone()["value"]
            )
            pending = conn.execute(
                "SELECT COUNT(*) AS count FROM intents WHERE status='pending'"
            ).fetchone()["count"]
            events = conn.execute(
                """
                SELECT recovered_at, intent_id, commit_id, status,
                       last_complete_version, facts_json
                  FROM recovery_events ORDER BY id DESC
                """
            ).fetchall()
            recovered_events = []
            for row in events:
                item = dict(row)
                item["facts"] = json.loads(item.pop("facts_json"))
                recovered_events.append(item)
            completed = conn.execute(
                """
                SELECT COUNT(*) AS count FROM dataset_revisions r
                  JOIN commits c ON c.revision=r.version AND c.status='success'
                """
            ).fetchone()["count"]
            last_complete_row = conn.execute(
                """
                SELECT MAX(r.version) AS version
                  FROM dataset_revisions r
                  JOIN commits c ON c.revision=r.version AND c.status='success'
                """
            ).fetchone()
            last_complete = int(last_complete_row["version"] or 0)
            return {
                "basis": "sqlite_committed_rows_only",
                "current_version": current,
                "last_complete_version": last_complete,
                "pointer_matches_last_complete": current == last_complete,
                "complete_revisions": completed,
                "pending_intents": pending,
                "recovered_events": recovered_events,
                "exit_report_guaranteed": False,
                "statement": (
                    "A forced termination may prevent any process-local exit report. "
                    "This report is reconstructed only from committed SQLite facts."
                ),
            }

        return self._read(operation)

    # ------------------------------------------------------------------
    # Serialization helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _unit_from_model(unit_model: Any) -> dict[str, Any]:
        unit = unit_model.model_dump(mode="json")
        unit["dependencies"] = sorted(unit["dependencies"])
        return unit

    @classmethod
    def _environment_from_model(cls, model: Any) -> dict[str, Any]:
        units = {unit.name: cls._unit_from_model(unit) for unit in model.units}
        return {
            "name": model.name,
            "version": model.version,
            "is_baseline": model.is_baseline,
            "version_lag_tolerance": model.version_lag_tolerance,
            "units": units,
        }

    def _load_environments_at(self, revision: int) -> dict[str, dict[str, Any]]:
        def operation(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
            return self._load_environments_conn(conn, revision)

        return self._read(operation)

    @staticmethod
    def _load_environments_conn(
        conn: sqlite3.Connection, revision: int
    ) -> dict[str, dict[str, Any]]:
        specs = conn.execute(
            "SELECT * FROM environment_specs WHERE revision=? ORDER BY environment",
            (revision,),
        ).fetchall()
        environments: dict[str, dict[str, Any]] = {}
        for spec in specs:
            unit_rows = conn.execute(
                """
                SELECT service, image_digest, config_json, dependencies_json,
                       expected_version, row_hash
                  FROM deployment_units
                 WHERE revision=? AND environment=? ORDER BY service
                """,
                (revision, spec["environment"]),
            ).fetchall()
            units = {
                row["service"]: {
                    "name": row["service"],
                    "image_digest": row["image_digest"],
                    "config": json.loads(row["config_json"]),
                    "dependencies": json.loads(row["dependencies_json"]),
                    "expected_version": row["expected_version"],
                    "row_hash": row["row_hash"],
                }
                for row in unit_rows
            }
            environments[spec["environment"]] = {
                "name": spec["environment"],
                "version": spec["env_version"],
                "is_baseline": bool(spec["is_baseline"]),
                "version_lag_tolerance": spec["version_lag_tolerance"],
                "state_hash": spec["state_hash"],
                "units": units,
            }
        return environments

    @staticmethod
    def _changed_units(
        old_units: dict[str, dict[str, Any]],
        new_units: dict[str, dict[str, Any]],
        model: IncrementalSnapshotInput,
    ) -> tuple[list[str], dict[str, list[str]]]:
        changed: list[str] = []
        fields: dict[str, list[str]] = {}
        for unit_model in model.units:
            service = unit_model.name
            old = old_units.get(service)
            new = new_units[service]
            if old is None:
                changed.append(service)
                fields[service] = ["created"]
                continue
            differences: list[str] = []
            if old["image_digest"] != new["image_digest"]:
                differences.append("image_digest")
            if old["config"] != new["config"]:
                differences.append("config")
            if set(old["dependencies"]) != set(new["dependencies"]):
                differences.append("dependencies")
            if old["expected_version"] != new["expected_version"]:
                differences.append("expected_version")
            if differences:
                changed.append(service)
                fields[service] = differences
        return sorted(changed), {key: fields[key] for key in sorted(fields)}

    def pending_intents(self) -> list[dict[str, Any]]:
        def operation(conn: sqlite3.Connection) -> list[dict[str, Any]]:
            rows = conn.execute(
                "SELECT * FROM intents WHERE status='pending' ORDER BY created_at"
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["facts"] = json.loads(item.pop("facts_json"))
                result.append(item)
            return result

        return self._read(operation)
