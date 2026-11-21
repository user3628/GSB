from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .errors import BusinessError
from .models import MirrorRecord, canonical_json, record_from_stored_dict


SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS commits (
    commit_version INTEGER PRIMARY KEY,
    parent_version INTEGER,
    kind TEXT NOT NULL,
    snapshot_version TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    snapshot_hash TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    selected_count INTEGER NOT NULL,
    batch_count INTEGER NOT NULL,
    cascade_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS policies (
    commit_version INTEGER PRIMARY KEY,
    policy_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS records (
    batch_id TEXT NOT NULL,
    line_no INTEGER NOT NULL,
    record_key TEXT NOT NULL UNIQUE,
    content_json TEXT NOT NULL,
    PRIMARY KEY (batch_id, line_no)
);
CREATE TABLE IF NOT EXISTS commit_records (
    commit_version INTEGER NOT NULL,
    batch_id TEXT NOT NULL,
    line_no INTEGER NOT NULL,
    record_key TEXT NOT NULL,
    PRIMARY KEY (commit_version, batch_id, line_no)
);
CREATE INDEX IF NOT EXISTS idx_commit_records_key ON commit_records(record_key);
CREATE TABLE IF NOT EXISTS artifacts (
    commit_version INTEGER NOT NULL,
    artifact_type TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    content_json TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    PRIMARY KEY (commit_version, artifact_type)
);
CREATE TABLE IF NOT EXISTS artifact_dependencies (
    commit_version INTEGER NOT NULL,
    artifact_type TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    rule_keys_json TEXT NOT NULL,
    PRIMARY KEY (commit_version, artifact_type)
);
CREATE TABLE IF NOT EXISTS batch_states (
    commit_version INTEGER NOT NULL,
    batch_id TEXT NOT NULL,
    state TEXT NOT NULL,
    run_id TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (commit_version, batch_id)
);
CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    base_version INTEGER,
    status TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    result_version INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS replay_runs (
    run_id TEXT PRIMARY KEY,
    source_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    batch_ids_json TEXT NOT NULL,
    intent_id TEXT,
    result_version INTEGER,
    metrics_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    report_json TEXT NOT NULL
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class Store:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._writer_lock = threading.RLock()
        self.initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def initialize(self) -> None:
        with self._writer_lock:
            connection = self.connect()
            try:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("PRAGMA synchronous=FULL")
                connection.executescript(SCHEMA)
            finally:
                connection.close()

    def current_version(self) -> int | None:
        connection = self.connect()
        try:
            row = connection.execute("SELECT value FROM meta WHERE key='current_version'").fetchone()
            return int(row["value"]) if row else None
        finally:
            connection.close()

    def _current_version_tx(self, connection: sqlite3.Connection) -> int | None:
        row = connection.execute("SELECT value FROM meta WHERE key='current_version'").fetchone()
        return int(row["value"]) if row else None

    def _next_version_tx(self, connection: sqlite3.Connection) -> int:
        row = connection.execute("SELECT COALESCE(MAX(commit_version), 0) AS version FROM commits").fetchone()
        return int(row["version"]) + 1

    def create_intent(self, kind: str, base_version: int | None, detail: dict[str, Any]) -> str:
        intent_id = f"intent-{uuid.uuid4().hex}"
        now = utc_now()
        connection = self.connect()
        try:
            connection.execute(
                "INSERT INTO intents(intent_id,kind,base_version,status,detail_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                (intent_id, kind, base_version, "pending", canonical_json(detail), now, now),
            )
        finally:
            connection.close()
        return intent_id

    def complete_intent(self, connection: sqlite3.Connection, intent_id: str, result_version: int) -> None:
        connection.execute(
            "UPDATE intents SET status='committed', result_version=?, updated_at=? WHERE intent_id=? AND status='pending'",
            (result_version, utc_now(), intent_id),
        )

    def abort_intent(self, intent_id: str, error: str | None = None) -> None:
        connection = self.connect()
        try:
            row = connection.execute("SELECT detail_json FROM intents WHERE intent_id=?", (intent_id,)).fetchone()
            detail = json.loads(row["detail_json"]) if row else {}
            if error:
                detail["error"] = error
            connection.execute(
                "UPDATE intents SET status='aborted', detail_json=?, updated_at=? WHERE intent_id=? AND status='pending'",
                (canonical_json(detail), utc_now(), intent_id),
            )
        finally:
            connection.close()

    @contextmanager
    def writer(self) -> Iterator[None]:
        with self._writer_lock:
            yield

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def read_snapshot(self, version: int | None = None) -> dict[str, Any] | None:
        connection = self.connect()
        try:
            connection.execute("BEGIN")
            resolved = version
            if resolved is None:
                resolved = self._current_version_tx(connection)
                if resolved is None:
                    connection.commit()
                    return None
            commit_row = connection.execute("SELECT * FROM commits WHERE commit_version=?", (resolved,)).fetchone()
            if commit_row is None:
                raise BusinessError("E_VERSION_NOT_FOUND", "Committed version was not found", {"version": resolved})
            policy_row = connection.execute("SELECT policy_json FROM policies WHERE commit_version=?", (resolved,)).fetchone()
            if policy_row is None:
                raise BusinessError("E_INTERNAL", "Committed version has no policy snapshot", {"version": resolved})
            record_rows = connection.execute(
                """
                SELECT r.content_json
                FROM commit_records cr JOIN records r ON r.record_key=cr.record_key
                WHERE cr.commit_version=?
                ORDER BY r.batch_id, r.line_no
                """,
                (resolved,),
            ).fetchall()
            artifact_rows = connection.execute(
                "SELECT artifact_type,artifact_id,content_json,content_hash FROM artifacts WHERE commit_version=?",
                (resolved,),
            ).fetchall()
            dependency_rows = connection.execute(
                "SELECT artifact_type,artifact_id,rule_keys_json FROM artifact_dependencies WHERE commit_version=?",
                (resolved,),
            ).fetchall()
            state_rows = connection.execute(
                "SELECT batch_id,state,run_id,updated_at FROM batch_states WHERE commit_version=?",
                (resolved,),
            ).fetchall()
            connection.commit()
            artifacts = {row["artifact_type"]: json.loads(row["content_json"]) for row in artifact_rows}
            dependencies = {
                row["artifact_type"]: {
                    "artifact_id": row["artifact_id"],
                    "rule_keys": json.loads(row["rule_keys_json"]),
                }
                for row in dependency_rows
            }
            records = [record_from_stored_dict(json.loads(row["content_json"])) for row in record_rows]
            return {
                "commit": {
                    "version": int(commit_row["commit_version"]),
                    "parent_version": commit_row["parent_version"],
                    "kind": commit_row["kind"],
                    "snapshot_version": commit_row["snapshot_version"],
                    "policy_version": commit_row["policy_version"],
                    "snapshot_hash": commit_row["snapshot_hash"],
                    "record_count": commit_row["record_count"],
                    "selected_count": commit_row["selected_count"],
                    "batch_count": commit_row["batch_count"],
                    "cascade": json.loads(commit_row["cascade_json"]),
                    "created_at": commit_row["created_at"],
                },
                "policy": json.loads(policy_row["policy_json"]),
                "records": records,
                "artifacts": artifacts,
                "dependencies": dependencies,
                "batch_states": {row["batch_id"]: dict(row) for row in state_rows},
            }
        finally:
            connection.close()

    def list_commits(self) -> list[dict[str, Any]]:
        connection = self.connect()
        try:
            rows = connection.execute(
                "SELECT commit_version,parent_version,kind,snapshot_version,policy_version,snapshot_hash,record_count,selected_count,batch_count,created_at FROM commits ORDER BY commit_version"
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            connection.close()

    def get_records_for_batch(self, batch_id: str) -> list[MirrorRecord]:
        connection = self.connect()
        try:
            rows = connection.execute(
                "SELECT content_json FROM records WHERE batch_id=? ORDER BY line_no", (batch_id,)
            ).fetchall()
            return [record_from_stored_dict(json.loads(row["content_json"])) for row in rows]
        finally:
            connection.close()

    def existing_record_map(self, batch_id: str) -> dict[int, dict[str, Any]]:
        connection = self.connect()
        try:
            rows = connection.execute(
                "SELECT line_no,content_json FROM records WHERE batch_id=? ORDER BY line_no", (batch_id,)
            ).fetchall()
            return {int(row["line_no"]): json.loads(row["content_json"]) for row in rows}
        finally:
            connection.close()

    def record_with_key(self, record_key: str) -> dict[str, Any] | None:
        connection = self.connect()
        try:
            row = connection.execute("SELECT content_json FROM records WHERE record_key=?", (record_key,)).fetchone()
            return json.loads(row["content_json"]) if row else None
        finally:
            connection.close()

    def _insert_records_tx(self, connection: sqlite3.Connection, records: list[MirrorRecord]) -> None:
        connection.executemany(
            "INSERT INTO records(batch_id,line_no,record_key,content_json) VALUES(?,?,?,?)",
            [
                (record.batch_id, record.line_no, record.record_key, canonical_json(record.stored_dict()))
                for record in records
            ],
        )

    def _write_commit_tx(
        self,
        connection: sqlite3.Connection,
        *,
        parent_version: int | None,
        kind: str,
        snapshot_version: str,
        snapshot_hash: str,
        policy: dict[str, Any],
        records: list[MirrorRecord],
        selection: dict[str, Any],
        batches: dict[str, Any],
        bias: dict[str, Any],
        cascade: dict[str, Any],
        dependency_map: dict[str, list[str]],
        batch_state_override: dict[str, str] | None = None,
        run_id: str | None = None,
    ) -> int:
        version = self._next_version_tx(connection)
        now = utc_now()
        connection.execute(
            """
            INSERT INTO commits(commit_version,parent_version,kind,snapshot_version,policy_version,snapshot_hash,
                                record_count,selected_count,batch_count,cascade_json,created_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                version,
                parent_version,
                kind,
                snapshot_version,
                policy["policy_version"],
                snapshot_hash,
                len(records),
                selection["selected_count"],
                batches["batch_count"],
                canonical_json(cascade),
                now,
            ),
        )
        connection.execute(
            "INSERT INTO policies(commit_version,policy_json) VALUES(?,?)",
            (version, canonical_json(policy)),
        )
        connection.executemany(
            "INSERT INTO commit_records(commit_version,batch_id,line_no,record_key) VALUES(?,?,?,?)",
            [(version, record.batch_id, record.line_no, record.record_key) for record in records],
        )
        artifacts = {"selection": selection, "batches": batches, "bias": bias}
        for artifact_type, artifact in artifacts.items():
            content = canonical_json(artifact)
            connection.execute(
                "INSERT INTO artifacts(commit_version,artifact_type,artifact_id,content_json,content_hash) VALUES(?,?,?,?,?)",
                (version, artifact_type, artifact["artifact_id"], content, artifact["artifact_id"]),
            )
            connection.execute(
                "INSERT INTO artifact_dependencies(commit_version,artifact_type,artifact_id,rule_keys_json) VALUES(?,?,?,?)",
                (version, artifact_type, artifact["artifact_id"], canonical_json(dependency_map[artifact_type])),
            )
        states = []
        for batch in batches["batches"]:
            state = (batch_state_override or {}).get(batch["batch_id"], "ready")
            states.append((version, batch["batch_id"], state, run_id if state == "complete" else None, now))
        connection.executemany(
            "INSERT INTO batch_states(commit_version,batch_id,state,run_id,updated_at) VALUES(?,?,?,?,?)",
            states,
        )
        connection.execute(
            "INSERT INTO meta(key,value) VALUES('current_version',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(version),),
        )
        return version

    def validate_base_tx(self, connection: sqlite3.Connection, base_version: int | None) -> int:
        current = self._current_version_tx(connection)
        if current is None or base_version != current:
            raise BusinessError(
                "E_STALE_BASE",
                "base_version does not match the current committed version",
                {"base_version": base_version, "current_version": current},
            )
        return current

    def commit_snapshot(
        self,
        *,
        base_version: int | None,
        snapshot_version: str,
        records: list[MirrorRecord],
        policy: dict[str, Any],
        snapshot_hash: str,
        selection: dict[str, Any],
        batches: dict[str, Any],
        bias: dict[str, Any],
        cascade: dict[str, Any],
        dependency_map: dict[str, list[str]],
        intent_id: str,
    ) -> int:
        with self.transaction() as connection:
            current = self._current_version_tx(connection)
            if current is None:
                if base_version not in (None, 0):
                    raise BusinessError("E_STALE_BASE", "No committed version exists", {"base_version": base_version})
            else:
                self.validate_base_tx(connection, base_version)
                current_snapshot = connection.execute(
                    "SELECT snapshot_version FROM commits WHERE commit_version=?", (current,)
                ).fetchone()["snapshot_version"]
                if current_snapshot == snapshot_version:
                    raise BusinessError(
                        "E_SNAPSHOT_INVALID",
                        "Snapshot replacement requires a new snapshot_version",
                        {"snapshot_version": snapshot_version, "current_snapshot_version": current_snapshot},
                    )
            for record in records:
                row = connection.execute(
                    "SELECT content_json FROM records WHERE record_key=?", (record.record_key,)
                ).fetchone()
                if row is not None and json.loads(row["content_json"]) != record.stored_dict():
                    raise BusinessError(
                        "E_CONFLICTING_RECORD",
                        "Snapshot reuses a record identity with different content; first submission is preserved",
                        {"record_key": record.record_key},
                    )
            new_keys = [record.record_key for record in records]
            if len(new_keys) != len(set(new_keys)):
                raise BusinessError("E_RECORD_INVALID", "Snapshot contains duplicate record identities")
            connection.executemany(
                "INSERT OR IGNORE INTO records(batch_id,line_no,record_key,content_json) VALUES(?,?,?,?)",
                [
                    (record.batch_id, record.line_no, record.record_key, canonical_json(record.stored_dict()))
                    for record in records
                ],
            )
            version = self._write_commit_tx(
                connection,
                parent_version=current,
                kind="snapshot",
                snapshot_version=snapshot_version,
                snapshot_hash=snapshot_hash,
                policy=policy,
                records=records,
                selection=selection,
                batches=batches,
                bias=bias,
                cascade=cascade,
                dependency_map=dependency_map,
            )
            self.complete_intent(connection, intent_id, version)
            return version

    def commit_sample_batch(
        self,
        *,
        base_version: int,
        new_records: list[MirrorRecord],
        all_records: list[MirrorRecord],
        policy: dict[str, Any],
        snapshot_version: str,
        snapshot_hash: str,
        selection: dict[str, Any],
        batches: dict[str, Any],
        bias: dict[str, Any],
        cascade: dict[str, Any],
        dependency_map: dict[str, list[str]],
        intent_id: str,
    ) -> int:
        with self.transaction() as connection:
            current = self.validate_base_tx(connection, base_version)
            self._insert_records_tx(connection, new_records)
            version = self._write_commit_tx(
                connection,
                parent_version=current,
                kind="sample_batch",
                snapshot_version=snapshot_version,
                snapshot_hash=snapshot_hash,
                policy=policy,
                records=all_records,
                selection=selection,
                batches=batches,
                bias=bias,
                cascade=cascade,
                dependency_map=dependency_map,
            )
            self.complete_intent(connection, intent_id, version)
            return version

    def commit_policy_increment(
        self,
        *,
        base_version: int,
        all_records: list[MirrorRecord],
        policy: dict[str, Any],
        snapshot_version: str,
        snapshot_hash: str,
        selection: dict[str, Any],
        batches: dict[str, Any],
        bias: dict[str, Any],
        cascade: dict[str, Any],
        dependency_map: dict[str, list[str]],
        intent_id: str,
    ) -> int:
        with self.transaction() as connection:
            current = self.validate_base_tx(connection, base_version)
            version = self._write_commit_tx(
                connection,
                parent_version=current,
                kind="policy_increment",
                snapshot_version=snapshot_version,
                snapshot_hash=snapshot_hash,
                policy=policy,
                records=all_records,
                selection=selection,
                batches=batches,
                bias=bias,
                cascade=cascade,
                dependency_map=dependency_map,
            )
            self.complete_intent(connection, intent_id, version)
            return version

    def start_replay_run(
        self,
        *,
        run_id: str,
        source_version: int,
        batch_ids: list[str],
        intent_id: str,
    ) -> None:
        now = utc_now()
        with self.transaction() as connection:
            current = self._current_version_tx(connection)
            if current != source_version:
                raise BusinessError(
                    "E_STALE_BASE",
                    "Replay source_version is no longer current",
                    {"source_version": source_version, "current_version": current},
                )
            connection.execute(
                "INSERT INTO replay_runs(run_id,source_version,status,batch_ids_json,intent_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                (run_id, source_version, "running", canonical_json(batch_ids), intent_id, now, now),
            )
            connection.executemany(
                "UPDATE batch_states SET state='running',run_id=?,updated_at=? WHERE commit_version=? AND batch_id=?",
                [(run_id, now, source_version, batch_id) for batch_id in batch_ids],
            )

    def finish_replay_run(
        self,
        *,
        run_id: str,
        source_version: int,
        batch_ids: list[str],
        all_records: list[MirrorRecord],
        policy: dict[str, Any],
        snapshot_version: str,
        snapshot_hash: str,
        selection: dict[str, Any],
        batches: dict[str, Any],
        bias: dict[str, Any],
        cascade: dict[str, Any],
        dependency_map: dict[str, list[str]],
        metrics: dict[str, Any],
        intent_id: str,
    ) -> int:
        now = utc_now()
        with self.transaction() as connection:
            current = self.validate_base_tx(connection, source_version)
            version = self._write_commit_tx(
                connection,
                parent_version=current,
                kind="replay",
                snapshot_version=snapshot_version,
                snapshot_hash=snapshot_hash,
                policy=policy,
                records=all_records,
                selection=selection,
                batches=batches,
                bias=bias,
                cascade=cascade,
                dependency_map=dependency_map,
                batch_state_override={batch_id: "complete" for batch_id in batch_ids},
                run_id=run_id,
            )
            connection.execute(
                "UPDATE replay_runs SET status='completed',result_version=?,metrics_json=?,updated_at=? WHERE run_id=? AND status='running'",
                (version, canonical_json(metrics), now, run_id),
            )
            connection.executemany(
                "UPDATE batch_states SET state='complete',run_id=?,updated_at=? WHERE commit_version=? AND batch_id=?",
                [(run_id, now, source_version, batch_id) for batch_id in batch_ids],
            )
            self.complete_intent(connection, intent_id, version)
            return version

    def abort_replay_run(self, run_id: str, error: str) -> None:
        now = utc_now()
        with self.transaction() as connection:
            run = connection.execute("SELECT source_version,intent_id FROM replay_runs WHERE run_id=?", (run_id,)).fetchone()
            connection.execute(
                "UPDATE replay_runs SET status='aborted',metrics_json=?,updated_at=? WHERE run_id=? AND status='running'",
                (canonical_json({"status": "aborted", "error": error}), now, run_id),
            )
            if run is not None:
                connection.execute(
                    "UPDATE batch_states SET state='aborted',updated_at=? WHERE commit_version=? AND run_id=? AND state='running'",
                    (now, run["source_version"], run_id),
                )
                if run["intent_id"]:
                    connection.execute(
                        "UPDATE intents SET status='aborted',updated_at=? WHERE intent_id=? AND status='pending'",
                        (now, run["intent_id"]),
                    )

    def get_replay_run(self, run_id: str) -> dict[str, Any] | None:
        connection = self.connect()
        try:
            row = connection.execute("SELECT * FROM replay_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                return None
            value = dict(row)
            value["batch_ids"] = json.loads(value.pop("batch_ids_json"))
            value["metrics"] = json.loads(value["metrics_json"]) if value["metrics_json"] else None
            value.pop("metrics_json", None)
            return value
        finally:
            connection.close()

    def recover(self) -> dict[str, Any]:
        with self.transaction() as connection:
            now = utc_now()
            pending = connection.execute("SELECT * FROM intents WHERE status='pending' ORDER BY created_at").fetchall()
            aborted_intent_ids: list[str] = []
            aborted_run_ids: list[str] = []
            aborted_batch_ids: list[str] = []
            for row in pending:
                detail = json.loads(row["detail_json"])
                run_id = detail.get("run_id")
                if run_id:
                    run = connection.execute("SELECT status FROM replay_runs WHERE run_id=?", (run_id,)).fetchone()
                    if run is not None and run["status"] == "running":
                        connection.execute(
                            "UPDATE replay_runs SET status='aborted',updated_at=? WHERE run_id=?",
                            (now, run_id),
                        )
                        aborted_run_ids.append(run_id)
                    aborted_batch_ids.extend(
                        row2["batch_id"]
                        for row2 in connection.execute(
                            "SELECT batch_id FROM batch_states WHERE run_id=? AND state='running'", (run_id,)
                        ).fetchall()
                    )
                connection.execute(
                    "UPDATE intents SET status='aborted',updated_at=? WHERE intent_id=?",
                    (now, row["intent_id"]),
                )
                aborted_intent_ids.append(row["intent_id"])

            orphan_runs = connection.execute("SELECT run_id,source_version FROM replay_runs WHERE status='running'").fetchall()
            for run in orphan_runs:
                connection.execute("UPDATE replay_runs SET status='aborted',updated_at=? WHERE run_id=?", (now, run["run_id"]))
                aborted_run_ids.append(run["run_id"])

            running_batches = connection.execute(
                "SELECT commit_version,batch_id FROM batch_states WHERE state='running'"
            ).fetchall()
            for row in running_batches:
                connection.execute(
                    "UPDATE batch_states SET state='aborted',updated_at=? WHERE commit_version=? AND batch_id=?",
                    (now, row["commit_version"], row["batch_id"]),
                )
                aborted_batch_ids.append(row["batch_id"])

            current = self._current_version_tx(connection)
            current_commit = None
            if current is not None:
                row = connection.execute(
                    "SELECT kind,snapshot_version,policy_version,snapshot_hash FROM commits WHERE commit_version=?",
                    (current,),
                ).fetchone()
                current_commit = dict(row) if row else None
            report = {
                "report_source": "sqlite_persisted_facts_only",
                "created_at": now,
                "database_path": str(self.db_path),
                "last_complete_version": current,
                "current_commit": current_commit,
                "pending_intent_count": len(pending),
                "aborted_intent_ids": sorted(set(aborted_intent_ids)),
                "aborted_replay_run_ids": sorted(set(aborted_run_ids)),
                "aborted_batch_ids": sorted(set(aborted_batch_ids)),
                "recovery_action": "uncommitted intents aborted; visible versions remain immutable; running replay batches marked aborted",
            }
            connection.execute(
                "INSERT INTO recovery_reports(created_at,report_json) VALUES(?,?)",
                (now, canonical_json(report)),
            )
            return report

    def latest_recovery_report(self) -> dict[str, Any] | None:
        connection = self.connect()
        try:
            row = connection.execute(
                "SELECT created_at,report_json FROM recovery_reports ORDER BY report_id DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            report = json.loads(row["report_json"])
            report["stored_at"] = row["created_at"]
            return report
        finally:
            connection.close()

    def event_counts(self) -> dict[str, int]:
        connection = self.connect()
        try:
            return {
                "commits": connection.execute("SELECT COUNT(*) AS n FROM commits").fetchone()["n"],
                "records": connection.execute("SELECT COUNT(*) AS n FROM records").fetchone()["n"],
                "intents_pending": connection.execute("SELECT COUNT(*) AS n FROM intents WHERE status='pending'").fetchone()["n"],
                "intents_aborted": connection.execute("SELECT COUNT(*) AS n FROM intents WHERE status='aborted'").fetchone()["n"],
                "replay_runs_aborted": connection.execute("SELECT COUNT(*) AS n FROM replay_runs WHERE status='aborted'").fetchone()["n"],
            }
        finally:
            connection.close()
