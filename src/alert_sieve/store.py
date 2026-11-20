from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Sequence

from .domain import (
    DEFAULT_TIME_WINDOW_MS,
    SCORE_SCALE,
    SCORE_TOLERANCE,
    Alert,
    AlertIncrement,
    FingerprintMeta,
    SuppressionRule,
    parse_alert_increment,
    parse_alert_snapshot,
    parse_rules_increment,
    parse_rules_snapshot,
    parse_topology_increment,
    parse_topology_snapshot,
    verify_checksum,
    alert_increment_checksum,
)
from .errors import (
    BusinessError,
    alert_invalid,
    conflicting_alert,
    order_not_found,
    rebuild_conflict,
    stale_base,
)
from .scoring import (
    FixedPointContractError,
    rank_with_tolerance,
    score_pair,
    shortest_downstream_distances,
)
from .util import canonical_json, digest, utc_now

SEVERITY_RANK = {
    "P0": 0,
    "CRITICAL": 1,
    "FATAL": 1,
    "P1": 2,
    "ERROR": 3,
    "P2": 4,
    "WARNING": 5,
    "P3": 6,
    "INFO": 7,
    "P4": 8,
    "DEBUG": 9,
}


SCHEMA = """
CREATE TABLE IF NOT EXISTS runtime_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    commit_seq INTEGER NOT NULL DEFAULT 0,
    default_window_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS alert_meta (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    version INTEGER NOT NULL DEFAULT 0,
    checksum TEXT NOT NULL DEFAULT '',
    commit_seq INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS topology_meta (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    version INTEGER NOT NULL DEFAULT 0,
    checksum TEXT NOT NULL DEFAULT '',
    commit_seq INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rules_meta (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    version INTEGER NOT NULL DEFAULT 0,
    checksum TEXT NOT NULL DEFAULT '',
    commit_seq INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS commits (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    target_version INTEGER,
    checksum TEXT,
    created_at TEXT NOT NULL,
    summary_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS intent_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    base_version INTEGER,
    target_version INTEGER,
    payload_checksum TEXT,
    status TEXT NOT NULL CHECK (status IN ('pending', 'committed', 'aborted')),
    created_at TEXT NOT NULL,
    finished_at TEXT,
    commit_seq INTEGER,
    error_code TEXT,
    details_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_intent_status ON intent_log(status, id);

CREATE TABLE IF NOT EXISTS alerts (
    alert_uid TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    line_no INTEGER NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    fingerprint TEXT NOT NULL,
    service TEXT NOT NULL,
    resource TEXT NOT NULL,
    severity TEXT NOT NULL,
    labels_json TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    commit_seq INTEGER NOT NULL,
    UNIQUE (batch_id, line_no)
);
CREATE INDEX IF NOT EXISTS ix_alerts_fp_ts ON alerts(fingerprint, timestamp_ms, alert_uid);
CREATE INDEX IF NOT EXISTS ix_alerts_service_ts ON alerts(service, timestamp_ms, alert_uid);

CREATE TABLE IF NOT EXISTS fingerprint_meta (
    fingerprint TEXT PRIMARY KEY,
    service TEXT NOT NULL,
    labels_json TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    commit_seq INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_fp_meta_service ON fingerprint_meta(service);

CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    checksum TEXT NOT NULL,
    commit_seq INTEGER NOT NULL,
    result_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS topology_nodes (
    service TEXT PRIMARY KEY,
    commit_seq INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS topology_edges (
    source TEXT NOT NULL,
    target TEXT NOT NULL,
    commit_seq INTEGER NOT NULL,
    PRIMARY KEY (source, target)
);
CREATE INDEX IF NOT EXISTS ix_topology_target ON topology_edges(target);

CREATE TABLE IF NOT EXISTS suppression_rules (
    rule_id TEXT PRIMARY KEY,
    scope_service TEXT NOT NULL,
    scope_severity TEXT,
    match_labels_json TEXT NOT NULL,
    window_ms INTEGER NOT NULL,
    enabled INTEGER NOT NULL,
    commit_seq INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS aggregate_index (
    service TEXT NOT NULL,
    severity TEXT NOT NULL,
    raw_count INTEGER NOT NULL,
    retained_count INTEGER NOT NULL,
    deduped_count INTEGER NOT NULL,
    fingerprint_count INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    PRIMARY KEY (service, severity)
);

CREATE TABLE IF NOT EXISTS suppression_index (
    fingerprint TEXT PRIMARY KEY,
    service TEXT NOT NULL,
    rule_ids_json TEXT NOT NULL,
    effective_window_ms INTEGER NOT NULL,
    alert_count INTEGER NOT NULL,
    canonical_count INTEGER NOT NULL,
    deduped_count INTEGER NOT NULL,
    generation INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_suppression_service ON suppression_index(service);

CREATE TABLE IF NOT EXISTS alert_dedup (
    alert_uid TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    service TEXT NOT NULL,
    severity TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    canonical_alert_uid TEXT NOT NULL,
    is_canonical INTEGER NOT NULL,
    deduped INTEGER NOT NULL,
    window_ms INTEGER NOT NULL,
    rule_ids_json TEXT NOT NULL,
    generation INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_dedup_fp ON alert_dedup(fingerprint, timestamp_ms);
CREATE INDEX IF NOT EXISTS ix_dedup_service ON alert_dedup(service, canonical_alert_uid);

CREATE TABLE IF NOT EXISTS rootcause_scores (
    source_alert_uid TEXT NOT NULL,
    candidate_alert_uid TEXT NOT NULL,
    source_service TEXT NOT NULL,
    source_fingerprint TEXT NOT NULL,
    source_timestamp_ms INTEGER NOT NULL,
    candidate_service TEXT NOT NULL,
    candidate_fingerprint TEXT NOT NULL,
    candidate_timestamp_ms INTEGER NOT NULL,
    hops INTEGER NOT NULL,
    delta_ms INTEGER NOT NULL,
    hop_component INTEGER NOT NULL,
    time_component INTEGER NOT NULL,
    score_fixed INTEGER NOT NULL,
    rank INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    PRIMARY KEY (source_alert_uid, candidate_alert_uid)
);
CREATE INDEX IF NOT EXISTS ix_rootcause_source_service ON rootcause_scores(source_service, source_alert_uid);
CREATE INDEX IF NOT EXISTS ix_rootcause_candidate ON rootcause_scores(candidate_alert_uid);

CREATE TABLE IF NOT EXISTS artifact_refs (
    artifact TEXT NOT NULL,
    ref_kind TEXT NOT NULL CHECK (ref_kind IN ('service', 'fingerprint')),
    ref_key TEXT NOT NULL,
    product_key TEXT NOT NULL,
    generation INTEGER NOT NULL,
    PRIMARY KEY (artifact, ref_kind, ref_key, product_key)
);
CREATE INDEX IF NOT EXISTS ix_artifact_refs_product ON artifact_refs(product_key);

CREATE TABLE IF NOT EXISTS invalidation_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    commit_seq INTEGER NOT NULL,
    request_id TEXT NOT NULL,
    cause TEXT NOT NULL,
    scope_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_invalidation_commit ON invalidation_events(commit_seq);

CREATE TABLE IF NOT EXISTS invalidation_targets (
    event_id INTEGER NOT NULL,
    artifact TEXT NOT NULL,
    object_kind TEXT NOT NULL,
    object_key TEXT NOT NULL,
    state TEXT NOT NULL,
    generation INTEGER NOT NULL,
    product_count INTEGER NOT NULL,
    PRIMARY KEY (event_id, artifact, object_kind, object_key)
);

CREATE TABLE IF NOT EXISTS recovery_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_id INTEGER,
    outcome TEXT NOT NULL,
    details_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


@dataclass(frozen=True)
class CommitSnapshot:
    commit_seq: int
    alert_version: int
    alert_checksum: str
    topology_version: int
    topology_checksum: str
    rules_version: int
    rules_checksum: str
    default_window_ms: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "commit_seq": self.commit_seq,
            "alert_version": self.alert_version,
            "alert_checksum": self.alert_checksum,
            "topology_version": self.topology_version,
            "topology_checksum": self.topology_checksum,
            "rules_version": self.rules_version,
            "rules_checksum": self.rules_checksum,
            "default_window_ms": self.default_window_ms,
        }


class Store:
    """SQLite-backed single-writer/multi-reader alert reduction store."""

    def __init__(self, db_path: str | os.PathLike[str], *, recover: bool = True) -> None:
        self.db_path = os.fspath(db_path)
        parent = os.path.dirname(os.path.abspath(self.db_path))
        os.makedirs(parent, exist_ok=True)
        self._writer_lock = threading.Lock()
        self._init_db()
        if recover:
            self.recover_pending()

    # ------------------------------------------------------------------
    # connection / schema / recovery
    # ------------------------------------------------------------------
    def _connect(self, *, read_only: bool = False) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA foreign_keys = ON")
        if read_only:
            conn.execute("PRAGMA query_only = ON")
        return conn

    def _init_db(self) -> None:
        conn = self._connect()
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = FULL")
            conn.executescript(SCHEMA)
            conn.execute(
                "INSERT OR IGNORE INTO runtime_state(id, commit_seq, default_window_ms) VALUES (1, 0, ?)",
                (60_000,),
            )
            conn.execute("INSERT OR IGNORE INTO alert_meta(id) VALUES (1)")
            conn.execute("INSERT OR IGNORE INTO topology_meta(id) VALUES (1)")
            conn.execute("INSERT OR IGNORE INTO rules_meta(id) VALUES (1)")
        finally:
            conn.close()

    @contextmanager
    def _read_transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect(read_only=True)
        try:
            conn.execute("BEGIN")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def _snapshot(self, conn: sqlite3.Connection) -> CommitSnapshot:
        runtime = conn.execute(
            "SELECT commit_seq, default_window_ms FROM runtime_state WHERE id = 1"
        ).fetchone()
        alert = conn.execute("SELECT version, checksum FROM alert_meta WHERE id = 1").fetchone()
        topo = conn.execute("SELECT version, checksum FROM topology_meta WHERE id = 1").fetchone()
        rules = conn.execute("SELECT version, checksum FROM rules_meta WHERE id = 1").fetchone()
        return CommitSnapshot(
            commit_seq=int(runtime["commit_seq"]),
            alert_version=int(alert["version"]),
            alert_checksum=str(alert["checksum"]),
            topology_version=int(topo["version"]),
            topology_checksum=str(topo["checksum"]),
            rules_version=int(rules["version"]),
            rules_checksum=str(rules["checksum"]),
            default_window_ms=int(runtime["default_window_ms"]),
        )

    def _record_recovery(self) -> None:
        conn = self._connect()
        try:
            pending = conn.execute(
                "SELECT * FROM intent_log WHERE status = 'pending' ORDER BY id"
            ).fetchall()
            if not pending:
                return
            conn.execute("BEGIN IMMEDIATE")
            snapshot = self._snapshot(conn)
            for row in pending:
                commit = conn.execute(
                    "SELECT seq FROM commits WHERE request_id = ?",
                    (row["request_id"],),
                ).fetchone()
                if commit is not None:
                    outcome = "committed_fact_found"
                    conn.execute(
                        "UPDATE intent_log SET status='committed', finished_at=?, commit_seq=? WHERE id=?",
                        (utc_now(), commit["seq"], row["id"]),
                    )
                else:
                    outcome = "aborted_after_uncommitted_rebuild"
                    conn.execute(
                        "UPDATE intent_log SET status='aborted', finished_at=?, error_code='E_RECOVERY_ABORTED' WHERE id=?",
                        (utc_now(), row["id"]),
                    )
                details = {
                    "intent_id": row["id"],
                    "request_id": row["request_id"],
                    "kind": row["kind"],
                    "base_version": row["base_version"],
                    "target_version": row["target_version"],
                    "observed_versions": snapshot.as_dict(),
                    "outcome": outcome,
                }
                conn.execute(
                    "INSERT INTO recovery_events(intent_id, outcome, details_json, created_at) VALUES (?,?,?,?)",
                    (row["id"], outcome, canonical_json(details), utc_now()),
                )
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def recover_pending(self) -> dict[str, Any]:
        with self._writer_lock:
            self._record_recovery()
        return self.query_recovery_report()

    # ------------------------------------------------------------------
    # transaction helpers
    # ------------------------------------------------------------------
    def _new_intent(
        self,
        conn: sqlite3.Connection,
        *,
        request_id: str,
        kind: str,
        base_version: int | None,
        target_version: int | None,
        payload_checksum: str,
        details: dict[str, Any],
    ) -> int:
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                """
                INSERT INTO intent_log(
                    request_id, kind, base_version, target_version, payload_checksum,
                    status, created_at, details_json
                ) VALUES (?,?,?,?,?,'pending',?,?)
                """,
                (
                    request_id,
                    kind,
                    base_version,
                    target_version,
                    payload_checksum,
                    utc_now(),
                    canonical_json(details),
                ),
            )
            conn.execute("COMMIT")
            return int(cur.lastrowid)
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    def _mark_intent_aborted(self, intent_id: int, error: BaseException) -> None:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            code = error.code if isinstance(error, BusinessError) else "E_INTERNAL"
            details = {
                "error_code": code,
                "message": str(error),
            }
            conn.execute(
                "UPDATE intent_log SET status='aborted', finished_at=?, error_code=?, details_json=? WHERE id=? AND status='pending'",
                (utc_now(), code, canonical_json(details), intent_id),
            )
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def _insert_commit(
        self,
        conn: sqlite3.Connection,
        *,
        request_id: str,
        kind: str,
        target_version: int | None,
        checksum: str,
    ) -> int:
        cur = conn.execute(
            """
            INSERT INTO commits(request_id, kind, target_version, checksum, created_at, summary_json)
            VALUES (?,?,?,?,?,'{}')
            """,
            (request_id, kind, target_version, checksum, utc_now()),
        )
        return int(cur.lastrowid)

    def _finish_commit(
        self,
        conn: sqlite3.Connection,
        *,
        seq: int,
        intent_id: int,
        summary: dict[str, Any],
        now: str | None = None,
    ) -> None:
        stamp = now or utc_now()
        conn.execute(
            "UPDATE commits SET summary_json=? WHERE seq=?",
            (canonical_json(summary), seq),
        )
        conn.execute(
            "UPDATE runtime_state SET commit_seq=? WHERE id=1",
            (seq,),
        )
        for table in ("alert_meta", "topology_meta", "rules_meta"):
            conn.execute(f"UPDATE {table} SET commit_seq=? WHERE id=1", (seq,))
        conn.execute(
            "UPDATE intent_log SET status='committed', finished_at=?, commit_seq=?, details_json=? WHERE id=?",
            (stamp, seq, canonical_json(summary), intent_id),
        )

    def _run_intent_transaction(
        self,
        *,
        intent_id: int,
        operation,
        fault_crash_after_products: bool = False,
        fault_sleep_after_products_ms: int = 0,
    ) -> tuple[Any, CommitSnapshot]:
        conn = self._connect()
        seq: int | None = None
        try:
            conn.execute("BEGIN IMMEDIATE")
            result = operation(conn)
            seq = int(result["seq"])
            summary = dict(result.get("summary", {}))
            self._finish_commit(
                conn,
                seq=seq,
                intent_id=intent_id,
                summary=summary,
            )
            if fault_crash_after_products:
                os._exit(91)
            if fault_sleep_after_products_ms > 0:
                time.sleep(fault_sleep_after_products_ms / 1000.0)
            conn.execute("COMMIT")
            snapshot = self._snapshot(conn)
            return summary, snapshot
        except BaseException as exc:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            self._mark_intent_aborted(intent_id, exc)
            raise
        finally:
            conn.close()

    @staticmethod
    def _replay(summary: dict[str, Any]) -> dict[str, Any]:
        result = dict(summary)
        result["replay"] = True
        return result

    def _find_commit_result(
        self,
        conn: sqlite3.Connection,
        *,
        kind: str,
        checksum: str | None = None,
        target_version: int | None = None,
    ) -> dict[str, Any] | None:
        clauses = ["kind = ?"]
        params: list[Any] = [kind]
        if checksum is not None:
            clauses.append("checksum = ?")
            params.append(checksum)
        if target_version is not None:
            clauses.append("target_version = ?")
            params.append(target_version)
        row = conn.execute(
            f"SELECT summary_json FROM commits WHERE {' AND '.join(clauses)} ORDER BY seq DESC LIMIT 1",
            params,
        ).fetchone()
        if row is None:
            return None
        value = json.loads(row["summary_json"])
        return value if isinstance(value, dict) else None

    # ------------------------------------------------------------------
    # public write operations
    # ------------------------------------------------------------------
    def submit_alert_snapshot(
        self,
        body: Any,
        *,
        request_id: str | None = None,
        fault_crash_after_products: bool = False,
        fault_sleep_after_products_ms: int = 0,
    ) -> dict[str, Any]:
        parsed = parse_alert_snapshot(body)
        request_id = request_id or uuid.uuid4().hex
        with self._writer_lock:
            conn = self._connect()
            try:
                snap = self._snapshot(conn)
                replay = self._find_commit_result(
                    conn,
                    kind="alert_snapshot",
                    checksum=parsed.checksum,
                    target_version=parsed.version,
                )
                if replay is not None:
                    return self._replay(replay)
                if parsed.version < snap.alert_version:
                    raise stale_base(parsed.version, snap.alert_version)
                if parsed.version == snap.alert_version:
                    raise rebuild_conflict(
                        "alert snapshot version is already committed with different content",
                        version=parsed.version,
                        current_checksum=snap.alert_checksum,
                        supplied_checksum=parsed.checksum,
                    )
                intent_id = self._new_intent(
                    conn,
                    request_id=request_id,
                    kind="alert_snapshot",
                    base_version=None,
                    target_version=parsed.version,
                    payload_checksum=parsed.checksum,
                    details={"alert_count": len(parsed.alerts)},
                )
            finally:
                conn.close()

            def operation(tx: sqlite3.Connection) -> dict[str, Any]:
                snap_now = self._snapshot(tx)
                if parsed.version < snap_now.alert_version:
                    raise stale_base(parsed.version, snap_now.alert_version)
                if parsed.version == snap_now.alert_version:
                    raise rebuild_conflict(
                        "alert snapshot version is already committed with different content",
                        version=parsed.version,
                    )
                seq = self._insert_commit(
                    tx,
                    request_id=request_id,
                    kind="alert_snapshot",
                    target_version=parsed.version,
                    checksum=parsed.checksum,
                )
                old_services = {
                    row["service"] for row in tx.execute("SELECT DISTINCT service FROM alerts")
                }
                old_fps = {
                    row["fingerprint"] for row in tx.execute("SELECT fingerprint FROM fingerprint_meta")
                }
                tx.execute("DELETE FROM alerts")
                tx.execute("DELETE FROM fingerprint_meta")
                self._insert_alerts(tx, parsed.alerts, seq)
                for meta in parsed.fingerprints.values():
                    self._upsert_fingerprint(tx, meta, seq)
                tx.execute(
                    "UPDATE alert_meta SET version=?, checksum=?, commit_seq=? WHERE id=1",
                    (parsed.version, parsed.checksum, seq),
                )
                new_services = {alert.service for alert in parsed.alerts}
                affected_services, affected_fps = self._rebuild_scope(
                    tx,
                    base_services=old_services | new_services,
                    base_fps=old_fps | set(parsed.fingerprints),
                )
                stats = self._rebuild_artifacts(
                    tx,
                    generation=seq,
                    affected_services=affected_services,
                    affected_fingerprints=affected_fps,
                    full=True,
                    reason="alert_snapshot",
                    request_id=request_id,
                    include_ancestors=True,
                )
                summary = {
                    "request_id": request_id,
                    "kind": "alert_snapshot",
                    "version": parsed.version,
                    "checksum": parsed.checksum,
                    "commit_seq": seq,
                    "alert_count": len(parsed.alerts),
                    "alert_version": parsed.version,
                    "products": stats,
                }
                tx.execute(
                    "UPDATE alert_meta SET version=?, checksum=?, commit_seq=? WHERE id=1",
                    (parsed.version, parsed.checksum, seq),
                )
                return {"seq": seq, "summary": summary}

            summary, snap_after = self._run_intent_transaction(
                intent_id=intent_id,
                operation=operation,
                fault_crash_after_products=fault_crash_after_products,
                fault_sleep_after_products_ms=fault_sleep_after_products_ms,
            )
            result = dict(summary)
            result["state"] = snap_after.as_dict()
            return result

    def submit_alert_increment(
        self,
        body: Any,
        *,
        request_id: str | None = None,
        fault_crash_after_products: bool = False,
        fault_sleep_after_products_ms: int = 0,
    ) -> dict[str, Any]:
        # Parse all alert records first, but defer checksum verification until
        # after batch/line conflict checks. This gives callers the more specific
        # E_CONFLICTING_ALERT when replaying a batch id with altered content.
        parsed = parse_alert_increment(body, verify_checksum_value=False)
        request_id = request_id or uuid.uuid4().hex
        with self._writer_lock:
            conn = self._connect()
            try:
                snap = self._snapshot(conn)
                expected_checksum = alert_increment_checksum(
                    parsed.batch_id,
                    parsed.base_version,
                    parsed.version,
                    parsed.alerts,
                    parsed.affected_fingerprints,
                )
                batch_row = conn.execute(
                    "SELECT * FROM batches WHERE batch_id=?",
                    (parsed.batch_id,),
                ).fetchone()
                if batch_row is not None:
                    # Never trust the checksum supplied by the caller for
                    # idempotency. Recompute it from parsed alerts and metadata;
                    # this prevents altered content from reusing an old digest.
                    if expected_checksum != batch_row["checksum"]:
                        raise conflicting_alert(
                            "batch_id was already committed with different content",
                            batch_id=parsed.batch_id,
                            first_checksum=batch_row["checksum"],
                            recomputed_checksum=expected_checksum,
                        )
                    verify_checksum(expected_checksum, parsed.checksum, where="alert increment")
                    return self._replay(json.loads(batch_row["result_json"]))
                existing = self._load_alerts_by_uids(conn, [a.alert_uid for a in parsed.alerts])
                for alert in parsed.alerts:
                    prior = existing.get(alert.alert_uid)
                    if prior is not None and prior["content_hash"] != alert.content_hash:
                        raise conflicting_alert(
                            "batch_id/line_no already exists with different content",
                            batch_id=alert.batch_id,
                            line_no=alert.line_no,
                            alert_uid=alert.alert_uid,
                        )
                new_metadata = self._validate_increment_fingerprints(conn, parsed)
                verify_checksum(expected_checksum, parsed.checksum, where="alert increment")
                if parsed.base_version != snap.alert_version:
                    raise stale_base(parsed.base_version, snap.alert_version)
                if parsed.version <= parsed.base_version:
                    raise stale_base(parsed.base_version, parsed.version)
                intent_id = self._new_intent(
                    conn,
                    request_id=request_id,
                    kind="alert_increment",
                    base_version=parsed.base_version,
                    target_version=parsed.version,
                    payload_checksum=parsed.checksum,
                    details={
                        "batch_id": parsed.batch_id,
                        "alert_count": len(parsed.alerts),
                        "declared_fingerprint_changes": sorted(new_metadata),
                    },
                )
            finally:
                conn.close()

            def operation(tx: sqlite3.Connection) -> dict[str, Any]:
                snap_now = self._snapshot(tx)
                if parsed.base_version != snap_now.alert_version:
                    raise stale_base(parsed.base_version, snap_now.alert_version)
                if parsed.version <= parsed.base_version:
                    raise stale_base(parsed.base_version, parsed.version)
                for alert in parsed.alerts:
                    prior = tx.execute(
                        "SELECT content_hash FROM alerts WHERE alert_uid=?",
                        (alert.alert_uid,),
                    ).fetchone()
                    if prior is not None and prior["content_hash"] != alert.content_hash:
                        raise conflicting_alert(
                            "batch_id/line_no already exists with different content",
                            batch_id=alert.batch_id,
                            line_no=alert.line_no,
                            alert_uid=alert.alert_uid,
                        )
                tx_metadata = self._validate_increment_fingerprints(tx, parsed)
                seq = self._insert_commit(
                    tx,
                    request_id=request_id,
                    kind="alert_increment",
                    target_version=parsed.version,
                    checksum=parsed.checksum,
                )
                inserted = self._insert_alerts(tx, parsed.alerts, seq, ignore_duplicates=True)
                for meta in tx_metadata.values():
                    self._upsert_fingerprint(tx, meta, seq)
                tx.execute(
                    "UPDATE alert_meta SET version=?, checksum=?, commit_seq=? WHERE id=1",
                    (parsed.version, parsed.checksum, seq),
                )
                direct_services = {meta.service for meta in tx_metadata.values()}
                direct_services |= {alert.service for alert in parsed.alerts}
                affected_fps = set(tx_metadata) | {alert.fingerprint for alert in parsed.alerts}
                affected_services, affected_fps = self._rebuild_scope(
                    tx,
                    base_services=direct_services,
                    base_fps=affected_fps,
                )
                stats = self._rebuild_artifacts(
                    tx,
                    generation=seq,
                    affected_services=affected_services,
                    affected_fingerprints=affected_fps,
                    full=False,
                    reason="alert_increment",
                    request_id=request_id,
                    include_ancestors=True,
                )
                summary = {
                    "request_id": request_id,
                    "kind": "alert_increment",
                    "batch_id": parsed.batch_id,
                    "base_version": parsed.base_version,
                    "version": parsed.version,
                    "checksum": parsed.checksum,
                    "commit_seq": seq,
                    "inserted_count": inserted,
                    "alert_count": len(parsed.alerts),
                    "affected_services": sorted(affected_services),
                    "affected_fingerprints": sorted(affected_fps),
                    "products": stats,
                }
                if parsed.version != snap_now.alert_version:
                    tx.execute(
                        "UPDATE alert_meta SET version=?, checksum=?, commit_seq=? WHERE id=1",
                        (parsed.version, parsed.checksum, seq),
                    )
                tx.execute(
                    """
                    INSERT INTO batches(batch_id, version, checksum, commit_seq, result_json, created_at)
                    VALUES (?,?,?,?,?,?)
                    """,
                    (
                        parsed.batch_id,
                        parsed.version,
                        parsed.checksum,
                        seq,
                        canonical_json(summary),
                        utc_now(),
                    ),
                )
                return {"seq": seq, "summary": summary}

            summary, snap_after = self._run_intent_transaction(
                intent_id=intent_id,
                operation=operation,
                fault_crash_after_products=fault_crash_after_products,
                fault_sleep_after_products_ms=fault_sleep_after_products_ms,
            )
            result = dict(summary)
            result["state"] = snap_after.as_dict()
            return result

    def submit_topology_snapshot(
        self,
        body: Any,
        *,
        request_id: str | None = None,
        fault_crash_after_products: bool = False,
        fault_sleep_after_products_ms: int = 0,
    ) -> dict[str, Any]:
        parsed = parse_topology_snapshot(body)
        request_id = request_id or uuid.uuid4().hex
        with self._writer_lock:
            conn = self._connect()
            try:
                replay = self._find_commit_result(
                    conn,
                    kind="topology_snapshot",
                    checksum=parsed.checksum,
                    target_version=parsed.version,
                )
                if replay is not None:
                    return self._replay(replay)
                snap = self._snapshot(conn)
                if parsed.version < snap.topology_version:
                    raise stale_base(parsed.version, snap.topology_version)
                if parsed.version == snap.topology_version:
                    raise rebuild_conflict(
                        "topology snapshot version is already committed with different content",
                        version=parsed.version,
                        current_checksum=snap.topology_checksum,
                        supplied_checksum=parsed.checksum,
                    )
                intent_id = self._new_intent(
                    conn,
                    request_id=request_id,
                    kind="topology_snapshot",
                    base_version=None,
                    target_version=parsed.version,
                    payload_checksum=parsed.checksum,
                    details={"edge_count": len(parsed.edges), "node_count": len(parsed.nodes)},
                )
            finally:
                conn.close()

            def operation(tx: sqlite3.Connection) -> dict[str, Any]:
                snap_now = self._snapshot(tx)
                if parsed.version < snap_now.topology_version:
                    raise stale_base(parsed.version, snap_now.topology_version)
                if parsed.version == snap_now.topology_version:
                    raise rebuild_conflict(
                        "topology snapshot version is already committed with different content",
                        version=parsed.version,
                    )
                old_services = {row["service"] for row in tx.execute("SELECT DISTINCT service FROM alerts")}
                old_services |= {row["service"] for row in tx.execute("SELECT service FROM topology_nodes")}
                seq = self._insert_commit(
                    tx,
                    request_id=request_id,
                    kind="topology_snapshot",
                    target_version=parsed.version,
                    checksum=parsed.checksum,
                )
                tx.execute("DELETE FROM topology_edges")
                tx.execute("DELETE FROM topology_nodes")
                tx.executemany(
                    "INSERT INTO topology_nodes(service, commit_seq) VALUES (?,?)",
                    [(node, seq) for node in sorted(parsed.nodes)],
                )
                tx.executemany(
                    "INSERT INTO topology_edges(source, target, commit_seq) VALUES (?,?,?)",
                    [(source, target, seq) for source, target in sorted(parsed.edges)],
                )
                tx.execute(
                    "UPDATE topology_meta SET version=?, checksum=?, commit_seq=? WHERE id=1",
                    (parsed.version, parsed.checksum, seq),
                )
                new_services = {row["service"] for row in tx.execute("SELECT DISTINCT service FROM alerts")}
                new_services |= set(parsed.nodes)
                affected_services = old_services | new_services
                fps = {
                    row["fingerprint"]
                    for row in tx.execute("SELECT fingerprint FROM fingerprint_meta")
                }
                stats = self._rebuild_artifacts(
                    tx,
                    generation=seq,
                    affected_services=affected_services,
                    affected_fingerprints=fps,
                    full=True,
                    reason="topology_snapshot",
                    request_id=request_id,
                    include_ancestors=True,
                )
                summary = {
                    "request_id": request_id,
                    "kind": "topology_snapshot",
                    "version": parsed.version,
                    "checksum": parsed.checksum,
                    "commit_seq": seq,
                    "edge_count": len(parsed.edges),
                    "node_count": len(parsed.nodes),
                    "affected_services": sorted(affected_services),
                    "products": stats,
                }
                return {"seq": seq, "summary": summary}

            summary, snap_after = self._run_intent_transaction(
                intent_id=intent_id,
                operation=operation,
                fault_crash_after_products=fault_crash_after_products,
                fault_sleep_after_products_ms=fault_sleep_after_products_ms,
            )
            result = dict(summary)
            result["state"] = snap_after.as_dict()
            return result

    def submit_topology_increment(
        self,
        body: Any,
        *,
        request_id: str | None = None,
        fault_crash_after_products: bool = False,
        fault_sleep_after_products_ms: int = 0,
    ) -> dict[str, Any]:
        parsed = parse_topology_increment(body)
        request_id = request_id or uuid.uuid4().hex
        with self._writer_lock:
            conn = self._connect()
            try:
                replay = self._find_commit_result(
                    conn,
                    kind="topology_increment",
                    checksum=parsed.checksum,
                    target_version=parsed.version,
                )
                if replay is not None:
                    return self._replay(replay)
                snap = self._snapshot(conn)
                if parsed.base_version != snap.topology_version:
                    raise stale_base(parsed.base_version, snap.topology_version)
                if parsed.version <= parsed.base_version:
                    raise stale_base(parsed.base_version, parsed.version)
                intent_id = self._new_intent(
                    conn,
                    request_id=request_id,
                    kind="topology_increment",
                    base_version=parsed.base_version,
                    target_version=parsed.version,
                    payload_checksum=parsed.checksum,
                    details={
                        "upsert_edges": [list(x) for x in sorted(parsed.upsert_edges)],
                        "remove_edges": [list(x) for x in sorted(parsed.remove_edges)],
                    },
                )
            finally:
                conn.close()

            def operation(tx: sqlite3.Connection) -> dict[str, Any]:
                snap_now = self._snapshot(tx)
                if parsed.base_version != snap_now.topology_version:
                    raise stale_base(parsed.base_version, snap_now.topology_version)
                if parsed.version <= parsed.base_version:
                    raise stale_base(parsed.base_version, parsed.version)
                old_edges = {
                    (row["source"], row["target"])
                    for row in tx.execute("SELECT source, target FROM topology_edges")
                }
                old_nodes = {
                    row["service"] for row in tx.execute("SELECT service FROM topology_nodes")
                }
                new_edges = (old_edges - set(parsed.remove_edges)) | set(parsed.upsert_edges)
                new_nodes = (old_nodes - set(parsed.remove_nodes)) | set(parsed.upsert_nodes)
                new_nodes |= {s for s, _ in new_edges} | {t for _, t in new_edges}
                removed_but_referenced = (new_nodes & set(parsed.remove_nodes)) & (
                    {s for s, _ in new_edges} | {t for _, t in new_edges}
                )
                if removed_but_referenced:
                    raise BusinessError(
                        "E_SNAPSHOT_INVALID",
                        "a removed topology node is still referenced by an edge",
                        details={"nodes": sorted(removed_but_referenced)},
                    )
                changed_edges = old_edges ^ new_edges
                changed_sources = {source for source, _ in changed_edges}
                changed_sources |= set(parsed.upsert_nodes) | set(parsed.remove_nodes)
                seq = self._insert_commit(
                    tx,
                    request_id=request_id,
                    kind="topology_increment",
                    target_version=parsed.version,
                    checksum=parsed.checksum,
                )
                tx.execute("DELETE FROM topology_edges")
                tx.execute("DELETE FROM topology_nodes")
                tx.executemany(
                    "INSERT INTO topology_nodes(service, commit_seq) VALUES (?,?)",
                    [(node, seq) for node in sorted(new_nodes)],
                )
                tx.executemany(
                    "INSERT INTO topology_edges(source, target, commit_seq) VALUES (?,?,?)",
                    [(source, target, seq) for source, target in sorted(new_edges)],
                )
                tx.execute(
                    "UPDATE topology_meta SET version=?, checksum=?, commit_seq=? WHERE id=1",
                    (parsed.version, parsed.checksum, seq),
                )
                affected_services, affected_fps = self._rebuild_scope(
                    tx,
                    base_services=changed_sources,
                    base_fps=set(),
                )
                stats = self._rebuild_artifacts(
                    tx,
                    generation=seq,
                    affected_services=affected_services,
                    affected_fingerprints=affected_fps,
                    full=False,
                    reason="topology_increment",
                    request_id=request_id,
                    include_ancestors=True,
                )
                summary = {
                    "request_id": request_id,
                    "kind": "topology_increment",
                    "base_version": parsed.base_version,
                    "version": parsed.version,
                    "checksum": parsed.checksum,
                    "commit_seq": seq,
                    "changed_edges": [
                        {"source": s, "target": t} for s, t in sorted(changed_edges)
                    ],
                    "affected_services": sorted(affected_services),
                    "affected_fingerprints": sorted(affected_fps),
                    "products": stats,
                }
                return {"seq": seq, "summary": summary}

            summary, snap_after = self._run_intent_transaction(
                intent_id=intent_id,
                operation=operation,
                fault_crash_after_products=fault_crash_after_products,
                fault_sleep_after_products_ms=fault_sleep_after_products_ms,
            )
            result = dict(summary)
            result["state"] = snap_after.as_dict()
            return result

    def submit_rules_snapshot(
        self,
        body: Any,
        *,
        request_id: str | None = None,
        fault_crash_after_products: bool = False,
        fault_sleep_after_products_ms: int = 0,
    ) -> dict[str, Any]:
        parsed = parse_rules_snapshot(body)
        request_id = request_id or uuid.uuid4().hex
        with self._writer_lock:
            conn = self._connect()
            try:
                replay = self._find_commit_result(
                    conn,
                    kind="rules_snapshot",
                    checksum=parsed.checksum,
                    target_version=parsed.version,
                )
                if replay is not None:
                    return self._replay(replay)
                snap = self._snapshot(conn)
                if parsed.version < snap.rules_version:
                    raise stale_base(parsed.version, snap.rules_version)
                if parsed.version == snap.rules_version:
                    raise rebuild_conflict(
                        "rules snapshot version is already committed with different content",
                        version=parsed.version,
                        current_checksum=snap.rules_checksum,
                        supplied_checksum=parsed.checksum,
                    )
                intent_id = self._new_intent(
                    conn,
                    request_id=request_id,
                    kind="rules_snapshot",
                    base_version=None,
                    target_version=parsed.version,
                    payload_checksum=parsed.checksum,
                    details={"rule_count": len(parsed.rules)},
                )
            finally:
                conn.close()

            def operation(tx: sqlite3.Connection) -> dict[str, Any]:
                snap_now = self._snapshot(tx)
                if parsed.version < snap_now.rules_version:
                    raise stale_base(parsed.version, snap_now.rules_version)
                if parsed.version == snap_now.rules_version:
                    raise rebuild_conflict(
                        "rules snapshot version is already committed with different content",
                        version=parsed.version,
                    )
                seq = self._insert_commit(
                    tx,
                    request_id=request_id,
                    kind="rules_snapshot",
                    target_version=parsed.version,
                    checksum=parsed.checksum,
                )
                tx.execute("DELETE FROM suppression_rules")
                for rule in sorted(parsed.rules.values(), key=lambda r: r.rule_id):
                    self._upsert_rule(tx, rule, seq)
                tx.execute(
                    "UPDATE rules_meta SET version=?, checksum=?, commit_seq=? WHERE id=1",
                    (parsed.version, parsed.checksum, seq),
                )
                tx.execute("UPDATE runtime_state SET default_window_ms=? WHERE id=1", (parsed.default_window_ms,))
                fps = {
                    row["fingerprint"] for row in tx.execute("SELECT fingerprint FROM fingerprint_meta")
                }
                services = {
                    row["service"] for row in tx.execute("SELECT DISTINCT service FROM fingerprint_meta")
                }
                services |= {
                    row["service"] for row in tx.execute("SELECT DISTINCT service FROM alerts")
                }
                affected_services, affected_fps = self._rebuild_scope(
                    tx,
                    base_services=services,
                    base_fps=fps,
                )
                stats = self._rebuild_artifacts(
                    tx,
                    generation=seq,
                    affected_services=affected_services,
                    affected_fingerprints=affected_fps,
                    full=True,
                    reason="rules_snapshot",
                    request_id=request_id,
                    include_ancestors=True,
                )
                summary = {
                    "request_id": request_id,
                    "kind": "rules_snapshot",
                    "version": parsed.version,
                    "checksum": parsed.checksum,
                    "commit_seq": seq,
                    "rule_count": len(parsed.rules),
                    "default_window_ms": parsed.default_window_ms,
                    "affected_services": sorted(affected_services),
                    "affected_fingerprints": sorted(affected_fps),
                    "products": stats,
                }
                return {"seq": seq, "summary": summary}

            summary, snap_after = self._run_intent_transaction(
                intent_id=intent_id,
                operation=operation,
                fault_crash_after_products=fault_crash_after_products,
                fault_sleep_after_products_ms=fault_sleep_after_products_ms,
            )
            result = dict(summary)
            result["state"] = snap_after.as_dict()
            return result

    def submit_rules_increment(
        self,
        body: Any,
        *,
        request_id: str | None = None,
        fault_crash_after_products: bool = False,
        fault_sleep_after_products_ms: int = 0,
    ) -> dict[str, Any]:
        parsed = parse_rules_increment(body)
        request_id = request_id or uuid.uuid4().hex
        with self._writer_lock:
            conn = self._connect()
            try:
                replay = self._find_commit_result(
                    conn,
                    kind="rules_increment",
                    checksum=parsed.checksum,
                    target_version=parsed.version,
                )
                if replay is not None:
                    return self._replay(replay)
                snap = self._snapshot(conn)
                if parsed.base_version != snap.rules_version:
                    raise stale_base(parsed.base_version, snap.rules_version)
                if parsed.version <= parsed.base_version:
                    raise stale_base(parsed.base_version, parsed.version)
                intent_id = self._new_intent(
                    conn,
                    request_id=request_id,
                    kind="rules_increment",
                    base_version=parsed.base_version,
                    target_version=parsed.version,
                    payload_checksum=parsed.checksum,
                    details={
                        "upsert_rules": sorted(parsed.upsert_rules),
                        "remove_rules": sorted(parsed.remove_rules),
                    },
                )
            finally:
                conn.close()

            def operation(tx: sqlite3.Connection) -> dict[str, Any]:
                snap_now = self._snapshot(tx)
                if parsed.base_version != snap_now.rules_version:
                    raise stale_base(parsed.base_version, snap_now.rules_version)
                if parsed.version <= parsed.base_version:
                    raise stale_base(parsed.base_version, parsed.version)
                old_rules, old_default = self._load_rules(tx)
                new_rules = dict(old_rules)
                for rule_id in parsed.remove_rules:
                    new_rules.pop(rule_id, None)
                new_rules.update(parsed.upsert_rules)
                new_default = parsed.default_window_ms
                if new_default is None:
                    new_default = old_default
                affected_fps: set[str] = set()
                fps_rows = tx.execute(
                    "SELECT fingerprint, service, labels_json FROM fingerprint_meta"
                ).fetchall()
                for row in fps_rows:
                    labels = json.loads(row["labels_json"])
                    old_info = self._matching_rule_info(
                        old_rules.values(), old_default, row["service"], None, labels
                    )
                    new_info = self._matching_rule_info(
                        new_rules.values(), new_default, row["service"], None, labels
                    )
                    if old_default != new_default or old_info != new_info:
                        affected_fps.add(row["fingerprint"])
                direct_services = {
                    row["service"]
                    for row in fps_rows
                    if row["fingerprint"] in affected_fps
                }
                changed_rule_ids = (
                    set(parsed.upsert_rules)
                    | set(parsed.remove_rules)
                    | {
                        rule_id
                        for rule_id in set(old_rules) & set(new_rules)
                        if old_rules[rule_id] != new_rules[rule_id]
                    }
                )
                if changed_rule_ids:
                    changed_objects = [new_rules[rule_id] for rule_id in changed_rule_ids if rule_id in new_rules]
                    services_for_changed_rules = {
                        row["service"]
                        for row in fps_rows
                        if changed_objects
                        and self._rule_ids_matching_meta(
                            changed_objects,
                            row["service"],
                            None,
                            json.loads(row["labels_json"]),
                        )
                    }
                    direct_services |= services_for_changed_rules
                seq = self._insert_commit(
                    tx,
                    request_id=request_id,
                    kind="rules_increment",
                    target_version=parsed.version,
                    checksum=parsed.checksum,
                )
                tx.execute("DELETE FROM suppression_rules")
                for rule in sorted(new_rules.values(), key=lambda r: r.rule_id):
                    self._upsert_rule(tx, rule, seq)
                tx.execute(
                    "UPDATE rules_meta SET version=?, checksum=?, commit_seq=? WHERE id=1",
                    (parsed.version, parsed.checksum, seq),
                )
                tx.execute("UPDATE runtime_state SET default_window_ms=? WHERE id=1", (new_default,))
                affected_services, affected_fps = self._rebuild_scope(
                    tx,
                    base_services=direct_services,
                    base_fps=affected_fps,
                )
                stats = self._rebuild_artifacts(
                    tx,
                    generation=seq,
                    affected_services=affected_services,
                    affected_fingerprints=affected_fps,
                    full=False,
                    reason="rules_increment",
                    request_id=request_id,
                    include_ancestors=True,
                )
                summary = {
                    "request_id": request_id,
                    "kind": "rules_increment",
                    "base_version": parsed.base_version,
                    "version": parsed.version,
                    "checksum": parsed.checksum,
                    "commit_seq": seq,
                    "upsert_rule_ids": sorted(parsed.upsert_rules),
                    "remove_rule_ids": sorted(parsed.remove_rules),
                    "default_window_ms": new_default,
                    "affected_services": sorted(affected_services),
                    "affected_fingerprints": sorted(affected_fps),
                    "products": stats,
                }
                return {"seq": seq, "summary": summary}

            summary, snap_after = self._run_intent_transaction(
                intent_id=intent_id,
                operation=operation,
                fault_crash_after_products=fault_crash_after_products,
                fault_sleep_after_products_ms=fault_sleep_after_products_ms,
            )
            result = dict(summary)
            result["state"] = snap_after.as_dict()
            return result

    # ------------------------------------------------------------------
    # write/rebuild helpers
    # ------------------------------------------------------------------
    def _insert_alerts(
        self,
        conn: sqlite3.Connection,
        alerts: Sequence[Alert],
        commit_seq: int,
        *,
        ignore_duplicates: bool = False,
    ) -> int:
        inserted = 0
        verb = "INSERT OR IGNORE" if ignore_duplicates else "INSERT"
        for alert in alerts:
            cur = conn.execute(
                f"""
                {verb} INTO alerts(
                    alert_uid, batch_id, line_no, timestamp_ms, fingerprint, service,
                    resource, severity, labels_json, content_hash, commit_seq
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    alert.alert_uid,
                    alert.batch_id,
                    alert.line_no,
                    alert.timestamp_ms,
                    alert.fingerprint,
                    alert.service,
                    alert.resource,
                    alert.severity,
                    canonical_json(alert.labels),
                    alert.content_hash,
                    commit_seq,
                ),
            )
            if cur.rowcount:
                inserted += 1
        return inserted

    @staticmethod
    def _load_alerts_by_uids(
        conn: sqlite3.Connection, uids: Iterable[str]
    ) -> dict[str, sqlite3.Row]:
        values = sorted(set(uids))
        if not values:
            return {}
        placeholders = ",".join("?" for _ in values)
        rows = conn.execute(
            f"SELECT * FROM alerts WHERE alert_uid IN ({placeholders})",
            values,
        ).fetchall()
        return {row["alert_uid"]: row for row in rows}

    def _validate_increment_fingerprints(
        self, conn: sqlite3.Connection, parsed: AlertIncrement
    ) -> dict[str, FingerprintMeta]:
        involved = {alert.fingerprint for alert in parsed.alerts}
        involved |= {meta.fingerprint for meta in parsed.affected_fingerprints}
        if not involved:
            return {}
        placeholders = ",".join("?" for _ in involved)
        rows = conn.execute(
            f"SELECT * FROM fingerprint_meta WHERE fingerprint IN ({placeholders})",
            sorted(involved),
        ).fetchall()
        existing = {row["fingerprint"]: row for row in rows}
        declared = {meta.fingerprint: meta for meta in parsed.affected_fingerprints}
        changed: dict[str, FingerprintMeta] = {}

        for meta in parsed.affected_fingerprints:
            prior = existing.get(meta.fingerprint)
            if prior is not None:
                if prior["service"] != meta.service:
                    raise alert_invalid(
                        "a fingerprint cannot move to a different service",
                        fingerprint=meta.fingerprint,
                        existing_service=prior["service"],
                        supplied_service=meta.service,
                    )
                prior_labels = json.loads(prior["labels_json"])
                if prior_labels != meta.labels:
                    changed[meta.fingerprint] = meta
            else:
                changed[meta.fingerprint] = meta

        for alert in parsed.alerts:
            prior = existing.get(alert.fingerprint)
            if prior is not None:
                if prior["service"] != alert.service:
                    raise alert_invalid(
                        "a fingerprint cannot move to a different service",
                        fingerprint=alert.fingerprint,
                        existing_service=prior["service"],
                        supplied_service=alert.service,
                    )
                prior_labels = json.loads(prior["labels_json"])
                if prior_labels != alert.labels and alert.fingerprint not in declared:
                    raise alert_invalid(
                        "fingerprint label changes must be declared in affected_fingerprints",
                        fingerprint=alert.fingerprint,
                    )
            else:
                meta = declared.get(alert.fingerprint)
                if meta is None:
                    changed[alert.fingerprint] = FingerprintMeta(
                        alert.fingerprint, alert.service, alert.labels
                    )
        return changed

    @staticmethod
    def _upsert_fingerprint(
        conn: sqlite3.Connection, meta: FingerprintMeta, commit_seq: int
    ) -> None:
        conn.execute(
            """
            INSERT INTO fingerprint_meta(fingerprint, service, labels_json, content_hash, commit_seq)
            VALUES (?,?,?,?,?)
            ON CONFLICT(fingerprint) DO UPDATE SET
                service=excluded.service,
                labels_json=excluded.labels_json,
                content_hash=excluded.content_hash,
                commit_seq=excluded.commit_seq
            """,
            (
                meta.fingerprint,
                meta.service,
                canonical_json(meta.labels),
                meta.content_hash,
                commit_seq,
            ),
        )

    @staticmethod
    def _upsert_rule(conn: sqlite3.Connection, rule: SuppressionRule, commit_seq: int) -> None:
        conn.execute(
            """
            INSERT INTO suppression_rules(
                rule_id, scope_service, scope_severity, match_labels_json,
                window_ms, enabled, commit_seq
            ) VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(rule_id) DO UPDATE SET
                scope_service=excluded.scope_service,
                scope_severity=excluded.scope_severity,
                match_labels_json=excluded.match_labels_json,
                window_ms=excluded.window_ms,
                enabled=excluded.enabled,
                commit_seq=excluded.commit_seq
            """,
            (
                rule.rule_id,
                rule.scope_service,
                rule.scope_severity,
                canonical_json(rule.match_labels),
                rule.window_ms,
                1 if rule.enabled else 0,
                commit_seq,
            ),
        )

    @staticmethod
    def _load_rules(
        conn: sqlite3.Connection,
    ) -> tuple[dict[str, SuppressionRule], int]:
        rows = conn.execute("SELECT * FROM suppression_rules ORDER BY rule_id").fetchall()
        rules = {
            row["rule_id"]: SuppressionRule(
                rule_id=row["rule_id"],
                scope_service=row["scope_service"],
                scope_severity=row["scope_severity"],
                match_labels=json.loads(row["match_labels_json"]),
                window_ms=int(row["window_ms"]),
                enabled=bool(row["enabled"]),
            )
            for row in rows
        }
        runtime = conn.execute("SELECT default_window_ms FROM runtime_state WHERE id=1").fetchone()
        return rules, int(runtime["default_window_ms"])

    @staticmethod
    def _matching_rule_info(
        rules: Iterable[SuppressionRule],
        default_window_ms: int,
        service: str,
        severity: str | None,
        labels: dict[str, str],
    ) -> tuple[tuple[str, ...], int]:
        matched: list[SuppressionRule] = []
        for rule in rules:
            if not rule.enabled:
                continue
            if rule.scope_service not in ("*", service):
                continue
            if severity is not None and rule.scope_severity not in (None, "*", severity):
                continue
            if all(labels.get(key) == value for key, value in rule.match_labels.items()):
                matched.append(rule)
        if not matched:
            return ((), default_window_ms)
        return (
            tuple(sorted(rule.rule_id for rule in matched)),
            max(rule.window_ms for rule in matched),
        )

    @staticmethod
    def _rule_ids_matching_meta(
        rules: Iterable[SuppressionRule],
        service: str,
        severity: str | None,
        labels: dict[str, str],
    ) -> tuple[str, ...]:
        matched = []
        for rule in rules:
            if not rule.enabled:
                continue
            if rule.scope_service not in ("*", service):
                continue
            if severity is not None and rule.scope_severity not in (None, "*", severity):
                continue
            if all(labels.get(key) == value for key, value in rule.match_labels.items()):
                matched.append(rule.rule_id)
        return tuple(sorted(matched))

    @staticmethod
    def _load_adjacency(conn: sqlite3.Connection) -> dict[str, set[str]]:
        adjacency: dict[str, set[str]] = {}
        for row in conn.execute("SELECT source, target FROM topology_edges"):
            adjacency.setdefault(row["source"], set()).add(row["target"])
        return adjacency

    @staticmethod
    def _expand_ancestors(
        services: Iterable[str], adjacency: dict[str, set[str]]
    ) -> set[str]:
        reverse: dict[str, set[str]] = {}
        for source, targets in adjacency.items():
            for target in targets:
                reverse.setdefault(target, set()).add(source)
        closure = set(services)
        queue = list(services)
        while queue:
            current = queue.pop()
            for upstream in reverse.get(current, ()):
                if upstream not in closure:
                    closure.add(upstream)
                    queue.append(upstream)
        return closure

    def _rebuild_scope(
        self,
        conn: sqlite3.Connection,
        *,
        base_services: Iterable[str],
        base_fps: Iterable[str],
    ) -> tuple[set[str], set[str]]:
        services = set(base_services)
        fps = set(base_fps)
        if fps:
            placeholders = ",".join("?" for _ in fps)
            for row in conn.execute(
                f"SELECT fingerprint, service FROM fingerprint_meta WHERE fingerprint IN ({placeholders})",
                sorted(fps),
            ):
                services.add(row["service"])
                fps.add(row["fingerprint"])
        adjacency = self._load_adjacency(conn)
        services = self._expand_ancestors(services, adjacency)
        if services:
            placeholders = ",".join("?" for _ in services)
            rows = conn.execute(
                f"SELECT fingerprint FROM fingerprint_meta WHERE service IN ({placeholders})",
                sorted(services),
            ).fetchall()
            fps |= {row["fingerprint"] for row in rows}
        return services, fps

    def _rebuild_artifacts(
        self,
        conn: sqlite3.Connection,
        *,
        generation: int,
        affected_services: Iterable[str],
        affected_fingerprints: Iterable[str],
        full: bool,
        reason: str,
        request_id: str,
        include_ancestors: bool,
    ) -> dict[str, Any]:
        services = set(affected_services)
        fps = set(affected_fingerprints)
        all_meta = conn.execute(
            "SELECT fingerprint, service FROM fingerprint_meta ORDER BY fingerprint"
        ).fetchall()
        if full:
            services |= {row["service"] for row in all_meta}
            services |= {row["service"] for row in conn.execute("SELECT DISTINCT service FROM alerts")}
            fps = {row["fingerprint"] for row in all_meta}
        elif include_ancestors:
            services = self._expand_ancestors(services, self._load_adjacency(conn))
        if services:
            placeholders = ",".join("?" for _ in services)
            fps |= {
                row["fingerprint"]
                for row in conn.execute(
                    f"SELECT fingerprint FROM fingerprint_meta WHERE service IN ({placeholders})",
                    sorted(services),
                )
            }
        if fps:
            placeholders = ",".join("?" for _ in fps)
            services |= {
                row["service"]
                for row in conn.execute(
                    f"SELECT service FROM fingerprint_meta WHERE fingerprint IN ({placeholders})",
                    sorted(fps),
                )
            }

        if full:
            conn.execute("DELETE FROM aggregate_index")
            conn.execute("DELETE FROM suppression_index")
            conn.execute("DELETE FROM alert_dedup")
            conn.execute("DELETE FROM rootcause_scores")
            conn.execute("DELETE FROM artifact_refs")

        if services:
            placeholders = ",".join("?" for _ in services)
            conn.execute(
                f"DELETE FROM aggregate_index WHERE service IN ({placeholders})",
                sorted(services),
            )
            conn.execute(
                f"DELETE FROM rootcause_scores WHERE source_service IN ({placeholders})",
                sorted(services),
            )
            conn.execute(
                "DELETE FROM artifact_refs WHERE ref_kind='service' AND ref_key IN ("
                + placeholders
                + ")",
                sorted(services),
            )
        if fps:
            placeholders = ",".join("?" for _ in fps)
            conn.execute(
                f"DELETE FROM suppression_index WHERE fingerprint IN ({placeholders})",
                sorted(fps),
            )
            conn.execute(
                f"DELETE FROM alert_dedup WHERE fingerprint IN ({placeholders})",
                sorted(fps),
            )
            conn.execute(
                "DELETE FROM artifact_refs WHERE ref_kind='fingerprint' AND ref_key IN ("
                + placeholders
                + ")",
                sorted(fps),
            )

        rules, default_window = self._load_rules(conn)
        self._rebuild_dedup_and_suppression(
            conn,
            generation=generation,
            services=services,
            fingerprints=fps,
            rules=rules,
            default_window_ms=default_window,
        )
        self._rebuild_aggregates(conn, generation=generation, services=services)
        self._rebuild_root_causes(conn, generation=generation, services=services)
        self._insert_artifact_refs(conn, generation=generation, services=services, fingerprints=fps)

        service_counts = self._product_counts_for_services(conn, services)
        fp_counts = self._product_counts_for_fingerprints(conn, fps)
        scope = {
            "reason": reason,
            "services": sorted(services),
            "fingerprints": sorted(fps),
            "full": full,
        }
        cur = conn.execute(
            """
            INSERT INTO invalidation_events(commit_seq, request_id, cause, scope_json, created_at)
            VALUES (?,?,?,?,?)
            """,
            (generation, request_id, reason, canonical_json(scope), utc_now()),
        )
        event_id = int(cur.lastrowid)
        targets: list[tuple[Any, ...]] = []
        for artifact in ("aggregate", "suppression", "rootcause"):
            for service in sorted(services):
                targets.append(
                    (
                        event_id,
                        artifact,
                        "service",
                        service,
                        "rebuilt",
                        generation,
                        service_counts.get((artifact, service), 0),
                    )
                )
            for fingerprint in sorted(fps):
                targets.append(
                    (
                        event_id,
                        artifact,
                        "fingerprint",
                        fingerprint,
                        "rebuilt",
                        generation,
                        fp_counts.get((artifact, fingerprint), 0),
                    )
                )
        conn.executemany(
            """
            INSERT OR REPLACE INTO invalidation_targets(
                event_id, artifact, object_kind, object_key, state, generation, product_count
            ) VALUES (?,?,?,?,?,?,?)
            """,
            targets,
        )
        dedup_count = 0
        if fps:
            placeholders = ",".join("?" for _ in fps)
            dedup_count = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM alert_dedup WHERE fingerprint IN ({placeholders})",
                    sorted(fps),
                ).fetchone()[0]
            )
        return {
            "invalidation_event_id": event_id,
            "affected_services": sorted(services),
            "affected_fingerprints": sorted(fps),
            "dedup_alerts": dedup_count,
            "aggregate_groups": sum(
                count for (artifact, _), count in service_counts.items() if artifact == "aggregate"
            ),
            "suppression_fingerprints": sum(
                count for (artifact, _), count in fp_counts.items() if artifact == "suppression"
            ),
            "root_cause_rows": sum(
                count for (artifact, _), count in service_counts.items() if artifact == "rootcause"
            ),
        }

    def _rebuild_dedup_and_suppression(
        self,
        conn: sqlite3.Connection,
        *,
        generation: int,
        services: set[str],
        fingerprints: set[str],
        rules: dict[str, SuppressionRule],
        default_window_ms: int,
    ) -> None:
        del services  # service scope is expanded to fingerprints by the caller
        meta_rows = {}
        if fingerprints:
            placeholders = ",".join("?" for _ in fingerprints)
            meta_rows = {
                row["fingerprint"]: row
                for row in conn.execute(
                    f"SELECT * FROM fingerprint_meta WHERE fingerprint IN ({placeholders})",
                    sorted(fingerprints),
                )
            }
        dedup_rows: list[tuple[Any, ...]] = []
        suppression_rows: list[tuple[Any, ...]] = []
        for fingerprint in sorted(fingerprints):
            meta = meta_rows.get(fingerprint)
            if meta is None:
                continue
            alerts = conn.execute(
                "SELECT * FROM alerts WHERE fingerprint=? ORDER BY timestamp_ms, alert_uid",
                (fingerprint,),
            ).fetchall()
            last_canonical_ts: int | None = None
            last_canonical_uid: str | None = None
            canonical_count = 0
            matched_any: set[str] = set()
            max_window = 0
            rows_for_fp: list[tuple[Any, ...]] = []
            labels = json.loads(meta["labels_json"])
            for alert in alerts:
                rule_ids, window = self._matching_rule_info(
                    rules.values(),
                    default_window_ms,
                    alert["service"],
                    alert["severity"],
                    labels,
                )
                matched_any.update(rule_ids)
                max_window = max(max_window, window)
                is_duplicate = (
                    last_canonical_ts is not None
                    and alert["timestamp_ms"] >= last_canonical_ts
                    and alert["timestamp_ms"] - last_canonical_ts <= window
                )
                if is_duplicate:
                    canonical_uid = str(last_canonical_uid)
                    is_canonical = 0
                else:
                    canonical_uid = alert["alert_uid"]
                    is_canonical = 1
                    canonical_count += 1
                    last_canonical_ts = int(alert["timestamp_ms"])
                    last_canonical_uid = alert["alert_uid"]
                rows_for_fp.append(
                    (
                        alert["alert_uid"],
                        fingerprint,
                        alert["service"],
                        alert["severity"],
                        alert["timestamp_ms"],
                        canonical_uid,
                        is_canonical,
                        0 if is_canonical else 1,
                        window,
                        canonical_json(rule_ids),
                        generation,
                    )
                )
            if not alerts:
                max_window = default_window_ms
            dedup_rows.extend(rows_for_fp)
            suppression_rows.append(
                (
                    fingerprint,
                    meta["service"],
                    canonical_json(sorted(matched_any)),
                    max_window,
                    len(alerts),
                    canonical_count,
                    len(alerts) - canonical_count,
                    generation,
                )
            )
        if dedup_rows:
            conn.executemany(
                """
                INSERT INTO alert_dedup(
                    alert_uid, fingerprint, service, severity, timestamp_ms,
                    canonical_alert_uid, is_canonical, deduped, window_ms,
                    rule_ids_json, generation
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                dedup_rows,
            )
        if suppression_rows:
            conn.executemany(
                """
                INSERT INTO suppression_index(
                    fingerprint, service, rule_ids_json, effective_window_ms,
                    alert_count, canonical_count, deduped_count, generation
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                suppression_rows,
            )

    def _rebuild_aggregates(
        self, conn: sqlite3.Connection, *, generation: int, services: set[str]
    ) -> None:
        if not services:
            return
        placeholders = ",".join("?" for _ in services)
        rows = conn.execute(
            f"""
            SELECT a.service, a.severity,
                   COUNT(*) AS raw_count,
                   SUM(d.is_canonical) AS retained_count,
                   COUNT(DISTINCT a.fingerprint) AS fingerprint_count
            FROM alerts a
            JOIN alert_dedup d ON d.alert_uid = a.alert_uid
            WHERE a.service IN ({placeholders})
            GROUP BY a.service, a.severity
            ORDER BY a.service, a.severity
            """,
            sorted(services),
        ).fetchall()
        conn.executemany(
            """
            INSERT INTO aggregate_index(
                service, severity, raw_count, retained_count, deduped_count,
                fingerprint_count, generation
            ) VALUES (?,?,?,?,?,?,?)
            """,
            [
                (
                    row["service"],
                    row["severity"],
                    row["raw_count"],
                    row["retained_count"],
                    row["raw_count"] - row["retained_count"],
                    row["fingerprint_count"],
                    generation,
                )
                for row in rows
            ],
        )

    def _rebuild_root_causes(
        self, conn: sqlite3.Connection, *, generation: int, services: set[str]
    ) -> None:
        if not services:
            return
        adjacency = self._load_adjacency(conn)
        canonical = conn.execute(
            """
            SELECT a.*
            FROM alerts a
            JOIN alert_dedup d ON d.alert_uid = a.alert_uid
            WHERE d.is_canonical = 1
            ORDER BY a.service, a.timestamp_ms, a.alert_uid
            """
        ).fetchall()
        by_service: dict[str, list[sqlite3.Row]] = {}
        for row in canonical:
            by_service.setdefault(row["service"], []).append(row)
        score_rows: list[tuple[Any, ...]] = []
        for source in canonical:
            if source["service"] not in services:
                continue
            distances = shortest_downstream_distances(adjacency, source["service"])
            candidates: list[dict[str, Any]] = []
            for candidate_service, hops in sorted(distances.items()):
                for candidate in by_service.get(candidate_service, ()):
                    if candidate["alert_uid"] == source["alert_uid"]:
                        continue
                    delta_ms = int(source["timestamp_ms"]) - int(candidate["timestamp_ms"])
                    if delta_ms < 0 or delta_ms > DEFAULT_TIME_WINDOW_MS:
                        continue
                    try:
                        scored = score_pair(
                            hops=hops,
                            source_timestamp_ms=int(source["timestamp_ms"]),
                            candidate_timestamp_ms=int(candidate["timestamp_ms"]),
                            time_window_ms=DEFAULT_TIME_WINDOW_MS,
                        )
                    except FixedPointContractError as exc:
                        raise rebuild_conflict(
                            "fixed-point root-cause calculation violated its numeric contract",
                            source_alert_uid=source["alert_uid"],
                            candidate_alert_uid=candidate["alert_uid"],
                            reason=str(exc),
                        ) from exc
                    candidates.append(
                        {
                            "source_alert_uid": source["alert_uid"],
                            "candidate_alert_uid": candidate["alert_uid"],
                            "source_service": source["service"],
                            "source_fingerprint": source["fingerprint"],
                            "source_timestamp_ms": source["timestamp_ms"],
                            "candidate_service": candidate["service"],
                            "candidate_fingerprint": candidate["fingerprint"],
                            "candidate_timestamp_ms": candidate["timestamp_ms"],
                            "hops": hops,
                            "delta_ms": scored.delta_ms,
                            "hop_component": scored.hop_component,
                            "time_component": scored.time_component,
                            "score_fixed": scored.score_fixed,
                        }
                    )
            for row in rank_with_tolerance(candidates, tolerance=SCORE_TOLERANCE):
                score_rows.append(
                    (
                        row["source_alert_uid"],
                        row["candidate_alert_uid"],
                        row["source_service"],
                        row["source_fingerprint"],
                        row["source_timestamp_ms"],
                        row["candidate_service"],
                        row["candidate_fingerprint"],
                        row["candidate_timestamp_ms"],
                        row["hops"],
                        row["delta_ms"],
                        row["hop_component"],
                        row["time_component"],
                        row["score_fixed"],
                        row["rank"],
                        generation,
                    )
                )
        if score_rows:
            conn.executemany(
                """
                INSERT INTO rootcause_scores(
                    source_alert_uid, candidate_alert_uid, source_service,
                    source_fingerprint, source_timestamp_ms, candidate_service,
                    candidate_fingerprint, candidate_timestamp_ms, hops, delta_ms,
                    hop_component, time_component, score_fixed, rank, generation
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                score_rows,
            )

    def _insert_artifact_refs(
        self,
        conn: sqlite3.Connection,
        *,
        generation: int,
        services: set[str],
        fingerprints: set[str],
    ) -> None:
        rows: list[tuple[Any, ...]] = []
        if services:
            placeholders = ",".join("?" for _ in services)
            for row in conn.execute(
                f"SELECT service, severity FROM aggregate_index WHERE service IN ({placeholders})",
                sorted(services),
            ):
                product_key = f"{row['service']}|{row['severity']}"
                rows.append(("aggregate", "service", row["service"], product_key, generation))
            for row in conn.execute(
                f"""
                SELECT DISTINCT a.service, a.severity, a.fingerprint
                FROM alerts a JOIN alert_dedup d ON d.alert_uid=a.alert_uid
                WHERE a.service IN ({placeholders})
                """,
                sorted(services),
            ):
                product_key = f"{row['service']}|{row['severity']}"
                rows.append(("aggregate", "fingerprint", row["fingerprint"], product_key, generation))
            for row in conn.execute(
                f"""
                SELECT source_alert_uid, candidate_alert_uid, source_service,
                       source_fingerprint
                FROM rootcause_scores WHERE source_service IN ({placeholders})
                """,
                sorted(services),
            ):
                product_key = f"{row['source_alert_uid']}->{row['candidate_alert_uid']}"
                rows.append(("rootcause", "service", row["source_service"], product_key, generation))
                rows.append(
                    ("rootcause", "fingerprint", row["source_fingerprint"], product_key, generation)
                )
        if fingerprints:
            placeholders = ",".join("?" for _ in fingerprints)
            for row in conn.execute(
                f"SELECT fingerprint, service FROM suppression_index WHERE fingerprint IN ({placeholders})",
                sorted(fingerprints),
            ):
                rows.append(("suppression", "service", row["service"], row["fingerprint"], generation))
                rows.append(("suppression", "fingerprint", row["fingerprint"], row["fingerprint"], generation))
        if rows:
            conn.executemany(
                """
                INSERT OR REPLACE INTO artifact_refs(
                    artifact, ref_kind, ref_key, product_key, generation
                ) VALUES (?,?,?,?,?)
                """,
                rows,
            )

    @staticmethod
    def _product_counts_for_services(
        conn: sqlite3.Connection, services: set[str]
    ) -> dict[tuple[str, str], int]:
        counts: dict[tuple[str, str], int] = {}
        if not services:
            return counts
        placeholders = ",".join("?" for _ in services)
        for artifact, query in (
            ("aggregate", f"SELECT service, COUNT(*) n FROM aggregate_index WHERE service IN ({placeholders}) GROUP BY service"),
            ("suppression", f"SELECT service, COUNT(*) n FROM suppression_index WHERE service IN ({placeholders}) GROUP BY service"),
            ("rootcause", f"SELECT source_service AS service, COUNT(*) n FROM rootcause_scores WHERE source_service IN ({placeholders}) GROUP BY source_service"),
        ):
            for row in conn.execute(query, sorted(services)):
                counts[(artifact, row["service"])] = int(row["n"])
        return counts

    @staticmethod
    def _product_counts_for_fingerprints(
        conn: sqlite3.Connection, fingerprints: set[str]
    ) -> dict[tuple[str, str], int]:
        counts: dict[tuple[str, str], int] = {}
        if not fingerprints:
            return counts
        placeholders = ",".join("?" for _ in fingerprints)
        for artifact, query in (
            (
                "aggregate",
                f"""
                SELECT d.fingerprint, COUNT(DISTINCT a.service || '|' || a.severity) n
                FROM alert_dedup d JOIN alerts a ON a.alert_uid=d.alert_uid
                WHERE d.fingerprint IN ({placeholders}) GROUP BY d.fingerprint
                """,
            ),
            (
                "suppression",
                f"SELECT fingerprint, COUNT(*) n FROM suppression_index WHERE fingerprint IN ({placeholders}) GROUP BY fingerprint",
            ),
            (
                "rootcause",
                f"SELECT source_fingerprint AS fingerprint, COUNT(*) n FROM rootcause_scores WHERE source_fingerprint IN ({placeholders}) GROUP BY source_fingerprint",
            ),
        ):
            for row in conn.execute(query, sorted(fingerprints)):
                counts[(artifact, row["fingerprint"])] = int(row["n"])
        return counts

    # ------------------------------------------------------------------
    # read-side queries
    # ------------------------------------------------------------------
    def query_state(self) -> dict[str, Any]:
        with self._read_transaction() as conn:
            state = self._snapshot(conn).as_dict()
            state["counts"] = self._counts(conn)
            return state

    @staticmethod
    def _counts(conn: sqlite3.Connection) -> dict[str, int]:
        tables = (
            "alerts",
            "fingerprint_meta",
            "topology_edges",
            "suppression_rules",
            "aggregate_index",
            "suppression_index",
            "alert_dedup",
            "rootcause_scores",
            "artifact_refs",
            "invalidation_events",
            "recovery_events",
        )
        return {
            table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in tables
        }

    def health(self) -> dict[str, Any]:
        import platform
        import sqlite3 as sqlite_module

        try:
            import numpy as np

            numpy_version = np.__version__
        except Exception:  # pragma: no cover - optional diagnostic
            numpy_version = None
        state = self.query_state()
        return {
            "status": "ok",
            "python": platform.python_version(),
            "sqlite": sqlite_module.sqlite_version,
            "numpy": numpy_version,
            "db_path": os.path.abspath(self.db_path),
            "state": state,
        }

    def query_reduction(self, *, include_alerts: bool = False) -> dict[str, Any]:
        with self._read_transaction() as conn:
            snap = self._snapshot(conn)
            rows = conn.execute("SELECT * FROM aggregate_index").fetchall()
            groups = [
                {
                    "service": row["service"],
                    "severity": row["severity"],
                    "raw_count": int(row["raw_count"]),
                    "retained_count": int(row["retained_count"]),
                    "deduped_count": int(row["deduped_count"]),
                    "fingerprint_count": int(row["fingerprint_count"]),
                    "generation": int(row["generation"]),
                }
                for row in rows
            ]
            groups.sort(key=lambda x: (x["service"], SEVERITY_RANK.get(x["severity"], 99), x["severity"]))
            result: dict[str, Any] = {
                "state": snap.as_dict(),
                "groups": groups,
                "totals": {
                    "raw_count": sum(g["raw_count"] for g in groups),
                    "retained_count": sum(g["retained_count"] for g in groups),
                    "deduped_count": sum(g["deduped_count"] for g in groups),
                    "fingerprint_count": sum(g["fingerprint_count"] for g in groups),
                },
            }
            if include_alerts:
                alert_rows = conn.execute(
                    """
                    SELECT a.*, d.canonical_alert_uid, d.is_canonical, d.deduped,
                           d.window_ms, d.rule_ids_json, d.generation AS dedup_generation,
                           m.service AS effective_service, m.labels_json AS effective_labels_json
                    FROM alerts a
                    JOIN alert_dedup d ON d.alert_uid = a.alert_uid
                    LEFT JOIN fingerprint_meta m ON m.fingerprint = a.fingerprint
                    """
                ).fetchall()
                alerts = []
                for row in alert_rows:
                    alerts.append(
                        {
                            "alert_uid": row["alert_uid"],
                            "batch_id": row["batch_id"],
                            "line_no": int(row["line_no"]),
                            "timestamp_ms": int(row["timestamp_ms"]),
                            "fingerprint": row["fingerprint"],
                            "service": row["service"],
                            "resource": row["resource"],
                            "severity": row["severity"],
                            "labels": json.loads(row["effective_labels_json"] or row["labels_json"]),
                            "canonical_alert_uid": row["canonical_alert_uid"],
                            "is_canonical": bool(row["is_canonical"]),
                            "deduped": bool(row["deduped"]),
                            "window_ms": int(row["window_ms"]),
                            "matched_rule_ids": json.loads(row["rule_ids_json"]),
                            "generation": int(row["dedup_generation"]),
                        }
                    )
                alerts.sort(
                    key=lambda x: (
                        x["service"],
                        SEVERITY_RANK.get(x["severity"], 99),
                        x["timestamp_ms"],
                        x["alert_uid"],
                    )
                )
                result["alerts"] = alerts
            return result

    def query_suppression_index(self, fingerprint: str | None = None) -> dict[str, Any]:
        with self._read_transaction() as conn:
            snap = self._snapshot(conn)
            rules, _ = self._load_rules(conn)
            if fingerprint is not None:
                rows = conn.execute(
                    "SELECT * FROM suppression_index WHERE fingerprint=?",
                    (fingerprint,),
                ).fetchall()
                if not rows:
                    raise order_not_found("fingerprint was not found in the suppression index", fingerprint=fingerprint)
            else:
                rows = conn.execute("SELECT * FROM suppression_index ORDER BY fingerprint").fetchall()
            result = []
            for row in rows:
                rule_ids = json.loads(row["rule_ids_json"])
                result.append(
                    {
                        "fingerprint": row["fingerprint"],
                        "service": row["service"],
                        "rule_ids": rule_ids,
                        "rules": [rules[rule_id].payload() for rule_id in rule_ids if rule_id in rules],
                        "effective_window_ms": int(row["effective_window_ms"]),
                        "alert_count": int(row["alert_count"]),
                        "canonical_count": int(row["canonical_count"]),
                        "deduped_count": int(row["deduped_count"]),
                        "generation": int(row["generation"]),
                    }
                )
            return {"state": snap.as_dict(), "rows": result, "row_count": len(result)}

    def query_root_causes(
        self,
        *,
        alert_uid: str | None = None,
        service: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1 or limit > 10_000:
            raise BusinessError("E_SNAPSHOT_INVALID", "limit must be an integer between 1 and 10000")
        with self._read_transaction() as conn:
            snap = self._snapshot(conn)
            source_alert = None
            source_canonical = None
            if alert_uid is not None:
                source_alert = conn.execute(
                    """
                    SELECT a.*, d.is_canonical
                    FROM alerts a LEFT JOIN alert_dedup d ON d.alert_uid=a.alert_uid
                    WHERE a.alert_uid=?
                    """,
                    (alert_uid,),
                ).fetchone()
                if source_alert is None:
                    raise order_not_found("alert_uid was not found", alert_uid=alert_uid)
                source_canonical = bool(source_alert["is_canonical"])
            clauses = []
            params: list[Any] = []
            if alert_uid is not None:
                clauses.append("r.source_alert_uid = ?")
                params.append(alert_uid)
            if service is not None:
                clauses.append("r.source_service = ?")
                params.append(service)
            where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
            params.append(limit)
            rows = conn.execute(
                f"""
                SELECT r.*,
                       s.resource AS source_resource, s.severity AS source_severity,
                       s.service AS source_alert_service,
                       sm.labels_json AS source_labels_json,
                       c.resource AS candidate_resource, c.severity AS candidate_severity,
                       c.service AS candidate_alert_service,
                       cm.labels_json AS candidate_labels_json
                FROM rootcause_scores r
                JOIN alerts s ON s.alert_uid = r.source_alert_uid
                JOIN alerts c ON c.alert_uid = r.candidate_alert_uid
                LEFT JOIN fingerprint_meta sm ON sm.fingerprint = s.fingerprint
                LEFT JOIN fingerprint_meta cm ON cm.fingerprint = c.fingerprint
                {where}
                ORDER BY r.source_service, r.source_timestamp_ms,
                         r.source_alert_uid, r.rank, r.candidate_alert_uid
                LIMIT ?
                """,
                params,
            ).fetchall()
            groups: dict[str, dict[str, Any]] = {}
            for row in rows:
                key = row["source_alert_uid"]
                group = groups.get(key)
                if group is None:
                    group = {
                        "source": {
                            "alert_uid": row["source_alert_uid"],
                            "service": row["source_alert_service"],
                            "fingerprint": row["source_fingerprint"],
                            "timestamp_ms": int(row["source_timestamp_ms"]),
                            "resource": row["source_resource"],
                            "severity": row["source_severity"],
                            "labels": json.loads(row["source_labels_json"] or "{}"),
                        },
                        "candidates": [],
                    }
                    groups[key] = group
                group["candidates"].append(
                    {
                        "rank": int(row["rank"]),
                        "candidate_alert_uid": row["candidate_alert_uid"],
                        "service": row["candidate_service"],
                        "fingerprint": row["candidate_fingerprint"],
                        "timestamp_ms": int(row["candidate_timestamp_ms"]),
                        "resource": row["candidate_resource"],
                        "severity": row["candidate_severity"],
                        "labels": json.loads(row["candidate_labels_json"] or "{}"),
                        "hops": int(row["hops"]),
                        "delta_ms": int(row["delta_ms"]),
                        "hop_component": int(row["hop_component"]),
                        "time_component": int(row["time_component"]),
                        "score_fixed": int(row["score_fixed"]),
                        "score_scale": SCORE_SCALE,
                        "generation": int(row["generation"]),
                    }
                )
            alerts = list(groups.values())
            if source_alert is not None and alert_uid not in groups:
                labels = conn.execute(
                    "SELECT labels_json FROM fingerprint_meta WHERE fingerprint=?",
                    (source_alert["fingerprint"],),
                ).fetchone()
                alerts = [
                    {
                        "source": {
                            "alert_uid": source_alert["alert_uid"],
                            "service": source_alert["service"],
                            "fingerprint": source_alert["fingerprint"],
                            "timestamp_ms": int(source_alert["timestamp_ms"]),
                            "resource": source_alert["resource"],
                            "severity": source_alert["severity"],
                            "labels": json.loads(labels["labels_json"]) if labels else json.loads(source_alert["labels_json"]),
                        },
                        "canonical": source_canonical,
                        "candidates": [],
                    }
                ]
            return {
                "state": snap.as_dict(),
                "alerts": alerts,
                "row_count": sum(len(group["candidates"]) for group in alerts),
                "score_scale": SCORE_SCALE,
                "score_tolerance": SCORE_TOLERANCE,
            }

    def query_invalidations(self, *, limit: int = 100) -> dict[str, Any]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1 or limit > 10_000:
            raise BusinessError("E_SNAPSHOT_INVALID", "limit must be an integer between 1 and 10000")
        with self._read_transaction() as conn:
            snap = self._snapshot(conn)
            rows = conn.execute(
                """
                SELECT e.*, COUNT(t.object_key) AS target_count
                FROM invalidation_events e
                LEFT JOIN invalidation_targets t ON t.event_id=e.id
                GROUP BY e.id
                ORDER BY e.id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            events = []
            for row in rows:
                scope = json.loads(row["scope_json"])
                events.append(
                    {
                        "event_id": int(row["id"]),
                        "commit_seq": int(row["commit_seq"]),
                        "request_id": row["request_id"],
                        "cause": row["cause"],
                        "scope": scope,
                        "target_count": int(row["target_count"]),
                        "created_at": row["created_at"],
                    }
                )
            return {"state": snap.as_dict(), "events": events, "event_count": len(events)}

    def query_invalidation(self, event_id: int) -> dict[str, Any]:
        with self._read_transaction() as conn:
            snap = self._snapshot(conn)
            row = conn.execute("SELECT * FROM invalidation_events WHERE id=?", (event_id,)).fetchone()
            if row is None:
                raise order_not_found("invalidation event was not found", event_id=event_id)
            targets = conn.execute(
                """
                SELECT artifact, object_kind, object_key, state, generation, product_count
                FROM invalidation_targets WHERE event_id=?
                ORDER BY artifact, object_kind, object_key
                """,
                (event_id,),
            ).fetchall()
            return {
                "state": snap.as_dict(),
                "event": {
                    "event_id": int(row["id"]),
                    "commit_seq": int(row["commit_seq"]),
                    "request_id": row["request_id"],
                    "cause": row["cause"],
                    "scope": json.loads(row["scope_json"]),
                    "created_at": row["created_at"],
                },
                "targets": [
                    {
                        "artifact": target["artifact"],
                        "object_kind": target["object_kind"],
                        "object_key": target["object_key"],
                        "state": target["state"],
                        "generation": int(target["generation"]),
                        "product_count": int(target["product_count"]),
                    }
                    for target in targets
                ],
            }

    def query_recovery_report(self) -> dict[str, Any]:
        with self._read_transaction() as conn:
            snap = self._snapshot(conn)
            rows = conn.execute("SELECT * FROM recovery_events ORDER BY id").fetchall()
            pending = int(
                conn.execute("SELECT COUNT(*) FROM intent_log WHERE status='pending'").fetchone()[0]
            )
            events = []
            for row in rows:
                events.append(
                    {
                        "recovery_id": int(row["id"]),
                        "intent_id": row["intent_id"],
                        "outcome": row["outcome"],
                        "details": json.loads(row["details_json"]),
                        "created_at": row["created_at"],
                    }
                )
            return {
                "state": snap.as_dict(),
                "recovered_intents": events,
                "recovered_count": len(events),
                "pending_intent_count": pending,
                "facts_only": True,
                "disclaimer": (
                    "This report is reconstructed only from committed SQLite facts and intent_log rows. "
                    "A forced process termination does not promise a graceful shutdown report; it does promise "
                    "that an uncommitted cascade transaction is not visible after restart."
                ),
            }

    def query_batch(self, batch_id: str) -> dict[str, Any]:
        with self._read_transaction() as conn:
            snap = self._snapshot(conn)
            row = conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if row is None:
                raise order_not_found("batch_id was not found", batch_id=batch_id)
            return {
                "state": snap.as_dict(),
                "batch": {
                    "batch_id": row["batch_id"],
                    "version": int(row["version"]),
                    "checksum": row["checksum"],
                    "commit_seq": int(row["commit_seq"]),
                    "result": json.loads(row["result_json"]),
                    "created_at": row["created_at"],
                },
            }

    def query_product_digest(self) -> str:
        """Digest committed product facts; intended for recovery consistency checks."""
        with self._read_transaction() as conn:
            payload = {
                "state": self._snapshot(conn).as_dict(),
                "aggregate": [
                    list(row)
                    for row in conn.execute(
                        "SELECT service,severity,raw_count,retained_count,deduped_count,fingerprint_count,generation FROM aggregate_index ORDER BY service,severity"
                    )
                ],
                "suppression": [
                    list(row)
                    for row in conn.execute(
                        "SELECT fingerprint,service,rule_ids_json,effective_window_ms,alert_count,canonical_count,deduped_count,generation FROM suppression_index ORDER BY fingerprint"
                    )
                ],
                "dedup": [
                    list(row)
                    for row in conn.execute(
                        "SELECT alert_uid,canonical_alert_uid,is_canonical,deduped,window_ms,rule_ids_json,generation FROM alert_dedup ORDER BY alert_uid"
                    )
                ],
                "rootcause": [
                    list(row)
                    for row in conn.execute(
                        "SELECT source_alert_uid,candidate_alert_uid,hops,delta_ms,hop_component,time_component,score_fixed,rank,generation FROM rootcause_scores ORDER BY source_alert_uid,rank,candidate_alert_uid"
                    )
                ],
            }
            return digest(payload)
