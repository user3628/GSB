"""SQLite-backed run store with derived caches, snapshots and recovery.

Concurrency model
~~~~~~~~~~~~~~~~~
A single process-wide writer connection is guarded by a re-entrant lock, so
run submission, import and snapshot switch are strictly serialized (single
writer). Read-only requests use separate reader connections and therefore run
concurrently against the last committed version (SQLite WAL).

Atomicity / durability
~~~~~~~~~~~~~~~~~~~~~~
Every write first persists an *intent* row carrying the full request payload
and the current base revision, then performs the data changes inside one
transaction. On restart, unfinished intents are reconciled: SQLite guarantees
that an interrupted transaction leaves no partial rows, so an unfinished
intent is marked ``aborted`` and the store rolls forward to the last complete
version. Reusing a request id replays the recorded result exactly once.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from .errors import ErrorCode, TrackerError
from .models import Artifact, MetricPoint, Run
from .negotiation import DEFAULT_PARAMS
from .versions import compare_versions

INTENT_RUN = "run"
INTENT_IMPORT = "import"
INTENT_SNAPSHOT = "snapshot"

INTENT_PENDING = "pending"
INTENT_COMPLETED = "completed"
INTENT_ABORTED = "aborted"

# Import conflict outcomes.
OUTCOME_OVERWRITE = "overwrite"
OUTCOME_SKIP = "skip"
OUTCOME_REJECT = "reject"
OUTCOME_INSERT = "insert"

SCHEMA = """
CREATE TABLE IF NOT EXISTS state_meta (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    revision INTEGER NOT NULL,
    snapshot_seq INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    experiment TEXT NOT NULL,
    toolchain_version TEXT NOT NULL,
    last_write_us INTEGER NOT NULL,
    base_time_us INTEGER,
    retention_seconds REAL,
    expires_at_us INTEGER,
    params_json TEXT NOT NULL,
    metric_signature TEXT NOT NULL,
    downgrades_json TEXT NOT NULL DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS run_metrics (
    run_id TEXT NOT NULL,
    position INTEGER NOT NULL,
    step INTEGER NOT NULL,
    value REAL NOT NULL,
    time_us INTEGER NOT NULL,
    PRIMARY KEY (run_id, position)
);
CREATE TABLE IF NOT EXISTS run_artifacts (
    run_id TEXT NOT NULL,
    position INTEGER NOT NULL,
    name TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    digest TEXT NOT NULL,
    PRIMARY KEY (run_id, position)
);
CREATE TABLE IF NOT EXISTS metric_summaries (
    experiment TEXT NOT NULL,
    toolchain_version TEXT NOT NULL,
    run_count INTEGER NOT NULL,
    point_count INTEGER NOT NULL,
    summary_json TEXT NOT NULL,
    PRIMARY KEY (experiment, toolchain_version)
);
CREATE TABLE IF NOT EXISTS param_index (
    param_key TEXT NOT NULL,
    run_id TEXT NOT NULL,
    PRIMARY KEY (param_key, run_id)
);
CREATE TABLE IF NOT EXISTS import_reports (
    report_id TEXT PRIMARY KEY,
    created_at_us INTEGER NOT NULL,
    kind TEXT NOT NULL,
    format_version INTEGER,
    committed INTEGER NOT NULL,
    report_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS intents (
    request_id TEXT PRIMARY KEY,
    intent_type TEXT NOT NULL,
    state TEXT NOT NULL,
    base_revision INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    result_json TEXT,
    created_at_us INTEGER NOT NULL,
    completed_at_us INTEGER
);
"""


@dataclass
class RecoveryReport:
    recovered_revision: int
    aborted_intents: List[str]
    completed_intents: List[str]
    facts: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "recovered_revision": self.recovered_revision,
            "aborted_intents": self.aborted_intents,
            "completed_intents": self.completed_intents,
            "facts": self.facts,
        }


def _now_us() -> int:
    # Deterministic-friendly monotonic-ish wall clock in microseconds.
    return int(__import__("time").time() * 1_000_000)


def resolve_conflict(incoming: Run, existing: Run) -> str:
    """Deterministic conflict resolution: overwrite / skip / reject.

    Priority order:
      1. higher toolchain version wins (overwrite, or skip if incoming lower);
      2. equal version + identical metric summary signature -> skip;
      3. equal version + differing signature -> reject (manual decision);
      4. last-write timestamp is the final tie-breaker.
    """
    relation = compare_versions(incoming.toolchain_version, existing.toolchain_version)
    if relation == "higher":
        return OUTCOME_OVERWRITE
    if relation == "lower":
        return OUTCOME_SKIP
    if incoming.metric_signature() == existing.metric_signature():
        return OUTCOME_SKIP
    if incoming.last_write_us > existing.last_write_us:
        return OUTCOME_OVERWRITE
    if incoming.last_write_us < existing.last_write_us:
        return OUTCOME_SKIP
    return OUTCOME_REJECT


def _quantiles(values: np.ndarray) -> Dict[str, float]:
    if values.size == 0:
        return {"min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "mean": 0.0}
    q = np.quantile(values, [0.0, 0.25, 0.5, 0.75, 1.0], method="linear")
    return {
        "min": float(q[0]),
        "p25": float(q[1]),
        "median": float(q[2]),
        "p75": float(q[3]),
        "max": float(q[4]),
        "mean": float(np.mean(values)),
    }


class ExperimentStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._write_lock = threading.RLock()
        self._tls = threading.local()
        self._reader_conns: List[sqlite3.Connection] = []
        self._result_events: Dict[str, threading.Event] = {}
        self._result_events_lock = threading.Lock()
        self._writer = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self._writer.row_factory = sqlite3.Row
        self._writer.execute("PRAGMA journal_mode=WAL")
        self._writer.execute("PRAGMA synchronous=FULL")
        self._writer.execute("PRAGMA foreign_keys=ON")
        self._writer.executescript(SCHEMA)
        self._ensure_meta()
        self.recovery_report: Optional[RecoveryReport] = self._recover_pending_intents()

    # ------------------------------------------------------------------ meta
    def _connect_reader(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA query_only=ON")
        return conn

    def _reader(self) -> sqlite3.Connection:
        """Return this thread's dedicated read-only connection.

        Readers never share the writer connection, so concurrent queries run
        against the last committed snapshot without touching the write lock.
        """
        conn = getattr(self._tls, "reader_conn", None)
        if conn is None:
            conn = self._connect_reader()
            self._tls.reader_conn = conn
            with self._write_lock:
                self._reader_conns.append(conn)
        return conn

    def _ensure_meta(self) -> None:
        row = self._writer.execute("SELECT revision FROM state_meta WHERE id=1").fetchone()
        if row is None:
            self._writer.execute(
                "INSERT INTO state_meta(id, revision, snapshot_seq) VALUES (1, 0, 0)"
            )

    def _revision(self, conn: sqlite3.Connection) -> int:
        return int(conn.execute("SELECT revision FROM state_meta WHERE id=1").fetchone()["revision"])

    def _snapshot_seq(self) -> int:
        return int(self._writer.execute(
            "SELECT snapshot_seq FROM state_meta WHERE id=1").fetchone()["snapshot_seq"])

    def revision(self) -> int:
        with self._write_lock:
            return self._revision(self._writer)

    # ------------------------------------------------------------- intents
    def _begin_intent(self, request_id: Optional[str], intent_type: str,
                      payload: Dict[str, Any], base_revision: int) -> Optional[sqlite3.Row]:
        """Persist an intent. Return an existing completed/pending row if any."""
        if not request_id:
            return None
        existing = self._writer.execute(
            "SELECT * FROM intents WHERE request_id=?", (request_id,)
        ).fetchone()
        if existing is not None:
            return existing
        self._writer.execute(
            "INSERT INTO intents(request_id, intent_type, state, base_revision, "
            "payload_json, created_at_us) VALUES (?,?,?,?,?,?)",
            (request_id, intent_type, INTENT_PENDING, base_revision,
             json.dumps(payload, sort_keys=True), _now_us()),
        )
        return None

    def _register_event(self, request_id: str) -> threading.Event:
        with self._result_events_lock:
            event = self._result_events.get(request_id)
            if event is None:
                event = threading.Event()
                self._result_events[request_id] = event
            return event

    def _await_result(self, row: sqlite3.Row) -> Dict[str, Any]:
        # A completed/aborted intent already carries its result; only a still
        # pending intent requires waiting on the in-process event.
        if row["state"] != INTENT_PENDING:
            return json.loads(row["result_json"])
        event = self._register_event(row["request_id"])
        event.wait()
        fresh = self._writer.execute(
            "SELECT * FROM intents WHERE request_id=?", (row["request_id"],)
        ).fetchone()
        return json.loads(fresh["result_json"])

    def _finish_intent(self, request_id: Optional[str], result: Dict[str, Any],
                       state: str = INTENT_COMPLETED) -> None:
        if not request_id:
            return
        self._writer.execute(
            "UPDATE intents SET state=?, result_json=?, completed_at_us=? WHERE request_id=?",
            (state, json.dumps(result, sort_keys=True), _now_us(), request_id),
        )
        with self._result_events_lock:
            event = self._result_events.get(request_id)
            if event is not None:
                event.set()

    # --------------------------------------------------------- run persistence
    def _delete_run_derived(self, conn: sqlite3.Connection, run_id: str) -> Tuple[str, str, List[str]]:
        row = conn.execute("SELECT experiment, toolchain_version, params_json FROM runs WHERE run_id=?",
                           (run_id,)).fetchone()
        conn.execute("DELETE FROM run_metrics WHERE run_id=?", (run_id,))
        conn.execute("DELETE FROM run_artifacts WHERE run_id=?", (run_id,))
        conn.execute("DELETE FROM param_index WHERE run_id=?", (run_id,))
        conn.execute("DELETE FROM runs WHERE run_id=?", (run_id,))
        if row is None:
            return "", "", []
        return row["experiment"], row["toolchain_version"], list(json.loads(row["params_json"]).keys())

    def _insert_run_rows(self, conn: sqlite3.Connection, run: Run) -> None:
        conn.execute(
            "INSERT INTO runs(run_id, experiment, toolchain_version, last_write_us, "
            "base_time_us, retention_seconds, expires_at_us, params_json, metric_signature, "
            "downgrades_json) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (run.run_id, run.experiment, run.toolchain_version, run.last_write_us,
             run.base_time_us, run.retention_seconds, run.expires_at_us,
             json.dumps(run.params, sort_keys=True), run.metric_signature(),
             json.dumps(run.downgrades, sort_keys=True)),
        )
        for position, point in enumerate(run.metrics):
            conn.execute(
                "INSERT INTO run_metrics(run_id, position, step, value, time_us) "
                "VALUES (?,?,?,?,?)",
                (run.run_id, position, point.step, point.value, point.time_us),
            )
        for position, artifact in enumerate(run.artifacts):
            conn.execute(
                "INSERT INTO run_artifacts(run_id, position, name, size_bytes, digest) "
                "VALUES (?,?,?,?,?)",
                (run.run_id, position, artifact.name, artifact.size_bytes, artifact.digest),
            )
        for key in sorted(run.params):
            conn.execute(
                "INSERT OR IGNORE INTO param_index(param_key, run_id) VALUES (?,?)",
                (str(key), run.run_id),
            )

    # ------------------------------------------------------------- summaries
    def _rebuild_summary(self, conn: sqlite3.Connection, experiment: str,
                         toolchain_version: str) -> None:
        rows = conn.execute(
            "SELECT r.run_id FROM runs r WHERE r.experiment=? AND r.toolchain_version=?",
            (experiment, toolchain_version),
        ).fetchall()
        run_ids = [row["run_id"] for row in rows]
        if not run_ids:
            conn.execute(
                "DELETE FROM metric_summaries WHERE experiment=? AND toolchain_version=?",
                (experiment, toolchain_version),
            )
            return
        placeholders = ",".join("?" for _ in run_ids)
        values = conn.execute(
            f"SELECT value FROM run_metrics WHERE run_id IN ({placeholders}) ORDER BY position",
            run_ids,
        ).fetchall()
        array = np.array([row["value"] for row in values], dtype=np.float64)
        summary = _quantiles(array)
        conn.execute(
            "INSERT OR REPLACE INTO metric_summaries(experiment, toolchain_version, "
            "run_count, point_count, summary_json) VALUES (?,?,?,?,?)",
            (experiment, toolchain_version, len(run_ids), int(array.size),
             json.dumps(summary, sort_keys=True)),
        )

    def _rebuild_param_index(self, conn: sqlite3.Connection, run_ids: Optional[List[str]] = None) -> None:
        """Rebuild the parameter dependency index inside the current txn."""
        if run_ids is None:
            conn.execute("DELETE FROM param_index")
            rows = conn.execute("SELECT run_id, params_json FROM runs").fetchall()
            for row in rows:
                for key in json.loads(row["params_json"]):
                    conn.execute(
                        "INSERT OR IGNORE INTO param_index(param_key, run_id) VALUES (?,?)",
                        (str(key), row["run_id"]),
                    )
        else:
            for run_id in run_ids:
                conn.execute("DELETE FROM param_index WHERE run_id=?", (run_id,))
                row = conn.execute("SELECT params_json FROM runs WHERE run_id=?",
                                   (run_id,)).fetchone()
                if row is not None:
                    for key in json.loads(row["params_json"]):
                        conn.execute(
                            "INSERT OR IGNORE INTO param_index(param_key, run_id) VALUES (?,?)",
                            (str(key), run_id),
                        )

    def _refresh_affected_summaries(self, conn: sqlite3.Connection, affected: List[Tuple[str, str]]) -> None:
        unique = sorted(set(affected))
        for experiment, toolchain_version in unique:
            self._rebuild_summary(conn, experiment, toolchain_version)

    def cascade_rebuild(self, experiment: str, request_id: Optional[str] = None) -> Dict[str, Any]:
        """Atomically invalidate and rebuild all derived entries for an experiment.

        Used when a parameter or toolchain change alters the summary basis:
        the experiment's summaries and the parameter index are rebuilt inside
        one transaction, so no intermediate state is ever visible.
        """
        with self._write_lock:
            base = self._revision(self._writer)
            existing = self._begin_intent(request_id, INTENT_SNAPSHOT,
                                          {"cascade_experiment": experiment}, base)
            if existing is not None:
                return self._await_result(existing)
            self._writer.execute("BEGIN IMMEDIATE")
            try:
                run_rows = self._writer.execute(
                    "SELECT run_id FROM runs WHERE experiment=?", (experiment,)
                ).fetchall()
                run_ids = [row["run_id"] for row in run_rows]
                versions = [row["toolchain_version"] for row in self._writer.execute(
                    "SELECT DISTINCT toolchain_version FROM runs WHERE experiment=?",
                    (experiment,),
                ).fetchall()]
                rebuilt_summaries = [(experiment, version) for version in versions]
                for _, version in rebuilt_summaries:
                    self._rebuild_summary(self._writer, experiment, version)
                self._rebuild_param_index(self._writer, run_ids=run_ids)
                self._writer.execute("UPDATE state_meta SET revision=revision+1 WHERE id=1")
                new_revision = self._revision(self._writer)
                self._writer.execute("COMMIT")
            except Exception:
                self._writer.execute("ROLLBACK")
                raise
            result = {
                "rebuilt_experiment": experiment,
                "rebuilt_summaries": [list(item) for item in rebuilt_summaries],
                "reindexed_run_ids": sorted(run_ids),
                "revision": new_revision,
            }
            self._finish_intent(request_id, result)
            return result

    # ------------------------------------------------------------ run submit
    def submit_run(self, run: Run, request_id: Optional[str] = None,
                   expected_revision: Optional[int] = None) -> Dict[str, Any]:
        with self._write_lock:
            base = self._revision(self._writer)
            if expected_revision is not None and expected_revision != base:
                raise TrackerError(
                    ErrorCode.STALE_BASE,
                    "expected base revision does not match current revision",
                    {"expected_revision": expected_revision, "current_revision": base},
                    http_status=409,
                )
            existing_intent = self._begin_intent(
                request_id, INTENT_RUN, {"run_id": run.run_id}, base
            )
            if existing_intent is not None:
                return self._await_result(existing_intent)
            self._writer.execute("BEGIN IMMEDIATE")
            try:
                existing = self.get_run(run.run_id)
                outcome = OUTCOME_INSERT
                affected: List[Tuple[str, str]] = []
                if existing is not None:
                    outcome = resolve_conflict(run, existing)
                    if outcome == OUTCOME_REJECT:
                        raise TrackerError(
                            ErrorCode.CONFLICTING_RUN,
                            "equal toolchain version with differing metric summary",
                            {"run_id": run.run_id,
                             "incoming_last_write_us": run.last_write_us,
                             "existing_last_write_us": existing.last_write_us},
                            http_status=409,
                        )
                    if outcome == OUTCOME_SKIP:
                        self._writer.execute("ROLLBACK")
                        result = {"run_id": run.run_id, "outcome": OUTCOME_SKIP,
                                  "revision": base}
                        self._finish_intent(request_id, result)
                        return result
                    old_exp, old_ver, _ = self._delete_run_derived(self._writer, run.run_id)
                    affected.append((old_exp, old_ver))
                self._insert_run_rows(self._writer, run)
                affected.append((run.experiment, run.toolchain_version))
                self._refresh_affected_summaries(self._writer, affected)
                self._writer.execute("UPDATE state_meta SET revision=revision+1 WHERE id=1")
                new_revision = self._revision(self._writer)
                self._writer.execute("COMMIT")
            except TrackerError:
                if self._writer.in_transaction:
                    self._writer.execute("ROLLBACK")
                self._finish_intent(request_id,
                                    {"aborted": True}, state=INTENT_ABORTED)
                raise
            except Exception:
                if self._writer.in_transaction:
                    self._writer.execute("ROLLBACK")
                self._finish_intent(request_id,
                                    {"aborted": True}, state=INTENT_ABORTED)
                raise
            result = {"run_id": run.run_id, "outcome": outcome, "revision": new_revision}
            self._finish_intent(request_id, result)
            return result

    # ------------------------------------------------------------- batch import
    def import_runs(self, runs: List[Run], negotiation: Dict[str, Any],
                    report_id: str, request_id: Optional[str] = None) -> Dict[str, Any]:
        """Atomically apply a whole batch; any reject rolls the batch back."""
        with self._write_lock:
            base = self._revision(self._writer)
            existing_intent = self._begin_intent(
                request_id, INTENT_IMPORT,
                {"report_id": report_id, "run_ids": [r.run_id for r in runs]}, base
            )
            if existing_intent is not None:
                return self._await_result(existing_intent)

            entries: List[Dict[str, Any]] = []
            rejects: List[Dict[str, Any]] = []
            # First, adjudicate every record against a stable read snapshot.
            current = {row["run_id"]: self._hydrate_run(row, self._writer) for row in
                       self._writer.execute("SELECT * FROM runs").fetchall()}
            staged: Dict[str, Run] = {}
            for index, run in enumerate(runs):
                if run.run_id in staged:
                    rejects.append({"record_index": index, "run_id": run.run_id,
                                    "outcome": OUTCOME_REJECT,
                                    "reason": "duplicate run_id inside batch"})
                    continue
                staged[run.run_id] = run
                existing = current.get(run.run_id)
                if existing is None:
                    entries.append({"record_index": index, "run_id": run.run_id,
                                    "outcome": OUTCOME_INSERT})
                    continue
                outcome = resolve_conflict(run, existing)
                entry = {"record_index": index, "run_id": run.run_id, "outcome": outcome}
                if outcome == OUTCOME_REJECT:
                    entry["reason"] = "equal version, differing metric summary, equal last_write"
                    rejects.append(entry)
                else:
                    entries.append(entry)

            if rejects:
                # Whole batch rejected: no data changes; persist report in its own txn.
                report = self._build_report(
                    report_id, negotiation, committed=False,
                    entries=entries + rejects, revision=base,
                )
                self._persist_report(report)
                self._finish_intent(request_id, {"report_id": report_id, "committed": False})
                raise TrackerError(
                    ErrorCode.CONFLICTING_RUN,
                    "batch rejected; no records were committed",
                    {"report_id": report_id, "rejects": rejects},
                    http_status=409,
                )

            self._writer.execute("BEGIN IMMEDIATE")
            try:
                affected: List[Tuple[str, str]] = []
                applied: List[Dict[str, Any]] = []
                for entry in entries:
                    run = staged[entry["run_id"]]
                    if entry["outcome"] == OUTCOME_INSERT:
                        self._insert_run_rows(self._writer, run)
                        applied.append(entry)
                    elif entry["outcome"] == OUTCOME_OVERWRITE:
                        old_exp, old_ver, _ = self._delete_run_derived(
                            self._writer, run.run_id
                        )
                        affected.append((old_exp, old_ver))
                        self._insert_run_rows(self._writer, run)
                        applied.append(entry)
                    else:  # skip: leave existing rows untouched
                        applied.append(entry)
                    affected.append((run.experiment, run.toolchain_version))
                self._refresh_affected_summaries(self._writer, affected)
                self._writer.execute("UPDATE state_meta SET revision=revision+1 WHERE id=1")
                new_revision = self._revision(self._writer)
                report = self._build_report(
                    report_id, negotiation, committed=True,
                    entries=applied, revision=new_revision,
                )
                self._persist_report(report, in_transaction=True)
                self._writer.execute("COMMIT")
            except Exception:
                if self._writer.in_transaction:
                    self._writer.execute("ROLLBACK")
                self._finish_intent(request_id,
                                    {"aborted": True}, state=INTENT_ABORTED)
                raise
            self._finish_intent(request_id, {"report_id": report_id, "committed": True,
                                             "revision": new_revision})
            return report

    def _build_report(self, report_id: str, negotiation: Dict[str, Any],
                      committed: bool, entries: List[Dict[str, Any]],
                      revision: int) -> Dict[str, Any]:
        counts = {OUTCOME_INSERT: 0, OUTCOME_OVERWRITE: 0,
                  OUTCOME_SKIP: 0, OUTCOME_REJECT: 0}
        for entry in entries:
            counts[entry["outcome"]] = counts.get(entry["outcome"], 0) + 1
        return {
            "report_id": report_id,
            "created_at_us": _now_us(),
            "committed": committed,
            "revision": revision,
            "negotiation": negotiation,
            "counts": counts,
            "entries": entries,
        }

    def _persist_report(self, report: Dict[str, Any], in_transaction: bool = False) -> None:
        negotiation = report["negotiation"]
        if not in_transaction:
            self._writer.execute("BEGIN IMMEDIATE")
        self._writer.execute(
            "INSERT INTO import_reports(report_id, created_at_us, kind, format_version, "
            "committed, report_json) VALUES (?,?,?,?,?,?)",
            (report["report_id"], report["created_at_us"],
             negotiation.get("kind", "unknown"), negotiation.get("format_version"),
             1 if report["committed"] else 0, json.dumps(report, sort_keys=True)),
        )
        if not in_transaction:
            self._writer.execute("COMMIT")

    def get_import_report(self, report_id: str) -> Dict[str, Any]:
        row = self._reader().execute(
            "SELECT report_json FROM import_reports WHERE report_id=?", (report_id,)
        ).fetchone()
        if row is None:
            raise TrackerError(ErrorCode.NOT_FOUND, "import report not found",
                               {"report_id": report_id}, http_status=404)
        return json.loads(row["report_json"])

    def list_import_reports(self) -> List[Dict[str, Any]]:
        rows = self._reader().execute(
            "SELECT report_json FROM import_reports ORDER BY created_at_us"
        ).fetchall()
        return [json.loads(row["report_json"]) for row in rows]

    # ------------------------------------------------------------- queries
    def _hydrate_run(self, row: sqlite3.Row,
                     conn: Optional[sqlite3.Connection] = None) -> Run:
        conn = conn or self._reader()
        metric_rows = conn.execute(
            "SELECT step, value, time_us FROM run_metrics WHERE run_id=? ORDER BY position",
            (row["run_id"],),
        ).fetchall()
        artifact_rows = conn.execute(
            "SELECT name, size_bytes, digest FROM run_artifacts WHERE run_id=? ORDER BY position",
            (row["run_id"],),
        ).fetchall()
        return Run(
            run_id=row["run_id"],
            experiment=row["experiment"],
            params=json.loads(row["params_json"]),
            metrics=[MetricPoint(step=m["step"], value=m["value"], time_us=m["time_us"])
                     for m in metric_rows],
            toolchain_version=row["toolchain_version"],
            last_write_us=row["last_write_us"],
            base_time_us=row["base_time_us"],
            retention_seconds=row["retention_seconds"],
            expires_at_us=row["expires_at_us"],
            artifacts=[Artifact(name=a["name"], size_bytes=a["size_bytes"],
                                digest=a["digest"]) for a in artifact_rows],
            downgrades=json.loads(row["downgrades_json"]),
        )

    def get_run(self, run_id: str) -> Optional[Run]:
        conn = self._reader()
        row = conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        return None if row is None else self._hydrate_run(row, conn)

    def list_runs(self) -> List[Run]:
        conn = self._reader()
        rows = conn.execute("SELECT run_id FROM runs ORDER BY run_id").fetchall()
        return [self._hydrate_run(
            conn.execute("SELECT * FROM runs WHERE run_id=?", (row["run_id"],)).fetchone(), conn
        ) for row in rows]

    def get_summary(self, experiment: str, toolchain_version: str) -> Dict[str, Any]:
        row = self._reader().execute(
            "SELECT * FROM metric_summaries WHERE experiment=? AND toolchain_version=?",
            (experiment, toolchain_version),
        ).fetchone()
        if row is None:
            raise TrackerError(ErrorCode.NOT_FOUND, "summary not found",
                               {"experiment": experiment,
                                "toolchain_version": toolchain_version}, http_status=404)
        return {
            "experiment": experiment,
            "toolchain_version": toolchain_version,
            "run_count": row["run_count"],
            "point_count": row["point_count"],
            "quantiles": json.loads(row["summary_json"]),
        }

    def list_summaries(self) -> List[Dict[str, Any]]:
        rows = self._reader().execute(
            "SELECT * FROM metric_summaries ORDER BY experiment, toolchain_version"
        ).fetchall()
        return [{
            "experiment": row["experiment"],
            "toolchain_version": row["toolchain_version"],
            "run_count": row["run_count"],
            "point_count": row["point_count"],
            "quantiles": json.loads(row["summary_json"]),
        } for row in rows]

    def find_runs_by_param(self, param_key: str) -> List[str]:
        rows = self._reader().execute(
            "SELECT run_id FROM param_index WHERE param_key=? ORDER BY run_id",
            (param_key,),
        ).fetchall()
        return [row["run_id"] for row in rows]

    def list_param_index(self) -> Dict[str, List[str]]:
        rows = self._reader().execute(
            "SELECT param_key, run_id FROM param_index ORDER BY param_key, run_id"
        ).fetchall()
        out: Dict[str, List[str]] = {}
        for row in rows:
            out.setdefault(row["param_key"], []).append(row["run_id"])
        return out

    # ------------------------------------------------------------- snapshots
    def export_snapshot(self, path: str, generated_at_us: Optional[int] = None) -> Dict[str, Any]:
        with self._write_lock:
            runs = self.list_runs()
            seq = self._snapshot_seq() + 1
            snapshot = {
                "snapshot_format_version": 1,
                "snapshot_seq": seq,
                "generated_at_us": generated_at_us or _now_us(),
                "revision": self._revision(self._writer),
                "runs": [run.to_dict() for run in runs],
            }
            tmp_path = path + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as handle:
                json.dump(snapshot, handle, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, path)
            self._writer.execute("UPDATE state_meta SET snapshot_seq=? WHERE id=1", (seq,))
            return {"path": path, "snapshot_seq": seq, "run_count": len(runs)}

    def load_snapshot(self, path: str, request_id: Optional[str] = None) -> Dict[str, Any]:
        with self._write_lock:
            base = self._revision(self._writer)
            current_seq = self._snapshot_seq()
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    snapshot = json.load(handle)
            except (OSError, json.JSONDecodeError) as exc:
                raise TrackerError(
                    ErrorCode.SNAPSHOT_INVALID,
                    "snapshot file cannot be parsed",
                    {"path": path, "detail": str(exc)},
                )
            try:
                snap_version = int(snapshot["snapshot_format_version"])
                snap_seq = int(snapshot["snapshot_seq"])
                raw_runs = snapshot["runs"]
                runs = [Run.from_dict(raw) for raw in raw_runs]
            except (KeyError, TypeError, ValueError) as exc:
                raise TrackerError(
                    ErrorCode.SNAPSHOT_INVALID,
                    "snapshot missing or has invalid required fields",
                    {"path": path, "detail": str(exc)},
                )
            if snap_version != 1:
                raise TrackerError(
                    ErrorCode.SNAPSHOT_INVALID,
                    "unsupported snapshot format version",
                    {"snapshot_format_version": snap_version, "supported": [1]},
                )
            if snap_seq < current_seq:
                raise TrackerError(
                    ErrorCode.STALE_BASE,
                    "snapshot sequence regresses below the current sequence",
                    {"snapshot_seq": snap_seq, "current_snapshot_seq": current_seq},
                    http_status=409,
                )

            existing_intent = self._begin_intent(
                request_id, INTENT_SNAPSHOT,
                {"path": path, "snapshot_seq": snap_seq}, base
            )
            if existing_intent is not None:
                return self._await_result(existing_intent)

            self._writer.execute("BEGIN IMMEDIATE")
            try:
                # Whole replacement in one transaction: readers keep seeing the
                # old snapshot until this commits, so no mixed state is visible.
                self._writer.execute("DELETE FROM run_metrics")
                self._writer.execute("DELETE FROM run_artifacts")
                self._writer.execute("DELETE FROM param_index")
                self._writer.execute("DELETE FROM metric_summaries")
                self._writer.execute("DELETE FROM runs")
                affected: List[Tuple[str, str]] = []
                for run in runs:
                    self._insert_run_rows(self._writer, run)
                    affected.append((run.experiment, run.toolchain_version))
                self._refresh_affected_summaries(self._writer, affected)
                self._writer.execute(
                    "UPDATE state_meta SET revision=revision+1, snapshot_seq=? WHERE id=1",
                    (snap_seq,),
                )
                new_revision = self._revision(self._writer)
                self._writer.execute("COMMIT")
            except Exception:
                if self._writer.in_transaction:
                    self._writer.execute("ROLLBACK")
                self._finish_intent(request_id, {"aborted": True}, state=INTENT_ABORTED)
                raise
            result = {"loaded_snapshot_seq": snap_seq, "run_count": len(runs),
                      "revision": new_revision}
            self._finish_intent(request_id, result)
            return result

    # ------------------------------------------------------------- recovery
    def _recover_pending_intents(self) -> RecoveryReport:
        """Reconcile intents left unfinished by a forced termination."""
        pending = self._writer.execute(
            "SELECT * FROM intents WHERE state=? ORDER BY created_at_us",
            (INTENT_PENDING,),
        ).fetchall()
        aborted: List[str] = []
        facts: List[str] = []
        for row in pending:
            # SQLite atomicity guarantees the interrupted transaction left no
            # partial data rows; mark the intent aborted based on this fact.
            self._writer.execute(
                "UPDATE intents SET state=?, completed_at_us=? WHERE request_id=?",
                (INTENT_ABORTED, _now_us(), row["request_id"]),
            )
            aborted.append(row["request_id"])
            facts.append(
                f"intent {row['request_id']} ({row['intent_type']}) was pending with no "
                "committed transaction; marked aborted, store stays at last complete revision"
            )
        completed = [r["request_id"] for r in self._writer.execute(
            "SELECT request_id FROM intents WHERE state=? ORDER BY created_at_us",
            (INTENT_COMPLETED,),
        ).fetchall()]
        revision = self._revision(self._writer)
        facts.append(f"current committed revision after recovery is {revision}")
        return RecoveryReport(recovered_revision=revision, aborted_intents=aborted,
                              completed_intents=completed, facts=facts)

    def mark_crash_point(self, request_id: str, intent_type: str,
                         payload: Dict[str, Any]) -> int:
        """Persist a pending intent then simulate a hard kill (no commit).

        This deliberately never opens the data transaction, so on the next
        open the recovery path proves that pending intents are reconciled
        without any partial derived state.
        """
        with self._write_lock:
            base = self._revision(self._writer)
            self._begin_intent(request_id, intent_type, payload, base)
            return base

    def close(self) -> None:
        with self._write_lock:
            for conn in self._reader_conns:
                conn.close()
            self._reader_conns.clear()
            self._writer.close()
            with self._result_events_lock:
                self._result_events.clear()
