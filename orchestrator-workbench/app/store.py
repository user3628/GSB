"""SQLite persistence with intent logging and atomic snapshot switching.

All mutable state lives behind a single writer lock. SQLite WAL mode lets read
transactions observe a fully committed snapshot while a writer transaction is
in flight, so readers never see a half-updated resource/dependency/layer mix.
"""
from __future__ import annotations

import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from app.models import LayerBatch, NormalizedResource, RecoveryIntent, RecoveryReport, SnapshotStats

_SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_seq INTEGER PRIMARY KEY,
    format TEXT NOT NULL,
    format_version INTEGER NOT NULL,
    generated_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS resources (
    snapshot_seq INTEGER NOT NULL,
    resource_id TEXT NOT NULL,
    budget INTEGER NOT NULL,
    weight_permille INTEGER NOT NULL,
    expected_version INTEGER NOT NULL,
    layer INTEGER NOT NULL,
    batch INTEGER NOT NULL,
    PRIMARY KEY (snapshot_seq, resource_id)
);
CREATE TABLE IF NOT EXISTS dependencies (
    snapshot_seq INTEGER NOT NULL,
    resource_id TEXT NOT NULL,
    parent_id TEXT NOT NULL,
    position INTEGER NOT NULL,
    PRIMARY KEY (snapshot_seq, resource_id, parent_id)
);
CREATE TABLE IF NOT EXISTS layer_batches (
    snapshot_seq INTEGER NOT NULL,
    layer INTEGER NOT NULL,
    batch INTEGER NOT NULL,
    position INTEGER NOT NULL,
    resource_id TEXT NOT NULL,
    PRIMARY KEY (snapshot_seq, layer, position)
);
CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    submit_id TEXT,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    base_snapshot_seq INTEGER,
    detail TEXT NOT NULL,
    result_json TEXT,
    created_at TEXT NOT NULL,
    finished_at TEXT
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Store:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._writer_lock = threading.RLock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_SCHEMA)
        self._memoized_intents: dict[str, dict] = {}

    @property
    def writer_lock(self) -> threading.RLock:
        return self._writer_lock

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------ meta
    def active_snapshot_seq(self) -> int | None:
        row = self._conn.execute("SELECT value FROM meta WHERE key='active_snapshot_seq'").fetchone()
        return int(row["value"]) if row else None

    def _set_active_snapshot(self, seq: int) -> None:
        self._conn.execute(
            "INSERT INTO meta(key,value) VALUES('active_snapshot_seq',?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(seq),),
        )

    # --------------------------------------------------------------- intents
    def begin_intent(
        self, kind: str, submit_id: str | None, base_snapshot_seq: int | None, detail: str
    ) -> tuple[str, dict | None]:
        """Register an intent first. Returns (intent_id, memoized_result).

        A memoized result means this submit_id already completed and the caller
        must reuse it without re-executing any state change.
        """
        intent_id = str(uuid.uuid4())
        if submit_id is not None:
            existing = self._conn.execute(
                "SELECT * FROM intents WHERE submit_id=? AND status='committed'", (submit_id,)
            ).fetchone()
            if existing:
                return intent_id, dict(existing)
        # Intent journaling uses autocommit so the pending fact survives even a
        # SIGKILL before the main writer transaction reaches COMMIT.
        in_transaction = self._conn.in_transaction
        if in_transaction:
            self._conn.execute("COMMIT")
        self._conn.execute(
            "INSERT INTO intents(intent_id,submit_id,kind,status,base_snapshot_seq,detail,created_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (intent_id, submit_id, kind, "pending", base_snapshot_seq, detail, utc_now()),
        )
        if in_transaction:
            self._conn.execute("BEGIN IMMEDIATE")
        return intent_id, None

    def finish_intent(
        self, intent_id: str, status: str, result_json: str | None
    ) -> None:
        self._conn.execute(
            "UPDATE intents SET status=?, result_json=?, finished_at=? WHERE intent_id=?",
            (status, result_json, utc_now(), intent_id),
        )

    def memoized_result(self, submit_id: str) -> dict | None:
        row = self._conn.execute(
            "SELECT * FROM intents WHERE submit_id=? AND status='committed'", (submit_id,)
        ).fetchone()
        return dict(row) if row else None

    # -------------------------------------------------------------- snapshots
    def commit_snapshot(
        self,
        snapshot_seq: int,
        format_name: str,
        format_version: int,
        generated_at: str,
        resources: list[NormalizedResource],
        batches: list[LayerBatch],
    ) -> None:
        """Persist a full snapshot and atomically promote it to active.

        All rows are written inside one transaction; the active pointer only
        moves at commit time, so a crash leaves the previous snapshot intact.
        """
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT snapshot_seq FROM snapshots WHERE snapshot_seq=?", (snapshot_seq,)
            ).fetchone()
            if existing:
                raise ValueError(f"snapshot_seq {snapshot_seq} already exists")
            conn.execute(
                "INSERT INTO snapshots(snapshot_seq,format,format_version,generated_at,created_at) "
                "VALUES(?,?,?,?,?)",
                (snapshot_seq, format_name, format_version, generated_at, utc_now()),
            )
            for resource in resources:
                conn.execute(
                    "INSERT INTO resources(snapshot_seq,resource_id,budget,weight_permille,"
                    "expected_version,layer,batch) VALUES(?,?,?,?,?,?,?)",
                    (
                        snapshot_seq,
                        resource.resource_id,
                        resource.budget,
                        resource.weight_permille,
                        resource.expected_version,
                        resource.layer,
                        resource.batch,
                    ),
                )
                for position, parent in enumerate(resource.parents):
                    conn.execute(
                        "INSERT INTO dependencies(snapshot_seq,resource_id,parent_id,position) "
                        "VALUES(?,?,?,?)",
                        (snapshot_seq, resource.resource_id, parent, position),
                    )
            for batch in batches:
                for position, resource_id in enumerate(batch.resources):
                    conn.execute(
                        "INSERT INTO layer_batches(snapshot_seq,layer,batch,position,resource_id) "
                        "VALUES(?,?,?,?,?)",
                        (snapshot_seq, batch.layer, batch.batch, position, resource_id),
                    )
            self._set_active_snapshot(snapshot_seq)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    # ---------------------------------------------------------------- reads
    def load_active(self) -> tuple[SnapshotStats | None, list[NormalizedResource], list[LayerBatch]]:
        seq = self.active_snapshot_seq()
        if seq is None:
            return None, [], []
        return self.load_snapshot(seq)

    def load_snapshot(
        self, snapshot_seq: int
    ) -> tuple[SnapshotStats, list[NormalizedResource], list[LayerBatch]]:
        snap = self._conn.execute(
            "SELECT * FROM snapshots WHERE snapshot_seq=?", (snapshot_seq,)
        ).fetchone()
        if snap is None:
            raise KeyError(snapshot_seq)
        rows = self._conn.execute(
            "SELECT * FROM resources WHERE snapshot_seq=? ORDER BY resource_id", (snapshot_seq,)
        ).fetchall()
        dep_rows = self._conn.execute(
            "SELECT resource_id,parent_id FROM dependencies WHERE snapshot_seq=? ORDER BY resource_id,position",
            (snapshot_seq,),
        ).fetchall()
        parents: dict[str, list[str]] = {}
        for row in dep_rows:
            parents.setdefault(row["resource_id"], []).append(row["parent_id"])
        resources = [
            NormalizedResource(
                resource_id=row["resource_id"],
                budget=row["budget"],
                weight_permille=row["weight_permille"],
                parents=parents.get(row["resource_id"], []),
                expected_version=row["expected_version"],
                layer=row["layer"],
                batch=row["batch"],
            )
            for row in rows
        ]
        batch_rows = self._conn.execute(
            "SELECT layer,batch,resource_id FROM layer_batches WHERE snapshot_seq=? "
            "ORDER BY layer,position",
            (snapshot_seq,),
        ).fetchall()
        grouped: dict[int, list[str]] = {}
        batch_index: dict[int, int] = {}
        for row in batch_rows:
            grouped.setdefault(row["layer"], []).append(row["resource_id"])
            batch_index[row["layer"]] = row["batch"]
        batches = [
            LayerBatch(layer=layer, batch=batch_index[layer], resources=members)
            for layer, members in sorted(grouped.items())
        ]
        edge_count = sum(len(parents.get(resource.resource_id, [])) for resource in resources)
        stats = SnapshotStats(
            snapshot_seq=snapshot_seq,
            generated_at=snap["generated_at"],
            resource_count=len(resources),
            edge_count=edge_count,
            layer_count=len(batches),
            batch_count=len(batches),
        )
        return stats, resources, batches

    def list_snapshots(self) -> list[SnapshotStats]:
        rows = self._conn.execute("SELECT snapshot_seq FROM snapshots ORDER BY snapshot_seq").fetchall()
        result = []
        for row in rows:
            stats, _, _ = self.load_snapshot(row["snapshot_seq"])
            result.append(stats)
        return result

    # -------------------------------------------------------------- recovery
    def recover(self) -> RecoveryReport:
        """Resolve intents left unfinished by a hard process termination."""
        pending = self._conn.execute(
            "SELECT * FROM intents WHERE status='pending' ORDER BY created_at"
        ).fetchall()
        aborted: list[RecoveryIntent] = []
        for row in pending:
            # Any uncommitted writer transaction was already rolled back by
            # SQLite on close; mark the intent aborted using persisted facts.
            self._conn.execute(
                "UPDATE intents SET status='aborted', finished_at=? WHERE intent_id=?",
                (utc_now(), row["intent_id"]),
            )
            aborted.append(
                RecoveryIntent(
                    intent_id=row["intent_id"],
                    kind=row["kind"],
                    status="aborted",
                    detail=row["detail"],
                    created_at=row["created_at"],
                    finished_at=utc_now(),
                )
            )
        active = self.active_snapshot_seq()
        import json as _json

        report = RecoveryReport(
            recovered=bool(pending),
            unfinished_intents=[
                RecoveryIntent(
                    intent_id=row["intent_id"],
                    kind=row["kind"],
                    status=row["status"],
                    detail=row["detail"],
                    created_at=row["created_at"],
                )
                for row in pending
            ],
            aborted_intents=aborted,
            active_snapshot_seq=active,
            note=(
                "pending intents aborted; active pointer remains on the last fully committed snapshot"
                if pending
                else "no unfinished intents found"
            ),
        )
        self._conn.execute(
            "INSERT INTO meta(key,value) VALUES('last_recovery',?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (_json.dumps(report.model_dump()),),
        )
        return report
