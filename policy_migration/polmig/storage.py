"""SQLite persistence for legacy/new policies, batches, decisions and intents."""

import json
import sqlite3
import threading


class Store:
    def __init__(self, path):
        self.path = path
        self._write_lock = threading.RLock()
        self._connect()
        self._create_schema()

    def _connect(self):
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=5000")

    def _create_schema(self):
        with self._write_lock:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS versions (
                    version INTEGER PRIMARY KEY,
                    created_by_batch TEXT,
                    created_at_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS batches (
                    batch_id TEXT PRIMARY KEY,
                    base_version INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    source_format TEXT,
                    source_version INTEGER,
                    content_hash TEXT,
                    verdict TEXT,
                    committed_version INTEGER,
                    request_json TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    error_code TEXT,
                    error_message TEXT
                );
                CREATE TABLE IF NOT EXISTS decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT NOT NULL,
                    record_index INTEGER NOT NULL,
                    tenant_id TEXT,
                    verdict TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    UNIQUE(batch_id, record_index)
                );
                CREATE TABLE IF NOT EXISTS intents (
                    batch_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    resolved_at_ms INTEGER,
                    detail_json TEXT
                );
                CREATE TABLE IF NOT EXISTS legacy_policies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tenant_id TEXT NOT NULL,
                    batch_id TEXT,
                    payload_json TEXT NOT NULL,
                    UNIQUE(batch_id, tenant_id)
                );
                CREATE TABLE IF NOT EXISTS new_policies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version INTEGER NOT NULL,
                    tenant_id TEXT NOT NULL,
                    parent_ref TEXT,
                    payload_json TEXT NOT NULL,
                    canonical_digest TEXT NOT NULL,
                    UNIQUE(version, tenant_id)
                );
                CREATE TABLE IF NOT EXISTS recovery_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recovered_at_ms INTEGER NOT NULL,
                    batch_id TEXT,
                    action TEXT NOT NULL,
                    detail_json TEXT NOT NULL
                );
                """
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO versions(version, created_by_batch, created_at_ms) "
                "VALUES (0, NULL, 0)"
            )
            self.conn.commit()

    @property
    def write_lock(self):
        return self._write_lock

    def current_version(self):
        row = self.conn.execute(
            "SELECT MAX(version) AS v FROM versions"
        ).fetchone()
        return int(row["v"] or 0)

    def insert_intent(self, batch_id, now_ms, detail):
        self.conn.execute(
            "INSERT INTO intents(batch_id, state, created_at_ms, detail_json) "
            "VALUES (?, 'INTENDED', ?, ?)",
            (batch_id, now_ms, json.dumps(detail, ensure_ascii=False)),
        )

    def resolve_intent(self, batch_id, state, now_ms, detail=None):
        self.conn.execute(
            "UPDATE intents SET state=?, resolved_at_ms=?, detail_json=? "
            "WHERE batch_id=?",
            (state, now_ms,
             json.dumps(detail, ensure_ascii=False) if detail is not None else None,
             batch_id),
        )

    def unfinished_intents(self):
        return self.conn.execute(
            "SELECT * FROM intents WHERE state='INTENDED' ORDER BY created_at_ms, batch_id"
        ).fetchall()

    def insert_batch(self, row):
        self.conn.execute(
            "INSERT INTO batches(batch_id, base_version, status, source_format, "
            "source_version, content_hash, verdict, committed_version, request_json, "
            "created_at_ms, updated_at_ms, error_code, error_message) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (row["batch_id"], row["base_version"], row["status"],
             row.get("source_format"), row.get("source_version"),
             row.get("content_hash"), row.get("verdict"),
             row.get("committed_version"), json.dumps(row["request"], ensure_ascii=False),
             row["created_at_ms"], row["updated_at_ms"],
             row.get("error_code"), row.get("error_message")),
        )

    def update_batch_status(self, batch_id, status, now_ms, **extra):
        fields = ["status=?", "updated_at_ms=?"]
        params = [status, now_ms]
        for key, value in extra.items():
            fields.append("%s=?" % key)
            params.append(value)
        params.append(batch_id)
        self.conn.execute(
            "UPDATE batches SET %s WHERE batch_id=?" % ", ".join(fields), params
        )

    def get_batch(self, batch_id):
        return self.conn.execute(
            "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
        ).fetchone()

    def insert_decisions(self, batch_id, decisions):
        self.conn.executemany(
            "INSERT INTO decisions(batch_id, record_index, tenant_id, verdict, detail_json) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                (batch_id, d["record_index"], d.get("tenant_id"), d["verdict"],
                 json.dumps(d, ensure_ascii=False))
                for d in decisions
            ],
        )

    def get_decisions(self, batch_id):
        rows = self.conn.execute(
            "SELECT detail_json FROM decisions WHERE batch_id=? ORDER BY record_index",
            (batch_id,),
        ).fetchall()
        return [json.loads(r["detail_json"]) for r in rows]

    def insert_legacy_policies(self, batch_id, policies):
        self.conn.executemany(
            "INSERT INTO legacy_policies(tenant_id, batch_id, payload_json) "
            "VALUES (?, ?, ?)",
            [
                (p.tenant_id, batch_id,
                 json.dumps({
                     "tenant_id": p.tenant_id,
                     "concurrency_limit": p.concurrency_limit,
                     "priority_min": p.priority_min,
                     "priority_max": p.priority_max,
                     "preempt_enabled": p.preempt_enabled,
                     "deprecated_fields": p.deprecated_fields,
                 }, ensure_ascii=False))
                for p in policies
            ],
        )

    def active_new_policies(self, tenant_id=None):
        version = self.current_version()
        sql = "SELECT * FROM new_policies WHERE version=?"
        params = [version]
        if tenant_id is not None:
            sql += " AND tenant_id=?"
            params.append(tenant_id)
        return self.conn.execute(sql, params).fetchall()

    def stored_parent_ref(self, tenant_id):
        row = self.conn.execute(
            "SELECT parent_ref FROM new_policies WHERE version=? AND tenant_id=?",
            (self.current_version(), tenant_id),
        ).fetchone()
        return row["parent_ref"] if row else None

    def tenant_exists_stored(self, tenant_id):
        row = self.conn.execute(
            "SELECT 1 FROM new_policies WHERE version=? AND tenant_id=?",
            (self.current_version(), tenant_id),
        ).fetchone()
        return row is not None

    def insert_new_version(self, version, batch_id, now_ms, policies):
        self.conn.execute(
            "INSERT INTO versions(version, created_by_batch, created_at_ms) "
            "VALUES (?, ?, ?)",
            (version, batch_id, now_ms),
        )
        self.conn.executemany(
            "INSERT INTO new_policies(version, tenant_id, parent_ref, payload_json, "
            "canonical_digest) VALUES (?, ?, ?, ?, ?)",
            [
                (version, p.tenant_id, p.parent_ref,
                 json.dumps(p.to_dict(), ensure_ascii=False), p.canonical_digest())
                for p in policies
            ],
        )

    def insert_recovery_event(self, now_ms, batch_id, action, detail):
        self.conn.execute(
            "INSERT INTO recovery_events(recovered_at_ms, batch_id, action, detail_json) "
            "VALUES (?, ?, ?, ?)",
            (now_ms, batch_id, action, json.dumps(detail, ensure_ascii=False)),
        )

    def recovery_events(self):
        return self.conn.execute(
            "SELECT * FROM recovery_events ORDER BY id"
        ).fetchall()

    def list_versions(self):
        return self.conn.execute(
            "SELECT * FROM versions ORDER BY version"
        ).fetchall()

    def policies_at_version(self, version):
        return self.conn.execute(
            "SELECT * FROM new_policies WHERE version=? ORDER BY tenant_id",
            (version,),
        ).fetchall()

    def reset(self):
        with self._write_lock:
            self.conn.executescript(
                """
                DELETE FROM recovery_events;
                DELETE FROM new_policies;
                DELETE FROM legacy_policies;
                DELETE FROM decisions;
                DELETE FROM intents;
                DELETE FROM batches;
                DELETE FROM versions;
                INSERT INTO versions(version, created_by_batch, created_at_ms)
                VALUES (0, NULL, 0);
                """
            )
            self.conn.commit()
