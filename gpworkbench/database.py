"""SQLite 连接管理与 schema 初始化。

采用 WAL 模式：多个只读查询可并发，写事务通过 BEGIN IMMEDIATE 串行化。
每个线程持有独立连接，事务边界由业务层显式控制，禁止隐式事务。
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

SCHEMA_VERSION = "1"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quota_groups (
    group_id TEXT PRIMARY KEY,
    memory_limit INTEGER NOT NULL,
    compute_limit INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS nodes (
    node_id TEXT PRIMARY KEY,
    total_memory INTEGER NOT NULL,
    total_compute INTEGER NOT NULL,
    base_used_memory INTEGER NOT NULL,
    group_id TEXT NOT NULL,
    health TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS models (
    model_id TEXT PRIMARY KEY,
    cost_per_sample INTEGER NOT NULL,
    max_batch_samples INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS requests (
    request_id TEXT PRIMARY KEY,
    service_name TEXT NOT NULL,
    model_id TEXT NOT NULL,
    memory_need INTEGER NOT NULL,
    compute_need INTEGER NOT NULL,
    samples_per_batch INTEGER NOT NULL,
    latency_limit_ms INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    group_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    state TEXT NOT NULL,
    reserved_memory INTEGER NOT NULL DEFAULT 0,
    reserved_compute INTEGER NOT NULL DEFAULT 0,
    node_id TEXT,
    batch_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_requests_state ON requests(state);
CREATE INDEX IF NOT EXISTS idx_requests_group ON requests(group_id);
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    model_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    total_samples INTEGER NOT NULL,
    total_memory INTEGER NOT NULL,
    total_compute INTEGER NOT NULL,
    estimated_latency_ms INTEGER NOT NULL,
    state TEXT NOT NULL,
    reorg_count INTEGER NOT NULL DEFAULT 0,
    created_intent TEXT NOT NULL,
    version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batches_node ON batches(node_id);
CREATE INDEX IF NOT EXISTS idx_batches_state ON batches(state);
CREATE TABLE IF NOT EXISTS batch_items (
    batch_id TEXT NOT NULL,
    request_id TEXT NOT NULL UNIQUE,
    seq INTEGER NOT NULL,
    PRIMARY KEY (batch_id, request_id)
);
CREATE TABLE IF NOT EXISTS request_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT NOT NULL,
    reason TEXT NOT NULL,
    node_id TEXT,
    batch_id TEXT,
    intent_id TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_request ON request_events(request_id);
CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    intent_type TEXT NOT NULL,
    status TEXT NOT NULL,
    payload TEXT NOT NULL,
    result TEXT,
    created_at TEXT NOT NULL,
    committed_at TEXT
);
CREATE TABLE IF NOT EXISTS recovery_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_id TEXT NOT NULL,
    action TEXT NOT NULL,
    detail TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


class Database:
    """线程局部连接的 SQLite 访问入口。"""

    def __init__(self, path: str | Path):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._bootstrap_lock = threading.Lock()
        self._bootstrap()

    def _new_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    def _bootstrap(self) -> None:
        with self._bootstrap_lock:
            conn = self._new_connection()
            try:
                # executescript 会先隐式提交，因此 DDL 单独执行（IF NOT EXISTS 天然幂等）
                conn.executescript(SCHEMA_SQL)
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', ?)",
                    (SCHEMA_VERSION,),
                )
                conn.execute("COMMIT")
            except Exception:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

    def connect(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._new_connection()
            self._local.conn = conn
        return conn

    @contextmanager
    def transaction(self):
        conn = self.connect()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None
