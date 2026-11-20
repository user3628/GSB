"""SQLite 持久化层：连接、schema、文件清理。

所有派生表（classification_index / timeline_index / batch_stats）都在
同一事务内随批次一起提交，WAL 模式下读连接永远只能看到某个完整提交
版本，看不到写到一半的中间状态。
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Optional

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
PRAGMA synchronous=FULL;
PRAGMA busy_timeout=10000;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id            TEXT PRIMARY KEY,
    state               TEXT NOT NULL,
    base_version        INTEGER,
    version             INTEGER,
    path_mode           TEXT,
    created_at_ms       INTEGER NOT NULL,
    committed_at_ms     INTEGER,
    invalidated_version INTEGER
);

CREATE TABLE IF NOT EXISTS records (
    batch_id    TEXT NOT NULL,
    line_no     INTEGER NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    service     TEXT NOT NULL,
    level       TEXT NOT NULL,
    message     TEXT NOT NULL,
    PRIMARY KEY (batch_id, line_no),
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
);
CREATE INDEX IF NOT EXISTS idx_records_service_ts ON records(service, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_records_batch ON records(batch_id);

CREATE TABLE IF NOT EXISTS record_ts_history (
    batch_id      TEXT NOT NULL,
    line_no       INTEGER NOT NULL,
    old_ts        INTEGER NOT NULL,
    new_ts        INTEGER NOT NULL,
    event_version INTEGER NOT NULL,
    changed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (batch_id, line_no, event_version)
);

CREATE TABLE IF NOT EXISTS classification_index (
    service       TEXT NOT NULL,
    level         TEXT NOT NULL,
    batch_id      TEXT NOT NULL,
    record_count  INTEGER NOT NULL,
    updated_version INTEGER NOT NULL,
    PRIMARY KEY (service, level, batch_id)
);

CREATE TABLE IF NOT EXISTS timeline_index (
    service        TEXT NOT NULL,
    start_ms       INTEGER NOT NULL,
    end_ms         INTEGER NOT NULL,
    ordinal        INTEGER NOT NULL,
    built_version  INTEGER NOT NULL,
    rebuilt_count  INTEGER NOT NULL DEFAULT 0,
    last_reason    TEXT,
    PRIMARY KEY (service, ordinal)
);
CREATE INDEX IF NOT EXISTS idx_timeline_service ON timeline_index(service);

CREATE TABLE IF NOT EXISTS batch_stats (
    batch_id      TEXT NOT NULL,
    service       TEXT NOT NULL,
    level         TEXT NOT NULL,
    record_count  INTEGER NOT NULL,
    updated_version INTEGER NOT NULL,
    PRIMARY KEY (batch_id, service, level)
);

CREATE TABLE IF NOT EXISTS service_watermark (
    service     TEXT PRIMARY KEY,
    max_ts      INTEGER NOT NULL,
    updated_version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS version_events (
    version       INTEGER PRIMARY KEY,
    event_type    TEXT NOT NULL,
    batch_id      TEXT,
    reason        TEXT,
    affected      TEXT,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
    intent_id     TEXT PRIMARY KEY,
    batch_id      TEXT NOT NULL,
    op_type       TEXT NOT NULL,
    state         TEXT NOT NULL,
    payload       TEXT,
    created_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER
);

CREATE TABLE IF NOT EXISTS write_lock (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    holder      TEXT NOT NULL,
    acquired_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_events (
    event_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    recovered_at_ms INTEGER NOT NULL,
    kind          TEXT NOT NULL,
    detail        TEXT NOT NULL
);
"""


def default_db_path(base_dir: os.PathLike | str) -> Path:
    return Path(base_dir) / "logindex.db"


def reset_database(db_path: os.PathLike | str) -> None:
    """删除数据库主文件及 WAL/SHM 侧车文件，保证演示可从空状态重跑。"""
    path = Path(db_path)
    for suffix in ("", "-wal", "-shm"):
        side = Path(str(path) + suffix)
        if side.exists():
            side.unlink()


def connect(db_path: os.PathLike | str, *, readonly: bool = False) -> sqlite3.Connection:
    """打开连接。只读连接使用 immutable/uri 以保证不阻塞写者。"""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if readonly:
        uri = f"file:{path.resolve()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=15, check_same_thread=False)
    else:
        conn = sqlite3.connect(str(path), timeout=15, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    if not readonly:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA busy_timeout=10000")
    return conn


def initialize(db_path: os.PathLike | str) -> None:
    """建库建表（幂等）。"""
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()


def now_ms() -> int:
    import time

    return time.time_ns() // 1_000_000
