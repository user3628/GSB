"""SQLite connection factory.

Design points:
* WAL mode + a busy timeout allow concurrent readers alongside one writer.
* Connections are short-lived per unit of work so the FastAPI handlers do not
  share mutable cursors across threads.
* Every connection enforces foreign keys and uses a deterministic row factory.
"""
from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from app.config import Settings


def _apply_pragmas(conn: sqlite3.Connection, settings: Settings) -> None:
    conn.execute(f"PRAGMA busy_timeout={settings.busy_timeout_ms}")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.row_factory = sqlite3.Row


def connect(settings: Settings) -> sqlite3.Connection:
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(settings.db_path),
        timeout=settings.busy_timeout_ms / 1000.0,
        isolation_level=None,  # explicit BEGIN/COMMIT, no implicit transactions
        check_same_thread=False,
    )
    _apply_pragmas(conn, settings)
    return conn


@contextmanager
def connection(settings: Settings) -> Iterator[sqlite3.Connection]:
    conn = connect(settings)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection, immediate: bool = False) -> Iterator[sqlite3.Connection]:
    """Explicit transaction boundary.

    ``immediate=True`` acquires the RESERVED write lock up front (BEGIN IMMEDIATE)
    which serialises writers and prevents SQLITE_BUSY upgrade deadlocks between
    the single-writer sessions.
    """
    conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
