"""SQLite-backed durable storage for pages, checkpoints, intents and request ledger."""
from __future__ import annotations

import json
import os
import sqlite3
from typing import Any, Dict, List, Optional

from .models import Checkpoint, Page
from .util import canonical_json


class SqliteStore:
    """One connection guarded by the engine's single-writer lock."""

    def __init__(self, db_path: str, synchronous: str = "FULL"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute(f"PRAGMA synchronous={synchronous}")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._create_schema()

    def _create_schema(self):
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS pages (
                page_no   INTEGER PRIMARY KEY,
                digest    TEXT NOT NULL,
                version   INTEGER NOT NULL,
                last_lsn  INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS checkpoints (
                seq        INTEGER PRIMARY KEY,
                start_lsn  INTEGER NOT NULL,
                checksum   TEXT NOT NULL,
                payload    TEXT NOT NULL,
                status     TEXT NOT NULL,
                created_ts INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS intents (
                intent_id   TEXT PRIMARY KEY,
                kind        TEXT NOT NULL,
                state       TEXT NOT NULL,
                created_ts  INTEGER NOT NULL,
                payload     TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS request_ledger (
                request_id TEXT PRIMARY KEY,
                status     TEXT NOT NULL,
                result     TEXT,
                created_ts INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )

    def close(self):
        self.conn.close()

    # ------------------------------------------------------------------ meta
    def get_meta(self, key: str) -> Optional[str]:
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: str):
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

    # ----------------------------------------------------------------- pages
    def upsert_page(self, page: Page):
        self.conn.execute(
            "INSERT INTO pages(page_no, digest, version, last_lsn) VALUES(?, ?, ?, ?) "
            "ON CONFLICT(page_no) DO UPDATE SET "
            "digest=excluded.digest, version=excluded.version, last_lsn=excluded.last_lsn",
            (page.page_no, page.digest, page.version, page.last_lsn),
        )

    def upsert_pages_batch(self, pages: List[Page]):
        self.conn.executemany(
            "INSERT INTO pages(page_no, digest, version, last_lsn) VALUES(?, ?, ?, ?) "
            "ON CONFLICT(page_no) DO UPDATE SET "
            "digest=excluded.digest, version=excluded.version, last_lsn=excluded.last_lsn",
            [(p.page_no, p.digest, p.version, p.last_lsn) for p in pages],
        )

    def get_page(self, page_no: int) -> Optional[Page]:
        row = self.conn.execute(
            "SELECT page_no, digest, version, last_lsn FROM pages WHERE page_no=?",
            (page_no,),
        ).fetchone()
        return Page.from_dict(dict(row)) if row else None

    def all_pages(self) -> Dict[int, Page]:
        rows = self.conn.execute(
            "SELECT page_no, digest, version, last_lsn FROM pages"
        ).fetchall()
        return {r["page_no"]: Page.from_dict(dict(r)) for r in rows}

    def reset_pages(self, pages: Dict[int, Page]):
        """Atomically replace the whole page table (checkpoint rollback/restore)."""
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute("DELETE FROM pages")
            self.upsert_pages_batch(list(pages.values()))
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------ checkpoints
    def insert_checkpoint(self, cp: Checkpoint, status: str, created_ts: int):
        self.conn.execute(
            "INSERT INTO checkpoints(seq, start_lsn, checksum, payload, status, created_ts) "
            "VALUES(?, ?, ?, ?, ?, ?)",
            (
                cp.seq,
                cp.start_lsn,
                cp.checksum,
                canonical_json(cp.to_dict()),
                status,
                created_ts,
            ),
        )

    def set_checkpoint_status(self, seq: int, status: str):
        self.conn.execute(
            "UPDATE checkpoints SET status=? WHERE seq=?", (status, seq)
        )

    def latest_checkpoint_row(self) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM checkpoints WHERE status='COMMITTED' ORDER BY seq DESC LIMIT 1"
        ).fetchone()

    def checkpoint_rows(self) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM checkpoints ORDER BY seq"
        ).fetchall()

    @staticmethod
    def row_to_checkpoint(row: sqlite3.Row) -> Checkpoint:
        return Checkpoint.from_dict(json.loads(row["payload"]))

    def replace_checkpoint(self, cp: Checkpoint, created_ts: int):
        """Whole-state replacement used when committing a new checkpoint."""
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.reset_pages(cp.pages)
            self.insert_checkpoint(cp, "COMMITTED", created_ts)
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    # ---------------------------------------------------------------- intents
    def begin_intent(self, intent_id: str, kind: str, payload: Dict[str, Any], ts: int):
        self.conn.execute(
            "INSERT INTO intents(intent_id, kind, state, created_ts, payload) "
            "VALUES(?, ?, 'OPEN', ?, ?)",
            (intent_id, kind, ts, canonical_json(payload)),
        )

    def update_intent(self, intent_id: str, state: str, payload: Optional[Dict[str, Any]] = None):
        if payload is None:
            self.conn.execute(
                "UPDATE intents SET state=? WHERE intent_id=?", (state, intent_id)
            )
        else:
            self.conn.execute(
                "UPDATE intents SET state=?, payload=? WHERE intent_id=?",
                (state, canonical_json(payload), intent_id),
            )

    def get_intent(self, intent_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM intents WHERE intent_id=?", (intent_id,)
        ).fetchone()

    def open_intents(self) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM intents WHERE state='OPEN' ORDER BY created_ts, intent_id"
        ).fetchall()

    # --------------------------------------------------------- request ledger
    def ledger_get(self, request_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM request_ledger WHERE request_id=?", (request_id,)
        ).fetchone()

    def ledger_begin(self, request_id: str, ts: int) -> bool:
        """Return True if this call owns the request, False if it already exists."""
        try:
            self.conn.execute(
                "INSERT INTO request_ledger(request_id, status, result, created_ts) "
                "VALUES(?, 'RUNNING', NULL, ?)",
                (request_id, ts),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    def ledger_finish(self, request_id: str, result: Dict[str, Any]):
        self.conn.execute(
            "UPDATE request_ledger SET status='DONE', result=? WHERE request_id=?",
            (canonical_json(result), request_id),
        )
