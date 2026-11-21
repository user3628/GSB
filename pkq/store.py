"""SQLite 持久化：会话、取数任务、写前意图记录、分页结果缓冲、恢复扫描。

写前意图协议：任务命中行写入缓冲后，在同一事务内先插入意图记录再提交；
进程被强制终止时未提交事务整体回滚，恢复扫描只依据已持久化事实。
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
  session_id TEXT PRIMARY KEY,
  query_id TEXT UNIQUE NOT NULL,
  fingerprint TEXT NOT NULL,
  keys_json TEXT NOT NULL,
  columns_json TEXT NOT NULL,
  page_size INTEGER NOT NULL,
  status TEXT NOT NULL,               -- MATERIALIZING / DONE
  next_page_seq INTEGER NOT NULL DEFAULT 0,
  consumed_pk_upper INTEGER,
  total_estimate INTEGER NOT NULL DEFAULT 0,
  exhausted_reported INTEGER NOT NULL DEFAULT 0,
  error_code TEXT,
  error_detail TEXT,
  created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks (
  session_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  status TEXT NOT NULL,               -- READY / DONE / SKIPPED
  reason TEXT NOT NULL DEFAULT '',
  payload_json TEXT NOT NULL,
  hit_count INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (session_id, task_id)
);
CREATE TABLE IF NOT EXISTS intents (
  session_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  state TEXT NOT NULL,                -- COMMITTED（意图随事务提交才可见）
  created_at REAL NOT NULL,
  PRIMARY KEY (session_id, task_id)
);
CREATE TABLE IF NOT EXISTS rows_buffer (
  session_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  row_json TEXT NOT NULL,
  PRIMARY KEY (session_id, seq)
);
CREATE TABLE IF NOT EXISTS pages (
  session_id TEXT NOT NULL,
  page_seq INTEGER NOT NULL,
  rows_json TEXT NOT NULL,
  row_count INTEGER NOT NULL,
  pk_lo INTEGER,
  pk_hi INTEGER,
  state TEXT NOT NULL,                -- MATERIALIZED / CONSUMED
  budget_registered INTEGER NOT NULL, -- 预算占用登记（与页提交同事务）
  PRIMARY KEY (session_id, page_seq)
);
"""


class Store:
    def __init__(self, db_path: str):
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._local = threading.local()
        self.db_path = db_path
        with self.conn() as c:
            c.executescript(SCHEMA)
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=FULL")

    def conn(self) -> sqlite3.Connection:
        c = getattr(self._local, "conn", None)
        if c is None:
            c = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA busy_timeout=30000")
            self._local.conn = c
        return c

    # ------------------------------------------------------------ 会话
    def create_session(self, session_id: str, query_id: str, fingerprint: str,
                       keys: list[int], columns: list[str], page_size: int,
                       total_estimate: int, status: str) -> None:
        with self.conn() as c:
            c.execute(
                "INSERT INTO sessions(session_id, query_id, fingerprint, keys_json, columns_json,"
                " page_size, status, total_estimate, created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (session_id, query_id, fingerprint, json.dumps(keys), json.dumps(columns),
                 page_size, status, total_estimate, time.time()))

    def get_session(self, session_id: str) -> sqlite3.Row | None:
        return self.conn().execute("SELECT * FROM sessions WHERE session_id=?", (session_id,)).fetchone()

    def get_session_by_query(self, query_id: str) -> sqlite3.Row | None:
        return self.conn().execute("SELECT * FROM sessions WHERE query_id=?", (query_id,)).fetchone()

    def list_sessions(self) -> list[sqlite3.Row]:
        return self.conn().execute("SELECT * FROM sessions ORDER BY created_at").fetchall()

    def insert_tasks(self, session_id: str, tasks: list[dict]) -> None:
        with self.conn() as c:
            for t in tasks:
                c.execute("INSERT OR REPLACE INTO tasks(session_id, task_id, status, payload_json)"
                          " VALUES(?,?,?,?)",
                          (session_id, t["task_id"], "READY", json.dumps(t)))

    def ready_tasks(self, session_id: str) -> list[sqlite3.Row]:
        return self.conn().execute(
            "SELECT * FROM tasks WHERE session_id=? AND status='READY' ORDER BY task_id",
            (session_id,)).fetchall()

    def task_counts(self, session_id: str) -> dict:
        rows = self.conn().execute(
            "SELECT status, COUNT(*) n FROM tasks WHERE session_id=? GROUP BY status",
            (session_id,)).fetchall()
        return {r["status"]: r["n"] for r in rows}

    # ------------------------------------------------------------ 任务提交（写前意图 + 页切割，单事务）
    def commit_task(self, session_id: str, task_id: str, hit_rows: list[dict],
                    pk_hi: int, page_size: int, finish: bool,
                    skip_reason: str | None = None) -> list[int]:
        """命中行写入缓冲 -> 写意图记录 -> 切割并提交页（同事务登记预算占用）-> 更新进度 -> 提交。
        返回新提交的页序号。"""
        c = self.conn()
        new_pages: list[int] = []
        try:
            c.execute("BEGIN IMMEDIATE")
            # 1) 命中行写入分页结果缓冲
            row = c.execute("SELECT COALESCE(MAX(seq), -1) m FROM rows_buffer WHERE session_id=?",
                            (session_id,)).fetchone()
            seq = row["m"] + 1
            for r in hit_rows:
                c.execute("INSERT INTO rows_buffer(session_id, seq, row_json) VALUES(?,?,?)",
                          (session_id, seq, json.dumps(r, ensure_ascii=False)))
                seq += 1
            # 2) 写意图记录（先写意图，再随事务提交）
            c.execute("INSERT OR REPLACE INTO intents(session_id, task_id, state, created_at)"
                      " VALUES(?,?,?,?)", (session_id, task_id, "COMMITTED", time.time()))
            # 3) 任务终态
            if skip_reason is None:
                c.execute("UPDATE tasks SET status='DONE', hit_count=? WHERE session_id=? AND task_id=?",
                          (len(hit_rows), session_id, task_id))
            else:
                c.execute("UPDATE tasks SET status='SKIPPED', reason=? WHERE session_id=? AND task_id=?",
                          (skip_reason, session_id, task_id))
            # 4) 切割页：页边界不得切断同一主键的命中行
            new_pages = self._cut_pages(c, session_id, page_size, finish)
            # 5) 会话进度（已消费主键上界）
            c.execute("UPDATE sessions SET consumed_pk_upper=? WHERE session_id=?",
                      (pk_hi, session_id))
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return new_pages

    def _cut_pages(self, c: sqlite3.Connection, session_id: str, page_size: int,
                   finish: bool) -> list[int]:
        new_pages: list[int] = []
        while True:
            rows = c.execute("SELECT seq, row_json FROM rows_buffer WHERE session_id=? ORDER BY seq",
                             (session_id,)).fetchall()
            if len(rows) < page_size:
                if not (finish and rows):
                    break
            take = min(len(rows), page_size)
            # 主键边界保护：若下一行与页末行同主键，则扩展页
            parsed = [json.loads(r["row_json"]) for r in rows]
            while take < len(rows) and parsed[take]["pk"] == parsed[take - 1]["pk"]:
                take += 1
            page_rows = parsed[:take]
            seqs = [r["seq"] for r in rows[:take]]
            row = c.execute("SELECT COALESCE(MAX(page_seq), -1) m FROM pages WHERE session_id=?",
                            (session_id,)).fetchone()
            page_seq = row["m"] + 1
            c.execute(
                "INSERT INTO pages(session_id, page_seq, rows_json, row_count, pk_lo, pk_hi,"
                " state, budget_registered) VALUES(?,?,?,?,?,?,?,?)",
                (session_id, page_seq, json.dumps(page_rows, ensure_ascii=False), len(page_rows),
                 page_rows[0]["pk"], page_rows[-1]["pk"], "MATERIALIZED", 1))
            c.execute(f"DELETE FROM rows_buffer WHERE session_id=? AND seq IN"
                      f" ({','.join('?' * len(seqs))})", [session_id, *seqs])
            new_pages.append(page_seq)
            if len(rows) - take < page_size and not finish:
                break
        return new_pages

    def mark_session_failed(self, session_id: str, code: str, detail: str) -> None:
        with self.conn() as c:
            c.execute("UPDATE sessions SET status='FAILED', error_code=?, error_detail=?"
                      " WHERE session_id=?", (code, detail, session_id))

    def finalize_session(self, session_id: str, page_size: int) -> None:
        """物化结束：切割剩余不足页（finish=True）并标记 DONE，单事务。"""
        c = self.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            self._cut_pages(c, session_id, page_size, finish=True)
            c.execute("UPDATE sessions SET status='DONE' WHERE session_id=?", (session_id,))
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------ 拉取
    def get_page(self, session_id: str, page_seq: int) -> sqlite3.Row | None:
        return self.conn().execute(
            "SELECT * FROM pages WHERE session_id=? AND page_seq=?",
            (session_id, page_seq)).fetchone()

    def last_page_seq(self, session_id: str) -> int:
        row = self.conn().execute(
            "SELECT COALESCE(MAX(page_seq), -1) m FROM pages WHERE session_id=?",
            (session_id,)).fetchone()
        return row["m"]

    def materialized_page_count(self, session_id: str) -> int:
        row = self.conn().execute(
            "SELECT COUNT(*) n FROM pages WHERE session_id=?", (session_id,)).fetchone()
        return row["n"]

    def consume_page(self, session_id: str, page_seq: int) -> bool:
        """原子消费：仅一个客户端成功。"""
        c = self.conn()
        with c:
            cur = c.execute(
                "UPDATE pages SET state='CONSUMED' WHERE session_id=? AND page_seq=?"
                " AND state='MATERIALIZED'", (session_id, page_seq))
            ok = cur.rowcount == 1
            if ok:
                c.execute("UPDATE sessions SET next_page_seq=? WHERE session_id=?",
                          (page_seq + 1, session_id))
        return ok

    def mark_exhausted_reported(self, session_id: str) -> None:
        with self.conn() as c:
            c.execute("UPDATE sessions SET exhausted_reported=1 WHERE session_id=?", (session_id,))

    # ------------------------------------------------------------ 恢复
    def recover(self) -> dict:
        """扫描未完成意图：已提交任务可复用，未提交任务恢复为就绪。只基于已持久化事实。"""
        c = self.conn()
        report = {"sessions": []}
        sessions = c.execute("SELECT * FROM sessions").fetchall()
        for s in sessions:
            tasks = c.execute("SELECT task_id, status FROM tasks WHERE session_id=? ORDER BY task_id",
                              (s["session_id"],)).fetchall()
            reused, requeued = [], []
            for t in tasks:
                intent = c.execute(
                    "SELECT state FROM intents WHERE session_id=? AND task_id=?",
                    (s["session_id"], t["task_id"])).fetchone()
                if intent is not None and t["status"] in ("DONE", "SKIPPED"):
                    reused.append(t["task_id"])  # 已提交：可复用，不重算
                else:
                    # 未提交：恢复为就绪，重新排队（未提交事务已回滚，无残留缓冲行）
                    c.execute("UPDATE tasks SET status='READY' WHERE session_id=? AND task_id=?",
                              (s["session_id"], t["task_id"]))
                    requeued.append(t["task_id"])
            report["sessions"].append({
                "session_id": s["session_id"],
                "status": s["status"],
                "reused_tasks": reused,
                "requeued_tasks": requeued,
                "materialized_pages": self.materialized_page_count(s["session_id"]),
                "next_page_seq": s["next_page_seq"],
                "consumed_pk_upper": s["consumed_pk_upper"],
            })
        return report
