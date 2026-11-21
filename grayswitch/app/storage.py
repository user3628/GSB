"""SQLite 持久化层：意图日志先行、单写者事务、崩溃恢复。

写事务遵循固定顺序：
  1. 先向 ``intents`` 写入一条 ``PENDING`` 意图并 COMMIT（崩溃点可被观测）；
  2. 在同一个*后续*事务里写入业务数据、把意图置为 ``COMMITTED`` 并 COMMIT。

进程在第 1 步之后、第 2 步之前被强制终止时，重启 :meth:`Storage.recover` 会
扫描到 ``PENDING`` 意图：批次裁决/统计重建回滚为 ``ABORTED``（其业务数据在同一
事务中随回滚消失，灰度累计自然停留在最后一个完整版本）。恢复报告只引用库里
实际持久化的事实，不推测“本来会发生什么”。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS policies (
    version INTEGER PRIMARY KEY,
    payload_json TEXT NOT NULL,
    digest TEXT NOT NULL,
    committed_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    policy_version INTEGER,
    batch_id TEXT,
    status TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL,
    policy_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    request_count INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    request_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    policy_version INTEGER NOT NULL,
    event_seq INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    verdict_json TEXT NOT NULL,
    status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS request_index (
    request_id TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    batch_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS counters (
    policy_version INTEGER PRIMARY KEY,
    state_json TEXT NOT NULL,
    updated_batch_id TEXT
);
CREATE TABLE IF NOT EXISTS numeric_traces (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    policy_version INTEGER NOT NULL,
    batch_id TEXT,
    kind TEXT NOT NULL,
    trace_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS recovery_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at_ms INTEGER NOT NULL,
    report_json TEXT NOT NULL
);
"""


class Storage:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        # check_same_thread=False：读连接可并发；所有写访问由服务层单写者锁串行化。
        self._conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=FULL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._conn.execute("PRAGMA busy_timeout=5000;")
        # 独立只读连接：WAL 模式下读者不阻塞写者、读者之间也不互斥。
        # 懒加载 + 线程本地：每个读者线程持有自己的只读连接，既允许真正并发，
        # 又避免多个线程共享同一 sqlite 连接导致的 InterfaceError。
        self._db_path = db_path
        self._read_local = threading.local()
        self._conn.executescript(SCHEMA)
        self._write_lock = threading.RLock()

    def close(self) -> None:
        self._conn.close()
        conn = getattr(self._read_local, "conn", None)
        if conn is not None:
            conn.close()

    def _get_read_conn(self) -> sqlite3.Connection:
        conn = getattr(self._read_local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(
                f"file:{self._db_path}?mode=ro", uri=True,
                check_same_thread=True, isolation_level=None,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=5000;")
            self._read_local.conn = conn
        return conn

    @contextmanager
    def read_cursor(self) -> Iterator[sqlite3.Connection]:
        """只读快照访问：不获取写锁，多读者基于线程本地连接并发。"""
        yield self._get_read_conn()

    # ---------------- 事务工具 ----------------
    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._write_lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    @property
    def write_lock(self) -> threading.RLock:
        return self._write_lock

    # ---------------- 策略快照 ----------------
    def latest_policy_version(self) -> Optional[int]:
        row = self._get_read_conn().execute(
            "SELECT MAX(version) AS v FROM policies"
        ).fetchone()
        return row["v"] if row and row["v"] is not None else None

    def policy_exists(self, version: int) -> bool:
        return self._get_read_conn().execute(
            "SELECT 1 FROM policies WHERE version=?", (version,)
        ).fetchone() is not None

    def insert_policy(self, intent_id: str, version: int, payload: Dict[str, Any],
                      digest: str, now_ms: int) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO policies(version, payload_json, digest, committed_at_ms)"
                " VALUES(?,?,?,?)",
                (version, json.dumps(payload, ensure_ascii=False, sort_keys=True),
                 digest, now_ms),
            )
            conn.execute(
                "UPDATE intents SET status='COMMITTED', finished_at_ms=? WHERE intent_id=?",
                (now_ms, intent_id),
            )

    def get_policy_payload(self, version: Optional[int] = None) -> Optional[Dict[str, Any]]:
        if version is None:
            version = self.latest_policy_version()
        if version is None:
            return None
        row = self._get_read_conn().execute(
            "SELECT payload_json FROM policies WHERE version=?", (version,)
        ).fetchone()
        return json.loads(row["payload_json"]) if row else None

    def list_policy_versions(self) -> List[int]:
        return [r["version"] for r in self._get_read_conn().execute(
            "SELECT version FROM policies ORDER BY version"
        ).fetchall()]

    # ---------------- 意图 ----------------
    def create_intent(self, kind: str, now_ms: int,
                      policy_version: Optional[int], batch_id: Optional[str],
                      detail: Dict[str, Any]) -> str:
        intent_id = uuid.uuid4().hex
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO intents(intent_id, kind, policy_version, batch_id, status,"
                " detail_json, created_at_ms) VALUES(?,?,?,?,?,?,?)",
                (intent_id, kind, policy_version, batch_id, "PENDING",
                 json.dumps(detail, ensure_ascii=False, sort_keys=True), now_ms),
            )
        return intent_id

    def get_intent(self, intent_id: str) -> Optional[sqlite3.Row]:
        return self._get_read_conn().execute(
            "SELECT * FROM intents WHERE intent_id=?", (intent_id,)
        ).fetchone()

    def list_pending_intents(self) -> List[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM intents WHERE status='PENDING' ORDER BY created_at_ms, intent_id"
        ).fetchall()

    # ---------------- 批次 / 事件 / 累计状态 ----------------
    def commit_batch(self, intent_id: str, batch_id: str, policy_version: int,
                     events_payload: List[Dict[str, Any]], verdicts: List[Dict[str, Any]],
                     state: Dict[str, Any], now_ms: int,
                     content_hashes: Dict[str, str]) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO batches(batch_id, intent_id, policy_version, status,"
                " request_count, payload_json, created_at_ms)"
                " VALUES(?,?,?,?,?,?,?)",
                (batch_id, intent_id, policy_version, "COMMITTED", len(events_payload),
                 json.dumps(events_payload, ensure_ascii=False), now_ms),
            )
            for ev, verdict in zip(events_payload, verdicts):
                rid = ev["request_id"]
                conn.execute(
                    "INSERT INTO events(request_id, batch_id, policy_version, event_seq,"
                    " payload_json, verdict_json, status) VALUES(?,?,?,?,?,?,?)",
                    (rid, batch_id, policy_version, verdict["event_seq"],
                     json.dumps(ev, ensure_ascii=False, sort_keys=True),
                     json.dumps(verdict, ensure_ascii=False, sort_keys=True),
                     verdict["status"]),
                )
                conn.execute(
                    "INSERT INTO request_index(request_id, content_hash, batch_id)"
                    " VALUES(?,?,?)",
                    (rid, content_hashes[rid], batch_id),
                )
            conn.execute(
                "INSERT INTO counters(policy_version, state_json, updated_batch_id)"
                " VALUES(?,?,?) ON CONFLICT(policy_version) DO UPDATE SET"
                " state_json=excluded.state_json, updated_batch_id=excluded.updated_batch_id",
                (policy_version, json.dumps(state, ensure_ascii=False, sort_keys=True), batch_id),
            )
            conn.execute(
                "UPDATE intents SET status='COMMITTED', finished_at_ms=? WHERE intent_id=?",
                (now_ms, intent_id),
            )

    def lookup_request(self, request_id: str) -> Optional[sqlite3.Row]:
        return self._get_read_conn().execute(
            "SELECT * FROM request_index WHERE request_id=?", (request_id,)
        ).fetchone()

    def get_verdict(self, request_id: str) -> Optional[Dict[str, Any]]:
        row = self._get_read_conn().execute(
            "SELECT verdict_json FROM events WHERE request_id=?", (request_id,)
        ).fetchone()
        return json.loads(row["verdict_json"]) if row else None

    def list_verdicts(self, policy_version: Optional[int] = None) -> List[Dict[str, Any]]:
        if policy_version is None:
            rows = self._get_read_conn().execute(
                "SELECT verdict_json FROM events ORDER BY policy_version, event_seq"
            ).fetchall()
        else:
            rows = self._get_read_conn().execute(
                "SELECT verdict_json FROM events WHERE policy_version=?"
                " ORDER BY event_seq", (policy_version,)
            ).fetchall()
        return [json.loads(r["verdict_json"]) for r in rows]

    def load_state(self, policy_version: int) -> Optional[Dict[str, Any]]:
        row = self._get_read_conn().execute(
            "SELECT state_json FROM counters WHERE policy_version=?", (policy_version,)
        ).fetchone()
        return json.loads(row["state_json"]) if row else None

    def save_state(self, policy_version: int, state: Dict[str, Any],
                   batch_id: Optional[str]) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO counters(policy_version, state_json, updated_batch_id)"
                " VALUES(?,?,?) ON CONFLICT(policy_version) DO UPDATE SET"
                " state_json=excluded.state_json, updated_batch_id=excluded.updated_batch_id",
                (policy_version, json.dumps(state, ensure_ascii=False, sort_keys=True), batch_id),
            )

    # ---------------- 数值轨迹 ----------------
    def save_traces(self, policy_version: int, batch_id: Optional[str],
                    kind: str, traces: List[Dict[str, Any]]) -> None:
        if not traces:
            return
        with self.transaction() as conn:
            for tr in traces:
                conn.execute(
                    "INSERT INTO numeric_traces(policy_version, batch_id, kind, trace_json)"
                    " VALUES(?,?,?,?)",
                    (policy_version, batch_id, kind,
                     json.dumps(tr, ensure_ascii=False, sort_keys=True)),
                )

    def get_traces(self, policy_version: int, kind: Optional[str] = None) -> List[Dict[str, Any]]:
        if kind is None:
            rows = self._get_read_conn().execute(
                "SELECT trace_json FROM numeric_traces WHERE policy_version=?"
                " ORDER BY id", (policy_version,)
            ).fetchall()
        else:
            rows = self._get_read_conn().execute(
                "SELECT trace_json FROM numeric_traces WHERE policy_version=? AND kind=?"
                " ORDER BY id", (policy_version, kind)
            ).fetchall()
        return [json.loads(r["trace_json"]) for r in rows]

    # ---------------- 崩溃恢复 ----------------
    def abort_intent(self, intent_id: str, now_ms: int,
                     batch_id: Optional[str]) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE intents SET status='ABORTED', finished_at_ms=? WHERE intent_id=?",
                (now_ms, intent_id),
            )
            if batch_id:
                conn.execute(
                    "UPDATE batches SET status='ABORTED' WHERE batch_id=? AND status='PENDING'",
                    (batch_id,),
                )

    def save_recovery_report(self, report: Dict[str, Any], now_ms: int) -> int:
        with self.transaction() as conn:
            cur = conn.execute(
                "INSERT INTO recovery_reports(created_at_ms, report_json) VALUES(?,?)",
                (now_ms, json.dumps(report, ensure_ascii=False, sort_keys=True)),
            )
            return int(cur.lastrowid)

    def list_recovery_reports(self) -> List[Dict[str, Any]]:
        return [
            {"id": r["id"], "created_at_ms": r["created_at_ms"],
             "report": json.loads(r["report_json"])}
            for r in self._get_read_conn().execute(
                "SELECT * FROM recovery_reports ORDER BY id"
            ).fetchall()
        ]

    def list_batches(self) -> List[Dict[str, Any]]:
        return [
            {"batch_id": r["batch_id"], "policy_version": r["policy_version"],
             "status": r["status"], "request_count": r["request_count"],
             "intent_id": r["intent_id"]}
            for r in self._get_read_conn().execute(
                "SELECT * FROM batches ORDER BY created_at_ms"
            ).fetchall()
        ]

    def recover(self, now_ms: int) -> Dict[str, Any]:
        """扫描 PENDING 意图并恢复，报告内容只来自数据库中可核验的事实。"""
        pending = self.list_pending_intents()
        actions: List[Dict[str, Any]] = []
        for row in pending:
            kind = row["kind"]
            batch_id = row["batch_id"]
            if kind in ("batch_decision", "stats_rebuild"):
                # 业务写与意图完成在同一事务，未 COMMITTED 即视为全部未发生。
                self.abort_intent(row["intent_id"], now_ms, batch_id)
                actions.append(
                    {
                        "intent_id": row["intent_id"],
                        "kind": kind,
                        "batch_id": batch_id,
                        "action": "marked_aborted",
                        "basis": "intent remained PENDING; its business writes were never"
                                 " committed and are absent from committed tables",
                    }
                )
            elif kind == "policy_submit":
                # 策略行与意图完成同事务，未完成则不存在策略行，直接废弃意图。
                self.abort_intent(row["intent_id"], now_ms, None)
                actions.append(
                    {
                        "intent_id": row["intent_id"],
                        "kind": kind,
                        "action": "marked_aborted",
                        "basis": "no policy row exists outside the unfinished transaction",
                    }
                )
            else:
                self.abort_intent(row["intent_id"], now_ms, batch_id)
                actions.append(
                    {
                        "intent_id": row["intent_id"],
                        "kind": kind,
                        "action": "marked_aborted_unknown_kind",
                        "basis": "unrecognized intent kind treated conservatively",
                    }
                )

        committed_versions = self.list_policy_versions()
        counter_versions = [
            r["policy_version"]
            for r in self._get_read_conn().execute(
                "SELECT policy_version FROM counters ORDER BY policy_version"
            ).fetchall()
        ]
        report = {
            "recovered_at_ms": now_ms,
            "pending_intents_found": len(pending),
            "actions": actions,
            "committed_policy_versions": committed_versions,
            "counter_versions_present": counter_versions,
            "last_complete_policy_version": (
                committed_versions[-1] if committed_versions else None
            ),
            "note": "report derived only from persisted rows; no exit report is promised"
                    " for a process killed mid-transaction",
        }
        report_id = self.save_recovery_report(report, now_ms)
        report["report_id"] = report_id
        return report
