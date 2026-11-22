"""SQLite 持久化层。

数据库文件默认位于 ``data/graph_bench.db``（可通过环境变量覆盖），
启用 WAL 模式，实现“单写者提交 + 多读者并发查询”：

- 写路径持有进程内 :class:`threading.RLock`，并使用 ``BEGIN IMMEDIATE``，
  保证快照提交、增量提交与级联重建严格串行；
- 读路径不加进程锁，仅打开短事务读取某个完整版本，WAL 保证读不阻塞写、
  写不阻塞读，且读者永远看不到未提交事务的中间状态；
- 所有写操作先落意图日志（独立事务先提交），再在第二个事务内整体写入
  快照/资源/依赖/派生结构；派生结构与资源同事务提交，因此不存在
  “上游已更新、下游仍旧值”的可见中间态。

意图日志表结构（也在 docs/design.md 中约定）：

    CREATE TABLE intent_log (
        intent_id      TEXT PRIMARY KEY,
        kind           TEXT NOT NULL,           -- snapshot / increment
        request_hash   TEXT NOT NULL,
        status         TEXT NOT NULL,           -- PENDING/COMMITTED/ABORTED
        base_version   INTEGER,
        target_version INTEGER,
        detail_json    TEXT NOT NULL,
        error_code     TEXT,
        created_at     TEXT NOT NULL,
        updated_at     TEXT NOT NULL
    )
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    version              INTEGER PRIMARY KEY,
    generated_at         TEXT NOT NULL,
    checksum             TEXT NOT NULL,
    supplied_checksum    TEXT,
    created_at           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS resources (
    snapshot_version INTEGER NOT NULL,
    resource_id      TEXT NOT NULL,
    replicas         INTEGER NOT NULL,
    weight_milli     INTEGER NOT NULL,
    desired_version  TEXT NOT NULL,
    ordinal          INTEGER NOT NULL,
    PRIMARY KEY (snapshot_version, resource_id)
);

CREATE TABLE IF NOT EXISTS edges (
    snapshot_version INTEGER NOT NULL,
    resource_id      TEXT NOT NULL,
    parent_id        TEXT NOT NULL,
    ordinal          INTEGER NOT NULL,
    PRIMARY KEY (snapshot_version, resource_id, parent_id)
);

CREATE TABLE IF NOT EXISTS derived (
    snapshot_version INTEGER NOT NULL,
    resource_id      TEXT NOT NULL,
    layer            INTEGER,
    batch            TEXT,
    downstream_json  TEXT NOT NULL,
    weight_consistency REAL NOT NULL,
    status           TEXT NOT NULL,
    PRIMARY KEY (snapshot_version, resource_id)
);

CREATE TABLE IF NOT EXISTS graph_meta (
    snapshot_version INTEGER PRIMARY KEY,
    is_acyclic       INTEGER NOT NULL,
    cycles_json      TEXT NOT NULL,
    blocked_json     TEXT NOT NULL,
    layers_json      TEXT NOT NULL,
    batches_json     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS intent_log (
    intent_id      TEXT PRIMARY KEY,
    kind           TEXT NOT NULL,
    request_hash   TEXT NOT NULL,
    status         TEXT NOT NULL,
    base_version   INTEGER,
    target_version INTEGER,
    detail_json    TEXT NOT NULL,
    error_code     TEXT,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT NOT NULL,
    recovered_at    TEXT NOT NULL,
    report_json     TEXT NOT NULL
);
"""

INTENT_PENDING = "PENDING"
INTENT_COMMITTED = "COMMITTED"
INTENT_ABORTED = "ABORTED"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def default_db_path() -> Path:
    override = os.environ.get("GRAPH_BENCH_DB")
    if override:
        return Path(override)
    return Path(__file__).resolve().parent.parent / "data" / "graph_bench.db"


class Storage:
    """SQLite 封装；线程安全，写操作经 ``write_lock`` 串行化。"""

    def __init__(self, db_path: str | Path | None = None) -> None:
        self.db_path = Path(db_path) if db_path else default_db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.write_lock = threading.RLock()
        self._crash_point: str | None = os.environ.get("GRAPH_BENCH_CRASH_POINT")
        self._init_schema()

    # ---------- 连接与建表 ----------

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self.db_path),
            timeout=30.0,
            check_same_thread=False,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def _init_schema(self) -> None:
        with self._connect() as connection:
            connection.executescript(SCHEMA_SQL)

    @contextmanager
    def read_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN")
            yield connection
        finally:
            connection.rollback()
            connection.close()

    # ---------- 元信息 ----------

    def get_current_version(self) -> int | None:
        with self.read_transaction() as connection:
            row = connection.execute("SELECT value FROM meta WHERE key='current_version'").fetchone()
            return int(row["value"]) if row else None

    def set_current_version(self, connection: sqlite3.Connection, version: int) -> None:
        connection.execute(
            "INSERT INTO meta(key, value) VALUES('current_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(version),),
        )

    # ---------- 意图日志 ----------

    def insert_pending_intent(
        self,
        *,
        intent_id: str,
        kind: str,
        request_hash: str,
        base_version: int | None,
        detail: dict[str, Any],
    ) -> None:
        """第一阶段事务：意图先落盘并独立提交。"""

        now = utc_now_iso()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT INTO intent_log(intent_id, kind, request_hash, status, base_version, "
                "target_version, detail_json, error_code, created_at, updated_at) "
                "VALUES(?, ?, ?, ?, ?, NULL, ?, NULL, ?, ?) "
                "ON CONFLICT(intent_id) DO UPDATE SET kind=excluded.kind, "
                "request_hash=excluded.request_hash, status=excluded.status, "
                "base_version=excluded.base_version, target_version=NULL, "
                "detail_json=excluded.detail_json, error_code=NULL, updated_at=excluded.updated_at",
                (
                    intent_id,
                    kind,
                    request_hash,
                    INTENT_PENDING,
                    base_version,
                    json.dumps(detail, ensure_ascii=False, sort_keys=True),
                    now,
                    now,
                ),
            )
            connection.commit()

    def get_intent(self, intent_id: str) -> dict[str, Any] | None:
        with self.read_transaction() as connection:
            row = connection.execute("SELECT * FROM intent_log WHERE intent_id=?", (intent_id,)).fetchone()
            return dict(row) if row else None

    def list_intents(self) -> list[dict[str, Any]]:
        with self.read_transaction() as connection:
            rows = connection.execute("SELECT * FROM intent_log ORDER BY rowid").fetchall()
            return [dict(row) for row in rows]

    def mark_intent(
        self,
        connection: sqlite3.Connection,
        intent_id: str,
        status: str,
        *,
        target_version: int | None = None,
        error_code: str | None = None,
    ) -> None:
        connection.execute(
            "UPDATE intent_log SET status=?, target_version=?, error_code=?, updated_at=? WHERE intent_id=?",
            (status, target_version, error_code, utc_now_iso(), intent_id),
        )

    def scan_pending_intents(self) -> list[dict[str, Any]]:
        with self.read_transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM intent_log WHERE status=? ORDER BY rowid", (INTENT_PENDING,)
            ).fetchall()
            return [dict(row) for row in rows]

    def save_recovery_report(self, report: dict[str, Any]) -> None:
        now = utc_now_iso()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT INTO recovery_reports(created_at, recovered_at, report_json) VALUES(?, ?, ?)",
                (now, now, json.dumps(report, ensure_ascii=False, sort_keys=True)),
            )
            connection.commit()

    def list_recovery_reports(self) -> list[dict[str, Any]]:
        with self.read_transaction() as connection:
            rows = connection.execute("SELECT * FROM recovery_reports ORDER BY id").fetchall()
            return [dict(row) for row in rows]

    # ---------- 第二阶段：整版本原子写入 ----------

    def crash_if(self, point: str) -> None:
        """测试用崩溃点：命中后以 os._exit 强制终止，模拟 kill -9。

        此时第二阶段事务尚未 commit，SQLite 会在下次打开时丢弃未提交页，
        用以验证重启恢复逻辑。仅在设置 GRAPH_BENCH_CRASH_POINT 时生效。
        """

        if self._crash_point == point:
            self._crash_point = None
            flush = os.open(os.devnull, os.O_WRONLY)
            os.write(flush, b"crash point reached: " + point.encode() + b"\n")
            os.close(flush)
            os._exit(86)

    def commit_version(
        self,
        *,
        intent_id: str,
        version: int,
        generated_at: str,
        checksum: str,
        supplied_checksum: str | None,
        resources: list[dict[str, Any]],
        view: dict[str, Any],
        kind: str,
    ) -> None:
        """第二阶段事务：资源、边、派生结构与版本指针整体提交。

        全部写入在同一 ``BEGIN IMMEDIATE`` 事务中，要么整体可见，要么整体不可见。
        """

        with self.write_lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self.crash_if("before_version_insert")
            connection.execute(
                "INSERT OR REPLACE INTO snapshots(version, generated_at, checksum, supplied_checksum, created_at) "
                "VALUES(?, ?, ?, ?, ?)",
                (version, generated_at, checksum, supplied_checksum, utc_now_iso()),
            )

            for ordinal, item in enumerate(sorted(resources, key=lambda r: r["id"])):
                connection.execute(
                    "INSERT OR REPLACE INTO resources(snapshot_version, resource_id, replicas, "
                    "weight_milli, desired_version, ordinal) VALUES(?, ?, ?, ?, ?, ?)",
                    (
                        version,
                        item["id"],
                        item["replicas"],
                        item["weight_milli"],
                        item["desired_version"],
                        ordinal,
                    ),
                )
                for edge_ordinal, parent in enumerate(sorted(item["parents"])):
                    connection.execute(
                        "INSERT OR REPLACE INTO edges(snapshot_version, resource_id, parent_id, ordinal) "
                        "VALUES(?, ?, ?, ?)",
                        (version, item["id"], parent, edge_ordinal),
                    )

            resource_views = view["resources"]
            for rid in sorted(resource_views):
                entry = resource_views[rid]
                connection.execute(
                    "INSERT OR REPLACE INTO derived(snapshot_version, resource_id, layer, batch, "
                    "downstream_json, weight_consistency, status) VALUES(?, ?, ?, ?, ?, ?, ?)",
                    (
                        version,
                        rid,
                        entry["layer"],
                        entry["batch"],
                        json.dumps(entry["downstream"], ensure_ascii=False),
                        entry["weight_consistency"],
                        entry["status"],
                    ),
                )

            self.crash_if("after_derived_before_commit")
            connection.execute(
                "INSERT OR REPLACE INTO graph_meta(snapshot_version, is_acyclic, cycles_json, "
                "blocked_json, layers_json, batches_json) VALUES(?, ?, ?, ?, ?, ?)",
                (
                    version,
                    1 if view["is_acyclic"] else 0,
                    json.dumps(view["cycles"], ensure_ascii=False),
                    json.dumps(view["blocked_resources"], ensure_ascii=False),
                    json.dumps(view["layers"], ensure_ascii=False),
                    json.dumps(view["batches"], ensure_ascii=False),
                ),
            )
            self.set_current_version(connection, version)
            self.mark_intent(connection, intent_id, INTENT_COMMITTED, target_version=version)
            self.crash_if("before_commit")
            connection.commit()

    def abort_intent(self, intent_id: str, error_code: str) -> None:
        with self.write_lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self.mark_intent(connection, intent_id, INTENT_ABORTED, error_code=error_code)
            connection.commit()

    def rearm_intent(self, intent_id: str) -> None:
        """同 request_id 同内容重试：把 ABORTED 意图重新置为 PENDING。"""

        with self.write_lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "UPDATE intent_log SET status=?, error_code=NULL, updated_at=? WHERE intent_id=?",
                (INTENT_PENDING, utc_now_iso(), intent_id),
            )
            connection.commit()

    # ---------- 读路径 ----------

    def load_snapshot_rows(self, version: int) -> dict[str, Any] | None:
        with self.read_transaction() as connection:
            snapshot = connection.execute(
                "SELECT * FROM snapshots WHERE version=?", (version,)
            ).fetchone()
            if snapshot is None:
                return None
            resources = connection.execute(
                "SELECT * FROM resources WHERE snapshot_version=? ORDER BY ordinal", (version,)
            ).fetchall()
            edges = connection.execute(
                "SELECT * FROM edges WHERE snapshot_version=? ORDER BY resource_id, ordinal", (version,)
            ).fetchall()
            parent_map: dict[str, list[str]] = {}
            for edge in edges:
                parent_map.setdefault(edge["resource_id"], []).append(edge["parent_id"])
            resource_dicts = [
                {
                    "id": row["resource_id"],
                    "replicas": row["replicas"],
                    "parents": parent_map.get(row["resource_id"], []),
                    "weight_milli": row["weight_milli"],
                    "desired_version": row["desired_version"],
                }
                for row in resources
            ]
            return {
                "version": snapshot["version"],
                "generated_at": snapshot["generated_at"],
                "checksum": snapshot["checksum"],
                "supplied_checksum": snapshot["supplied_checksum"],
                "resources": resource_dicts,
            }

    def load_view_rows(self, version: int) -> dict[str, Any] | None:
        with self.read_transaction() as connection:
            meta = connection.execute("SELECT * FROM graph_meta WHERE snapshot_version=?", (version,)).fetchone()
            if meta is None:
                return None
            derived_rows = connection.execute(
                "SELECT * FROM derived WHERE snapshot_version=? ORDER BY resource_id", (version,)
            ).fetchall()
            resources = {}
            for row in derived_rows:
                resources[row["resource_id"]] = {
                    "resource_id": row["resource_id"],
                    "layer": row["layer"],
                    "batch": row["batch"],
                    "downstream": json.loads(row["downstream_json"]),
                    "weight_consistency": row["weight_consistency"],
                    "status": row["status"],
                }
            return {
                "version": version,
                "is_acyclic": bool(meta["is_acyclic"]),
                "cycles": json.loads(meta["cycles_json"]),
                "blocked_resources": json.loads(meta["blocked_json"]),
                "layers": json.loads(meta["layers_json"]),
                "batches": json.loads(meta["batches_json"]),
                "resources": resources,
            }
