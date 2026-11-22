"""基于 SQLite 的持久化层。

写入遵循"先写意图记录并提交，再在单个事务内完成图数据/索引/缓存写入并把
意图标记完成"的两段式流程：

1. 事务 A：插入 ``PENDING`` 意图并提交（崩溃点之后重启可见该意图）。
2. 事务 B：一次性写入新版本的图数据、索引、缓存继承/失效结果、提交标记，
   最后把意图置为 ``COMPLETE`` 并提交。

进程在任一时点被强杀，重启时 :mod:`graphstore.recovery` 都能仅凭已持久化
的事实判断该回滚还是确认。读连接按线程隔离，WAL 模式下读写互不阻塞。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from .indexes import IndexBundle
from .models import Edge, Node
from .snapshot import GraphSnapshot

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS committed_versions (
    version INTEGER PRIMARY KEY,
    generated_at TEXT NOT NULL,
    checksum TEXT NOT NULL,
    committed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS nodes (
    version INTEGER NOT NULL,
    node_id TEXT NOT NULL,
    label TEXT NOT NULL,
    properties TEXT NOT NULL,
    PRIMARY KEY (version, node_id)
);
CREATE TABLE IF NOT EXISTS edges (
    version INTEGER NOT NULL,
    source TEXT NOT NULL,
    edge_type TEXT NOT NULL,
    target TEXT NOT NULL,
    weight REAL NOT NULL,
    PRIMARY KEY (version, source, edge_type, target)
);
CREATE TABLE IF NOT EXISTS label_index (
    version INTEGER NOT NULL,
    label TEXT NOT NULL,
    node_id TEXT NOT NULL,
    PRIMARY KEY (version, label, node_id)
);
CREATE TABLE IF NOT EXISTS adj_index (
    version INTEGER NOT NULL,
    edge_type TEXT NOT NULL,
    source TEXT NOT NULL,
    target TEXT NOT NULL,
    weight REAL NOT NULL,
    PRIMARY KEY (version, edge_type, source, target)
);
CREATE TABLE IF NOT EXISTS query_cache (
    version INTEGER NOT NULL,
    signature TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    mode TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (version, signature)
);
CREATE TABLE IF NOT EXISTS cache_deps (
    version INTEGER NOT NULL,
    signature TEXT NOT NULL,
    dependency TEXT NOT NULL,
    PRIMARY KEY (version, signature, dependency)
);
CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    version INTEGER NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    detail_json TEXT
);
CREATE TABLE IF NOT EXISTS request_runs (
    request_id TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    version INTEGER,
    result_json TEXT,
    error_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""

GRAPH_TABLES = ("nodes", "edges", "label_index", "adj_index", "query_cache", "cache_deps")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class Store:
    def __init__(self, path: str):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._writer = self._connect()
        self._writer.execute("PRAGMA journal_mode=WAL")
        self._writer.executescript(SCHEMA)
        self._writer.commit()
        self._local = threading.local()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    @property
    def reader(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._connect()
            self._local.conn = conn
        return conn

    def close(self):
        self._writer.close()
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()

    # ------------------------------------------------------------------ 意图
    def create_intent(self, kind: str, version: int, payload: dict) -> str:
        intent_id = uuid.uuid4().hex
        now = utc_now_iso()
        self._writer.execute(
            "INSERT INTO intents VALUES (?, ?, ?, ?, ?, ?, ?, NULL)",
            (
                intent_id,
                kind,
                version,
                "PENDING",
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
                now,
                now,
            ),
        )
        return intent_id

    def set_intent_status(self, intent_id: str, status: str, detail: Optional[dict] = None):
        self._writer.execute(
            "UPDATE intents SET status=?, updated_at=?, detail_json=? WHERE intent_id=?",
            (status, utc_now_iso(), json.dumps(detail) if detail else None, intent_id),
        )

    def list_intents(self) -> List[sqlite3.Row]:
        return list(self._writer.execute("SELECT * FROM intents ORDER BY created_at"))

    def is_version_committed(self, version: int) -> bool:
        row = self._writer.execute(
            "SELECT 1 FROM committed_versions WHERE version=?", (version,)
        ).fetchone()
        return row is not None

    def current_version(self) -> Optional[int]:
        row = self.reader.execute(
            "SELECT MAX(version) AS v FROM committed_versions"
        ).fetchone()
        return row["v"]

    # -------------------------------------------------------------- 版本物化
    def materialize_version(
        self,
        snapshot: GraphSnapshot,
        index: IndexBundle,
        intent_id: str,
        *,
        previous_version: Optional[int],
        affected_dependencies: Optional[Sequence[str]] = None,
    ):
        """单事务写入新版本图数据、索引与缓存继承/失效，并提交版本标记。"""
        writer = self._writer
        writer.execute("BEGIN IMMEDIATE")
        try:
            version = snapshot.version
            writer.executemany(
                "INSERT INTO nodes VALUES (?, ?, ?, ?)",
                [
                    (version, node.id, node.label, json.dumps(node.properties, sort_keys=True))
                    for node in snapshot.nodes.values()
                ],
            )
            writer.executemany(
                "INSERT INTO edges VALUES (?, ?, ?, ?, ?)",
                [
                    (version, edge.source, edge.type, edge.target, edge.weight)
                    for edge in snapshot.edges.values()
                ],
            )
            writer.executemany(
                "INSERT INTO label_index VALUES (?, ?, ?)",
                [
                    (version, label, node_id)
                    for label, ids in index.label_index.items()
                    for node_id in ids
                ],
            )
            writer.executemany(
                "INSERT INTO adj_index VALUES (?, ?, ?, ?, ?)",
                [
                    (version, edge_type, source, target, weight)
                    for (edge_type, source), targets in index.adj_index.items()
                    for target, weight in targets.items()
                ],
            )
            invalidated = 0
            if previous_version is not None:
                if affected_dependencies:
                    placeholders = ",".join("?" for _ in affected_dependencies)
                    invalidated = writer.execute(
                        f"""
                        DELETE FROM query_cache
                         WHERE version=? AND signature IN (
                            SELECT signature FROM cache_deps
                             WHERE version=? AND dependency IN ({placeholders})
                         )
                        """,
                        [previous_version, previous_version, *affected_dependencies],
                    ).rowcount
                    writer.execute(
                        f"""
                        DELETE FROM cache_deps
                         WHERE version=? AND signature IN (
                            SELECT signature FROM cache_deps
                             WHERE version=? AND dependency IN ({placeholders})
                         )
                        """,
                        [previous_version, previous_version, *affected_dependencies],
                    )
            writer.execute(
                "INSERT INTO query_cache "
                "SELECT ?, signature, request_hash, mode, result_json, created_at "
                "FROM query_cache WHERE version=?",
                (version, previous_version),
            )
            writer.execute(
                "INSERT OR IGNORE INTO cache_deps "
                "SELECT ?, signature, dependency FROM cache_deps WHERE version=?",
                (version, previous_version),
            )
            writer.execute(
                "INSERT INTO committed_versions VALUES (?, ?, ?, ?)",
                (version, snapshot.generated_at, snapshot.checksum, utc_now_iso()),
            )
            writer.execute(
                "UPDATE intents SET status='COMPLETE', updated_at=?, detail_json=? WHERE intent_id=?",
                (
                    utc_now_iso(),
                    json.dumps({"invalidated_cache_entries": invalidated}),
                    intent_id,
                ),
            )
            writer.execute("COMMIT")
            return invalidated
        except Exception:
            writer.execute("ROLLBACK")
            raise

    def delete_version(self, version: int) -> Dict[str, int]:
        writer = self._writer
        writer.execute("BEGIN IMMEDIATE")
        counts = {}
        try:
            for table in GRAPH_TABLES:
                counts[table] = writer.execute(
                    f"DELETE FROM {table} WHERE version=?", (version,)
                ).rowcount
            writer.execute("DELETE FROM committed_versions WHERE version=?", (version,))
            writer.execute("COMMIT")
            return counts
        except Exception:
            writer.execute("ROLLBACK")
            raise

    def prune_old_versions(self, keep_version: int):
        writer = self._writer
        writer.execute("BEGIN IMMEDIATE")
        try:
            for table in GRAPH_TABLES:
                writer.execute(f"DELETE FROM {table} WHERE version<>?", (keep_version,))
            writer.execute("DELETE FROM committed_versions WHERE version<>?", (keep_version,))
            writer.execute("COMMIT")
        except Exception:
            writer.execute("ROLLBACK")
            raise

    def load_version(self, version: int) -> Optional[tuple[GraphSnapshot, IndexBundle]]:
        reader = self.reader
        marker = reader.execute(
            "SELECT * FROM committed_versions WHERE version=?", (version,)
        ).fetchone()
        if marker is None:
            return None
        nodes = {}
        for row in reader.execute("SELECT * FROM nodes WHERE version=?", (version,)):
            nodes[row["node_id"]] = Node(
                id=row["node_id"],
                label=row["label"],
                properties=json.loads(row["properties"]),
            )
        edges = {}
        for row in reader.execute("SELECT * FROM edges WHERE version=?", (version,)):
            edge = Edge(
                source=row["source"],
                target=row["target"],
                type=row["edge_type"],
                weight=row["weight"],
            )
            edges[(edge.source, edge.type, edge.target)] = edge
        snapshot = GraphSnapshot(
            version=version,
            generated_at=marker["generated_at"],
            nodes=nodes,
            edges=edges,
            checksum=marker["checksum"],
        )
        return snapshot, IndexBundle.from_snapshot(snapshot)

    # ------------------------------------------------------------------ 缓存
    def cache_get(self, version: int, signature: str) -> Optional[dict]:
        row = self.reader.execute(
            "SELECT * FROM query_cache WHERE version=? AND signature=?",
            (version, signature),
        ).fetchone()
        if row is None:
            return None
        return {
            "signature": signature,
            "mode": row["mode"],
            "result": json.loads(row["result_json"]),
            "created_at": row["created_at"],
        }

    def cache_put(
        self,
        version: int,
        signature: str,
        request_hash: str,
        mode: str,
        result: dict,
        dependencies: Sequence[str],
        intent_id: str,
    ):
        writer = self._writer
        writer.execute("BEGIN IMMEDIATE")
        try:
            writer.execute(
                "INSERT OR REPLACE INTO query_cache VALUES (?, ?, ?, ?, ?, ?)",
                (
                    version,
                    signature,
                    request_hash,
                    mode,
                    json.dumps(result, sort_keys=True, separators=(",", ":")),
                    utc_now_iso(),
                ),
            )
            writer.executemany(
                "INSERT OR IGNORE INTO cache_deps VALUES (?, ?, ?)",
                [(version, signature, dep) for dep in dependencies],
            )
            writer.execute(
                "UPDATE intents SET status='COMPLETE', updated_at=? WHERE intent_id=?",
                (utc_now_iso(), intent_id),
            )
            writer.execute("COMMIT")
        except Exception:
            writer.execute("ROLLBACK")
            raise

    def cache_dependencies(self, version: int, signature: str) -> List[str]:
        rows = self.reader.execute(
            "SELECT dependency FROM cache_deps WHERE version=? AND signature=? ORDER BY dependency",
            (version, signature),
        ).fetchall()
        return [row["dependency"] for row in rows]

    # ------------------------------------------------------------- 请求去重
    def get_request_run(self, request_id: str) -> Optional[sqlite3.Row]:
        return self.reader.execute(
            "SELECT * FROM request_runs WHERE request_id=?", (request_id,)
        ).fetchone()

    def save_request_run(self, request_id: str, request_hash: str, status: str,
                         version: Optional[int], result: Optional[dict],
                         error: Optional[dict]):
        self._writer.execute(
            "INSERT OR REPLACE INTO request_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                request_id,
                request_hash,
                status,
                version,
                json.dumps(result) if result is not None else None,
                json.dumps(error) if error is not None else None,
                utc_now_iso(),
                utc_now_iso(),
            ),
        )

    # --------------------------------------------------------------- 完整性
    def verify_persistence(self, version: int) -> List[str]:
        """交叉核对存储的图数据与索引，返回问题描述列表（空列表表示一致）。"""
        _, stored = self.load_version(version)  # 重建索引
        problems: List[str] = []
        reader = self.reader
        stored_label = {
            (row["label"], row["node_id"])
            for row in reader.execute(
                "SELECT label, node_id FROM label_index WHERE version=?", (version,)
            )
        }
        rebuilt_label = {
            (label, node_id)
            for label, ids in stored.label_index.items()
            for node_id in ids
        }
        if stored_label != rebuilt_label:
            problems.append("label_index 与节点数据不一致")
        stored_adj = {
            (row["edge_type"], row["source"], row["target"], row["weight"])
            for row in reader.execute(
                "SELECT edge_type, source, target, weight FROM adj_index WHERE version=?",
                (version,),
            )
        }
        rebuilt_adj = {
            (edge_type, source, target, weight)
            for (edge_type, source), targets in stored.adj_index.items()
            for target, weight in targets.items()
        }
        if stored_adj != rebuilt_adj:
            problems.append("adj_index 与边数据不一致")
        return problems
