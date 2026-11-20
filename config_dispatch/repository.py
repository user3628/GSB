"""SQLite 持久化层：快照、计划、组件状态、状态审计与分发意图。

写操作遵循“先落意图记录再提交”的原则；组件状态迁移用带版本断言的
单行 UPDATE 实现 CAS，非法迁移不会修改任何持久化状态。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Iterable

from .errors import InvalidTransitionError
from .models import ComponentDeclaration, Snapshot
from .state_machine import ensure_transition

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS snapshots (
    version INTEGER PRIMARY KEY,
    generated_at TEXT NOT NULL,
    config_items TEXT NOT NULL,
    checksum TEXT NOT NULL,
    submitted_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS component_declarations (
    component_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dispatch_plans (
    version INTEGER PRIMARY KEY,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(version) REFERENCES snapshots(version)
);

CREATE TABLE IF NOT EXISTS dispatch_runs (
    version INTEGER PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    failure_policy TEXT NOT NULL,
    status TEXT NOT NULL,
    error_code TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(version) REFERENCES snapshots(version)
);

CREATE TABLE IF NOT EXISTS component_states (
    version INTEGER NOT NULL,
    component_id TEXT NOT NULL,
    state TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    error_code TEXT,
    error_message TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(version, component_id),
    FOREIGN KEY(version) REFERENCES snapshots(version)
);

CREATE TABLE IF NOT EXISTS state_transition_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL,
    component_id TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT NOT NULL,
    reason TEXT,
    error_code TEXT,
    created_at TEXT NOT NULL
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Repository:
    def __init__(self, db_path: str):
        self.db_path = db_path
        # check_same_thread=False：协调器用应用级锁串行化写者，
        # 只读查询使用独立短连接，允许与写事务并发（WAL）。
        self._writer_lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30.0)
        self._conn.row_factory = sqlite3.Row
        with self._writer_lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        with self._writer_lock:
            self._conn.close()

    def _read_connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    # ------------------------------------------------------------------ 快照
    def latest_version(self) -> int | None:
        with self._writer_lock:
            row = self._conn.execute("SELECT MAX(version) AS v FROM snapshots").fetchone()
        return row["v"] if row and row["v"] is not None else None

    def insert_snapshot(self, snapshot: Snapshot) -> None:
        with self._writer_lock:
            self._conn.execute(
                "INSERT INTO snapshots(version, generated_at, config_items, checksum, submitted_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (
                    snapshot.version,
                    snapshot.generated_at,
                    json.dumps(snapshot.config_items, sort_keys=True, ensure_ascii=False),
                    snapshot.checksum,
                    utc_now(),
                ),
            )
            self._conn.commit()

    def get_snapshot(self, version: int) -> Snapshot | None:
        with self._writer_lock:
            row = self._conn.execute(
                "SELECT * FROM snapshots WHERE version = ?", (version,)
            ).fetchone()
        if row is None:
            return None
        return Snapshot(
            version=row["version"],
            generated_at=row["generated_at"],
            config_items=json.loads(row["config_items"]),
            checksum=row["checksum"],
        )

    def previous_snapshot(self, version: int) -> Snapshot | None:
        with self._writer_lock:
            row = self._conn.execute(
                "SELECT * FROM snapshots WHERE version < ? ORDER BY version DESC LIMIT 1",
                (version,),
            ).fetchone()
        if row is None:
            return None
        return Snapshot(
            version=row["version"],
            generated_at=row["generated_at"],
            config_items=json.loads(row["config_items"]),
            checksum=row["checksum"],
        )

    def list_snapshot_versions(self) -> list[int]:
        with self._writer_lock:
            rows = self._conn.execute("SELECT version FROM snapshots ORDER BY version").fetchall()
        return [row["version"] for row in rows]

    # ------------------------------------------------------------ 组件声明
    def replace_declarations(self, declarations: dict[str, ComponentDeclaration] | Iterable[ComponentDeclaration]) -> None:
        now = utc_now()
        if isinstance(declarations, dict):
            declarations = declarations.values()
        with self._writer_lock:
            self._conn.execute("DELETE FROM component_declarations")
            for declaration in declarations:
                self._conn.execute(
                    "INSERT INTO component_declarations(component_id, payload, updated_at)"
                    " VALUES (?, ?, ?)",
                    (declaration.component_id, json.dumps(declaration.to_dict(), ensure_ascii=False), now),
                )
            self._conn.commit()

    def load_declarations(self) -> dict[str, ComponentDeclaration]:
        with self._writer_lock:
            rows = self._conn.execute(
                "SELECT payload FROM component_declarations ORDER BY component_id"
            ).fetchall()
        result: dict[str, ComponentDeclaration] = {}
        for row in rows:
            declaration = ComponentDeclaration.from_dict(json.loads(row["payload"]))
            result[declaration.component_id] = declaration
        return result

    # ---------------------------------------------------------------- 计划
    def save_plan(self, version: int, plan_payload: dict) -> None:
        with self._writer_lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO dispatch_plans(version, payload, created_at)"
                " VALUES (?, ?, ?)",
                (version, json.dumps(plan_payload, ensure_ascii=False, sort_keys=True), utc_now()),
            )
            self._conn.commit()

    def get_plan(self, version: int) -> dict | None:
        with self._writer_lock:
            row = self._conn.execute(
                "SELECT payload FROM dispatch_plans WHERE version = ?", (version,)
            ).fetchone()
        return json.loads(row["payload"]) if row else None

    # ------------------------------------------------------------ 分发意图
    def create_run_intent(
        self, version: int, idempotency_key: str, failure_policy: str
    ) -> bool:
        """先落意图记录。已存在同版本意图时返回 False（复用既有运行）。"""

        now = utc_now()
        with self._writer_lock:
            existing = self._conn.execute(
                "SELECT version FROM dispatch_runs WHERE version = ?", (version,)
            ).fetchone()
            if existing is not None:
                return False
            self._conn.execute(
                "INSERT INTO dispatch_runs(version, idempotency_key, failure_policy, status,"
                " error_code, error_message, created_at, updated_at)"
                " VALUES (?, ?, ?, 'PENDING', NULL, NULL, ?, ?)",
                (version, idempotency_key, failure_policy, now, now),
            )
            self._conn.commit()
            return True

    def get_run(self, version: int) -> dict | None:
        with self._writer_lock:
            row = self._conn.execute(
                "SELECT * FROM dispatch_runs WHERE version = ?", (version,)
            ).fetchone()
        return dict(row) if row else None

    def update_run_status(
        self,
        version: int,
        status: str,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        with self._writer_lock:
            self._conn.execute(
                "UPDATE dispatch_runs SET status = ?, error_code = ?, error_message = ?,"
                " updated_at = ? WHERE version = ?",
                (status, error_code, error_message, utc_now(), version),
            )
            self._conn.commit()

    def list_unfinished_runs(self) -> list[dict]:
        with self._writer_lock:
            rows = self._conn.execute(
                "SELECT * FROM dispatch_runs WHERE status NOT IN"
                " ('COMPLETED', 'PARTIAL', 'ROLLED_BACK', 'ABORTED') ORDER BY version"
            ).fetchall()
        return [dict(row) for row in rows]

    def list_runs(self) -> list[dict]:
        with self._writer_lock:
            rows = self._conn.execute(
                "SELECT * FROM dispatch_runs ORDER BY version"
            ).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------ 组件状态
    def ensure_component_state(
        self, version: int, component_id: str, idempotency_key: str
    ) -> str:
        """幂等确保组件存在 PENDING 初始记录，返回当前状态。"""

        with self._writer_lock:
            row = self._conn.execute(
                "SELECT state FROM component_states WHERE version = ? AND component_id = ?",
                (version, component_id),
            ).fetchone()
            if row is None:
                now = utc_now()
                self._conn.execute(
                    "INSERT INTO component_states(version, component_id, state, idempotency_key,"
                    " error_code, error_message, updated_at) VALUES (?, ?, 'PENDING', ?, NULL, NULL, ?)",
                    (version, component_id, idempotency_key, now),
                )
                self._conn.execute(
                    "INSERT INTO state_transition_audit(version, component_id, from_state,"
                    " to_state, reason, error_code, created_at) VALUES (?, ?, NULL, 'PENDING', ?, NULL, ?)",
                    (version, component_id, "initial", now),
                )
                self._conn.commit()
                return "PENDING"
            return row["state"]

    def transition_component_state(
        self,
        version: int,
        component_id: str,
        target: str,
        reason: str,
        error_code: str | None = None,
        error_message: str | None = None,
        idempotency_key: str | None = None,
    ) -> str:
        """带状态机校验与版本断言的 CAS 迁移；非法迁移不修改任何数据。"""

        with self._writer_lock:
            row = self._conn.execute(
                "SELECT state FROM component_states WHERE version = ? AND component_id = ?",
                (version, component_id),
            ).fetchone()
            if row is None:
                raise InvalidTransitionError(
                    f"组件 {component_id} 在版本 {version} 不存在状态记录",
                    details={"version": version, "component_id": component_id, "target": target},
                )
            current = row["state"]
            ensure_transition(current, target)  # 非法迁移在此抛出，事务内不产生写入
            now = utc_now()
            if idempotency_key is not None:
                self._conn.execute(
                    "UPDATE component_states SET state = ?, idempotency_key = ?, error_code = ?,"
                    " error_message = ?, updated_at = ? WHERE version = ? AND component_id = ?",
                    (target, idempotency_key, error_code, error_message, now, version, component_id),
                )
            else:
                self._conn.execute(
                    "UPDATE component_states SET state = ?, error_code = ?, error_message = ?,"
                    " updated_at = ? WHERE version = ? AND component_id = ?",
                    (target, error_code, error_message, now, version, component_id),
                )
            self._conn.execute(
                "INSERT INTO state_transition_audit(version, component_id, from_state, to_state,"
                " reason, error_code, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (version, component_id, current, target, reason, error_code, now),
            )
            self._conn.commit()
            return target

    def get_component_state(self, version: int, component_id: str) -> dict | None:
        with self._writer_lock:
            row = self._conn.execute(
                "SELECT * FROM component_states WHERE version = ? AND component_id = ?",
                (version, component_id),
            ).fetchone()
        return dict(row) if row else None

    def list_component_states(self, version: int) -> list[dict]:
        with self._writer_lock:
            rows = self._conn.execute(
                "SELECT * FROM component_states WHERE version = ? ORDER BY component_id",
                (version,),
            ).fetchall()
        return [dict(row) for row in rows]

    def state_trajectory(self, version: int, component_id: str) -> list[dict]:
        with self._writer_lock:
            rows = self._conn.execute(
                "SELECT from_state, to_state, reason, error_code, created_at"
                " FROM state_transition_audit WHERE version = ? AND component_id = ? ORDER BY id",
                (version, component_id),
            ).fetchall()
        return [dict(row) for row in rows]

    def all_trajectories(self, version: int) -> dict[str, list[dict]]:
        with self._writer_lock:
            rows = self._conn.execute(
                "SELECT component_id, from_state, to_state, reason, error_code, created_at"
                " FROM state_transition_audit WHERE version = ? ORDER BY id",
                (version,),
            ).fetchall()
        trajectories: dict[str, list[dict]] = {}
        for row in rows:
            trajectories.setdefault(row["component_id"], []).append(dict(row))
        return trajectories

    # ------------------------------------------------------------ 只读视图
    def read_only_snapshot_view(self) -> dict[str, Any]:
        """独立只读连接读取一致视图，供验证工具与查询 API 使用。"""

        connection = self._read_connection()
        try:
            snapshots = [
                dict(row)
                for row in connection.execute("SELECT version, checksum FROM snapshots ORDER BY version")
            ]
            runs = [dict(row) for row in connection.execute("SELECT * FROM dispatch_runs ORDER BY version")]
            states = [
                dict(row)
                for row in connection.execute(
                    "SELECT version, component_id, state, error_code FROM component_states"
                    " ORDER BY version, component_id"
                )
            ]
            audits = [
                dict(row)
                for row in connection.execute(
                    "SELECT version, component_id, from_state, to_state FROM"
                    " state_transition_audit ORDER BY id"
                )
            ]
        finally:
            connection.close()
        return {"snapshots": snapshots, "runs": runs, "states": states, "audits": audits}
