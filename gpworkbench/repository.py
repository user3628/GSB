"""SQLite 仓储层：写操作遵循“先落意图记录，再在同一事务应用并提交”。

意图（intent）生命周期：
  PENDING -- 已在独立小事务落盘，应用事务尚未提交；此刻进程被 kill 后，
             重启恢复时该意图是唯一可依赖的持久化事实。
  COMMITTED -- 应用事务已提交，所有行变更与意图状态在同一事务内原子可见。
  ABORTED -- 恢复流程判定无法前滚（如业务校验不再满足），显式中止。

因此任何已 COMMITTED 的意图，其全部效果必然已持久化；
任何 PENDING 的意图，其效果必然尚未持久化，恢复时前滚或中止即可，
不存在“部分写入”的中间状态。
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from .database import Database
from .errors import CrashSimulated
from .spec import WorkloadSpec


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def row_to_dict(row: sqlite3.Row | None) -> dict | None:
    return dict(row) if row is not None else None


class Repository:
    def __init__(self, database: Database):
        self.db = database
        # 测试钩子：在指定意图落盘后、应用事务提交前抛出，模拟 kill -9
        self.crash_after_intent: str | None = None
        # 测试钩子：下一个意图落盘后崩溃（一次性，按意图类型过滤）
        self.crash_next_intent_type: str | None = None

    # ------------------------------------------------------------------ 意图
    def run_intent(
        self,
        intent_type: str,
        payload: dict,
        apply: Callable[[sqlite3.Connection, str], Any],
    ) -> dict:
        intent_id = f"{intent_type.lower()}-{uuid.uuid4().hex[:16]}"
        created_at = utc_now()
        # 阶段一：意图单独小事务落盘（PENDING）
        conn = self.db.connect()
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO intents(intent_id, intent_type, status, payload, created_at) "
            "VALUES(?, ?, 'PENDING', ?, ?)",
            (intent_id, intent_type, json.dumps(payload, ensure_ascii=False), created_at),
        )
        conn.execute("COMMIT")

        if self.crash_after_intent == intent_id or (
            self.crash_next_intent_type and intent_type == self.crash_next_intent_type
        ):
            self.crash_next_intent_type = None
            raise CrashSimulated(intent_id, intent_type)

        # 阶段二：应用变更 + 意图置为 COMMITTED，单事务原子提交
        with self.db.transaction() as tx:
            result = apply(tx, intent_id)
            tx.execute(
                "UPDATE intents SET status='COMMITTED', committed_at=?, result=? "
                "WHERE intent_id=?",
                (utc_now(), json.dumps(result, ensure_ascii=False), intent_id),
            )
        return {"intent_id": intent_id, "status": "COMMITTED", "result": result}

    def mark_intent_aborted(self, intent_id: str, detail: str) -> None:
        with self.db.transaction() as tx:
            tx.execute(
                "UPDATE intents SET status='ABORTED', committed_at=? WHERE intent_id=? "
                "AND status='PENDING'",
                (utc_now(), intent_id),
            )
            tx.execute(
                "INSERT INTO recovery_events(intent_id, action, detail, created_at) "
                "VALUES(?, 'ABORT', ?, ?)",
                (intent_id, detail, utc_now()),
            )

    def record_recovery_event(self, intent_id: str, action: str, detail: dict) -> None:
        with self.db.transaction() as tx:
            tx.execute(
                "INSERT INTO recovery_events(intent_id, action, detail, created_at) "
                "VALUES(?, ?, ?, ?)",
                (intent_id, action, json.dumps(detail, ensure_ascii=False), utc_now()),
            )

    def list_pending_intents(self) -> list[dict]:
        conn = self.db.connect()
        rows = conn.execute(
            "SELECT * FROM intents WHERE status='PENDING' ORDER BY created_at, intent_id"
        ).fetchall()
        return [dict(row) for row in rows]

    def get_intent(self, intent_id: str) -> dict | None:
        return row_to_dict(
            self.db.connect().execute(
                "SELECT * FROM intents WHERE intent_id=?", (intent_id,)
            ).fetchone()
        )

    # ------------------------------------------------------------- 规格装载
    def load_spec(self, spec: WorkloadSpec) -> dict:
        payload = {
            "groups": [vars(g) for g in spec.groups.values()],
            "nodes": [vars(n) for n in spec.nodes.values()],
            "models": [vars(m) for m in spec.models.values()],
            "requests": [vars(r) for r in spec.requests],
            "source_path": spec.source_path,
        }

        def apply(tx: sqlite3.Connection, intent_id: str) -> dict:
            tx.execute("DELETE FROM batch_items")
            tx.execute("DELETE FROM batches")
            tx.execute("DELETE FROM request_events")
            tx.execute("DELETE FROM requests")
            tx.execute("DELETE FROM nodes")
            tx.execute("DELETE FROM models")
            tx.execute("DELETE FROM quota_groups")
            now = utc_now()
            tx.executemany(
                "INSERT INTO quota_groups(group_id, memory_limit, compute_limit) "
                "VALUES(?, ?, ?)",
                [(g.group_id, g.memory_limit, g.compute_limit) for g in spec.groups.values()],
            )
            tx.executemany(
                "INSERT INTO nodes(node_id, total_memory, total_compute, base_used_memory, "
                "group_id, health) VALUES(?, ?, ?, ?, ?, ?)",
                [
                    (
                        n.node_id,
                        n.total_memory,
                        n.total_compute,
                        n.used_memory,
                        n.group_id,
                        n.health,
                    )
                    for n in spec.nodes.values()
                ],
            )
            tx.executemany(
                "INSERT INTO models(model_id, cost_per_sample, max_batch_samples) "
                "VALUES(?, ?, ?)",
                [
                    (m.model_id, m.cost_per_sample, m.max_batch_samples)
                    for m in spec.models.values()
                ],
            )
            tx.executemany(
                "INSERT INTO requests(request_id, service_name, model_id, memory_need, "
                "compute_need, samples_per_batch, latency_limit_ms, priority, group_id, seq, "
                "state, reserved_memory, reserved_compute, created_at, updated_at) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, 0, ?, ?)",
                [
                    (
                        r.request_id,
                        r.service_name,
                        r.model_id,
                        r.memory_need,
                        r.compute_need,
                        r.samples_per_batch,
                        r.latency_limit_ms,
                        r.priority,
                        r.group_id,
                        r.seq,
                        now,
                        now,
                    )
                    for r in spec.requests
                ],
            )
            return {
                "groups": len(spec.groups),
                "nodes": len(spec.nodes),
                "models": len(spec.models),
                "requests": len(spec.requests),
            }

        return self.run_intent("LOAD_SPEC", payload, apply)

    # ------------------------------------------------------------- 只读视图
    def list_groups(self) -> list[dict]:
        rows = self.db.connect().execute("SELECT * FROM quota_groups ORDER BY group_id").fetchall()
        return [dict(r) for r in rows]

    def list_nodes(self) -> list[dict]:
        rows = self.db.connect().execute("SELECT * FROM nodes ORDER BY node_id").fetchall()
        return [dict(r) for r in rows]

    def get_node(self, tx_or_none, node_id: str) -> dict | None:
        conn = tx_or_none or self.db.connect()
        return row_to_dict(conn.execute(
            "SELECT * FROM nodes WHERE node_id=?", (node_id,)
        ).fetchone())

    def list_models(self) -> list[dict]:
        rows = self.db.connect().execute("SELECT * FROM models ORDER BY model_id").fetchall()
        return [dict(r) for r in rows]

    def get_model(self, model_id: str) -> dict | None:
        return row_to_dict(
            self.db.connect().execute("SELECT * FROM models WHERE model_id=?", (model_id,)).fetchone()
        )

    def list_requests(self, state: str | None = None, group_id: str | None = None) -> list[dict]:
        sql = "SELECT * FROM requests WHERE 1=1"
        params: list[Any] = []
        if state:
            sql += " AND state=?"
            params.append(state)
        if group_id:
            sql += " AND group_id=?"
            params.append(group_id)
        sql += " ORDER BY seq, request_id"
        return [dict(r) for r in self.db.connect().execute(sql, params).fetchall()]

    def get_request(self, request_id: str) -> dict | None:
        return row_to_dict(
            self.db.connect().execute(
                "SELECT * FROM requests WHERE request_id=?", (request_id,)
            ).fetchone()
        )

    def list_batches(self, state: str | None = None) -> list[dict]:
        sql = "SELECT * FROM batches"
        params: list[Any] = []
        if state:
            sql += " WHERE state=?"
            params.append(state)
        sql += " ORDER BY batch_id"
        return [dict(r) for r in self.db.connect().execute(sql, params).fetchall()]

    def get_batch(self, batch_id: str) -> dict | None:
        return row_to_dict(
            self.db.connect().execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        )

    def list_batch_items(self, batch_id: str) -> list[dict]:
        rows = self.db.connect().execute(
            "SELECT bi.*, r.state AS request_state FROM batch_items bi "
            "JOIN requests r ON r.request_id=bi.request_id "
            "WHERE bi.batch_id=? ORDER BY bi.seq",
            (batch_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def list_events(self, request_id: str | None = None) -> list[dict]:
        sql = "SELECT * FROM request_events"
        params: list[Any] = []
        if request_id:
            sql += " WHERE request_id=?"
            params.append(request_id)
        sql += " ORDER BY event_id"
        return [dict(r) for r in self.db.connect().execute(sql, params).fetchall()]

    def list_recovery_events(self) -> list[dict]:
        rows = self.db.connect().execute(
            "SELECT * FROM recovery_events ORDER BY event_id"
        ).fetchall()
        return [dict(r) for r in rows]

    def counts_by_state(self) -> dict[str, int]:
        rows = self.db.connect().execute(
            "SELECT state, COUNT(*) AS n FROM requests GROUP BY state"
        ).fetchall()
        return {row["state"]: row["n"] for row in rows}
