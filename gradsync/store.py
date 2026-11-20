from __future__ import annotations

import base64
import json
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import numpy as np

from .aggregation import sha256_json
from .domain import FinalizationPolicy, RoundState, assert_legal_transition
from .errors import ErrorCode, GradSyncError


SCHEMA = """
CREATE TABLE IF NOT EXISTS rounds (
    round_id TEXT PRIMARY KEY,
    seed INTEGER NOT NULL,
    shard_count INTEGER NOT NULL,
    shard_dim INTEGER NOT NULL,
    worker_ids_json TEXT NOT NULL,
    sample_counts_json TEXT NOT NULL,
    worker_endpoints_json TEXT NOT NULL,
    policy TEXT NOT NULL,
    status TEXT NOT NULL,
    collect_deadline_at REAL NOT NULL,
    max_wait_deadline_at REAL NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    finalization_policy TEXT,
    snapshot_json TEXT,
    snapshot_hash TEXT,
    last_error_code TEXT
);

CREATE TABLE IF NOT EXISTS submissions (
    round_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    shard_id INTEGER NOT NULL,
    sample_count INTEGER NOT NULL,
    gradient_blob BLOB NOT NULL,
    content_hash TEXT NOT NULL,
    received_at REAL NOT NULL,
    late INTEGER NOT NULL,
    PRIMARY KEY (round_id, node_id, shard_id),
    FOREIGN KEY (round_id) REFERENCES rounds(round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS aggregate_results (
    round_id TEXT PRIMARY KEY,
    tensor_blob BLOB NOT NULL,
    shard_count INTEGER NOT NULL,
    shard_dim INTEGER NOT NULL,
    digest_sha256 TEXT NOT NULL,
    element_sum REAL NOT NULL,
    element_sum_text TEXT NOT NULL,
    missing_nodes_json TEXT NOT NULL,
    missing_blocks_json TEXT NOT NULL,
    degraded INTEGER NOT NULL,
    policy TEXT NOT NULL,
    snapshot_hash TEXT NOT NULL,
    computed_at REAL NOT NULL,
    FOREIGN KEY (round_id) REFERENCES rounds(round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    round_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    snapshot_hash TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    error_code TEXT,
    error_message TEXT,
    FOREIGN KEY (round_id) REFERENCES rounds(round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT,
    at REAL NOT NULL,
    details_json TEXT NOT NULL,
    FOREIGN KEY (round_id) REFERENCES rounds(round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS deliveries (
    round_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    status TEXT NOT NULL,
    http_status INTEGER,
    error TEXT,
    attempted_at REAL NOT NULL,
    PRIMARY KEY (round_id, node_id),
    FOREIGN KEY (round_id) REFERENCES rounds(round_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS recovery_runs (
    recovery_id TEXT PRIMARY KEY,
    started_at REAL NOT NULL,
    finished_at REAL NOT NULL,
    report_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rounds_status ON rounds(status);
CREATE INDEX IF NOT EXISTS idx_events_round ON events(round_id, event_id);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);
"""


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _parse_json(value: str | None, default: Any) -> Any:
    if value is None:
        return default
    return json.loads(value)


class SQLiteStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(self.path), timeout=15) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.executescript(SCHEMA)

    @contextmanager
    def _connect(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.path), timeout=15, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=15000")
        if write:
            conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            if write:
                conn.commit()
        except Exception:
            if write:
                conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _round_from_row(row: sqlite3.Row) -> dict[str, Any]:
        data = dict(row)
        data["worker_ids"] = _parse_json(data.pop("worker_ids_json"), [])
        data["sample_counts"] = _parse_json(data.pop("sample_counts_json"), {})
        data["worker_endpoints"] = _parse_json(data.pop("worker_endpoints_json"), {})
        data["snapshot"] = _parse_json(data.pop("snapshot_json"), None)
        return data

    @staticmethod
    def _event(conn: sqlite3.Connection, round_id: str, event_type: str, at: float, **kwargs: Any) -> None:
        from_state = kwargs.pop("from_state", None)
        to_state = kwargs.pop("to_state", None)
        details = kwargs.pop("details", {})
        conn.execute(
            "INSERT INTO events(round_id,event_type,from_state,to_state,at,details_json) VALUES(?,?,?,?,?,?)",
            (round_id, event_type, from_state, to_state, float(at), _json(details)),
        )

    def require_round(self, round_id: str, *, write: bool = False) -> dict[str, Any]:
        with self._connect(write=write) as conn:
            row = conn.execute("SELECT * FROM rounds WHERE round_id=?", (round_id,)).fetchone()
            if row is None:
                raise GradSyncError(
                    ErrorCode.E_ROUND_NOT_FOUND,
                    "轮次不存在",
                    round_id=round_id,
                )
            return self._round_from_row(row)

    def get_round(self, round_id: str) -> dict[str, Any]:
        return self.require_round(round_id)

    def list_rounds(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM rounds ORDER BY created_at, round_id").fetchall()
            return [self._round_from_row(row) for row in rows]

    def create_round(self, spec: dict[str, Any], *, now: float) -> dict[str, Any]:
        collect_deadline_at = now + float(spec["collect_seconds"])
        max_wait_deadline_at = collect_deadline_at + float(spec["max_wait_seconds"])
        with self._connect(write=True) as conn:
            exists = conn.execute(
                "SELECT 1 FROM rounds WHERE round_id=?", (spec["round_id"],)
            ).fetchone()
            if exists is not None:
                raise GradSyncError(
                    ErrorCode.E_INVALID_SUBMISSION,
                    "轮次已存在",
                    round_id=spec["round_id"],
                )
            conn.execute(
                """
                INSERT INTO rounds(
                    round_id, seed, shard_count, shard_dim, worker_ids_json,
                    sample_counts_json, worker_endpoints_json, policy, status,
                    collect_deadline_at, max_wait_deadline_at, created_at, updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    spec["round_id"],
                    int(spec.get("seed", 0)),
                    int(spec["shard_count"]),
                    int(spec["shard_dim"]),
                    _json(list(spec["worker_ids"])),
                    _json(dict(spec["sample_counts"])),
                    _json(dict(spec.get("worker_endpoints", {}))),
                    FinalizationPolicy(spec["policy"]).value,
                    RoundState.COLLECTING.value,
                    collect_deadline_at,
                    max_wait_deadline_at,
                    now,
                    now,
                ),
            )
            self._event(
                conn,
                spec["round_id"],
                "ROUND_CREATED",
                now,
                to_state=RoundState.COLLECTING.value,
                details={
                    "collect_deadline_at": collect_deadline_at,
                    "max_wait_deadline_at": max_wait_deadline_at,
                    "policy": FinalizationPolicy(spec["policy"]).value,
                },
            )
        return self.require_round(spec["round_id"])

    def list_submissions(self, round_id: str, *, include_blob: bool = False) -> list[dict[str, Any]]:
        columns = "node_id,shard_id,sample_count,content_hash,received_at,late"
        if include_blob:
            columns += ",gradient_blob"
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT {columns} FROM submissions WHERE round_id=? ORDER BY shard_id,node_id",
                (round_id,),
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["late"] = bool(item["late"])
                if include_blob:
                    item["gradient_blob"] = bytes(item["gradient_blob"])
                result.append(item)
            return result

    def get_submission(self, round_id: str, node_id: str, shard_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM submissions WHERE round_id=? AND node_id=? AND shard_id=?",
                (round_id, node_id, shard_id),
            ).fetchone()
            if row is None:
                return None
            item = dict(row)
            item["gradient_blob"] = bytes(item["gradient_blob"])
            item["late"] = bool(item["late"])
            return item

    def record_submission(
        self,
        *,
        round_id: str,
        node_id: str,
        shard_id: int,
        sample_count: int,
        gradient_blob: bytes,
        content_hash: str,
        received_at: float,
    ) -> dict[str, Any]:
        with self._connect(write=True) as conn:
            row = conn.execute("SELECT * FROM rounds WHERE round_id=?", (round_id,)).fetchone()
            if row is None:
                raise GradSyncError(ErrorCode.E_ROUND_NOT_FOUND, "轮次不存在", round_id=round_id)
            round_data = self._round_from_row(row)
            existing = conn.execute(
                "SELECT * FROM submissions WHERE round_id=? AND node_id=? AND shard_id=?",
                (round_id, node_id, shard_id),
            ).fetchone()
            if existing is not None:
                if existing["content_hash"] == content_hash:
                    self._event(
                        conn,
                        round_id,
                        "DUPLICATE_SUBMISSION",
                        received_at,
                        from_state=round_data["status"],
                        to_state=round_data["status"],
                        details={
                            "node_id": node_id,
                            "shard_id": shard_id,
                            "content_hash": content_hash,
                            "counted": False,
                        },
                    )
                    return {
                        "outcome": "DUPLICATE",
                        "duplicate": True,
                        "late": bool(existing["late"]),
                        "state": round_data["status"],
                        "content_hash": content_hash,
                    }
                self._event(
                    conn,
                    round_id,
                    "CONFLICTING_REPORT_REJECTED",
                    received_at,
                    from_state=round_data["status"],
                    to_state=round_data["status"],
                    details={
                        "node_id": node_id,
                        "shard_id": shard_id,
                        "first_content_hash": existing["content_hash"],
                        "incoming_content_hash": content_hash,
                    },
                )
                return {
                    "outcome": "CONFLICT",
                    "duplicate": False,
                    "state": round_data["status"],
                    "first_content_hash": existing["content_hash"],
                    "incoming_content_hash": content_hash,
                }

            state = RoundState(round_data["status"])
            if state in {RoundState.AGGREGATED, RoundState.ABORTED}:
                raise GradSyncError(
                    ErrorCode.E_INVALID_TRANSITION,
                    f"轮次处于终态 {state.value}，拒绝新分片",
                    round_id=round_id,
                    details={"state": state.value},
                )
            if node_id not in round_data["worker_ids"]:
                raise GradSyncError(
                    ErrorCode.E_INVALID_SUBMISSION,
                    "未知工作节点",
                    round_id=round_id,
                    details={"node_id": node_id},
                )
            if not 0 <= shard_id < round_data["shard_count"]:
                raise GradSyncError(
                    ErrorCode.E_INVALID_SUBMISSION,
                    "未知参数分片",
                    round_id=round_id,
                    details={"shard_id": shard_id},
                )
            expected_count = int(round_data["sample_counts"].get(node_id, -1))
            if sample_count != expected_count:
                raise GradSyncError(
                    ErrorCode.E_INVALID_SUBMISSION,
                    "提交样本数与轮次元数据不一致",
                    round_id=round_id,
                    details={
                        "node_id": node_id,
                        "expected_sample_count": expected_count,
                        "actual_sample_count": sample_count,
                    },
                )
            expected_bytes = int(round_data["shard_dim"]) * 8
            if len(gradient_blob) != expected_bytes:
                raise GradSyncError(
                    ErrorCode.E_INVALID_SUBMISSION,
                    "梯度块字节长度与轮次元数据不一致",
                    round_id=round_id,
                    details={
                        "expected_bytes": expected_bytes,
                        "actual_bytes": len(gradient_blob),
                    },
                )

            snapshot_locked = round_data["snapshot"] is not None
            late = snapshot_locked or float(received_at) > float(round_data["collect_deadline_at"])
            conn.execute(
                """
                INSERT INTO submissions(
                    round_id,node_id,shard_id,sample_count,gradient_blob,
                    content_hash,received_at,late
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    round_id,
                    node_id,
                    int(shard_id),
                    int(sample_count),
                    sqlite3.Binary(gradient_blob),
                    content_hash,
                    float(received_at),
                    1 if late else 0,
                ),
            )
            self._event(
                conn,
                round_id,
                "SUBMISSION_LATE_QUEUED" if late else "SUBMISSION_ACCEPTED",
                received_at,
                from_state=state.value,
                to_state=state.value,
                details={
                    "node_id": node_id,
                    "shard_id": shard_id,
                    "late": late,
                    "content_hash": content_hash,
                    "queued_after_snapshot": snapshot_locked,
                },
            )

            new_state = state
            if state == RoundState.COLLECTING:
                submissions = conn.execute(
                    "SELECT node_id,shard_id,late,received_at FROM submissions WHERE round_id=?",
                    (round_id,),
                ).fetchall()
                expected_blocks = len(round_data["worker_ids"]) * int(round_data["shard_count"])
                if round_data["policy"] == FinalizationPolicy.DEGRADED.value:
                    complete = sum(1 for item in submissions if not item["late"]) == expected_blocks
                else:
                    complete = (
                        sum(
                            1
                            for item in submissions
                            if float(item["received_at"]) <= float(round_data["max_wait_deadline_at"])
                        )
                        == expected_blocks
                    )
                if complete:
                    assert_legal_transition(state, RoundState.READY)
                    conn.execute(
                        "UPDATE rounds SET status=?, updated_at=? WHERE round_id=?",
                        (RoundState.READY.value, float(received_at), round_id),
                    )
                    self._event(
                        conn,
                        round_id,
                        "ROUND_READY",
                        received_at,
                        from_state=state.value,
                        to_state=RoundState.READY.value,
                        details={"trigger": "all_expected_blocks"},
                    )
                    new_state = RoundState.READY

            return {
                "outcome": "ACCEPTED",
                "duplicate": False,
                "late": late,
                "state": new_state.value,
                "content_hash": content_hash,
                "received_at": float(received_at),
            }

    def get_events(self, round_id: str) -> list[dict[str, Any]]:
        self.require_round(round_id)
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT event_id,event_type,from_state,to_state,at,details_json "
                "FROM events WHERE round_id=? ORDER BY event_id",
                (round_id,),
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["details"] = _parse_json(item.pop("details_json"), {})
                result.append(item)
            return result

    def prepare_aggregation(
        self,
        *,
        round_id: str,
        requested_policy: FinalizationPolicy | None,
        now: float,
    ) -> dict[str, Any]:
        with self._connect(write=True) as conn:
            row = conn.execute("SELECT * FROM rounds WHERE round_id=?", (round_id,)).fetchone()
            if row is None:
                raise GradSyncError(ErrorCode.E_ROUND_NOT_FOUND, "轮次不存在", round_id=round_id)
            round_data = self._round_from_row(row)
            state = RoundState(round_data["status"])
            cached = conn.execute(
                "SELECT 1 FROM aggregate_results WHERE round_id=?", (round_id,)
            ).fetchone()
            if state == RoundState.AGGREGATED and cached is not None:
                return {"cached": True}
            if state == RoundState.ABORTED:
                raise GradSyncError(
                    ErrorCode.E_INVALID_TRANSITION,
                    "ABORTED 轮次不能再聚合",
                    round_id=round_id,
                    details={"state": state.value},
                )

            pending = conn.execute(
                "SELECT * FROM intents WHERE round_id=? AND status IN ('PENDING','IN_PROGRESS') "
                "ORDER BY created_at LIMIT 1",
                (round_id,),
            ).fetchone()
            if pending is not None:
                return {
                    "cached": False,
                    "reused_intent": True,
                    "intent_id": pending["intent_id"],
                    "snapshot": _parse_json(pending["snapshot_json"], {}),
                    "snapshot_hash": pending["snapshot_hash"],
                }

            if state == RoundState.READY and round_data["snapshot"]:
                snapshot = round_data["snapshot"]
                mode = FinalizationPolicy(round_data["finalization_policy"])
            else:
                policy = FinalizationPolicy(requested_policy or round_data["policy"])
                submissions = [
                    dict(item)
                    for item in conn.execute(
                        "SELECT node_id,shard_id,sample_count,content_hash,received_at,late "
                        "FROM submissions WHERE round_id=? ORDER BY shard_id,node_id",
                        (round_id,),
                    ).fetchall()
                ]
                expected_blocks = len(round_data["worker_ids"]) * int(round_data["shard_count"])
                all_complete = len(submissions) == expected_blocks
                deadline = float(round_data["collect_deadline_at"])
                max_wait_deadline = float(round_data["max_wait_deadline_at"])

                if state == RoundState.COLLECTING:
                    if now < deadline and not all_complete:
                        raise GradSyncError(
                            ErrorCode.E_BUSY,
                            "收集截止时间未到且分片不完整",
                            round_id=round_id,
                            details={
                                "now": now,
                                "collect_deadline_at": deadline,
                                "received_blocks": len(submissions),
                                "expected_blocks": expected_blocks,
                            },
                        )
                    if deadline <= now < max_wait_deadline:
                        if policy == FinalizationPolicy.STRICT and not all_complete:
                            raise GradSyncError(
                                ErrorCode.E_BUSY,
                                "严格模式仍在最大等待窗口内等待迟到分片",
                                round_id=round_id,
                                details={
                                    "now": now,
                                    "max_wait_deadline_at": max_wait_deadline,
                                    "received_blocks": len(submissions),
                                    "expected_blocks": expected_blocks,
                                },
                            )

                if policy == FinalizationPolicy.DEGRADED:
                    included = [
                        item for item in submissions if float(item["received_at"]) <= deadline
                    ]
                    cutoff_at = deadline
                else:
                    cutoff_at = min(now, max_wait_deadline)
                    included = [
                        item
                        for item in submissions
                        if float(item["received_at"]) <= cutoff_at
                    ]

                included_keys = {(int(item["shard_id"]), item["node_id"]) for item in included}
                expected_keys = {
                    (shard_id, node_id)
                    for shard_id in range(int(round_data["shard_count"]))
                    for node_id in round_data["worker_ids"]
                }
                missing_blocks = sorted(
                    [{"shard_id": shard_id, "node_id": node_id} for shard_id, node_id in expected_keys - included_keys],
                    key=lambda item: (item["shard_id"], item["node_id"]),
                )
                missing_nodes = sorted({item["node_id"] for item in missing_blocks})
                snapshot = {
                    "schema_version": 1,
                    "round_id": round_id,
                    "mode": policy.value,
                    "decision_at": now,
                    "cutoff_at": cutoff_at,
                    "collect_deadline_at": deadline,
                    "max_wait_deadline_at": max_wait_deadline,
                    "expected_blocks": expected_blocks,
                    "included_blocks": len(included),
                    "included": [
                        {
                            "node_id": item["node_id"],
                            "shard_id": int(item["shard_id"]),
                            "sample_count": int(item["sample_count"]),
                            "content_hash": item["content_hash"],
                            "received_at": float(item["received_at"]),
                            "late": bool(item["late"]),
                        }
                        for item in sorted(
                            included, key=lambda item: (int(item["shard_id"]), item["node_id"])
                        )
                    ],
                    "missing_blocks": missing_blocks,
                    "missing_nodes": missing_nodes,
                }
                snapshot_hash = sha256_json(snapshot)
                mode = policy

            intent_id = str(uuid.uuid4())
            snapshot_json = _json(snapshot)
            snapshot_hash = sha256_json(snapshot)
            conn.execute(
                """
                INSERT INTO intents(
                    intent_id,round_id,kind,status,snapshot_json,snapshot_hash,
                    created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    intent_id,
                    round_id,
                    "AGGREGATE",
                    "PENDING",
                    snapshot_json,
                    snapshot_hash,
                    now,
                    now,
                ),
            )
            if state == RoundState.COLLECTING:
                assert_legal_transition(state, RoundState.READY)
                conn.execute(
                    "UPDATE rounds SET status=?, updated_at=?, finalization_policy=?, "
                    "snapshot_json=?, snapshot_hash=?, last_error_code=NULL WHERE round_id=?",
                    (
                        RoundState.READY.value,
                        now,
                        mode.value,
                        snapshot_json,
                        snapshot_hash,
                        round_id,
                    ),
                )
                self._event(
                    conn,
                    round_id,
                    "ROUND_READY",
                    now,
                    from_state=state.value,
                    to_state=RoundState.READY.value,
                    details={"trigger": "finalization_barrier"},
                )
            else:
                conn.execute(
                    "UPDATE rounds SET updated_at=?, finalization_policy=?, snapshot_json=?, "
                    "snapshot_hash=?, last_error_code=NULL WHERE round_id=?",
                    (now, mode.value, snapshot_json, snapshot_hash, round_id),
                )
            self._event(
                conn,
                round_id,
                "SNAPSHOT_LOCKED",
                now,
                from_state=RoundState.READY.value,
                to_state=RoundState.READY.value,
                details={
                    "intent_id": intent_id,
                    "snapshot_hash": snapshot_hash,
                    "mode": mode.value,
                    "included_blocks": snapshot["included_blocks"],
                    "missing_nodes": snapshot["missing_nodes"],
                },
            )
            return {
                "cached": False,
                "reused_intent": False,
                "intent_id": intent_id,
                "snapshot": snapshot,
                "snapshot_hash": snapshot_hash,
            }

    def commit_aggregation(
        self,
        *,
        round_id: str,
        intent_id: str,
        tensor_blob: bytes,
        shard_count: int,
        shard_dim: int,
        digest: str,
        element_sum: float,
        element_sum_text: str,
        missing_nodes: list[str],
        missing_blocks: list[dict[str, Any]],
        degraded: bool,
        policy: FinalizationPolicy,
        snapshot_hash: str,
        computed_at: float,
        recovered: bool = False,
    ) -> None:
        with self._connect(write=True) as conn:
            round_row = conn.execute(
                "SELECT * FROM rounds WHERE round_id=?", (round_id,)
            ).fetchone()
            if round_row is None:
                raise GradSyncError(ErrorCode.E_ROUND_NOT_FOUND, "轮次不存在", round_id=round_id)
            state = RoundState(round_row["status"])
            existing_result = conn.execute(
                "SELECT 1 FROM aggregate_results WHERE round_id=?", (round_id,)
            ).fetchone()
            if state == RoundState.AGGREGATED and existing_result is not None:
                conn.execute(
                    "UPDATE intents SET status='COMMITTED', updated_at=? WHERE intent_id=?",
                    (computed_at, intent_id),
                )
                return
            if state == RoundState.ABORTED:
                raise GradSyncError(
                    ErrorCode.E_INVALID_TRANSITION,
                    "ABORTED 轮次不能写入聚合结果",
                    round_id=round_id,
                    details={"state": state.value},
                )
            if state != RoundState.READY:
                raise GradSyncError(
                    ErrorCode.E_INVALID_TRANSITION,
                    "仅 READY 轮次可写入聚合结果",
                    round_id=round_id,
                    details={"state": state.value},
                )
            intent = conn.execute(
                "SELECT * FROM intents WHERE intent_id=? AND round_id=?",
                (intent_id, round_id),
            ).fetchone()
            if intent is None or intent["status"] not in {"PENDING", "IN_PROGRESS"}:
                raise GradSyncError(
                    ErrorCode.E_INVALID_TRANSITION,
                    "聚合意图不是待提交状态",
                    round_id=round_id,
                    details={"intent_id": intent_id},
                )
            assert_legal_transition(state, RoundState.AGGREGATED)
            conn.execute(
                """
                INSERT INTO aggregate_results(
                    round_id,tensor_blob,shard_count,shard_dim,digest_sha256,
                    element_sum,element_sum_text,missing_nodes_json,missing_blocks_json,
                    degraded,policy,snapshot_hash,computed_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    round_id,
                    sqlite3.Binary(tensor_blob),
                    int(shard_count),
                    int(shard_dim),
                    digest,
                    float(element_sum),
                    element_sum_text,
                    _json(missing_nodes),
                    _json(missing_blocks),
                    1 if degraded else 0,
                    policy.value,
                    snapshot_hash,
                    computed_at,
                ),
            )
            conn.execute(
                "UPDATE rounds SET status=?, updated_at=?, last_error_code=? WHERE round_id=?",
                (
                    RoundState.AGGREGATED.value,
                    computed_at,
                    ErrorCode.E_DEGRADED_ROUND.value if degraded else None,
                    round_id,
                ),
            )
            conn.execute(
                "UPDATE intents SET status='COMMITTED', updated_at=?, error_code=NULL, "
                "error_message=NULL WHERE intent_id=?",
                (computed_at, intent_id),
            )
            self._event(
                conn,
                round_id,
                "ROUND_AGGREGATED",
                computed_at,
                from_state=RoundState.READY.value,
                to_state=RoundState.AGGREGATED.value,
                details={
                    "intent_id": intent_id,
                    "digest_sha256": digest,
                    "element_sum": element_sum_text,
                    "degraded": degraded,
                    "missing_nodes": missing_nodes,
                    "recovered": recovered,
                },
            )

    def fail_intent(
        self,
        *,
        round_id: str,
        intent_id: str,
        code: ErrorCode,
        message: str,
        at: float,
    ) -> None:
        with self._connect(write=True) as conn:
            row = conn.execute("SELECT status FROM rounds WHERE round_id=?", (round_id,)).fetchone()
            if row is None:
                raise GradSyncError(ErrorCode.E_ROUND_NOT_FOUND, "轮次不存在", round_id=round_id)
            conn.execute(
                "UPDATE intents SET status='FAILED', updated_at=?, error_code=?, error_message=? "
                "WHERE intent_id=?",
                (at, code.value, message, intent_id),
            )
            conn.execute(
                "UPDATE rounds SET last_error_code=?, updated_at=? WHERE round_id=?",
                (code.value, at, round_id),
            )
            self._event(
                conn,
                round_id,
                "AGGREGATION_FAILED",
                at,
                from_state=row["status"],
                to_state=row["status"],
                details={"intent_id": intent_id, "code": code.value, "message": message},
            )

    def abort_round(self, round_id: str, *, now: float) -> dict[str, Any]:
        with self._connect(write=True) as conn:
            row = conn.execute("SELECT * FROM rounds WHERE round_id=?", (round_id,)).fetchone()
            if row is None:
                raise GradSyncError(ErrorCode.E_ROUND_NOT_FOUND, "轮次不存在", round_id=round_id)
            state = RoundState(row["status"])
            if state not in {RoundState.COLLECTING, RoundState.READY}:
                raise GradSyncError(
                    ErrorCode.E_INVALID_TRANSITION,
                    f"{state.value} 轮次不能中止",
                    round_id=round_id,
                    details={"state": state.value},
                )
            assert_legal_transition(state, RoundState.ABORTED)
            conn.execute(
                "UPDATE rounds SET status=?, updated_at=? WHERE round_id=?",
                (RoundState.ABORTED.value, now, round_id),
            )
            conn.execute(
                "UPDATE intents SET status='CANCELLED', updated_at=? "
                "WHERE round_id=? AND status IN ('PENDING','IN_PROGRESS')",
                (now, round_id),
            )
            self._event(
                conn,
                round_id,
                "ROUND_ABORTED",
                now,
                from_state=state.value,
                to_state=RoundState.ABORTED.value,
                details={},
            )
        return self.require_round(round_id)

    def get_aggregate(self, round_id: str) -> dict[str, Any] | None:
        self.require_round(round_id)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM aggregate_results WHERE round_id=?", (round_id,)
            ).fetchone()
            if row is None:
                return None
            data = dict(row)
            tensor_blob = bytes(data.pop("tensor_blob"))
            tensor = np.frombuffer(tensor_blob, dtype="<f8").reshape(
                int(data["shard_count"]), int(data["shard_dim"])
            )
            data["tensor_b64"] = base64.b64encode(tensor_blob).decode("ascii")
            data["tensor_preview"] = [
                [f"{float(value):.8f}" for value in row_values] for row_values in tensor
            ]
            data["missing_nodes"] = _parse_json(data.pop("missing_nodes_json"), [])
            data["missing_blocks"] = _parse_json(data.pop("missing_blocks_json"), [])
            data["degraded"] = bool(data["degraded"])
            return data

    def get_pending_intents(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT i.*, r.status AS round_status FROM intents i "
                "JOIN rounds r ON r.round_id=i.round_id "
                "WHERE i.status IN ('PENDING','IN_PROGRESS') "
                "ORDER BY i.created_at, i.intent_id"
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["snapshot"] = _parse_json(item.pop("snapshot_json"), {})
                result.append(item)
            return result

    def mark_recovery_started(
        self, *, round_id: str, intent_id: str, at: float
    ) -> dict[str, Any]:
        with self._connect(write=True) as conn:
            row = conn.execute("SELECT * FROM rounds WHERE round_id=?", (round_id,)).fetchone()
            if row is None:
                raise GradSyncError(ErrorCode.E_ROUND_NOT_FOUND, "轮次不存在", round_id=round_id)
            state = RoundState(row["status"])
            if state == RoundState.ABORTED:
                conn.execute(
                    "UPDATE intents SET status='CANCELLED', updated_at=? WHERE intent_id=?",
                    (at, intent_id),
                )
                return {"cancelled": True, "state": state.value}
            if state == RoundState.AGGREGATED:
                return {"already_aggregated": True, "state": state.value}
            if state == RoundState.COLLECTING:
                assert_legal_transition(state, RoundState.READY)
                conn.execute(
                    "UPDATE rounds SET status=?, updated_at=? WHERE round_id=?",
                    (RoundState.READY.value, at, round_id),
                )
                self._event(
                    conn,
                    round_id,
                    "ROUND_READY",
                    at,
                    from_state=state.value,
                    to_state=RoundState.READY.value,
                    details={"trigger": "recovery_scan"},
                )
            conn.execute(
                "UPDATE intents SET status='IN_PROGRESS', updated_at=? WHERE intent_id=?",
                (at, intent_id),
            )
            self._event(
                conn,
                round_id,
                "RECOVERY_STARTED",
                at,
                from_state=state.value,
                to_state=RoundState.READY.value,
                details={"intent_id": intent_id, "source": "sqlite_pending_intent"},
            )
        return {"cancelled": False, "already_aggregated": False, "state": "READY"}

    def get_intent(self, intent_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM intents WHERE intent_id=?", (intent_id,)).fetchone()
            if row is None:
                return None
            item = dict(row)
            item["snapshot"] = _parse_json(item.pop("snapshot_json"), {})
            return item

    def mark_intent_committed(self, *, intent_id: str, at: float) -> None:
        with self._connect(write=True) as conn:
            conn.execute(
                "UPDATE intents SET status='COMMITTED', updated_at=?, error_code=NULL, "
                "error_message=NULL WHERE intent_id=?",
                (at, intent_id),
            )

    def record_recovery_report(self, report: dict[str, Any], *, started_at: float, finished_at: float) -> None:
        with self._connect(write=True) as conn:
            conn.execute(
                "INSERT INTO recovery_runs(recovery_id,started_at,finished_at,report_json) VALUES(?,?,?,?)",
                (str(uuid.uuid4()), float(started_at), float(finished_at), _json(report)),
            )

    def latest_recovery_report(self) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM recovery_runs ORDER BY finished_at DESC, rowid DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            item = dict(row)
            item["report"] = _parse_json(item.pop("report_json"), {})
            return item

    def record_delivery(
        self,
        *,
        round_id: str,
        node_id: str,
        endpoint: str,
        status: str,
        http_status: int | None,
        error: str | None,
        attempted_at: float,
    ) -> None:
        with self._connect(write=True) as conn:
            conn.execute(
                "INSERT INTO deliveries(round_id,node_id,endpoint,status,http_status,error,attempted_at) "
                "VALUES(?,?,?,?,?,?,?) ON CONFLICT(round_id,node_id) DO UPDATE SET "
                "endpoint=excluded.endpoint,status=excluded.status,http_status=excluded.http_status,"
                "error=excluded.error,attempted_at=excluded.attempted_at",
                (round_id, node_id, endpoint, status, http_status, error, attempted_at),
            )
            EventExists = conn.execute(
                "SELECT 1 FROM events WHERE round_id=? AND event_type='RESULT_DELIVERY' "
                "AND details_json LIKE ? LIMIT 1",
                (round_id, f'%"{node_id}"%'),
            ).fetchone()
            if EventExists is None:
                self._event(
                    conn,
                    round_id,
                    "RESULT_DELIVERY",
                    attempted_at,
                    from_state=RoundState.AGGREGATED.value,
                    to_state=RoundState.AGGREGATED.value,
                    details={"node_id": node_id, "status": status, "endpoint": endpoint},
                )

    def get_deliveries(self, round_id: str) -> list[dict[str, Any]]:
        self.require_round(round_id)
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM deliveries WHERE round_id=? ORDER BY node_id", (round_id,)
            ).fetchall()
            return [dict(row) for row in rows]

    def health(self) -> dict[str, Any]:
        with self._connect() as conn:
            round_count = conn.execute("SELECT COUNT(*) FROM rounds").fetchone()[0]
            pending = conn.execute(
                "SELECT COUNT(*) FROM intents WHERE status IN ('PENDING','IN_PROGRESS')"
            ).fetchone()[0]
            return {"database": str(self.path), "rounds": round_count, "pending_intents": pending}

