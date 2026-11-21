from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .models import Scenario


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _loads(value: str | None, default: Any) -> Any:
    if value is None:
        return default
    return json.loads(value)


class PDBCommitConflict(Exception):
    def __init__(self, pdb: str, current_available: int, required_available: int) -> None:
        super().__init__(f"{pdb} changed concurrently")
        self.pdb = pdb
        self.current_available = current_available
        self.required_available = required_available


class Repository:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.db_path,
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def _read_snapshot(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize_schema(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS scenario_meta (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    name TEXT NOT NULL,
                    source_path TEXT,
                    loaded_at TEXT NOT NULL,
                    scenario_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS nodes (
                    name TEXT PRIMARY KEY,
                    cpu_milli INTEGER NOT NULL,
                    memory_bytes INTEGER NOT NULL,
                    drain_requested INTEGER NOT NULL,
                    labels_json TEXT NOT NULL,
                    taints_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS workloads (
                    name TEXT PRIMARY KEY,
                    controller_type TEXT NOT NULL,
                    desired_replicas INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS pods (
                    name TEXT PRIMARY KEY,
                    workload_name TEXT NOT NULL,
                    controller_type TEXT NOT NULL,
                    node TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    eviction_state TEXT NOT NULL,
                    terminating INTEGER NOT NULL,
                    labels_json TEXT NOT NULL,
                    cpu_milli INTEGER NOT NULL,
                    memory_bytes INTEGER NOT NULL,
                    node_selector_json TEXT NOT NULL,
                    tolerations_json TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    healthy INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS pdbs (
                    name TEXT PRIMARY KEY,
                    selector_json TEXT NOT NULL,
                    controller_type TEXT NOT NULL,
                    controller_name TEXT,
                    budget_kind TEXT NOT NULL,
                    budget_input_json TEXT NOT NULL,
                    is_percentage INTEGER NOT NULL,
                    desired_replicas INTEGER NOT NULL,
                    rounding TEXT NOT NULL,
                    calculated_integer INTEGER NOT NULL,
                    max_unavailable INTEGER,
                    required_available INTEGER NOT NULL,
                    initial_available INTEGER NOT NULL,
                    current_available INTEGER NOT NULL,
                    matched_pods_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS drains (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id TEXT NOT NULL UNIQUE,
                    node TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    naive_release_count INTEGER,
                    optimized_release_count INTEGER,
                    actual_release_count INTEGER,
                    result_json TEXT,
                    error_json TEXT
                );

                CREATE TABLE IF NOT EXISTS drain_steps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    drain_id INTEGER NOT NULL REFERENCES drains(id) ON DELETE CASCADE,
                    seq INTEGER NOT NULL,
                    pod_name TEXT NOT NULL,
                    source_node TEXT NOT NULL,
                    status TEXT NOT NULL,
                    outcome TEXT,
                    code TEXT,
                    target_node TEXT,
                    affected_json TEXT NOT NULL,
                    reasons_json TEXT NOT NULL,
                    placement_json TEXT NOT NULL,
                    availability_before_json TEXT NOT NULL,
                    availability_after_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    committed_at TEXT,
                    UNIQUE (drain_id, seq)
                );

                CREATE TABLE IF NOT EXISTS decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kind TEXT NOT NULL,
                    request_id TEXT,
                    node TEXT,
                    pod_name TEXT,
                    allowed INTEGER NOT NULL,
                    code TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS recovery_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    report_json TEXT NOT NULL
                );
                """
            )
        finally:
            connection.close()

    def replace_scenario(
        self, scenario: Scenario, raw_json: str, source_path: str | None
    ) -> None:
        with self._write() as connection:
            connection.execute("DELETE FROM drain_steps")
            connection.execute("DELETE FROM drains")
            connection.execute("DELETE FROM decisions")
            connection.execute("DELETE FROM recovery_events")
            connection.execute("DELETE FROM pdbs")
            connection.execute("DELETE FROM pods")
            connection.execute("DELETE FROM workloads")
            connection.execute("DELETE FROM nodes")
            connection.execute("DELETE FROM scenario_meta")
            connection.execute(
                "INSERT INTO scenario_meta(id, name, source_path, loaded_at, scenario_json) "
                "VALUES(1, ?, ?, ?, ?)",
                (scenario.name, source_path, utc_now(), raw_json),
            )
            for node in scenario.nodes.values():
                connection.execute(
                    "INSERT INTO nodes(name, cpu_milli, memory_bytes, drain_requested, labels_json, taints_json) "
                    "VALUES(?, ?, ?, ?, ?, ?)",
                    (
                        node.name,
                        node.cpu_milli,
                        node.memory_bytes,
                        int(node.drain_requested),
                        _json(node.labels),
                        _json(
                            [
                                {"key": taint.key, "value": taint.value, "effect": taint.effect}
                                for taint in node.taints
                            ]
                        ),
                    ),
                )
            for workload in scenario.workloads.values():
                connection.execute(
                    "INSERT INTO workloads(name, controller_type, desired_replicas) VALUES(?, ?, ?)",
                    (workload.name, workload.controller_type, workload.desired_replicas),
                )
            for pod in scenario.ordered_pods:
                connection.execute(
                    "INSERT INTO pods(name, workload_name, controller_type, node, phase, eviction_state, "
                    "terminating, labels_json, cpu_milli, memory_bytes, node_selector_json, tolerations_json, "
                    "ordinal, healthy) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        pod.name,
                        pod.workload_name,
                        pod.controller_type,
                        pod.node,
                        pod.phase,
                        pod.eviction_state,
                        int(pod.terminating),
                        _json(pod.labels),
                        pod.cpu_milli,
                        pod.memory_bytes,
                        _json(pod.node_selector),
                        _json(
                            [
                                {
                                    "key": toleration.key,
                                    "operator": toleration.operator,
                                    "value": toleration.value,
                                    "effect": toleration.effect,
                                }
                                for toleration in pod.tolerations
                            ]
                        ),
                        pod.ordinal,
                        int(pod.healthy),
                    ),
                )
            for pdb in scenario.pdbs.values():
                connection.execute(
                    "INSERT INTO pdbs(name, selector_json, controller_type, controller_name, budget_kind, "
                    "budget_input_json, is_percentage, desired_replicas, rounding, calculated_integer, "
                    "max_unavailable, required_available, initial_available, current_available, matched_pods_json) "
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        pdb.name,
                        _json(pdb.selector),
                        pdb.controller_type,
                        pdb.controller_name,
                        pdb.budget_kind,
                        _json(pdb.budget_input),
                        int(pdb.is_percentage),
                        pdb.desired_replicas,
                        pdb.rounding,
                        pdb.calculated_integer,
                        pdb.max_unavailable,
                        pdb.required_available,
                        pdb.initial_available,
                        pdb.initial_available,
                        _json(list(pdb.matched_pods)),
                    ),
                )

    def get_scenario_meta(self) -> dict[str, Any] | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT name, source_path, loaded_at, scenario_json FROM scenario_meta WHERE id=1"
            ).fetchone()
            return dict(row) if row else None
        finally:
            connection.close()

    def has_active_drains(self) -> bool:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM drains WHERE status IN ('INTENT', 'RUNNING')"
            ).fetchone()
            return bool(row["count"])
        finally:
            connection.close()

    def get_pdb_state(self) -> dict[str, dict[str, int]]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT name, required_available, initial_available, current_available FROM pdbs ORDER BY name"
            ).fetchall()
            return {
                row["name"]: {
                    "required_available": row["required_available"],
                    "initial_available": row["initial_available"],
                    "current_available": row["current_available"],
                }
                for row in rows
            }
        finally:
            connection.close()

    def planning_snapshot(self) -> dict[str, Any]:
        with self._read_snapshot() as connection:
            availability = {
                row["name"]: row["current_available"]
                for row in connection.execute("SELECT name, current_available FROM pdbs ORDER BY name")
            }
            steps = [
                self._step_row_to_dict(row)
                for row in connection.execute(
                    "SELECT * FROM drain_steps WHERE status='COMMITTED' ORDER BY id"
                )
            ]
            return {"pdb_available": availability, "movement_steps": steps}

    def create_drain(self, request_id: str, node: str) -> dict[str, Any]:
        now = utc_now()
        with self._write() as connection:
            cursor = connection.execute(
                "INSERT INTO drains(request_id, node, status, created_at, updated_at) "
                "VALUES(?, ?, 'INTENT', ?, ?)",
                (request_id, node, now, now),
            )
            drain_id = int(cursor.lastrowid)
        return self.get_drain_by_id(drain_id)

    def update_drain_status(self, drain_id: int, status: str) -> None:
        with self._write() as connection:
            connection.execute(
                "UPDATE drains SET status=?, updated_at=? WHERE id=?",
                (status, utc_now(), drain_id),
            )

    def complete_drain(
        self,
        drain_id: int,
        result: dict[str, Any],
        naive_count: int,
        optimized_count: int,
        actual_count: int,
    ) -> None:
        with self._write() as connection:
            connection.execute(
                "UPDATE drains SET status='COMPLETED', updated_at=?, naive_release_count=?, "
                "optimized_release_count=?, actual_release_count=?, result_json=? WHERE id=?",
                (
                    utc_now(),
                    naive_count,
                    optimized_count,
                    actual_count,
                    _json(result),
                    drain_id,
                ),
            )

    def get_drain_by_request(self, request_id: str) -> dict[str, Any] | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM drains WHERE request_id=?", (request_id,)
            ).fetchone()
            return self._drain_row_to_dict(row) if row else None
        finally:
            connection.close()

    def get_drain_by_id(self, drain_id: int) -> dict[str, Any] | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM drains WHERE id=?", (drain_id,)
            ).fetchone()
            return self._drain_row_to_dict(row) if row else None
        finally:
            connection.close()

    def insert_step_intent(
        self,
        drain_id: int,
        seq: int,
        pod_name: str,
        source_node: str,
        availability_before: dict[str, int],
    ) -> int:
        now = utc_now()
        with self._write() as connection:
            cursor = connection.execute(
                "INSERT INTO drain_steps(drain_id, seq, pod_name, source_node, status, affected_json, "
                "reasons_json, placement_json, availability_before_json, availability_after_json, created_at) "
                "VALUES(?, ?, ?, ?, 'STEP_INTENT', '[]', '[]', 'null', ?, ?, ?)",
                (
                    drain_id,
                    seq,
                    pod_name,
                    source_node,
                    _json(availability_before),
                    _json(availability_before),
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def commit_allowed_step(
        self,
        step_id: int,
        drain_id: int,
        seq: int,
        pod_name: str,
        source_node: str,
        target_node: str,
        affected: list[dict[str, Any]],
        placement: dict[str, Any],
        decision_payload: dict[str, Any],
    ) -> dict[str, int]:
        with self._write() as connection:
            for check in affected:
                if not check.get("pod_counted_as_healthy"):
                    continue
                cursor = connection.execute(
                    "UPDATE pdbs SET current_available=current_available-1 "
                    "WHERE name=? AND current_available >= required_available + 1",
                    (check["pdb"],),
                )
                if cursor.rowcount != 1:
                    row = connection.execute(
                        "SELECT current_available, required_available FROM pdbs WHERE name=?",
                        (check["pdb"],),
                    ).fetchone()
                    if row is None:
                        raise PDBCommitConflict(check["pdb"], 0, 0)
                    raise PDBCommitConflict(
                        check["pdb"], row["current_available"], row["required_available"]
                    )
            after = {
                row["name"]: row["current_available"]
                for row in connection.execute("SELECT name, current_available FROM pdbs ORDER BY name")
            }
            committed_payload = dict(decision_payload)
            committed_payload["availability_after"] = after
            now = utc_now()
            connection.execute(
                "UPDATE drain_steps SET status='COMMITTED', outcome='ALLOWED', code='ALLOWED', "
                "target_node=?, affected_json=?, reasons_json='[]', placement_json=?, "
                "availability_after_json=?, committed_at=? WHERE id=? AND status='STEP_INTENT'",
                (
                    target_node,
                    _json(affected),
                    _json(placement),
                    _json(after),
                    now,
                    step_id,
                ),
            )
            connection.execute(
                "INSERT INTO decisions(kind, request_id, node, pod_name, allowed, code, payload_json, created_at) "
                "SELECT 'DRAIN_STEP', request_id, node, ?, 1, 'ALLOWED', ?, ? FROM drains WHERE id=?",
                (pod_name, _json(committed_payload), now, drain_id),
            )
            return after

    def commit_blocked_step(
        self,
        step_id: int,
        drain_id: int,
        pod_name: str,
        source_node: str,
        code: str,
        affected: list[dict[str, Any]],
        reasons: list[dict[str, Any]],
        availability_before: dict[str, int],
        decision_payload: dict[str, Any],
    ) -> None:
        now = utc_now()
        with self._write() as connection:
            connection.execute(
                "UPDATE drain_steps SET status='COMMITTED', outcome='BLOCKED', code=?, target_node=NULL, "
                "affected_json=?, reasons_json=?, placement_json='null', availability_after_json=?, "
                "committed_at=? WHERE id=? AND status='STEP_INTENT'",
                (
                    code,
                    _json(affected),
                    _json(reasons),
                    _json(availability_before),
                    now,
                    step_id,
                ),
            )
            connection.execute(
                "INSERT INTO decisions(kind, request_id, node, pod_name, allowed, code, payload_json, created_at) "
                "SELECT 'DRAIN_STEP', request_id, node, ?, 0, ?, ?, ? FROM drains WHERE id=?",
                (pod_name, code, _json(decision_payload), now, drain_id),
            )

    def persist_single_decision(
        self,
        request_id: str,
        node: str,
        pod_name: str,
        decision: dict[str, Any],
    ) -> None:
        with self._write() as connection:
            connection.execute(
                "INSERT INTO decisions(kind, request_id, node, pod_name, allowed, code, payload_json, created_at) "
                "VALUES('SINGLE_POD', ?, ?, ?, ?, ?, ?, ?)",
                (
                    request_id,
                    node,
                    pod_name,
                    int(decision["decision"] == "ALLOWED"),
                    decision["code"],
                    _json(decision),
                    utc_now(),
                ),
            )

    def list_steps(self, drain_id: int, include_intents: bool = False) -> list[dict[str, Any]]:
        connection = self._connect()
        try:
            where = "drain_id=?" if include_intents else "drain_id=? AND status='COMMITTED'"
            rows = connection.execute(
                f"SELECT * FROM drain_steps WHERE {where} ORDER BY seq", (drain_id,)
            ).fetchall()
            return [self._step_row_to_dict(row) for row in rows]
        finally:
            connection.close()

    def recover_incomplete(self) -> dict[str, Any] | None:
        with self._write() as connection:
            drains = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM drains WHERE status IN ('INTENT', 'RUNNING') ORDER BY id"
                )
            ]
            if not drains:
                return None

            aborted_drains: list[dict[str, Any]] = []
            for drain in drains:
                steps = [
                    dict(row)
                    for row in connection.execute(
                        "SELECT * FROM drain_steps WHERE drain_id=? ORDER BY seq", (drain["id"],)
                    )
                ]
                pending = [step for step in steps if step["status"] == "STEP_INTENT"]
                committed = [step for step in steps if step["status"] == "COMMITTED"]
                for step in pending:
                    connection.execute(
                        "UPDATE drain_steps SET status='ABORTED', outcome='ABORTED', "
                        "code='E_DRAIN_ABORTED', committed_at=? WHERE id=? AND status='STEP_INTENT'",
                        (utc_now(), step["id"]),
                    )
                last = committed[-1] if committed else None
                aborted_drains.append(
                    {
                        "request_id": drain["request_id"],
                        "node": drain["node"],
                        "status_before": drain["status"],
                        "status_after": "ABORTED",
                        "committed_step_count": len(committed),
                        "pending_intent_count": len(pending),
                        "last_complete_step": (
                            {
                                "sequence": last["seq"],
                                "pod": last["pod_name"],
                                "outcome": last["outcome"],
                                "code": last["code"],
                                "availability_after": _loads(
                                    last["availability_after_json"], {}
                                ),
                            }
                            if last
                            else None
                        ),
                    }
                )
                connection.execute(
                    "UPDATE drains SET status='ABORTED', updated_at=?, error_json=? WHERE id=?",
                    (
                        utc_now(),
                        _json(
                            {
                                "code": "E_DRAIN_ABORTED",
                                "message": "process terminated before the drain completed",
                            }
                        ),
                        drain["id"],
                    ),
                )

            initial = {
                row["name"]: row["initial_available"]
                for row in connection.execute("SELECT name, initial_available FROM pdbs ORDER BY name")
            }
            restored = dict(initial)
            for step in connection.execute(
                "SELECT affected_json FROM drain_steps WHERE status='COMMITTED' AND outcome='ALLOWED' ORDER BY id"
            ):
                for check in _loads(step["affected_json"], []):
                    if check.get("pod_counted_as_healthy") and check["pdb"] in restored:
                        restored[check["pdb"]] = max(0, restored[check["pdb"]] - 1)
            for pdb_name, available in restored.items():
                connection.execute(
                    "UPDATE pdbs SET current_available=? WHERE name=?", (available, pdb_name)
                )

            report = {
                "report_type": "RECOVERY",
                "generated_at": utc_now(),
                "facts_scope": "PERSISTED_ONLY",
                "complete_exit_report": False,
                "notice": (
                    "The process was terminated mid-drain. This report is reconstructed only from "
                    "committed SQLite facts; no complete exit report is claimed."
                ),
                "aborted_drains": aborted_drains,
                "pdb_availability": {
                    name: {
                        "initial_available": initial[name],
                        "restored_available": available,
                    }
                    for name, available in restored.items()
                },
            }
            connection.execute(
                "INSERT INTO recovery_events(created_at, report_json) VALUES(?, ?)",
                (utc_now(), _json(report)),
            )
            return report

    def latest_recovery(self) -> dict[str, Any] | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT report_json FROM recovery_events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            return _loads(row["report_json"], None) if row else None
        finally:
            connection.close()

    @staticmethod
    def _drain_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["result"] = _loads(result.pop("result_json"), None)
        result["error"] = _loads(result.pop("error_json"), None)
        return result

    @staticmethod
    def _step_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        for key in ("affected_json", "reasons_json", "placement_json"):
            output_key = key.removesuffix("_json")
            result[output_key] = _loads(result.pop(key), [] if output_key != "placement" else None)
        for key in ("availability_before_json", "availability_after_json"):
            output_key = key.removesuffix("_json")
            result[output_key] = _loads(result.pop(key), {})
        return result

