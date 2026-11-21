from __future__ import annotations

import os
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .adjudicator import (
    adjudicate_single,
    affected_pdbs,
    build_usage,
    choose_optimized_plan,
    effective_locations,
    evaluate_pdb_eviction,
    pdb_blocking_reasons,
)
from .errors import BusinessError, spec_invalid
from .models import Scenario
from .placement import choose_placement
from .repository import PDBCommitConflict, Repository
from .scenario import load_scenario_file, parse_scenario_text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SAMPLE = PROJECT_ROOT / "samples" / "scenario.json"
DEFAULT_DB = Path(os.environ.get("PDB_ENGINE_DB", PROJECT_ROOT / "data" / "pdb_engine.sqlite3"))

VISIBILITY_RULES = {
    "isolation": "SQLite WAL gives every read a consistent committed snapshot.",
    "node_serialization": (
        "All drain transitions for one node are serialized by a single-writer lock; "
        "different nodes may analyze concurrently."
    ),
    "pdb_commit_gate": (
        "A short process-wide commit gate plus SQLite conditional updates prevents two drains "
        "from committing a PDB decrement below its required lower bound."
    ),
    "publish_rule": (
        "A step becomes query-visible only in the transaction that changes its status from "
        "STEP_INTENT to COMMITTED and applies all affected PDB decrements. Intent rows are hidden."
    ),
    "batch_visibility": (
        "A RUNNING drain may expose committed prefix steps, but never a partial allowed decision "
        "whose PDB decrements are not yet committed. Final allowed/blocked lists are published "
        "only when the drain becomes COMPLETED."
    ),
    "idempotency": (
        "The same request_id is executed once. Concurrent duplicates wait and reuse the persisted "
        "drain result; a completed replay does not reapply PDB decrements."
    ),
    "recovery": (
        "After an abrupt stop, uncommitted intent rows are ABORTED and PDB availability is rebuilt "
        "from initial availability minus committed allowed steps."
    ),
}


@dataclass
class _InflightDrain:
    node: str
    event: threading.Event = field(default_factory=threading.Event)


class EngineService:
    def __init__(self, db_path: str | Path | None = None) -> None:
        self.db_path = Path(db_path) if db_path is not None else DEFAULT_DB
        self.repo = Repository(self.db_path)
        self.repo.initialize_schema()
        self._scenario: Scenario | None = None
        self._initialized = False
        self._init_lock = threading.Lock()
        self._scenario_lock = threading.RLock()
        self._inflight_guard = threading.Lock()
        self._inflight: dict[str, _InflightDrain] = {}
        self._node_locks_guard = threading.Lock()
        self._node_locks: dict[str, threading.Lock] = {}
        self._pdb_commit_lock = threading.Lock()
        self.recovery_report: dict[str, Any] | None = None

    def initialize(self) -> dict[str, Any] | None:
        with self._init_lock:
            if self._initialized:
                return self.recovery_report
            self.recovery_report = self.repo.recover_incomplete()
            meta = self.repo.get_scenario_meta()
            if meta:
                self._scenario = parse_scenario_text(
                    meta["scenario_json"], source_path=meta.get("source_path")
                )
            self._initialized = True
            return self.recovery_report

    @property
    def scenario(self) -> Scenario:
        self.initialize()
        if self._scenario is None:
            raise spec_invalid("no sample scenario is loaded; call POST /api/v1/samples/load first")
        return self._scenario

    def _resolve_sample_path(self, sample_path: str | None) -> Path:
        if sample_path is None:
            return DEFAULT_SAMPLE
        path = Path(sample_path)
        if path.is_absolute():
            return path
        cwd_candidate = Path.cwd() / path
        if cwd_candidate.exists():
            return cwd_candidate
        return PROJECT_ROOT / path

    def load_sample(self, sample_path: str | None = None) -> dict[str, Any]:
        self.initialize()
        path = self._resolve_sample_path(sample_path)
        scenario, raw_json = load_scenario_file(path)
        with self._scenario_lock:
            if self.repo.has_active_drains():
                raise spec_invalid(
                    "cannot replace the sample while a drain is INTENT or RUNNING"
                )
            self.repo.replace_scenario(scenario, raw_json, str(path.resolve()))
            self._scenario = scenario
            self.recovery_report = None
        return self.scenario_summary()

    def scenario_summary(self) -> dict[str, Any]:
        scenario = self.scenario
        pdb_state = self.repo.get_pdb_state()
        return {
            "loaded": True,
            "scenario": scenario.name,
            "source_path": scenario.source_path,
            "nodes": [
                {
                    "name": node.name,
                    "allocatable": {
                        "cpu_milli": node.cpu_milli,
                        "memory_bytes": node.memory_bytes,
                    },
                    "drain_requested": node.drain_requested,
                    "labels": node.labels,
                    "taints": [
                        {"key": taint.key, "value": taint.value, "effect": taint.effect}
                        for taint in node.taints
                    ],
                }
                for node in scenario.nodes.values()
            ],
            "workloads": [
                {
                    "name": workload.name,
                    "controller_type": workload.controller_type,
                    "desired_replicas": workload.desired_replicas,
                    "pods": list(workload.pod_names),
                }
                for workload in scenario.workloads.values()
            ],
            "healthy_pods": sum(1 for pod in scenario.ordered_pods if pod.healthy),
            "pdbs": [
                {
                    **pdb.semantics(),
                    "current_available": pdb_state[pdb.name]["current_available"],
                }
                for pdb in scenario.pdbs.values()
            ],
            "quantity_units": {
                "cpu": "integer millicores",
                "memory": "integer bytes",
                "overflow_limit": 9_223_372_036_854_775_807,
            },
            "visibility": VISIBILITY_RULES,
        }

    def check_eviction(self, pod_name: str, request_id: str | None = None) -> dict[str, Any]:
        scenario = self.scenario
        pod = scenario.pods.get(pod_name)
        if pod is None:
            raise BusinessError("E_POD_NOT_FOUND", "pod does not exist", {"pod": pod_name})
        request_id = request_id or f"single-{uuid.uuid4().hex}"
        snapshot = self.repo.planning_snapshot()
        locations = effective_locations(scenario, snapshot["movement_steps"])
        source_node = locations[pod_name]
        decision = adjudicate_single(
            scenario,
            pod,
            source_node,
            snapshot["pdb_available"],
            snapshot["movement_steps"],
        )
        decision["request_id"] = request_id
        self.repo.persist_single_decision(request_id, source_node, pod_name, decision)
        if decision["code"] == "E_PDB_BLOCKED":
            raise BusinessError(
                "E_PDB_BLOCKED",
                "eviction is blocked by one or more PDB constraints",
                {"decision": decision},
            )
        if decision["code"] == "E_NO_FEASIBLE_NODE":
            raise BusinessError(
                "E_NO_FEASIBLE_NODE",
                "eviction is blocked because no feasible replacement node exists",
                {"decision": decision},
            )
        return decision

    def _node_lock(self, node: str) -> threading.Lock:
        with self._node_locks_guard:
            return self._node_locks.setdefault(node, threading.Lock())

    def drain_node(
        self,
        node: str,
        request_id: str,
        *,
        crash_after_steps: int | None = None,
    ) -> dict[str, Any]:
        """Execute or replay a node drain.

        ``crash_after_steps`` is intentionally not exposed by the HTTP schema. It is
        used only by the local crash-recovery demonstration worker to terminate the
        process after a real committed step.
        """

        scenario = self.scenario
        if not request_id or not request_id.strip():
            raise spec_invalid("request_id must be a non-empty string")
        if node not in scenario.nodes:
            raise BusinessError("E_SPEC_INVALID", "node does not exist", {"node": node})
        if not scenario.nodes[node].drain_requested:
            raise spec_invalid("node is not marked as drain_requested", node=node)

        existing = self.repo.get_drain_by_request(request_id)
        if existing is not None:
            if existing["node"] != node:
                raise spec_invalid(
                    "request_id is already associated with another node",
                    request_id=request_id,
                    existing_node=existing["node"],
                    requested_node=node,
                )
            if existing["status"] in {"INTENT", "RUNNING"}:
                with self._inflight_guard:
                    inflight = self._inflight.get(request_id)
                if inflight is not None:
                    inflight.event.wait()
                    persisted = self.repo.get_drain_by_request(request_id)
                    assert persisted is not None
                    return self._drain_view(persisted, replayed=True)
                # The leader may have completed after our first read but before
                # acquiring the inflight guard. Re-read before returning a stale
                # RUNNING snapshot.
                refreshed = self.repo.get_drain_by_request(request_id)
                assert refreshed is not None
                return self._drain_view(refreshed, replayed=True)
            return self._drain_view(existing, replayed=True)

        with self._inflight_guard:
            inflight = self._inflight.get(request_id)
            if inflight is not None and inflight.node != node:
                raise spec_invalid(
                    "request_id is already executing for another node",
                    request_id=request_id,
                    existing_node=inflight.node,
                    requested_node=node,
                )
            leader = inflight is None
            if leader:
                inflight = _InflightDrain(node=node)
                self._inflight[request_id] = inflight

        assert inflight is not None
        if not leader:
            inflight.event.wait()
            persisted = self.repo.get_drain_by_request(request_id)
            if persisted is None:
                raise BusinessError(
                    "E_INTERNAL",
                    "duplicate drain wait finished without a persisted result",
                    {"request_id": request_id},
                )
            return self._drain_view(persisted, replayed=True)

        try:
            with self._node_lock(node):
                existing = self.repo.get_drain_by_request(request_id)
                if existing is not None:
                    if existing["node"] != node:
                        raise spec_invalid(
                            "request_id is already associated with another node",
                            request_id=request_id,
                            existing_node=existing["node"],
                        )
                    return self._drain_view(existing, replayed=True)
                return self._execute_drain(
                    node, request_id, crash_after_steps=crash_after_steps
                )
        finally:
            with self._inflight_guard:
                inflight.event.set()
                self._inflight.pop(request_id, None)

    def _execute_drain(
        self, node: str, request_id: str, *, crash_after_steps: int | None
    ) -> dict[str, Any]:
        scenario = self.scenario
        initial_snapshot = self.repo.planning_snapshot()
        locations = effective_locations(scenario, initial_snapshot["movement_steps"])
        candidates = [
            pod
            for pod in scenario.ordered_pods
            if locations.get(pod.name) == node
        ]
        naive_plan, optimized_plan = choose_optimized_plan(
            scenario,
            candidates,
            node,
            initial_snapshot["pdb_available"],
            initial_snapshot["movement_steps"],
        )

        drain = self.repo.create_drain(request_id, node)
        drain_id = int(drain["id"])
        self.repo.update_drain_status(drain_id, "RUNNING")

        finalized_steps = 0
        for seq, planned in enumerate(optimized_plan["steps"], start=1):
            pod = scenario.pods[planned["pod"]]
            crash_now = False
            with self._pdb_commit_lock:
                latest = self.repo.planning_snapshot()
                before = dict(latest["pdb_available"])
                latest_locations = effective_locations(
                    scenario, latest["movement_steps"]
                )
                source_node = latest_locations.get(pod.name, pod.node)
                step_id = self.repo.insert_step_intent(
                    drain_id, seq, pod.name, source_node, before
                )

                if source_node != node:
                    reason = {
                        "code": "E_SPEC_INVALID",
                        "pod": pod.name,
                        "message": "pod is no longer on the drained node",
                        "current_node": source_node,
                    }
                    payload = self._step_payload(
                        seq, pod, source_node, [], [reason], before, before
                    )
                    self.repo.commit_blocked_step(
                        step_id,
                        drain_id,
                        pod.name,
                        source_node,
                        reason["code"],
                        [],
                        [reason],
                        before,
                        payload,
                    )
                else:
                    pdbs = affected_pdbs(scenario, pod)
                    checks = [
                        evaluate_pdb_eviction(pdb, before[pdb.name], pod)
                        for pdb in pdbs
                    ]
                    reasons = pdb_blocking_reasons(checks)
                    placement = None
                    if not reasons:
                        usage = build_usage(scenario, latest["movement_steps"])
                        placement = choose_placement(scenario, pod, source_node, usage)
                        if placement is None:
                            reasons = [
                                {
                                    "code": "E_NO_FEASIBLE_NODE",
                                    "pod": pod.name,
                                    "source_node": source_node,
                                    "message": (
                                        "no other node satisfies capacity, selector and taint constraints"
                                    ),
                                }
                            ]
                    if reasons:
                        payload = self._step_payload(
                            seq, pod, source_node, checks, reasons, before, before
                        )
                        self.repo.commit_blocked_step(
                            step_id,
                            drain_id,
                            pod.name,
                            source_node,
                            reasons[0]["code"],
                            checks,
                            reasons,
                            before,
                            payload,
                        )
                    else:
                        payload = self._step_payload(
                            seq,
                            pod,
                            source_node,
                            checks,
                            [],
                            before,
                            before,
                            placement=placement,
                        )
                        try:
                            after = self.repo.commit_allowed_step(
                                step_id,
                                drain_id,
                                seq,
                                pod.name,
                                source_node,
                                placement["target_node"],
                                checks,
                                placement,
                                payload,
                            )
                        except PDBCommitConflict as conflict:
                            refreshed = self.repo.planning_snapshot()
                            conflict_before = dict(refreshed["pdb_available"])
                            conflict_checks = [
                                evaluate_pdb_eviction(
                                    pdb, conflict_before[pdb.name], pod
                                )
                                for pdb in pdbs
                            ]
                            conflict_reasons = pdb_blocking_reasons(conflict_checks)
                            if not conflict_reasons:
                                conflict_reasons = [
                                    {
                                        "code": "E_PDB_BLOCKED",
                                        "pdb": conflict.pdb,
                                        "current_available": conflict.current_available,
                                        "required_available": conflict.required_available,
                                        "after_available": conflict.current_available - 1,
                                        "gap": max(
                                            0,
                                            conflict.required_available
                                            - (conflict.current_available - 1),
                                        ),
                                        "message": "PDB changed concurrently during commit",
                                    }
                                ]
                            blocked_payload = self._step_payload(
                                seq,
                                pod,
                                source_node,
                                conflict_checks,
                                conflict_reasons,
                                conflict_before,
                                conflict_before,
                            )
                            self.repo.commit_blocked_step(
                                step_id,
                                drain_id,
                                pod.name,
                                source_node,
                                conflict_reasons[0]["code"],
                                conflict_checks,
                                conflict_reasons,
                                conflict_before,
                                blocked_payload,
                            )
                finalized_steps += 1
                crash_now = (
                    crash_after_steps is not None
                    and finalized_steps >= crash_after_steps
                )
            if crash_now:
                os._exit(97)

        steps = self.repo.list_steps(drain_id)
        allowed = [step for step in steps if step["outcome"] == "ALLOWED"]
        blocked = [step for step in steps if step["outcome"] == "BLOCKED"]
        result = {
            "drain_id": drain_id,
            "request_id": request_id,
            "node": node,
            "status": "COMPLETED",
            "release_summary": {
                "naive_allowed": naive_plan["allowed_count"],
                "optimized_preview_allowed": optimized_plan["allowed_count"],
                "actual_allowed": len(allowed),
                "actual_blocked": len(blocked),
                "optimized_not_less_than_naive": (
                    optimized_plan["allowed_count"] >= naive_plan["allowed_count"]
                ),
            },
            "optimization": {
                "strategy": optimized_plan["strategy"],
                "rule": optimized_plan["rule"],
                "improved_over_naive": (
                    optimized_plan["allowed_count"] > naive_plan["allowed_count"]
                ),
                "constraint_bypass_used": False,
            },
            "naive_sequence": naive_plan,
            "optimized_sequence": optimized_plan,
            "actual_sequence": [
                self._compact_step(step) for step in steps
            ],
            "allowed": [
                {
                    "pod": step["pod_name"],
                    "target_node": step["target_node"],
                    "placement_score": (step.get("placement") or {}).get("score"),
                    "affected_pdbs": step["affected"],
                }
                for step in allowed
            ],
            "blocked": [
                {
                    "pod": step["pod_name"],
                    "code": step["code"],
                    "reasons": step["reasons"],
                }
                for step in blocked
            ],
            "visibility": VISIBILITY_RULES,
        }
        self.repo.complete_drain(
            drain_id,
            result,
            naive_plan["allowed_count"],
            optimized_plan["allowed_count"],
            len(allowed),
        )
        persisted = self.repo.get_drain_by_request(request_id)
        assert persisted is not None
        return self._drain_view(persisted, replayed=False)

    @staticmethod
    def _step_payload(
        seq: int,
        pod,
        source_node: str,
        affected: list[dict[str, Any]],
        reasons: list[dict[str, Any]],
        before: dict[str, int],
        after: dict[str, int],
        placement: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "sequence": seq,
            "pod": pod.name,
            "source_node": source_node,
            "decision": "BLOCKED" if reasons else "ALLOWED",
            "code": reasons[0]["code"] if reasons else "ALLOWED",
            "affected_pdbs": affected,
            "blocking_reasons": reasons,
            "placement": placement,
            "availability_before": before,
            "availability_after": after,
        }

    @staticmethod
    def _compact_step(step: dict[str, Any]) -> dict[str, Any]:
        return {
            "sequence": step["seq"],
            "pod": step["pod_name"],
            "source_node": step["source_node"],
            "decision": step["outcome"],
            "code": step["code"],
            "target_node": step["target_node"],
            "affected_pdbs": step["affected"],
            "blocking_reasons": step["reasons"],
            "placement": step["placement"],
            "availability_before": step["availability_before"],
            "availability_after": step["availability_after"],
        }

    def get_drain_detail(self, request_id: str) -> dict[str, Any]:
        drain = self.repo.get_drain_by_request(request_id)
        if drain is None:
            raise BusinessError(
                "E_DRAIN_NOT_FOUND",
                "drain request was not found",
                {"request_id": request_id},
            )
        return self._drain_view(drain, replayed=False)

    def _drain_view(self, drain: dict[str, Any], replayed: bool) -> dict[str, Any]:
        steps = self.repo.list_steps(int(drain["id"]))
        if drain.get("result"):
            output = dict(drain["result"])
            output.update(
                {
                    "status": drain["status"],
                    "replayed": replayed,
                    "created_at": drain["created_at"],
                    "updated_at": drain["updated_at"],
                    "steps": steps,
                }
            )
            return output
        return {
            "drain_id": drain["id"],
            "request_id": drain["request_id"],
            "node": drain["node"],
            "status": drain["status"],
            "created_at": drain["created_at"],
            "updated_at": drain["updated_at"],
            "replayed": replayed,
            "steps": steps,
            "error": drain.get("error"),
            "recovery_hint": (
                "GET /api/v1/recovery/report" if drain["status"] == "ABORTED" else None
            ),
            "visibility": VISIBILITY_RULES,
        }

    def get_recovery_report(self) -> dict[str, Any]:
        report = self.repo.latest_recovery()
        return {
            "available": report is not None,
            "facts_scope": "PERSISTED_ONLY",
            "complete_exit_report": False,
            "report": report,
        }

    def list_drains(self) -> list[dict[str, Any]]:
        # Small local demonstration API; no pagination is needed for the fixed sample.
        connection = self.repo._connect()
        try:
            rows = connection.execute(
                "SELECT request_id, node, status, created_at, updated_at, naive_release_count, "
                "optimized_release_count, actual_release_count FROM drains ORDER BY id"
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            connection.close()

