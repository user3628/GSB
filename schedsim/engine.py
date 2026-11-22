"""Deterministic multi-budget scheduling engine backed by SQLite facts."""

from __future__ import annotations

import os
import threading
import uuid
from pathlib import Path
from typing import Any, Iterable

from .affinity import (
    PlacedPodFact,
    hard_pod_affinity_rejections,
    hard_pod_anti_affinity_rejections,
    matching_pods_in_topology,
    node_required_affinity_matches,
    node_selector_matches,
    node_term_matches,
    score_candidate,
    topology_value,
    untolerated_taints,
)
from .errors import (
    E_BATCH_NOT_FOUND,
    E_INVALID_TRANSITION,
    E_SCHED_QUEUE_FULL,
    SchedError,
)
from .models import (
    BatchSpec,
    MaterializedBatch,
    NodeSpec,
    PlacementFact,
    PodSpec,
    ResourceVector,
)
from .quantities import QuantityError, subtract_nonnegative
from .spec import canonical_json, parse_batch
from .storage import Storage

TERMINAL_POD_STATUSES = {"PLACED", "UNSCHEDULABLE", "ABORTED"}
RETRYABLE_NODE_REJECTIONS = {"node_placement_cap", "pod_affinity_required"}


class SchedulingEngine:
    """Single-process scheduling coordinator with process-safe SQLite facts."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        crash_after_intent: bool | None = None,
        crash_after_placements: int | None = None,
    ) -> None:
        self.storage = Storage(db_path)
        self.storage.initialize()
        self._lock = threading.RLock()
        self._placements_committed = 0
        if crash_after_intent is None:
            crash_after_intent = os.getenv("SCHEDSIM_CRASH_AFTER_INTENT", "0") == "1"
        if crash_after_placements is None:
            env_value = os.getenv("SCHEDSIM_CRASH_AFTER_PLACEMENTS")
            crash_after_placements = int(env_value) if env_value else None
        self.crash_after_intent = bool(crash_after_intent)
        self.crash_after_placements = crash_after_placements

    def _crash_after_intent(self) -> None:
        if self.crash_after_intent:
            os._exit(91)

    def _crash_after_placement(self) -> None:
        self._placements_committed += 1
        if (
            self.crash_after_placements is not None
            and self._placements_committed >= self.crash_after_placements
        ):
            os._exit(92)

    @staticmethod
    def _generate_batch_id() -> str:
        return f"batch-{uuid.uuid4().hex}"

    def submit_batch(
        self, payload: Any, *, batch_id_override: str | None = None
    ) -> dict[str, Any]:
        with self._lock:
            if isinstance(payload, dict) and payload.get("batch_id") is None and batch_id_override is None:
                payload = dict(payload)
                payload["batch_id"] = self._generate_batch_id()
            spec = parse_batch(payload, batch_id_override=batch_id_override)
            if self.storage.batch_exists(spec.batch_id):
                raise SchedError(
                    E_INVALID_TRANSITION,
                    "batch_id already exists",
                    {"batch_id": spec.batch_id},
                    http_status=409,
                )

            queued_count = max(0, len(spec.pods) - spec.budget.concurrency_limit)
            if queued_count > spec.budget.queue_capacity:
                raise SchedError(
                    E_SCHED_QUEUE_FULL,
                    "bounded scheduling wait queue is full; no batch was persisted",
                    {
                        "batch_id": spec.batch_id,
                        "pod_count": len(spec.pods),
                        "concurrency_limit": spec.budget.concurrency_limit,
                        "queue_capacity": spec.budget.queue_capacity,
                        "queued_count": queued_count,
                    },
                    http_status=429,
                )

            intent_id = self.storage.create_intent(
                spec.batch_id,
                "SUBMIT_BATCH",
                {"pod_count": len(spec.pods), "node_count": len(spec.nodes)},
            )
            self.storage.insert_batch(spec, payload, intent_id)
            return {
                "batch_id": spec.batch_id,
                "status": "PENDING",
                "node_count": len(spec.nodes),
                "pod_count": len(spec.pods),
                "active_slots": min(len(spec.pods), spec.budget.concurrency_limit),
                "waiting_queue": queued_count,
                "budget": spec.budget.to_dict(),
                "nodes": [node.to_dict() for node in spec.nodes],
            }

    def _batch_or_404(self, batch_id: str) -> dict[str, Any]:
        batch = self.storage.get_batch(batch_id)
        if batch is None:
            raise SchedError(
                E_BATCH_NOT_FOUND,
                "batch was not found",
                {"batch_id": batch_id},
                http_status=404,
            )
        return batch

    def _load_spec(self, batch_row: dict[str, Any]) -> BatchSpec:
        return parse_batch(
            batch_row["original_payload"], batch_id_override=batch_row["batch_id"]
        )

    def _materialize(
        self,
        spec: BatchSpec,
        placements: Iterable[PlacementFact],
        *,
        round_no: int | None = None,
    ) -> MaterializedBatch:
        node_by_name = spec.node_by_name
        remaining = {
            node.name: ResourceVector(
                node.allocatable.cpu_millicores, node.allocatable.memory_bytes
            )
            for node in spec.nodes
        }
        round_node_counts: dict[str, int] = {}
        placed_names: set[str] = set()
        ordered = sorted(placements, key=lambda item: (item.sequence, item.placement_id))
        for placement in ordered:
            if placement.node_name not in remaining:
                raise SchedError(
                    E_INVALID_TRANSITION,
                    "persisted placement references an unknown node",
                    {"placement_id": placement.placement_id, "node": placement.node_name},
                    http_status=409,
                )
            before = remaining[placement.node_name]
            try:
                after = ResourceVector(
                    subtract_nonnegative(
                        before.cpu_millicores,
                        placement.requests.cpu_millicores,
                        field="remaining cpu millicores",
                    ),
                    subtract_nonnegative(
                        before.memory_bytes,
                        placement.requests.memory_bytes,
                        field="remaining memory bytes",
                    ),
                )
            except QuantityError as exc:
                raise SchedError(
                    E_INVALID_TRANSITION,
                    "persisted placements overdraw a node",
                    {
                        "placement_id": placement.placement_id,
                        "node": placement.node_name,
                        "reason": str(exc),
                    },
                    http_status=409,
                ) from exc
            remaining[placement.node_name] = after
            placed_names.add(placement.pod_name)
            if round_no is not None and placement.round_no == round_no:
                round_node_counts[placement.node_name] = (
                    round_node_counts.get(placement.node_name, 0) + 1
                )
        return MaterializedBatch(
            batch=spec,
            remaining=remaining,
            placements=ordered,
            placed_pod_names=placed_names,
            round_node_counts=round_node_counts,
        )

    def _placed_pod_facts(
        self, spec: BatchSpec, placements: Iterable[PlacementFact]
    ) -> list[PlacedPodFact]:
        pods = spec.pod_by_name
        facts: list[PlacedPodFact] = []
        for placement in placements:
            pod = pods.get(placement.pod_name)
            if pod is None:
                continue
            facts.append(
                PlacedPodFact(
                    pod_name=pod.name,
                    namespace=pod.namespace,
                    labels=dict(pod.labels),
                    node_name=placement.node_name,
                )
            )
        return facts

    @staticmethod
    def _decision(
        pod: PodSpec,
        node: NodeSpec | None,
        constraint_type: str,
        constraint_text: str,
        remaining: ResourceVector,
        *,
        detail: dict[str, Any] | None = None,
        phase: str = "rejection",
    ) -> dict[str, Any]:
        return {
            "phase": phase,
            "node_name": node.name if node is not None else None,
            "constraint_type": constraint_type,
            "constraint_text": constraint_text,
            "remaining_snapshot": remaining.to_dict(),
            "detail": detail or {},
            "pod_name": pod.name,
        }

    def _evaluate_nodes(
        self,
        spec: BatchSpec,
        pod: PodSpec,
        materialized: MaterializedBatch,
        placed_facts: list[PlacedPodFact],
    ) -> tuple[list[dict[str, Any]], dict[str, Any] | None, list[dict[str, Any]]]:
        rejections: list[dict[str, Any]] = []
        candidates: list[dict[str, Any]] = []
        node_by_name = spec.node_by_name

        for node in sorted(spec.nodes, key=lambda item: item.name):
            before = materialized.remaining[node.name]
            node_rejections: list[dict[str, Any]] = []

            def deny(
                constraint_type: str,
                constraint_text: str,
                *,
                detail: dict[str, Any] | None = None,
            ) -> None:
                node_rejections.append(
                    self._decision(
                        pod,
                        node,
                        constraint_type,
                        constraint_text,
                        before,
                        detail=detail,
                    )
                )

            if not node.healthy:
                deny("node_unhealthy", "healthy=false", detail={"healthy": False})

            for key in sorted(pod.node_selector):
                expected = pod.node_selector[key]
                actual = node.labels.get(key)
                if actual != expected:
                    deny(
                        "node_selector",
                        canonical_json({"key": key, "operator": "Equals", "values": [expected]}),
                        detail={"key": key, "expected": expected, "actual": actual},
                    )

            if pod.node_affinity.required_terms and not node_required_affinity_matches(pod, node):
                for term_index, term in enumerate(pod.node_affinity.required_terms):
                    deny(
                        "node_affinity_required",
                        term.source_text,
                        detail={"term_index": term_index},
                    )

            for taint in untolerated_taints(pod, node, effect="NoSchedule"):
                deny(
                    "taint_no_schedule",
                    taint.source_text,
                    detail={"taint_key": taint.key, "effect": "NoSchedule"},
                )
            for taint in untolerated_taints(pod, node, effect="NoExecute"):
                deny(
                    "taint_no_execute",
                    taint.source_text,
                    detail={"taint_key": taint.key, "effect": "NoExecute"},
                )

            if pod.requests.cpu_millicores > before.cpu_millicores:
                deny(
                    "insufficient_cpu",
                    canonical_json({"request": pod.raw_requests.get("cpu"), "remaining": before.cpu_millicores}),
                    detail={
                        "requested_millicores": pod.requests.cpu_millicores,
                        "remaining_millicores": before.cpu_millicores,
                        "shortfall_millicores": pod.requests.cpu_millicores - before.cpu_millicores,
                    },
                )
            if pod.requests.memory_bytes > before.memory_bytes:
                deny(
                    "insufficient_memory",
                    canonical_json({"request": pod.raw_requests.get("memory"), "remaining": before.memory_bytes}),
                    detail={
                        "requested_bytes": pod.requests.memory_bytes,
                        "remaining_bytes": before.memory_bytes,
                        "shortfall_bytes": pod.requests.memory_bytes - before.memory_bytes,
                    },
                )

            for term in pod.pod_affinity.required_terms:
                if topology_value(node, term.topology_key) is None:
                    deny(
                        "topology_key_missing",
                        term.source_text,
                        detail={
                            "topology_key": term.topology_key,
                            "node": node.name,
                            "scope": "podAffinity",
                        },
                    )
            for term in pod.pod_anti_affinity.required_terms:
                if topology_value(node, term.topology_key) is None:
                    deny(
                        "topology_key_missing",
                        term.source_text,
                        detail={
                            "topology_key": term.topology_key,
                            "node": node.name,
                            "scope": "podAntiAffinity",
                        },
                    )

            for term in hard_pod_affinity_rejections(pod, node, placed_facts, node_by_name):
                if topology_value(node, term.topology_key) is None:
                    continue
                deny(
                    "pod_affinity_required",
                    term.source_text,
                    detail={
                        "topology_key": term.topology_key,
                        "satisfied": False,
                        "scope": "podAffinity",
                    },
                )
            for term, matches in hard_pod_anti_affinity_rejections(
                pod, node, placed_facts, node_by_name
            ):
                if topology_value(node, term.topology_key) is None:
                    continue
                deny(
                    "pod_anti_affinity_required",
                    term.source_text,
                    detail={
                        "topology_key": term.topology_key,
                        "violated_by": [match.pod_name for match in matches],
                        "scope": "podAntiAffinity",
                    },
                )

            placements_on_node = materialized.round_node_counts.get(node.name, 0)
            if placements_on_node >= spec.budget.per_node_round_limit:
                deny(
                    "node_placement_cap",
                    f"per_node_round_limit={spec.budget.per_node_round_limit}",
                    detail={
                        "placements_this_round": placements_on_node,
                        "per_node_round_limit": spec.budget.per_node_round_limit,
                    },
                )

            if node_rejections:
                rejections.extend(node_rejections)
                continue

            after = ResourceVector(
                before.cpu_millicores - pod.requests.cpu_millicores,
                before.memory_bytes - pod.requests.memory_bytes,
            )
            score_breakdown = score_candidate(
                pod,
                node,
                after,
                placed_facts,
                node_by_name,
                tie_tolerance=spec.budget.score_tolerance,
            )
            candidates.append(
                {
                    "node": node,
                    "remaining_before": before,
                    "remaining_after": after,
                    "score_breakdown": score_breakdown,
                    "total": int(score_breakdown["total"]),
                }
            )

        if not candidates:
            return rejections, None, []

        max_score = max(candidate["total"] for candidate in candidates)
        tolerance = spec.budget.score_tolerance
        within_tolerance = [
            candidate for candidate in candidates if candidate["total"] >= max_score - tolerance
        ]
        selected = sorted(within_tolerance, key=lambda item: item["node"].name)[0]
        all_scores = [
            {
                "node": candidate["node"].name,
                "score": candidate["total"],
                "components": candidate["score_breakdown"]["components"],
                "selected_by_tie_break": candidate is selected,
            }
            for candidate in candidates
        ]
        selected_breakdown = dict(selected["score_breakdown"])
        selected_breakdown["affinity_satisfaction"] = self._affinity_satisfaction(
            pod, selected["node"], placed_facts, node_by_name
        )
        selected_breakdown["selection"] = {
            "max_candidate_score": max_score,
            "tolerance": tolerance,
            "selected_node": selected["node"].name,
            "within_tolerance": sorted(
                candidate["node"].name for candidate in within_tolerance
            ),
            "node_scores": all_scores,
        }
        selected["score_breakdown"] = selected_breakdown
        return rejections, selected, all_scores

    def _affinity_satisfaction(
        self,
        pod: PodSpec,
        node: NodeSpec,
        placed_facts: list[PlacedPodFact],
        node_by_name: dict[str, NodeSpec],
    ) -> dict[str, Any]:
        node_terms = [
            {
                "term_index": index,
                "matched": node_term_matches(term, node),
                "constraint": term.source_text,
            }
            for index, term in enumerate(pod.node_affinity.required_terms)
        ]
        pod_affinity_terms = [
            {
                "term_index": index,
                "satisfied": bool(
                    matching_pods_in_topology(term, pod, node, placed_facts, node_by_name)
                ),
                "matched_pods": [
                    placed.pod_name
                    for placed in matching_pods_in_topology(
                        term, pod, node, placed_facts, node_by_name
                    )
                ],
                "constraint": term.source_text,
            }
            for index, term in enumerate(pod.pod_affinity.required_terms)
        ]
        pod_anti_terms = [
            {
                "term_index": index,
                "violated": bool(
                    matching_pods_in_topology(term, pod, node, placed_facts, node_by_name)
                ),
                "matching_pods": [
                    placed.pod_name
                    for placed in matching_pods_in_topology(
                        term, pod, node, placed_facts, node_by_name
                    )
                ],
                "constraint": term.source_text,
            }
            for index, term in enumerate(pod.pod_anti_affinity.required_terms)
        ]
        return {
            "node_selector": {
                "satisfied": node_selector_matches(pod, node),
                "selector": dict(sorted(pod.node_selector.items())),
            },
            "required_node_affinity": {
                "satisfied": not node_terms or any(term["matched"] for term in node_terms),
                "terms": node_terms,
            },
            "taints": {
                "no_schedule_satisfied": not untolerated_taints(
                    pod, node, effect="NoSchedule"
                ),
                "no_execute_satisfied": not untolerated_taints(
                    pod, node, effect="NoExecute"
                ),
                "prefer_no_schedule_penalties": [
                    taint.source_text
                    for taint in untolerated_taints(
                        pod, node, effect="PreferNoSchedule"
                    )
                ],
            },
            "required_pod_affinity": {
                "satisfied": all(term["satisfied"] for term in pod_affinity_terms),
                "terms": pod_affinity_terms,
            },
            "required_pod_anti_affinity": {
                "satisfied": all(not term["violated"] for term in pod_anti_terms),
                "terms": pod_anti_terms,
            },
        }

    def _pending_candidates(
        self, spec: BatchSpec, materialized: MaterializedBatch, states: dict[str, dict[str, Any]]
    ) -> list[PodSpec]:
        candidates: list[PodSpec] = []
        for pod in spec.pods:
            if pod.name in materialized.placed_pod_names:
                continue
            state = states.get(pod.name, {})
            if state.get("status") in TERMINAL_POD_STATUSES:
                continue
            candidates.append(pod)
        return sorted(
            candidates,
            key=lambda pod: (-pod.priority, pod.arrival_seq, pod.name),
        )

    def _queue_snapshot(
        self, spec: BatchSpec, states: dict[str, dict[str, Any]]
    ) -> dict[str, Any]:
        pending = [
            pod
            for pod in spec.pods
            if states.get(pod.name, {}).get("status") not in TERMINAL_POD_STATUSES
        ]
        pending.sort(key=lambda pod: (-pod.priority, pod.arrival_seq, pod.name))
        active = min(len(pending), spec.budget.concurrency_limit)
        return {
            "active_slots": active,
            "waiting_count": max(0, len(pending) - active),
            "capacity": spec.budget.queue_capacity,
            "pending_pods": [pod.name for pod in pending],
            "waiting_pods": [pod.name for pod in pending[active:]],
        }

    def run_round(self, batch_id: str, *, round_no: int | None = None) -> dict[str, Any]:
        with self._lock:
            batch_row = self._batch_or_404(batch_id)
            if batch_row["status"] in {"COMPLETED", "ABORTED"}:
                raise SchedError(
                    E_INVALID_TRANSITION,
                    "cannot start a scheduling round for a terminal batch",
                    {"batch_id": batch_id, "status": batch_row["status"]},
                    http_status=409,
                )
            spec = self._load_spec(batch_row)
            existing_placements = self.storage.get_placements(batch_id)
            materialized = self._materialize(spec, existing_placements)
            states = self.storage.get_pod_states_map(batch_id)
            candidates = self._pending_candidates(spec, materialized, states)

            next_round = self.storage.next_round_no(batch_id)
            selected_round_no = next_round if round_no is None else round_no
            if selected_round_no != next_round:
                raise SchedError(
                    E_INVALID_TRANSITION,
                    "round_no must be the next monotonic round number",
                    {
                        "batch_id": batch_id,
                        "requested_round_no": selected_round_no,
                        "next_round_no": next_round,
                    },
                    http_status=409,
                )
            if selected_round_no < 1:
                raise SchedError(
                    E_INVALID_TRANSITION,
                    "round_no must be at least 1",
                    {"batch_id": batch_id, "round_no": selected_round_no},
                    http_status=409,
                )

            intent_id = self.storage.create_intent(
                batch_id,
                "RUN_ROUND",
                {"round_no": selected_round_no, "candidate_pods": [pod.name for pod in candidates]},
            )
            self._crash_after_intent()
            self.storage.create_round(batch_id, selected_round_no, spec.budget.to_dict())

            placed_facts = self._placed_pod_facts(spec, existing_placements)
            node_count = len(spec.nodes)
            cost_per_pod = max(1, node_count * spec.budget.node_check_cost_ms)
            spent_ms = 0
            concurrency_used = 0
            round_placements: list[dict[str, Any]] = []
            unschedulable: list[dict[str, Any]] = []
            skipped: list[dict[str, Any]] = []
            budget_exhausted = False

            processed_index = 0
            for index, pod in enumerate(candidates):
                if concurrency_used >= spec.budget.concurrency_limit:
                    for remaining_pod in candidates[index:]:
                        skipped.append(
                            {
                                "pod_name": remaining_pod.name,
                                "priority": remaining_pod.priority,
                                "arrival_seq": remaining_pod.arrival_seq,
                                "reason": "CONCURRENCY_LIMIT",
                            }
                        )
                        self.storage.commit_pod_decision(
                            batch_id=batch_id,
                            round_no=selected_round_no,
                            pod_name=remaining_pod.name,
                            decisions=[
                                self._decision(
                                    remaining_pod,
                                    None,
                                    "CONCURRENCY_LIMIT",
                                    f"concurrency_limit={spec.budget.concurrency_limit}",
                                    materialized.remaining[
                                        sorted(spec.nodes, key=lambda item: item.name)[0].name
                                    ],
                                    phase="skip",
                                )
                            ],
                            placement=None,
                            pod_status="PENDING",
                            pod_reason="CONCURRENCY_LIMIT",
                        )
                    processed_index = len(candidates)
                    break

                if spent_ms + cost_per_pod > spec.budget.time_budget_ms:
                    budget_exhausted = True
                    for remaining_pod in candidates[index:]:
                        skipped.append(
                            {
                                "pod_name": remaining_pod.name,
                                "priority": remaining_pod.priority,
                                "arrival_seq": remaining_pod.arrival_seq,
                                "reason": "ROUND_TIME_BUDGET_EXHAUSTED",
                            }
                        )
                        self.storage.commit_pod_decision(
                            batch_id=batch_id,
                            round_no=selected_round_no,
                            pod_name=remaining_pod.name,
                            decisions=[
                                self._decision(
                                    remaining_pod,
                                    None,
                                    "ROUND_TIME_BUDGET_EXHAUSTED",
                                    f"time_budget_ms={spec.budget.time_budget_ms}",
                                    materialized.remaining[
                                        sorted(spec.nodes, key=lambda item: item.name)[0].name
                                    ],
                                    detail={
                                        "spent_ms": spent_ms,
                                        "next_pod_cost_ms": cost_per_pod,
                                    },
                                    phase="skip",
                                )
                            ],
                            placement=None,
                            pod_status="PENDING",
                            pod_reason="ROUND_TIME_BUDGET_EXHAUSTED",
                        )
                    processed_index = len(candidates)
                    break

                spent_ms += cost_per_pod
                concurrency_used += 1
                rejections, selected, _ = self._evaluate_nodes(
                    spec, pod, materialized, placed_facts
                )
                decisions = list(rejections)
                if selected is not None:
                    node: NodeSpec = selected["node"]
                    before: ResourceVector = selected["remaining_before"]
                    after: ResourceVector = selected["remaining_after"]
                    seq_no = self.storage.next_placement_sequence(batch_id)
                    placement = {
                        "seq_no": seq_no,
                        "node_name": node.name,
                        "requests": pod.requests,
                        "remaining_before": before,
                        "remaining_after": after,
                        "score": int(selected["total"]),
                        "score_breakdown": selected["score_breakdown"],
                    }
                    decisions.append(
                        self._decision(
                            pod,
                            node,
                            "placed",
                            canonical_json(
                                {
                                    "node": node.name,
                                    "score": int(selected["total"]),
                                    "tie_tolerance": spec.budget.score_tolerance,
                                }
                            ),
                            after,
                            detail={
                                "score_breakdown": selected["score_breakdown"],
                                "remaining_before": before.to_dict(),
                                "remaining_after": after.to_dict(),
                            },
                            phase="placement",
                        )
                    )
                    self.storage.commit_pod_decision(
                        batch_id=batch_id,
                        round_no=selected_round_no,
                        pod_name=pod.name,
                        decisions=decisions,
                        placement=placement,
                        pod_status="PLACED",
                        pod_reason=None,
                    )
                    self._crash_after_placement()
                    materialized.remaining[node.name] = after
                    materialized.placed_pod_names.add(pod.name)
                    materialized.round_node_counts[node.name] = (
                        materialized.round_node_counts.get(node.name, 0) + 1
                    )
                    placed_facts.append(
                        PlacedPodFact(
                            pod_name=pod.name,
                            namespace=pod.namespace,
                            labels=dict(pod.labels),
                            node_name=node.name,
                        )
                    )
                    round_placements.append(
                        {
                            "pod_name": pod.name,
                            "node_name": node.name,
                            "requests": pod.requests.to_dict(),
                            "remaining_before": before.to_dict(),
                            "remaining_after": after.to_dict(),
                            "score": int(selected["total"]),
                            "score_breakdown": selected["score_breakdown"],
                            "seq_no": seq_no,
                        }
                    )
                    processed_index = index + 1
                    continue

                rejection_types = {decision["constraint_type"] for decision in rejections}
                types_by_node: dict[str, set[str]] = {}
                for decision in rejections:
                    node_key = decision.get("node_name") or "<pod>"
                    types_by_node.setdefault(node_key, set()).add(decision["constraint_type"])
                retryable = any(
                    node_types.issubset(RETRYABLE_NODE_REJECTIONS)
                    for node_types in types_by_node.values()
                )
                if retryable and rejection_types == {"node_placement_cap"}:
                    reason = "NODE_PLACEMENT_CAP_RETRY"
                elif retryable:
                    reason = "POD_AFFINITY_PENDING"
                else:
                    reason = "ALL_NODES_REJECTED"
                decisions.append(
                    self._decision(
                        pod,
                        None,
                        reason,
                        "all candidate nodes were rejected",
                        materialized.remaining[
                            sorted(spec.nodes, key=lambda item: item.name)[0].name
                        ],
                        detail={"rejection_types": sorted(rejection_types)},
                        phase="skip",
                    )
                )
                self.storage.commit_pod_decision(
                    batch_id=batch_id,
                    round_no=selected_round_no,
                    pod_name=pod.name,
                    decisions=decisions,
                    placement=None,
                    pod_status="PENDING" if retryable else "UNSCHEDULABLE",
                    pod_reason=reason,
                )
                unschedulable.append(
                    {
                        "pod_name": pod.name,
                        "priority": pod.priority,
                        "arrival_seq": pod.arrival_seq,
                        "status": "PENDING" if retryable else "UNSCHEDULABLE",
                        "reason": reason,
                    }
                )
                processed_index = index + 1

            final_placements = self.storage.get_placements(batch_id)
            final_materialized = self._materialize(
                spec, final_placements, round_no=selected_round_no
            )
            final_states = self.storage.get_pod_states_map(batch_id)
            has_pending = any(
                state.get("status") not in TERMINAL_POD_STATUSES
                for state in final_states.values()
            )
            batch_status = "INCOMPLETE" if has_pending else "COMPLETED"
            result = {
                "batch_id": batch_id,
                "round_no": selected_round_no,
                "placements": round_placements,
                "unschedulable_pods": unschedulable,
                "skipped_pods": skipped,
                "budget_usage": {
                    "spent_ms": spent_ms,
                    "time_budget_ms": spec.budget.time_budget_ms,
                    "time_budget_exhausted": budget_exhausted,
                    "concurrency_limit": spec.budget.concurrency_limit,
                    "concurrency_used": concurrency_used,
                    "per_node_round_limit": spec.budget.per_node_round_limit,
                    "node_round_placements": dict(sorted(final_materialized.round_node_counts.items())),
                },
                "remaining": final_materialized.remaining_snapshot(),
                "batch_status": batch_status,
                "candidate_count": len(candidates),
                "processed_candidate_count": processed_index,
                "queue_after": self._queue_snapshot(spec, final_states),
            }
            self.storage.finalize_round(
                batch_id=batch_id,
                round_no=selected_round_no,
                intent_id=intent_id,
                spent_ms=spent_ms,
                concurrency_used=concurrency_used,
                skipped=skipped,
                result=result,
                batch_status=batch_status,
            )
            return result

    def abort_batch(self, batch_id: str, reason: str = "operator requested abort") -> dict[str, Any]:
        with self._lock:
            batch = self._batch_or_404(batch_id)
            if batch["status"] in {"COMPLETED", "ABORTED"}:
                raise SchedError(
                    E_INVALID_TRANSITION,
                    "cannot abort a terminal batch",
                    {"batch_id": batch_id, "status": batch["status"]},
                    http_status=409,
                )
            intent_id = self.storage.create_intent(
                batch_id, "ABORT_BATCH", {"reason": reason}
            )
            self._crash_after_intent()
            self.storage.abort_batch(batch_id, intent_id, reason)
            return {"batch_id": batch_id, "status": "ABORTED", "reason": reason}

    def get_batch_summary(self, batch_id: str) -> dict[str, Any]:
        batch = self._batch_or_404(batch_id)
        spec = self._load_spec(batch)
        states = self.storage.get_pod_states(batch_id)
        counts = self.storage.get_counts(batch_id)
        return {
            "batch_id": batch_id,
            "status": batch["status"],
            "created_at": batch["created_at"],
            "updated_at": batch["updated_at"],
            "budget": batch["budget"],
            "counts": counts,
            "queue": self._queue_snapshot(
                spec, {state["pod_name"]: state for state in states}
            ),
            "pod_states": states,
            "rounds": self.storage.get_rounds(batch_id),
        }

    def get_results(self, batch_id: str) -> dict[str, Any]:
        batch = self._batch_or_404(batch_id)
        spec = self._load_spec(batch)
        placements = self.storage.get_placements(batch_id)
        materialized = self._materialize(spec, placements)
        decisions = self.storage.get_decisions(batch_id)
        rejections = [item for item in decisions if item["phase"] == "rejection"]
        placement_payloads = [
            {
                "pod_name": placement.pod_name,
                "node_name": placement.node_name,
                "round_no": placement.round_no,
                "seq_no": placement.sequence,
                "requests": placement.requests.to_dict(),
                "remaining_before": placement.remaining_before.to_dict(),
                "remaining_after": placement.remaining_after.to_dict(),
                "score": placement.score,
                "score_breakdown": placement.score_breakdown,
            }
            for placement in placements
        ]
        return {
            "batch_id": batch_id,
            "status": batch["status"],
            "budget": batch["budget"],
            "placements": placement_payloads,
            "rejections": rejections,
            "pod_verdicts": self.storage.get_pod_states(batch_id),
            "rounds": self.storage.get_rounds(batch_id),
            "remaining": materialized.remaining_snapshot(),
        }

    def get_placements(self, batch_id: str) -> dict[str, Any]:
        results = self.get_results(batch_id)
        return {
            "batch_id": batch_id,
            "status": results["status"],
            "placements": results["placements"],
            "remaining": results["remaining"],
        }

    def get_rejections(self, batch_id: str, pod_name: str | None = None) -> dict[str, Any]:
        self._batch_or_404(batch_id)
        if pod_name is not None:
            with self.storage.read() as connection:
                exists = connection.execute(
                    "SELECT 1 FROM pod_states WHERE batch_id = ? AND pod_name = ?",
                    (batch_id, pod_name),
                ).fetchone()
            if exists is None:
                raise SchedError(
                    E_BATCH_NOT_FOUND,
                    "Pod was not found in batch",
                    {"batch_id": batch_id, "pod_name": pod_name},
                    http_status=404,
                )
        decisions = self.storage.get_decisions(batch_id, pod_name)
        by_pod: dict[str, list[dict[str, Any]]] = {}
        for item in decisions:
            if item["phase"] != "rejection":
                continue
            by_pod.setdefault(item["pod_name"], []).append(item)
        return {
            "batch_id": batch_id,
            "pod_name": pod_name,
            "rejections": [item for item in decisions if item["phase"] == "rejection"],
            "by_pod": {name: rows for name, rows in sorted(by_pod.items())},
        }

    def get_node_usage(self, batch_id: str, node_name: str) -> dict[str, Any]:
        batch = self._batch_or_404(batch_id)
        spec = self._load_spec(batch)
        node = spec.node_by_name.get(node_name)
        if node is None:
            raise SchedError(
                E_BATCH_NOT_FOUND,
                "node was not found in batch",
                {"batch_id": batch_id, "node_name": node_name},
                http_status=404,
            )
        placements = self.storage.get_placements(batch_id)
        materialized = self._materialize(spec, placements)
        node_placements = [item for item in placements if item.node_name == node_name]
        decisions = [item for item in self.storage.get_decisions(batch_id) if item["node_name"] == node_name]
        rounds = self.storage.get_rounds(batch_id)
        round_counts = {
            str(round_row["round_no"]): sum(
                1
                for placement in node_placements
                if placement.round_no == int(round_row["round_no"])
            )
            for round_row in rounds
        }
        return {
            "batch_id": batch_id,
            "node": node.to_dict(),
            "remaining": materialized.remaining[node_name].to_dict(),
            "placements": [
                {
                    "pod_name": placement.pod_name,
                    "round_no": placement.round_no,
                    "seq_no": placement.sequence,
                    "requests": placement.requests.to_dict(),
                    "remaining_before": placement.remaining_before.to_dict(),
                    "remaining_after": placement.remaining_after.to_dict(),
                    "score": placement.score,
                }
                for placement in node_placements
            ],
            "round_placement_counts": round_counts,
            "constraint_details": decisions,
        }

    def _recovery_report_for_intent(self, intent: dict[str, Any]) -> tuple[dict[str, Any], str]:
        batch_id = intent["batch_id"]
        batch = self.storage.get_batch(batch_id)
        if batch is None:
            report = {
                "report_version": 1,
                "factual_only": True,
                "derived_from": "sqlite_persisted_facts_only",
                "batch_id": batch_id,
                "batch_exists": False,
                "recovered_intent": {
                    "id": intent["id"],
                    "operation": intent["operation"],
                    "status_before": "PENDING",
                    "status_after": "RECOVERED",
                },
                "persisted_facts": {
                    "placements": [],
                    "rounds": [],
                    "decisions": [],
                    "pod_states": [],
                },
                "recovery_actions": [
                    "The pending intent was marked RECOVERED. No batch row was fabricated."
                ],
                "exit_report_available": False,
                "exit_report": None,
                "process_exit_observed": False,
            }
            return report, "INCOMPLETE"

        spec = self._load_spec(batch)
        placements = self.storage.get_placements(batch_id)
        materialized = self._materialize(spec, placements)
        rounds = self.storage.get_rounds(batch_id)
        decisions = self.storage.get_decisions(batch_id)
        states = self.storage.get_pod_states(batch_id)
        status_after = "ABORTED" if intent["operation"] == "ABORT_BATCH" else "INCOMPLETE"
        recovery_actions = (
            [
                "Completed the persisted abort intent: unfinished rounds and Pods were marked ABORTED.",
                "No process exit report was inferred or claimed.",
            ]
            if status_after == "ABORTED"
            else [
                "Marked the unfinished round(s) INTERRUPTED without inventing an exit report.",
                f"Set batch status to {status_after}; later persisted facts remain authoritative.",
            ]
        )
        report = {
            "report_version": 1,
            "factual_only": True,
            "derived_from": "sqlite_persisted_facts_only",
            "batch_id": batch_id,
            "batch_exists": True,
            "batch_status_before": batch["status"],
            "batch_status_after": status_after,
            "recovered_intent": {
                "id": intent["id"],
                "operation": intent["operation"],
                "status_before": "PENDING",
                "status_after": "RECOVERED",
                "payload": intent.get("payload", {}),
            },
            "persisted_facts": {
                "placements": [
                    {
                        "placement_id": placement.placement_id,
                        "pod_name": placement.pod_name,
                        "node_name": placement.node_name,
                        "round_no": placement.round_no,
                        "seq_no": placement.sequence,
                        "requests": placement.requests.to_dict(),
                        "remaining_before": placement.remaining_before.to_dict(),
                        "remaining_after": placement.remaining_after.to_dict(),
                        "score": placement.score,
                    }
                    for placement in placements
                ],
                "rounds": rounds,
                "decision_count": len(decisions),
                "rejection_count": sum(1 for item in decisions if item["phase"] == "rejection"),
                "pod_states": states,
            },
            "recomputed": {
                "remaining": materialized.remaining_snapshot(),
                "placed_pods": sorted(materialized.placed_pod_names),
            },
            "recovery_actions": recovery_actions,
            "exit_report_available": False,
            "exit_report": None,
            "process_exit_observed": False,
            "limitations": [
                "No graceful process exit report was persisted; none is inferred or claimed."
            ],
        }
        return report, status_after

    def recover_pending(self) -> list[dict[str, Any]]:
        with self._lock:
            pending = self.storage.find_pending_intents()
            reports: list[dict[str, Any]] = []
            for intent in pending:
                batch_id = intent["batch_id"]
                report, status_after = self._recovery_report_for_intent(intent)
                recovery_intent_id = self.storage.apply_recovery(
                    batch_id=batch_id,
                    target_intent_id=int(intent["id"]),
                    report=report,
                    batch_status_after=status_after,
                )
                report["recovery_intent_id"] = recovery_intent_id
                reports.append(
                    {
                        "batch_id": batch_id,
                        "target_intent_id": intent["id"],
                        "report": report,
                    }
                )
            return reports

    def get_recovery_reports(self, batch_id: str) -> dict[str, Any]:
        self._batch_or_404(batch_id)
        rows = self.storage.get_recoveries(batch_id)
        return {
            "batch_id": batch_id,
            "recovery_report_available": bool(rows),
            "reports": rows,
            "exit_report_available": False,
            "exit_report": None,
            "statement": (
                "Only SQLite-persisted facts and explicit recovery actions are reported. "
                "No process exit report is claimed."
            ),
        }

    def get_all_recovery_reports(self) -> dict[str, Any]:
        rows = self.storage.get_recoveries()
        return {
            "recovery_report_count": len(rows),
            "reports": rows,
            "exit_report_available": False,
            "exit_report": None,
            "statement": (
                "Only SQLite-persisted facts and explicit recovery actions are reported. "
                "No process exit report is claimed."
            ),
        }

