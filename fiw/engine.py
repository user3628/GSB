"""Orchestration: plan loading, state machine, round execution, recovery."""

from __future__ import annotations

import json
import os
import threading

from .catalog import load_samples
from .errors import (
    BizError,
    E_INVALID_TRANSITION,
    E_NOT_FOUND,
    E_PLAN_INVALID,
    E_ROUND_INVALID,
)
from .models import (
    STATUS_ABORTED,
    STATUS_COMPLETED,
    STATUS_RUNNING,
    Catalog,
    EndpointSpec,
    InjectionItem,
    PlanSpec,
    SchedulerConfig,
)
from .observation import compute_observations
from .scheduler import RoundScheduler
from .statemachine import check_transition
from .store import (
    INTENT_FAILED,
    INTENT_INTERRUPTED,
    INTENT_LOAD_PLAN,
    INTENT_ROUND_EXEC,
    INTENT_STATE_TRANSITION,
    Store,
)


def _maybe_crash(token_scope):
    """Test hook: FIW_CRASH_AFTER_INTENT forces a hard kill right after the
    intent record has been committed, simulating a mid-round power loss."""
    token = os.environ.get("FIW_CRASH_AFTER_INTENT")
    if token and token in ("1", token_scope):
        os._exit(2)


def _domain_from_payload(payload):
    cat_raw = payload["catalog"]
    catalog = Catalog(endpoints=tuple(
        EndpointSpec(endpoint_id=e["endpoint_id"], node=e["node"],
                     weight=e["weight"], upstreams=tuple(e["upstreams"]),
                     host=e["host"], port=e["port"], profile=e["profile"])
        for e in cat_raw["endpoints"]))
    plan_raw = payload["plan"]
    sched = plan_raw["scheduler"]
    plan = PlanSpec(
        plan_id=plan_raw["plan_id"], version=plan_raw["version"],
        rounds=plan_raw["rounds"], fault_switches=dict(plan_raw["fault_switches"]),
        scheduler=SchedulerConfig(
            global_concurrency=sched["global_concurrency"],
            queue_capacity=sched["queue_capacity"],
            round_time_budget_ms=sched["round_time_budget_ms"],
            connect_timeout_ms=sched["connect_timeout_ms"],
            cost_estimates_ms=dict(sched["cost_estimates_ms"]),
        ),
        items=tuple(InjectionItem(
            item_id=i["item_id"], target=i["target"], fault_type=i["fault_type"],
            duration_rounds=i["duration_rounds"], priority=i["priority"])
            for i in plan_raw["items"]),
    )
    return catalog, plan


class Engine:
    def __init__(self, db_path):
        self.store = Store(db_path)
        self._round_locks = {}
        self._round_locks_guard = threading.Lock()

    def close(self):
        self.store.close()

    # ------------------------------------------------------------ plan load
    def load_plan(self, endpoints_path, plan_path):
        catalog, plan = load_samples(endpoints_path, plan_path)
        existing = self.store.get_plan(plan.plan_id)
        if existing is not None:
            if plan.version < existing["version"]:
                raise BizError(
                    E_PLAN_INVALID,
                    "plan version must increase monotonically to replace the "
                    "stored plan",
                    {"stored_version": existing["version"],
                     "offered_version": plan.version})
            if plan.version == existing["version"]:
                return {"plan_id": plan.plan_id, "version": plan.version,
                        "loaded": False,
                        "reason": "same version already loaded; idempotent no-op"}
        intent_id = self.store.begin_intent(
            INTENT_LOAD_PLAN, plan.plan_id,
            {"version": plan.version, "rounds": plan.rounds})
        _maybe_crash(f"{plan.plan_id}:load")
        try:
            self.store.save_plan(catalog, plan, intent_id)
        except Exception:
            self.store.resolve_intent(intent_id, INTENT_FAILED)
            raise
        return {"plan_id": plan.plan_id, "version": plan.version, "loaded": True,
                "status": "PLANNED", "replaced": existing is not None}

    # -------------------------------------------------------- state machine
    def transition(self, plan_id, target):
        row = self._require_plan(plan_id)
        check_transition(row["status"], target)
        intent_id = self.store.begin_intent(
            INTENT_STATE_TRANSITION, plan_id,
            {"from": row["status"], "to": target})
        _maybe_crash(f"{plan_id}:transition")
        try:
            self.store.set_plan_status(plan_id, target, intent_id)
        except Exception:
            self.store.resolve_intent(intent_id, INTENT_FAILED)
            raise
        return {"plan_id": plan_id, "status": target}

    # ---------------------------------------------------------- round exec
    def _round_lock(self, plan_id, round_no):
        with self._round_locks_guard:
            return self._round_locks.setdefault((plan_id, round_no),
                                                threading.Lock())

    def run_round(self, plan_id, round_no):
        row = self._require_plan(plan_id)
        existing = self.store.get_round(plan_id, round_no)
        if existing is not None:
            existing["reused"] = True
            return existing
        lock = self._round_lock(plan_id, round_no)
        with lock:
            existing = self.store.get_round(plan_id, round_no)
            if existing is not None:
                existing["reused"] = True
                return existing
            return self._execute_round(row, plan_id, round_no)

    def _execute_round(self, row, plan_id, round_no):
        catalog, plan = _domain_from_payload(json.loads(row["payload_json"]))
        if row["status"] != STATUS_RUNNING:
            raise BizError(
                E_INVALID_TRANSITION,
                f"rounds can only execute while the plan is RUNNING "
                f"(current: {row['status']})",
                {"plan_id": plan_id, "status": row["status"]})
        if not 1 <= round_no <= plan.rounds:
            raise BizError(E_ROUND_INVALID,
                           f"round {round_no} outside plan range 1..{plan.rounds}",
                           {"plan_id": plan_id, "rounds": plan.rounds})
        intent_id = self.store.begin_intent(
            INTENT_ROUND_EXEC, plan_id, {"round_no": round_no})
        _maybe_crash(f"{plan_id}:{round_no}")
        try:
            result = self._compute_round(catalog, plan, round_no)
            final_status = STATUS_COMPLETED if round_no == plan.rounds else None
            self.store.commit_round(
                plan_id, round_no, result["budget"], result["trajectory"],
                result["observations"], result["skipped"], intent_id,
                final_status=final_status)
        except Exception:
            self.store.resolve_intent(intent_id, INTENT_FAILED)
            raise
        stored = self.store.get_round(plan_id, round_no)
        stored["reused"] = False
        return stored

    def _compute_round(self, catalog, plan, round_no):
        due = [item for item in plan.items if round_no <= item.duration_rounds]
        scheduler = RoundScheduler(plan.scheduler)
        admitted, skipped, remaining_estimate_ms = scheduler.admit(
            due, catalog, round_no, plan.fault_switches)
        executed, wall_ms = scheduler.execute(admitted, catalog)
        trajectory = executed
        observations = compute_observations(catalog, trajectory)
        cfg = plan.scheduler
        budget = {
            "limit_ms": cfg.round_time_budget_ms,
            "reserved_ms": round(cfg.round_time_budget_ms - remaining_estimate_ms, 3),
            "used_ms": round(wall_ms, 3),
            "remaining_ms": round(cfg.round_time_budget_ms - wall_ms, 3),
            "injection_quota": len(admitted),
            "concurrency_limit": cfg.global_concurrency,
            "queue_capacity": cfg.queue_capacity,
        }
        return {"budget": budget, "trajectory": trajectory,
                "observations": observations, "skipped": skipped}

    # -------------------------------------------------------------- queries
    def _require_plan(self, plan_id):
        row = self.store.get_plan(plan_id)
        if row is None:
            raise BizError(E_NOT_FOUND, f"unknown plan {plan_id!r}",
                           {"plan_id": plan_id})
        return row

    def get_plan(self, plan_id):
        row = self._require_plan(plan_id)
        return {"plan_id": row["plan_id"], "version": row["version"],
                "status": row["status"],
                "last_committed_round": self.store.last_committed_round(plan_id),
                "updated_at": row["updated_at"]}

    def get_round(self, plan_id, round_no):
        self._require_plan(plan_id)
        rnd = self.store.get_round(plan_id, round_no)
        if rnd is None:
            raise BizError(E_NOT_FOUND,
                           f"round {round_no} not committed for plan {plan_id!r}",
                           {"plan_id": plan_id, "round_no": round_no})
        return rnd

    # ------------------------------------------------------------- recovery
    def recover(self):
        """Scan unfinished intents and restore the last consistent state."""
        pending = self.store.pending_intents()
        actions = []
        interrupted = []
        for intent in pending:
            kind = intent["kind"]
            plan_id = intent["plan_id"]
            payload = json.loads(intent["payload_json"])
            entry = {"intent_id": intent["intent_id"], "kind": kind,
                     "plan_id": plan_id, "payload": payload,
                     "created_at": intent["created_at"]}
            interrupted.append(entry)
            if kind == INTENT_ROUND_EXEC:
                last = self.store.last_committed_round(plan_id)
                actions.append({
                    "action": "round_rolled_back",
                    "plan_id": plan_id,
                    "interrupted_round": payload.get("round_no"),
                    "last_committed_round": last,
                })
                plan = self.store.get_plan(plan_id)
                if plan is not None and plan["status"] == STATUS_RUNNING:
                    self.store.set_plan_status(plan_id, STATUS_ABORTED)
                    actions.append({"action": "plan_aborted", "plan_id": plan_id,
                                    "reason": "interrupted round intent found "
                                              "during recovery"})
            elif kind == INTENT_STATE_TRANSITION:
                actions.append({"action": "transition_discarded",
                                "plan_id": plan_id,
                                "requested_target": payload.get("to"),
                                "reason": "intent never committed; stored state "
                                          "remains authoritative"})
            elif kind == INTENT_LOAD_PLAN:
                actions.append({"action": "plan_load_discarded",
                                "plan_id": plan_id,
                                "reason": "load intent never committed; previous "
                                          "stored version (if any) remains active"})
            self.store.resolve_intent(intent["intent_id"], INTENT_INTERRUPTED)
        report = {
            "scanned_pending_intents": len(pending),
            "interrupted_intents": interrupted,
            "actions": actions,
            "plans": self.store.list_plans(),
            "basis": "report derived only from persisted intents, plans and "
                     "committed rounds",
        }
        self.store.save_recovery_report(report)
        return report
