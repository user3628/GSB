"""Round engine: admission -> parallel execution -> failover adjudication.

All state mutations for a round happen in exactly two serialized write
transactions (admission, adjudication) bracketing a side-effect-free
concurrent execution phase, so observers only ever see complete versions.
"""
from __future__ import annotations

import heapq
import json
import queue
import threading
import time
import uuid

from . import statemachine as sm
from .db import Database
from .errors import (BizError, E_FAULT_DOMAIN_FULL, E_INJECTION_QUEUE_FULL,
                     E_INJECTION_TIMEOUT, E_FAILOVER_DENIED, E_ROUND_ABORTED,
                     SKIP_CONCURRENCY, SKIP_DUPLICATE_SERVICE, SKIP_QUOTA,
                     SKIP_QUEUE_FULL, SKIP_TIME_BUDGET)
from .executor import execute_injection


# ---------------------------------------------------------------------------
# deterministic scheduling order: priority desc, fault-domain round-robin
# ---------------------------------------------------------------------------
def deterministic_order(items: list[dict]) -> list[dict]:
    by_domain: dict[str, list[dict]] = {}
    for it in items:
        by_domain.setdefault(it["fault_domain"], []).append(it)
    heap: list[tuple] = []
    for domain in sorted(by_domain):
        group = sorted(by_domain[domain], key=lambda i: (-i["priority"], i["item_key"]))
        for rr, it in enumerate(group):
            heap.append((-it["priority"], rr, domain, it["item_key"], it))
    heapq.heapify(heap)
    ordered = []
    while heap:
        ordered.append(heapq.heappop(heap)[4])
    return ordered


def active_items(plan: dict, round_no: int) -> list[dict]:
    return [it for it in plan["items"]
            if it["start_round"] <= round_no < it["start_round"] + it["duration_rounds"]]


# ---------------------------------------------------------------------------
# admission (single write transaction)
# ---------------------------------------------------------------------------
def _domain_occupancy(conn, domain: str) -> list[dict]:
    rows = conn.execute(
        "SELECT service, injection_id, round_id FROM domain_quota_ledger"
        " WHERE fault_domain=? AND released_at IS NULL ORDER BY id", (domain,)).fetchall()
    return [dict(r) for r in rows]


def _admit(db: Database, plan_id: str, round_id: str, round_no: int,
           topology: dict, items: list[dict]) -> tuple[list[dict], list[dict]]:
    """Returns (admitted, skipped).  Quota acquire + ledger in ONE transaction."""
    quotas = {d: v["isolation_quota"] for d, v in topology["fault_domains"].items()}
    admitted: list[dict] = []
    skipped: list[dict] = []

    def _do(conn):
        busy_services: set[str] = set()
        for it in items:
            inj_id = f"{plan_id}:{it['item_key']}"
            svc = it["service"]
            if svc in busy_services:
                skipped.append({**it, "reason": SKIP_DUPLICATE_SERVICE,
                                "detail": f"service {svc} already injected this round"})
                continue
            already = conn.execute(
                "SELECT id FROM domain_quota_ledger WHERE injection_id=? AND released_at IS NULL",
                (inj_id,)).fetchone()
            if already:  # multi-round item keeps its previously acquired slot
                admitted.append(it)
                busy_services.add(svc)
                conn.execute(
                    "UPDATE injections SET status='ADMITTED', round_id=?, skip_reason=NULL"
                    " WHERE injection_id=?", (round_id, inj_id))
                continue
            occupancy = _domain_occupancy(conn, it["fault_domain"])
            if len(occupancy) >= quotas[it["fault_domain"]]:
                skipped.append({**it, "reason": SKIP_QUOTA,
                                "error_code": E_FAULT_DOMAIN_FULL,
                                "detail": json.dumps({"domain": it["fault_domain"],
                                                      "quota": quotas[it["fault_domain"]],
                                                      "occupancy": occupancy},
                                                     ensure_ascii=False)})
                conn.execute(
                    "UPDATE injections SET status='SKIPPED', round_id=?, skip_reason=?,"
                    " detail=? WHERE injection_id=?",
                    (round_id, SKIP_QUOTA,
                     f"domain {it['fault_domain']} full: {len(occupancy)}/{quotas[it['fault_domain']]}",
                     inj_id))
                continue
            conn.execute(
                "INSERT INTO domain_quota_ledger(round_id,fault_domain,service,injection_id,"
                "acquired_at) VALUES(?,?,?,?,?)",
                (round_id, it["fault_domain"], svc, inj_id, time.time()))
            conn.execute(
                "UPDATE injections SET status='ADMITTED', round_id=?, skip_reason=NULL"
                " WHERE injection_id=?", (round_id, inj_id))
            admitted.append(it)
            busy_services.add(svc)

    db.write(_do)
    return admitted, skipped


# ---------------------------------------------------------------------------
# parallel execution with concurrency cap + bounded queue + time budget
# ---------------------------------------------------------------------------
def _execute_round(items: list[dict], concurrency: int, queue_capacity: int,
                   time_budget_ms: int) -> tuple[list[dict], list[dict], int]:
    """Returns (results, skipped, time_used_ms)."""
    capacity = concurrency + queue_capacity
    runnable, skipped = [], []
    for it in items:
        if len(runnable) < capacity:
            runnable.append(it)
        else:
            skipped.append({**it, "reason": SKIP_QUEUE_FULL,
                            "error_code": E_INJECTION_QUEUE_FULL,
                            "detail": f"execution capacity {capacity} exhausted"})

    deadline = time.monotonic() + time_budget_ms / 1000.0
    work: queue.Queue = queue.Queue()
    for it in runnable:
        work.put(it)
    results: list[dict] = []
    lock = threading.Lock()

    def worker():
        while True:
            try:
                it = work.get_nowait()
            except queue.Empty:
                return
            if time.monotonic() >= deadline:
                with lock:
                    skipped.append({**it, "reason": SKIP_TIME_BUDGET,
                                    "detail": "round time budget exhausted before start"})
                continue
            res = execute_injection(it)
            with lock:
                results.append({"item": it, "result": res})

    started = time.monotonic()
    threads = [threading.Thread(target=worker, daemon=True) for _ in range(concurrency)]
    for t in threads:
        t.start()
    # never let the round run past its time budget
    remaining = max(deadline - time.monotonic(), 0.0)
    for t in threads:
        t.join(timeout=remaining)
        remaining = max(deadline - time.monotonic(), 0.0)
    done_keys = {r["item"]["item_key"] for r in results}
    for it in runnable:
        if it["item_key"] not in done_keys and all(s.get("item_key") != it["item_key"]
                                                   for s in skipped):
            skipped.append({**it, "reason": SKIP_TIME_BUDGET,
                            "error_code": E_INJECTION_TIMEOUT,
                            "detail": "did not finish within round time budget"})
    time_used_ms = int((min(time.monotonic(), deadline) - started) * 1000)
    return results, skipped, time_used_ms


# ---------------------------------------------------------------------------
# adjudication (single write transaction)
# ---------------------------------------------------------------------------
def _adjudicate(db: Database, plan_id: str, round_id: str, round_no: int,
                topology: dict, results: list[dict], skipped: list[dict],
                time_used_ms: int, intent_id: str) -> dict:
    budgets = topology["budgets"]
    failover_budget = budgets["failover_ops_per_round"]
    effects: dict[str, list[str]] = {}
    for r in results:
        res = r["result"]
        if res.effect != "none":
            effects.setdefault(r["item"]["service"], []).append(res.effect)

    # only hard faults (replica down / link timeout) disqualify a domain as a
    # failover target; latency jitter degrades but keeps the domain eligible
    injected_domains: set[str] = set()
    for r in results:
        if r["result"].effect in ("replica_unavailable", "link_timeout"):
            injected_domains.add(r["item"]["fault_domain"])

    failover_used = 0
    states: dict[str, str] = {}
    failovers: list[dict] = []

    def _do(conn):
        nonlocal failover_used
        prev_rows = conn.execute(
            "SELECT service, state FROM service_states ss JOIN rounds r ON r.round_id=ss.round_id"
            " WHERE r.plan_id=? AND r.status='COMPLETED' AND r.round_no<?"
            " ORDER BY r.round_no DESC", (plan_id, round_no)).fetchall()
        prev = {}
        for row in prev_rows:  # first row per service = latest completed version
            prev.setdefault(row["service"], row["state"])

        for svc, spec in topology["services"].items():
            current = prev.get(svc, sm.HEALTHY)
            svc_effects = effects.get(svc, [])
            if any(e in ("replica_unavailable", "link_timeout") for e in svc_effects):
                strategy = spec["failover_strategy"]
                candidates = [d for d in spec["fault_domains"]
                              if d not in injected_domains]
                if current == sm.FAILED_OVER:
                    # failover already in place: keep it while a target domain
                    # remains usable; drop to FAILED when none does
                    if not candidates:
                        failovers.append({"service": svc, "status": "DENIED",
                                          "reason": "failover target domain lost",
                                          "error_code": E_FAILOVER_DENIED})
                        sm.check_transition(current, sm.FAILED)
                        current = sm.FAILED
                elif current == sm.FAILED:
                    pass  # stays FAILED until the fault lifts (restore path)
                else:
                    sm.check_transition(current, sm.ISOLATED)
                    current = sm.ISOLATED
                    if strategy != "none":
                        if not candidates:
                            failovers.append({"service": svc, "status": "DENIED",
                                              "reason": "no available replica domain",
                                              "error_code": E_FAILOVER_DENIED})
                            sm.check_transition(current, sm.FAILED)
                            current = sm.FAILED
                        elif failover_used >= failover_budget:
                            failovers.append({"service": svc, "status": "DENIED",
                                              "reason": "failover op quota exhausted",
                                              "error_code": E_FAILOVER_DENIED})
                            sm.check_transition(current, sm.FAILED)
                            current = sm.FAILED
                        else:
                            to_domain = (candidates[0] if strategy == "cross_domain"
                                         else candidates[-1])
                            failover_used += 1
                            failovers.append({"service": svc, "status": "SUCCESS",
                                              "from_domain": spec["fault_domains"][0],
                                              "to_domain": to_domain, "reason": ""})
                            sm.check_transition(current, sm.FAILED_OVER)
                            current = sm.FAILED_OVER
                    # strategy none: stays ISOLATED
            elif "latency_spike" in svc_effects:
                sm.check_transition(current, sm.DEGRADED)
                current = sm.DEGRADED
            else:
                if current != sm.HEALTHY:
                    sm.check_transition(current, sm.HEALTHY)
                    current = sm.HEALTHY
            states[svc] = current
            conn.execute(
                "INSERT OR REPLACE INTO service_states(round_id,service,state,version)"
                " VALUES(?,?,?,?)", (round_id, svc, current, round_no))

        for fo in failovers:
            conn.execute(
                "INSERT INTO failover_ops(op_id,round_id,service,from_domain,to_domain,"
                "status,deny_reason,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (uuid.uuid4().hex, round_id, fo["service"], fo.get("from_domain"),
                 fo.get("to_domain"), fo["status"], fo.get("reason") or None, time.time()))

        # execution outcomes persisted
        for r in results:
            res, it = r["result"], r["item"]
            inj_id = f"{plan_id}:{it['item_key']}"
            status = "DONE" if res.ok else "TIMEOUT"
            conn.execute(
                "UPDATE injections SET status=?, detail=?, finished_at=? WHERE injection_id=?",
                (status, res.detail, time.time(), inj_id))
        for s in skipped:
            inj_id = f"{plan_id}:{s['item_key']}"
            conn.execute(
                "UPDATE injections SET status='SKIPPED', round_id=?, skip_reason=?, detail=?"
                " WHERE injection_id=?",
                (round_id, s["reason"], s.get("detail", ""), inj_id))

        # quota release for injections whose last active round is this one;
        # release + injection completion commit atomically in this transaction
        release_ids = active_items_ending(plan_id, round_no, conn)
        conn.executemany(
            "UPDATE domain_quota_ledger SET released_at=? WHERE injection_id=?"
            " AND released_at IS NULL",
            [(time.time(), rid) for rid in release_ids])

        conn.execute(
            "UPDATE rounds SET status='COMPLETED', finished_at=?, time_used_ms=?"
            " WHERE round_id=?", (time.time(), time_used_ms, round_id))
        conn.execute(
            "UPDATE intents SET status='committed', resolved_at=? WHERE intent_id=?",
            (time.time(), intent_id))

    db.write(_do)
    return {"states": states, "failovers": failovers, "failover_used": failover_used}


def active_items_ending(plan_id: str, round_no: int, conn) -> list[str]:
    payload = conn.execute("SELECT payload FROM plans WHERE plan_id=?", (plan_id,)).fetchone()
    plan = json.loads(payload["payload"])
    ending = []
    for it in plan["items"]:
        if it["start_round"] + it["duration_rounds"] - 1 == round_no:
            ending.append(f"{plan_id}:{it['item_key']}")
    return ending


# ---------------------------------------------------------------------------
# public entry point
# ---------------------------------------------------------------------------
def run_round(db: Database, plan_id: str, round_no: int) -> dict:
    row = db.query_one("SELECT p.payload, p.topology_id FROM plans p WHERE p.plan_id=?",
                       (plan_id,))
    if not row:
        from .errors import E_NOT_FOUND
        raise BizError(E_NOT_FOUND, "plan not found", {"plan_id": plan_id})
    plan = json.loads(row["payload"])
    from .topology import get_topology
    topology = get_topology(db, row["topology_id"])
    if round_no < 1 or round_no > plan["rounds"]:
        raise BizError(E_ROUND_ABORTED, "round out of range",
                       {"round_no": round_no, "rounds": plan["rounds"]})
    existing = db.query_one("SELECT status FROM rounds WHERE plan_id=? AND round_no=?",
                            (plan_id, round_no))
    if existing and existing["status"] == "COMPLETED":
        return get_round_report(db, plan_id, round_no)

    budgets = topology["budgets"]
    round_id = f"{plan_id}:r{round_no}"
    intent_id = db.write_intent("ROUND_RUN", {"plan_id": plan_id, "round_no": round_no,
                                              "round_id": round_id})

    def _open(conn):
        conn.execute(
            "INSERT OR REPLACE INTO rounds(round_id,plan_id,round_no,status,started_at,"
            "time_budget_ms) VALUES(?,?,?,?,?,?)",
            (round_id, plan_id, round_no, "RUNNING", time.time(),
             budgets["round_time_budget_ms"]))
    db.write(_open)

    items = deterministic_order(active_items(plan, round_no))
    admitted, skipped = _admit(db, plan_id, round_id, round_no, topology, items)
    results, exec_skipped, time_used_ms = _execute_round(
        admitted, budgets["injection_concurrency"], budgets["injection_queue_capacity"],
        budgets["round_time_budget_ms"])
    all_skipped = skipped + exec_skipped
    adj = _adjudicate(db, plan_id, round_id, round_no, topology, results,
                      all_skipped, time_used_ms, intent_id)
    return get_round_report(db, plan_id, round_no)


def get_round_report(db: Database, plan_id: str, round_no: int) -> dict:
    rnd = db.query_one("SELECT * FROM rounds WHERE plan_id=? AND round_no=?",
                       (plan_id, round_no))
    if not rnd:
        from .errors import E_NOT_FOUND
        raise BizError(E_NOT_FOUND, "round not found",
                       {"plan_id": plan_id, "round_no": round_no})
    states = {r["service"]: r["state"] for r in db.query(
        "SELECT service, state FROM service_states WHERE round_id=? ORDER BY service",
        (rnd["round_id"],))}
    injections = [dict(r) for r in db.query(
        "SELECT item_key, service, fault_domain, fault_type, priority, status,"
        " skip_reason, detail FROM injections WHERE round_id=? ORDER BY item_key",
        (rnd["round_id"],))]
    failovers = [dict(r) for r in db.query(
        "SELECT service, from_domain, to_domain, status, deny_reason FROM failover_ops"
        " WHERE round_id=? ORDER BY created_at", (rnd["round_id"],))]
    return {
        "round_id": rnd["round_id"], "plan_id": plan_id, "round_no": round_no,
        "status": rnd["status"], "time_budget_ms": rnd["time_budget_ms"],
        "time_used_ms": rnd["time_used_ms"],
        "time_remaining_ms": max(rnd["time_budget_ms"] - rnd["time_used_ms"], 0),
        "service_states": states, "injections": injections, "failovers": failovers,
    }
