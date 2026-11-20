"""Local HTTP API for the fault-isolation / failover simulation platform."""
from __future__ import annotations

import json
import os
import threading

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

from .db import Database
from .engine import get_round_report, run_round
from .errors import BizError, E_NOT_FOUND
from .recovery import recover
from .topology import get_topology, load_plan, load_topology

DB_PATH = os.environ.get("SIMLAB_DB", "simlab.db")
db = Database(DB_PATH)
_run_lock = threading.Lock()  # single-writer serialization for round runs

app = FastAPI(title="simlab", version="1.0")


@app.exception_handler(BizError)
async def biz_error_handler(_: Request, exc: BizError):
    status = 404 if exc.code == E_NOT_FOUND else 422
    return JSONResponse(status_code=status, content=exc.to_dict())


@app.post("/api/topologies")
def submit_topology(payload: dict):
    result = load_topology(db, payload)
    return {"topology_id": result["topology_id"], "name": result["name"],
            "reused": result["reused"]}


@app.post("/api/topologies/{topology_id}/plans")
def submit_plan(topology_id: str, payload: dict):
    result = load_plan(db, topology_id, payload)
    return {"plan_id": result["plan_id"], "reused": result["reused"],
            "rounds": result["plan"]["rounds"]}


@app.post("/api/plans/{plan_id}/run")
def run_plan(plan_id: str, idempotency_key: str | None = Header(default=None)):
    if idempotency_key:
        cached = db.idempotency_lookup(f"run:{plan_id}:{idempotency_key}")
        if cached:
            return {**cached, "idempotent_replay": True}
    row = db.query_one("SELECT payload FROM plans WHERE plan_id=?", (plan_id,))
    if not row:
        raise BizError(E_NOT_FOUND, "plan not found", {"plan_id": plan_id})
    plan = json.loads(row["payload"])
    with _run_lock:  # same-plan rounds advance serially; writes stay serialized
        rounds = [run_round(db, plan_id, n) for n in range(1, plan["rounds"] + 1)]
    response = {"plan_id": plan_id, "rounds": rounds}
    if idempotency_key:
        db.idempotency_store(f"run:{plan_id}:{idempotency_key}", response)
    return response


@app.get("/api/plans/{plan_id}/rounds/{round_no}")
def round_report(plan_id: str, round_no: int):
    return get_round_report(db, plan_id, round_no)


@app.get("/api/plans/{plan_id}/rounds/{round_no}/budget")
def round_budget(plan_id: str, round_no: int):
    report = get_round_report(db, plan_id, round_no)
    row = db.query_one("SELECT topology_id FROM plans WHERE plan_id=?", (plan_id,))
    topology = get_topology(db, row["topology_id"])
    budgets = topology["budgets"]
    failovers = report["failovers"]
    fo_used = sum(1 for f in failovers if f["status"] == "SUCCESS")
    quota = {}
    for d, spec in topology["fault_domains"].items():
        open_rows = db.query(
            "SELECT service, injection_id FROM domain_quota_ledger"
            " WHERE fault_domain=? AND released_at IS NULL", (d,))
        quota[d] = {"isolation_quota": spec["isolation_quota"],
                    "in_use": len(open_rows),
                    "remaining": spec["isolation_quota"] - len(open_rows),
                    "occupants": [dict(r) for r in open_rows]}
    ran = [i for i in report["injections"] if i["status"] in ("DONE", "TIMEOUT")]
    return {
        "round_id": report["round_id"], "status": report["status"],
        "injection_concurrency": budgets["injection_concurrency"],
        "injection_queue_capacity": budgets["injection_queue_capacity"],
        "injections_executed": len(ran),
        "failover_ops_per_round": budgets["failover_ops_per_round"],
        "failover_ops_used": fo_used,
        "failover_ops_remaining": budgets["failover_ops_per_round"] - fo_used,
        "time_budget_ms": report["time_budget_ms"],
        "time_used_ms": report["time_used_ms"],
        "time_remaining_ms": report["time_remaining_ms"],
        "fault_domain_quota": quota,
    }


@app.get("/api/plans/{plan_id}/skips")
def plan_skips(plan_id: str):
    rows = db.query(
        "SELECT item_key, service, fault_domain, fault_type, priority, skip_reason,"
        " detail, round_id FROM injections WHERE plan_id=? AND status='SKIPPED'"
        " ORDER BY round_id, item_key", (plan_id,))
    return {"plan_id": plan_id, "skipped": [dict(r) for r in rows]}


@app.get("/api/plans/{plan_id}/services/{service}/history")
def service_history(plan_id: str, service: str):
    rows = db.query(
        "SELECT r.round_no, ss.state, ss.version FROM service_states ss"
        " JOIN rounds r ON r.round_id=ss.round_id"
        " WHERE r.plan_id=? AND ss.service=? AND r.status='COMPLETED'"
        " ORDER BY r.round_no", (plan_id, service))
    return {"plan_id": plan_id, "service": service,
            "history": [dict(r) for r in rows]}


@app.post("/api/recovery/report")
def recovery_report():
    return recover(db)


@app.get("/api/health")
def health():
    return {"status": "ok"}
