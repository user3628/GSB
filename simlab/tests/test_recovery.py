import json
import time

from app.recovery import recover
from app.topology import load_plan, load_topology

from conftest import TOPOLOGY, make_plan


def _simulate_crash(db, plan_id, round_no=1):
    """Persist the exact footprint a hard-killed round leaves behind."""
    round_id = f"{plan_id}:r{round_no}"
    intent_id = db.write_intent("ROUND_RUN", {"plan_id": plan_id, "round_no": round_no,
                                              "round_id": round_id})
    def _do(conn):
        conn.execute(
            "INSERT INTO rounds(round_id,plan_id,round_no,status,started_at,time_budget_ms)"
            " VALUES(?,?,?,?,?,?)", (round_id, plan_id, round_no, "RUNNING",
                                     time.time(), 1000))
        conn.execute(
            "INSERT INTO domain_quota_ledger(round_id,fault_domain,service,injection_id,"
            "acquired_at) VALUES(?,?,?,?,?)",
            (round_id, "fd-a", "svc-a", f"{plan_id}:i1", time.time()))
        conn.execute(
            "INSERT INTO service_states(round_id,service,state,version) VALUES(?,?,?,?)",
            (round_id, "svc-a", "ISOLATED", round_no))
        conn.execute(
            "INSERT INTO failover_ops(op_id,round_id,service,from_domain,to_domain,status,"
            "created_at) VALUES(?,?,?,?,?,?,?)",
            ("op-crash", round_id, "svc-a", "fd-a", "fd-b", "SUCCESS", time.time()))
    db.write(_do)
    return intent_id


def test_recovery_aborts_and_rolls_back(db, dead_port):
    topo = load_topology(db, TOPOLOGY)
    plan = load_plan(db, topo["topology_id"], make_plan([
        {"id": "i1", "target": "svc-a", "fault_type": "node_down",
         "endpoint": f"http://127.0.0.1:{dead_port}/x"}]))
    _simulate_crash(db, plan["plan_id"])

    report = recover(db)

    assert report["scanned_pending_intents"] == 1
    assert report["aborted_rounds"][0]["round_id"] == f"{plan['plan_id']}:r1"
    assert report["quota_rollbacks"][0]["fault_domain"] == "fd-a"
    assert report["failover_rollbacks"][0]["service"] == "svc-a"
    assert report["state_diffs"][0]["aborted_state"] == "ISOLATED"
    assert report["state_diffs"][0]["restored_state"] == "HEALTHY"
    assert "not guaranteed" in report["note"]

    # persisted facts after recovery
    rnd = db.query_one("SELECT status FROM rounds")
    assert rnd["status"] == "ABORTED"
    assert db.query("SELECT * FROM domain_quota_ledger WHERE released_at IS NULL") == []
    assert db.query("SELECT * FROM failover_ops") == []
    assert db.query("SELECT * FROM service_states") == []
    assert db.query_one("SELECT status FROM intents")["status"] == "aborted"


def test_recovery_is_idempotent(db, dead_port):
    topo = load_topology(db, TOPOLOGY)
    plan = load_plan(db, topo["topology_id"], make_plan([
        {"id": "i1", "target": "svc-a", "fault_type": "node_down",
         "endpoint": f"http://127.0.0.1:{dead_port}/x"}]))
    _simulate_crash(db, plan["plan_id"])
    recover(db)
    second = recover(db)
    assert second["scanned_pending_intents"] == 0
    assert second["aborted_rounds"] == []
