import json
import time

from app.engine import deterministic_order, run_round
from app.errors import (E_INJECTION_QUEUE_FULL, SKIP_DUPLICATE_SERVICE,
                        SKIP_QUOTA, SKIP_QUEUE_FULL, SKIP_TIME_BUDGET)
from app.topology import load_plan, load_topology

from conftest import TOPOLOGY, make_plan


def _item(key, target, fault="node_down", pri=0, dur=1, start=1,
          endpoint=None, timeout_ms=300):
    return {"id": key, "target": target, "fault_type": fault, "priority": pri,
            "duration_rounds": dur, "start_round": start,
            "endpoint": endpoint, "timeout_ms": timeout_ms}


def _setup(db, items, topo=None, rounds=1):
    t = load_topology(db, topo or TOPOLOGY)
    p = load_plan(db, t["topology_id"], make_plan(items, rounds=rounds))
    return t, p


def test_failover_success_and_states(db, dead_port):
    items = [_item("i1", "svc-a", pri=10, endpoint=f"http://127.0.0.1:{dead_port}/x")]
    _, p = _setup(db, items)
    report = run_round(db, p["plan_id"], 1)
    assert report["status"] == "COMPLETED"
    # svc-a isolated then failed over to fd-b
    assert report["service_states"]["svc-a"] == "FAILED_OVER"
    assert report["service_states"]["svc-b"] == "HEALTHY"
    fo = report["failovers"][0]
    assert fo["status"] == "SUCCESS" and fo["to_domain"] == "fd-b"


def test_domain_quota_full_skip_with_occupancy(db, dead_port):
    items = [
        _item("hi", "svc-a", pri=100, endpoint=f"http://127.0.0.1:{dead_port}/x"),
        _item("lo", "svc-d", pri=50, endpoint=f"http://127.0.0.1:{dead_port}/x"),
    ]
    _, p = _setup(db, items)  # both svc-a and svc-d live in fd-a (quota 1)
    report = run_round(db, p["plan_id"], 1)
    skipped = [i for i in report["injections"] if i["status"] == "SKIPPED"]
    assert len(skipped) == 1
    assert skipped[0]["item_key"] == "lo"
    assert skipped[0]["skip_reason"] == SKIP_QUOTA
    assert "fd-a" in skipped[0]["detail"]


def test_duplicate_service_same_round(db, dead_port):
    items = [
        _item("first", "svc-a", pri=100, endpoint=f"http://127.0.0.1:{dead_port}/x"),
        _item("second", "svc-a", pri=10, endpoint=f"http://127.0.0.1:{dead_port}/x"),
    ]
    _, p = _setup(db, items)
    report = run_round(db, p["plan_id"], 1)
    skipped = [i for i in report["injections"] if i["status"] == "SKIPPED"]
    assert [s["item_key"] for s in skipped] == ["second"]
    assert skipped[0]["skip_reason"] == SKIP_DUPLICATE_SERVICE


def test_queue_full_skip(db, dead_port):
    topo = json.loads(json.dumps(TOPOLOGY))
    topo["fault_domains"] = [{"name": "fd-a", "isolation_quota": 3},
                             {"name": "fd-b", "isolation_quota": 1},
                             {"name": "fd-c", "isolation_quota": 1}]
    topo["budgets"]["injection_queue_capacity"] = 0  # capacity == concurrency == 2
    items = [
        _item("k1", "svc-a", pri=100, endpoint=f"http://127.0.0.1:{dead_port}/x"),
        _item("k2", "svc-d", pri=90, endpoint=f"http://127.0.0.1:{dead_port}/x"),
        _item("k3", "svc-b", pri=80, endpoint=f"http://127.0.0.1:{dead_port}/x"),
    ]
    # svc-a/svc-d in fd-a, svc-b in fd-b -> 3 admitted, capacity 2 -> 1 queue-full
    _, p = _setup(db, items, topo=topo)
    report = run_round(db, p["plan_id"], 1)
    skipped = [i for i in report["injections"] if i["status"] == "SKIPPED"]
    assert len(skipped) == 1
    assert skipped[0]["item_key"] == "k3"
    assert skipped[0]["skip_reason"] == SKIP_QUEUE_FULL


def test_time_budget_skip_and_round_not_overrun(db, endpoint_server):
    topo = json.loads(json.dumps(TOPOLOGY))
    topo["budgets"]["injection_concurrency"] = 1
    topo["budgets"]["injection_queue_capacity"] = 1
    topo["budgets"]["round_time_budget_ms"] = 600
    items = [
        _item("hang1", "svc-a", fault="link_timeout", pri=100,
              endpoint=f"{endpoint_server}/hang", timeout_ms=500),
        _item("hang2", "svc-b", fault="link_timeout", pri=90,
              endpoint=f"{endpoint_server}/hang", timeout_ms=500),
    ]
    _, p = _setup(db, items, topo=topo)
    start = time.monotonic()
    report = run_round(db, p["plan_id"], 1)
    elapsed_ms = (time.monotonic() - start) * 1000
    assert elapsed_ms < 600 + 400  # round never overruns its budget meaningfully
    skipped = [i for i in report["injections"] if i["status"] == "SKIPPED"]
    assert any(s["skip_reason"] == SKIP_TIME_BUDGET for s in skipped)
    assert report["time_used_ms"] <= 600


def test_link_timeout_records_error_code(db, endpoint_server):
    items = [_item("t1", "svc-a", fault="link_timeout", pri=1,
                   endpoint=f"{endpoint_server}/hang", timeout_ms=200)]
    _, p = _setup(db, items)
    report = run_round(db, p["plan_id"], 1)
    inj = [i for i in report["injections"] if i["item_key"] == "t1"][0]
    assert inj["status"] == "TIMEOUT"
    assert "timed out" in inj["detail"]


def test_jitter_degrades_service(db, endpoint_server):
    items = [_item("j1", "svc-a", fault="replica_jitter", pri=1,
                   endpoint=f"{endpoint_server}/delay?ms=260", timeout_ms=300)]
    _, p = _setup(db, items)
    report = run_round(db, p["plan_id"], 1)
    assert report["service_states"]["svc-a"] == "DEGRADED"


def test_failover_quota_exhausted_denied(db, dead_port):
    topo = json.loads(json.dumps(TOPOLOGY))
    topo["services"] = [
        {"name": "svc-a", "replicas": 2, "fault_domains": ["fd-a", "fd-c"],
         "failover_strategy": "cross_domain"},
        {"name": "svc-b", "replicas": 2, "fault_domains": ["fd-b", "fd-c"],
         "failover_strategy": "cross_domain"},
        {"name": "svc-c", "replicas": 1, "fault_domains": ["fd-c"],
         "failover_strategy": "none"},
        {"name": "svc-d", "replicas": 1, "fault_domains": ["fd-a"],
         "failover_strategy": "none"},
    ]
    topo["budgets"]["failover_ops_per_round"] = 1
    items = [
        _item("a", "svc-a", pri=100, endpoint=f"http://127.0.0.1:{dead_port}/x"),
        _item("b", "svc-b", pri=90, endpoint=f"http://127.0.0.1:{dead_port}/x"),
    ]
    _, p = _setup(db, items, topo=topo)
    report = run_round(db, p["plan_id"], 1)
    fos = sorted(report["failovers"], key=lambda f: f["service"])
    assert fos[0]["status"] == "SUCCESS"     # svc-a takes the only failover slot
    assert fos[1]["status"] == "DENIED"
    assert "quota" in fos[1]["deny_reason"]
    assert report["service_states"]["svc-a"] == "FAILED_OVER"
    assert report["service_states"]["svc-b"] == "FAILED"


def test_multi_round_quota_hold_and_release(db, dead_port):
    items = [_item("long", "svc-a", pri=1, dur=2,
                   endpoint=f"http://127.0.0.1:{dead_port}/x")]
    _, p = _setup(db, items, rounds=2)
    run_round(db, p["plan_id"], 1)
    open_q = db.query("SELECT * FROM domain_quota_ledger WHERE released_at IS NULL")
    assert len(open_q) == 1  # quota held across rounds
    run_round(db, p["plan_id"], 2)
    open_q = db.query("SELECT * FROM domain_quota_ledger WHERE released_at IS NULL")
    assert len(open_q) == 0  # released atomically at the end of its last round
    closed = db.query("SELECT * FROM domain_quota_ledger WHERE released_at IS NOT NULL")
    assert len(closed) == 1


def test_service_recovers_after_injection_ends(db, dead_port):
    items = [_item("once", "svc-a", pri=1, dur=1,
                   endpoint=f"http://127.0.0.1:{dead_port}/x")]
    _, p = _setup(db, items, rounds=2)
    r1 = run_round(db, p["plan_id"], 1)
    assert r1["service_states"]["svc-a"] == "FAILED_OVER"
    r2 = run_round(db, p["plan_id"], 2)
    assert r2["service_states"]["svc-a"] == "HEALTHY"


def test_deterministic_order_priority_then_domain_round_robin():
    items = [
        {"item_key": "a1", "priority": 10, "fault_domain": "fd-a"},
        {"item_key": "a2", "priority": 10, "fault_domain": "fd-a"},
        {"item_key": "b1", "priority": 10, "fault_domain": "fd-b"},
        {"item_key": "c1", "priority": 20, "fault_domain": "fd-c"},
    ]
    order = [i["item_key"] for i in deterministic_order(items)]
    assert order[0] == "c1"                      # highest priority first
    assert set(order[1:]) == {"a1", "a2", "b1"}  # then domain round-robin
    assert order[1] == "a1" and order[2] == "b1" and order[3] == "a2"
