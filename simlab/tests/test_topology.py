import pytest

from app.errors import BizError, E_PLAN_INVALID, E_TOPOLOGY_INVALID
from app.topology import load_plan, load_topology, normalize_topology

from conftest import TOPOLOGY, make_plan


def test_valid_topology_loads(db):
    res = load_topology(db, TOPOLOGY)
    assert res["reused"] is False
    assert res["topology_id"].startswith("topo-")


def test_identical_topology_is_idempotent(db):
    first = load_topology(db, TOPOLOGY)
    second = load_topology(db, TOPOLOGY)
    assert second["reused"] is True
    assert second["topology_id"] == first["topology_id"]
    rows = db.query("SELECT * FROM topologies")
    assert len(rows) == 1


def test_cycle_rejected_and_nothing_persisted(db):
    bad = dict(TOPOLOGY)
    bad["dependencies"] = [
        {"from": "svc-a", "to": "svc-b"},
        {"from": "svc-b", "to": "svc-a"},
    ]
    with pytest.raises(BizError) as exc:
        load_topology(db, bad)
    assert exc.value.code == E_TOPOLOGY_INVALID
    assert "cyclic_services" in exc.value.details
    assert db.query("SELECT * FROM topologies") == []


def test_unknown_domain_rejected(db):
    bad = dict(TOPOLOGY)
    bad["services"] = [{"name": "x", "replicas": 1, "fault_domains": ["fd-zzz"]}]
    with pytest.raises(BizError) as exc:
        normalize_topology(bad)
    assert exc.value.code == E_TOPOLOGY_INVALID


def test_plan_validation(db):
    topo = load_topology(db, TOPOLOGY)
    with pytest.raises(BizError) as exc:
        load_plan(db, topo["topology_id"], make_plan([
            {"id": "i1", "target": "ghost", "fault_type": "node_down",
             "endpoint": "http://127.0.0.1:9/x"}]))
    assert exc.value.code == E_PLAN_INVALID
    with pytest.raises(BizError) as exc:
        load_plan(db, topo["topology_id"], make_plan([
            {"id": "i1", "target": "svc-a", "fault_type": "node_down",
             "endpoint": "http://10.0.0.1:9/x"}]))
    assert exc.value.code == E_PLAN_INVALID  # non-loopback endpoint refused


def test_plan_idempotent(db, dead_port):
    topo = load_topology(db, TOPOLOGY)
    plan = make_plan([{"id": "i1", "target": "svc-a", "fault_type": "node_down",
                       "endpoint": f"http://127.0.0.1:{dead_port}/x"}])
    first = load_plan(db, topo["topology_id"], plan)
    second = load_plan(db, topo["topology_id"], plan)
    assert second["reused"] is True
    assert second["plan_id"] == first["plan_id"]
