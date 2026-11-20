import os
import tempfile

_tmp = tempfile.mkdtemp(prefix="simlab-api-")
os.environ["SIMLAB_DB"] = os.path.join(_tmp, "api.db")

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402

from conftest import TOPOLOGY  # noqa: E402

client = TestClient(app)


def _plan(endpoint_base, dead_port):
    return {
        "name": "api-plan", "rounds": 2,
        "items": [
            {"id": "down", "target": "svc-a", "fault_type": "node_down",
             "priority": 100, "duration_rounds": 1,
             "endpoint": f"http://127.0.0.1:{dead_port}/x", "timeout_ms": 300},
            {"id": "slow", "target": "svc-b", "fault_type": "replica_jitter",
             "priority": 90, "duration_rounds": 1,
             "endpoint": f"{endpoint_base}/delay?ms=260", "timeout_ms": 300},
            {"id": "hang", "target": "svc-c", "fault_type": "link_timeout",
             "priority": 80, "duration_rounds": 1, "start_round": 2,
             "endpoint": f"{endpoint_base}/hang", "timeout_ms": 200},
        ],
    }


def test_full_api_flow(endpoint_server, dead_port):
    r = client.post("/api/topologies", json=TOPOLOGY)
    assert r.status_code == 200
    topo_id = r.json()["topology_id"]

    # idempotent topology re-submit
    r2 = client.post("/api/topologies", json=TOPOLOGY)
    assert r2.json()["reused"] is True
    assert r2.json()["topology_id"] == topo_id

    r = client.post(f"/api/topologies/{topo_id}/plans",
                    json=_plan(endpoint_server, dead_port))
    assert r.status_code == 200
    plan_id = r.json()["plan_id"]

    r = client.post(f"/api/plans/{plan_id}/run",
                    headers={"Idempotency-Key": "run-1"})
    assert r.status_code == 200
    body = r.json()
    assert len(body["rounds"]) == 2
    r1, r2 = body["rounds"]
    assert r1["service_states"]["svc-a"] == "FAILED_OVER"
    assert r1["service_states"]["svc-b"] == "DEGRADED"
    assert r2["service_states"]["svc-a"] == "HEALTHY"

    # idempotent replay returns the same persisted conclusion
    r = client.post(f"/api/plans/{plan_id}/run",
                    headers={"Idempotency-Key": "run-1"})
    assert r.json()["idempotent_replay"] is True
    assert r.json()["rounds"][0]["round_id"] == r1["round_id"]

    # round conclusion + budget details
    r = client.get(f"/api/plans/{plan_id}/rounds/1")
    assert r.status_code == 200
    assert r.json()["status"] == "COMPLETED"
    r = client.get(f"/api/plans/{plan_id}/rounds/1/budget")
    budget = r.json()
    assert budget["time_budget_ms"] == 1500
    assert budget["failover_ops_used"] == 1
    assert budget["failover_ops_remaining"] == 0
    assert "fd-a" in budget["fault_domain_quota"]

    # service history across rounds
    r = client.get(f"/api/plans/{plan_id}/services/svc-a/history")
    states = [h["state"] for h in r.json()["history"]]
    assert states == ["FAILED_OVER", "HEALTHY"]

    # recovery endpoint is always available
    r = client.post("/api/recovery/report")
    assert r.status_code == 200
    assert "aborted_rounds" in r.json()


def test_invalid_topology_returns_business_code():
    bad = dict(TOPOLOGY)
    bad["dependencies"] = [{"from": "svc-a", "to": "svc-b"},
                           {"from": "svc-b", "to": "svc-a"}]
    r = client.post("/api/topologies", json=bad)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "E_TOPOLOGY_INVALID"


def test_unknown_plan_404():
    r = client.get("/api/plans/plan-nope/rounds/1")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "E_NOT_FOUND"
