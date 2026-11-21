import pytest
from fastapi.testclient import TestClient

from fiw.app import create_app


@pytest.fixture()
def client(tmp_path):
    app = create_app(str(tmp_path / "fiw.db"))
    with TestClient(app) as test_client:
        yield test_client


def test_full_http_flow(env, client):
    resp = client.post("/plans/load", json={
        "endpoints_path": str(env.endpoints_path),
        "plan_path": str(env.plan_path)})
    assert resp.status_code == 200
    assert resp.json()["loaded"] is True

    resp = client.post("/plans/plan-test/transitions", json={"target": "RUNNING"})
    assert resp.status_code == 200

    resp = client.post("/plans/plan-test/rounds/1/run")
    assert resp.status_code == 200
    rnd = resp.json()
    outcomes = {t["item_id"]: t["outcome"] for t in rnd["trajectory"]}
    assert outcomes["it-2"] == "TIMEOUT"
    assert outcomes["it-4"] == "FAULT_UNSUPPORTED"

    resp = client.get("/plans/plan-test/rounds/1")
    assert resp.status_code == 200
    assert resp.json()["observations"]["ep-api-1"]["conclusion"] == "UNREACHABLE"

    resp = client.get("/plans/plan-test")
    assert resp.json()["status"] == "RUNNING"
    assert resp.json()["last_committed_round"] == 1


def test_illegal_transition_over_http(env, client):
    client.post("/plans/load", json={
        "endpoints_path": str(env.endpoints_path),
        "plan_path": str(env.plan_path)})
    resp = client.post("/plans/plan-test/transitions", json={"target": "COMPLETED"})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "E_INVALID_TRANSITION"


def test_invalid_plan_over_http(env, client):
    env.plan_path.write_text("{broken")
    resp = client.post("/plans/load", json={
        "endpoints_path": str(env.endpoints_path),
        "plan_path": str(env.plan_path)})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "E_PLAN_INVALID"


def test_unknown_plan_404(client):
    resp = client.get("/plans/nope")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "E_NOT_FOUND"


def test_recover_endpoint(client):
    resp = client.post("/recover")
    assert resp.status_code == 200
    assert resp.json()["scanned_pending_intents"] == 0
