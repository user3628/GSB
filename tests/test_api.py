import json
import os

import pytest
from fastapi.testclient import TestClient

from xfpipe.server import create_app


@pytest.fixture()
def client(bench):
    return TestClient(create_app(bench.workdir))


def test_full_http_flow(client):
    assert client.get("/health").json()["status"] == "ok"
    run = client.post("/pipeline/run").json()
    assert run["status"] == "completed"

    status = client.get("/pipeline/status").json()
    assert all(s["present"] for s in status["stages"].values())

    q1 = client.post("/query/explained_variance").json()
    q2 = client.post("/query/explained_variance").json()
    assert q1["cache_hit"] is False and q2["cache_hit"] is True
    assert q1["result"] == q2["result"]

    stats = client.get("/cache/stats").json()
    assert stats["hits"] >= 1

    checkpoints = client.get("/checkpoints").json()
    assert checkpoints["snapshot"]["status"] == "normal"
    assert len(checkpoints["records"]) == 4

    verify = client.post("/verify").json()
    assert verify["status"] == "match"


def test_config_update_triggers_incremental(client):
    client.post("/pipeline/run")
    cfg = client.get("/config").json()
    cfg["reduction"]["candidates"][0]["target_dim"] = 3
    client.put("/config", json=cfg)
    run = client.put("/config", json=cfg)  # idempotent
    assert run.status_code == 200
    report = client.post("/pipeline/run").json()
    assert report["reused_stages"] == ["infer", "cross"]
    assert report["recomputed_stages"] == ["reduce", "summary"]


def test_invalid_config_returns_structured_error(client):
    cfg = client.get("/config").json()
    cfg["crossing"]["max_order"] = 0
    response = client.put("/config", json=cfg)
    assert response.status_code == 422
    body = response.json()
    assert body["error"]["code"] == "E_INVALID_CONFIG"
    assert "crossing.max_order" in body["error"]["details"]["problems"]


def test_stop_and_recover_over_http(client):
    client.post("/pipeline/stop")
    stopped = client.post("/pipeline/run").json()
    assert stopped["status"] == "stopped"
    client.post("/pipeline/stop/clear")
    recovered = client.post("/pipeline/recover").json()
    assert recovered["status"] == "completed"


def test_dim_invalid_over_http(client):
    cfg = client.get("/config").json()
    cfg["reduction"]["candidates"][0]["target_dim"] = 100000
    client.put("/config", json=cfg)
    response = client.post("/pipeline/run")
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "E_DIM_INVALID"
