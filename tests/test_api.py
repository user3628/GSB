from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from rca_pipeline.app import create_app


@pytest.fixture()
def client(tmp_path):
    app = create_app(str(tmp_path / "api.db"), run_recovery=False)
    with TestClient(app) as test_client:
        yield test_client


def test_full_api_flow(client, topo_v1, dataset):
    response = client.get("/api/v1/health")
    assert response.status_code == 200

    bad = client.post("/api/v1/samples/batches", json=dataset[0])
    assert bad.status_code == 422
    assert bad.json()["error"]["code"] == "E_TOPOLOGY_INVALID"

    topo = client.post("/api/v1/topology/changes", json=topo_v1)
    assert topo.status_code == 200
    assert topo.json()["data"]["topology_version"] == 1

    first = client.post("/api/v1/samples/batches", json=dataset[0])
    assert first.status_code == 200
    assert first.json()["data"]["duplicate_count"] == 1

    stale = client.post("/api/v1/samples/batches", json={**dataset[1], "base_version": 0})
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "E_STALE_BASE"

    for batch in dataset[1:]:
        ok = client.post("/api/v1/samples/batches", json=batch)
        assert ok.status_code == 200

    alerts = client.get("/api/v1/alerts").json()["data"]["alerts"]
    assert alerts
    payment = next(a for a in alerts if a["service"] == "payment")

    rca = client.get(f"/api/v1/rca/{payment['id']}")
    assert rca.status_code == 200
    body = rca.json()["data"]
    assert body["numeric_contract"]["scale"] == 1_000_000
    assert body["candidates"]

    quantiles = client.get("/api/v1/aggregations/quantiles", params={"service": "payment"})
    assert quantiles.status_code == 200
    assert quantiles.json()["data"]["quantile_method"] == "nearest_rank"

    diagnostics = client.get("/api/v1/diagnostics")
    assert diagnostics.status_code == 200
    assert diagnostics.json()["data"]["counts"]["samples"] == 2352

    faults = client.post("/api/v1/faults/inject", json={"mode": "fail_rebuild"})
    assert faults.status_code == 200
    failing = client.post(
        "/api/v1/samples/batches",
        json={"commit_id": "after-fault", "base_version": 4,
              "records": [{"ts_ms": 9_999_999_000 + i, "service": "gateway",
                           "metric": "request_latency_us", "is_good": True,
                           "latency_us": 1000} for i in range(5)]},
    )
    assert failing.status_code == 500
    assert failing.json()["error"]["code"] == "E_REBUILD_FAILED"


def test_invalid_transition_endpoint(client, topo_v1, dataset):
    client.post("/api/v1/topology/changes", json=topo_v1)
    for batch in dataset:
        client.post("/api/v1/samples/batches", json=batch)
    alert = client.get("/api/v1/alerts").json()["data"]["alerts"][0]
    response = client.post(
        f"/api/v1/alerts/{alert['id']}/transitions", json={"to_status": "FIRING"}
    )
    if alert["status"] == "FIRING":
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "E_INVALID_TRANSITION"
    else:
        assert response.status_code == 200
