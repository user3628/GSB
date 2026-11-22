from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from conftest import load_sample
from graph_bench.api import create_app


@pytest.fixture()
def client(tmp_path: Path) -> TestClient:
    app = create_app(tmp_path / "api.db", run_recovery=False)
    with TestClient(app) as test_client:
        yield test_client


def test_full_http_flow(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["current_version"] is None

    submit = client.post(
        "/snapshots",
        json={"request_id": "s1", "payload": load_sample("snapshot_v1.json")},
    )
    assert submit.status_code == 200
    assert submit.json()["snapshot_version"] == 1

    layers = client.get("/views/layers")
    assert layers.status_code == 200
    assert layers.json()["layers"][0]["resources"] == ["gateway"]

    impact = client.get("/resources/gateway/impact")
    assert impact.status_code == 200
    assert impact.json()["completed"] is True
    assert "shipment" in impact.json()["downstream"]

    batches = client.get("/views/batches", params={"budget_ms": 1000})
    assert batches.status_code == 200
    assert batches.json()["completed"] is True
    assert len(batches.json()["batches"]) == 4

    partial = client.get(
        "/resources/gateway/impact",
        params={"budget_ms": 1, "delay_per_item_ms": 5},
    )
    assert partial.status_code == 200
    body = partial.json()
    assert body["snapshot_version"] == 1
    if not body["completed"]:
        assert body["remaining"]


def test_http_error_codes(client: TestClient) -> None:
    client.post("/snapshots", json={"request_id": "s1", "payload": load_sample("snapshot_v1.json")})

    bad_snapshot = client.post(
        "/snapshots",
        json={"request_id": "s-bad", "payload": load_sample("snapshot_bad_checksum_v2.json")},
    )
    assert bad_snapshot.status_code == 409
    assert bad_snapshot.json()["error"]["code"] == "E_SNAPSHOT_INVALID"

    bad_increment = client.post(
        "/increments",
        json={"request_id": "i-bad", "payload": load_sample("increment_bad_weight.json")},
    )
    assert bad_increment.status_code == 422
    assert bad_increment.json()["error"]["code"] == "E_SPEC_INVALID"

    client.post(
        "/increments",
        json={"request_id": "i1", "payload": load_sample("increment_weight_auth.json")},
    )
    stale = client.post(
        "/increments",
        json={"request_id": "i2", "payload": load_sample("increment_stale_base.json")},
    )
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "E_STALE_BASE"


def test_http_cycle_contract_and_idempotency(client: TestClient) -> None:
    client.post("/snapshots", json={"request_id": "s1", "payload": load_sample("snapshot_v1.json")})
    cycle = client.post(
        "/snapshots",
        json={"request_id": "cyc", "payload": load_sample("snapshot_cycle_v9.json")},
    )
    assert cycle.status_code == 200
    assert cycle.json()["is_acyclic"] is False

    layers = client.get("/views/layers")
    assert layers.status_code == 200
    error = layers.json()["error"]
    assert error["code"] == "E_DEPENDENCY_CYCLE"
    partial = error["details"]["partial_result"]
    assert partial["layers"] == [{"layer": 0, "resources": ["standalone-report"]}]
    assert "gateway" in error["details"]["blocked_resources"]

    replay_a = client.post(
        "/snapshots", json={"request_id": "s1", "payload": load_sample("snapshot_v1.json")}
    )
    replay_b = client.post(
        "/snapshots", json={"request_id": "s1", "payload": load_sample("snapshot_v1.json")}
    )
    assert replay_a.json()["idempotent_replay"] is True
    assert replay_a.json() == replay_b.json()

    intents = client.get("/intents")
    assert intents.status_code == 200
    assert {item["intent_id"] for item in intents.json()["intents"]} >= {"s1", "cyc"}
