from __future__ import annotations

from fastapi.testclient import TestClient

from leasefair.api import create_app
from leasefair.clock import FixedClock
from tests.conftest import ORIGIN_MS


def test_http_contract_and_business_statuses(tmp_path) -> None:
    app = create_app(tmp_path / "api.sqlite", clock=FixedClock(ORIGIN_MS))
    client = TestClient(app)
    assert client.get("/healthz").status_code == 200

    granted = client.post(
        "/v1/locks/a%2Fb/acquire",
        json={"request_id": "r1", "holder_id": "h1", "ttl_ms": 3000, "weight": 0.501},
    )
    assert granted.status_code == 200
    body = granted.json()
    assert body["code"] == "OK"
    assert body["lease"]["weight"]["q"] == 501_000_000

    held = client.post(
        "/v1/locks/a%2Fb/acquire",
        json={"request_id": "r2", "holder_id": "h2", "ttl_ms": 3000, "weight": 0.499},
    )
    assert held.status_code == 200
    assert held.json()["code"] == "E_LOCK_HELD"
    assert held.json()["remaining_ttl_ms"] == 3000

    conflict = client.post(
        "/v1/locks/a%2Fb/acquire",
        json={"request_id": "r1", "holder_id": "different", "ttl_ms": 3000, "weight": 0.501},
    )
    assert conflict.status_code == 409
    assert conflict.json()["code"] == "E_CONFLICTING_REQUEST"

    metrics = client.get(f"/v1/metrics?now_ms={ORIGIN_MS}")
    assert metrics.status_code == 200
    assert metrics.json()["cluster"]["occupancy"]["decimal"] == "1.000000000"
    diagnostics = client.get(f"/v1/diagnostics?now_ms={ORIGIN_MS}")
    assert diagnostics.status_code == 200
    assert diagnostics.json()["fixed_point"]["scale"] == 1_000_000_000
    assert diagnostics.json()["recovery"] is not None
    intents = client.get("/v1/intents?phase=committed")
    assert intents.status_code == 200
    assert intents.json()["intentions"][0]["operation"] == "acquire"
    samples = client.get("/v1/samples")
    assert samples.status_code == 200
    assert samples.json()["samples"]["name"] == "fixed-local-lease-samples-v1"


def test_http_validation_returns_stable_code(tmp_path) -> None:
    app = create_app(tmp_path / "api.sqlite", clock=FixedClock(ORIGIN_MS))
    client = TestClient(app)
    response = client.post(
        "/v1/locks/x/acquire",
        json={"request_id": "bad", "holder_id": "h", "ttl_ms": 0, "weight": "nan"},
    )
    assert response.status_code == 422
    assert response.json()["code"] == "E_INVALID_REQUEST"

