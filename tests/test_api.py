from __future__ import annotations

from fastapi.testclient import TestClient

from app.api import create_app
from tests.conftest import spec


def test_http_api_envelope_and_invocation(engine_factory):
    engine = engine_factory()
    client = TestClient(create_app(engine=engine))
    loaded = client.post(
        "/v1/functions/load",
        json={"functions": [spec("api-fn", function_concurrency_limit=2)]},
    )
    assert loaded.status_code == 200
    assert loaded.json()["ok"] is True
    cold = client.post(
        "/v1/invocations",
        json={"call_id": "api-1", "function_id": "api-fn", "execution_us": 5_000},
    )
    assert cold.status_code == 200
    assert cold.json()["data"]["cold_start"] is True
    warm = client.post(
        "/v1/invocations",
        json={"call_id": "api-2", "function_id": "api-fn", "execution_us": 5_000},
    )
    assert warm.json()["data"]["warm_start"] is True
    metrics = client.get("/v1/metrics").json()["data"]
    assert metrics["runtime"][0]["cold_starts"] == 1
    assert metrics["runtime"][0]["warm_starts"] == 1
    instance_id = cold.json()["data"]["instance_id"]
    snapshot = client.get("/v1/snapshot", params={"function_id": "api-fn"}).json()["data"]
    assert snapshot["commit_seq"] >= 1
    assert any(item["instance_id"] == instance_id for item in snapshot["instances"])


def test_http_validation_error_uses_business_code(engine_factory):
    engine = engine_factory()
    client = TestClient(create_app(engine=engine))
    response = client.post(
        "/v1/functions/load",
        json={"functions": [spec("bad", per_instance_concurrency=0)]},
    )
    assert response.status_code == 400
    assert response.json()["business_code"] == "E_SPEC_INVALID"
    assert engine.list_functions() == []


def test_http_not_found_is_structured(engine_factory):
    client = TestClient(create_app(engine=engine_factory()))
    response = client.get("/v1/calls/missing")
    assert response.status_code == 404
    assert response.json()["business_code"] == "E_NOT_FOUND"

