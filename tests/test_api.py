from __future__ import annotations

from fastapi.testclient import TestClient

from obs_sampler.api import create_app
from obs_sampler.service import SamplerService

from .conftest import make_log, policy_document


def _client(tmp_path) -> TestClient:
    service = SamplerService(tmp_path / "api.sqlite3")
    return TestClient(create_app(service=service))


def _assert_error(response, status: int, code: str):
    assert response.status_code == status, response.text
    payload = response.json()
    assert payload["code"] == code
    assert isinstance(payload["message"], str)
    assert isinstance(payload["details"], dict)


def test_http_policy_batch_rate_and_bias_flow(tmp_path):
    client = _client(tmp_path)
    policy = policy_document(
        rates={"checkout": {"INFO": 1.0}},
        min_samples=1,
    )
    response = client.post("/api/v1/policies", json=policy)
    assert response.status_code == 200
    assert response.json()["status"] == "COMMITTED"

    current = client.get("/api/v1/policies/current")
    assert current.status_code == 200
    assert current.json()["policy_version"] == 1

    rate = client.get(
        "/api/v1/policies/1/sampling-rate",
        params={"service": "checkout", "level": "info"},
    )
    assert rate.status_code == 200
    assert rate.json()["head_rate"] == 1.0
    assert rate.json()["level"] == "INFO"

    batch = {
        "batch_id": "api-batch",
        "logs": [make_log(index, trace_id=f"api-{index}") for index in range(3)],
    }
    submitted = client.post("/api/v1/batches", json=batch)
    assert submitted.status_code == 200
    assert submitted.json()["record_count"] == 3

    bias = client.get(
        "/api/v1/policies/1/bias",
        params={"service": "checkout", "level": "INFO"},
    )
    assert bias.status_code == 200
    assert bias.json()["report"]["population_count"] == 3
    assert bias.json()["report"]["weighted_total"] == 3.0

    replay = client.post("/api/v1/batches", json=batch)
    assert replay.status_code == 200
    assert replay.json()["execution"] == "REPLAYED_COMMITTED_RESULT"

    recovered = client.get("/api/v1/recovery-report")
    assert recovered.status_code == 200
    assert recovered.json()["scope"] == "COMMITTED_SQLITE_FACTS_ONLY"


def test_http_unified_business_errors(tmp_path):
    client = _client(tmp_path)
    policy = policy_document(rates={"checkout": {"INFO": 0.0}}, min_samples=2)
    assert client.post("/api/v1/policies", json=policy).status_code == 200

    _assert_error(client.post("/api/v1/policies", json=policy), 409, "E_STALE_SNAPSHOT")

    invalid_schema = client.post(
        "/api/v1/batches",
        json={"batch_id": "invalid-schema", "logs": [{"timestamp_ms": "not-an-int"}]},
    )
    _assert_error(invalid_schema, 422, "E_RECORD_INVALID")

    whitespace_batch_id = client.post(
        "/api/v1/batches",
        json={"batch_id": "has whitespace", "logs": [make_log(0)]},
    )
    _assert_error(whitespace_batch_id, 422, "E_RECORD_INVALID")

    non_monotonic = client.post(
        "/api/v1/batches",
        json={
            "batch_id": "non-monotonic",
            "logs": [
                make_log(0, timestamp_ms=2000),
                make_log(1, timestamp_ms=1000),
            ],
        },
    )
    _assert_error(non_monotonic, 422, "E_RECORD_INVALID")
    assert non_monotonic.json()["details"]["reason"] == "TIMESTAMP_NOT_MONOTONIC"

    first_batch = client.post(
        "/api/v1/batches",
        json={"batch_id": "conflict", "logs": [make_log(0, trace_id="c-0")]},
    )
    assert first_batch.status_code == 200
    conflicting_batch = client.post(
        "/api/v1/batches",
        json={
            "batch_id": "conflict",
            "logs": [make_log(0, trace_id="c-0", sample_weight=4.0)],
        },
    )
    _assert_error(conflicting_batch, 409, "E_CONFLICTING_RECORD")

    insufficient = client.get(
        "/api/v1/policies/1/bias",
        params={"service": "checkout", "level": "INFO"},
    )
    _assert_error(insufficient, 422, "E_INSUFFICIENT_SAMPLES")
    assert insufficient.json()["details"]["reason"] == "UNSAMPLED_STRATUM"


def test_http_unknown_policy_is_unified_404(tmp_path):
    client = _client(tmp_path)
    _assert_error(client.get("/api/v1/policies/99"), 404, "E_POLICY_NOT_FOUND")
    _assert_error(
        client.get(
            "/api/v1/policies/99/sampling-rate",
            params={"service": "checkout", "level": "INFO"},
        ),
        404,
        "E_POLICY_NOT_FOUND",
    )
