from __future__ import annotations

from fastapi.testclient import TestClient

from gateway_middleware.api import create_app
from gateway_middleware.errors import (
    E_AUTH_DENIED,
    E_BATCH_NOT_FOUND,
    E_CONFLICTING_REQUEST,
    E_INVALID_INPUT,
    E_INVALID_TRANSITION,
    E_RATE_LIMITED,
    E_REQUEST_NOT_FOUND,
    E_SNAPSHOT_INVALID,
    E_TOKEN_EXPIRED,
)


def _error_code(response):
    body = response.json()
    assert body["ok"] is False
    assert set(body["error"]) >= {"code", "message", "details"}
    return body["error"]["code"]


def test_http_api_happy_path_and_decision_error_codes(small_service):
    service, dataset = small_service
    app = create_app(service=service, auto_recover=True)
    with TestClient(app) as client:
        response = client.post("/v1/batches", json=dataset.batch("api-batch").model_dump())
        assert response.status_code == 200
        body = response.json()
        assert body["ok"] is True
        assert body["status"] == "COMMITTED"
        assert body["commit_version"] == body["base_version"] + 1
        codes = {decision["decision_code"] for decision in body["decisions"]}
        assert {E_AUTH_DENIED, E_TOKEN_EXPIRED, E_RATE_LIMITED, "ALLOW"} <= codes

        request_id = body["decisions"][0]["request_id"]
        request_response = client.get(f"/v1/requests/{request_id}")
        assert request_response.status_code == 200
        decision = request_response.json()["decision"]
        versions = decision["versions"]
        assert versions["read_version"] == versions["window_version"] == versions["token_version"]

        rate_limited = next(
            decision for decision in body["decisions"] if decision["decision_code"] == E_RATE_LIMITED
        )
        assert rate_limited["auth"]["code"] == "AUTH_OK"
        assert rate_limited["error"]["code"] == E_RATE_LIMITED
        assert rate_limited["windows"]["short"]["estimated_weight"] > rate_limited["quotas"]["short"]
        auth_denied = next(
            decision for decision in body["decisions"] if decision["decision_code"] == E_AUTH_DENIED
        )
        expired = next(
            decision for decision in body["decisions"] if decision["decision_code"] == E_TOKEN_EXPIRED
        )
        assert auth_denied["error"]["code"] == E_AUTH_DENIED
        assert expired["error"]["code"] == E_TOKEN_EXPIRED

        windows = client.get(
            "/v1/windows",
            params={
                "route_key": rate_limited["route_key"],
                "client_id": rate_limited["client_id"],
                "at_ms": rate_limited["timestamp_ms"],
            },
        )
        assert windows.status_code == 200
        assert windows.json()["window_boundary"].startswith("left_open_right_closed")

        sampling = client.get(
            "/v1/sampling/errors",
            params={"route_key": rate_limited["route_key"], "client_id": rate_limited["client_id"]},
        )
        assert sampling.status_code == 200
        assert sampling.json()["confidence_level"] == 0.99

        recovery = client.get("/v1/recovery-report")
        assert recovery.status_code == 200
        assert recovery.json()["report"]["facts_only"] is True

        replay = client.post("/v1/batches", json=dataset.batch("api-batch").model_dump())
        assert replay.status_code == 200
        assert replay.json()["replayed"] is True
        replay_get = client.get("/v1/batches/api-batch")
        assert replay_get.status_code == 200
        assert replay_get.json()["commit_version"] == body["commit_version"]


def test_http_api_failure_envelopes_cover_required_codes(small_service):
    service, dataset = small_service
    app = create_app(service=service, auto_recover=False)
    with TestClient(app, raise_server_exceptions=False) as client:
        first = client.post("/v1/batches", json=dataset.batch("api-batch").model_dump())
        assert first.status_code == 200

        changed = dataset.batch("api-batch")
        changed.events[0].timestamp_ms += 1
        conflict = client.post("/v1/batches", json=changed.model_dump())
        assert conflict.status_code == 409
        assert _error_code(conflict) == E_CONFLICTING_REQUEST

        malformed = client.post("/v1/batches", json={"batch_id": "bad", "events": [{"event_seq": "x"}]})
        assert malformed.status_code == 422
        assert _error_code(malformed) == E_INVALID_INPUT

        missing_batch = client.get("/v1/batches/does-not-exist")
        assert missing_batch.status_code == 404
        assert _error_code(missing_batch) == E_BATCH_NOT_FOUND

        missing_request = client.get("/v1/requests/does-not-exist")
        assert missing_request.status_code == 404
        assert _error_code(missing_request) == E_REQUEST_NOT_FOUND

        bad_snapshot = client.get(
            "/v1/windows",
            params={"route_key": "/api/orders", "client_id": "c-free-1", "version": 999_999},
        )
        assert bad_snapshot.status_code == 409
        assert _error_code(bad_snapshot) == E_SNAPSHOT_INVALID

        base_version = service.snapshot_info()["version"]
        service.store.create_intent(
            batch_id="pending-api-batch",
            fingerprint="pending-fingerprint",
            base_version=base_version,
            target_version=base_version + 1,
            event_count=1,
            payload={"test": True},
        )
        pending = client.get("/v1/batches/pending-api-batch")
        assert pending.status_code == 409
        assert _error_code(pending) == E_INVALID_TRANSITION
