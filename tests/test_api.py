from __future__ import annotations

from fastapi.testclient import TestClient

from eventbus.api import create_app


def test_http_error_envelope_and_happy_path(tmp_path):
    app = create_app(tmp_path / "api.db", test_hooks=False)
    with TestClient(app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["ok"] is True

        created = client.post(
            "/api/v1/topics",
            json={"topic": "http-topic", "partitions": 2, "group_id": "http-group"},
        )
        assert created.status_code == 200
        assert created.json()["topic"] == "http-topic"

        invalid = client.post(
            "/api/v1/topics/http-topic/events",
            json={"batch_id": "bad", "events": [{"event_id": "x"}]},
        )
        assert invalid.status_code == 400
        assert invalid.json()["error"]["code"] == "E_EVENT_INVALID"
        assert invalid.json()["error"]["context"]

        published = client.post(
            "/api/v1/topics/http-topic/events",
            json={
                "batch_id": "good",
                "events": [
                    {
                        "event_id": "http-1",
                        "partition_key": "a",
                        "event_time": "2026-03-01T00:00:00Z",
                        "key": "group-a",
                        "measure": 2.5,
                    }
                ],
            },
        )
        assert published.status_code == 200
        partition = published.json()["events"][0]["partition"]
        polled = client.post(
            "/api/v1/consumers/poll",
            json={
                "topic": "http-topic",
                "group_id": "http-group",
                "partition": partition,
                "consumer_id": "c1",
                "lease_seconds": 30,
            },
        ).json()
        assert polled["event"]["state"] == "DISPATCHED"

        held = client.post(
            "/api/v1/consumers/poll",
            json={
                "topic": "http-topic",
                "group_id": "http-group",
                "partition": partition,
                "consumer_id": "c2",
            },
        )
        assert held.status_code == 409
        assert held.json()["error"]["code"] == "E_LEASE_HELD"

        acked = client.post(
            "/api/v1/consumers/ack",
            json={
                "topic": "http-topic",
                "group_id": "http-group",
                "partition": partition,
                "consumer_id": "c1",
                "event_id": "http-1",
                "lease_token": polled["lease"]["lease_token"],
            },
        )
        assert acked.status_code == 200
        assert acked.json()["idempotent"] is False
        assert client.get(
            "/api/v1/aggregates", params={"topic": "http-topic", "group_id": "http-group"}
        ).json()["groups"][0]["sum"] == 2.5
        assert client.get(
            "/api/v1/verify", params={"topic": "http-topic"}
        ).json()["ok"] is True

        bad_route = client.get("/api/v1/does-not-exist")
        assert bad_route.status_code == 404
        assert bad_route.json()["error"]["code"] == "E_ROUTE_NOT_FOUND"

        hook_disabled = client.post(
            "/api/v1/consumers/poll",
            json={
                "topic": "http-topic",
                "group_id": "http-group",
                "partition": partition,
                "consumer_id": "c3",
                "test_crash_after_intent": True,
            },
        )
        assert hook_disabled.status_code == 403
        assert hook_disabled.json()["error"]["code"] == "E_TEST_HOOK_DISABLED"


def test_request_validation_uses_json_business_error(tmp_path):
    app = create_app(tmp_path / "validation.db")
    with TestClient(app) as client:
        response = client.post("/api/v1/topics", json={"partitions": 1})
        assert response.status_code == 400
        assert response.headers["content-type"].startswith("application/json")
        assert response.json()["error"]["code"] == "E_REQUEST_INVALID"

        malformed = client.post(
            "/api/v1/topics",
            content='{"topic": "broken"',
            headers={"content-type": "application/json"},
        )
        assert malformed.status_code == 400
        assert malformed.headers["content-type"].startswith("application/json")
        assert malformed.json()["error"]["code"] == "E_REQUEST_INVALID"
