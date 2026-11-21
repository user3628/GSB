from __future__ import annotations

from fastapi.testclient import TestClient


def test_unified_error_shape_and_request_id(seeded_client: TestClient):
    response = seeded_client.get("/api/batches/B-INSUFFICIENT/risk?mode=FULL")
    assert response.status_code == 422
    payload = response.json()
    assert payload["ok"] is False
    assert payload["error"]["code"] == "E_INSUFFICIENT_SAMPLES"
    assert payload["error"]["context"]["sample_count"] == 3
    assert payload["request_id"]
    assert response.headers["X-Request-ID"] == payload["request_id"]

    missing = seeded_client.get("/api/does-not-exist")
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "E_NOT_FOUND"
    assert missing.json()["error"]["context"]["path"] == "/api/does-not-exist"


def test_http_flow_release_rollback_compare_and_cooldown(seeded_client: TestClient):
    release = seeded_client.post(
        "/api/batches/B-PASS/decision",
        json={"mode": "FULL", "decision_time_ms": 1_763_900_070_000},
    )
    assert release.status_code == 200
    assert release.json()["data"]["batch"]["status"] == "PASSED"

    rollback = seeded_client.post(
        "/api/batches/B-ROLLBACK/decision",
        json={"mode": "FULL", "decision_time_ms": 1_763_910_070_000},
    )
    assert rollback.status_code == 200
    assert rollback.json()["data"]["batch"]["status"] == "ROLLED_BACK"

    cooldown = seeded_client.get(
        "/api/cooldowns/checkout-api?at_ms=1763910100000"
    )
    assert cooldown.status_code == 200
    assert cooldown.json()["data"]["active"] is True
    assert cooldown.json()["data"]["remaining_ms"] == 270_000

    retrigger = seeded_client.post(
        "/api/batches/B-ROLLBACK/decision",
        json={"mode": "FULL", "decision_time_ms": 1_763_910_100_000},
    )
    assert retrigger.status_code == 409
    assert retrigger.json()["error"]["code"] == "E_ROLLBACK_COOLDOWN"

    compare = seeded_client.get("/api/batches/B-COMPARE/compare")
    assert compare.status_code == 200
    data = compare.json()["data"]
    assert all(data["assertions"].values()) is True
    assert data["fast"]["decision"]["requires_full_review"] is True


def test_static_dashboard_and_openapi_are_served(seeded_client: TestClient):
    root = seeded_client.get("/")
    assert root.status_code == 200
    assert "ChangeGuard" in root.text
    assert seeded_client.get("/static/app.js").status_code == 200
    assert seeded_client.get("/static/app.css").status_code == 200
    assert seeded_client.get("/api/health").json()["data"]["status"] == "ok"


def test_seed_endpoint_is_idempotent(client: TestClient):
    first = client.post("/api/demo/seed")
    assert first.status_code == 201
    second = client.post("/api/demo/seed")
    assert second.status_code == 201
    actions = second.json()["data"]["actions"]
    assert any(item["result"] == "SKIPPED_STALE" for item in actions)
    assert any(item["result"] == "SKIPPED_EXISTS" for item in actions)
    ingest_actions = [
        item for item in actions if item["action"] == "INGEST_OBSERVATIONS"
    ]
    assert ingest_actions
    assert all(item["accepted_count"] == 0 for item in ingest_actions)
    assert all(item["duplicate_count"] > 0 for item in ingest_actions)


def test_observation_conflict_over_http_keeps_first_report(seeded_client: TestClient):
    batch = seeded_client.get("/api/batches/B-PASS").json()["data"]
    service = seeded_client.app.state.service
    with service.db.read_snapshot() as connection:
        sequence_one = service._load_observations(connection, "B-PASS", 0)[0]
    conflicting = {
        "batch_id": "B-PASS",
        "observations": [
            {
                "sequence": sequence_one.sequence,
                "timestamp_ms": sequence_one.timestamp_ms,
                "latency_ms": float(sequence_one.latency_ms) + 10,
                "success": sequence_one.success,
            }
        ],
    }
    response = seeded_client.post("/api/observations", json=conflicting)
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "E_CONFLICTING_REPORT"
    after = seeded_client.get("/api/batches/B-PASS").json()["data"]
    assert after["windows"][0]["sample_count"] == batch["windows"][0]["sample_count"]
