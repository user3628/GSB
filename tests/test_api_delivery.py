from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from .conftest import BASE_MS, claim, create_queue, enqueue


def test_at_least_once_redelivers_after_visibility_timeout(client: TestClient) -> None:
    create_queue(client, "alo", "at_least_once", 100)
    enqueue(client, "alo", "m-1")
    first = claim(client, "alo")
    assert first is not None
    time.sleep(0.15)
    second = claim(client, "alo")
    assert second is not None
    assert second["message_id"] == first["message_id"]
    assert second["delivery_count"] == 2


def test_exactly_once_duplicate_ack_is_rejected_without_decrement(client: TestClient) -> None:
    create_queue(client, "exact", "exactly_once", 1_000)
    enqueue(client, "exact", "m-1")
    message = claim(client, "exact", "consumer-1")
    assert message is not None
    payload = {
        "consumer_id": "consumer-1",
        "acks": [{"message_id": "m-1", "lease_token": message["lease_token"]}],
    }
    first = client.post("/v1/queues/exact/acks", json=payload).json()
    duplicate = client.post("/v1/queues/exact/acks", json=payload).json()
    assert first["succeeded"] == 1
    assert duplicate["results"][0]["error"]["code"] == "E_DUPLICATE_ACK"
    single_duplicate = client.post(
        "/v1/queues/exact/ack?consumer_id=consumer-1",
        json={"message_id": "m-1", "lease_token": message["lease_token"]},
    )
    assert single_duplicate.status_code == 409
    assert single_duplicate.json()["error"]["code"] == "E_DUPLICATE_ACK"
    snapshot = client.get("/v1/queues/exact/snapshot").json()
    assert snapshot["ack_count"] == 1
    assert snapshot["counts"] == {"pending": 0, "leased": 0, "acked": 1, "expired": 0}


def test_batch_ack_reports_partial_success_and_keeps_success(client: TestClient) -> None:
    create_queue(client, "batch", "at_least_once", 1_000)
    enqueue(client, "batch", "m-1")
    enqueue(client, "batch", "m-2")
    first = claim(client, "batch")
    second = claim(client, "batch")
    assert first is not None and second is not None
    response = client.post(
        "/v1/queues/batch/acks",
        json={
            "consumer_id": "consumer",
            "acks": [
                {"message_id": first["message_id"], "lease_token": first["lease_token"]},
                {"message_id": second["message_id"], "lease_token": "wrong"},
            ],
        },
    ).json()
    assert response["succeeded"] == 1
    assert response["failed"] == 1
    assert response["partial_success"] is True
    snapshot = client.get("/v1/queues/batch/snapshot").json()
    assert snapshot["counts"]["acked"] == 1
    assert snapshot["counts"]["leased"] == 1


def test_concurrent_claims_have_exactly_one_winner(client: TestClient) -> None:
    create_queue(client, "race", "exactly_once", 10_000)
    enqueue(client, "race", "only-message")

    def attempt(index: int) -> str | None:
        response = client.post(
            "/v1/queues/race/claim", json={"consumer_id": f"consumer-{index}"}
        )
        assert response.status_code == 200, response.text
        message = response.json()["message"]
        return message["message_id"] if message else None

    with ThreadPoolExecutor(max_workers=8) as executor:
        winners = list(executor.map(attempt, range(8)))
    assert sum(value is not None for value in winners) == 1
    snapshot = client.get("/v1/queues/race/snapshot").json()
    assert snapshot["messages"][0]["state"] == "leased"
    assert snapshot["messages"][0]["delivery_count"] == 1
    assert snapshot["counts"]["leased"] == 1


def test_semantics_migration_is_explicit_and_lease_aware(client: TestClient) -> None:
    create_queue(client, "migrate", "at_least_once", 100)
    enqueue(client, "migrate", "m-1")
    assert claim(client, "migrate") is not None
    blocked = client.post(
        "/v1/queues/migrate/migrate-semantics", json={"semantics": "exactly_once"}
    )
    assert blocked.status_code == 409
    assert blocked.json()["error"]["code"] == "E_QUEUE_BUSY"
    time.sleep(0.15)
    migrated = client.post(
        "/v1/queues/migrate/migrate-semantics", json={"semantics": "exactly_once"}
    )
    assert migrated.status_code == 200
    assert migrated.json()["migration"]["explicit_migration"] is True
    assert client.get("/v1/queues/migrate").json()["queue"]["semantics"] == "exactly_once"


def test_json_export_refuses_oversize_body_instead_of_truncating(client: TestClient) -> None:
    create_queue(client, "large", "at_least_once", 1_000)
    enqueue(client, "large", "large-1", "x" * 5_000, produced_at_ms=BASE_MS)
    response = client.get("/v1/export/jsonl?queue=large")
    assert response.status_code == 413
    assert response.json()["error"]["code"] == "E_RECORD_TOO_LARGE"


def test_validation_and_not_found_errors_share_the_json_envelope(client: TestClient) -> None:
    invalid = client.post(
        "/v1/queues", json={"name": "x", "semantics": "not-a-mode"}
    )
    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "E_BAD_REQUEST"
    missing = client.get("/v1/queues/does-not-exist")
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "E_QUEUE_NOT_FOUND"


def test_queue_version_snapshots_are_persisted(client: TestClient) -> None:
    create_queue(client, "versions", "at_least_once", 1_000)
    enqueue(client, "versions", "m-1")
    response = client.get("/v1/queues/versions/versions")
    assert response.status_code == 200
    versions = response.json()["versions"]
    assert len(versions) >= 2
    assert versions[0]["snapshot"]["message_count"] == 1

