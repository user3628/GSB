from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from fastapi.testclient import TestClient

from msmq.formats import encode_jsonl
from msmq.models import MessageRecord, QueueMeta

from .conftest import BASE_MS, create_queue, enqueue

SAMPLES = Path(__file__).resolve().parents[1] / "artifacts" / "samples"


def import_jsonl(client: TestClient, data: bytes):
    return client.post(
        "/v1/import/jsonl",
        content=data,
        headers={"content-type": "application/x-ndjson"},
    )


def import_compact(client: TestClient, data: bytes):
    return client.post(
        "/v1/import/compact",
        content=data,
        headers={"content-type": "application/octet-stream"},
    )


def test_legacy_degrades_and_unknown_optional_fields_are_preserved(client: TestClient) -> None:
    legacy = import_compact(client, (SAMPLES / "legacy_v1.bin").read_bytes())
    assert legacy.status_code == 200
    report = legacy.json()["import_report"]
    assert report["decision"] == "degrade_accept"
    assert report["degradations"]
    optional = import_compact(client, (SAMPLES / "optional_v2.bin").read_bytes())
    assert optional.status_code == 200
    preserved = optional.json()["import_report"]["preserved_optional_fields"]
    assert any("vendor_trace_id" in key for key in preserved)
    exported = client.get("/v1/export/compact?queue=demo-orders")
    assert exported.status_code == 200
    assert b"vendor_trace_id" in exported.content


def test_future_required_import_is_atomic(client: TestClient) -> None:
    assert import_compact(client, (SAMPLES / "legacy_v1.bin").read_bytes()).status_code == 200
    before = client.get("/v1/queues/demo-orders/snapshot").json()
    response = import_compact(client, (SAMPLES / "future_required_v3.bin").read_bytes())
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "E_FORMAT_INCOMPATIBLE"
    after = client.get("/v1/queues/demo-orders/snapshot").json()
    assert before["messages"] == after["messages"]
    assert response.json()["error"]["details"]["state_changed"] is False


def test_conflict_rejection_does_not_commit_earlier_or_later_rows(client: TestClient) -> None:
    assert import_jsonl(client, (SAMPLES / "conflict_seed.jsonl").read_bytes()).status_code == 200
    before = client.get("/v1/queues/conflict-queue/snapshot").json()
    response = import_jsonl(
        client, (SAMPLES / "conflict_atomic_reject.jsonl").read_bytes()
    )
    assert response.status_code == 409
    error = response.json()["error"]
    assert error["code"] == "E_CONFLICT_AMBIGUOUS"
    assert error["details"]["rejected_index"] == 0
    after = client.get("/v1/queues/conflict-queue/snapshot").json()
    assert before["messages"] == after["messages"]
    assert all(message["message_id"] != "conflict-2" for message in after["messages"])


def test_import_version_overwrite_and_skip_rules(client: TestClient) -> None:
    queue = QueueMeta("rules", "exactly_once", 1_000)
    original = MessageRecord(
        message_id="rules-1",
        queue="rules",
        body="original",
        produced_at_us=BASE_MS * 1_000,
        expires_at_us=(BASE_MS + 60_000) * 1_000,
        state="pending",
        record_version=1,
        last_write_at_us=BASE_MS * 1_000,
    )
    assert import_jsonl(client, encode_jsonl([original], [queue])).status_code == 200
    higher = replace(
        original,
        body="higher-version",
        record_version=2,
        last_write_at_us=BASE_MS * 1_000 + 100_000,
    )
    overwrite = import_jsonl(client, encode_jsonl([higher], [queue]))
    assert overwrite.status_code == 200
    assert overwrite.json()["import_report"]["overwritten"] == 1
    assert client.get("/v1/queues/rules/snapshot").json()["messages"][0]["body"] == "higher-version"
    lower = replace(
        original,
        body="must-be-skipped",
        record_version=1,
        last_write_at_us=BASE_MS * 1_000,
    )
    skipped = import_jsonl(client, encode_jsonl([lower], [queue]))
    assert skipped.status_code == 200
    assert skipped.json()["import_report"]["skipped"] == 1
    snapshot_message = client.get("/v1/queues/rules/snapshot").json()["messages"][0]
    assert snapshot_message["body"] == "higher-version"
    assert snapshot_message["record_version"] == 2


def test_duplicate_ids_inside_import_batch_are_rejected_atomically(client: TestClient) -> None:
    queue = QueueMeta("dupes", "at_least_once", 1_000)
    message = MessageRecord(
        message_id="dup-1",
        queue="dupes",
        body="same",
        produced_at_us=BASE_MS * 1_000,
        expires_at_us=(BASE_MS + 60_000) * 1_000,
        last_write_at_us=BASE_MS * 1_000,
    )
    response = import_jsonl(client, encode_jsonl([message, message], [queue]))
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "E_FORMAT_INVALID"
    assert response.json()["error"]["details"]["rejected_index"] == 1
    queue_response = client.get("/v1/queues/dupes")
    assert queue_response.status_code == 404


def test_import_cannot_switch_existing_queue_mode(client: TestClient) -> None:
    create_queue(client, "demo-orders", "exactly_once", 1_000)
    response = import_jsonl(client, (SAMPLES / "current_v1.jsonl").read_bytes())
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "E_QUEUE_MODE_MISMATCH"
    snapshot = client.get("/v1/queues/demo-orders/snapshot").json()
    assert snapshot["message_count"] == 0


def test_duplicate_ack_from_other_consumer_is_not_duplicate_key(client: TestClient) -> None:
    create_queue(client, "other", "exactly_once", 1_000)
    enqueue(client, "other", "m-1")
    message = client.post(
        "/v1/queues/other/claim", json={"consumer_id": "consumer-a"}
    ).json()["message"]
    first = client.post(
        "/v1/queues/other/acks",
        json={
            "consumer_id": "consumer-a",
            "acks": [{"message_id": "m-1", "lease_token": message["lease_token"]}],
        },
    ).json()
    second = client.post(
        "/v1/queues/other/acks",
        json={
            "consumer_id": "consumer-b",
            "acks": [{"message_id": "m-1", "lease_token": message["lease_token"]}],
        },
    ).json()
    assert first["succeeded"] == 1
    assert second["results"][0]["error"]["code"] == "E_ALREADY_ACKED"

