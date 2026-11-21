from __future__ import annotations

import hashlib

from fastapi.testclient import TestClient


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _create(client: TestClient, **overrides):
    payload = {
        "object_key": "bucket/object.bin",
        "backend_id": "test-backend",
        "max_parts": 4,
        "total_parts": 3,
        "priority": 10,
        "session_id": "sess-test",
    }
    payload.update(overrides)
    return client.post("/v1/sessions", json=payload, headers={"Idempotency-Key": payload["session_id"] + "-create"})


def test_session_conflict_and_part_sequence_without_state_change(app_client) -> None:
    client, _ = app_client
    created = _create(client)
    assert created.status_code == 201, created.text
    version = created.json()["version"]

    conflict = _create(client, session_id="sess-other", object_key="bucket/object.bin")
    assert conflict.status_code == 409
    assert conflict.json()["code"] == "E_SESSION_CONFLICT"
    assert conflict.json()["context"]["existing_session_id"] == "sess-test"

    invalid = client.post(
        "/v1/sessions/sess-test/parts",
        json={"seq": 9, "size_bytes": 10, "sha256": _digest("bad")},
    )
    assert invalid.status_code == 422
    assert invalid.json()["code"] == "E_PART_SEQ_INVALID"
    assert client.get("/v1/sessions/sess-test").json()["version"] == version


def test_part_idempotency_mismatch_and_gap_finalize(app_client) -> None:
    client, _ = app_client
    assert _create(client).status_code == 201
    first_digest = _digest("part-1")
    first = client.post(
        "/v1/sessions/sess-test/parts",
        json={"seq": 1, "size_bytes": 100, "sha256": first_digest},
    )
    assert first.status_code == 200
    first_version = first.json()["version"]

    duplicate = client.post(
        "/v1/sessions/sess-test/parts",
        json={"seq": 1, "size_bytes": 100, "sha256": first_digest},
    )
    assert duplicate.status_code == 200
    assert duplicate.json()["duplicate"] is True
    assert duplicate.json()["version"] == first_version

    mismatch = client.post(
        "/v1/sessions/sess-test/parts",
        json={"seq": 1, "size_bytes": 100, "sha256": _digest("changed")},
    )
    assert mismatch.status_code == 409
    assert mismatch.json()["code"] == "E_PART_MISMATCH"
    assert mismatch.json()["context"]["existing_sha256"] == first_digest
    detail = client.get("/v1/sessions/sess-test").json()
    assert detail["parts"][0]["sha256"] == first_digest
    assert detail["version"] == first_version

    gap = client.post(
        "/v1/sessions/sess-test/parts",
        json={"seq": 3, "size_bytes": 100, "sha256": _digest("part-3")},
    )
    assert gap.status_code == 200
    gap_version = gap.json()["version"]
    finalize = client.post("/v1/sessions/sess-test/finalize", json={})
    assert finalize.status_code == 422
    assert finalize.json()["code"] == "E_PART_SEQ_INVALID"
    assert finalize.json()["context"]["missing_seqs"] == [2]
    assert client.get("/v1/sessions/sess-test").json()["version"] == gap_version


def test_request_id_reuse_and_conflict(app_client) -> None:
    client, _ = app_client
    payload = {
        "object_key": "bucket/idempotent.bin",
        "backend_id": "test-backend",
        "max_parts": 2,
        "total_parts": 2,
        "priority": 1,
        "session_id": "sess-idempotent",
    }
    first = client.post("/v1/sessions", json=payload, headers={"Idempotency-Key": "same-key"})
    second = client.post("/v1/sessions", json=payload, headers={"Idempotency-Key": "same-key"})
    assert first.status_code == second.status_code == 201
    assert first.json()["version"] == second.json()["version"]

    conflicting = dict(payload, object_key="bucket/different.bin")
    conflict = client.post(
        "/v1/sessions", json=conflicting, headers={"Idempotency-Key": "same-key"}
    )
    assert conflict.status_code == 409
    assert conflict.json()["code"] == "E_CONFLICTING_REQUEST"
