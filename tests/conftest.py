from __future__ import annotations

from pathlib import Path
from typing import Iterator

import pytest
from fastapi.testclient import TestClient

from msmq.api import create_app

BASE_MS = 4_102_444_800_000


@pytest.fixture
def client(tmp_path: Path) -> Iterator[TestClient]:
    app = create_app(tmp_path / "test.sqlite3")
    with TestClient(app) as test_client:
        yield test_client


def create_queue(
    client: TestClient,
    name: str = "test-queue",
    semantics: str = "at_least_once",
    visibility_timeout_ms: int = 1_000,
) -> dict:
    response = client.post(
        "/v1/queues",
        json={
            "name": name,
            "semantics": semantics,
            "visibility_timeout_ms": visibility_timeout_ms,
        },
    )
    assert response.status_code == 201, response.text
    return response.json()["queue"]


def enqueue(
    client: TestClient,
    queue: str,
    message_id: str,
    body: str = "body",
    *,
    ttl_ms: int = 60_000,
    produced_at_ms: int = BASE_MS,
    extensions: dict | None = None,
) -> dict:
    response = client.post(
        f"/v1/queues/{queue}/messages",
        json={
            "message_id": message_id,
            "body": body,
            "ttl_ms": ttl_ms,
            "produced_at_ms": produced_at_ms,
            "extensions": extensions or {},
        },
    )
    assert response.status_code == 201, response.text
    return response.json()["message"]


def claim(client: TestClient, queue: str, consumer_id: str = "consumer") -> dict | None:
    response = client.post(f"/v1/queues/{queue}/claim", json={"consumer_id": consumer_id})
    assert response.status_code == 200, response.text
    return response.json()["message"]

