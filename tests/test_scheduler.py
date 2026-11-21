from __future__ import annotations

import hashlib
import json
import socket
import time
from typing import Any

import httpx
from fastapi.testclient import TestClient

from app.main import create_app


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _create_and_submit(
    client,
    *,
    session_id: str,
    object_key: str,
    parts: list[tuple[int, int]],
    priority: int = 10,
) -> dict[str, Any]:
    created = client.post(
        "/v1/sessions",
        json={
            "object_key": object_key,
            "backend_id": "test-backend",
            "max_parts": len(parts),
            "total_parts": len(parts),
            "priority": priority,
            "session_id": session_id,
        },
        headers={"Idempotency-Key": f"{session_id}-create"},
    )
    assert created.status_code == 201, created.text
    for seq, size in parts:
        response = client.post(
            f"/v1/sessions/{session_id}/parts",
            json={
                "seq": seq,
                "size_bytes": size,
                "sha256": _digest(f"{session_id}:{seq}"),
            },
            headers={"Idempotency-Key": f"{session_id}-{seq}"},
        )
        assert response.status_code == 200, response.text
    return created.json()


def _round(client, request_id: str, **overrides):
    payload = {"time_budget_ms": 500, "request_id": request_id}
    payload.update(overrides)
    return client.post("/v1/scheduler/rounds", json=payload)


def test_parallel_round_and_backend_concurrency(app_client) -> None:
    client, backend = app_client
    _create_and_submit(
        client,
        session_id="sess-a",
        object_key="bucket/a.bin",
        parts=[(1, 100), (2, 100)],
        priority=1,
    )
    _create_and_submit(
        client,
        session_id="sess-b",
        object_key="bucket/b.bin",
        parts=[(1, 100), (2, 100)],
        priority=2,
    )
    result = _round(client, "parallel-round", queue_limit=4)
    assert result.status_code == 200, result.text
    body = result.json()
    assert body["summary"]["verified"] == 4
    assert body["summary"]["scheduler_metrics"]["global_peak_active"] <= 2
    assert body["summary"]["scheduler_metrics"]["global_peak_queued"] >= 1
    backend_state = httpx.get(f"{backend['base_url']}/admin/state").json()
    assert backend_state["peak_active_writes"] <= 2
    assert backend_state["used_bytes"] == 400
    quota = client.get("/v1/backends/test-backend/quota").json()
    assert quota["committed_bytes"] == 400
    assert quota["reserved_bytes"] == 0
    for session_id in ("sess-a", "sess-b"):
        detail = client.get(f"/v1/sessions/{session_id}").json()
        assert detail["session"]["status"] == "COMPLETED"
        assert detail["object_metadata"]["status"] == "READY"


def test_readback_mismatch_rolls_back_quota(app_client) -> None:
    client, backend = app_client
    _create_and_submit(
        client, session_id="sess-mismatch", object_key="bucket/mismatch.bin", parts=[(1, 100)]
    )
    httpx.post(
        f"{backend['base_url']}/admin/mode", json={"mode": "mismatch"}, timeout=2
    ).raise_for_status()
    response = _round(client, "mismatch-round")
    assert response.status_code == 502, response.text
    body = response.json()
    assert body["code"] == "E_READBACK_MISMATCH"
    assert body["context"]["seq"] == 1
    quota = client.get("/v1/backends/test-backend/quota").json()
    assert quota["reserved_bytes"] == 0
    assert quota["committed_bytes"] == 0
    part = client.get("/v1/sessions/sess-mismatch").json()["parts"][0]
    assert part["status"] == "DEFERRED"
    assert part["last_error_code"] == "E_READBACK_MISMATCH"


def test_timeout_closes_request_and_retry_succeeds(app_client) -> None:
    client, backend = app_client
    _create_and_submit(
        client, session_id="sess-timeout", object_key="bucket/timeout.bin", parts=[(1, 100)]
    )
    httpx.post(
        f"{backend['base_url']}/admin/mode",
        json={"mode": "hang", "hang_seconds": 0.6},
        timeout=2,
    ).raise_for_status()
    started = time.monotonic()
    timed_out = _round(
        client,
        "timeout-round",
        write_timeout_ms=80,
        read_timeout_ms=80,
    )
    elapsed = time.monotonic() - started
    assert timed_out.status_code == 504, timed_out.text
    assert timed_out.json()["code"] == "E_BACKEND_TIMEOUT"
    assert elapsed < 0.55
    quota = client.get("/v1/backends/test-backend/quota").json()
    assert quota["reserved_bytes"] == 0
    time.sleep(0.7)
    httpx.post(
        f"{backend['base_url']}/admin/mode", json={"mode": "normal"}, timeout=2
    ).raise_for_status()
    retried = _round(client, "timeout-retry")
    assert retried.status_code == 200, retried.text
    assert retried.json()["summary"]["verified"] == 1
    detail = client.get("/v1/sessions/sess-timeout").json()
    assert detail["parts"][0]["attempt_count"] == 2
    assert detail["session"]["status"] == "COMPLETED"


def test_quota_exhaustion_reports_remaining(app_client) -> None:
    client, _ = app_client
    _create_and_submit(
        client,
        session_id="sess-large-ok",
        object_key="bucket/large-ok.bin",
        parts=[(1, 24000)],
    )
    first = _round(client, "consume-capacity")
    assert first.status_code == 200
    _create_and_submit(
        client,
        session_id="sess-too-large",
        object_key="bucket/too-large.bin",
        parts=[(1, 10000)],
    )
    second = _round(client, "quota-failure")
    assert second.status_code == 409, second.text
    body = second.json()
    assert body["code"] == "E_QUOTA_EXCEEDED"
    assert body["context"]["remaining_bytes"] == 6000
    assert body["context"]["requested_bytes"] == 10000
    quota = client.get("/v1/backends/test-backend/quota").json()
    assert quota["reserved_bytes"] == 0
    assert quota["committed_bytes"] == 24000


def test_queue_full_does_not_reserve_quota(app_client) -> None:
    client, _ = app_client
    _create_and_submit(
        client,
        session_id="sess-queue",
        object_key="bucket/queue.bin",
        parts=[(1, 100), (2, 100), (3, 100)],
    )
    response = _round(client, "queue-full", queue_limit=0)
    assert response.status_code == 429, response.text
    assert response.json()["code"] == "E_SCHED_QUEUE_FULL"
    quota = client.get("/v1/backends/test-backend/quota").json()
    assert quota["reserved_bytes"] == 0
    detail = client.get("/v1/sessions/sess-queue").json()
    assert {part["status"] for part in detail["parts"]} == {"PENDING"}


def test_budget_degrades_by_priority_then_sequence(app_client) -> None:
    client, _ = app_client
    _create_and_submit(
        client,
        session_id="sess-low",
        object_key="bucket/low.bin",
        parts=[(1, 100)],
        priority=20,
    )
    _create_and_submit(
        client,
        session_id="sess-high",
        object_key="bucket/high.bin",
        parts=[(1, 100), (2, 100)],
        priority=1,
    )
    response = _round(client, "budget-round", time_budget_ms=21)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "DEGRADED"
    committed = [task for task in body["trace"] if task["state"] == "COMMITTED"]
    deferred = [task for task in body["trace"] if task["state"] == "DEFERRED"]
    assert [(task["session_id"], task["seq"]) for task in committed] == [("sess-high", 1)]
    assert {(task["session_id"], task["seq"]) for task in deferred} == {
        ("sess-high", 2),
        ("sess-low", 1),
    }
    assert [task["detail"]["downgrade_rank"] for task in deferred] == [1, 2]
    assert all(
        task["detail"]["budget_remaining_ms"] == 1
        for task in deferred
    )


def test_arrival_order_does_not_change_object_digest_and_completed_duplicate(app_client) -> None:
    client, _ = app_client
    parts = [(1, 100), (2, 100), (3, 100)]
    for session_id, object_key, order in (
        ("sess-order-a", "bucket/order-a.bin", [3, 1, 2]),
        ("sess-order-b", "bucket/order-b.bin", [2, 3, 1]),
    ):
        created = client.post(
            "/v1/sessions",
            json={
                "object_key": object_key,
                "backend_id": "test-backend",
                "max_parts": 3,
                "total_parts": 3,
                "priority": 10,
                "session_id": session_id,
            },
            headers={"Idempotency-Key": f"{session_id}-create"},
        )
        assert created.status_code == 201
        for seq in order:
            response = client.post(
                f"/v1/sessions/{session_id}/parts",
                json={
                    "seq": seq,
                    "size_bytes": 100,
                    "sha256": _digest(f"same-data:{seq}"),
                },
            )
            assert response.status_code == 200
        result = _round(client, f"{session_id}-round", session_ids=[session_id])
        assert result.status_code == 200, result.text
    first = client.get("/v1/sessions/sess-order-a").json()
    second = client.get("/v1/sessions/sess-order-b").json()
    assert first["session"]["object_digest"] == second["session"]["object_digest"]
    duplicate = client.post(
        "/v1/sessions/sess-order-a/parts",
        json={"seq": 1, "size_bytes": 100, "sha256": _digest("same-data:1")},
    )
    assert duplicate.status_code == 200
    assert duplicate.json()["duplicate"] is True
    assert duplicate.json()["version"] == first["version"]


def test_connection_refused_returns_backend_unavailable_and_rolls_back(tmp_path) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        dead_port = sock.getsockname()[1]
    config = {
        "global_concurrency": 2,
        "queue_limit": 2,
        "default_time_budget_ms": 500,
        "write_timeout_ms": 200,
        "read_timeout_ms": 200,
        "backends": [
            {
                "id": "dead-backend",
                "url": f"http://127.0.0.1:{dead_port}",
                "capacity_bytes": 10000,
                "max_concurrency": 1,
                "max_parts_per_session": 4,
                "estimated_latency_ms": 10,
                "enabled": True,
            }
        ],
    }
    config_path = tmp_path / "dead.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    app = create_app(db_file=str(tmp_path / "dead.sqlite3"), settings_file=str(config_path))
    with TestClient(app) as client:
        created = client.post(
            "/v1/sessions",
            json={
                "object_key": "bucket/dead.bin",
                "backend_id": "dead-backend",
                "max_parts": 1,
                "total_parts": 1,
                "session_id": "sess-dead",
            },
        )
        assert created.status_code == 201
        submitted = client.post(
            "/v1/sessions/sess-dead/parts",
            json={"seq": 1, "size_bytes": 100, "sha256": _digest("dead")},
        )
        assert submitted.status_code == 200
        response = client.post(
            "/v1/scheduler/rounds",
            json={"time_budget_ms": 500, "request_id": "dead-round"},
        )
        assert response.status_code == 502, response.text
        assert response.json()["code"] == "E_BACKEND_UNAVAILABLE"
        quota = client.get("/v1/backends/dead-backend/quota").json()
        assert quota["reserved_bytes"] == 0
        assert quota["committed_bytes"] == 0
