from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from delay_queue.api import create_app


def test_http_api_contract(settings):
    app = create_app(settings)
    with TestClient(app) as client:
        bad = client.post(
            "/api/v1/queues/q/tasks",
            json={
                "tasks": [
                    {
                        "task_id": "bad",
                        "due_at_ms": 0,
                        "priority": 99,
                        "remaining_retries": 0,
                        "resource_requirement": 1,
                    }
                ]
            },
        )
        assert bad.status_code == 422
        assert bad.json()["code"] == "E_TASK_INVALID"

        ok = client.post(
            "/api/v1/queues/q/tasks",
            json={"tasks": [{"task_id": "ok", "due_at_ms": 0, "priority": 1, "remaining_retries": 0, "resource_requirement": 1}]},
        )
        assert ok.status_code == 201
        assert ok.json()["inserted"] == 1

        round_response = client.post(
            "/api/v1/queues/q/rounds/r1",
            json={"max_concurrency": 1, "time_budget_ms": 500, "dispatch_timeout_ms": 200, "waiting_queue_capacity": 10, "max_task_resource": 10},
        )
        assert round_response.status_code == 200
        assert round_response.json()["dispatched"][0]["outcome"] == "SUCCEEDED"

        trajectory = client.get("/api/v1/queues/q/tasks/ok")
        assert trajectory.status_code == 200
        assert trajectory.json()["task"]["state"] == "COMPLETED"

        status = client.get("/api/v1/queues/q/status")
        assert status.json()["consistency"]["ok"] is True

        transition = client.post(
            "/api/v1/queues/q/tasks/ok/transitions",
            json={"to_state": "READY", "reason": "not terminal"},
        )
        assert transition.status_code == 409
        assert transition.json()["code"] == "E_INVALID_TRANSITION"
        assert transition.json()["context"]["from_state"] == "COMPLETED"

        conflict = client.post(
            "/api/v1/queues/q/rounds/r1",
            json={"max_concurrency": 2, "time_budget_ms": 500, "dispatch_timeout_ms": 200, "waiting_queue_capacity": 10, "max_task_resource": 10},
        )
        assert conflict.status_code == 409
        assert conflict.json()["code"] == "E_CONFLICTING_ROUND"

        recovered = client.get("/api/v1/recovery/report")
        assert recovered.status_code == 200
        assert recovered.json()["ok"] is True

        assert client.get("/api/v1/queues/q").json()["queue"] == "q"
        global_submit = client.post(
            "/api/v1/tasks",
            json={
                "queue": "q2",
                "tasks": [
                    {
                        "task_id": "global",
                        "due_at_ms": 0,
                        "priority": 1,
                        "remaining_retries": 0,
                        "resource_requirement": 1,
                    }
                ],
            },
        )
        assert global_submit.status_code == 201
        global_round = client.post(
            "/api/v1/schedule",
            json={
                "queue": "q2",
                "round_id": "global-round",
                "max_concurrency": 1,
                "time_budget_ms": 300,
                "max_task_resource": 10,
                "waiting_queue_capacity": 5,
                "dispatch_timeout_ms": 100,
            },
        )
        assert global_round.status_code == 200
        assert global_round.json()["dispatched"][0]["outcome"] == "SUCCEEDED"

        missing_queue = client.post("/api/v1/tasks", json={"tasks": []})
        assert missing_queue.status_code == 422
        assert missing_queue.json()["code"] == "E_TASK_INVALID"


def test_api_queue_full_and_timeout_codes(settings):
    app = create_app(settings)
    with TestClient(app) as client:
        tasks = [
            {"task_id": f"t{i}", "due_at_ms": 0, "priority": i, "remaining_retries": 0, "resource_requirement": 1}
            for i in range(4)
        ]
        client.post("/api/v1/queues/q/tasks", json={"tasks": tasks})
        full = client.post(
            "/api/v1/queues/q/rounds/full",
            json={"max_concurrency": 1, "waiting_queue_capacity": 1, "time_budget_ms": 100, "dispatch_timeout_ms": 50, "max_task_resource": 10},
        )
        assert full.status_code == 429
        assert full.json()["code"] == "E_QUEUE_FULL"

        client.post(
            "/api/v1/queues/hang/tasks",
            json={"tasks": [{"task_id": "hang", "due_at_ms": 0, "priority": 1, "remaining_retries": 0, "resource_requirement": 1, "payload": {"mode": "hang", "hang_seconds": 5}}]},
        )
        timed = client.post(
            "/api/v1/queues/hang/rounds/timeout",
            json={"max_concurrency": 1, "waiting_queue_capacity": 5, "time_budget_ms": 500, "dispatch_timeout_ms": 50, "max_task_resource": 10},
        )
        assert timed.status_code == 200
        assert timed.json()["dispatched"][0]["error_code"] == "E_DISPATCH_TIMEOUT"

