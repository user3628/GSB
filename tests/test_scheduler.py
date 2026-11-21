from __future__ import annotations

import time

from tests.helpers import sample_batch, sample_route, wait_status, wait_terminal


def _run_reference_scenario(client, backend):
    assert client.post("/routes", json=sample_route(backend)).status_code == 200
    submitted = client.post("/requests/batch", json=sample_batch())
    assert submitted.status_code == 200
    assert submitted.json()["accepted"] == 8

    round1 = client.post(
        "/rounds", json={"time_budget_ms": 600, "degrade_excess": False}
    ).json()["round"]
    assert [item["request_id"] for item in round1["trace"]["dispatch_order"]] == ["r-001", "r-002"]
    assert all(
        item["reason"] == "GLOBAL_CONCURRENCY"
        for item in round1["trace"]["skipped"]
        if item["action"] == "QUEUED"
    )
    assert wait_terminal(client, "r-001")["status"] == "COMPLETED"
    failed = wait_terminal(client, "r-002")
    assert failed["status"] == "FAILED"
    assert failed["error_code"] == "E_BACKEND_FAILURE"

    round2 = client.post(
        "/rounds", json={"time_budget_ms": 200, "degrade_excess": True}
    ).json()["round"]
    assert [item["request_id"] for item in round2["trace"]["dispatch_order"]] == ["r-003", "r-007"]
    assert round2["trace"]["summary"]["estimated_used_ms"] == 200
    assert all(
        item["reason"] == "TIME_BUDGET" and item["action"] == "SHED"
        for item in round2["trace"]["skipped"]
    )
    assert wait_terminal(client, "r-003")["status"] == "COMPLETED"
    timeout = wait_terminal(client, "r-007")
    assert timeout["status"] == "FAILED"
    assert timeout["error_code"] == "E_BACKEND_TIMEOUT"
    assert timeout["vram_reserved_mb"] == 0
    return round1, round2


def test_two_round_parallel_budget_timeout_and_validation(gateway_factory, backend):
    client, _ = gateway_factory()
    round1, round2 = _run_reference_scenario(client, backend)
    assert round1["trace"]["summary"]["dispatched_count"] == 2
    assert round2["trace"]["summary"]["dispatched_count"] == 2

    backends = client.get("/backends").json()["backends"]
    by_id = {item["backend_id"]: item for item in backends}
    assert by_id["backend-a"]["in_flight"] == 0
    assert by_id["backend-b"]["in_flight"] == 0
    assert by_id["backend-a"]["vram_reserved_mb"] == 0
    assert by_id["backend-b"]["vram_reserved_mb"] == 0
    assert by_id["backend-b"]["state"] == "DEGRADED"
    assert by_id["backend-b"]["timeout_count"] == 1
    assert by_id["backend-b"]["total_failures"] == 2
    assert client.get("/backends").json()["active_http_connections"] == []

    for request_id in ("r-004", "r-005", "r-006", "r-008"):
        request = client.get(f"/requests/{request_id}").json()["request"]
        assert request["status"] == "SHED"
        assert request["error_code"] == "E_TIME_BUDGET_EXHAUSTED"

    rejected = client.post(
        "/requests",
        json={
            "request_id": "degraded-new",
            "model_id": "beta",
            "sample_count": 1,
            "latency_cap_ms": 200,
            "priority": 1,
            "tenant_id": "tenant-new",
        },
    )
    assert rejected.status_code == 503
    assert rejected.json()["code"] == "E_BACKEND_DEGRADED"

    duplicate_after_degrade = client.post(
        "/requests",
        json=next(item for item in sample_batch()["requests"] if item["request_id"] == "r-002"),
    )
    assert duplicate_after_degrade.status_code == 200, duplicate_after_degrade.text
    duplicate_request = duplicate_after_degrade.json()["request"]
    assert duplicate_request["duplicate"] is True
    assert duplicate_request["status"] == "FAILED"
    assert duplicate_request["error_code"] == "E_BACKEND_FAILURE"

    validation = client.get("/validation/report").json()["report"]
    assert validation["verdict"] == "PASS", validation
    assert all(check["status"] == "PASS" for check in validation["checks"])


def test_dispatched_cancel_closes_connection_and_releases_resources(gateway_factory, backend):
    client, _ = gateway_factory()
    assert client.post("/routes", json=sample_route(backend)).status_code == 200
    request = {
        "request_id": "cancel-hang",
        "model_id": "alpha",
        "sample_count": 4,
        "latency_cap_ms": 5_000,
        "priority": 50,
        "tenant_id": "tenant-cancel",
        "fault_mode": "hang",
    }
    assert client.post("/requests", json=request).status_code == 200
    round_row = client.post("/rounds", json={"time_budget_ms": 500}).json()["round"]
    assert round_row["trace"]["dispatch_order"][0]["request_id"] == "cancel-hang"
    wait_status(client, "cancel-hang", "DISPATCHED")
    started = time.monotonic()
    response = client.post("/requests/cancel-hang/cancel", json={"reason": "test_cancel"})
    assert response.status_code == 200
    assert response.json()["request"]["status"] == "FAILED"
    assert response.json()["request"]["error_code"] == "E_REQUEST_CANCELLED"
    assert time.monotonic() - started < 2.0
    backend_row = client.get("/backends").json()["backends"][0]
    assert backend_row["in_flight"] == 0
    assert backend_row["vram_reserved_mb"] == 0
    assert client.get("/backends").json()["active_http_connections"] == []
    assert client.get("/validation/report").json()["report"]["verdict"] == "PASS"


def test_explicit_round_id_is_honored_and_cannot_be_reused(gateway_factory, backend):
    client, _ = gateway_factory()
    assert client.post("/routes", json=sample_route(backend)).status_code == 200
    first = client.post("/rounds", json={"round_id": 42, "time_budget_ms": 100})
    assert first.status_code == 200
    assert first.json()["round"]["id"] == 42
    duplicate = client.post("/rounds", json={"round_id": 42, "time_budget_ms": 100})
    assert duplicate.status_code == 409
    assert duplicate.json()["code"] == "E_ROUND_CONFLICT"


def test_global_concurrency_is_never_exceeded_with_hangs(gateway_factory, backend):
    client, _ = gateway_factory(global_concurrency_limit=2)
    assert client.post("/routes", json=sample_route(backend)).status_code == 200
    for index in range(4):
        item = {
            "request_id": f"hang-{index}",
            "model_id": "alpha",
            "sample_count": 1,
            "latency_cap_ms": 5_000,
            "priority": 10 - index,
            "tenant_id": f"tenant-{index}",
            "fault_mode": "hang",
        }
        assert client.post("/requests", json=item).status_code == 200
    round_row = client.post(
        "/rounds", json={"time_budget_ms": 1_000, "degrade_excess": False}
    ).json()["round"]
    assert len(round_row["trace"]["dispatch_order"]) == 2
    assert round_row["trace"]["summary"]["estimated_used_ms"] == 160
    status = client.get("/backends").json()
    assert sum(row["in_flight"] for row in status["backends"]) == 2
    validation = client.get("/validation/report").json()["report"]
    assert validation["verdict"] == "PASS"
    for index in range(4):
        client.post(f"/requests/hang-{index}/cancel", json={})

