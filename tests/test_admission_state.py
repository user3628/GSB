from __future__ import annotations

from tests.helpers import sample_route, wait_terminal, wait_status


def _request(request_id: str, **overrides):
    value = {
        "request_id": request_id,
        "model_id": "alpha",
        "sample_count": 1,
        "latency_cap_ms": 500,
        "priority": 10,
        "tenant_id": "tenant-a",
    }
    value.update(overrides)
    return value


def test_duplicate_request_reuses_result_and_conflict_is_rejected(gateway_factory, backend):
    client, _ = gateway_factory()
    assert client.post("/routes", json=sample_route(backend)).status_code == 200
    request = _request("dup-1")
    first = client.post("/requests", json=request)
    second = client.post("/requests", json=request)
    assert first.status_code == 200 and second.status_code == 200
    assert second.json()["request"]["duplicate"] is True
    assert second.json()["request"]["seq"] == first.json()["request"]["seq"]
    conflict = client.post("/requests", json={**request, "sample_count": 2})
    assert conflict.status_code == 409
    assert conflict.json()["code"] == "E_CONFLICTING_REQUEST"
    with client.app.state.gateway.store._read() as conn:
        count = conn.execute("SELECT COUNT(*) AS n FROM requests").fetchone()["n"]
    assert count == 1


def test_latency_field_aliases_are_accepted_without_extra_fields(gateway_factory, backend):
    client, _ = gateway_factory()
    assert client.post("/routes", json=sample_route(backend)).status_code == 200
    request = _request("alias-latency")
    request.pop("latency_cap_ms")
    request["expected_latency_ms"] = 321
    response = client.post("/requests", json=request)
    assert response.status_code == 200, response.text
    assert response.json()["request"]["latency_cap_ms"] == 321


def test_vram_oversize_returns_capacity_details(gateway_factory, backend):
    client, _ = gateway_factory()
    assert client.post("/routes", json=sample_route(backend)).status_code == 200
    response = client.post("/requests", json=_request("too-big", sample_count=129))
    assert response.status_code == 429
    payload = response.json()
    assert payload["code"] == "E_CAPACITY_EXCEEDED"
    assert payload["details"]["required_vram_mb"] == 258.0
    assert payload["details"]["vram_limit_mb"] == 256.0


def test_queue_full_returns_business_code(gateway_factory, backend):
    client, _ = gateway_factory(queue_limit=1)
    assert client.post("/routes", json=sample_route(backend)).status_code == 200
    assert client.post("/requests", json=_request("q-1")).status_code == 200
    response = client.post("/requests", json=_request("q-2"))
    assert response.status_code == 429
    assert response.json()["code"] == "E_QUEUE_FULL"
    round_row = client.post("/rounds", json={"time_budget_ms": 500}).json()["round"]
    rejected = [item for item in round_row["trace"]["skipped"] if item["action"] == "REJECTED"]
    assert rejected and rejected[0]["reason"] == "QUEUE_FULL"


def test_illegal_transition_does_not_change_persisted_state(gateway_factory, backend):
    client, _ = gateway_factory()
    assert client.post("/routes", json=sample_route(backend)).status_code == 200
    assert client.post("/requests", json=_request("transition-1")).status_code == 200
    response = client.post(
        "/requests/transition-1/transition",
        json={"to_status": "COMPLETED", "reason_code": "BAD"},
    )
    assert response.status_code == 409
    assert response.json()["code"] == "E_INVALID_TRANSITION"
    request = client.get("/requests/transition-1?include_transitions=true").json()
    assert request["request"]["status"] == "QUEUED"
    assert [item["to_status"] for item in request["transitions"]] == ["PENDING", "QUEUED"]


def test_strict_admission_rejects_when_backend_slot_is_busy(gateway_factory, backend):
    client, _ = gateway_factory(global_concurrency_limit=2)
    assert client.post("/routes", json=sample_route(backend)).status_code == 200
    hang = _request(
        "occupy-beta",
        model_id="beta",
        latency_cap_ms=2_000,
        fault_mode="hang",
        priority=100,
    )
    assert client.post("/requests", json=hang).status_code == 200
    round_row = client.post("/rounds", json={"time_budget_ms": 500}).json()["round"]
    assert round_row["trace"]["dispatch_order"][0]["request_id"] == "occupy-beta"
    wait_status(client, "occupy-beta", "DISPATCHED")
    strict = client.post(
        "/requests?strict_capacity=true",
        json=_request("strict-reject", model_id="beta"),
    )
    assert strict.status_code == 429
    assert strict.json()["code"] == "E_CAPACITY_EXCEEDED"
    assert "BACKEND_CONCURRENCY" in strict.json()["details"]["lacks"]
    rejected_state = client.get("/requests/strict-reject").json()["request"]
    assert rejected_state["status"] == "FAILED"
    assert rejected_state["error_code"] == "E_CAPACITY_EXCEEDED"
    cancelled = client.post("/requests/occupy-beta/cancel", json={}).json()["request"]
    assert cancelled["status"] in {"DISPATCHED", "FAILED"}
    if cancelled["status"] == "DISPATCHED":
        cancelled = wait_terminal(client, "occupy-beta")
    assert cancelled["status"] == "FAILED"

