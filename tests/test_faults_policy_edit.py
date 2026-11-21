from __future__ import annotations

import copy

from fastapi.testclient import TestClient

from tests.conftest import state_fingerprint


def _event(time_ms: int, tenant: str = "isolated", payload: int = 1) -> dict:
    return {
        "event_time_ms": time_ms,
        "tenant_id": tenant,
        "meter_mode": "BYTES",
        "io_direction": "WRITE",
        "payload_bytes": payload,
        "request_weight": 0,
    }


def test_policy_parse_stale_and_cycle_errors_do_not_change_state(
    client: TestClient, seed_policy: dict
) -> None:
    before_policy = client.get("/api/v1/policy").json()
    before_state = state_fingerprint(client)

    negative = copy.deepcopy(seed_policy)
    negative["version"] = 2
    negative["generated_at_ms"] = 1000100
    negative["tenants"][0]["capacity_bytes"] = -1
    response = client.post("/api/v1/policy", json=negative)
    assert response.status_code == 422
    assert response.json()["code"] == "E_SPEC_INVALID"

    stale = copy.deepcopy(seed_policy)
    response = client.post("/api/v1/policy", json=stale)
    assert response.status_code == 409
    assert response.json()["code"] == "E_STALE_SNAPSHOT"

    cycle = copy.deepcopy(seed_policy)
    cycle["version"] = 2
    cycle["generated_at_ms"] = 1000100
    for tenant in cycle["tenants"]:
        if tenant["tenant_id"] == "child":
            tenant["parent_id"] = "grand"
        if tenant["tenant_id"] == "grand":
            tenant["parent_id"] = "child"
    response = client.post("/api/v1/policy", json=cycle)
    assert response.status_code == 409
    body = response.json()
    assert body["code"] == "E_TENANT_CYCLE"
    assert set(body["context"]["cycle_ids"]) == {"child", "grand"}
    assert body["context"]["cycle"]

    malformed = client.post(
        "/api/v1/policy",
        content="{this is not json",
        headers={"content-type": "application/json"},
    )
    assert malformed.status_code == 422
    assert malformed.json()["code"] == "E_SPEC_INVALID"

    assert client.get("/api/v1/policy").json() == before_policy
    assert state_fingerprint(client) == before_state


def test_batch_fault_order_and_no_state_pollution(client: TestClient) -> None:
    before = state_fingerprint(client)
    base = {"events": [_event(1001000)], "faults": {}}

    all_faults = copy.deepcopy(base)
    all_faults["faults"] = {
        "invalid_payload": True,
        "unknown_tenant": True,
        "clock_rewind": True,
        "abort_midway": True,
    }
    response = client.post("/api/v1/batches", json=all_faults)
    assert response.status_code == 422
    assert response.json()["code"] == "E_SPEC_INVALID"

    unknown_and_later = copy.deepcopy(base)
    unknown_and_later["faults"] = {
        "unknown_tenant": True,
        "clock_rewind": True,
        "abort_midway": True,
    }
    response = client.post("/api/v1/batches", json=unknown_and_later)
    assert response.status_code == 404
    assert response.json()["code"] == "E_UNKNOWN_TENANT"

    clock_and_abort = copy.deepcopy(base)
    clock_and_abort["faults"] = {"clock_rewind": True, "abort_midway": True}
    response = client.post("/api/v1/batches", json=clock_and_abort)
    assert response.status_code == 409
    assert response.json()["code"] == "E_CLOCK_REWIND"

    aborted = copy.deepcopy(base)
    aborted["batch_id"] = "intentional-abort"
    aborted["faults"] = {"abort_midway": True}
    response = client.post("/api/v1/batches", json=aborted)
    assert response.status_code == 409
    body = response.json()
    assert body["code"] == "E_BATCH_ABORTED"
    assert body["context"]["committed_events"] == 0
    assert body["context"]["pending_recovery_required"] is True
    assert state_fingerprint(client) == before

    pending = client.get("/api/v1/intents", params={"status": "PENDING"}).json()["intents"]
    assert len(pending) == 1
    report = client.post("/api/v1/recover").json()
    assert body["context"]["batch_id"] in report["aborted_batches"]
    assert body["context"]["batch_id"] in report["rolled_back_batches"]
    assert any(
        item["intent_id"] == body["context"]["intent_id"]
        for item in report["pending_intents"]
    )
    assert client.get("/api/v1/intents", params={"status": "PENDING"}).json()["intents"] == []
    assert state_fingerprint(client) == before


def test_clock_rewind_inside_batch_is_atomic(client: TestClient) -> None:
    before = state_fingerprint(client)
    batch = {
        "batch_id": "clock-rewind-natural",
        "events": [_event(1001100), _event(1001000)],
    }
    response = client.post("/api/v1/batches", json=batch)
    assert response.status_code == 409
    assert response.json()["code"] == "E_CLOCK_REWIND"
    assert response.json()["context"]["event_index"] == 1
    assert state_fingerprint(client) == before
    assert client.get("/api/v1/batches/clock-rewind-natural").status_code == 404


def test_incremental_edit_stale_base_atomic_rebuild_and_simulation_invalidation(
    client: TestClient, fixed_batch: dict
) -> None:
    assert client.post("/api/v1/batches", json=fixed_batch).status_code == 201
    before_state = state_fingerprint(client)
    before_policy = client.get("/api/v1/policy").json()

    simulation = client.post(
        "/api/v1/simulations",
        json={
            "label": "before-edit",
            "events": [
                {
                    "event_time_ms": 1003000,
                    "tenant_id": "isolated",
                    "meter_mode": "COMPOSITE",
                    "io_direction": "WRITE",
                    "payload_bytes": 100,
                    "request_weight": 1,
                }
            ],
        },
    )
    assert simulation.status_code == 201
    run_id = simulation.json()["run_id"]
    assert simulation.json()["actual_state_mutated"] is False
    assert client.get(f"/api/v1/simulations/{run_id}").status_code == 200

    stale = {
        "base_version": 999,
        "generated_at_ms": 1002600,
        "tenant_patches": [{"tenant_id": "child", "capacity_bytes": 50000}],
    }
    response = client.patch("/api/v1/policy", json=stale)
    assert response.status_code == 409
    assert response.json()["code"] == "E_STALE_BASE"
    assert client.get("/api/v1/policy").json() == before_policy
    assert state_fingerprint(client) == before_state

    failing = {
        "base_version": 1,
        "generated_at_ms": 1002600,
        "tenant_patches": [{"tenant_id": "child", "capacity_bytes": 50000}],
        "faults": {"rebuild_failure": True},
    }
    response = client.patch("/api/v1/policy", json=failing)
    assert response.status_code == 500
    assert response.json()["code"] == "E_REBUILD_FAILED"
    assert client.get("/api/v1/policy").json()["version"] == 1
    assert state_fingerprint(client) == before_state
    assert client.get("/api/v1/intents", params={"status": "PENDING"}).json()["intents"]
    recovery = client.post("/api/v1/recover").json()
    assert recovery["restored_policy_versions"] == [1]
    assert client.get("/api/v1/policy").json()["version"] == 1
    assert state_fingerprint(client) == before_state

    patch = {
        "base_version": 1,
        "generated_at_ms": 1002600,
        "tenant_patches": [
            {"tenant_id": "child", "capacity_bytes": 50000},
            {"tenant_id": "sibling", "capacity_bytes": 60000},
        ],
    }
    response = client.patch("/api/v1/policy", json=patch)
    assert response.status_code == 200
    body = response.json()
    assert body["new_version"] == 2
    assert body["rebuild"]["atomic"] is True
    assert body["rebuild"]["usage_views_rebuilt"] is True
    assert set(body["affected_tenant_ids"]) == {"child", "grand", "root", "sibling"}

    state = {item["tenant_id"]: item for item in client.get("/api/v1/state").json()["tenants"]}
    assert state["child"]["state"]["used_bytes"] == "20000.000000"
    assert state["child"]["state"]["status"] == "ACTIVE"
    assert state["sibling"]["state"]["status"] == "ACTIVE"
    assert state["root"]["state"]["used_bytes"] == "30000.000000"

    stale_again = client.patch("/api/v1/policy", json=patch)
    assert stale_again.status_code == 409
    assert stale_again.json()["code"] == "E_STALE_BASE"

    stale_simulation = client.get(f"/api/v1/simulations/{run_id}")
    assert stale_simulation.status_code == 409
    assert stale_simulation.json()["code"] == "E_SIMULATION_STALE"

    next_batch = {
        "batch_id": "post-rebuild",
        "events": [_event(1003000, "child", 10000)],
    }
    response = client.post("/api/v1/batches", json=next_batch)
    assert response.status_code == 201
    assert response.json()["events"][0]["code"] == "OK"
    tenant = client.get("/api/v1/tenants/child").json()
    assert tenant["state"]["used_bytes"] == "30000.000000"
    assert tenant["state"]["status"] == "ACTIVE"


def test_lowered_quota_rebuild_marks_existing_usage_blocked(client: TestClient) -> None:
    batch = {
        "batch_id": "lower-quota-before",
        "events": [
            {
                "event_time_ms": 1001000,
                "tenant_id": "grand",
                "meter_mode": "BYTES",
                "io_direction": "WRITE",
                "payload_bytes": 10000,
                "request_weight": 0,
            }
        ],
    }
    assert client.post("/api/v1/batches", json=batch).status_code == 201
    patch = {
        "base_version": 1,
        "generated_at_ms": 1002000,
        "tenant_patches": [{"tenant_id": "grand", "capacity_bytes": 5000}],
    }
    response = client.patch("/api/v1/policy", json=patch)
    assert response.status_code == 200
    grand = client.get("/api/v1/tenants/grand").json()
    assert grand["state"]["used_bytes"] == "10000.000000"
    assert grand["state"]["remaining_bytes"] == "0.000000"
    assert grand["state"]["status"] == "QUOTA_BLOCKED"
    assert grand["state"]["last_reason"] == "REBUILT_POLICY_v2"
