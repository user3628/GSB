from __future__ import annotations


from fastapi.testclient import TestClient

from tests.conftest import state_fingerprint


def _event(
    time_ms: int,
    tenant: str = "isolated",
    mode: str = "BYTES",
    payload: str | int = 0,
    weight: str | int = 0,
    direction: str = "WRITE",
) -> dict:
    return {
        "event_time_ms": time_ms,
        "tenant_id": tenant,
        "meter_mode": mode,
        "io_direction": direction,
        "payload_bytes": payload,
        "request_weight": weight,
    }


def test_seeded_policy_is_checksummed_and_state_is_fixed_point(client: TestClient) -> None:
    policy = client.get("/api/v1/policy")
    assert policy.status_code == 200
    body = policy.json()
    assert body["version"] == 1
    assert len(body["tenants"]) == 5
    assert len(body["checksum"]) == 64

    state = client.get("/api/v1/state").json()
    assert state["policy_version"] == 1
    assert state["accounting"]["scale"] == 6
    assert "ROUND_FLOOR" in state["accounting"]["rounding"]
    by_id = {item["tenant_id"]: item for item in state["tenants"]}
    assert by_id["root"]["state"]["tokens"] == "5.000000"
    assert by_id["root"]["state"]["used_bytes"] == "0.000000"


def test_fixed_batch_three_modes_parent_cascade_and_atomic_rejection(
    client: TestClient, fixed_batch: dict
) -> None:
    response = client.post("/api/v1/batches", json=fixed_batch)
    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "COMPLETED"
    assert body["summary"]["accepted_count"] == 4
    assert body["summary"]["rejected_count"] == 3

    events = body["events"]
    assert [event["code"] for event in events] == [
        "OK",
        "OK",
        "OK",
        "E_QUOTA_EXCEEDED",
        "E_RATE_LIMITED",
        "E_PARENT_QUOTA_EXCEEDED",
        "OK",
    ]

    # BYTES, REQUESTS, COMPOSITE semantics.
    assert events[0]["charged"]["capacity_bytes"] == "20000.000000"
    assert events[0]["charged"]["request_tokens"] == "0.000000"
    assert events[1]["charged"]["capacity_bytes"] == "0.000000"
    assert events[1]["charged"]["request_tokens"] == "1.000000"
    assert events[2]["charged"]["capacity_bytes"] == "10000.000000"
    assert events[2]["charged"]["request_tokens"] == "2.000000"

    # Child-own capacity failure.
    assert events[3]["context"]["resource"] == "capacity_bytes"
    assert events[3]["context"]["remaining_bytes"] == "20000.000000"
    assert events[3]["context"]["shortage_bytes"] == "5000.000000"
    assert events[3]["tenant_status"] == "QUOTA_BLOCKED"

    # Rate failure is reported with balance and a deterministic next time.
    assert events[4]["context"]["resource"] == "request_tokens"
    assert events[4]["context"]["tokens_remaining"] == "0.600000"
    assert events[4]["context"]["next_available_at_ms"] == 1001534

    # Composite event 4 failed before any partial capacity deduction. The
    # rejected event 5 is the first ancestor-capacity failure.
    assert events[5]["code"] == "E_PARENT_QUOTA_EXCEEDED"
    assert events[5]["context"]["resource"] == "capacity_bytes"
    assert events[5]["context"]["insufficient_tenant_id"] == "root"
    assert events[5]["context"]["difference_bytes"] == "5000.000000"

    state = {item["tenant_id"]: item for item in client.get("/api/v1/state").json()["tenants"]}
    assert state["child"]["state"]["used_bytes"] == "20000.000000"
    assert state["root"]["state"]["used_bytes"] == "30000.000000"
    assert state["sibling"]["state"]["used_bytes"] == "10000.000000"
    assert state["child"]["state"]["status"] == "QUOTA_BLOCKED"
    assert state["root"]["state"]["status"] == "QUOTA_BLOCKED"

    # Exactly four accepted event rows, one per accepted event.
    deductions = client.get("/api/v1/deductions", params={"tenant_id": "root"}).json()[
        "deductions"
    ]
    assert len(deductions) == 4  # one root-account ledger row per accepted event


def test_grandchild_usage_is_aggregated_into_all_ancestors(client: TestClient) -> None:
    batch = {
        "batch_id": "grand-capacity",
        "events": [_event(1001000, "grand", "BYTES", 10000)],
    }
    response = client.post("/api/v1/batches", json=batch)
    assert response.status_code == 201
    state = {item["tenant_id"]: item for item in client.get("/api/v1/state").json()["tenants"]}
    assert state["grand"]["state"]["used_bytes_units"] == 10_000_000_000
    assert state["child"]["state"]["used_bytes_units"] == 10_000_000_000
    assert state["root"]["state"]["used_bytes_units"] == 10_000_000_000
    assert state["grand"]["state"]["direct_used_bytes_units"] == 10_000_000_000
    assert state["child"]["state"]["direct_used_bytes_units"] == 0


def test_parent_request_token_bucket_cascade(client: TestClient) -> None:
    patch = {
        "base_version": 1,
        "generated_at_ms": 1000500,
        "tenant_patches": [
            {"tenant_id": "root", "burst_capacity": 2},
            {"tenant_id": "child", "burst_capacity": 10},
            {"tenant_id": "sibling", "burst_capacity": 10},
        ],
    }
    assert client.patch("/api/v1/policy", json=patch).status_code == 200
    batch = {
        "batch_id": "parent-token-cascade",
        "events": [
            _event(1001000, "sibling", "REQUESTS", weight=2),
            _event(1001000, "child", "REQUESTS", weight=1),
        ],
    }
    response = client.post("/api/v1/batches", json=batch)
    assert response.status_code == 201
    rejection = response.json()["events"][1]
    assert rejection["code"] == "E_PARENT_QUOTA_EXCEEDED"
    assert rejection["context"]["resource"] == "request_tokens"
    assert rejection["context"]["insufficient_tenant_id"] == "root"
    assert rejection["context"]["difference_tokens"] == "1.000000"
    assert rejection["context"]["next_available_at_ms"] is not None


def test_fixed_point_accumulation_has_no_float_drift(client: TestClient) -> None:
    patch = {
        "base_version": 1,
        "generated_at_ms": 1000500,
        "tenant_patches": [{"tenant_id": "isolated", "capacity_bytes": "2000000"}],
    }
    assert client.patch("/api/v1/policy", json=patch).status_code == 200
    batch = {
        "batch_id": "fixed-point-sum",
        "events": [
            _event(1001000, "isolated", "BYTES", "1000000.000001"),
            _event(1001001, "isolated", "BYTES", "0.000002"),
            _event(1001002, "isolated", "BYTES", "0.000003"),
        ],
    }
    response = client.post("/api/v1/batches", json=batch)
    assert response.status_code == 201
    assert all(item["accepted"] for item in response.json()["events"])
    tenant = client.get("/api/v1/tenants/isolated").json()
    assert tenant["state"]["used_bytes"] == "1000000.000006"
    assert tenant["state"]["remaining_bytes"] == "999999.999994"


def test_restart_uses_same_sqlite_facts(client: TestClient, fixed_batch: dict) -> None:
    assert client.post("/api/v1/batches", json=fixed_batch).status_code == 201
    before = state_fingerprint(client)
    database = client.settings.database_path  # type: ignore[attr-defined]

    from app.config import Settings
    from app.main import create_app

    second = create_app(
        Settings(
            database_path=database,
            samples_dir=client.settings.samples_dir,  # type: ignore[attr-defined]
            auto_seed=True,
            auto_recover=True,
            throttle_rejection_threshold=3,
        )
    )
    with TestClient(second) as restarted:
        assert state_fingerprint(restarted) == before
        assert restarted.get("/api/v1/batches/fixed-verification-batch-001").status_code == 200


def test_simulation_can_replay_fixed_batch_from_policy_baseline(
    client: TestClient, fixed_batch: dict
) -> None:
    execution = client.post("/api/v1/batches", json=fixed_batch)
    assert execution.status_code == 201
    state_before = state_fingerprint(client)
    replay = client.post(
        "/api/v1/simulations",
        json={
            "label": "replay-fixed-batch",
            "baseline": "POLICY",
            "events": fixed_batch["events"],
        },
    )
    assert replay.status_code == 201
    body = replay.json()
    assert body["baseline"] == "POLICY"
    assert body["actual_state_mutated"] is False
    assert [event["code"] for event in body["events"]] == [
        event["code"] for event in execution.json()["events"]
    ]
    assert state_fingerprint(client) == state_before

    current_clock_replay = client.post(
        "/api/v1/simulations",
        json={
            "label": "replay-from-current-clock",
            "baseline": "CURRENT",
            "events": fixed_batch["events"],
        },
    )
    assert current_clock_replay.status_code == 409
    assert current_clock_replay.json()["code"] == "E_CLOCK_REWIND"


def test_parent_reassignment_rebuilds_ancestor_usage_views(client: TestClient) -> None:
    batch = {
        "batch_id": "move-tree-before",
        "events": [_event(1001000, "grand", "BYTES", 10000)],
    }
    assert client.post("/api/v1/batches", json=batch).status_code == 201
    patch = {
        "base_version": 1,
        "generated_at_ms": 1002000,
        "tenant_patches": [{"tenant_id": "child", "parent_id": "isolated"}],
    }
    response = client.patch("/api/v1/policy", json=patch)
    assert response.status_code == 200
    state = {item["tenant_id"]: item for item in client.get("/api/v1/state").json()["tenants"]}
    assert state["grand"]["state"]["used_bytes_units"] == 10_000_000_000
    assert state["child"]["state"]["used_bytes_units"] == 10_000_000_000
    assert state["isolated"]["state"]["used_bytes_units"] == 10_000_000_000
    assert state["root"]["state"]["used_bytes_units"] == 0


def test_submillisecond_timestamp_is_floored_before_engine(client: TestClient) -> None:
    batch = {
        "batch_id": "submillisecond-floor",
        "events": [
            {
                "event_time_ms": 1001000.999,
                "tenant_id": "isolated",
                "meter_mode": "BYTES",
                "io_direction": "WRITE",
                "payload_bytes": 1,
                "request_weight": 0,
            }
        ],
    }
    response = client.post("/api/v1/batches", json=batch)
    assert response.status_code == 201
    assert response.json()["events"][0]["event_time_ms"] == 1001000
    assert client.get("/health").json()["last_event_ms"] == 1001000
