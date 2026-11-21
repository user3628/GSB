from __future__ import annotations

import json

from tests.conftest import load_sample, round_body


def _submit_policy(client) -> dict:
    response = client.post("/api/v1/policy", json=load_sample("policy_v1.json"))
    assert response.status_code == 200, response.text
    return response.json()


def _assert_resource_invariant(client) -> None:
    state = client.get("/api/v1/state").json()
    for node in state["nodes"]:
        assert node["free_memory_bytes"] >= 0
        assert node["free_compute_milli"] >= 0
        managed = [p for p in state["placements"] if p["node_id"] == node["id"]]
        expected_memory = node["reserved_memory_bytes"] + sum(p["memory_bytes"] for p in managed)
        expected_compute = node["reserved_compute_milli"] + sum(p["compute_milli"] for p in managed)
        assert node["used_memory_bytes"] == expected_memory
        assert node["used_compute_milli"] == expected_compute
    for service in state["services"]:
        assert service["current_replicas"] == len(service["placements"])


def test_policy_validation_stale_and_no_state_mutation(client) -> None:
    invalid = client.post("/api/v1/policy", json=load_sample("policy_invalid_unit.json"))
    assert invalid.status_code == 400
    assert invalid.json()["error"]["code"] == "E_SPEC_INVALID"

    committed = _submit_policy(client)
    revision = committed["revision"]
    stale = client.post("/api/v1/policy", json=load_sample("policy_v1.json"))
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "E_STALE_SNAPSHOT"
    assert client.get("/api/v1/state").json()["revision"] == revision

    # Version precedence: even an otherwise invalid stale body is reported as stale.
    invalid_v1 = load_sample("policy_invalid_unit.json")
    invalid_v1["version"] = 1
    stale_invalid = client.post("/api/v1/policy", json=invalid_v1)
    assert stale_invalid.status_code == 409
    assert stale_invalid.json()["error"]["code"] == "E_STALE_SNAPSHOT"


def test_derivation_endpoint_is_bitwise_stable(client) -> None:
    _submit_policy(client)
    payload = {"policy_version": 1, "observations": load_sample("observations_01.json")}
    first = client.post("/api/v1/derive", json=payload)
    second = client.post("/api/v1/derive", json=payload)
    assert first.status_code == second.status_code == 200
    assert json.dumps(first.json(), sort_keys=True) == json.dumps(second.json(), sort_keys=True)
    assert [item["desired_replicas"] for item in first.json()["derivations"]] == [3, 3, 3]


def test_parallel_budget_priority_queue_and_idempotent_round(client) -> None:
    _submit_policy(client)
    body = round_body("round-001", "observations_01.json")
    response = client.post("/api/v1/rounds", json=body)
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["summary"]["scale_up_requested"] == 6
    assert result["summary"]["scale_up_granted"] == 3
    assert result["summary"]["scale_up_queued"] == 3
    assert result["summary"]["admission_slots_used"] == 3
    assert [action["service_id"] for action in result["actions"]] == [
        "chat",
        "chat",
        "rerank",
    ]
    status = {item["service_id"]: item for item in result["services"]}
    assert status["chat"]["state"] == "STABLE"
    assert status["rerank"]["state"] == "SCALING_UP"
    assert status["embed"]["state"] == "SCALING_UP"
    revision = result["revision"]

    replay = client.post("/api/v1/rounds", json=body)
    assert replay.status_code == 200
    assert replay.json()["replayed"] is True
    assert replay.json()["revision"] == revision

    conflicting = dict(body)
    conflicting["observations"] = load_sample("observations_02.json")
    conflict_response = client.post("/api/v1/rounds", json=conflicting)
    assert conflict_response.status_code == 409
    assert conflict_response.json()["error"]["code"] == "E_CONFLICTING_ROUND"
    assert client.get("/api/v1/state").json()["revision"] == revision
    _assert_resource_invariant(client)


def test_scale_down_reclaim_fault_domain_degrade_and_atomic_state(client) -> None:
    _submit_policy(client)
    for name, round_id in (
        ("observations_01.json", "r-1"),
        ("observations_02.json", "r-2"),
        ("observations_03.json", "r-3"),
    ):
        response = client.post("/api/v1/rounds", json=round_body(round_id, name))
        assert response.status_code == 200, response.text
    result = response.json()
    assert result["summary"]["scale_down_removed"] == 2
    assert result["summary"]["scale_up_granted"] == 3
    action_types = [item["type"] for item in result["actions"]]
    assert action_types.index("SCALE_DOWN") < action_types.index("SCALE_UP")
    chat = next(item for item in result["services"] if item["service_id"] == "chat")
    assert chat["state"] == "DEGRADED"
    assert chat["current_replicas"] == 6
    assert chat["degraded_domains"]
    assert all(item["actual"] > item["max_allowed"] for item in chat["degraded_domains"])
    _assert_resource_invariant(client)


def test_node_failure_migration_is_reported_not_silently_skipped(client) -> None:
    _submit_policy(client)
    for name, round_id in (
        ("observations_01.json", "r-1"),
        ("observations_02.json", "r-2"),
        ("observations_03.json", "r-3"),
    ):
        assert client.post("/api/v1/rounds", json=round_body(round_id, name)).status_code == 200

    first = client.post(
        "/api/v1/rounds", json=round_body("failover-1", "observations_04_failover.json")
    )
    assert first.status_code == 200, first.text
    data = first.json()
    assert data["summary"]["migrations_completed"] == 3
    assert data["summary"]["migrations_queued"] == 1
    assert any(item["type"] == "MIGRATE" for item in data["actions"])
    embed = next(item for item in data["services"] if item["service_id"] == "embed")
    assert embed["state"] == "BLOCKED"

    second = client.post(
        "/api/v1/rounds", json=round_body("failover-2", "observations_04_failover.json")
    )
    assert second.status_code == 200, second.text
    second_data = second.json()
    assert second_data["summary"]["migrations_completed"] == 1
    state = client.get("/api/v1/state").json()
    node_health = {node["id"]: node["healthy"] for node in state["nodes"]}
    assert node_health["gpu-b1"] is False
    assert all(node_health[placement["node_id"]] for placement in state["placements"])
    _assert_resource_invariant(client)


def test_queue_full_aborts_without_revision_change(client) -> None:
    committed = _submit_policy(client)
    body = round_body("queue-full", "observations_05_queue_full.json")
    response = client.post("/api/v1/rounds", json=body)
    assert response.status_code == 429
    error = response.json()["error"]
    assert error["code"] == "E_SCALE_QUEUE_FULL"
    assert error["details"]["queued_replicas"] > error["details"]["wait_queue_limit"]
    assert client.get("/api/v1/state").json()["revision"] == committed["revision"]
    stored = client.get("/api/v1/rounds/queue-full").json()
    assert stored["status"] == "ABORTED"

    replay = client.post("/api/v1/rounds", json=body)
    assert replay.status_code == 429
    assert replay.json()["error"]["details"]["replayed"] is True


def test_transition_validation_and_missing_observation_have_no_commit(client) -> None:
    committed = _submit_policy(client)
    revision = committed["revision"]
    self_transition = client.post(
        "/api/v1/services/chat/transition",
        json={"target_state": "STABLE", "expected_revision": revision},
    )
    assert self_transition.status_code == 409
    assert self_transition.json()["error"]["code"] == "E_INVALID_TRANSITION"
    assert client.get("/api/v1/state").json()["revision"] == revision

    missing = load_sample("observations_01.json")
    missing["services"] = missing["services"][:-1]
    response = client.post(
        "/api/v1/rounds",
        json={"round_id": "missing-observation", "policy_version": 1, "observations": missing},
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "E_OBSERVATION_INVALID"
    assert client.get("/api/v1/state").json()["revision"] == revision

    legal = client.post(
        "/api/v1/services/chat/transition",
        json={"target_state": "BLOCKED", "expected_revision": revision},
    )
    assert legal.status_code == 200
    assert legal.json()["revision"] == revision + 1


def test_round_not_found_and_json_error_envelope(client) -> None:
    response = client.get("/api/v1/rounds/does-not-exist")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "E_ROUND_NOT_FOUND"
    route = client.get("/api/v1/no-such-route")
    assert route.status_code == 404
    assert route.json()["error"]["code"] == "E_NOT_FOUND"


def test_parallel_waves_are_explicit_and_time_budget_leaves_one_queued(client) -> None:
    policy = {
        "version": 1,
        "generated_at": "2026-11-21T11:00:00+08:00",
        "runtime": {
            "max_parallel_scales": 2,
            "round_time_budget_ms": 20,
            "placement_cost_ms": 10,
            "wait_queue_limit": 2,
        },
        "services": [
            {
                "id": "wave-svc",
                "model": "m",
                "current_replicas": 1,
                "min_replicas": 1,
                "max_replicas": 6,
                "replica_memory": "1Gi",
                "replica_compute": "100m",
                "target_concurrency": 1,
                "latency_budget_ms": 100,
                "priority": 1,
                "max_replicas_per_fault_domain": 6,
            }
        ],
        "nodes": [
            {
                "id": "n1",
                "fault_domain": "d1",
                "memory": "16Gi",
                "compute_share": "8",
            }
        ],
        "initial_placements": [
            {"service_id": "wave-svc", "node_id": "n1", "count": 1}
        ],
    }
    observation = {
        "batch_id": "wave-obs",
        "sampled_at": "2026-11-21T11:01:00+08:00",
        "window_ms": 60000,
        "services": [
            {
                "service_id": "wave-svc",
                "sampled_at": "2026-11-21T11:01:00+08:00",
                "arrivals": 6000,
                "queue_duration_ms": 0,
                "p99_latency_ms": 10,
            }
        ],
    }
    assert client.post("/api/v1/policy", json=policy).status_code == 200
    response = client.post(
        "/api/v1/rounds",
        json={"round_id": "wave-round", "policy_version": 1, "observations": observation},
    )
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["budget"]["available_time_waves"] == 2
    assert result["budget"]["grant_slots"] == 4
    assert [item["parallel_wave"] for item in result["actions"]] == [1, 1, 2, 2]
    assert result["summary"]["scale_up_granted"] == 4
    assert result["summary"]["scale_up_queued"] == 1
    assert result["services"][0]["state"] == "SCALING_UP"
