from __future__ import annotations

from app.db import Database
from app.service import ScalingControlService
from tests.conftest import load_sample


def _capacity_policy() -> dict:
    return {
        "version": 1,
        "generated_at": "2026-11-21T10:00:00+08:00",
        "runtime": {
            "max_parallel_scales": 2,
            "round_time_budget_ms": 20,
            "placement_cost_ms": 10,
            "wait_queue_limit": 4,
        },
        "services": [
            {
                "id": "svc",
                "model": "m",
                "current_replicas": 1,
                "min_replicas": 1,
                "max_replicas": 3,
                "replica_memory": "1Gi",
                "replica_compute": "500m",
                "target_concurrency": 1,
                "latency_budget_ms": 100,
                "priority": 1,
                "max_replicas_per_fault_domain": 1,
            }
        ],
        "nodes": [
            {
                "id": "n1",
                "fault_domain": "d1",
                "memory": "1Gi",
                "compute_share": "1",
                "healthy": True,
            }
        ],
        "initial_placements": [{"service_id": "svc", "node_id": "n1", "count": 1}],
    }


def _capacity_observation() -> dict:
    return {
        "batch_id": "cap-obs",
        "sampled_at": "2026-11-21T10:01:00+08:00",
        "window_ms": 60000,
        "services": [
            {
                "service_id": "svc",
                "sampled_at": "2026-11-21T10:01:00+08:00",
                "arrivals": 120,
                "queue_duration_ms": 80,
                "p99_latency_ms": 500,
            }
        ],
    }


def test_insufficient_capacity_is_explicit_and_atomic(client, db_path) -> None:
    assert client.post("/api/v1/policy", json=_capacity_policy()).status_code == 200
    before = client.get("/api/v1/state").json()
    response = client.post(
        "/api/v1/rounds",
        json={
            "round_id": "capacity",
            "policy_version": 1,
            "observations": _capacity_observation(),
        },
    )
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["rejected"]
    assert result["rejected"][0]["code"] == "E_INSUFFICIENT_CAPACITY"
    assert result["rejected"][0]["node_free"][0]["free_memory_bytes"] == 0
    service = result["services"][0]
    assert service["state"] == "BLOCKED"
    assert service["current_replicas"] == 1
    assert service["desired_replicas"] == 3

    after = client.get("/api/v1/state").json()
    assert after["placements"] == before["placements"]
    assert after["nodes"][0]["used_memory_bytes"] == before["nodes"][0]["used_memory_bytes"]

    # A capacity-reducing policy update is rejected atomically with a top-level
    # E_INSUFFICIENT_CAPACITY and current usage details.
    tight = _capacity_policy()
    tight["version"] = 2
    tight["generated_at"] = "2026-11-21T10:02:00+08:00"
    tight["nodes"][0]["memory"] = "512Mi"
    tight.pop("initial_placements", None)
    rejected = client.post("/api/v1/policy", json=tight)
    assert rejected.status_code == 409
    assert rejected.json()["error"]["code"] == "E_INSUFFICIENT_CAPACITY"
    assert client.get("/api/v1/state").json()["revision"] == after["revision"]


def test_startup_recovery_marks_unfinished_round_aborted(db_path) -> None:
    db = Database(db_path)
    service = ScalingControlService(db)
    service.initialize_policy(load_sample("policy_v1.json"))
    revision = db.latest_revision()
    db.start_round(
        round_id="forced-crash",
        policy_version=1,
        request_hash="deadbeef",
        request_json="{}",
        base_revision=revision,
    )
    assert db.pending_intent_count() == 1

    restarted = ScalingControlService(Database(db_path))
    assert restarted.startup_recovery
    record = db.get_round("forced-crash")
    assert record is not None
    assert record.status == "ABORTED"
    assert record.error["error"]["code"] == "E_ROUND_ABORTED"
    assert db.latest_revision() == revision
    assert db.pending_intent_count() == 0

    report = db.recovery_report()
    assert report["source"] == "sqlite_persisted_facts"
    assert any(item["round_id"] == "forced-crash" for item in report["items"])
    assert report["latest_committed_revision"] == revision


def test_concurrent_readers_see_complete_revisions(client) -> None:
    assert client.post("/api/v1/policy", json=load_sample("policy_v1.json")).status_code == 200
    assert client.post(
        "/api/v1/rounds",
        json={
            "round_id": "read-1",
            "policy_version": 1,
            "observations": load_sample("observations_01.json"),
        },
    ).status_code == 200
    db = client.app.state.db
    revision = db.latest_revision()

    from concurrent.futures import ThreadPoolExecutor

    def read() -> tuple[int, int, int]:
        snapshot = db.get_state(revision)
        assert snapshot is not None
        return snapshot.revision, len(snapshot.placements), len(snapshot.services)

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: read(), range(24)))
    assert len(set(results)) == 1
