from __future__ import annotations

from pathlib import Path

from quota_scheduler.api import create_app
from quota_scheduler.service import QuotaScheduler

from conftest import request_payload, write_bundle


def test_flask_api_end_to_end(tmp_path) -> None:
    samples = write_bundle(
        tmp_path / "samples",
        groups=[{"group_id": "g1", "memory_limit": "50Gi", "compute_limit": "100000m"}],
        nodes=[
            {
                "node_id": "n1",
                "total_memory": "40Gi",
                "available_compute": "10000m",
                "occupied_memory": "0",
                "quota_group": "g1",
                "healthy": True,
            }
        ],
        config={
            "batch_strategy": "greedy",
            "global_max_parallel_batches": 1,
            "round_time_budget": "2000ms",
            "execution_queue_capacity": 4,
        },
    )
    app = create_app(tmp_path / "api.db", sample_dir=samples, bootstrap=True)
    client = app.test_client()
    submit = client.post(
        "/api/requests",
        json=request_payload("r1", memory="2Gi", samples=2),
        query_string={"auto_admit": "1"},
    )
    assert submit.status_code == 201
    assert submit.get_json()["status"] == "ADMITTED"
    organized = client.post("/api/batches/organize", json={"strategy": "greedy"})
    assert organized.status_code == 200
    assert organized.get_json()["batch_count"] == 1
    scheduled = client.post(
        "/api/scheduler/rounds",
        json={"max_parallel_batches": 1, "time_budget_millis": "2s"},
    )
    assert scheduled.status_code == 200
    round_no = scheduled.get_json()["round_no"]
    completed = client.post(f"/api/scheduler/rounds/{round_no}/complete")
    assert completed.status_code == 200
    final = client.get("/api/requests/r1").get_json()
    assert final["status"] == "COMPLETED"
    assert final["reservation"]["active"] is False


def test_api_error_shape(tmp_path) -> None:
    samples = write_bundle(
        tmp_path / "samples",
        groups=[{"group_id": "g1", "memory_limit": "8Gi", "compute_limit": "10000m"}],
        nodes=[
            {
                "node_id": "n1",
                "total_memory": "20Gi",
                "available_compute": "10000m",
                "occupied_memory": "0",
                "quota_group": "g1",
                "healthy": True,
            }
        ],
    )
    app = create_app(tmp_path / "api.db", sample_dir=samples, bootstrap=True)
    client = app.test_client()
    assert client.post("/api/requests", json=request_payload("r1", memory="6Gi"), query_string={"auto_admit": "1"}).status_code == 201
    second = client.post("/api/requests", json=request_payload("r2", memory="6Gi"), query_string={"auto_admit": "1"})
    assert second.status_code == 409
    assert second.get_json()["error"]["code"] == "E_QUOTA_EXCEEDED"


def test_repository_demo_samples_run_to_completion(tmp_path) -> None:
    repo = Path(__file__).resolve().parents[1]
    service = QuotaScheduler(tmp_path / "demo.db")
    service.bootstrap(repo / "sample_data")
    loaded = service.load_sample_requests(repo / "sample_data")
    for request in loaded["requests"]:
        assert request["status"] == "PENDING"
    high = service.admit_request("req-vision-high")
    assert high["status"] == "ADMITTED"
    # Admit the rest after high priority is in place; one lower priority request
    # is preempted by the high-priority admission only when it was admitted
    # first.  Recreate the demo order for a clean deterministic assertion.
    service2 = QuotaScheduler(tmp_path / "demo2.db")
    service2.bootstrap(repo / "sample_data")
    service2.load_sample_requests(repo / "sample_data")
    for request_id in (
        "req-vision-a",
        "req-vision-b",
        "req-vision-c",
        "req-llm-a",
        "req-llm-b",
        "req-vision-high",
    ):
        service2.admit_request(request_id)
    assert service2.get_request("req-vision-c")["status"] == "PREEMPTED"
    batches = service2.organize_batches(strategy="auto")
    assert batches["batch_count"] == 5
    assert batches["errors"] == []
    for _ in range(5):
        if not service2.list_batches("PLANNED")["count"] and not service2.list_batches("QUEUED")["count"]:
            break
        scheduled = service2.schedule_round()
        service2.complete_round(scheduled["round_no"])
    assert service2.list_batches("COMPLETED")["count"] == 5
    assert service2.state_summary()["invariants"]["ok"] is True

