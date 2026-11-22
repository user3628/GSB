from __future__ import annotations

import pytest

from quota_scheduler.errors import E_EXEC_QUEUE_FULL, SchedulerError

from conftest import make_service, request_payload


def _scheduler_service(tmp_path):
    return make_service(
        tmp_path,
        groups=[
            {"group_id": "g1", "memory_limit": "50Gi", "compute_limit": "100000m"},
            {"group_id": "g2", "memory_limit": "50Gi", "compute_limit": "100000m"},
        ],
        nodes=[
            {
                "node_id": "n1",
                "total_memory": "40Gi",
                "available_compute": "10000m",
                "occupied_memory": "0",
                "quota_group": "g1",
                "healthy": True,
            },
            {
                "node_id": "n2",
                "total_memory": "40Gi",
                "available_compute": "10000m",
                "occupied_memory": "0",
                "quota_group": "g2",
                "healthy": True,
            },
        ],
        models=[{"model_id": "m1", "max_batch_samples": 2, "unit_sample_cost": "100m"}],
        config={
            "batch_strategy": "greedy",
            "global_max_parallel_batches": 2,
            "round_time_budget": "2000ms",
            "execution_queue_capacity": 4,
        },
    )


def _three_batches(service):
    for payload in (
        request_payload("r1", node_id="n1", group="g1", samples=2, priority=9),
        request_payload("r2", node_id="n2", group="g2", samples=2, priority=9),
        request_payload("r3", node_id="n1", group="g1", samples=2, priority=8),
    ):
        service.submit_request(payload, auto_admit=True)
    report = service.organize_batches(strategy="greedy")
    assert report["batch_count"] == 3
    return report


def test_scheduler_concurrency_queue_and_same_round_idempotency(tmp_path) -> None:
    service = _scheduler_service(tmp_path)
    _three_batches(service)
    round1 = service.schedule_round(max_parallel_batches=1, queue_capacity=2)
    assert len(round1["selected"]) == 1
    assert len(round1["queued"]) == 2
    assert len(round1["skipped"]) == 2
    assert all(item["reason"].startswith("QUEUED_") for item in round1["skipped"])
    selected_batch = round1["selected"][0]["batch_id"]
    assert service.get_batch(selected_batch)["status"] == "RUNNING"
    running_request = service.list_requests("RUNNING")
    assert running_request["count"] == 1

    replay = service.schedule_round(round_no=round1["round_no"], max_parallel_batches=1)
    assert replay["replayed"] is True
    batch = service.get_batch(selected_batch)
    assert len(batch["dispatches"]) == 1

    completed = service.complete_round(round1["round_no"])
    assert len(completed["completed_requests"]) == 1
    round2 = service.schedule_round(max_parallel_batches=1, queue_capacity=2)
    assert round2["round_no"] == round1["round_no"] + 1
    assert len(round2["selected"]) == 1


def test_time_budget_queues_batch_that_would_overflow(tmp_path) -> None:
    service = _scheduler_service(tmp_path)
    _three_batches(service)
    result = service.schedule_round(
        max_parallel_batches=3,
        time_budget_millis="250ms",
        queue_capacity=4,
    )
    assert len(result["selected"]) == 1
    assert len(result["queued"]) == 2
    assert any(item["reason"] == "QUEUED_TIME_BUDGET" for item in result["skipped"])
    assert result["time_used_millis"] <= 250


def test_queue_full_rolls_back_entire_round(tmp_path) -> None:
    service = _scheduler_service(tmp_path)
    _three_batches(service)
    with pytest.raises(SchedulerError) as caught:
        service.schedule_round(max_parallel_batches=1, queue_capacity=1)
    assert caught.value.code == E_EXEC_QUEUE_FULL
    assert caught.value.details["overflow"]
    assert service.list_batches("RUNNING")["count"] == 0
    assert service.list_batches("QUEUED")["count"] == 0
    assert service.list_batches("PLANNED")["count"] == 3
    # No round was committed because queue overflow is an all-or-nothing failure.
    with pytest.raises(SchedulerError):
        service.get_round(1)


def test_preempted_batch_is_never_dispatched(tmp_path) -> None:
    service = _scheduler_service(tmp_path)
    report = _three_batches(service)
    victim_batch = report["batches"][0]["batch_id"]
    victim = report["batches"][0]["members"][0]["request_id"]
    service.preempt_request(victim, "test_preempt")
    result = service.schedule_round(max_parallel_batches=4, queue_capacity=4)
    dispatched = {item["batch_id"] for item in result["selected"]}
    assert victim_batch not in dispatched
    assert service.get_batch(victim_batch)["status"] == "PREEMPTED"


def _preemption_service(tmp_path):
    return make_service(
        tmp_path,
        groups=[{"group_id": "g1", "memory_limit": "10Gi", "compute_limit": "10000m"}],
        nodes=[
            {
                "node_id": "n1",
                "total_memory": "20Gi",
                "available_compute": "10000m",
                "occupied_memory": "2Gi",
                "occupied_compute": "0m",
                "quota_group": "g1",
                "healthy": True,
            }
        ],
        config={"preemption_enabled": True},
    )


def test_priority_admission_preempts_and_recycles_real_resources(tmp_path) -> None:
    service = _preemption_service(tmp_path)
    service.submit_request(request_payload("low", memory="6Gi", samples=2, priority=1), auto_admit=True)
    low_batch = service.organize_batches(strategy="greedy")["batches"][0]
    assert service.quota_snapshot()["nodes"]["n1"]["allocated_bytes"] == 6 * (1 << 30)

    high = service.submit_request(
        request_payload("high", memory="6Gi", samples=2, priority=9),
        auto_admit=True,
    )
    assert len(high["preemptions"]) == 1
    assert high["status"] == "ADMITTED"
    low = service.get_request("low")
    assert low["status"] == "PREEMPTED"
    assert low["reservation"]["active"] is False
    assert service.get_batch(low_batch["batch_id"])["status"] == "PREEMPTED"
    node = service.quota_snapshot()["nodes"]["n1"]
    assert node["allocated_bytes"] == 0
    usage = service.quota_snapshot()["quota_groups"]["g1"]
    assert usage["memory_used_bytes"] == 8 * (1 << 30)

    service.organize_batches(strategy="greedy")
    assert service.quota_snapshot()["nodes"]["n1"]["allocated_bytes"] == 6 * (1 << 30)
    released = service.preempt_request("high", "cleanup")
    assert released["quota_after"]["memory_used_bytes"] == 2 * (1 << 30)
    assert service.quota_snapshot()["nodes"]["n1"]["allocated_bytes"] == 0
    assert service.state_summary()["invariants"]["ok"] is True


def test_insufficient_preemption_leaves_everything_unchanged(tmp_path) -> None:
    service = make_service(
        tmp_path,
        groups=[{"group_id": "g1", "memory_limit": "12Gi", "compute_limit": "10000m"}],
        nodes=[
            {
                "node_id": "n1",
                "total_memory": "30Gi",
                "available_compute": "10000m",
                "occupied_memory": "2Gi",
                "quota_group": "g1",
                "healthy": True,
            }
        ],
    )
    service.submit_request(request_payload("low", memory="5Gi", priority=1), auto_admit=True)
    with pytest.raises(SchedulerError) as caught:
        service.submit_request(request_payload("too-high", memory="12Gi", priority=10), auto_admit=True)
    assert caught.value.code == "E_QUOTA_EXCEEDED"
    assert caught.value.details["request_id"] == "too-high"
    assert service.get_request("too-high")["status"] == "PENDING"
    assert service.get_request("too-high")["reservation"] is None
    assert service.get_request("low")["status"] == "ADMITTED"
    assert service.quota_snapshot()["quota_groups"]["g1"]["memory_used_bytes"] == 7 * (1 << 30)

