from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from quota_scheduler.errors import (
    E_INVALID_TRANSITION,
    E_QUOTA_EXCEEDED,
    SchedulerError,
)

from conftest import make_service, request_payload


def _small_group_service(tmp_path):
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
    )


def test_quota_admission_and_no_partial_reservation(tmp_path) -> None:
    service = _small_group_service(tmp_path)
    first = service.submit_request(request_payload("r1", memory="5Gi", samples=1))
    admitted = service.admit_request(first["request_id"])
    assert admitted["status"] == "ADMITTED"
    assert admitted["reservation"]["active"] is True

    second = service.submit_request(request_payload("r2", memory="4Gi", samples=1))
    with pytest.raises(SchedulerError) as caught:
        service.admit_request(second["request_id"])
    error = caught.value
    assert error.code == E_QUOTA_EXCEEDED
    assert error.details["memory"]["current_occupancy_bytes"] == 2 * (1 << 30)
    assert error.details["memory"]["reserved_bytes"] == 5 * (1 << 30)
    assert error.details["memory"]["request_bytes"] == 4 * (1 << 30)

    pending = service.get_request("r2")
    assert pending["status"] == "PENDING"
    assert pending["reservation"] is None
    usage = service.quota_snapshot()["quota_groups"]["g1"]
    assert usage["reserved_bytes"] == 5 * (1 << 30)


def test_compute_quota_exceeded(tmp_path) -> None:
    service = make_service(
        tmp_path,
        groups=[{"group_id": "g1", "memory_limit": "100Gi", "compute_limit": "500m"}],
        models=[{"model_id": "m1", "max_batch_samples": 8, "unit_sample_cost": "100m"}],
    )
    service.submit_request(request_payload("r1", memory="1Gi", samples=3))
    service.admit_request("r1")
    service.submit_request(request_payload("r2", memory="1Gi", samples=3))
    with pytest.raises(SchedulerError) as caught:
        service.admit_request("r2")
    assert caught.value.code == E_QUOTA_EXCEEDED
    assert caught.value.details["compute"]["request_millis"] == 300
    assert caught.value.details["compute"]["reserved_millis"] == 300
    assert service.get_request("r2")["reservation"] is None


def test_invalid_state_transition_is_atomic(tmp_path) -> None:
    service = _small_group_service(tmp_path)
    service.submit_request(request_payload("r1"))
    before = service.get_request("r1")
    with pytest.raises(SchedulerError) as caught:
        service.transition_request("r1", "BATCHED", "illegal_shortcut")
    assert caught.value.code == E_INVALID_TRANSITION
    after = service.get_request("r1")
    assert after["status"] == "PENDING"
    assert after["state_history"] == before["state_history"]
    assert after["reservation"] is None


def test_state_history_records_before_after_reason_and_op_seq(tmp_path) -> None:
    service = _small_group_service(tmp_path)
    service.submit_request(request_payload("r1"))
    service.transition_request("r1", "ADMITTED", "manual_admit")
    view = service.get_request("r1")
    transitions = view["state_history"]
    assert transitions[0]["from_state"] is None
    assert transitions[0]["to_state"] == "PENDING"
    assert transitions[1]["from_state"] == "PENDING"
    assert transitions[1]["to_state"] == "ADMITTED"
    assert transitions[1]["reason"] == "manual_admit"
    assert all(item["op_seq"] > 0 for item in transitions)


def test_duplicate_submission_is_idempotent_under_concurrency(tmp_path) -> None:
    service = _small_group_service(tmp_path)
    payload = request_payload("same", memory="2Gi", samples=1)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: service.submit_request(payload), range(8)))
    assert sum(1 for result in results if result["created"]) == 1
    assert sum(1 for result in results if result["idempotent"]) == 7
    assert service.list_requests()["count"] == 1
    assert len(service.get_request("same")["state_history"]) == 1


def test_duplicate_auto_admit_only_reserves_once(tmp_path) -> None:
    service = _small_group_service(tmp_path)
    payload = request_payload("same-auto", memory="2Gi", samples=1)
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(
            lambda _: service.submit_request(payload, auto_admit=True), range(6)
        ))
    assert service.get_request("same-auto")["status"] == "ADMITTED"
    usage = service.quota_snapshot()["quota_groups"]["g1"]
    assert usage["reserved_bytes"] == 2 * (1 << 30)
    assert sum(1 for result in results if result["created"]) == 1


def test_running_request_can_complete_through_transition_api(tmp_path) -> None:
    service = _small_group_service(tmp_path)
    service.submit_request(request_payload("r1", memory="2Gi", samples=2), auto_admit=True)
    service.organize_batches(strategy="greedy")
    service.schedule_round(max_parallel_batches=1)
    result = service.transition_request("r1", "COMPLETED", "manual_completion")
    assert result["status"] == "COMPLETED"
    assert result["reservation"]["active"] is False
    assert len(result["completed_batches"]) == 1
    assert service.state_summary()["invariants"]["ok"] is True


def test_preempted_request_can_return_to_pending_and_readmit(tmp_path) -> None:
    service = _small_group_service(tmp_path)
    service.submit_request(request_payload("r1", memory="2Gi"), auto_admit=True)
    service.preempt_request("r1", "retry_test")
    assert service.get_request("r1")["status"] == "PREEMPTED"
    service.transition_request("r1", "PENDING", "retry_requested")
    readmitted = service.admit_request("r1")
    assert readmitted["status"] == "ADMITTED"
    assert readmitted["reservation"]["active"] is True
    assert service.quota_snapshot()["quota_groups"]["g1"]["reserved_bytes"] == 2 * (1 << 30)

