from __future__ import annotations

import pytest

from quota_scheduler.errors import E_BATCH_TOO_LARGE, SchedulerError

from conftest import make_service, request_payload


def _batch_service(tmp_path, **config):
    base_config = {
        "batch_strategy": "auto",
        "batch_strategy_threshold": 2,
        "global_max_parallel_batches": 2,
        "round_time_budget": "2000ms",
        "execution_queue_capacity": 8,
    }
    base_config.update(config)
    return make_service(
        tmp_path,
        groups=[{"group_id": "g1", "memory_limit": "80Gi", "compute_limit": "100000m"}],
        nodes=[
            {
                "node_id": "n1",
                "total_memory": "60Gi",
                "available_compute": "20000m",
                "occupied_memory": "0Gi",
                "occupied_compute": "0m",
                "quota_group": "g1",
                "healthy": True,
            }
        ],
        models=[{"model_id": "m1", "max_batch_samples": 8, "unit_sample_cost": "100m"}],
        config=base_config,
    )


def _admit_all(service, payloads):
    for payload in payloads:
        service.submit_request(payload, auto_admit=True)


def test_adjacent_same_model_node_requests_share_batch(tmp_path) -> None:
    service = _batch_service(tmp_path)
    _admit_all(
        service,
        [
            request_payload("r1", memory="2Gi", samples=3, latency="1000ms", priority=5),
            request_payload("r2", memory="2Gi", samples=4, latency="1000ms", priority=4),
            request_payload("r3", memory="2Gi", samples=2, latency="1000ms", priority=3),
        ],
    )
    report = service.organize_batches(strategy="greedy")
    assert report["ok"] is True
    assert report["batch_count"] == 2
    first = report["batches"][0]
    assert {member["request_id"] for member in first["members"]} == {"r1", "r2"}
    assert first["total_samples"] == 7
    assert first["total_samples"] <= 8
    assert first["estimated_latency_millis"] == 700
    assert all(item["status"] == "BATCHED" for item in service.list_requests()["requests"])


def test_batch_split_preserves_samples_and_memory(tmp_path) -> None:
    service = make_service(
        tmp_path,
        groups=[{"group_id": "g1", "memory_limit": "50Gi", "compute_limit": "100000m"}],
        nodes=[
            {
                "node_id": "n1",
                "total_memory": "40Gi",
                "available_compute": "10000m",
                "occupied_memory": "0Gi",
                "quota_group": "g1",
                "healthy": True,
            }
        ],
        models=[{"model_id": "m1", "max_batch_samples": 4, "unit_sample_cost": "100m"}],
    )
    service.submit_request(
        request_payload("r1", memory="8Gi", samples=10, latency="2000ms"),
        auto_admit=True,
    )
    report = service.organize_batches(strategy="greedy")
    assert report["ok"] is True
    assert report["batch_count"] == 3
    members = [member for batch in report["batches"] for member in batch["members"]]
    assert sum(member["samples"] for member in members) == 10
    assert sum(member["memory_bytes"] for member in members) == 8 * (1 << 30)
    assert all(batch["total_samples"] <= 4 for batch in report["batches"])
    assert len([a for a in report["adjustments"] if a["type"] == "sample_split"]) == 3
    assert all(a["total_samples_preserved"] for a in report["adjustments"])
    node = service.quota_snapshot()["nodes"]["n1"]
    assert node["allocated_bytes"] == 8 * (1 << 30)


def test_latency_limit_splits_request_without_losing_samples(tmp_path) -> None:
    service = _batch_service(tmp_path)
    service.submit_request(
        request_payload("r1", memory="5Gi", samples=10, latency="250ms"),
        auto_admit=True,
    )
    report = service.organize_batches(strategy="greedy")
    assert report["ok"] is True
    members = [member for batch in report["batches"] for member in batch["members"]]
    assert sum(member["samples"] for member in members) == 10
    assert len(report["batches"]) == 5
    assert all(batch["estimated_latency_millis"] <= 250 for batch in report["batches"])
    assert any("latency_limit" in a.get("constraints", []) for a in report["adjustments"])


def test_single_sample_over_latency_returns_batch_too_large(tmp_path) -> None:
    service = _batch_service(tmp_path)
    service.submit_request(
        request_payload("r1", memory="1Gi", samples=1, latency="50ms"),
        auto_admit=True,
    )
    report = service.organize_batches(strategy="greedy")
    assert report["ok"] is False
    assert report["errors"][0]["code"] == E_BATCH_TOO_LARGE
    assert report["batch_count"] == 0
    view = service.get_request("r1")
    assert view["status"] == "ADMITTED"
    assert view["reservation"]["active"] is True
    assert view["batches"] == []

    with pytest.raises(SchedulerError) as caught:
        service.organize_batches(strategy="greedy", strict=True)
    assert caught.value.code == E_BATCH_TOO_LARGE


def test_latency_cap_prevents_overlarge_merge(tmp_path) -> None:
    service = _batch_service(tmp_path)
    _admit_all(
        service,
        [
            request_payload("r1", samples=3, latency="300ms", priority=5),
            request_payload("r2", samples=3, latency="1000ms", priority=4),
        ],
    )
    report = service.organize_batches(strategy="greedy")
    assert report["batch_count"] == 2
    assert all(batch["estimated_latency_millis"] <= 300 for batch in report["batches"])


def test_auto_strategy_selects_memory_descending_when_fewer_batches(tmp_path) -> None:
    service = _batch_service(tmp_path)
    _admit_all(
        service,
        [
            request_payload("r1", memory="8Gi", samples=4, latency="1000ms", priority=5),
            request_payload("r2", memory="9Gi", samples=6, latency="1000ms", priority=4),
            request_payload("r3", memory="2Gi", samples=3, latency="1000ms", priority=3),
        ],
    )
    report = service.organize_batches(strategy="auto")
    assert report["strategy_selected"] == ["memory_descending"]
    assert report["batch_count"] == 2
    assert any(
        candidate["strategy"] == "greedy_submission"
        for candidate in report["strategy_candidates"]
    )


def test_unhealthy_node_is_not_used(tmp_path) -> None:
    service = make_service(
        tmp_path,
        groups=[{"group_id": "g1", "memory_limit": "100Gi", "compute_limit": "100000m"}],
        nodes=[
            {
                "node_id": "n1",
                "total_memory": "40Gi",
                "available_compute": "10000m",
                "occupied_memory": "0",
                "quota_group": "g1",
                "healthy": False,
            }
        ],
    )
    service.submit_request(request_payload("r1", node_id=None), auto_admit=True)
    report = service.organize_batches()
    assert report["ok"] is False
    assert report["errors"][0]["code"] == "E_NODE_UNHEALTHY"
    assert service.get_request("r1")["status"] == "ADMITTED"

