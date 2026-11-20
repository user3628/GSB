import pytest

from gpworkbench.errors import E_PREEMPTION_NOT_POSSIBLE, BusinessError
from gpworkbench.statemachine import ADMITTED, BATCHED, PREEMPTED, RUNNING


def test_preemption_releases_reservation_and_readmits(service):
    # r1/r2/r3 先占住 g1 配额，再让高优先级 r4 通过抢占进入
    for rid in ("r1", "r2", "r3"):
        service.admit_request(rid)
    usage_before = service.quota_snapshot("g1")[0]

    result = service.preempt_for("r4")
    assert result["state"] == ADMITTED
    assert result["preempted"], "应当至少抢占一个低优先级请求"

    r4 = service.repo.get_request("r4")
    assert r4["state"] == ADMITTED
    assert r4["reserved_memory"] > 0
    assert r4["reserved_compute"] > 0

    for victim in result["preempted"]:
        row = service.repo.get_request(victim["request_id"])
        assert row["state"] == PREEMPTED
        assert row["reserved_memory"] == 0
        assert row["reserved_compute"] == 0

    # 抢占前后：预留总量守恒（释放量 + 新预留），且组上限未被突破
    usage_after = service.quota_snapshot("g1")[0]
    assert usage_after["used_memory"] <= usage_after["memory_limit"]
    assert usage_after["used_compute"] <= usage_after["compute_limit"]
    assert usage_after["reserved_memory"] <= usage_before["reserved_memory"] + r4["memory_need"]

    # 被抢占请求的迁移事件带原因
    victim_id = result["preempted"][0]["request_id"]
    last_event = service.repo.list_events(victim_id)[-1]
    assert last_event["to_state"] == PREEMPTED
    assert last_event["reason"] == "preempted_by_higher_priority"


def test_preempted_request_can_be_readmitted_after_recycle(tmp_path):
    from gpworkbench.database import Database
    from gpworkbench.scheduler import SchedulerService
    from gpworkbench.spec import parse_workload

    payload = {
        "quota_groups": [
            {"group_id": "g", "memory_limit": "40Gi", "compute_limit": "1000m"}
        ],
        "nodes": [
            {
                "node_id": "n",
                "total_memory": "40Gi",
                "total_compute": "1000m",
                "used_memory": "0Gi",
                "group_id": "g",
                "health": "HEALTHY",
            }
        ],
        "models": [{"model_id": "m", "cost_per_sample": 1, "max_batch_samples": 8}],
        "requests": [
            {
                "request_id": "low",
                "service_name": "s",
                "model_id": "m",
                "memory_need": "20Gi",
                "compute_need": "300m",
                "samples_per_batch": 1,
                "latency_limit_ms": 10,
                "priority": 2,
                "group_id": "g",
            },
            {
                "request_id": "high",
                "service_name": "s",
                "model_id": "m",
                "memory_need": "25Gi",
                "compute_need": "400m",
                "samples_per_batch": 1,
                "latency_limit_ms": 10,
                "priority": 8,
                "group_id": "g",
            },
        ],
    }
    svc = SchedulerService(Database(tmp_path / "recycle.sqlite3"))
    svc.load_spec(parse_workload(payload))
    svc.admit_request("low")
    result = svc.preempt_for("high")
    victim_id = result["preempted"][0]["request_id"]
    svc.run_batching("g")
    high_batch = svc.repo.get_request("high")["batch_id"]
    assert high_batch is not None
    svc.start_batch(high_batch)
    svc.complete_batch(high_batch)
    readmit = svc.admit_request(victim_id)
    assert readmit["state"] == ADMITTED


def test_preemption_impossible_when_no_lower_priority_victim(tmp_path):
    from gpworkbench.database import Database
    from gpworkbench.scheduler import SchedulerService
    from gpworkbench.spec import parse_workload

    payload = {
        "quota_groups": [
            {"group_id": "g", "memory_limit": "12Gi", "compute_limit": "200m"}
        ],
        "nodes": [
            {
                "node_id": "n",
                "total_memory": "12Gi",
                "total_compute": "200m",
                "used_memory": "0Gi",
                "group_id": "g",
                "health": "HEALTHY",
            }
        ],
        "models": [{"model_id": "m", "cost_per_sample": 1, "max_batch_samples": 8}],
        "requests": [
            {
                "request_id": "low",
                "service_name": "s",
                "model_id": "m",
                "memory_need": "8Gi",
                "compute_need": "120m",
                "samples_per_batch": 1,
                "latency_limit_ms": 10,
                "priority": 9,
                "group_id": "g",
            },
            {
                "request_id": "high",
                "service_name": "s",
                "model_id": "m",
                "memory_need": "10Gi",
                "compute_need": "150m",
                "samples_per_batch": 1,
                "latency_limit_ms": 10,
                "priority": 1,
                "group_id": "g",
            },
        ],
    }
    svc = SchedulerService(Database(tmp_path / "preempt.sqlite3"))
    svc.load_spec(parse_workload(payload))
    svc.admit_request("low")
    # high 优先级反而更低，没有可抢占对象
    with pytest.raises(BusinessError) as exc:
        svc.preempt_for("high")
    assert exc.value.code == E_PREEMPTION_NOT_POSSIBLE
    assert svc.repo.get_request("high")["state"] == "PENDING"


def test_running_batch_is_not_preemptable(service):
    for rid in ("r1", "r2"):
        service.admit_request(rid)
    service.run_batching("g1")
    batch_id = service.repo.list_batches(state="PLACED")[0]["batch_id"]
    service.start_batch(batch_id)
    # RUNNING 状态请求不在抢占候选内；r4 找不到足够受害者应失败或不动在途批
    running_ids = [
        item["request_id"] for item in service.repo.list_batch_items(batch_id)
    ]
    try:
        service.preempt_for("r4")
    except BusinessError:
        pass
    for rid in running_ids:
        assert service.repo.get_request(rid)["state"] == RUNNING


def test_inflight_batch_shrinks_or_aborts_on_preemption(service):
    for rid in ("r1", "r2", "r3"):
        service.admit_request(rid)
    service.run_batching("g1")
    placed = service.repo.list_batches(state="PLACED")
    affected_batch_ids = {b["batch_id"] for b in placed}
    result = service.preempt_for("r4")
    touched = set(result["aborted_or_shrunk_batches"])
    assert touched & affected_batch_ids
    for batch_id in touched:
        batch = service.repo.get_batch(batch_id)
        items = service.repo.list_batch_items(batch_id)
        if not items:
            assert batch["state"] == "ABORTED"
        else:
            # 存活成员仍处于 BATCHED，且批资源已重算
            assert batch["state"] == "PLACED"
            assert batch["total_memory"] == sum(
                service.repo.get_request(i["request_id"])["memory_need"] for i in items
            )
