import pytest

from gpworkbench.errors import (
    E_BATCH_TOO_LARGE,
    E_INVALID_TRANSITION,
    E_QUOTA_EXCEEDED,
    BusinessError,
)
from gpworkbench.scheduler import BATCH_STRATEGY_THRESHOLD
from gpworkbench.statemachine import (
    ADMITTED,
    BATCHED,
    COMPLETED,
    PENDING,
    REJECTED,
    RUNNING,
)


def test_request_starts_pending(service):
    request = service.repo.get_request("r1")
    assert request["state"] == PENDING
    assert request["reserved_memory"] == 0


def test_admit_reserves_quota_in_one_transaction(service):
    before = service.quota_snapshot("g1")[0]
    result = service.admit_request("r1")
    assert result["state"] == ADMITTED
    after = service.quota_snapshot("g1")[0]
    r1 = service.repo.get_request("r1")
    assert after["reserved_memory"] - before["reserved_memory"] == r1["memory_need"]
    assert after["reserved_compute"] - before["reserved_compute"] == r1["compute_need"]
    events = service.repo.list_events("r1")
    assert events[-1]["to_state"] == ADMITTED
    assert events[-1]["reason"] == "quota_admitted"


def test_quota_exceeded_reports_usage_and_remaining(service):
    # r4 需要 30Gi 显存 + 900 算力，g1 上限 100Gi/1000，先占满
    service.admit_request("r1")
    service.admit_request("r2")
    service.admit_request("r3")
    with pytest.raises(BusinessError) as exc:
        service.admit_request("r4")
    assert exc.value.code == E_QUOTA_EXCEEDED
    detail = exc.value.details
    assert "usage" in detail
    assert (
        detail["usage"]["remaining_memory"] < detail["need_memory"]
        or detail["usage"]["remaining_compute"] < detail["need_compute"]
    )
    # 被拒请求保持 PENDING，且未产生预留
    assert service.repo.get_request("r4")["state"] == PENDING
    assert service.repo.get_request("r4")["reserved_memory"] == 0


def test_invalid_transition_keeps_original_state(service):
    service.admit_request("r1")
    with pytest.raises(BusinessError) as exc:
        # ADMITTED 不能直接 COMPLETED
        service.transition("r1", COMPLETED, "illegal")
    assert exc.value.code == E_INVALID_TRANSITION
    assert service.repo.get_request("r1")["state"] == ADMITTED


def test_reject_transition_and_zero_reservation(service):
    service.admit_request("r1")
    service.reject_request("r2")
    r2 = service.repo.get_request("r2")
    assert r2["state"] == REJECTED
    assert r2["reserved_memory"] == 0
    with pytest.raises(BusinessError) as exc:
        service.reject_request("r2")  # 终态不可再迁移
    assert exc.value.code == E_INVALID_TRANSITION


def test_batching_groups_same_model_and_respects_node_memory(service):
    for rid in ("r1", "r2", "r3"):
        service.admit_request(rid)
    report = service.run_batching("g1")
    group_report = report["groups"][0]
    assert group_report["strategy"] == "GREEDY_FF"
    assert group_report["batches_created"] >= 1
    batches = service.repo.list_batches(state="PLACED")
    # 同模型同节点才会同批
    for batch in batches:
        items = service.repo.list_batch_items(batch["batch_id"])
        models = {
            service.repo.get_request(i["request_id"])["model_id"] for i in items
        }
        assert len(models) == 1
    states = {rid: service.repo.get_request(rid)["state"] for rid in ("r1", "r2", "r3")}
    assert all(state == BATCHED for state in states.values())
    # 节点批占用合计不得超过节点剩余显存
    for node in service.node_snapshot():
        assert node["free_memory"] >= 0


def test_batch_latency_limit_enforced_and_split(service):
    # r1 延迟上限 100，m1 单样本代价 10；构造一个加入后超限必须拆批的场景
    for rid in ("r1", "r2"):
        service.admit_request(rid)
    report = service.run_batching("g1")
    for batch in report["groups"][0]["batches"]:
        assert batch["estimated_latency_ms"] <= 80 or batch["model_id"] != "m1"
    # 所有已形成批的延迟均不超过批内最严上限
    for batch in service.repo.list_batches():
        items = service.repo.list_batch_items(batch["batch_id"])
        strictest = min(
            service.repo.get_request(i["request_id"])["latency_limit_ms"] for i in items
        )
        assert batch["estimated_latency_ms"] <= strictest


def test_oversized_single_request_returns_batch_too_large(tmp_path):
    from gpworkbench.database import Database
    from gpworkbench.scheduler import SchedulerService
    from gpworkbench.spec import parse_workload

    payload = {
        "quota_groups": [
            {"group_id": "g", "memory_limit": "100Gi", "compute_limit": "1000m"}
        ],
        "nodes": [
            {
                "node_id": "n",
                "total_memory": "100Gi",
                "total_compute": "1000m",
                "used_memory": "0Gi",
                "group_id": "g",
                "health": "HEALTHY",
            }
        ],
        "models": [{"model_id": "m", "cost_per_sample": 10, "max_batch_samples": 8}],
        "requests": [
            {
                "request_id": "big",
                "service_name": "s",
                "model_id": "m",
                "memory_need": "4Gi",
                "compute_need": "10m",
                "samples_per_batch": 8,
                "latency_limit_ms": 50,  # 8*10=80 > 50
                "priority": 1,
                "group_id": "g",
            }
        ],
    }
    svc = SchedulerService(Database(tmp_path / "lat.sqlite3"))
    svc.load_spec(parse_workload(payload))
    svc.admit_request("big")
    report = svc.run_batching("g")
    assert report["oversized"] == ["big"]
    assert svc.repo.get_request("big")["state"] == ADMITTED


def test_start_and_complete_batch_releases_reservation(service):
    for rid in ("r1", "r2", "r3"):
        service.admit_request(rid)
    service.run_batching("g1")
    batch_id = service.repo.list_batches(state="PLACED")[0]["batch_id"]
    service.start_batch(batch_id)
    assert service.repo.get_batch(batch_id)["state"] == "RUNNING"
    items = service.repo.list_batch_items(batch_id)
    for item in items:
        assert service.repo.get_request(item["request_id"])["state"] == RUNNING
    usage_before = service.quota_snapshot("g1")[0]
    service.complete_batch(batch_id)
    usage_after = service.quota_snapshot("g1")[0]
    assert usage_after["reserved_memory"] <= usage_before["reserved_memory"]
    for item in items:
        request = service.repo.get_request(item["request_id"])
        assert request["state"] == COMPLETED
        assert request["reserved_memory"] == 0
        assert request["reserved_compute"] == 0


def test_duplicate_submission_is_idempotent(tmp_path):
    from gpworkbench.database import Database
    from gpworkbench.scheduler import SchedulerService
    from gpworkbench.spec import RequestSpec

    svc = SchedulerService(Database(tmp_path / "idem.sqlite3"))
    spec = RequestSpec(
        request_id="dup-1",
        service_name="s",
        model_id="m1",
        memory_need=1024,
        compute_need=10,
        samples_per_batch=1,
        latency_limit_ms=100,
        priority=1,
        group_id="g1",
        seq=99,
    )
    first = svc.submit_request(spec)
    second = svc.submit_request(spec)
    assert first["deduplicated"] is False
    assert second["deduplicated"] is True
    assert second["state"] == PENDING
    assert len(svc.repo.list_requests()) == 1


def test_strategy_switches_above_threshold(service):
    assert service._choose_strategy(10, None) == "GREEDY_FF"
    assert service._choose_strategy(BATCH_STRATEGY_THRESHOLD + 1, None) == "MEM_DESC_BF"
    assert service._choose_strategy(10, "MEM_DESC_BF") == "MEM_DESC_BF"
