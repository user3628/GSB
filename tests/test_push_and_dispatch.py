import json
import threading
from pathlib import Path

import pytest

from config_dispatch.errors import (
    EndpointConnectionError,
    EndpointRejectedError,
    EndpointTimeoutError,
)

SAMPLES_PATH = Path(__file__).resolve().parent.parent / "samples"


def _prepare(coordinator, declarations_raw, snapshot_payload, version=1):
    coordinator.register_declarations(declarations_raw)
    coordinator.submit_snapshot(snapshot_payload(version))
    return coordinator.ensure_plan(version)


def test_full_dispatch_completes_in_topological_order(
    coordinator, declarations_raw, snapshot_payload, success_script, cluster_factory
):
    plan = _prepare(coordinator, declarations_raw, snapshot_payload)
    cluster_factory(declarations_raw, success_script)
    run = coordinator.dispatch(1, failure_policy="rollback")
    assert run["status"] == "COMPLETED"
    states = {row["component_id"]: row["state"] for row in coordinator.repo.list_component_states(1)}
    assert set(states) == set(plan["ordered_components"])
    assert all(state == "APPLIED" for state in states.values())
    # 所有状态记录都携带其依据的快照版本
    assert all(row["version"] == 1 for row in coordinator.repo.list_component_states(1))


def test_three_distinct_push_failure_codes(
    coordinator, declarations_raw, cluster_factory
):
    from config_dispatch.models import compute_checksum

    script = {
        item["component_id"]: {
            "/apply": {"behavior": "success"},
            "/rollback": {"behavior": "success"},
        }
        for item in declarations_raw
    }
    script_path = SAMPLES_PATH / "endpoint_with_failures.json"
    overrides = json.loads(script_path.read_text(encoding="utf-8"))
    effective = []
    for item in declarations_raw:
        cloned = dict(item)
        override = overrides[item["component_id"]]
        if "endpoint_override" in override:
            cloned["endpoint"] = override["endpoint_override"]
        effective.append(cloned)
        if override.get("/apply", {}).get("behavior") != "success" or "endpoint_override" in override:
            script[item["component_id"]] = override
    coordinator.register_declarations(effective)
    # v1 先作为基线提交，v2 改变多个键使全部组件进入受影响集合
    coordinator.submit_snapshot(json.loads((SAMPLES_PATH / "snapshot_v1.json").read_text(encoding="utf-8")))
    coordinator.submit_snapshot(json.loads((SAMPLES_PATH / "snapshot_v2.json").read_text(encoding="utf-8")))
    coordinator.ensure_plan(2)
    cluster_factory(effective, script)

    run = coordinator.dispatch(2, failure_policy="continue")
    assert run["status"] == "PARTIAL"
    error_codes = {
        row["component_id"]: row["error_code"]
        for row in coordinator.repo.list_component_states(2)
        if row["state"] == "FAILED"
    }
    assert error_codes["cache-proxy"] == "E_PUSH_REJECTED"
    # inventory-service 连接失败后，其下游 order-service/api-gateway 在 continue
    # 策略下仍会尝试；连接失败事实由 inventory-service 或其下游（同样指向失效
    # 端点的场景下）体现，这里至少验证三类互斥业务码均出现
    assert "E_PUSH_CONNECTION_FAILED" in set(error_codes.values())
    assert error_codes["api-gateway"] == "E_PUSH_TIMEOUT"


def test_timeout_closes_connection(coordinator, declarations_raw, cluster_factory):
    from config_dispatch.endpoint_server import parse_endpoint

    target = next(item for item in declarations_raw if item["component_id"] == "audit-sink")
    script = {
        target["component_id"]: {
            "/apply": {"behavior": "delay", "delay_seconds": 5},
            "/rollback": {"behavior": "success"},
        }
    }
    cluster = cluster_factory(declarations_raw, script)
    client = coordinator.client
    payload = {"component_id": target["component_id"], "version": 1, "idempotency_key": "k1"}
    import time

    begin = time.monotonic()
    with pytest.raises(EndpointTimeoutError) as exc:
        client.post(target["endpoint"], "/apply", payload)
    elapsed = time.monotonic() - begin
    assert exc.value.code == "E_PUSH_TIMEOUT"
    assert elapsed < 3  # 没有以等待 5 秒冒充终止
    host, port = parse_endpoint(target["endpoint"])
    # 连接已释放：可以立即重新连上同一端点
    import http.client

    connection = http.client.HTTPConnection(host, port, timeout=2)
    connection.request("GET", "/health")
    assert connection.getresponse().status == 200
    connection.close()
    assert cluster.request_counts[target["component_id"]]["/apply"] >= 1


def test_connection_refused_is_distinct(coordinator, declarations_raw):
    target = next(item for item in declarations_raw if item["component_id"] == "audit-sink")
    with pytest.raises(EndpointConnectionError) as exc:
        coordinator.client.post(
            "http://127.0.0.1:18099",
            "/apply",
            {"component_id": target["component_id"], "version": 1, "idempotency_key": "k2"},
        )
    assert exc.value.code == "E_PUSH_CONNECTION_FAILED"


def test_rejection_is_distinct(coordinator, declarations_raw, cluster_factory):
    target = next(item for item in declarations_raw if item["component_id"] == "audit-sink")
    script = {
        target["component_id"]: {
            "/apply": {"behavior": "reject", "reason": "nope"},
            "/rollback": {"behavior": "success"},
        }
    }
    cluster_factory(declarations_raw, script)
    with pytest.raises(EndpointRejectedError) as exc:
        coordinator.client.post(
            target["endpoint"],
            "/apply",
            {"component_id": target["component_id"], "version": 1, "idempotency_key": "k3"},
        )
    assert exc.value.code == "E_PUSH_REJECTED"


def test_rollback_policy_converges_all_components(
    coordinator, declarations_raw, snapshot_payload, cluster_factory
):
    script = {
        item["component_id"]: {
            "/apply": {"behavior": "success"},
            "/rollback": {"behavior": "success"},
        }
        for item in declarations_raw
    }
    script["api-gateway"] = {
        "/apply": {"behavior": "delay", "delay_seconds": 5},
        "/rollback": {"behavior": "success"},
    }
    _prepare(coordinator, declarations_raw, snapshot_payload)
    cluster_factory(declarations_raw, script)
    run = coordinator.dispatch(1, failure_policy="rollback")
    assert run["status"] == "ROLLED_BACK"
    states = coordinator.repo.list_component_states(1)
    assert not any(row["state"] == "APPLIED" for row in states)
    assert any(row["state"] == "ROLLED_BACK" for row in states)
    # 回滚的组件其状态记录仍绑定同一快照版本
    assert all(row["version"] == 1 for row in states)


def test_concurrent_duplicate_dispatch_single_flight(
    coordinator, declarations_raw, snapshot_payload, success_script, cluster_factory
):
    _prepare(coordinator, declarations_raw, snapshot_payload)
    cluster = cluster_factory(declarations_raw, success_script)
    outcomes = []
    errors = []

    def runner():
        try:
            outcomes.append(coordinator.dispatch(1, failure_policy="continue"))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=runner) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    assert len(outcomes) == 4
    assert all(item["status"] == "COMPLETED" for item in outcomes)
    planned = coordinator.ensure_plan(1)["ordered_components"]
    total_hits = sum(
        count.get("/apply", 0)
        for count in cluster.request_counts.values()
    )
    assert total_hits == len(planned)
