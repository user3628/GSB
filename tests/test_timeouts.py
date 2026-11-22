from __future__ import annotations

from dataclasses import replace

from app.config import Settings
from app.engine import RuntimeEngine
from app.errors import E_EXEC_TIMEOUT, E_START_FAILED, E_START_TIMEOUT
from tests.conftest import spec, wait_for


def test_create_timeout_closes_connection_and_releases_everything(
    engine_factory, endpoint_server
):
    engine = engine_factory(start_timeout_s=0.08)
    engine.load_specs({"functions": [spec("slow", init_us=500_000)]})
    before = endpoint_server.stats()["hang_disconnects"]
    result = engine.invoke(
        {
            "call_id": "create-timeout",
            "function_id": "slow",
            "execution_us": 1_000,
            "injection": {"create_timeout": True},
        }
    )
    assert result["business_code"] == E_START_TIMEOUT
    assert result["status"] == "FAILED"
    assert result["detail"]["timed_out"] is True
    assert result["detail"]["elapsed_us"] >= 70_000
    instances = engine.list_instances("slow")
    assert len(instances) == 1
    assert instances[0]["state"] == "TERMINATED"
    assert instances[0]["occupied_slots"] == 0
    quota = engine.quota("slow")[0]
    assert quota["on_demand_used"] == 0
    assert quota["reserved_used"] == 0
    assert engine.list_functions()[0]["start_timeouts"] == 1
    wait_for(
        lambda: endpoint_server.stats()["hang_disconnects"] > before,
        timeout=2.0,
    )
    assert engine.audit_invariants()["ok"] is True


def test_execution_timeout_releases_quota_and_slot(engine_factory):
    engine = engine_factory(exec_timeout_s=0.08)
    engine.load_specs(
        {"functions": [spec("f", function_concurrency_limit=2, queue_capacity=2)]}
    )
    result = engine.invoke(
        {
            "call_id": "exec-timeout",
            "function_id": "f",
            "execution_us": 500_000,
            "injection": {"execution_interrupt": True},
        }
    )
    assert result["business_code"] == E_EXEC_TIMEOUT
    assert result["status"] == "FAILED"
    assert result["detail"]["timed_out"] is True
    instance = next(
        item
        for item in engine.list_instances("f")
        if item["instance_id"] == result["instance_id"]
    )
    assert instance["state"] == "READY"
    assert instance["occupied_slots"] == 0
    quota = engine.quota("f")[0]
    assert quota["on_demand_used"] == 0
    assert quota["reserved_used"] == 0
    assert engine.list_functions()[0]["exec_timeouts"] == 1
    assert engine.audit_invariants()["ok"] is True


def test_actively_refused_connection_is_not_reported_as_timeout(engine_factory, endpoint_server):
    base = engine_factory()
    settings = replace(base.settings, endpoint_base=endpoint_server.refuse_url)
    engine = RuntimeEngine(settings)
    engine.load_specs({"functions": [spec("f")]})
    result = engine.invoke(
        {"call_id": "refused", "function_id": "f", "execution_us": 1_000}
    )
    assert result["business_code"] == E_START_FAILED
    assert result["detail"]["timed_out"] is False
    assert engine.list_instances("f")[0]["state"] == "TERMINATED"
    assert engine.quota("f")[0]["on_demand_used"] == 0
    assert engine.audit_invariants()["ok"] is True

