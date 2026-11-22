from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from app.errors import BusinessError, E_INVALID_TRANSITION
from tests.conftest import call_status, spec, wait_for


def test_cold_then_warm_reuse(engine_factory):
    engine = engine_factory()
    engine.load_specs({"functions": [spec("f")]})
    first = engine.invoke({"call_id": "first", "function_id": "f", "execution_us": 10_000})
    second = engine.invoke({"call_id": "second", "function_id": "f", "execution_us": 10_000})
    assert first["cold_start"] is True
    assert first["warm_start"] is False
    assert first["cold_start_latency_us"] is not None
    assert first["cold_start_latency_us"] > 0
    assert first["detail"]["start_http_elapsed_us"] > 0
    assert second["cold_start"] is False
    assert second["warm_start"] is True
    assert second["cold_start_latency_us"] is None
    assert first["instance_id"] == second["instance_id"]
    runtime = engine.list_functions()[0]
    assert runtime["cold_starts"] == 1
    assert runtime["warm_starts"] == 1
    assert engine.audit_invariants()["ok"] is True


def test_prewarm_turns_first_call_warm(engine_factory):
    engine = engine_factory()
    engine.load_specs(
        {"functions": [spec("f", function_concurrency_limit=2, per_instance_concurrency=1, init_us=5_000)]}
    )
    prewarm = engine.prewarm("f", 1)
    assert prewarm["started"] == 1
    assert prewarm["outcomes"][0]["ok"] is True
    result = engine.invoke({"call_id": "warm-1", "function_id": "f", "execution_us": 5_000})
    assert result["warm_start"] is True
    runtime = engine.list_functions()[0]
    assert runtime["cold_starts"] == 0
    assert runtime["warm_starts"] == 1


def test_initialization_concurrency_limits_parallel_cold_starts(engine_factory):
    engine = engine_factory(exec_timeout_s=1.0)
    engine.load_specs(
        {
            "functions": [
                spec(
                    "f",
                    init_us=60_000,
                    init_concurrency=1,
                    function_concurrency_limit=2,
                    per_instance_concurrency=1,
                    queue_capacity=2,
                )
            ]
        }
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(
            engine.invoke,
            {"call_id": "cold-a", "function_id": "f", "execution_us": 5_000},
        )
        second = pool.submit(
            engine.invoke,
            {"call_id": "cold-b", "function_id": "f", "execution_us": 5_000},
        )
        first_result = first.result()
        second_result = second.result()
    assert first_result["cold_start"] is True
    assert second_result["cold_start"] is True
    waits = sorted(
        [
            first_result["detail"]["init_queue_wait_us"],
            second_result["detail"]["init_queue_wait_us"],
        ]
    )
    assert waits[0] < 20_000
    assert waits[1] >= 40_000
    assert max(
        first_result["cold_start_latency_us"], second_result["cold_start_latency_us"]
    ) >= 100_000


def test_invalid_transition_does_not_change_persisted_state(engine_factory):
    engine = engine_factory()
    engine.load_specs({"functions": [spec("f")]})
    call = engine.invoke({"call_id": "c", "function_id": "f", "execution_us": 5_000})
    instance_id = call["instance_id"]
    before = next(item for item in engine.list_instances("f") if item["instance_id"] == instance_id)
    assert before["state"] == "READY"
    with pytest.raises(BusinessError) as exc_info:
        engine.transition_instance(instance_id, "CREATING")
    assert exc_info.value.code == E_INVALID_TRANSITION
    after = next(item for item in engine.list_instances("f") if item["instance_id"] == instance_id)
    assert after["state"] == before["state"]
    assert after["occupied_slots"] == before["occupied_slots"]


def test_idle_drain_terminates_in_one_visible_commit(engine_factory):
    engine = engine_factory()
    engine.load_specs({"functions": [spec("f", function_concurrency_limit=2)]})
    call = engine.invoke({"call_id": "c", "function_id": "f", "execution_us": 5_000})
    drained = engine.drain_instance(call["instance_id"])
    assert drained["to"] == "TERMINATED"
    snapshot = engine.snapshot("f")
    instance = next(item for item in snapshot["instances"] if item["instance_id"] == call["instance_id"])
    assert instance["state"] == "TERMINATED"
    assert instance["occupied_slots"] == 0
    quota = snapshot["quota"][0]
    assert quota["on_demand_used"] == 0
    assert quota["reserved_used"] == 0
    assert engine.audit_invariants()["ok"] is True


def test_draining_instance_refuses_new_calls_and_terminates_after_inflight(engine_factory):
    engine = engine_factory(exec_timeout_s=1.0)
    engine.load_specs(
        {
            "functions": [
                spec(
                    "f",
                    function_concurrency_limit=2,
                    per_instance_concurrency=1,
                    queue_capacity=2,
                )
            ]
        }
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        long_call = pool.submit(
            engine.invoke,
            {"call_id": "inflight", "function_id": "f", "execution_us": 180_000},
        )
        wait_for(lambda: call_status(engine, "inflight") == "EXECUTING")
        draining_id = engine.get_call("inflight")["instance_id"]
        drained = engine.drain_instance(draining_id, reason="test-drain")
        assert drained["to"] == "DRAINING"
        draining = next(
            item for item in engine.list_instances("f") if item["instance_id"] == draining_id
        )
        assert draining["state"] == "DRAINING"
        assert draining["occupied_slots"] == 1
        next_call = engine.invoke(
            {"call_id": "after-drain", "function_id": "f", "execution_us": 10_000}
        )
        assert next_call["instance_id"] != draining_id
        assert next_call["cold_start"] is True
        assert long_call.result()["ok"] is True
    drained_after = next(
        item for item in engine.list_instances("f") if item["instance_id"] == draining_id
    )
    assert drained_after["state"] == "TERMINATED"
    assert drained_after["occupied_slots"] == 0
    assert engine.quota("f")[0]["on_demand_used"] == 0
    assert engine.audit_invariants()["ok"] is True

