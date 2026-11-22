from __future__ import annotations

from tests.conftest import spec


def _run_replay(engine_factory):
    engine = engine_factory()
    engine.load_specs(
        {
            "functions": [
                spec("a", function_concurrency_limit=2, per_instance_concurrency=1),
                spec("b", function_concurrency_limit=1, per_instance_concurrency=1, queue_capacity=2),
            ]
        }
    )
    calls = [
        ("a-1", "a", 5_000, 1, False),
        ("b-1", "b", 5_000, 2, False),
        ("a-2", "a", 5_000, 0, True),
        ("b-2", "b", 5_000, 3, False),
        ("a-3", "a", 5_000, 0, False),
    ]
    results = []
    for call_id, function_id, execution_us, priority, retry in calls:
        result = engine.invoke(
            {
                "call_id": call_id,
                "function_id": function_id,
                "execution_us": execution_us,
                "priority": priority,
                "retry": retry,
            }
        )
        results.append(
            {
                "call_id": call_id,
                "business_code": result["business_code"],
                "status": result["status"],
                "cold_start": result["cold_start"],
                "warm_start": result["warm_start"],
                "quota_bucket": result["quota_bucket"],
            }
        )
    runtime = {
        row["function_id"]: {
            "cold_starts": row["cold_starts"],
            "warm_starts": row["warm_starts"],
            "start_timeouts": row["start_timeouts"],
            "exec_timeouts": row["exec_timeouts"],
        }
        for row in engine.list_functions()
    }
    return {"results": results, "runtime": runtime}


def test_identical_seed_replay_has_identical_decisions(engine_factory):
    first = _run_replay(engine_factory)
    second = _run_replay(engine_factory)
    assert first == second
    assert first["runtime"]["a"] == {
        "cold_starts": 1,
        "warm_starts": 2,
        "start_timeouts": 0,
        "exec_timeouts": 0,
    }

