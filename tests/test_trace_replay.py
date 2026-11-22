from __future__ import annotations

from tests.conftest import spec


def test_trace_replay_uses_arrival_schedule_and_pool(engine_factory):
    engine = engine_factory(exec_timeout_s=1.0)
    engine.load_specs(
        {
            "functions": [
                spec(
                    "f",
                    init_us=10_000,
                    function_concurrency_limit=2,
                    per_instance_concurrency=1,
                    queue_capacity=2,
                )
            ]
        }
    )
    loaded = engine.load_trace(
        {
            "batch_id": "replay-batch",
            "calls": [
                {
                    "call_id": "r-1",
                    "function_id": "f",
                    "arrival_ms": 0,
                    "execution_us": 30_000,
                    "priority": 1,
                    "retry": False,
                },
                {
                    "call_id": "r-2",
                    "function_id": "f",
                    "arrival_ms": 1,
                    "execution_us": 30_000,
                    "priority": 2,
                    "retry": False,
                },
                {
                    "call_id": "r-3",
                    "function_id": "f",
                    "arrival_ms": 2,
                    "execution_us": 20_000,
                    "priority": 3,
                    "retry": True,
                },
                {
                    "call_id": "r-4",
                    "function_id": "f",
                    "arrival_ms": 3,
                    "execution_us": 20_000,
                    "priority": 0,
                    "retry": False,
                },
            ],
        }
    )
    trace = engine.get_trace(loaded["batch_id"])
    assert trace["record_count"] == 4
    assert len(trace["records"]) == 4
    replay = engine.replay_trace(loaded["batch_id"], time_scale=0.0, max_workers=4)
    assert replay["record_count"] == 4
    assert all(result["ok"] for result in replay["results"])
    assert replay["audit"]["ok"] is True
    assert engine.quota("f")[0]["on_demand_used"] == 0
