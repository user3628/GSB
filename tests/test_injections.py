from __future__ import annotations

from app.errors import (
    E_CONFLICTING_CALL,
    E_EXEC_TIMEOUT,
    E_QUOTA_DENIED,
    E_START_TIMEOUT,
    E_TRACE_OUT_OF_ORDER,
)
from tests.conftest import spec


def _scenarios():
    return [
        {
            "kind": "INSTANCE_CREATE_TIMEOUT",
            "function_id": "slow-init",
            "call_id": "inj-create",
            "params": {},
        },
        {
            "kind": "EXECUTION_INTERRUPT",
            "function_id": "quick",
            "call_id": "inj-exec",
            "params": {"execution_us": 500_000},
        },
        {
            "kind": "DUPLICATE_CALL_ID",
            "function_id": "quick",
            "call_id": "inj-dup",
            "params": {"execution_us": 10_000},
        },
        {
            "kind": "QUOTA_BURST",
            "function_id": "reserved",
            "call_id": "inj-quota",
            "params": {"execution_us": 120_000},
        },
        {
            "kind": "TRACE_OUT_OF_ORDER",
            "function_id": "quick",
            "call_id": "inj-trace",
            "params": {"arrival_ms": 100},
        },
    ]


def _load_injection_specs(engine):
    engine.load_specs(
        {
            "functions": [
                spec("slow-init", init_us=500_000, function_concurrency_limit=2, reserved_quota=1),
                spec("quick", function_concurrency_limit=2, reserved_quota=1, queue_capacity=4),
                spec(
                    "reserved",
                    function_concurrency_limit=3,
                    reserved_quota=2,
                    per_instance_concurrency=1,
                    queue_capacity=4,
                ),
            ]
        }
    )


def test_five_injections_have_distinct_codes_and_consistent_rechecks(engine_factory):
    engine = engine_factory(start_timeout_s=0.08, exec_timeout_s=0.08)
    _load_injection_specs(engine)
    results = [engine.run_injection(scenario) for scenario in _scenarios()]
    codes = {result["result_code"] for result in results}
    assert codes == {
        E_START_TIMEOUT,
        E_EXEC_TIMEOUT,
        E_CONFLICTING_CALL,
        E_QUOTA_DENIED,
        E_TRACE_OUT_OF_ORDER,
    }
    for result in results:
        recheck = engine.recheck_injection(result["injection_id"])
        assert recheck["consistent"] is True, recheck
        assert recheck["params"] == result["params"]
    events = engine.list_injections()
    assert len(events) == 5
    assert all(event["status"] == "COMPLETED" for event in events)
    assert engine.audit_invariants()["ok"] is True


def test_identical_injection_suite_replays_identically(engine_factory):
    summaries = []
    for _ in range(2):
        engine = engine_factory(start_timeout_s=0.08, exec_timeout_s=0.08)
        _load_injection_specs(engine)
        suite = []
        for scenario in _scenarios():
            result = engine.run_injection(scenario)
            recheck = engine.recheck_injection(result["injection_id"])
            suite.append(
                {
                    "kind": result["kind"],
                    "result_code": result["result_code"],
                    "consistent": recheck["consistent"],
                    "checks": recheck["checks"],
                }
            )
        summaries.append(suite)
    assert summaries[0] == summaries[1]


def test_repeating_injection_on_same_database_stays_consistent(engine_factory):
    engine = engine_factory(start_timeout_s=0.08, exec_timeout_s=0.08)
    _load_injection_specs(engine)
    first_pass = []
    second_pass = []
    for target in (first_pass, second_pass):
        for scenario in _scenarios():
            result = engine.run_injection(scenario)
            recheck = engine.recheck_injection(result["injection_id"])
            target.append(
                {
                    "kind": result["kind"],
                    "result_code": result["result_code"],
                    "consistent": recheck["consistent"],
                    "checks": recheck["checks"],
                }
            )
    assert first_pass == second_pass
    assert all(item["consistent"] for item in second_pass)

