from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.errors import (
    BusinessError,
    E_CONCURRENCY_LIMIT,
    E_CONFLICTING_CALL,
    E_NOT_FOUND,
    E_QUOTA_DENIED,
)
from tests.conftest import spec, wait_for


def _invoke_catching(engine, payload):
    try:
        return engine.invoke(payload)
    except BusinessError as exc:
        return {"ok": False, "business_code": exc.code, "detail": exc.detail}


def _status(engine, call_id):
    try:
        return engine.get_call(call_id)["status"]
    except BusinessError as exc:
        if exc.code == E_NOT_FOUND:
            return None
        raise


def test_bounded_queue_full_returns_concurrency_limit(engine_factory):
    engine = engine_factory(exec_timeout_s=1.0)
    engine.load_specs(
        {"functions": [spec("f", function_concurrency_limit=1, queue_capacity=0)]}
    )
    with ThreadPoolExecutor(max_workers=1) as pool:
        first = pool.submit(
            engine.invoke,
            {"call_id": "busy", "function_id": "f", "execution_us": 220_000},
        )
        wait_for(lambda: _status(engine, "busy") == "EXECUTING")
        with pytest.raises(BusinessError) as exc_info:
            engine.invoke(
                {"call_id": "rejected", "function_id": "f", "execution_us": 1_000}
            )
        assert exc_info.value.code == E_CONCURRENCY_LIMIT
        assert exc_info.value.detail["queue_capacity"] == 0
        assert first.result()["ok"] is True
    assert engine.audit_invariants()["ok"] is True


def test_priority_queue_admits_highest_priority_first(engine_factory):
    engine = engine_factory(exec_timeout_s=1.0)
    engine.load_specs(
        {
            "functions": [
                spec(
                    "f",
                    function_concurrency_limit=1,
                    per_instance_concurrency=1,
                    queue_capacity=2,
                )
            ]
        }
    )
    with ThreadPoolExecutor(max_workers=3) as pool:
        blocker = pool.submit(
            engine.invoke,
            {"call_id": "blocker", "function_id": "f", "execution_us": 240_000},
        )
        wait_for(lambda: _status(engine, "blocker") == "EXECUTING")
        low = pool.submit(
            engine.invoke,
            {
                "call_id": "low",
                "function_id": "f",
                "execution_us": 10_000,
                "priority": 1,
            },
        )
        wait_for(lambda: _status(engine, "low") == "QUEUED")
        high = pool.submit(
            engine.invoke,
            {
                "call_id": "high",
                "function_id": "f",
                "execution_us": 10_000,
                "priority": 9,
            },
        )
        wait_for(lambda: _status(engine, "high") == "QUEUED")
        assert blocker.result()["ok"] is True
        high_result = high.result()
        low_result = low.result()
    assert high_result["ok"] is True
    assert low_result["ok"] is True
    assert high_result["started_at_ms"] < low_result["started_at_ms"]
    assert engine.audit_invariants()["ok"] is True


def test_reserved_quota_is_separate_and_excess_reserved_is_denied(engine_factory):
    engine = engine_factory(exec_timeout_s=1.0)
    engine.load_specs(
        {
            "functions": [
                spec(
                    "f",
                    function_concurrency_limit=2,
                    reserved_quota=1,
                    per_instance_concurrency=1,
                    queue_capacity=2,
                )
            ]
        }
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(
            _invoke_catching,
            engine,
            {
                "call_id": "reserved-a",
                "function_id": "f",
                "execution_us": 180_000,
                "quota_class": "RESERVED",
            },
        )
        wait_for(lambda: _status(engine, "reserved-a") == "EXECUTING")
        second = pool.submit(
            _invoke_catching,
            engine,
            {
                "call_id": "reserved-b",
                "function_id": "f",
                "execution_us": 10_000,
                "quota_class": "RESERVED",
            },
        )
        denied = second.result()
        assert denied["business_code"] == E_QUOTA_DENIED
        assert denied["detail"]["current_reserved_used"] == 1
        assert first.result()["ok"] is True
    quota = engine.quota("f")[0]
    assert quota["on_demand_used"] == 0
    assert quota["reserved_used"] == 0
    assert engine.audit_invariants()["ok"] is True


def test_duplicate_call_is_executed_once_and_conflicting_content_is_rejected(engine_factory):
    engine = engine_factory(exec_timeout_s=1.0)
    engine.load_specs(
        {"functions": [spec("f", function_concurrency_limit=2, queue_capacity=4)]}
    )
    payload = {"call_id": "same", "function_id": "f", "execution_us": 35_000}
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda _: engine.invoke(payload), range(6)))
    assert all(result["call_id"] == "same" for result in results)
    assert sum(1 for result in results if not result["deduplicated"]) == 1
    assert sum(1 for result in results if result["deduplicated"]) == 5
    assert len({result["instance_id"] for result in results}) == 1
    with pytest.raises(BusinessError) as exc_info:
        engine.invoke(
            {"call_id": "same", "function_id": "f", "execution_us": 35_001}
        )
    assert exc_info.value.code == E_CONFLICTING_CALL
    assert engine.audit_invariants()["ok"] is True

