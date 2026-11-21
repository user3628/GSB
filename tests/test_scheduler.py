from __future__ import annotations

import asyncio
import dataclasses
import time

import pytest

from delay_queue.errors import E_CONFLICTING_ROUND, E_QUEUE_FULL, BusinessError
from delay_queue.service import DelayQueueService

from .conftest import task


def round_body(**overrides):
    body = {
        "max_concurrency": 2,
        "time_budget_ms": 800,
        "max_task_resource": 100,
        "waiting_queue_capacity": 20,
        "dispatch_timeout_ms": 250,
    }
    body.update(overrides)
    return body


def test_delay_priority_and_bounded_waiting_queue(service, endpoint):
    _, state = endpoint
    service.submit_tasks(
        "q",
        [
            task("future", due_at_ms=4_102_444_800_000, priority=0),
            task("low", priority=7),
            task("high", priority=1),
            task("middle", priority=3),
        ],
    )
    result = asyncio.run(service.trigger_round("q", "r1", round_body()))
    assert [item["task_id"] for item in result["dispatched"]] == ["high", "middle"]
    assert result["waiting_queue"]["task_ids"] == ["low"]
    assert result["skipped"][0]["task_id"] == "low"
    assert result["skipped"][0]["reason"] == "WAITING_QUEUE"
    status = service.get_queue_status("q")
    assert status["state_counts"]["PENDING"] == 1
    assert status["state_counts"]["READY"] == 1
    assert state.snapshot()["total_requests"] == 2


def test_queue_full_is_atomic_and_round_is_stored(service):
    service.submit_tasks(
        "q", [task(f"t{i}", priority=i) for i in range(4)]
    )
    with pytest.raises(BusinessError) as exc:
        asyncio.run(
            service.trigger_round(
                "q",
                "full",
                round_body(max_concurrency=1, waiting_queue_capacity=1),
            )
        )
    assert exc.value.code == E_QUEUE_FULL
    assert exc.value.http_status == 429
    status = service.get_queue_status("q")
    assert status["state_counts"]["PENDING"] == 4
    assert status["state_counts"]["DISPATCHED"] == 0
    assert service.storage.get_attempts_for_round("full") == []

    with pytest.raises(BusinessError) as duplicate:
        asyncio.run(
            service.trigger_round(
                "q",
                "full",
                round_body(max_concurrency=1, waiting_queue_capacity=1),
            )
        )
    assert duplicate.value.code == E_QUEUE_FULL
    assert service.storage.get_round("full")["status"] == "FAILED"


def test_resource_and_zero_time_budget_are_deterministic_skips(service, endpoint):
    _, state = endpoint
    service.submit_tasks(
        "q",
        [
            task("too-heavy", resource=9, priority=9),
            task("a", priority=2),
            task("b", priority=1),
        ],
    )
    result = asyncio.run(
        service.trigger_round(
            "q",
            "budget0",
            round_body(
                max_concurrency=3,
                time_budget_ms=0,
                max_task_resource=5,
                waiting_queue_capacity=5,
            ),
        )
    )
    assert result["counts"]["dispatched"] == 0
    assert state.snapshot()["total_requests"] == 0
    assert [(item["task_id"], item["reason"]) for item in result["skipped"]] == [
        ("b", "TIME_BUDGET_EXHAUSTED"),
        ("a", "TIME_BUDGET_EXHAUSTED"),
        ("too-heavy", "RESOURCE_BUDGET_EXCEEDED"),
    ]
    assert service.storage.get_task("q", "too-heavy")["state"] == "PENDING"
    assert service.storage.get_task("q", "a")["state"] == "READY"


def test_round_idempotency_reuse_and_conflict(service, endpoint):
    service.submit_tasks("q", [task("t1")])
    body = round_body(dispatch_timeout_ms=100)
    first = asyncio.run(service.trigger_round("q", "same", body))
    second = asyncio.run(service.trigger_round("q", "same", body))
    assert first["reused"] is False
    assert second["reused"] is True
    assert [x["attempt_id"] for x in first["dispatched"]] == [
        x["attempt_id"] for x in second["dispatched"]
    ]
    assert len(service.storage.get_attempts_for_round("same")) == 1

    with pytest.raises(BusinessError) as exc:
        asyncio.run(service.trigger_round("q", "same", round_body(max_concurrency=1)))
    assert exc.value.code == E_CONFLICTING_ROUND


def test_concurrent_duplicate_round_waits_and_reuses(service):
    service.submit_tasks("q", [task("slow", mode="slow", sleep_seconds=0.12)])

    async def both():
        body = round_body(max_concurrency=1, dispatch_timeout_ms=500)
        return await asyncio.gather(
            service.trigger_round("q", "concurrent", body),
            service.trigger_round("q", "concurrent", body),
        )

    left, right = asyncio.run(both())
    assert {left["reused"], right["reused"]} == {False, True}
    assert len(service.storage.get_attempts_for_round("concurrent")) == 1


def test_duplicate_round_reuses_across_two_service_objects(service, settings):
    service.submit_tasks("q", [task("slow", mode="slow", sleep_seconds=0.16)])
    second = DelayQueueService(settings)
    second.startup()

    async def both():
        body = round_body(max_concurrency=1, dispatch_timeout_ms=500)
        return await asyncio.gather(
            service.trigger_round("q", "two-services", body),
            second.trigger_round("q", "two-services", body),
        )

    left, right = asyncio.run(both())
    assert {left["reused"], right["reused"]} == {False, True}
    assert len(service.storage.get_attempts_for_round("two-services")) == 1


def test_global_concurrency_across_queues(service, settings, endpoint):
    _, state = endpoint
    service.submit_tasks("qa", [task("a1", mode="slow", sleep_seconds=0.12), task("a2", mode="slow", sleep_seconds=0.12)])
    service.submit_tasks("qb", [task("b1", mode="slow", sleep_seconds=0.12), task("b2", mode="slow", sleep_seconds=0.12)])
    constrained = DelayQueueService(dataclasses.replace(settings, global_concurrency=2))
    constrained.startup()

    async def run_both():
        return await asyncio.gather(
            constrained.trigger_round("qa", "ra", round_body(max_concurrency=2)),
            constrained.trigger_round("qb", "rb", round_body(max_concurrency=2)),
        )

    ra, rb = asyncio.run(run_both())
    assert ra["counts"]["dispatched"] + rb["counts"]["dispatched"] == 2
    loser = rb if ra["counts"]["dispatched"] else ra
    assert loser["counts"]["dispatched"] == 0
    assert any(
        item["reason"] == "GLOBAL_CONCURRENCY_LIMIT" for item in loser["skipped"]
    )
    stats = state.snapshot()
    assert stats["max_active"] <= 2


def test_readers_never_see_dispatched_without_attempt(service):
    service.submit_tasks("q", [task("live", mode="slow", sleep_seconds=0.25)])

    async def inspect_while_running():
        run = asyncio.create_task(
            service.trigger_round(
                "q", "observe", round_body(max_concurrency=1, dispatch_timeout_ms=1_000)
            )
        )
        for _ in range(50):
            await asyncio.sleep(0.01)
            snapshot = service.get_queue_status("q")
            if snapshot["state_counts"]["DISPATCHED"]:
                break
        assert snapshot["state_counts"]["DISPATCHED"] == 1
        assert snapshot["open_intents"] == 1
        assert snapshot["dispatching_without_attempt"] == 0
        assert snapshot["consistency"]["ok"] is True
        running_task = next(t for t in snapshot["tasks"] if t["task_id"] == "live")
        assert running_task["latest_attempt"]["outcome"] == "IN_FLIGHT"
        result = await run
        assert result["dispatched"][0]["outcome"] == "SUCCEEDED"

    asyncio.run(inspect_while_running())

