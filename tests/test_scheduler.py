from __future__ import annotations

import asyncio
import time
from pathlib import Path

import pytest

from dns_workbench.config import SchedulerConfig

from .conftest import running_stack


@pytest.mark.asyncio
async def test_hot_merge_uses_one_upstream_and_identical_ttl(tmp_path: Path) -> None:
    async with running_stack(tmp_path) as stack:
        before = stack.authority_a.stats.by_name.get("hot.lab.test.", 0)
        specs = [stack.service.prepare_spec(name="hot.lab.test.", qtype="A") for _ in range(24)]
        batch = await stack.service.submit_batch(specs, round_budget_ms=1000)
        after = stack.authority_a.stats.by_name.get("hot.lab.test.", 0)
        assert after - before == 1
        assert len(batch.trace["dispatch"]) == 1
        assert [item.outcome for item in batch.items].count("merged") == 23
        ttls = {item.answers[0]["ttl_ms"] for item in batch.items}
        assert ttls == {60_000}
        assert len(batch.trace["merge_decisions"]) == 23
        assert all(item["priority"] == 25 for item in batch.trace["dispatch"])


@pytest.mark.asyncio
async def test_bounded_queue_returns_queue_full_for_distinct_tail_tasks(tmp_path: Path) -> None:
    scheduler = SchedulerConfig(
        global_upstream_limit=1,
        per_authority_limit=1,
        queue_limit=0,
        round_budget_ms=1000,
        upstream_timeout_ms=300,
    )
    async with running_stack(tmp_path, scheduler=scheduler) as stack:
        names = ["a.lab.test.", "aaaa.lab.test.", "mx.lab.test."]
        batch = await stack.service.submit_batch(
            [stack.service.prepare_spec(name=name, qtype=TYPE) for name, TYPE in zip(names, ["A", "AAAA", "MX"])]
        )
        assert sum(item.business_code == "E_QUEUE_FULL" for item in batch.items) == 2
        assert len(batch.trace["skipped"]) == 2
        assert all(event["code"] == "E_QUEUE_FULL" for event in batch.trace["skipped"])


@pytest.mark.asyncio
async def test_cross_round_waiting_queue_is_also_bounded(tmp_path: Path) -> None:
    scheduler = SchedulerConfig(
        global_upstream_limit=1,
        per_authority_limit=1,
        queue_limit=0,
        round_budget_ms=1000,
        upstream_timeout_ms=300,
    )
    async with running_stack(tmp_path, scheduler=scheduler) as stack:
        slow_spec = stack.service.prepare_spec(name="slow.lab.test.", qtype="A")
        fast_spec = stack.service.prepare_spec(name="a.lab.test.", qtype="A")
        batches = await asyncio.gather(
            stack.service.submit_batch([slow_spec]),
            stack.service.submit_batch([fast_spec]),
        )
        items = [batch.items[0] for batch in batches]
        rejected = [item for item in items if item.business_code == "E_QUEUE_FULL"]
        assert len(rejected) == 1
        assert rejected[0].context["reason"] == "upstream_waiting_queue_full"
        assert rejected[0].context["queue_limit"] == 0
        trace = next(batch.trace for batch in batches if batch.items[0] is rejected[0])
        assert any(event["code"] == "E_QUEUE_FULL" for event in trace["skipped"])
        assert stack.service.upstream.stats.active_sockets == 0


@pytest.mark.asyncio
async def test_explicit_priority_dispatches_before_arrival_order(tmp_path: Path) -> None:
    scheduler = SchedulerConfig(
        global_upstream_limit=1,
        per_authority_limit=1,
        queue_limit=8,
        round_budget_ms=1000,
        upstream_timeout_ms=300,
    )
    async with running_stack(tmp_path, scheduler=scheduler) as stack:
        low = stack.service.prepare_spec(name="slow.lab.test.", qtype="A", priority=0, arrival_seq=0)
        high = stack.service.prepare_spec(name="a.lab.test.", qtype="A", priority=100, arrival_seq=1)
        batch = await stack.service.submit_batch([low, high])
        assert batch.trace["dispatch"][0]["cache_key"].startswith("a.lab.test.")
        assert batch.trace["dispatch"][0]["priority"] == 100
        assert batch.items[1].business_code is None


@pytest.mark.asyncio
async def test_per_authority_limit_and_global_limit_are_observed(tmp_path: Path) -> None:
    scheduler = SchedulerConfig(
        global_upstream_limit=4,
        per_authority_limit=1,
        queue_limit=8,
        round_budget_ms=1500,
        upstream_timeout_ms=500,
    )
    async with running_stack(tmp_path, scheduler=scheduler) as stack:
        specs = [
            stack.service.prepare_spec(name="slow.lab.test.", qtype="A"),
            stack.service.prepare_spec(name="slow2.lab.test.", qtype="A"),
        ]
        started = time.monotonic()
        batch = await stack.service.submit_batch(specs)
        elapsed = time.monotonic() - started
        assert all(item.business_code is None for item in batch.items)
        assert elapsed >= 0.32
        assert batch.trace["gate"]["authority_peak"]["auth-a"] == 1
        assert batch.trace["gate"]["global_peak"] <= 4


@pytest.mark.asyncio
async def test_round_budget_skips_waiting_tail_and_closes_active_socket(tmp_path: Path) -> None:
    scheduler = SchedulerConfig(
        global_upstream_limit=1,
        per_authority_limit=1,
        queue_limit=8,
        round_budget_ms=70,
        upstream_timeout_ms=400,
    )
    async with running_stack(tmp_path, scheduler=scheduler) as stack:
        specs = [
            stack.service.prepare_spec(name="slow.lab.test.", qtype="A"),
            stack.service.prepare_spec(name="tail.long.lab.test.", qtype="A"),
        ]
        started = time.monotonic()
        batch = await stack.service.submit_batch(specs, round_budget_ms=70)
        elapsed = time.monotonic() - started
        assert elapsed < 0.35
        assert all(item.business_code == "E_BUDGET_EXHAUSTED" for item in batch.items)
        assert any(item["reason"] == "round_budget_exhausted_while_waiting" for item in batch.trace["skipped"])
        active = next(
            item for item in batch.items if item.context.get("reason") == "round_budget_expired_during_active_upstream_query"
        )
        assert active.context["socket_closed"] is True
        assert stack.service.upstream.stats.active_sockets == 0
        assert stack.service.upstream.stats.budget_cancellations >= 1


@pytest.mark.asyncio
async def test_merge_budget_conflict_can_split_with_numeric_basis(tmp_path: Path) -> None:
    scheduler = SchedulerConfig(
        global_upstream_limit=2,
        per_authority_limit=2,
        queue_limit=8,
        round_budget_ms=300,
        upstream_timeout_ms=400,
        merge_split_enabled=True,
        merge_split_after_ms=10,
        merge_split_reserve_ms=20,
        min_split_budget_ms=40,
        max_splits_per_flight=1,
    )
    async with running_stack(tmp_path, scheduler=scheduler) as stack:
        before = stack.authority_a.stats.by_name.get("slow.lab.test.", 0)
        specs = [stack.service.prepare_spec(name="slow.lab.test.", qtype="A") for _ in range(2)]
        batch = await stack.service.submit_batch(specs, round_budget_ms=300)
        after = stack.authority_a.stats.by_name.get("slow.lab.test.", 0)
        assert after - before == 2
        assert {item.outcome for item in batch.items} == {"resolved", "split"}
        split = next(item for item in batch.items if item.outcome == "split")
        assert split.merge_decision["decision"] == "split"
        assert split.merge_decision["projected_shared_finish_ms"] is not None
        assert split.merge_decision["remaining_budget_ms"] > 0


@pytest.mark.asyncio
async def test_timeout_is_real_and_releases_capacity(tmp_path: Path) -> None:
    scheduler = SchedulerConfig(
        global_upstream_limit=1,
        per_authority_limit=1,
        queue_limit=4,
        round_budget_ms=1000,
        upstream_timeout_ms=100,
    )
    async with running_stack(tmp_path, scheduler=scheduler) as stack:
        started = time.monotonic()
        timed_out = (
            await stack.service.submit_batch(
                [stack.service.prepare_spec(name="timeout.lab.test.", qtype="A")],
                round_budget_ms=1000,
            )
        ).items[0]
        elapsed = time.monotonic() - started
        assert timed_out.business_code == "E_UPSTREAM_TIMEOUT"
        assert timed_out.context["socket_closed"] is True
        assert elapsed >= 0.09
        stats = stack.service.upstream.stats
        assert stats.active_sockets == 0
        assert stats.socket_balance == 0
        gate = await stack.service.gate.snapshot()
        assert gate["global_active"] == 0
        assert gate["authority_active"] == {}

        subsequent = (
            await stack.service.submit_batch(
                [stack.service.prepare_spec(name="a.lab.test.", qtype="A")]
            )
        ).items[0]
        assert subsequent.business_code is None

