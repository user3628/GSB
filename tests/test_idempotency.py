from __future__ import annotations

import json
import asyncio

from pathlib import Path

import pytest

from .conftest import running_stack


@pytest.mark.asyncio
async def test_same_query_id_is_executed_once_and_replayed(tmp_path: Path) -> None:
    async with running_stack(tmp_path) as stack:
        first_spec = stack.service.prepare_spec(
            name="a.lab.test.", qtype="A", query_id="stable-id"
        )
        first = (await stack.service.submit_batch([first_spec])).items[0]
        before = stack.authority_a.stats.by_name.get("a.lab.test.", 0)
        replay_spec = stack.service.prepare_spec(
            name="A.LAB.TEST.", qtype="A", query_id="stable-id"
        )
        replay = (await stack.service.submit_batch([replay_spec])).items[0]
        after = stack.authority_a.stats.by_name.get("a.lab.test.", 0)
        assert first.business_code is None
        assert replay.outcome == "idempotent_replay"
        assert replay.business_code is None
        assert replay.answers == first.answers
        assert replay.ttl_base_ms == first.ttl_base_ms
        assert after == before


@pytest.mark.asyncio
async def test_conflicting_query_id_returns_business_error(tmp_path: Path) -> None:
    async with running_stack(tmp_path) as stack:
        first = stack.service.prepare_spec(name="a.lab.test.", qtype="A", query_id="conflict-id")
        await stack.service.submit_batch([first])
        conflicting = stack.service.prepare_spec(
            name="mx.lab.test.", qtype="MX", query_id="conflict-id"
        )
        item = (await stack.service.submit_batch([conflicting])).items[0]
        assert item.business_code == "E_CONFLICTING_QUERY"
        assert item.context["existing"]["name"] == "a.lab.test."
        assert item.context["incoming"]["name"] == "mx.lab.test."


@pytest.mark.asyncio
async def test_duplicate_query_ids_in_one_batch_share_one_result(tmp_path: Path) -> None:
    async with running_stack(tmp_path) as stack:
        before = stack.authority_a.stats.by_name.get("a.lab.test.", 0)
        specs = [
            stack.service.prepare_spec(name="a.lab.test.", qtype="A", query_id="batch-id")
            for _ in range(8)
        ]
        batch = await stack.service.submit_batch(specs)
        after = stack.authority_a.stats.by_name.get("a.lab.test.", 0)
        assert after - before == 1
        assert all(item.business_code is None for item in batch.items)
        assert len({json.dumps(item.answers, sort_keys=True) for item in batch.items}) == 1


@pytest.mark.asyncio
async def test_cross_round_singleflight_coalesces_concurrent_cache_misses(tmp_path: Path) -> None:
    async with running_stack(tmp_path) as stack:
        before = stack.authority_a.stats.by_name.get("hot.lab.test.", 0)

        async def submit() -> object:
            spec = stack.service.prepare_spec(name="hot.lab.test.", qtype="A")
            return await stack.service.submit_batch([spec], round_budget_ms=1000)

        batches = await asyncio.gather(*(submit() for _ in range(10)))
        after = stack.authority_a.stats.by_name.get("hot.lab.test.", 0)
        assert after - before == 1
        assert all(batch.items[0].business_code is None for batch in batches)
        ttls = [batch.items[0].answers[0]["ttl_ms"] for batch in batches]
        # Requests that attached to the in-flight leader share its snapshot. Requests
        # that observed the already committed cache are normal hits and may have
        # independently decremented by a few milliseconds, but all remain near 60s.
        assert all(59_000 <= ttl <= 60_000 for ttl in ttls)
        assert max(ttls) - min(ttls) < 500

