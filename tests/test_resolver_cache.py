from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest

from dns_workbench.models import QuerySpec
from dns_workbench.protocol import NAME_BY_TYPE, TYPE_BY_NAME

from .conftest import running_stack


@pytest.mark.asyncio
async def test_positive_types_cname_chain_and_case_insensitive_cache(tmp_path: Path) -> None:
    async with running_stack(tmp_path) as stack:
        service = stack.service
        cases = [
            ("a.lab.test.", "A", 1),
            ("aaaa.lab.test.", "AAAA", 28),
            ("cname.lab.test.", "A", 1),
            ("mx.lab.test.", "MX", 15),
            ("txt.lab.test.", "TXT", 16),
            ("lab.test.", "NS", 2),
            ("lab.test.", "SOA", 6),
        ]
        for name, type_name, source_type in cases:
            spec = service.prepare_spec(name=name, qtype=type_name)
            batch = await service.submit_batch([spec])
            item = batch.items[0]
            assert item.business_code is None, item.to_dict()
            assert item.source_type == source_type
            assert item.answers

        cname_batch = await service.submit_batch(
            [service.prepare_spec(name="cname.lab.test.", qtype="A")]
        )
        cname_item = cname_batch.items[0]
        assert [answer["type"] for answer in cname_item.answers] == [5, 1]
        assert cname_item.source_type == 1

        mixed_spec = service.prepare_spec(name="A.LaB.TeSt.", qtype="A")
        mixed_batch = await service.submit_batch([mixed_spec])
        assert mixed_batch.items[0].outcome == "cache_hit"
        with sqlite3.connect(stack.config.db_file) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM cache_entries WHERE qname='a.lab.test.' AND qtype=1"
            ).fetchone()[0]
        assert count == 1


@pytest.mark.asyncio
async def test_escaped_label_is_distinct_and_normalized(tmp_path: Path) -> None:
    async with running_stack(tmp_path) as stack:
        item = (
            await stack.service.submit_batch(
                [stack.service.prepare_spec(name="Escaped\\.Label.Lab.Test.", qtype="A")]
            )
        ).items[0]
        assert item.business_code is None
        assert item.answers[0]["rdata"]["address"] == "192.0.2.55"
        entries = await stack.service.get_cache_entries("escaped\\.label.lab.test.", TYPE_BY_NAME["A"])
        assert len(entries) == 1
        assert entries[0]["cache_key"] == "escaped\\.label.lab.test.|1|1"


@pytest.mark.asyncio
async def test_negative_cache_uses_soa_minimum_and_does_not_requery(tmp_path: Path) -> None:
    async with running_stack(tmp_path) as stack:
        service = stack.service
        before = stack.authority_a.stats.snapshot()["queries"]
        for expected_rcode, name in [(3, "missing.lab.test."), (0, "nodata.lab.test.")]:
            spec = service.prepare_spec(name=name, qtype="A")
            first = (await service.submit_batch([spec])).items[0]
            assert first.rcode == expected_rcode
            assert first.source_type == 6
            assert first.authorities[0]["ttl_ms"] == 20_000
            assert first.cache is not None
            assert first.cache["kind"] == "negative"
            assert first.cache["original_ttl_ms"] == 20_000

            second = (
                await service.submit_batch([service.prepare_spec(name=name, qtype="A")])
            ).items[0]
            assert second.outcome == "cache_hit"
            assert second.rcode == expected_rcode
            assert second.answers == []
            assert second.cache["kind"] == "negative"
        after = stack.authority_a.stats.snapshot()["queries"]
        assert after - before == 2


@pytest.mark.asyncio
async def test_cname_loop_and_too_long_errors(tmp_path: Path) -> None:
    async with running_stack(tmp_path, max_cname_hops=3) as stack:
        loop = (
            await stack.service.submit_batch(
                [stack.service.prepare_spec(name="loop1.lab.test.", qtype="A")]
            )
        ).items[0]
        assert loop.business_code == "E_CNAME_LOOP"
        assert loop.context["loop_at"] == "loop1.lab.test."

        too_long = (
            await stack.service.submit_batch(
                [stack.service.prepare_spec(name="too-long.lab.test.", qtype="A")]
            )
        ).items[0]
        assert too_long.business_code == "E_CHAIN_TOO_LONG"
        assert too_long.context["max_cname_hops"] == 3
        assert len(too_long.context["chain"]) >= 4


@pytest.mark.asyncio
async def test_ttl_decrements_in_milliseconds_and_expired_entry_is_cleaned(tmp_path: Path) -> None:
    async with running_stack(tmp_path) as stack:
        service = stack.service
        first = (
            await service.submit_batch([service.prepare_spec(name="ttl.lab.test.", qtype="A")])
        ).items[0]
        assert first.ttl_base_ms is not None
        await asyncio.sleep(0.12)
        second = (
            await service.submit_batch([service.prepare_spec(name="ttl.lab.test.", qtype="A")])
        ).items[0]
        assert second.outcome == "cache_hit"
        assert second.cache is not None
        assert 0 < second.cache["remaining_ttl_ms"] < 2_000
        await asyncio.sleep(2.0)
        entries = await service.get_cache_entries("ttl.lab.test.", 1)
        assert entries == []
        with sqlite3.connect(stack.config.db_file) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM cache_entries WHERE qname='ttl.lab.test.'"
            ).fetchone()[0]
        assert count == 0


@pytest.mark.asyncio
async def test_udp_truncation_falls_back_to_tcp_upstream(tmp_path: Path) -> None:
    async with running_stack(tmp_path) as stack:
        spec = stack.service.prepare_spec(name="tc.lab.test.", qtype="TXT")
        item = (await stack.service.submit_batch([spec])).items[0]
        assert item.business_code is None
        assert len(item.answers[0]["rdata"]["texts"]) == 3
        assert stack.authority_a.stats.by_transport.get("tcp", 0) >= 1
        assert stack.service.upstream.stats.active_sockets == 0


@pytest.mark.asyncio
async def test_qtype_and_qclass_validation_do_not_create_sessions(tmp_path: Path) -> None:
    async with running_stack(tmp_path) as stack:
        with pytest.raises(Exception) as exc:
            stack.service.prepare_spec(name="a.lab.test.", qtype=99)
        assert getattr(exc.value, "code", None) == "E_REQUEST_INVALID"
        with sqlite3.connect(stack.config.db_file) as connection:
            assert connection.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 0

