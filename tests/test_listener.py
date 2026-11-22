from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest

from dns_workbench.protocol import RCODE_FORMERR, build_query, parse_message

from .conftest import running_stack, tcp_query, udp_query


@pytest.mark.asyncio
async def test_udp_and_tcp_listeners_answer_real_dns_queries(tmp_path: Path) -> None:
    async with running_stack(tmp_path) as stack:
        wire = build_query("a.lab.test.", "A", identifier=0x2222)
        udp_response = await asyncio.to_thread(udp_query, stack.config.dns_port, wire)
        udp_message = parse_message(udp_response)
        assert udp_message.id == 0x2222
        assert udp_message.qr and udp_message.rcode == 0
        assert udp_message.answers[0].rdata["address"] == "192.0.2.10"

        tcp_wire = build_query("mx.lab.test.", "MX", identifier=0x3333)
        tcp_response = await asyncio.to_thread(tcp_query, stack.config.dns_port, tcp_wire)
        tcp_message = parse_message(tcp_response)
        assert tcp_message.id == 0x3333
        assert tcp_message.answers[0].rtype == 15
        assert str(tcp_message.answers[0].rdata["target"]) == "mail.lab.test."


@pytest.mark.asyncio
async def test_malformed_listener_packet_returns_formerr_without_session(tmp_path: Path) -> None:
    async with running_stack(tmp_path) as stack:
        malformed = b"\x12\x34\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x7fshort"
        response = await asyncio.to_thread(udp_query, stack.config.dns_port, malformed)
        message = parse_message(response)
        assert message.rcode == RCODE_FORMERR
        assert message.id == 0x1234
        with sqlite3.connect(stack.config.db_file) as connection:
            assert connection.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 0
        assert stack.runtime.listener is not None
        assert stack.runtime.listener.stats.invalid_queries == 1


@pytest.mark.asyncio
async def test_listener_cache_hit_uses_remaining_wire_ttl(tmp_path: Path) -> None:
    async with running_stack(tmp_path) as stack:
        first = await asyncio.to_thread(
            udp_query,
            stack.config.dns_port,
            build_query("a.lab.test.", "A", identifier=1),
        )
        await asyncio.sleep(0.1)
        second = await asyncio.to_thread(
            udp_query,
            stack.config.dns_port,
            build_query("A.LAB.TEST.", "A", identifier=2),
        )
        first_message = parse_message(first)
        second_message = parse_message(second)
        assert second_message.id == 2
        assert second_message.answers[0].ttl_ms <= first_message.answers[0].ttl_ms
        assert stack.runtime.listener is not None
        assert stack.runtime.listener.stats.responses == 2

