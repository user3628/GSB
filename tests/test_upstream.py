from __future__ import annotations

import asyncio

import pytest

from dns_workbench.config import AuthorityConfig
from dns_workbench.upstream import UpstreamClient, UpstreamTimeout

from .conftest import free_port


@pytest.mark.asyncio
async def test_tcp_upstream_timeout_closes_connection_and_releases_socket_count() -> None:
    port = free_port()
    connections: list[asyncio.StreamWriter] = []
    release = asyncio.Event()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        connections.append(writer)
        try:
            await reader.read(4096)
            await release.wait()
        except asyncio.CancelledError:
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", port)
    client = UpstreamClient()
    authority = AuthorityConfig(
        id="tcp-drop",
        host="127.0.0.1",
        port=port,
        zones=["lab.test."],
        transport="tcp",
    )
    try:
        with pytest.raises(UpstreamTimeout) as exc:
            await client.query(authority, "a.lab.test.", 1, 1, timeout_ms=50)
        assert exc.value.transport == "tcp"
        assert exc.value.context()["socket_closed"] is True
        assert client.stats.active_sockets == 0
        assert client.stats.socket_balance == 0
        assert client.stats.closed_sockets == 1
    finally:
        release.set()
        server.close()
        await server.wait_closed()
        for writer in connections:
            writer.close()
        await asyncio.sleep(0)

