from __future__ import annotations

import asyncio
import json
import struct
from dataclasses import dataclass, field
from typing import Any

from .config import AppConfig
from .errors import BusinessError
from .models import ItemResult
from .protocol import (
    DnsMessage,
    RCODE_FORMERR,
    RCODE_SERVFAIL,
    build_minimal_error,
    build_response,
    build_udp_response,
    parse_query_packet,
    records_from_json,
)
from .service import DiagnosticService


@dataclass(slots=True)
class ListenerStats:
    udp_queries: int = 0
    tcp_queries: int = 0
    invalid_queries: int = 0
    responses: int = 0
    active: int = 0
    business_codes: dict[str, int] = field(default_factory=dict)

    def snapshot(self) -> dict[str, Any]:
        return {
            "udp_queries": self.udp_queries,
            "tcp_queries": self.tcp_queries,
            "invalid_queries": self.invalid_queries,
            "responses": self.responses,
            "active": self.active,
            "business_codes": dict(self.business_codes),
        }


class DnsListener:
    def __init__(self, config: AppConfig, service: DiagnosticService) -> None:
        self.config = config
        self.service = service
        self.stats = ListenerStats()
        self._transport: asyncio.DatagramTransport | None = None
        self._tcp_server: asyncio.AbstractServer | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._stopping = False

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: _ListenerDatagramProtocol(self),
            local_addr=(self.config.dns_host, self.config.dns_port),
        )
        self._transport = transport
        try:
            self._tcp_server = await asyncio.start_server(
                self._handle_tcp,
                host=self.config.dns_host,
                port=self.config.dns_port,
                limit=65535,
            )
        except Exception:
            transport.close()
            self._transport = None
            raise

    async def stop(self) -> None:
        self._stopping = True
        if self._tcp_server is not None:
            self._tcp_server.close()
            await self._tcp_server.wait_closed()
            self._tcp_server = None
        if self._transport is not None:
            self._transport.close()
            self._transport = None
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    def _track(self, task: asyncio.Task[Any]) -> None:
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def handle_packet(self, packet: bytes, transport: str, client: str) -> bytes | None:
        self.stats.active += 1
        try:
            try:
                parse_query_packet(packet)
            except BusinessError:
                self.stats.invalid_queries += 1
                return build_minimal_error(packet, RCODE_FORMERR)
            if transport == "udp":
                self.stats.udp_queries += 1
            else:
                self.stats.tcp_queries += 1
            try:
                response, business_code, _ = await self.service.resolve_wire(
                    packet, transport=transport, client=client
                )
            except BusinessError as exc:
                self.stats.business_codes[exc.code] = self.stats.business_codes.get(exc.code, 0) + 1
                return build_minimal_error(packet, RCODE_FORMERR if exc.code == "E_MESSAGE_INVALID" else RCODE_SERVFAIL)
            if business_code:
                self.stats.business_codes[business_code] = self.stats.business_codes.get(business_code, 0) + 1
            self.stats.responses += 1
            return response
        finally:
            self.stats.active = max(0, self.stats.active - 1)

    async def _handle_tcp(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._track(task)
        try:
            header = await reader.readexactly(2)
            length = struct.unpack("!H", header)[0]
            if length < 12:
                self.stats.invalid_queries += 1
                return
            packet = await reader.readexactly(length)
            response = await self.handle_packet(packet, "tcp", _peer_text(writer.get_extra_info("peername")))
            if response is not None and not self._stopping:
                writer.write(struct.pack("!H", len(response)) + response)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.CancelledError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, asyncio.CancelledError):
                pass


class _ListenerDatagramProtocol(asyncio.DatagramProtocol):
    def __init__(self, listener: DnsListener) -> None:
        self.listener = listener
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        if self.transport is None or self.listener._stopping:
            return
        task = asyncio.create_task(self._handle(data, addr))
        self.listener._track(task)

    async def _handle(self, data: bytes, addr: tuple[str, int]) -> None:
        response = await self.listener.handle_packet(data, "udp", _peer_text(addr))
        if response is not None and self.transport is not None:
            try:
                self.transport.sendto(response, addr)
            except (RuntimeError, OSError):
                pass


def _peer_text(peer: Any) -> str:
    if isinstance(peer, tuple) and peer:
        return f"{peer[0]}:{peer[1]}" if len(peer) > 1 else str(peer[0])
    return str(peer or "unknown")


def build_wire_result(query: DnsMessage, item: ItemResult, transport: str, max_udp_bytes: int) -> bytes:
    answers = records_from_json(item.answers)
    authorities = records_from_json(item.authorities)
    rcode = item.rcode if item.business_code is None else RCODE_SERVFAIL
    if transport == "udp":
        return build_udp_response(
            query,
            answers=answers,
            authorities=authorities,
            rcode=rcode,
            max_bytes=max_udp_bytes,
        )
    return build_response(query, answers=answers, authorities=authorities, rcode=rcode)

