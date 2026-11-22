from __future__ import annotations

import asyncio
import secrets
import socket
import struct
from dataclasses import dataclass, field
from typing import Any

from .config import AuthorityConfig
from .errors import BusinessError
from .models import now_ms
from .protocol import (
    DnsMessage,
    RCODE_SERVFAIL,
    build_query,
    parse_message,
    validate_response,
)


class UpstreamTimeout(Exception):
    def __init__(self, authority: AuthorityConfig, timeout_ms: int, transport: str) -> None:
        self.authority = authority
        self.timeout_ms = timeout_ms
        self.transport = transport
        super().__init__(f"upstream {authority.id} timed out after {timeout_ms} ms over {transport}")

    def context(self) -> dict[str, Any]:
        return {
            "authority_id": self.authority.id,
            "authority": f"{self.authority.host}:{self.authority.port}",
            "timeout_ms": self.timeout_ms,
            "transport": self.transport,
            "socket_closed": True,
        }


class UpstreamInvalid(Exception):
    def __init__(self, message: str, *, context: dict[str, Any] | None = None) -> None:
        self.message = message
        self.context = context or {}
        super().__init__(message)


@dataclass(slots=True)
class UpstreamResponse:
    message: DnsMessage
    transport: str
    authority_id: str
    elapsed_ms: int


@dataclass(slots=True)
class UpstreamStats:
    requests: int = 0
    responses: int = 0
    timeouts: int = 0
    budget_cancellations: int = 0
    invalid_responses: int = 0
    opened_sockets: int = 0
    closed_sockets: int = 0
    active_sockets: int = 0
    by_authority: dict[str, int] = field(default_factory=dict)
    last_error: dict[str, Any] | None = None

    @property
    def socket_balance(self) -> int:
        return self.opened_sockets - self.closed_sockets

    def snapshot(self) -> dict[str, Any]:
        return {
            "requests": self.requests,
            "responses": self.responses,
            "timeouts": self.timeouts,
            "budget_cancellations": self.budget_cancellations,
            "invalid_responses": self.invalid_responses,
            "opened_sockets": self.opened_sockets,
            "closed_sockets": self.closed_sockets,
            "active_sockets": self.active_sockets,
            "socket_balance": self.socket_balance,
            "by_authority": dict(self.by_authority),
            "last_error": dict(self.last_error) if self.last_error else None,
        }


class UpstreamClient:
    def __init__(self) -> None:
        self.stats = UpstreamStats()

    def _open_socket(self) -> None:
        self.stats.opened_sockets += 1
        self.stats.active_sockets += 1

    def _close_socket(self) -> None:
        self.stats.closed_sockets += 1
        self.stats.active_sockets = max(0, self.stats.active_sockets - 1)

    async def query(
        self,
        authority: AuthorityConfig,
        name: str,
        qtype: int,
        qclass: int,
        *,
        timeout_ms: int,
        timeout_is_round_budget: bool = False,
    ) -> UpstreamResponse:
        self.stats.requests += 1
        self.stats.by_authority[authority.id] = self.stats.by_authority.get(authority.id, 0) + 1
        started = now_ms()
        identifier = secrets.randbits(16)
        wire = build_query(name, qtype, qclass, identifier=identifier)
        query_message = parse_message(wire)
        transport = authority.transport
        transport_used = [transport]
        try:
            response_wire, actual_transport = await asyncio.wait_for(
                self._exchange(authority, wire, initial_transport=transport, transport_used=transport_used),
                timeout=max(1, timeout_ms) / 1000.0,
            )
        except asyncio.TimeoutError as exc:
            if timeout_is_round_budget:
                self.stats.budget_cancellations += 1
            else:
                self.stats.timeouts += 1
            error = UpstreamTimeout(authority, timeout_ms, transport_used[0])
            self.stats.last_error = {
                "code": "E_BUDGET_EXHAUSTED" if timeout_is_round_budget else "E_UPSTREAM_TIMEOUT",
                **error.context(),
                "configured_as_round_budget_cancellation": timeout_is_round_budget,
                "at_ms": now_ms(),
            }
            raise error from exc

        try:
            response = parse_message(response_wire)
            validate_response(query_message, response)
        except BusinessError as exc:
            self.stats.invalid_responses += 1
            self.stats.last_error = {
                "code": "E_UPSTREAM_INVALID",
                "authority_id": authority.id,
                "context": exc.context,
                "at_ms": now_ms(),
            }
            raise UpstreamInvalid(exc.message, context={"authority_id": authority.id, **exc.context}) from exc

        self.stats.responses += 1
        return UpstreamResponse(
            message=response,
            transport=actual_transport,
            authority_id=authority.id,
            elapsed_ms=max(0, now_ms() - started),
        )

    async def _exchange(
        self,
        authority: AuthorityConfig,
        wire: bytes,
        *,
        initial_transport: str,
        transport_used: list[str],
    ) -> tuple[bytes, str]:
        if initial_transport == "tcp":
            transport_used[0] = "tcp"
            return await self._tcp_exchange(authority, wire), "tcp"
        response, truncated = await self._udp_exchange(authority, wire)
        if truncated:
            transport_used[0] = "tcp"
            return await self._tcp_exchange(authority, wire), "tcp"
        return response, "udp"

    async def _udp_exchange(self, authority: AuthorityConfig, wire: bytes) -> tuple[bytes, bool]:
        loop = asyncio.get_running_loop()
        family = socket.AF_INET6 if ":" in authority.host else socket.AF_INET
        sock = socket.socket(family, socket.SOCK_DGRAM)
        sock.setblocking(False)
        self._open_socket()
        try:
            sock.connect((authority.host, authority.port))
            await loop.sock_sendall(sock, wire)
            packet = await loop.sock_recv(sock, 65535)
            if len(packet) < 12:
                raise UpstreamInvalid("upstream UDP response is shorter than header")
            flags = struct.unpack_from("!H", packet, 2)[0]
            return packet, bool(flags & 0x0200)
        finally:
            sock.close()
            self._close_socket()

    async def _tcp_exchange(self, authority: AuthorityConfig, wire: bytes) -> bytes:
        writer: asyncio.StreamWriter | None = None
        self._open_socket()
        try:
            reader, writer = await asyncio.open_connection(authority.host, authority.port)
            writer.write(struct.pack("!H", len(wire)) + wire)
            await writer.drain()
            header = await reader.readexactly(2)
            length = struct.unpack("!H", header)[0]
            if length < 12:
                raise UpstreamInvalid("upstream TCP response length is smaller than DNS header", context={"length": length})
            packet = await reader.readexactly(length)
            return packet
        except asyncio.IncompleteReadError as exc:
            raise UpstreamInvalid(
                "upstream TCP connection closed before a complete DNS message",
                context={"partial_bytes": len(exc.partial)},
            ) from exc
        finally:
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except (ConnectionError, asyncio.CancelledError):
                    pass
            self._close_socket()


def upstream_error_to_business(exc: Exception) -> BusinessError:
    if isinstance(exc, UpstreamTimeout):
        return BusinessError(
            code="E_UPSTREAM_TIMEOUT",
            message="upstream DNS query exceeded its per-hop timeout",
            context=exc.context(),
            http_status=504,
            dns_rcode=RCODE_SERVFAIL,
        )
    if isinstance(exc, UpstreamInvalid):
        return BusinessError(
            code="E_UPSTREAM_INVALID",
            message=exc.message,
            context=exc.context,
            http_status=502,
            dns_rcode=RCODE_SERVFAIL,
        )
    return BusinessError(
        code="E_UPSTREAM_INVALID",
        message=str(exc),
        context={"exception_type": type(exc).__name__},
        http_status=502,
        dns_rcode=RCODE_SERVFAIL,
    )

