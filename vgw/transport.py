from __future__ import annotations

import asyncio
import json
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .codec import (
    MAX_UDP_DATAGRAM,
    decode_request,
    encode_error_response,
    encode_response,
)
from .errors import GatewayError
from .models import parse_endpoint


@dataclass(frozen=True, slots=True)
class CarrierSpec:
    name: str
    listen_host: str
    listen_port: int
    egress_host: str
    egress_port: int
    egress_name: str

    @property
    def listen_address(self) -> str:
        return f"{self.listen_host}:{self.listen_port}"

    @property
    def egress_address(self) -> str:
        return f"{self.egress_host}:{self.egress_port}"


@dataclass(frozen=True, slots=True)
class Topology:
    carriers: tuple[CarrierSpec, ...]
    egress_timeout_ms: int = 5000

    @classmethod
    def from_file(cls, path: str | Path) -> "Topology":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Topology":
        carriers: list[CarrierSpec] = []
        seen_names: set[str] = set()
        for name, item in raw.get("carriers", {}).items():
            if name in seen_names:
                raise ValueError(f"duplicate carrier {name}")
            seen_names.add(name)
            listen_host, listen_port = parse_endpoint(item["listen"])
            egress_host, egress_port = parse_endpoint(item["egress"])
            carriers.append(
                CarrierSpec(
                    name=name,
                    listen_host=listen_host,
                    listen_port=listen_port,
                    egress_host=egress_host,
                    egress_port=egress_port,
                    egress_name=str(item.get("egress_name", f"decap-{name}")),
                )
            )
        if not carriers:
            raise ValueError("topology must define at least one carrier")
        return cls(
            carriers=tuple(carriers),
            egress_timeout_ms=int(raw.get("egress_timeout_ms", 5000)),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "egress_timeout_ms": self.egress_timeout_ms,
            "carriers": {
                spec.name: {
                    "listen": spec.listen_address,
                    "egress": spec.egress_address,
                    "egress_name": spec.egress_name,
                }
                for spec in self.carriers
            },
        }

    def endpoint_names(self) -> dict[str, str]:
        return {spec.listen_address: spec.name for spec in self.carriers}


class LocalTestNetwork:
    """Two real UDP hops: gateway -> carrier listener -> egress decapsulator."""

    def __init__(self, topology: Topology):
        self.topology = topology
        self.loop: asyncio.AbstractEventLoop | None = None
        self._carrier_sockets: dict[str, socket.socket] = {}
        self._egress_sockets: dict[str, socket.socket] = {}
        self._tasks: set[asyncio.Task[Any]] = set()
        self._child_tasks: set[asyncio.Task[Any]] = set()
        self._stopping = False
        self._egress_name_by_address = {
            spec.egress_address: spec.egress_name for spec in topology.carriers
        }
        self._egress_decoded_total = 0
        self._egress_invalid_total = 0
        self._egress_observations: list[dict[str, Any]] = []
        self.started_at: str | None = None

    async def start(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        for spec in self.topology.carriers:
            if spec.egress_address in self._egress_sockets:
                continue
            sock = self._bind_udp(spec.egress_host, spec.egress_port)
            self._egress_sockets[spec.egress_address] = sock
            self._spawn(self._egress_loop(spec.egress_address, sock))
        for spec in self.topology.carriers:
            sock = self._bind_udp(spec.listen_host, spec.listen_port)
            self._carrier_sockets[spec.name] = sock
            self._spawn(self._carrier_loop(spec, sock))

    async def stop(self) -> None:
        self._stopping = True
        tasks = list(self._tasks | self._child_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for sock in list(self._carrier_sockets.values()) + list(self._egress_sockets.values()):
            try:
                sock.close()
            except OSError:
                pass
        self._carrier_sockets.clear()
        self._egress_sockets.clear()
        self._tasks.clear()
        self._child_tasks.clear()

    def status(self) -> dict[str, Any]:
        return {
            "started_at": self.started_at,
            "carriers": [
                {
                    "name": spec.name,
                    "listen": spec.listen_address,
                    "egress": spec.egress_address,
                    "egress_name": spec.egress_name,
                }
                for spec in self.topology.carriers
            ],
            "egress_listeners": sorted(self._egress_sockets),
            "active_relay_tasks": len(self._child_tasks),
            "egress_decoded_total": self._egress_decoded_total,
            "egress_invalid_total": self._egress_invalid_total,
            "recent_egress_observations": self._egress_observations[-20:],
            "stopping": self._stopping,
        }

    def _spawn(self, coro: Any) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def _spawn_child(self, coro: Any) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        self._child_tasks.add(task)
        task.add_done_callback(self._child_tasks.discard)
        return task

    def _bind_udp(self, host: str, port: int) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((_numeric_host(host), port))
        sock.setblocking(False)
        return sock

    async def _carrier_loop(self, spec: CarrierSpec, sock: socket.socket) -> None:
        assert self.loop is not None
        while not self._stopping:
            try:
                data, client = await self.loop.sock_recvfrom(sock, MAX_UDP_DATAGRAM)
            except (asyncio.CancelledError, OSError):
                break
            self._spawn_child(self._relay(spec, sock, data, client))

    async def _relay(
        self,
        spec: CarrierSpec,
        carrier_sock: socket.socket,
        data: bytes,
        client: tuple[str, int],
    ) -> None:
        assert self.loop is not None
        relay_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        relay_sock.setblocking(False)
        try:
            relay_sock.connect((_numeric_host(spec.egress_host), spec.egress_port))
            await self.loop.sock_sendall(relay_sock, data)
            response = await asyncio.wait_for(
                self.loop.sock_recv(relay_sock, MAX_UDP_DATAGRAM),
                timeout=max(0.05, self.topology.egress_timeout_ms / 1000),
            )
            await self.loop.sock_sendto(carrier_sock, response, client)
        except (asyncio.TimeoutError, ConnectionError, OSError):
            return
        except asyncio.CancelledError:
            raise
        finally:
            relay_sock.close()

    async def _egress_loop(self, address: str, sock: socket.socket) -> None:
        assert self.loop is not None
        while not self._stopping:
            try:
                data, client = await self.loop.sock_recvfrom(sock, MAX_UDP_DATAGRAM)
            except (asyncio.CancelledError, OSError):
                break
            self._spawn_child(self._decap(address, sock, data, client))

    async def _decap(
        self,
        address: str,
        sock: socket.socket,
        data: bytes,
        client: tuple[str, int],
    ) -> None:
        assert self.loop is not None
        endpoint_name = self._egress_name_by_address.get(address, address)
        try:
            frame = decode_request(data, max_payload=60000)
        except GatewayError as exc:
            self._egress_invalid_total += 1
            response = encode_error_response(data, exc.code, exc.message)
            await self.loop.sock_sendto(sock, response, client)
            return
        self._egress_decoded_total += 1
        self._egress_observations.append(
            {
                "at": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
                + f".{int(time.time_ns() % 1_000_000):06d}Z",
                "endpoint": endpoint_name,
                "endpoint_address": address,
                "session_id": frame.session_id,
                "channel": frame.channel,
                "sequence": frame.sequence,
                "request_checksum": frame.checksum,
                "payload_bytes": len(frame.payload),
            }
        )
        if len(self._egress_observations) > 200:
            del self._egress_observations[:-200]
        payload_doc = frame.payload_json()
        options: dict[str, Any] = {}
        if isinstance(payload_doc, dict):
            candidate = payload_doc.get("options")
            if isinstance(candidate, dict):
                options = candidate
        delay_ms = _bounded_int(options.get("delay_ms", 0), 0, 30000)
        if delay_ms:
            try:
                await asyncio.sleep(delay_ms / 1000)
            except asyncio.CancelledError:
                raise
        if bool(options.get("drop", False)):
            return
        response_payload = {
            "status": "ok",
            "endpoint": endpoint_name,
            "endpoint_address": address,
            "client": f"{client[0]}:{client[1]}",
            "session_id": frame.session_id,
            "channel": frame.channel,
            "sequence": frame.sequence,
            "request_checksum": frame.checksum,
            "payload_bytes": len(frame.payload),
            "payload": payload_doc,
            "processed_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
            + f".{int(time.time_ns() % 1_000_000):06d}Z",
        }
        encoded_payload = json.dumps(
            response_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        response = encode_response(
            frame.session_id,
            frame.channel,
            frame.sequence,
            encoded_payload,
            status=0,
        )
        if bool(options.get("corrupt_response", False)):
            response = response[:-1] + bytes([response[-1] ^ 0xFF])
        await self.loop.sock_sendto(sock, response, client)


def _numeric_host(host: str) -> str:
    if host in {"localhost", "::1"}:
        return "127.0.0.1"
    return host


def _bounded_int(value: Any, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return minimum
    return max(minimum, min(maximum, parsed))


async def udp_exchange(
    endpoint: tuple[str, int],
    datagram: bytes,
    timeout_ms: int,
) -> tuple[bytes, dict[str, Any]]:
    """Send one UDP datagram and close the per-frame socket on timeout."""

    loop = asyncio.get_running_loop()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)
    started = time.perf_counter()
    timed_out = False
    try:
        sock.connect((_numeric_host(endpoint[0]), endpoint[1]))
        await loop.sock_sendall(sock, datagram)
        try:
            response = await asyncio.wait_for(
                loop.sock_recv(sock, MAX_UDP_DATAGRAM),
                timeout=timeout_ms / 1000,
            )
        except asyncio.TimeoutError as exc:
            timed_out = True
            raise GatewayError(
                "E_TUNNEL_TIMEOUT",
                "per-frame UDP wait timed out; the per-frame socket is being closed",
                {
                    "timeout_ms": timeout_ms,
                    "endpoint": f"{endpoint[0]}:{endpoint[1]}",
                    "wait_cancelled": True,
                    "socket_closed_after_timeout": True,
                    "remote_may_still_process": True,
                },
            ) from exc
        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
        return response, {
            "elapsed_ms": elapsed_ms,
            "socket_closed": True,
            "timed_out": timed_out,
        }
    except GatewayError:
        raise
    except (ConnectionError, OSError) as exc:
        raise GatewayError(
            "E_FRAME_INVALID",
            "UDP exchange failed before a valid response was received",
            {
                "phase": "udp_transport",
                "endpoint": f"{endpoint[0]}:{endpoint[1]}",
                "exception": type(exc).__name__,
            },
        ) from exc
    finally:
        sock.close()

