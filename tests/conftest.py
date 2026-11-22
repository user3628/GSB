from __future__ import annotations

import asyncio
import dataclasses
import inspect
import json
import socket
import struct
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator

import pytest

from dns_workbench.authority import AuthorityServer, ZoneData
from dns_workbench.config import AppConfig, AuthorityConfig, SchedulerConfig
from dns_workbench.runtime import Runtime
from dns_workbench.storage import Storage


ROOT = Path(__file__).resolve().parents[1]
ZONE_FILE = ROOT / "samples" / "zone_lab.json"


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "asyncio: run this test coroutine with asyncio.run")


@pytest.hookimpl(tryfirst=True)
def pytest_pyfunc_call(pyfuncitem: pytest.Function):
    if inspect.iscoroutinefunction(pyfuncitem.obj):
        kwargs = {
            name: pyfuncitem.funcargs[name]
            for name in pyfuncitem._fixtureinfo.argnames
        }
        asyncio.run(pyfuncitem.obj(**kwargs))
        return True
    return None


def free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
    finally:
        sock.close()


def unique_ports(count: int) -> list[int]:
    ports: list[int] = []
    while len(ports) < count:
        candidate = free_port()
        if candidate not in ports:
            ports.append(candidate)
    return ports


@dataclass
class Stack:
    config: AppConfig
    runtime: Runtime
    authority_a: AuthorityServer
    authority_b: AuthorityServer

    @property
    def service(self):
        assert self.runtime.service is not None
        return self.runtime.service

    @property
    def storage(self) -> Storage:
        return self.runtime.storage


@asynccontextmanager
async def running_stack(
    tmp_path: Path,
    *,
    scheduler: SchedulerConfig | None = None,
    max_cname_hops: int = 8,
    enable_dns: bool = True,
    zone_file: Path = ZONE_FILE,
) -> AsyncIterator[Stack]:
    auth_a_port, auth_b_port, dns_port = unique_ports(3)
    authority_a_config = AuthorityConfig(
        id="auth-a",
        host="127.0.0.1",
        port=auth_a_port,
        zones=["lab.test."],
        zone_file=str(zone_file),
    )
    authority_b_config = AuthorityConfig(
        id="auth-b",
        host="127.0.0.1",
        port=auth_b_port,
        zones=["long.lab.test."],
        zone_file=str(zone_file),
    )
    zone = ZoneData.load(zone_file)
    authority_a = AuthorityServer(authority_a_config, zone)
    authority_b = AuthorityServer(authority_b_config, zone)
    await authority_a.start()
    await authority_b.start()
    config = AppConfig(
        db_path=str(tmp_path / "workbench.sqlite3"),
        log_dir=str(tmp_path / "logs"),
        http_port=free_port(),
        dns_port=dns_port,
        enable_dns_listener=enable_dns,
        enable_authorities=False,
        hot_names=["hot.lab.test."],
        max_cname_hops=max_cname_hops,
        authorities=[authority_a_config, authority_b_config],
        scheduler=scheduler
        or SchedulerConfig(
            global_upstream_limit=4,
            per_authority_limit=2,
            queue_limit=16,
            round_budget_ms=1000,
            upstream_timeout_ms=300,
        ),
    )
    runtime = Runtime(config)
    await runtime.start(start_authorities=False, start_dns=enable_dns)
    try:
        yield Stack(config=config, runtime=runtime, authority_a=authority_a, authority_b=authority_b)
    finally:
        await runtime.stop(clean=True)
        await authority_a.stop()
        await authority_b.stop()


def udp_query(port: int, packet: bytes, timeout: float = 2.0) -> bytes:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    try:
        sock.sendto(packet, ("127.0.0.1", port))
        response, _ = sock.recvfrom(65535)
        return response
    finally:
        sock.close()


def tcp_query(port: int, packet: bytes, timeout: float = 2.0) -> bytes:
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
        sock.sendall(struct.pack("!H", len(packet)) + packet)
        header = _read_exact(sock, 2)
        length = struct.unpack("!H", header)[0]
        return _read_exact(sock, length)


def _read_exact(sock: socket.socket, length: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < length:
        chunk = sock.recv(length - len(chunks))
        if not chunk:
            raise RuntimeError("connection closed before full DNS message")
        chunks.extend(chunk)
    return bytes(chunks)


def session_count(db_path: Path) -> int:
    import sqlite3

    with sqlite3.connect(db_path) as connection:
        return int(connection.execute("SELECT COUNT(*) FROM sessions").fetchone()[0])

