from __future__ import annotations

from dataclasses import dataclass

import pytest

from dnsmw.authserver import AuthoritativeTestServer
from dnsmw.server import MiddlewareService


@dataclass
class Stack:
    auth: AuthoritativeTestServer
    service: MiddlewareService

    @property
    def dns_port(self) -> int:
        return self.service.health()["udp_port"]

    @property
    def tcp_port(self) -> int:
        return self.service.health()["tcp_port"]


@pytest.fixture
def stack(tmp_path):
    auth = AuthoritativeTestServer(port=0).start()
    service = MiddlewareService(
        udp_port=0,
        tcp_port=0,
        upstream_port=auth.port,
        upstream_timeout=0.8,
        max_cname_hops=4,
        db_path=tmp_path / "cache.sqlite3",
        admin_port=-1,
        purge_interval=0.05,
    ).start()
    try:
        yield Stack(auth=auth, service=service)
    finally:
        service.stop()
        auth.stop()
