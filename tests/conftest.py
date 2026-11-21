from __future__ import annotations

import asyncio
import socket
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

from vgw.gateway import Gateway
from vgw.models import PATCH_SCHEMA, SNAPSHOT_SCHEMA, PatchDocument, SnapshotDocument
from vgw.snapshot import compute_digest, validate_patch, validate_snapshot
from vgw.store import Store
from vgw.transport import LocalTestNetwork, Topology


def free_udp_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
    finally:
        sock.close()


def make_topology() -> Topology:
    east_carrier = free_udp_port()
    east_egress = free_udp_port()
    west_carrier = free_udp_port()
    west_egress = free_udp_port()
    return Topology.from_dict(
        {
            "egress_timeout_ms": 3000,
            "carriers": {
                "east": {
                    "listen": f"127.0.0.1:{east_carrier}",
                    "egress": f"127.0.0.1:{east_egress}",
                    "egress_name": "decap-east",
                },
                "west": {
                    "listen": f"127.0.0.1:{west_carrier}",
                    "egress": f"127.0.0.1:{west_egress}",
                    "egress_name": "decap-west",
                },
            },
        }
    )


def make_snapshot(
    topology: Topology,
    *,
    channel_count: int = 3,
    queue_capacity: int = 4,
    round_budget_ms: int = 25,
    default_timeout_ms: int = 1200,
    bindings: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    endpoint_addresses = {
        spec.name: spec.listen_address for spec in topology.carriers
    }
    if bindings is None:
        bindings = {
            "alpha": {"priority": 30, "channel": 0, "egress": "east", "max_payload": 4096},
            "beta": {"priority": 20, "channel": 1, "egress": "west", "max_payload": 4096},
            "gamma": {"priority": 10, "channel": 2, "egress": "east", "max_payload": 4096},
        }
    raw = {
        "schema": SNAPSHOT_SCHEMA,
        "version": 1,
        "generated_at": "2026-11-22T00:00:00.000Z",
        "channel_count": channel_count,
        "round_budget_ms": round_budget_ms,
        "queue_capacity": queue_capacity,
        "default_timeout_ms": default_timeout_ms,
        "egress_endpoints": endpoint_addresses,
        "bindings": bindings,
        "digest": "sha256:" + "0" * 64,
    }
    model = SnapshotDocument.model_validate(raw)
    raw["digest"] = compute_digest(model)
    validate_snapshot(raw)
    return raw


def make_patch(
    *,
    base_version: int,
    bindings: dict[str, dict[str, Any]],
    egress_endpoints: dict[str, str] | None = None,
    version: int | None = None,
) -> dict[str, Any]:
    raw = {
        "schema": PATCH_SCHEMA,
        "base_version": base_version,
        "version": version if version is not None else base_version + 1,
        "generated_at": "2026-11-22T00:10:00.000Z",
        "bindings": bindings,
        "egress_endpoints": egress_endpoints or {},
        "digest": "sha256:" + "0" * 64,
    }
    model = PatchDocument.model_validate(raw)
    raw["digest"] = compute_digest(model)
    validate_patch(raw)
    return raw


@asynccontextmanager
async def running_gateway(
    tmp_path: Path,
    *,
    topology: Topology | None = None,
    snapshot: dict[str, Any] | None = None,
    channel_count: int = 3,
    queue_capacity: int = 4,
    round_budget_ms: int = 25,
    default_timeout_ms: int = 1200,
    bindings: dict[str, dict[str, Any]] | None = None,
):
    topology = topology or make_topology()
    snapshot = snapshot or make_snapshot(
        topology,
        channel_count=channel_count,
        queue_capacity=queue_capacity,
        round_budget_ms=round_budget_ms,
        default_timeout_ms=default_timeout_ms,
        bindings=bindings,
    )
    network = LocalTestNetwork(topology)
    await network.start()
    store = Store(tmp_path / "gateway.db")
    gateway = Gateway(store)
    await gateway.start()
    await gateway.load_snapshot(snapshot)
    try:
        yield gateway, store, network, topology, snapshot
    finally:
        await gateway.stop()
        store.close()
        await network.stop()


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "asyncio: run the test in a fresh asyncio event loop")


def pytest_pyfunc_call(pyfuncitem: pytest.Function) -> bool | None:
    if pyfuncitem.get_closest_marker("asyncio") is None:
        return None
    kwargs = {
        name: pyfuncitem.funcargs[name]
        for name in pyfuncitem._fixtureinfo.argnames
        if name in pyfuncitem.funcargs
    }
    asyncio.run(pyfuncitem.obj(**kwargs))
    return True

