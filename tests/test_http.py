from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from leasecoord import Coordinator, CoordinatorConfig
from leasecoord.http_api import create_server

BASE = 1_760_000_000_000


class ProtocolClock:
    def __init__(self, now_ms: int = BASE) -> None:
        self.value = now_ms

    def now_ms(self) -> int:
        return self.value


@pytest.fixture()
def http_server(tmp_path: Path):
    config = CoordinatorConfig(
        exact_limit=8,
        heartbeat_timeout_ms=1000,
        min_lease_ttl_ms=1,
        allow_time_override=True,
    )
    coordinator = Coordinator(
        tmp_path / "http.sqlite",
        config,
        clock=ProtocolClock(),
        auto_recover=True,
    )
    server = create_server(coordinator, "127.0.0.1", 0, allow_time_override=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield coordinator, server.server_address[1]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def call(
    port: int,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    *,
    now_ms: int = BASE,
) -> tuple[int, dict[str, Any]]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=data,
        method=method,
        headers={
            "Content-Type": "application/json",
            "X-Coordinator-Now-Ms": str(now_ms),
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def register_http(port: int, node_id: str, priority: int, now_ms: int = BASE) -> None:
    status, payload = call(
        port,
        "POST",
        "/v1/nodes",
        {
            "id": node_id,
            "priority": priority,
            "lease_ttl_ms": 5000,
            "last_heartbeat_ms": now_ms,
        },
        now_ms=now_ms,
    )
    assert status == 200
    assert payload["ok"] is True


def test_http_concurrent_campaign_and_error_contract(http_server) -> None:
    coordinator, port = http_server
    for node_id, priority in [("alpha", 90), ("bravo", 90), ("charlie", 80)]:
        register_http(port, node_id, priority)

    def campaign(node_id: str) -> tuple[int, dict[str, Any]]:
        return call(
            port,
            "POST",
            "/v1/elections/campaign",
            {"node_id": node_id, "expected_lease_version": 0},
        )

    with ThreadPoolExecutor(max_workers=6) as executor:
        responses = list(executor.map(campaign, ["alpha", "bravo", "charlie"] * 2))
    successes = [payload for status, payload in responses if status == 200]
    failures = [payload for status, payload in responses if status != 200]
    assert len(successes) == 1
    assert len(failures) == 5
    assert all(item["error"]["code"] == "E_LEASE_HELD" for item in failures)
    assert all("holder_id" in item["error"]["context"] for item in failures)

    status, leader = call(port, "GET", "/v1/leader")
    assert status == 200
    assert leader["has_leader"] is True
    assert leader["leader"]["id"] == "alpha"
    assert leader["lease"]["state"] == "GRANTED"
    assert leader["lease"]["holder_status"] == "LEADER"
    assert leader["lease_version"] == 1

    status, error = call(
        port,
        "POST",
        "/v1/elections/campaign",
        {"node_id": "does-not-exist"},
    )
    assert status == 422
    assert error["error"]["code"] == "E_NODE_INVALID"
    assert error["error"]["context"]["reason"] == "node_not_registered"

    status, error = call(
        port,
        "POST",
        "/v1/elections/campaign",
        {"node_id": "charlie", "expected_lease_version": 0},
    )
    assert status == 409
    assert error["error"]["code"] == "E_LEASE_HELD"

    status, error = call(
        port,
        "POST",
        "/v1/lease/renew",
        {"node_id": "charlie", "expected_lease_version": 1},
    )
    assert status == 409
    assert error["error"]["code"] == "E_LEASE_HELD"

    status, error = call(
        port,
        "POST",
        "/v1/lease/renew",
        {"node_id": "alpha", "expected_lease_version": 0},
    )
    assert status == 409
    assert error["error"]["code"] == "E_STALE_LEASE"

    status, error = call(
        port,
        "POST",
        "/v1/nodes/charlie/transition",
        {"target_status": "LEADER", "expected_lease_version": 1},
    )
    assert status == 409
    assert error["error"]["code"] == "E_INVALID_TRANSITION"

    first_renew = call(
        port,
        "POST",
        "/v1/lease/renew",
        {
            "node_id": "alpha",
            "expected_lease_version": 1,
            "request_id": "http-req-1",
        },
        now_ms=BASE + 10,
    )
    assert first_renew[0] == 200
    assert first_renew[1]["lease_version"] == 2
    replay = call(
        port,
        "POST",
        "/v1/lease/renew",
        {
            "node_id": "alpha",
            "expected_lease_version": 1,
            "request_id": "http-req-1",
        },
        now_ms=BASE + 10,
    )
    assert replay[0] == 200
    assert replay[1]["replayed"] is True
    conflict = call(
        port,
        "POST",
        "/v1/lease/renew",
        {
            "node_id": "alpha",
            "expected_lease_version": 2,
            "request_id": "http-req-1",
        },
        now_ms=BASE + 20,
    )
    assert conflict[0] == 409
    assert conflict[1]["error"]["code"] == "E_CONFLICTING_REQUEST"


def test_http_recovery_and_read_consistency(http_server) -> None:
    coordinator, port = http_server
    register_http(port, "alpha", 100)
    register_http(port, "bravo", 50)
    status, campaign = call(
        port,
        "POST",
        "/v1/elections/campaign",
        {"node_id": "alpha", "expected_lease_version": 0},
    )
    assert status == 200
    assert campaign["lease_version"] == 1

    status, payload = call(port, "GET", "/v1/recovery")
    assert status == 200
    assert payload["facts_source"] == "sqlite_committed_state_only"
    assert "heartbeat_recheck" in payload

    status, payload = call(port, "GET", "/v1/nodes")
    assert status == 200
    assert payload["node_count"] == 2

    # At +1500 ms the stored lease is still GRANTED, but the effective query
    # must not expose GRANTED + a SUSPECT holder.
    status, payload = call(port, "GET", "/v1/lease", now_ms=BASE + 1500)
    assert status == 200
    assert payload["lease"]["stored_state"] == "GRANTED"
    assert payload["lease"]["state"] == "EXPIRED"
    assert payload["lease"]["holder_status"] == "SUSPECT"
    assert payload["leader"] is None

    status, error = call(
        port,
        "POST",
        "/v1/lease/failover",
        {"expected_lease_version": 1},
        now_ms=BASE + 1500,
    )
    assert status == 409
    assert error["error"]["code"] == "E_NO_ELIGIBLE_CANDIDATE"
    assert error["error"]["context"]["preserved_lease_version"] == 1
