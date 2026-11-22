from __future__ import annotations

import concurrent.futures
import time
import uuid

import httpx

from tests.support import (
    current_version,
    forward_route,
    free_port,
    install_full_snapshot,
    reverse_route,
)


def test_dashboard_and_budget_diagnostics_are_available(live_workbench: dict) -> None:
    base_url = str(live_workbench["base_url"])
    services = live_workbench["services"]
    install_full_snapshot(
        base_url,
        [
            reverse_route("api", "/api", services.backend_port),
            forward_route(services.egress_port, services.backend_port),
        ],
    )
    dashboard = httpx.get(base_url + "/", timeout=4.0)
    assert dashboard.status_code == 200
    assert "Diagnostics Workbench" in dashboard.text
    explain = httpx.get(
        base_url + "/workbench/api/budgets/explain?route_id=api", timeout=4.0
    ).json()
    assert explain["route"]["budget"]["cumulative_rounding_error_q6"]["ratio"] != "0/1"
    benchmark = httpx.get(
        base_url + "/workbench/api/budgets/benchmark", timeout=4.0
    ).json()
    assert benchmark["naive_float64"]["error_q6"] != 0
    assert benchmark["kahan_float64"]["error_q6"] == 0


def test_reverse_and_forward_proxy_contract(live_workbench: dict) -> None:
    base_url = str(live_workbench["base_url"])
    services = live_workbench["services"]
    install_full_snapshot(
        base_url,
        [
            reverse_route("api", "/api", services.backend_port),
            forward_route(services.egress_port, services.backend_port),
        ],
    )

    response = httpx.post(
        base_url + "/api/echo?alpha=1&beta=two",
        content=b"payload-123",
        headers={
            "X-Client-Header": "kept",
            "Connection": "X-Strip-Me",
            "X-Strip-Me": "must-not-forward",
            "Proxy-Authorization": "secret",
        },
        timeout=5.0,
    )
    assert response.status_code == 200
    assert response.headers["x-workbench-route"] == "api"
    payload = response.json()
    assert payload["instance"] == "primary"
    assert payload["method"] == "POST"
    assert payload["path"] == "/api/echo"
    assert payload["query"] == "alpha=1&beta=two"
    assert payload["body_text"] == "payload-123"
    assert payload["headers"]["x-client-header"] == "kept"
    assert payload["headers"]["x-test-route"] == "api"
    assert "x-strip-me" not in payload["headers"]
    assert "proxy-authorization" not in payload["headers"]

    forwarded = httpx.get(
        base_url + "/forward/echo?through=egress",
        headers={
            "X-Forward-Host": "127.0.0.1",
            "X-Forward-Port": str(services.backend_port),
            "X-Client-Header": "forwarded",
        },
        timeout=5.0,
    )
    assert forwarded.status_code == 200
    forwarded_payload = forwarded.json()
    assert forwarded_payload["path"] == "/echo"
    assert forwarded_payload["query"] == "through=egress"
    assert forwarded_payload["instance"] == "primary"
    assert forwarded_payload["headers"]["x-local-egress"].endswith(
        f":{services.egress_port}"
    )
    assert forwarded_payload["headers"]["x-client-header"] == "forwarded"


def test_upstream_5xx_read_timeout_and_connection_refusal_are_distinct(
    live_workbench: dict,
) -> None:
    base_url = str(live_workbench["base_url"])
    services = live_workbench["services"]
    closed_port = free_port()
    install_full_snapshot(
        base_url,
        [
            reverse_route("status", "/status", services.backend_port),
            reverse_route(
                "hang", "/hang", services.backend_port, timeout_ms=180, retries=0
            ),
            reverse_route(
                "refused", "/refused", closed_port, timeout_ms=150, retries=0
            ),
        ],
    )

    status_response = httpx.get(base_url + "/status/503", timeout=5.0)
    assert status_response.status_code == 503
    assert status_response.headers["x-workbench-upstream-failure"] == "upstream_5xx"
    assert status_response.json()["status"] == 503

    started = time.monotonic()
    timeout_response = httpx.get(base_url + "/hang", timeout=5.0)
    elapsed = time.monotonic() - started
    assert timeout_response.status_code == 504
    assert elapsed < 1.2
    timeout_payload = timeout_response.json()
    assert timeout_payload["business_code"] == "E_UPSTREAM_TIMEOUT"
    assert timeout_payload["details"]["stage"] == "read_timeout"

    refused_response = httpx.get(base_url + "/refused", timeout=5.0)
    assert refused_response.status_code == 502
    assert refused_response.json()["business_code"] == "E_UPSTREAM_UNAVAILABLE"
    assert refused_response.json()["details"]["stage"] == "connect"


def test_two_concurrent_increments_same_base_and_stale_state(live_workbench: dict) -> None:
    base_url = str(live_workbench["base_url"])
    services = live_workbench["services"]
    install_full_snapshot(
        base_url,
        [
            reverse_route("alpha", "/alpha", services.backend_port),
            reverse_route("beta", "/beta", services.backend_port),
        ],
    )
    base = current_version(base_url)
    first = {
        "base_version": base,
        "upserts": [reverse_route("alpha", "/alpha", services.alt_backend_port)],
    }
    second = {
        "base_version": base,
        "upserts": [reverse_route("beta", "/beta", services.alt_backend_port)],
    }

    def submit(payload: dict) -> tuple[int, dict]:
        with httpx.Client(timeout=8.0) as client:
            response = client.post(
                base_url + "/workbench/api/config/incremental", json=payload
            )
            return response.status_code, response.json()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(submit, [first, second]))
    codes = sorted(status for status, _ in results)
    assert codes == [200, 409]
    stale = next(payload for status, payload in results if status == 409)
    assert stale["business_code"] == "E_STALE_BASE"
    assert current_version(base_url) == base + 1
    state = httpx.get(base_url + "/workbench/api/state", timeout=5.0).json()
    assert state["version"] == base + 1


def test_in_flight_request_finishes_on_bound_version(live_workbench: dict) -> None:
    base_url = str(live_workbench["base_url"])
    services = live_workbench["services"]
    install_full_snapshot(
        base_url, [reverse_route("slow", "/slow", services.backend_port)]
    )
    base = current_version(base_url)

    def slow_request() -> dict:
        with httpx.Client(timeout=8.0) as client:
            return client.get(base_url + "/slow").json()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(slow_request)
        time.sleep(0.08)
        update = httpx.post(
            base_url + "/workbench/api/config/incremental",
            json={
                "base_version": base,
                "upserts": [reverse_route("slow", "/slow", services.alt_backend_port)],
            },
            timeout=8.0,
        )
        assert update.status_code == 200
        in_flight = future.result(timeout=5.0)
    assert in_flight["instance"] == "primary"
    new_request = httpx.get(base_url + "/slow", timeout=5.0).json()
    assert new_request["instance"] == "secondary"


def test_idempotent_request_is_forwarded_once_and_conflict_is_rejected(
    live_workbench: dict,
) -> None:
    base_url = str(live_workbench["base_url"])
    services = live_workbench["services"]
    install_full_snapshot(
        base_url,
        [
            reverse_route("slow", "/slow", services.backend_port),
            reverse_route("api", "/api", services.backend_port),
        ],
    )
    key = f"pytest-{uuid.uuid4()}"
    stats_url = f"http://127.0.0.1:{services.backend_port}/__stats"
    before = httpx.get(stats_url, timeout=3.0).json()["forwarded_requests"]

    def one_request(_: int) -> tuple[int, str]:
        with httpx.Client(timeout=8.0) as client:
            response = client.get(base_url + "/slow", headers={"X-Request-Id": key})
            return response.status_code, response.headers.get("x-workbench-request-id", "")

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(one_request, range(5)))
    assert [status for status, _ in results] == [200] * 5
    assert all(request_id == key for _, request_id in results)
    after = httpx.get(stats_url, timeout=3.0).json()["forwarded_requests"]
    assert after - before == 1

    conflict = httpx.post(
        base_url + "/api/echo",
        content=b"different request",
        headers={"X-Request-Id": key},
        timeout=5.0,
    )
    assert conflict.status_code == 409
    assert conflict.json()["business_code"] == "E_CONFLICTING_REQUEST"
    after_conflict = httpx.get(stats_url, timeout=3.0).json()["forwarded_requests"]
    assert after_conflict == after


def test_full_snapshot_replacement_is_visible_to_proxy_without_mixed_routes(
    live_workbench: dict,
) -> None:
    base_url = str(live_workbench["base_url"])
    services = live_workbench["services"]
    install_full_snapshot(
        base_url,
        [
            reverse_route("old-a", "/old-a", services.backend_port),
            reverse_route("old-b", "/old-b", services.backend_port),
        ],
    )
    install_full_snapshot(
        base_url, [reverse_route("new-only", "/new-only", services.backend_port)]
    )
    assert httpx.get(base_url + "/new-only/echo", timeout=5.0).status_code == 200
    missing = httpx.get(base_url + "/old-a/echo", timeout=5.0)
    assert missing.status_code == 404
    assert missing.json()["business_code"] == "E_ROUTE_NOT_FOUND"
