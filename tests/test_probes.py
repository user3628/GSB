from __future__ import annotations

import asyncio
import tempfile
import time

from svc_registry.api import create_app
from svc_registry.db import Database
from svc_registry.models import NormalizedRegistration
from svc_registry.probes import execute_probe
from svc_registry.scheduler import ProbeScheduler
from svc_registry.service import RegistryService


def _registration(
    instance_id: str,
    endpoint: str,
    method: str,
    params: dict[str, object],
    *,
    ttl_ms: int = 30_000,
) -> NormalizedRegistration:
    return NormalizedRegistration(
        service_name="probe",
        instance_id=instance_id,
        endpoint=endpoint,
        weight_permille=500,
        lease_ttl_ms=ttl_ms,
        probe_method=method,
        probe_params=params,
        registered_at_ms=1_700_000_000_000,
        lease_version=1,
        last_heartbeat_at_ms=1_700_000_000_000,
    )


def test_real_http_tcp_lease_and_timeout_probes(
    http_endpoint: str,
    tcp_endpoint: str,
    refused_tcp_endpoint: str,
) -> None:
    path = tempfile.mktemp(suffix=".db")
    service = RegistryService(path)

    async def scenario() -> None:
        cases = [
            _registration(
                "http-ok",
                f"{http_endpoint}/ok",
                "http",
                {"timeout_ms": 500, "expected_status": 200, "expected_body_contains": "OK"},
            ),
            _registration("tcp-ok", tcp_endpoint, "tcp", {"timeout_ms": 500}),
            _registration("tcp-refused", refused_tcp_endpoint, "tcp", {"timeout_ms": 200}),
            _registration(
                "http-slow",
                f"{http_endpoint}/slow?delay_ms=700",
                "http",
                {"timeout_ms": 120, "expected_status": 200, "expected_body_contains": "OK"},
            ),
            _registration("lease", f"{http_endpoint}/ok", "lease", {"timeout_ms": 100}, ttl_ms=50),
        ]
        for case in cases:
            await service.register(case)
        scheduler = ProbeScheduler(service.db, max_concurrency=4, queue_size=4)
        report = await scheduler.run_round(budget_ms=3000)
        by_id = {item["instance_id"]: item for item in report["results"]}
        assert by_id["http-ok"]["success"] is True
        assert by_id["tcp-ok"]["success"] is True
        assert by_id["tcp-refused"]["success"] is False
        assert by_id["tcp-refused"]["outcome"] == "tcp_refused"
        assert by_id["http-slow"]["error_code"] == "E_PROBE_TIMEOUT"
        assert by_id["http-slow"]["latency_ms"] < 650
        assert by_id["lease"]["status"] == "EXPIRED"
        assert by_id["http-ok"]["weight_after"] == 500
        assert by_id["tcp-refused"]["weight_after"] == 450
        history = scheduler.history(limit=20)
        assert len(history) == 5
        assert any(item["error_code"] == "E_PROBE_TIMEOUT" for item in history)
        await scheduler.stop()

    asyncio.run(scenario())
    time.sleep(0.8)
    from tests.conftest import ProbeHandler

    assert ProbeHandler.broken_slow_responses >= 1


def test_scheduler_queue_is_bounded_at_concurrency_plus_queue(http_endpoint: str) -> None:
    path = tempfile.mktemp(suffix=".db")
    service = RegistryService(path)

    async def scenario() -> None:
        for index in range(5):
            await service.register(
                _registration(
                    str(index),
                    f"{http_endpoint}/ok",
                    "http",
                    {"timeout_ms": 500, "expected_status": 200, "expected_body_contains": "OK"},
                )
            )
        scheduler = ProbeScheduler(service.db, max_concurrency=2, queue_size=2)
        report = await scheduler.run_round(budget_ms=3000)
        assert report["counts"] == {"requested": 5, "applied": 4, "skipped": 0, "errors": 1}
        rejected = [item for item in report["results"] if item.get("error_code") == "E_PROBE_QUEUE_FULL"]
        assert len(rejected) == 1
        await scheduler.stop()

    asyncio.run(scenario())
