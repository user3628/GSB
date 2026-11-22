from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from dns_workbench.api import create_app
from dns_workbench.config import SchedulerConfig

from .conftest import running_stack


@pytest.mark.asyncio
async def test_http_success_cache_session_trace_and_recovery(tmp_path: Path) -> None:
    async with running_stack(tmp_path, enable_dns=False) as stack:
        app = create_app(stack.runtime)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as client:
            response = await client.post(
                "/api/v1/queries", json={"name": "a.lab.test.", "type": "A"}
            )
            assert response.status_code == 200
            payload = response.json()
            query_id = payload["result"]["query_id"]
            round_id = payload["round_id"]
            assert payload["result"]["answers"][0]["rdata"]["address"] == "192.0.2.10"

            cache = await client.get("/api/v1/cache", params={"name": "A.LAB.TEST.", "type": "A"})
            assert cache.status_code == 200
            assert cache.json()["count"] == 1
            assert cache.json()["entries"][0]["remaining_ttl_ms"] > 0

            session = await client.get(f"/api/v1/sessions/{query_id}")
            assert session.status_code == 200
            assert session.json()["state"] == "complete"
            assert session.json()["round"]["trace"]["dispatch"]

            round_response = await client.get(f"/api/v1/rounds/{round_id}")
            assert round_response.status_code == 200
            assert len(round_response.json()["sessions"]) == 1

            recovery = await client.get("/api/v1/recovery")
            assert recovery.status_code == 200
            assert recovery.json()["source_of_truth"] == "sqlite_committed_rows"


@pytest.mark.asyncio
async def test_http_business_error_codes_are_json(tmp_path: Path) -> None:
    scheduler = SchedulerConfig(
        global_upstream_limit=1,
        per_authority_limit=1,
        queue_limit=0,
        round_budget_ms=1000,
        upstream_timeout_ms=100,
    )
    async with running_stack(tmp_path, scheduler=scheduler, enable_dns=False) as stack:
        app = create_app(stack.runtime)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as client:
            malformed = await client.post(
                "/api/v1/wire/query",
                json={"wire_hex": "0000010000010000000000000000"},
            )
            assert malformed.status_code == 400
            assert malformed.json()["error"]["code"] == "E_MESSAGE_INVALID"

            loop = await client.post(
                "/api/v1/queries", json={"name": "loop1.lab.test.", "type": "A"}
            )
            assert loop.status_code == 502
            assert loop.json()["error"]["code"] == "E_CNAME_LOOP"
            assert "trace_summary" in loop.json()["error"]["context"]

            timeout = await client.post(
                "/api/v1/queries",
                json={
                    "name": "timeout.lab.test.",
                    "type": "A",
                    "round_budget_ms": 1000,
                },
            )
            assert timeout.status_code == 504
            assert timeout.json()["error"]["code"] == "E_UPSTREAM_TIMEOUT"
            assert timeout.json()["error"]["context"]["socket_closed"] is True

            first = await client.post(
                "/api/v1/queries",
                json={"name": "a.lab.test.", "type": "A", "query_id": "api-conflict"},
            )
            assert first.status_code == 200
            conflict = await client.post(
                "/api/v1/queries",
                json={"name": "mx.lab.test.", "type": "MX", "query_id": "api-conflict"},
            )
            assert conflict.status_code == 409
            assert conflict.json()["error"]["code"] == "E_CONFLICTING_QUERY"

            queue_full = await client.post(
                "/api/v1/queries/batch?fail_fast=true",
                json={
                    "requests": [
                        {"name": "aaaa.lab.test.", "type": "AAAA"},
                        {"name": "slow.lab.test.", "type": "A"},
                    ],
                    "round_budget_ms": 1000,
                },
            )
            assert queue_full.status_code == 429
            assert queue_full.json()["error"]["code"] == "E_QUEUE_FULL"


@pytest.mark.asyncio
async def test_http_batch_reports_per_item_errors_and_idempotent_replay(tmp_path: Path) -> None:
    async with running_stack(tmp_path, enable_dns=False) as stack:
        app = create_app(stack.runtime)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as client:
            batch = await client.post(
                "/api/v1/queries/batch",
                json={
                    "requests": [
                        {"name": "a.lab.test.", "type": "A", "query_id": "batch-http"},
                        {"name": "loop1.lab.test.", "type": "A"},
                    ]
                },
            )
            assert batch.status_code == 200
            body = batch.json()
            assert body["items"][0]["business_code"] is None
            assert body["items"][1]["error"]["code"] == "E_CNAME_LOOP"
            assert body["trace"]["dispatch"]

            replay = await client.post(
                "/api/v1/queries",
                json={"name": "a.lab.test.", "type": "A", "query_id": "batch-http"},
            )
            assert replay.status_code == 200
            assert replay.json()["result"]["outcome"] == "idempotent_replay"

            missing = await client.get("/api/v1/cache/key/does-not-exist")
            assert missing.status_code == 404
            assert missing.json()["error"]["code"] == "E_CACHE_MISS"

            bad_type = await client.get("/api/v1/cache", params={"name": "a.lab.test.", "type": "BAD"})
            assert bad_type.status_code == 422
            assert bad_type.json()["error"]["code"] == "E_REQUEST_INVALID"

