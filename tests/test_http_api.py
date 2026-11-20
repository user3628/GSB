from __future__ import annotations

import copy
import json
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from conftest import finalized


def http_json(base_url: str, method: str, path: str, payload=None):
    body = None
    headers = {}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(base_url + path, data=body, headers=headers, method=method)
    try:
        with urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def test_http_query_state_and_snapshot_replacement(http_service, snapshot_document):
    _, _, base_url = http_service
    status, health = http_json(base_url, "GET", "/health")
    assert status == 200
    assert health["active_version"] == 1

    status, result = http_json(
        base_url,
        "POST",
        "/query",
        {
            "request_id": "http-query-1",
            "logical_table": "orders",
            "query_type": "eq",
            "shard_key_value": 7,
            "routing_version": 1,
        },
    )
    assert status == 200
    assert result["ok"] is True
    assert result["physical_tables"][0]["table"] == "orders_0003"
    assert [row["id"] for row in result["rows"]] == [10, 1003]

    tampered = copy.deepcopy(snapshot_document)
    tampered["snapshot_version"] = 2
    tampered["generated_at"] = "2026-11-21T06:00:00Z"
    status, error_body = http_json(base_url, "POST", "/snapshot", tampered)
    assert status == 422
    assert error_body["error"]["code"] == "E_SNAPSHOT_INVALID"

    replacement = copy.deepcopy(snapshot_document)
    replacement["snapshot_version"] = 2
    replacement["generated_at"] = "2026-11-21T06:00:00Z"
    replacement["tables"]["orders"]["algorithm"] = {
        "type": "hash",
        "modulus": 4,
        "seed": 20261121,
    }
    replacement = finalized(replacement)
    status, loaded = http_json(base_url, "POST", "/snapshot", replacement)
    assert status == 200
    assert loaded["active_version"] == 2

    status, state = http_json(base_url, "GET", "/state")
    assert status == 200
    assert state["active_version"] == 2
    assert state["caches"]["routing_version"] == 2
    assert len(state["snapshots"]) == 2

    status, cache = http_json(base_url, "GET", "/cache")
    assert status == 200
    assert cache["cache"]["tables"]["orders"]["cache_version"] == 2

    status, snapshots = http_json(base_url, "GET", "/snapshots")
    assert status == 200
    assert [item["version"] for item in snapshots["snapshots"]] == [2, 1]

    status, old_version = http_json(
        base_url,
        "GET",
        "/snapshots/1",
    )
    assert status == 200
    assert old_version["snapshot"]["snapshot_version"] == 1

    status, report = http_json(base_url, "GET", "/recovery/report")
    assert status == 200
    assert report["facts_available"] is True
    assert report["report"]["persisted_facts_only"] is True


def test_http_injections_have_distinct_json_business_codes(http_service):
    _, _, base_url = http_service
    injections = [
        "invalid_shard_key",
        "empty_physical_mapping",
        "version_rollback",
        "range_overlap",
        "execution_interrupt",
    ]
    responses = []
    for index, injection in enumerate(injections):
        status, body = http_json(
            base_url,
            "POST",
            "/injections",
            {
                "type": injection,
                "request_id": f"http-injection-{index}",
                "logical_table": "orders",
                "query_type": "scan",
                "routing_version": 1,
                "limit": 0,
            },
        )
        assert status != 200
        assert body["ok"] is False
        assert set(body["error"]) == {"code", "message", "context"}
        assert body["request_id"] == f"http-injection-{index}"
        assert body["error"]["context"]["injection"] == injection
        responses.append(body["error"]["code"])
    assert len(set(responses)) == 5

    status, repeated = http_json(
        base_url,
        "POST",
        "/injections",
        {
            "type": "invalid_shard_key",
            "request_id": "http-injection-0",
            "logical_table": "orders",
            "query_type": "scan",
            "routing_version": 1,
            "limit": 0,
        },
    )
    assert status == 400
    assert repeated["error"]["code"] == "E_SHARD_KEY_INVALID"


def test_http_patch_and_stale_base(http_service, patch_document):
    _, _, base_url = http_service
    status, patched = http_json(base_url, "POST", "/snapshot/patch", patch_document)
    assert status == 200
    assert patched["active_version"] == 2
    assert patched["rebuilt_tables"] == ["orders", "orders_view"]

    status, error_body = http_json(base_url, "POST", "/snapshot/patch", patch_document)
    assert status == 409
    assert error_body["error"]["code"] == "E_STALE_BASE"

    status, current = http_json(
        base_url,
        "POST",
        "/query",
        {
            "request_id": "http-v2-query",
            "logical_table": "orders",
            "query_type": "eq",
            "shard_key_value": 7,
            "routing_version": 2,
        },
    )
    assert status == 200
    assert current["physical_tables"][0]["table"] == "orders_0002"

