import json
import urllib.error
import urllib.request

import pytest

from graphstore.api import build_server


@pytest.fixture
def server(engine):
    srv = build_server(engine, "127.0.0.1", 0)
    host, port = srv.server_address
    srv.base_url = f"http://{host}:{port}"
    import threading

    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    yield srv
    srv.shutdown()


def call(server, path, payload=None):
    data = json.dumps(payload or {}).encode("utf-8")
    request = urllib.request.Request(
        server.base_url + path, data=data,
        headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def test_health_and_query_flow(server, sample_path):
    with urllib.request.urlopen(server.base_url + "/health", timeout=5) as response:
        health = json.loads(response.read())
    assert health["version"] == 1

    status, body = call(server, "/query", {
        "request_id": "api-1", "query": "multi_hop", "version": 1,
        "mode": "exact", "params": {"start": "p1", "max_hops": 1},
    })
    assert status == 200 and body["ok"]
    assert body["data"]["nodes_by_hop"]["1"]

    status, body = call(server, "/query", {
        "request_id": "api-1", "query": "multi_hop", "version": 1,
        "mode": "exact", "params": {"start": "p1", "max_hops": 1},
    })
    assert body["data"]["dedup"]["reused"] == "persisted"


def test_error_payload_uses_business_code(server):
    status, body = call(server, "/query", {
        "query": "multi_hop", "version": 1, "mode": "exact",
        "params": {"start": "missing", "max_hops": 1},
    })
    assert status == 400
    assert body["ok"] is False
    assert body["error"]["code"] == "E_GRAPH_INCONSISTENT"
    assert body["error"]["context"]["missing_node"] == "missing"


def test_version_retired_conflict_status(server):
    status, body = call(server, "/query", {
        "query": "multi_hop", "version": 42, "mode": "exact",
        "params": {"start": "p1", "max_hops": 1},
    })
    assert status == 409
    assert body["error"]["code"] == "E_VERSION_RETIRED"


def test_audit_and_verify_endpoints(server):
    status, audit = call(server, "/query/audit", {
        "query": "multi_hop", "version": 1,
        "fast_budget": {"node_budget": 4, "prefetch_budget": 3},
        "params": {"start": "p1", "max_hops": 2},
    })
    assert status == 200
    assert audit["data"]["diff"]["fast_subset_of_exact"]

    status, verify = call(server, "/verify", {"version": 1})
    assert verify["data"]["consistent"] is True
