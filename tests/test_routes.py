from __future__ import annotations

from gateway.domain import compute_route_checksum
from tests.helpers import sample_route


def test_route_checksum_endpoint_matches_submitted_checksum(gateway_factory, backend):
    client, _ = gateway_factory()
    document = sample_route(backend)
    response = client.post("/routes/checksum", json=document)
    assert response.status_code == 200
    calculated = response.json()["checksum"]
    document["checksum"] = calculated
    submitted = client.post("/routes", json=document)
    assert submitted.status_code == 200, submitted.text
    current = client.get("/routes/current").json()["route"]
    assert current["checksum"] == calculated


def test_stale_route_is_rejected_without_mutation(gateway_factory, backend):
    client, _ = gateway_factory()
    v1 = sample_route(backend, 1)
    assert client.post("/routes", json=v1).status_code == 200
    stale = client.post("/routes", json=v1)
    assert stale.status_code == 409
    assert stale.json()["code"] == "E_STALE_ROUTE"
    assert stale.json()["details"] == {"submitted": 1, "current": 1}
    assert len(client.get("/routes").json()["routes"]) == 1


def test_invalid_route_checksum_is_rejected(gateway_factory, backend):
    client, _ = gateway_factory()
    document = sample_route(backend)
    document["checksum"] = "sha256:" + "0" * 64
    response = client.post("/routes", json=document)
    assert response.status_code == 422
    assert response.json()["code"] == "E_ROUTE_INVALID"
    assert client.get("/routes").json()["routes"] == []


def test_route_replacement_does_not_mix_versions(gateway_factory, backend):
    client, _ = gateway_factory()
    v1 = sample_route(backend, 1)
    assert client.post("/routes", json=v1).status_code == 200
    request = {
        "request_id": "v1-bound",
        "model_id": "alpha",
        "sample_count": 1,
        "latency_cap_ms": 500,
        "priority": 1,
        "tenant_id": "tenant-v1",
    }
    assert client.post("/requests", json=request).status_code == 200
    v2 = sample_route(backend, 2)
    assert client.post("/routes", json=v2).status_code == 200
    current = client.get("/routes/current").json()["route"]
    assert current["version"] == 2
    assert current["backends"]["backend-a"]["vram_limit_mb"] == 320.0
    bound = client.get("/requests/v1-bound").json()["request"]
    assert bound["route_version"] == 1
    assert bound["endpoint"] == backend
    both = client.get("/backends?include_retired=true").json()["backends"]
    assert {row["route_version"] for row in both} == {1, 2}


def test_route_checksum_recomputation_after_canonicalization(gateway_factory, backend):
    client, _ = gateway_factory()
    document = sample_route(backend)
    assert document["checksum"] == compute_route_checksum(document)
    assert client.post("/routes", json=document).status_code == 200


def test_unknown_path_uses_unified_error_shape(gateway_factory, backend):
    client, _ = gateway_factory()
    response = client.get("/does-not-exist")
    assert response.status_code == 404
    assert response.json()["ok"] is False
    assert response.json()["code"] == "E_NOT_FOUND"

