from __future__ import annotations

from fastapi.testclient import TestClient

from app.http_api import create_app


def test_http_end_to_end(service, samples_dir):
    client = TestClient(create_app(service))
    assert client.get("/health").json()["ok"] is True

    binary_resp = client.post(
        "/snapshots/import",
        content=(samples_dir / "snapshot_v1.bin").read_bytes(),
        headers={"content-type": "application/octet-stream"},
    )
    assert binary_resp.status_code == 200
    assert binary_resp.json()["negotiation"]["decision"] == "accept"

    json_resp = client.post(
        "/snapshots/import",
        content=(samples_dir / "snapshot_v2.jsonl").read_bytes(),
        headers={"content-type": "application/x-ndjson"},
    )
    assert json_resp.status_code == 200
    assert json_resp.json()["negotiation"]["decision"] == "degraded_accept"

    cycle_resp = client.post(
        "/snapshots/import", content=(samples_dir / "snapshot_cycle.jsonl").read_bytes()
    )
    assert cycle_resp.status_code == 409
    assert cycle_resp.json()["error"]["code"] == "E_DEPENDENCY_CYCLE"

    unsupported = client.post(
        "/snapshots/import", content=(samples_dir / "snapshot_v2_unsupported.bin").read_bytes()
    )
    assert unsupported.status_code == 422
    assert unsupported.json()["error"]["code"] == "E_VERSION_UNSUPPORTED"

    impact = client.get("/diagnostics/impact/svc-gateway")
    assert impact.status_code == 200
    assert "svc-orders" in impact.json()["impact_downstream"]

    exported = client.get("/snapshots/export/binary")
    assert exported.status_code == 200
    assert exported.content[:4] == b"ROWB"
