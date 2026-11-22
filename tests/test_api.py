from __future__ import annotations

from fastapi.testclient import TestClient

from app.api import create_app
from app.service import GovernanceService
from tests.conftest import T0, sample


def test_api_smoke_unified_errors_and_pinned_pagination(tmp_path):
    service = GovernanceService(str(tmp_path / "api.sqlite"))
    app = create_app(service)
    with TestClient(app) as client:
        assert client.get("/health").json()["status"] == "ok"
        missing = client.get("/api/not-a-route")
        assert missing.status_code == 404
        assert missing.json()["error"]["code"] == "E_NOT_FOUND"
        loaded = client.put("/api/snapshot", json=sample("policies_hierarchical_v2.json"))
        assert loaded.status_code == 200
        assert loaded.json()["version"] == 2

        first = client.post(
            "/api/decisions",
            json={
                "request_id": "api-1",
                "tenant_id": "starter",
                "namespace": "shared-prod",
                "arrival_ms": T0 + 500,
                "cpu_millicores": 100,
                "memory_bytes": 1024,
            },
        )
        assert first.status_code == 200
        assert first.json()["decision"] == "ACCEPTED"

        replay = client.post(
            "/api/decisions",
            json={
                "request_id": "api-1",
                "tenant_id": "starter",
                "namespace": "shared-prod",
                "arrival_ms": T0 + 500,
                "cpu_millicores": 100,
                "memory_bytes": 1024,
            },
        )
        assert replay.status_code == 200
        assert replay.headers["Idempotent-Replayed"] == "true"
        assert replay.json() == first.json()

        conflict = client.post(
            "/api/decisions",
            json={
                "request_id": "api-1",
                "tenant_id": "starter",
                "namespace": "shared-prod",
                "arrival_ms": T0 + 600,
                "cpu_millicores": 101,
                "memory_bytes": 1024,
            },
        )
        assert conflict.status_code == 409
        assert conflict.json()["error"]["code"] == "E_CONFLICTING_REQUEST"

        rejected = client.post(
            "/api/decisions",
            json={
                "request_id": "api-rate",
                "tenant_id": "starter",
                "namespace": "shared-prod",
                "arrival_ms": T0 + 1000,
                "cpu_millicores": 100,
                "memory_bytes": 1024,
            },
        )
        # starter refills exactly one token over the next 500 ms at 2 tokens/s.
        assert rejected.status_code == 200
        rate_limited = client.post(
            "/api/decisions",
            json={
                "request_id": "api-rate-2",
                "tenant_id": "starter",
                "namespace": "shared-prod",
                "arrival_ms": T0 + 1200,
                "cpu_millicores": 100,
                "memory_bytes": 1024,
            },
        )
        assert rate_limited.status_code == 429
        assert rate_limited.json()["error"]["code"] == "E_RATE_LIMITED"
        assert "decision" in rate_limited.json()

        invalid = client.post(
            "/api/decisions",
            json={"request_id": "bad"},
        )
        assert invalid.status_code == 400
        assert invalid.json()["error"]["code"] == "E_SPEC_INVALID"

        page = client.get("/api/decisions", params={"page": 1, "page_size": 2})
        assert page.status_code == 200
        revision = page.json()["revision"]
        full = client.get(
            "/api/decisions", params={"page": 1, "page_size": 100, "revision": revision}
        ).json()
        concatenated = []
        first_part = client.get(
            "/api/decisions",
            params={"page": 1, "page_size": 2, "revision": revision},
        ).json()
        for number in range(1, first_part["pages"] + 1):
            part = client.get(
                "/api/decisions",
                params={"page": number, "page_size": 2, "revision": revision},
            ).json()
            concatenated.extend(part["items"])
        assert concatenated == full["items"]

        tenants = client.get("/api/tenants", params={"revision": revision})
        assert tenants.status_code == 200
        assert tenants.json()["snapshot_version"] == 2

        pending = client.post(
            "/api/migrations", json={"legacy": sample("migration_conflict.json")}
        )
        assert pending.status_code == 409
        assert pending.json()["error"]["code"] == "E_MIGRATION_REVIEW"
        migration_id = pending.json()["migration"]["migration_id"]
        replay = client.post(f"/api/migrations/{migration_id}/replay")
        assert replay.status_code == 200
        assert replay.json()["replayed"] is True
        assert replay.json()["replay_mode"] == "AUDIT_ONLY_NO_STATE_CHANGE"

        recovery = client.get("/api/recovery/latest")
        assert recovery.status_code == 200
        assert recovery.json()["replayable"] is True
