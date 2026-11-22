from __future__ import annotations

import base64
from pathlib import Path

from fastapi.testclient import TestClient

from diaglab.app import create_app


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"


def b64(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("ascii")


def test_preflight_import_export_reimport_through_http(tmp_path: Path):
    app = create_app(tmp_path / "api.sqlite3")
    with TestClient(app) as client:
        page = client.get("/")
        assert page.status_code == 200
        assert "Loopback Margin Lab" in page.text

        preflight = client.post(
            "/api/preflight",
            json={"content_base64": b64(SAMPLES / "binary_v2.bin"), "timestamp_policy": "truncate"},
        )
        assert preflight.status_code == 200
        assert preflight.json()["negotiation"]["decision"] == "accept"
        assert preflight.json()["persisted"] is False

        imported = client.post(
            "/api/imports",
            json={
                "content_base64": b64(SAMPLES / "binary_v2.bin"),
                "timestamp_policy": "truncate",
                "batch_id": "api-import-1",
            },
        )
        assert imported.status_code == 200
        result = imported.json()
        assert result["records_persisted"] == 21
        version = result["commit"]["version"]

        duplicate = client.post(
            "/api/imports",
            json={
                "content_base64": b64(SAMPLES / "binary_v2.bin"),
                "timestamp_policy": "truncate",
                "batch_id": "api-import-1",
            },
        )
        assert duplicate.status_code == 200
        assert duplicate.json()["reused"] is True
        assert duplicate.json()["commit"]["version"] == version

        conflict = client.post(
            "/api/imports",
            json={
                "content_base64": b64(SAMPLES / "jsonl_v2.jsonl"),
                "timestamp_policy": "truncate",
                "batch_id": "api-import-1",
            },
        )
        assert conflict.status_code == 409
        assert conflict.json()["code"] == "E_CONFLICTING_BATCH"

        binary_export = client.get("/api/exports/binary?endian=big")
        assert binary_export.status_code == 200
        assert binary_export.content[:4] == b"DXPK"
        assert binary_export.headers["x-diaglab-snapshot-version"] == str(version)
        jsonl_export = client.get("/api/exports/jsonl")
        assert jsonl_export.status_code == 200
        assert len(jsonl_export.text.strip().splitlines()) == 21

        reimport = client.post(
            "/api/imports",
            json={
                "content_base64": base64.b64encode(jsonl_export.content).decode("ascii"),
                "timestamp_policy": "truncate",
                "batch_id": "api-import-2",
            },
        )
        assert reimport.status_code == 200
        assert reimport.json()["commit"]["version"] == version + 1

        stats = client.get("/api/stats?path_node=local-edge-a&min_samples=5")
        assert stats.status_code == 200
        assert stats.json()["loss"]["percent"] == "12.50"

        sparse = client.get("/api/stats?path_node=local-edge-a&min_samples=20")
        assert sparse.status_code == 422
        assert sparse.json()["code"] == "E_INSUFFICIENT_SAMPLES"
        assert sparse.json()["details"]["required_samples"] == 20
        assert sparse.json()["details"]["actual_samples"] == 14

        roots = client.get("/api/root-causes?min_samples=5")
        assert roots.status_code == 200
        assert roots.json()["candidates"][0]["path_node"] == "local-edge-b"


def test_rejected_package_does_not_change_stored_version(tmp_path: Path):
    app = create_app(tmp_path / "reject.sqlite3")
    with TestClient(app) as client:
        imported = client.post(
            "/api/imports",
            json={
                "content_base64": b64(SAMPLES / "binary_v2.bin"),
                "timestamp_policy": "truncate",
            },
        )
        version = imported.json()["commit"]["version"]
        for name, code, status in (
            ("binary_v3_unsupported.bin", "E_FORMAT_UNSUPPORTED", 415),
            ("binary_bad_crc.bin", "E_FRAME_CORRUPT", 422),
            ("jsonl_unit_invalid.jsonl", "E_UNIT_INVALID", 422),
        ):
            response = client.post(
                "/api/imports",
                json={"content_base64": b64(SAMPLES / name), "timestamp_policy": "truncate"},
            )
            assert response.status_code == status
            assert response.json()["code"] == code
        overview = client.get("/api/overview").json()
        assert overview["snapshot_version"] == version
        assert overview["record_count"] == 21


def test_degraded_import_conclusions_are_queryable(tmp_path: Path):
    app = create_app(tmp_path / "degrade.sqlite3")
    with TestClient(app) as client:
        response = client.post(
            "/api/imports",
            json={
                "content_base64": b64(SAMPLES / "binary_v1_degraded.bin"),
                "timestamp_policy": "truncate",
                "batch_id": "degraded-api",
            },
        )
        assert response.status_code == 200
        assert response.json()["negotiation"]["decision"] == "degrade_accept"
        batch = client.get("/api/imports/degraded-api").json()
        assert batch["negotiations"][0]["decision"] == "degrade_accept"
        details = batch["negotiations"][0]["details"]
        assert details["normalization"]["degradation"]["missing_rtt_count"] == 2
        negotiations = client.get("/api/negotiations").json()
        assert negotiations["negotiations"][0]["batch_id"] == "degraded-api"
