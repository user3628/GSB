from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from image_workbench.api import create_app
from image_workbench.service import WorkbenchService


def test_service_persists_pipeline_state(sample_dir: Path, tmp_path: Path) -> None:
    service = WorkbenchService(tmp_path, runtime_dir=tmp_path / "runtime")
    service.import_manifest(sample_dir / "manifest.json")
    first = service.run()
    restored = WorkbenchService(tmp_path, runtime_dir=tmp_path / "runtime")
    assert set(restored.fragments) == set(service.fragments)
    second = restored.run()
    assert first["summary_digest"] == second["summary_digest"]
    assert all(status["reused"] for fragment in second["stage_statuses"].values() for status in fragment.values())


def test_service_convert_and_import_exchange(sample_dir: Path, tmp_path: Path) -> None:
    service = WorkbenchService(tmp_path, runtime_dir=tmp_path / "runtime")
    service.import_manifest(sample_dir / "manifest.json")
    service.run()
    png = service.convert("fragment_a", "png", tmp_path / "out")
    imported = service.import_exchange(png["files"]["image"], descriptor_path=png["files"]["descriptor"])
    assert imported["mask_pixel_count"] == 25
    assert imported["pixel_digest"] == service.cache.fragments["fragment_a"].artifacts["export"].descriptor["image"]["source_pixel_digest"]
    jpeg = service.convert("fragment_a", "jpeg", tmp_path / "out")
    assert "jpeg_alpha_channel_dropped" in {item["code"] for item in jpeg["loss_items"]}


def test_service_updates_and_invalidation_preview(sample_dir: Path, tmp_path: Path) -> None:
    service = WorkbenchService(tmp_path, runtime_dir=tmp_path / "runtime")
    service.import_manifest(sample_dir / "manifest.json")
    service.run()
    updated = service.update_fragment("fragment_a", {"style_strength": 0.71})
    assert updated["preview_earliest_invalidated_stage"] == "style"
    assert "style strength changed" in updated["preview_reason"]


def test_internal_project_can_be_loaded_and_run(sample_dir: Path, tmp_path: Path) -> None:
    service = WorkbenchService(tmp_path, runtime_dir=tmp_path / "runtime")
    service.import_manifest(sample_dir / "manifest.json")
    service.run()
    project_path = tmp_path / "internal.json"
    service.export_project(project_path, artifact="style")
    restored = WorkbenchService(tmp_path, runtime_dir=tmp_path / "runtime2", autoload=False)
    restored.load_project_file(project_path)
    result = restored.run()
    assert result["summary_digest"]
    assert len(result["processed_order"]) == 5


def test_api_end_to_end_and_error_codes(sample_dir: Path, tmp_path: Path) -> None:
    app = create_app(tmp_path)
    client = TestClient(app)
    assert client.get("/api/health").json()["ok"] is True
    imported = client.post(
        "/api/project/import",
        json={"manifest_path": str(sample_dir / "manifest.json"), "legacy": False},
    )
    assert imported.status_code == 200, imported.text
    run_response = client.post("/api/run", json={"force": False})
    assert run_response.status_code == 200, run_response.text
    assert run_response.json()["run"]["summary_digest"]
    status = client.get("/api/status").json()["status"]
    assert status["snapshot"]["status"] == "normal"
    assert len(status["fragments"]) == 5
    fingerprints = client.get("/api/fragments/fragment_a/fingerprints").json()
    assert fingerprints["fingerprints"]["style"]["fingerprint"]

    bad = client.patch("/api/fragments/fragment_a", json={"changes": {"style_strength": 1.25}})
    assert bad.status_code == 422
    assert bad.json()["error"]["code"] == "E_NUMERIC"
    assert bad.json()["error"]["fragment_id"] == "fragment_a"
    assert bad.json()["error"]["pixel"] == [0, 0]

    bad_type = client.patch("/api/fragments/fragment_a", json={"changes": {"style_strength": "not-a-number"}})
    assert bad_type.status_code == 422
    assert bad_type.json()["error"]["code"] == "E_NUMERIC"

    conversion = client.post("/api/convert", json={"fragment_id": "fragment_a", "target_format": "jpeg"}).json()["conversion"]
    missing_descriptor = client.post(
        "/api/import-exchange",
        json={"image_path": conversion["files"]["image"], "descriptor_path": None},
    )
    assert missing_descriptor.status_code == 422
    assert missing_descriptor.json()["error"]["code"] == "E_FORMAT_LOSS"

    assert client.post("/api/control/stop").json()["control"]["stop_requested"] is True
    assert client.post("/api/control/resume").json()["control"]["stop_requested"] is False


def test_api_legacy_import_and_export(sample_dir: Path, tmp_path: Path) -> None:
    app = create_app(tmp_path)
    client = TestClient(app)
    response = client.post(
        "/api/project/import",
        json={"manifest_path": str(sample_dir / "legacy_manifest.json"), "legacy": True},
    )
    assert response.status_code == 200, response.text
    project = response.json()["project"]
    assert project["legacy_upgrade"]["source_schema_version"] == 1
    assert Path(project["legacy_upgrade"]["upgraded_manifest"]).exists()
    assert all(fragment_id.startswith("legacy-") for fragment_id in project["fragment_ids"])

