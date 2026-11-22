from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from mmcoop.canonical import canonical_bytes
from mmcoop.errors import AppError
from mmcoop.pipeline import PipelineEngine
from mmcoop.service import WorkspaceService
from mmcoop.stages import STAGES

from .conftest import mutate_config


def _baseline(service: WorkspaceService) -> dict:
    result = service.build(verify_incremental=True)
    assert result["status"] == "completed"
    assert result["reused_stages"] == []
    assert result["rebuilt_stages"] == list(STAGES)
    return result


def test_full_build_status_results_and_atomic_files(built_service: WorkspaceService):
    status = built_service.status()
    assert status["status"] == "ready"
    assert status["checkpoint"]["latest_sequence"] == 4
    assert [row["state"] for row in status["stages"]] == ["valid"] * 4
    assert status["integrity"]["verdict"] == "normal"
    results = built_service.results()["result"]
    assert results["summary"]["verdict_counts"] == {"conflict": 1, "consistent": 2, "weak": 2}
    assert results["summary"]["missing_link_text_ids"] == ["t04"]
    assert results["summary"]["duplicate_content_groups"][0]["text_ids"] == ["t02", "t05"]
    for directory in (built_service.store.checkpoint_dir, built_service.store.artifact_dir):
        assert not list(directory.rglob("*.tmp.*"))


def test_staged_build_reuses_prior_stages(service: WorkspaceService):
    first = service.build(through_stage="text_encoding")
    assert first["status"] == "partial_completed"
    assert first["rebuilt_stages"] == ["text_encoding"]
    assert service.store.scan().latest_sequence == 1
    assert service.status()["earliest_affected_stage"] == "projection_alignment"

    second = service.build(through_stage="projection_alignment")
    assert second["reused_stages"] == ["text_encoding"]
    assert second["rebuilt_stages"] == ["projection_alignment"]
    assert service.store.scan().latest_sequence == 2

    third = service.build(through_stage="fusion_scoring")
    assert third["reused_stages"] == ["text_encoding", "projection_alignment"]
    assert third["rebuilt_stages"] == ["fusion_scoring"]
    assert service.store.scan().latest_sequence == 3

    fourth = service.build(through_stage="aggregation")
    assert fourth["reused_stages"] == ["text_encoding", "projection_alignment", "fusion_scoring"]
    assert fourth["rebuilt_stages"] == ["aggregation"]
    assert fourth["status"] == "completed"
    assert service.store.scan().latest_sequence == 4


def test_visual_feature_invalidates_from_projection_and_restores_baseline(service: WorkspaceService):
    baseline = _baseline(service)
    dataset = service._load()[1]
    original = list(dataset.image_map()["i01"]["feature"])
    changed = list(original)
    changed[0] = float(changed[0]) + 0.0001
    service.edit_image("i01", {"feature": changed})
    changed_result = service.build()
    assert changed_result["reused_stages"] == ["text_encoding"]
    assert changed_result["rebuilt_stages"] == [
        "projection_alignment",
        "fusion_scoring",
        "aggregation",
    ]
    assert changed_result["invalidations"][0]["stage"] == "projection_alignment"
    assert "visual_features_or_links_changed" in changed_result["invalidations"][0]["reason_codes"]

    service.edit_image("i01", {"feature": original})
    restored = service.build()
    assert restored["pipeline_fingerprint"] == baseline["pipeline_fingerprint"]
    assert restored["version_fingerprint"] == baseline["version_fingerprint"]


def test_text_content_and_language_metadata_have_different_earliest_stages(service: WorkspaceService):
    baseline = _baseline(service)
    dataset = service._load()[1]
    original_text = dataset.text_map()["t03"]["text"]
    original_lang = dataset.text_map()["t03"]["lang"]

    service.edit_text("t03", {"lang": "en"})
    language_result = service.build()
    assert language_result["rebuilt_stages"][0] == "fusion_scoring"
    assert language_result["reused_stages"] == ["text_encoding", "projection_alignment"]
    assert "language_or_source_metadata_changed" in language_result["invalidations"][0]["reason_codes"]
    service.edit_text("t03", {"lang": original_lang})
    service.build()

    service.edit_text("t03", {"text": original_text + " changed"})
    text_result = service.build()
    assert text_result["rebuilt_stages"][0] == "text_encoding"
    assert text_result["reused_stages"] == []
    service.edit_text("t03", {"text": original_text})
    restored = service.build()
    assert restored["pipeline_fingerprint"] == baseline["pipeline_fingerprint"]


def test_incremental_rebuild_allows_identical_downstream_artifacts(service: WorkspaceService):
    baseline = _baseline(service)
    original_text = service._load()[1].text_map()["t03"]["text"]
    case_only_change = original_text.replace("comparative", "COMPARATIVE")
    assert case_only_change != original_text
    service.edit_text("t03", {"text": case_only_change})
    changed = service.build()
    assert changed["status"] == "completed"
    assert changed["rebuilt_stages"] == list(STAGES)
    assert changed["pipeline_fingerprint"] == baseline["pipeline_fingerprint"]
    assert service.store.scan().latest_sequence == 8
    service.edit_text("t03", {"text": original_text})
    restored = service.build()
    assert restored["pipeline_fingerprint"] == baseline["pipeline_fingerprint"]


@pytest.mark.parametrize(
    ("mutator", "expected_stage", "expected_reused"),
    [
        (
            lambda raw: raw["fusion"]["weights"].__setitem__("similarity", 0.6),
            "fusion_scoring",
            ["text_encoding", "projection_alignment"],
        ),
        (
            lambda raw: raw["fusion"]["operator_confidence"].__setitem__("vision", 0.5),
            "fusion_scoring",
            ["text_encoding", "projection_alignment"],
        ),
        (
            lambda raw: raw["fusion"]["thresholds"].__setitem__("consistent", 0.8),
            "fusion_scoring",
            ["text_encoding", "projection_alignment"],
        ),
        (
            lambda raw: raw["aggregation"].__setitem__("missing_link_verdict", "conflict"),
            "aggregation",
            ["text_encoding", "projection_alignment", "fusion_scoring"],
        ),
        (
            lambda raw: raw.__setitem__("structure_version", "2026.11.2"),
            "text_encoding",
            [],
        ),
    ],
)
def test_configuration_changes_invalidate_earliest_stage(
    service: WorkspaceService,
    mutator,
    expected_stage: str,
    expected_reused: list[str],
):
    baseline = _baseline(service)
    original = service.config_path.read_text(encoding="utf-8")
    mutate_config(service.config_path, mutator)
    changed = service.build()
    assert changed["rebuilt_stages"][0] == expected_stage
    assert changed["reused_stages"] == expected_reused
    assert changed["invalidations"][0]["stage"] == expected_stage
    service.config_path.write_text(original, encoding="utf-8")
    restored = service.build()
    assert restored["pipeline_fingerprint"] == baseline["pipeline_fingerprint"]


def test_projection_matrix_change_invalidates_projection_only(service: WorkspaceService):
    baseline = _baseline(service)
    matrix_path = service.config_path.parent / "projection_matrix.json"
    original_text = matrix_path.read_text(encoding="utf-8")
    matrix = json.loads(original_text)
    matrix[0][0] = float(matrix[0][0]) + 0.01
    matrix_path.write_text(json.dumps(matrix, indent=2), encoding="utf-8")
    changed = service.build()
    assert changed["reused_stages"] == ["text_encoding"]
    assert changed["rebuilt_stages"][0] == "projection_alignment"
    assert "projection_matrix_changed" in changed["invalidations"][0]["reason_codes"]
    matrix_path.write_text(original_text, encoding="utf-8")
    restored = service.build()
    assert restored["pipeline_fingerprint"] == baseline["pipeline_fingerprint"]


def test_two_workspaces_produce_byte_identical_artifacts(service: WorkspaceService, tmp_path: Path):
    first = service.build(verify_incremental=True)
    other = WorkspaceService(service.config_path, service.data_dir, tmp_path / "workspace-2")
    second = other.build(verify_incremental=True)
    assert first["version_fingerprint"] == second["version_fingerprint"]
    assert first["pipeline_fingerprint"] == second["pipeline_fingerprint"]
    for stage in STAGES:
        left = service.store.current_stages()[stage]["artifact"]
        right = other.store.current_stages()[stage]["artifact"]
        assert canonical_bytes(left) == canonical_bytes(right)


def test_incremental_verification_reports_first_difference(service: WorkspaceService):
    _baseline(service)
    config, dataset = service._load()
    engine = PipelineEngine(config, dataset, service.data_dir)
    computation = engine.compute(reusable=service.store.current_stages())
    computation.artifacts["fusion_scoring"]["records"][0]["fusion_score"] += 0.123
    with pytest.raises(AppError) as captured:
        engine.verify_against_full(computation)
    assert captured.value.code == "E_INCREMENTAL_MISMATCH"
    assert captured.value.stage == "fusion_scoring"
    difference = captured.value.diagnostics["difference"]
    assert difference["path"].startswith("$.records[0]")
    assert captured.value.diagnostics["incremental_sha256"] != captured.value.diagnostics["full_sha256"]


def test_graceful_stop_does_not_advance_checkpoint_and_resume_completes(service: WorkspaceService):
    baseline = _baseline(service)
    before = service.store.scan().latest_sequence
    service.request_stop()
    stopped = service.build()
    assert stopped["status"] == "stopped"
    assert service.store.scan().latest_sequence == before
    assert service.store.stop_requested() is True
    resumed = service.resume()
    assert resumed["status"] == "completed"
    assert service.store.stop_requested() is False
    assert resumed["pipeline_fingerprint"] == baseline["pipeline_fingerprint"]


def test_duplicate_and_stale_publication_errors(built_service: WorkspaceService):
    scan = built_service.store.scan()
    stage1 = scan.stages["text_encoding"]
    with pytest.raises(AppError) as duplicate:
        built_service.store.publish(
            stage1["artifact"],
            input_fingerprint=stage1["input_fingerprint"],
            components=stage1["components"],
            config_version=stage1["config_version"],
        )
    assert duplicate.value.code == "E_STALE_CHECKPOINT"

    stage1_artifact = json.loads(json.dumps(stage1["artifact"]))
    stage1_artifact["summary"]["text_count"] += 100
    with pytest.raises(AppError) as sequence:
        built_service.store.publish(
            stage1_artifact,
            input_fingerprint="different-fingerprint",
            components=stage1["components"],
            config_version=stage1["config_version"],
            expected_sequence=999,
        )
    assert sequence.value.code == "E_STALE_CHECKPOINT"
    assert sequence.value.diagnostics["expected_sequence"] == 999


def test_missing_prerequisite_and_cross_stage_publish_errors(service: WorkspaceService):
    config, dataset = service._load()
    engine = PipelineEngine(config, dataset, service.data_dir)
    computation = engine.compute()
    with pytest.raises(AppError) as missing:
        service.store.publish(
            computation.artifacts["projection_alignment"],
            input_fingerprint=computation.input_fingerprints["projection_alignment"],
            components=computation.components["projection_alignment"],
            config_version=config.digest,
        )
    assert missing.value.code == "E_INVALID_STAGE"
    assert missing.value.diagnostics["required_stage"] == "text_encoding"

    service.build(through_stage="projection_alignment")
    stage3 = json.loads(json.dumps(computation.artifacts["fusion_scoring"]))
    stage3["upstream"]["projection_alignment"] = "0" * 64
    with pytest.raises(AppError) as stale:
        service.store.publish(
            stage3,
            input_fingerprint="cross-stage",
            components=computation.components["fusion_scoring"],
            config_version=config.digest,
        )
    assert stale.value.code == "E_STALE_CHECKPOINT"
    assert stale.value.diagnostics["required_upstream"] == "projection_alignment"
