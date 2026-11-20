from __future__ import annotations

import json
from pathlib import Path

import pytest

from ree.config import load_config
from ree.demo import run_demo
from ree.errors import REEError
from ree.models import canonical_bytes
from ree.pipeline import Pipeline


def _run(workspace: Path) -> dict:
    pipeline = Pipeline(workspace)
    return pipeline.run()


def test_full_run_constraints_and_fixed_roundtrips(workspace: Path) -> None:
    pipeline = Pipeline(workspace)
    result = pipeline.run()
    assert result["status"] == "complete"
    assert result["last_checkpoint_seq"] == 4
    assert result["validation_summary"]["passed_chain_count"] == 20
    assert result["validation_summary"]["failure_count"] == 0
    assert [record.stage for record in pipeline.checkpoints.records()] == [
        "parse",
        "estimate",
        "extract",
        "validate",
    ]
    assert [record.seq for record in pipeline.checkpoints.records()] == [1, 2, 3, 4]

    config = load_config(workspace / "config" / "extraction.yaml")
    for document in pipeline.get_documents():
        entities = {entity.id: entity for entity in document.entities}
        for relation in document.relations:
            spec = config.relation_types[relation.type]
            assert entities[relation.head].type in spec.head
            assert entities[relation.tail].type in spec.tail
        for event in document.events:
            spec = config.event_types[event.type]
            for role, members in event.arguments.items():
                allowed = set(spec.roles[role])
                assert members
                assert all(entities[entity_id].type in allowed for entity_id in members)


def test_stop_resume_is_byte_identical_to_clean_run(tmp_path: Path) -> None:
    from conftest import prepare_workspace

    clean_root = prepare_workspace(tmp_path / "clean")
    resumed_root = prepare_workspace(tmp_path / "resumed")

    clean = Pipeline(clean_root)
    clean.run()
    clean_extract = canonical_bytes(clean.get_stage_artifact("extract"))
    clean_validate = canonical_bytes(clean.get_stage_artifact("validate"))
    clean_sequences = [record.seq for record in clean.checkpoints.records()]

    resumed = Pipeline(resumed_root)
    stopped = resumed.run(stop_after_stage="estimate")
    assert stopped["status"] == "stopped"
    assert [record.seq for record in resumed.checkpoints.records()] == [1, 2]
    resumed.resume()
    assert canonical_bytes(resumed.get_stage_artifact("extract")) == clean_extract
    assert canonical_bytes(resumed.get_stage_artifact("validate")) == clean_validate
    resumed_sequences = [record.seq for record in resumed.checkpoints.records()]
    assert resumed_sequences == clean_sequences == [1, 2, 3, 4]


def test_version_invalidation_and_incremental_rebuild(tmp_path: Path) -> None:
    from conftest import prepare_workspace

    root = prepare_workspace(tmp_path / "invalidation")
    pipeline = Pipeline(root)
    pipeline.run()
    config_path = root / "config" / "extraction.yaml"
    original_estimate = pipeline.get_stage_artifact("estimate")
    original_extract = pipeline.get_stage_artifact("extract")

    original = config_path.read_text(encoding="utf-8")
    config_path.write_text(original.replace("smoothing: 0.5", "smoothing: 0.6"), encoding="utf-8")
    state = pipeline.status()
    assert state["stages"]["parse"]["state"] == "fresh"
    assert state["stages"]["estimate"]["state"] == "stale"
    assert state["stages"]["extract"]["state"] == "stale"
    assert state["stages"]["validate"]["state"] == "stale"
    latest = [
        item for item in state["invalidations"] if item["earliest_stage"] == "estimate"
    ][-1]
    assert latest["affected_stages"] == ["estimate", "extract", "validate"]
    assert latest["changed_components"] == ["smoothing"]
    assert latest["old_fingerprints"]["estimate"] != latest["new_fingerprints"]["estimate"]
    with pytest.raises(REEError) as captured:
        pipeline.get_stage_artifact("extract")
    assert captured.value.code == "E_STALE_ARTIFACT"
    assert captured.value.stage == "extract"

    config_path.write_text(original, encoding="utf-8")
    assert pipeline.status()["all_fresh"]
    assert canonical_bytes(pipeline.get_stage_artifact("estimate")) == canonical_bytes(
        original_estimate
    )

    config_path.write_text(
        original.replace(
            'MEETING: ["met", "meeting", "会谈", "会面", "held talks", "talks"]',
            'MEETING: ["met", "meeting", "会谈", "会面", "held talks", "talks", "gathered"]',
        ),
        encoding="utf-8",
    )
    extraction_state = pipeline.status()
    assert extraction_state["stages"]["parse"]["state"] == "fresh"
    assert extraction_state["stages"]["estimate"]["state"] == "fresh"
    assert extraction_state["stages"]["extract"]["state"] == "stale"
    with pytest.raises(REEError) as captured:
        pipeline.get_stage_artifact("extract")
    assert captured.value.code == "E_STALE_ARTIFACT"

    config_path.write_text(
        original.replace("confidence_threshold: 0.2", "confidence_threshold: 0.85"),
        encoding="utf-8",
    )
    rebuilt = pipeline.run()
    assert rebuilt["status"] == "complete"
    assert [record.seq for record in pipeline.checkpoints.records()] == [1, 2, 3, 4, 5, 6]
    assert [record.stage for record in pipeline.checkpoints.records()] == [
        "parse",
        "estimate",
        "extract",
        "validate",
        "extract",
        "validate",
    ]
    assert pipeline.status()["all_fresh"]
    assert canonical_bytes(pipeline.get_stage_artifact("extract")) != canonical_bytes(
        original_extract
    )


def test_stage_errors_and_missing_artifacts(workspace: Path) -> None:
    pipeline = Pipeline(workspace, runtime_dir=".ree_errors")
    with pytest.raises(REEError) as invalid:
        pipeline.run_stage("not-a-stage")
    assert invalid.value.code == "E_INVALID_STAGE"
    with pytest.raises(REEError) as missing:
        pipeline.run_stage("extract")
    assert missing.value.code == "E_MISSING_ARTIFACT"
    with pytest.raises(REEError) as artifact_missing:
        pipeline.get_stage_artifact("validate")
    assert artifact_missing.value.code == "E_MISSING_ARTIFACT"


def test_stop_is_checked_at_document_boundary(workspace: Path) -> None:
    pipeline = Pipeline(workspace, runtime_dir=".ree_stop_boundary")
    calls = {"count": 0}

    def stop_after_first_document() -> bool:
        calls["count"] += 1
        return calls["count"] >= 1

    artifact, stopped = pipeline._parse_stage(
        pipeline.load_config(),
        stop_requested=stop_after_first_document,
    )
    assert artifact is None
    assert stopped is not None
    assert stopped["status"] == "stopped"
    assert stopped["stage"] == "parse"
    assert stopped["doc_id"]
    assert pipeline.checkpoints.records() == []


def test_demo_is_repeatable_and_fixed_digest_is_stable(tmp_path: Path) -> None:
    from conftest import REPO_ROOT, prepare_workspace

    first_root = prepare_workspace(tmp_path / "demo_one")
    second_root = prepare_workspace(tmp_path / "demo_two")
    first = run_demo(first_root)
    second = run_demo(second_root)
    assert first["status"] == "ok"
    assert first["stop_resume"]["final_checkpoint_sequence"] == [1, 2, 3, 4]
    assert first["constraint_violation_count"] == 0
    assert first["validation"]["failure_count"] == 0
    assert first["error_injections"] == {
        "E_MISSING_ARTIFACT": "E_MISSING_ARTIFACT",
        "E_STALE_ARTIFACT": "E_STALE_ARTIFACT",
        "E_STALE_CHECKPOINT": "E_STALE_CHECKPOINT",
        "E_UNSUPPORTED_VERSION": "E_UNSUPPORTED_VERSION",
    }
    assert first["digests"]["run_sha256"] == second["digests"]["run_sha256"]
    expected = json.loads(
        (REPO_ROOT / "artifacts" / "expected_demo_summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert first["digests"]["run_sha256"] == expected["run_sha256"]
