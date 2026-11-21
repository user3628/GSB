from __future__ import annotations

import json

from factmerge import FactMergeEngine
from factmerge.evidence import export_manifest_bytes
from factmerge.util import canonical_json


def test_stop_after_entities_then_resume_is_byte_identical(config, rows, expected, tmp_path):
    full = FactMergeEngine(config, rows, state_dir=tmp_path / "full", expected=expected).run()
    stopped = FactMergeEngine(config, rows, state_dir=tmp_path / "resume", expected=expected).run(
        stop_after_entities=2
    )
    assert stopped["status"] == "stopped"
    assert stopped["next_entity_id"] == "ENT-CEDAR"
    assert len(stopped["entity_results"]) == 2
    resumed = FactMergeEngine(config, rows, state_dir=tmp_path / "resume", expected=expected).run(resume=True)
    assert resumed["status"] == "complete"
    assert export_manifest_bytes(resumed["manifest"]) == export_manifest_bytes(full["manifest"])
    assert resumed["entity_results"] == full["entity_results"]
    assert resumed["summary"] == full["summary"]


def test_external_stop_request_and_resume(config, rows, expected, tmp_path):
    state_dir = tmp_path / "state"
    first_entity = "ENT-ATLAS"
    FactMergeEngine(config, rows, state_dir=state_dir, expected=expected).run(entity_ids=[first_entity])
    engine = FactMergeEngine(config, rows, state_dir=state_dir, expected=expected)
    engine.request_stop()
    stopped = engine.run()
    assert stopped["status"] == "stopped"
    assert stopped["next_entity_id"] == "ENT-ATLAS"
    resumed = FactMergeEngine(config, rows, state_dir=state_dir, expected=expected).run(resume=True)
    full = FactMergeEngine(config, rows, expected=expected).run(use_state=False)
    assert export_manifest_bytes(resumed["manifest"]) == export_manifest_bytes(full["manifest"])


def test_checkpoint_sequences_are_monotonic_and_atomic_envelopes(config, rows, expected, tmp_path):
    state_dir = tmp_path / "state"
    stopped = FactMergeEngine(config, rows, state_dir=state_dir, expected=expected).run(stop_after_entities=2)
    assert stopped["status"] == "stopped"
    files = sorted((state_dir / "checkpoints").glob("*.json"))
    assert len(files) == 8
    sequences = [json.loads(path.read_text(encoding="utf-8"))["seq"] for path in files]
    assert sequences == sorted(sequences)
    assert sequences == list(range(1, 9))
    payloads = [json.loads(path.read_text(encoding="utf-8")) for path in files]
    assert {item["stage"] for item in payloads} == {"stage1", "stage2", "stage3", "stage4"}
    assert all("payload" in item and "fingerprint" in item for item in payloads)


def test_repeat_runs_and_batch_vs_single_entity_are_byte_stable(config, rows, expected):
    first = FactMergeEngine(config, rows, expected=expected).run(use_state=False)
    second = FactMergeEngine(config, rows, expected=expected).run(use_state=False)
    assert first["manifest_bytes"] == second["manifest_bytes"]
    assert canonical_json(first["summary"]) == canonical_json(second["summary"])
    check = FactMergeEngine(config, rows, expected=expected).consistency_check()
    assert check["repeat_manifest_byte_equal"] is True
    assert check["repeat_summary_byte_equal"] is True
    assert check["batch_vs_single_entity_equal"] is True
    assert first["summary"]["conflict_identification_rate"] == "1.000000"
    assert first["summary"]["decision_accuracy"] == "1.000000"
    assert first["summary"]["evidence_rebuild_rate"] == "1.000000"
    assert first["summary"]["parse_error_count"] == 2


def test_corrupt_checkpoint_cascades_downstream_rebuild(config, rows, expected, tmp_path):
    state_dir = tmp_path / "state"
    first = FactMergeEngine(config, rows, state_dir=state_dir, expected=expected).run(entity_ids=["ENT-ATLAS"])
    stage2_files = sorted((state_dir / "checkpoints").glob("*-stage2-*.json"))
    assert len(stage2_files) == 1
    stage2_files[0].write_text("{broken", encoding="utf-8")
    repaired = FactMergeEngine(config, rows, state_dir=state_dir, expected=expected).run(entity_ids=["ENT-ATLAS"])
    status = repaired["version_status"]
    assert status["last_run"]["reuse_counts"]["stage1"] == 1
    assert status["last_run"]["reuse_counts"]["stage2"] == 0
    assert status["last_run"]["reuse_counts"]["stage3"] == 0
    assert status["last_run"]["reuse_counts"]["stage4"] == 0
    assert any(
        event["reason"] == "CHECKPOINT_UNREADABLE" and event["invalidated_stages"] == ["stage2", "stage3", "stage4"]
        for event in status["content_invalidations"]
    )
    assert repaired["manifest_bytes"] == first["manifest_bytes"]
