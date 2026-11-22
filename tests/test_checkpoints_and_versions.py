from __future__ import annotations

import json
import multiprocessing as mp
import shutil
from pathlib import Path

import pytest

from tracklab.canonical import atomic_write_json, digest, hash_tree, read_json
from tracklab.checkpoints import CheckpointStore, finalize_checkpoint
from tracklab.errors import E_STALE_CHECKPOINT, WorkbenchError
from tracklab.manifest import validate_manifest
from tracklab.versions import prepare_sequence
from tracklab.workbench import Workbench


def _publish_worker(settings, sequence_id, record, queue) -> None:
    from tracklab.checkpoints import CheckpointStore
    from tracklab.errors import WorkbenchError

    try:
        result = CheckpointStore(settings).publish(sequence_id, record)
        queue.put({"ok": True, "ordinal": result["ordinal"]})
    except WorkbenchError as exc:
        queue.put({"ok": False, "code": exc.code})


def _partial_run(workspace, sequence_id="seq_uniform", frames=2):
    workbench = Workbench(workspace)
    result = workbench.run_stage(sequence_id, "all", stop_after=frames)
    assert result["status"] == "stopped"
    store = CheckpointStore(workspace)
    scan = store.scan(sequence_id)
    assert scan.count == frames
    return workbench, store, scan


def test_checkpoint_duplicate_cross_sequence_and_chain_replacement_are_rejected(workspace):
    _, store, scan = _partial_run(workspace)
    record0 = scan.records[0]
    record1 = scan.records[1]

    with pytest.raises(WorkbenchError) as duplicate:
        store.publish(scan.sequence_id, record0)
    assert duplicate.value.code == E_STALE_CHECKPOINT
    assert duplicate.value.context["reason"] == "duplicate_or_rollback"

    with pytest.raises(WorkbenchError) as cross_sequence:
        store.publish("seq_scaling", record0)
    assert cross_sequence.value.code == E_STALE_CHECKPOINT

    replacement = json.loads(json.dumps(record1))
    replacement["ordinal"] = 3
    replacement["frame_index"] = 2
    replacement["previous_state_digest"] = "0" * 64
    replacement["state"]["frame_index"] = 2
    replacement = finalize_checkpoint(replacement)
    with pytest.raises(WorkbenchError) as chain_mismatch:
        store.publish(scan.sequence_id, replacement)
    assert chain_mismatch.value.code == E_STALE_CHECKPOINT
    assert chain_mismatch.value.context["reason"] == "state_replacement_or_chain_mismatch"


def test_corrupt_checkpoint_is_ignored_and_quarantined(workspace):
    workbench, store, scan = _partial_run(workspace, frames=3)
    prefix_hashes = {
        path.name: digest(path.read_bytes().hex())
        for path in sorted(store.directory(scan.sequence_id).glob("checkpoint_*.json"))[:2]
    }
    corrupt_path = store.directory(scan.sequence_id) / "checkpoint_00000002.json"
    corrupt_path.write_text("{truncated", encoding="utf-8")

    manifest = validate_manifest(workspace, verify_frame_files=True)
    sequence = manifest.by_id(scan.sequence_id)
    prepared = prepare_sequence(workspace, manifest, sequence, store)
    assert prepared.start_frame == 1
    assert prepared.invalidation_events
    event = prepared.invalidation_events[0]
    assert event["stage"] == "checkpoint"
    assert event["earliest_affected_frame"] == 1
    assert prepared.scan.count == 1
    for name, value in prefix_hashes.items():
        path = store.directory(scan.sequence_id) / name
        if name == "checkpoint_00000001.json":
            assert path.exists()
            assert digest(path.read_bytes().hex()) == value
    assert not (store.directory(scan.sequence_id) / "checkpoint_00000002.json").exists()
    assert not (store.directory(scan.sequence_id) / "checkpoint_00000003.json").exists()


def test_two_processes_cannot_publish_the_same_next_ordinal(workspace):
    _, store, scan = _partial_run(workspace, frames=1)
    record0 = scan.records[0]
    shutil.rmtree(store.directory(scan.sequence_id))
    store.directory(scan.sequence_id).mkdir(parents=True, exist_ok=True)

    methods = mp.get_all_start_methods()
    context = mp.get_context("fork" if "fork" in methods else "spawn")
    queue = context.Queue()
    processes = [
        context.Process(target=_publish_worker, args=(workspace, scan.sequence_id, record0, queue))
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=30)
        assert process.exitcode == 0
    outcomes = [queue.get(timeout=5) for _ in processes]
    assert sum(1 for item in outcomes if item["ok"]) == 1
    assert sorted(item.get("code") for item in outcomes if not item["ok"]) == [E_STALE_CHECKPOINT]
    assert store.scan(scan.sequence_id).count == 1


def test_reference_truth_change_invalidates_only_frame_and_downstream(workspace):
    workbench = Workbench(workspace)
    sequence_id = "seq_direction"
    complete = workbench.run_stage(sequence_id, "all")
    assert complete["status"] == "complete"
    store = CheckpointStore(workspace)
    scan = store.scan(sequence_id)
    frame_count = scan.count
    prefix_dir = store.directory(sequence_id)
    prefix_hashes = {
        path.name: path.read_bytes() for path in sorted(prefix_dir.glob("checkpoint_*.json"))[:10]
    }

    manifest_raw = read_json(workspace.manifest_path)
    sequence_raw = next(seq for seq in manifest_raw["sequences"] if seq["sequence_id"] == sequence_id)
    sequence_raw["frames"][10]["box_xywh_norm"][0] = round(
        sequence_raw["frames"][10]["box_xywh_norm"][0] + 0.002, 12
    )
    atomic_write_json(workspace.manifest_path, manifest_raw, pretty=True)
    workbench.reload_manifest()
    manifest = workbench.manifest
    sequence = manifest.by_id(sequence_id)
    prepared = prepare_sequence(workspace, manifest, sequence, store)

    assert prepared.start_frame == 10
    assert prepared.retained_through_frame == 9
    assert prepared.scan.count == 10
    assert prepared.invalidation_events
    event = prepared.invalidation_events[0]
    assert event["stage"] == "appearance"
    assert event["reason"] == "reference_truth_changed"
    assert event["earliest_affected_frame"] == 10
    assert event["affected_frame_interval"] == [10, frame_count - 1]
    for name, payload in prefix_hashes.items():
        assert (prefix_dir / name).read_bytes() == payload

    sequence_raw["search_window_expansion"] = 0.65
    atomic_write_json(workspace.manifest_path, manifest_raw, pretty=True)
    workbench.reload_manifest()
    manifest = workbench.manifest
    sequence = manifest.by_id(sequence_id)
    prepared2 = prepare_sequence(workspace, manifest, sequence, store)
    assert prepared2.start_frame == 0
    assert prepared2.invalidation_events
    event2 = prepared2.invalidation_events[0]
    assert event2["stage"] == "candidates"
    assert event2["reason"] == "search_window_or_scoring_config_changed"
    assert event2["earliest_affected_frame"] == 0
    assert store.scan(sequence_id).count == 0


def test_illegal_stage_transition_returns_invalid_stage(workspace):
    workbench = Workbench(workspace)
    with pytest.raises(WorkbenchError) as missing_appearance:
        workbench.run_stage("seq_uniform", "tracking")
    assert missing_appearance.value.code == "E_INVALID_STAGE"
    with pytest.raises(WorkbenchError) as bad_stage:
        workbench.run_stage("seq_uniform", "teleport")
    assert bad_stage.value.code == "E_INVALID_STAGE"


def test_candidate_and_trajectory_stages_enforce_prerequisites_and_integrity(workspace):
    workbench = Workbench(workspace)
    sequence_id = "seq_uniform"
    for stage in ("candidates", "trajectory", "tracking"):
        with pytest.raises(WorkbenchError) as missing_appearance:
            workbench.run_stage(sequence_id, stage)
        assert missing_appearance.value.code == "E_INVALID_STAGE"
        assert missing_appearance.value.context["required_stage"] == "appearance"
        assert missing_appearance.value.context["sequence_id"] == sequence_id

    appearance = workbench.run_stage(sequence_id, "appearance")
    assert appearance["stage_run"] == "appearance"
    artifact_path = workspace.sequence_artifact_dir(sequence_id) / "candidates.json"
    assert not artifact_path.exists()

    standalone = workbench.run_stage(sequence_id, "candidates")
    assert standalone["stage_run"] == "candidates"
    assert standalone["candidates"]["audited_frames"] == 0
    assert standalone["candidates"]["preview_frame"] == 0
    assert artifact_path.exists()
    assert workbench.store.scan(sequence_id).count == 0
    original_artifact = artifact_path.read_bytes()

    repeated = workbench.run_stage(sequence_id, "candidates")
    assert repeated["candidates"]["stage_digest"] == standalone["candidates"]["stage_digest"]
    assert artifact_path.read_bytes() == original_artifact

    artifact_path.unlink()
    with pytest.raises(WorkbenchError) as missing_candidates:
        workbench.run_stage(sequence_id, "trajectory")
    assert missing_candidates.value.code == "E_INVALID_STAGE"
    assert missing_candidates.value.context["required_stage"] == "candidates"

    artifact_path.write_bytes(original_artifact)
    tampered = json.loads(original_artifact.decode("utf-8"))
    tampered["audited_frames"] += 1
    artifact_path.write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(WorkbenchError) as invalid_artifact:
        workbench.run_stage(sequence_id, "trajectory")
    assert invalid_artifact.value.code == "E_INVALID_STAGE"
    assert invalid_artifact.value.context["required_stage"] == "candidates"

    artifact_path.write_bytes(original_artifact)
    stopped = workbench.run_stage(sequence_id, "trajectory", stop_after=1)
    assert stopped["status"] == "stopped"
    assert stopped["valid_frames"] == 1
    assert stopped["candidates"]["reused"] is True
    workbench.clear_stop(sequence_id)


def test_all_refreshes_candidate_audit_and_reuses_it_for_stage_three(workspace):
    workbench = Workbench(workspace)
    sequence_id = "seq_uniform"
    completed = workbench.run_stage(sequence_id, "all")
    assert completed["status"] == "complete"

    artifact_path = workspace.sequence_artifact_dir(sequence_id) / "candidates.json"
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    scan = workbench.store.scan(sequence_id)
    expected_checked = [
        {
            "frame_index": record["frame_index"],
            "candidate_record_digest": record["candidate_record"]["candidate_record_digest"],
            "response_score": record["candidate_record"]["response_score"],
        }
        for record in scan.records
    ]
    assert artifact["audited_frames"] == len(scan.records)
    assert artifact["preview_frame"] is None
    assert artifact["source"]["checked_frame_digests"] == expected_checked
    artifact_bytes = artifact_path.read_bytes()

    reused = workbench.run_stage(sequence_id, "trajectory")
    assert reused["status"] == "complete"
    assert reused["processed_frames"] == []
    assert reused["candidates"]["audited_frames"] == len(scan.records)
    assert reused["candidates"]["stage_digest"] == artifact["stage_digest"]
    assert artifact_path.read_bytes() == artifact_bytes
