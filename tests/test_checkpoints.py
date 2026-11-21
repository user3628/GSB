from __future__ import annotations

import json
import multiprocessing
import shutil
from pathlib import Path

import pytest

from matgen.checkpoints import (
    CheckpointStore,
    STAGE_NAMES,
    classify_snapshot_transition,
)
from matgen.errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, MatgenError


def publish_dummy(store: CheckpointStore, stage_index: int, upstream: str = "") -> dict:
    return store.publish_stage(
        material_id="material_a",
        stage_index=stage_index,
        recipe_fingerprint="recipe",
        stage_input_fingerprint=f"input-{stage_index}",
        config_version="config",
        artifacts={f"stage-{stage_index}.json": json.dumps({"stage": stage_index}).encode()},
        upstream_record_digest=upstream,
    )


def test_missing_prerequisite_duplicate_and_cross_material_are_rejected(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path)
    with pytest.raises(MatgenError) as missing:
        publish_dummy(store, 1)
    assert missing.value.code == E_INVALID_STAGE

    control = publish_dummy(store, 0)
    with pytest.raises(MatgenError) as duplicate:
        publish_dummy(store, 0)
    assert duplicate.value.code == E_STALE_CHECKPOINT

    with pytest.raises(MatgenError) as cross_material:
        store.publish_stage(
            material_id="material_b",
            stage_index=1,
            recipe_fingerprint="recipe-b",
            stage_input_fingerprint="input-b",
            config_version="config",
            artifacts={"stage-1.json": b"other"},
            upstream_record_digest=control["record_digest"],
        )
    assert cross_material.value.code == E_INVALID_STAGE

    control_b = store.publish_stage(
        material_id="material_b",
        stage_index=0,
        recipe_fingerprint="recipe-b",
        stage_input_fingerprint="input-b-0",
        config_version="config",
        artifacts={"stage-0.json": b"b-control"},
    )
    assert control_b["material_id"] == "material_b"
    with pytest.raises(MatgenError) as misplaced:
        store.publish_stage(
            material_id="material_b",
            stage_index=1,
            recipe_fingerprint="recipe-b",
            stage_input_fingerprint="input-b-1",
            config_version="config",
            artifacts={"stage-1.json": b"b-noise"},
            upstream_record_digest=control["record_digest"],
        )
    assert misplaced.value.code == E_STALE_CHECKPOINT


def test_record_and_artifact_tampering_are_detected(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path)
    publish_dummy(store, 0)
    record_path = store.checkpoint_path("material_a", 0)
    record = json.loads(record_path.read_text(encoding="utf-8"))
    record["stage_input_fingerprint"] = "silently-replaced"
    record_path.write_text(json.dumps(record), encoding="utf-8")
    with pytest.raises(MatgenError) as tampered_record:
        store.status()
    assert tampered_record.value.code == E_STALE_CHECKPOINT
    assert tampered_record.value.details["transition"] == "tamper"


def test_artifact_summary_mismatch_is_detected(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path)
    publish_dummy(store, 0)
    artifact_path = store.artifact_dir("material_a", 0) / "stage-0.json"
    artifact_path.write_bytes(b"changed")
    with pytest.raises(MatgenError) as tampered_artifact:
        store.status()
    assert tampered_artifact.value.code == E_STALE_CHECKPOINT
    assert tampered_artifact.value.details["cause"] == "artifact_summary_mismatch"


def test_whole_snapshot_rollback_is_detected_by_external_anchor(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path)
    control = publish_dummy(store, 0)
    snapshot_root = tmp_path / "old_snapshot"
    shutil.copytree(tmp_path / ".matgen", snapshot_root / ".matgen")
    shutil.copy2(tmp_path / ".matgen.anchor.json", snapshot_root / ".matgen.anchor.json")
    publish_dummy(store, 1, control["record_digest"])
    shutil.rmtree(tmp_path / ".matgen")
    shutil.copytree(snapshot_root / ".matgen", tmp_path / ".matgen")
    # Deliberately retain the newer external anchor: this is the durable signal
    # that the snapshot directory was rolled back as a whole.
    with pytest.raises(MatgenError) as rolled_back:
        store.status()
    assert rolled_back.value.code == E_STALE_CHECKPOINT
    assert rolled_back.value.details["transition"] == "rollback"


def test_normal_advance_classifier_distinguishes_all_three_cases() -> None:
    assert (
        classify_snapshot_transition(candidate_sequence=2, current_sequence=1)
        == "normal_advance"
    )
    assert classify_snapshot_transition(candidate_sequence=1, current_sequence=1) == "rollback"
    assert classify_snapshot_transition(candidate_sequence=3, current_sequence=1) == "tamper"
    assert (
        classify_snapshot_transition(
            candidate_sequence=2,
            current_sequence=1,
            candidate_input_fingerprint="new",
            current_input_fingerprint="old",
        )
        == "tamper"
    )


def _concurrent_publish_worker(workspace: str, start_event, queue) -> None:
    store = CheckpointStore(workspace)
    start_event.wait(10)
    try:
        record = store.publish_stage(
            material_id="race_material",
            stage_index=0,
            recipe_fingerprint="recipe",
            stage_input_fingerprint="input",
            config_version="config",
            artifacts={"control.json": b"same bytes"},
        )
        queue.put(("ok", record["record_digest"]))
    except MatgenError as exc:
        queue.put(("error", exc.code))


def test_concurrent_process_publication_allows_only_one_duplicate(tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    start_event = context.Event()
    queue = context.Queue()
    processes = [
        context.Process(target=_concurrent_publish_worker, args=(str(tmp_path), start_event, queue))
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    start_event.set()
    results = [queue.get(timeout=30) for _ in processes]
    for process in processes:
        process.join(timeout=30)
        assert process.exitcode == 0
    statuses = sorted(result[0] for result in results)
    assert statuses == ["error", "ok"]
    assert [result[1] for result in results if result[0] == "error"] == [E_STALE_CHECKPOINT]
    integrity = CheckpointStore(tmp_path).verify_integrity()
    assert integrity["entry_count"] == 1

