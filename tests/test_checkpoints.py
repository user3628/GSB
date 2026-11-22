from __future__ import annotations

import json
import multiprocessing
from pathlib import Path

import pytest

from mmcoop.canonical import digest_json
from mmcoop.checkpoints import CheckpointStore
from mmcoop.errors import AppError


def test_incomplete_and_duplicate_records_are_ignored_and_quarantined(built_service):
    store = built_service.store
    marker_before = store.marker_path.read_bytes()
    # An incomplete checkpoint beyond the published marker must be ignored.
    (store.checkpoint_dir / "00000005-text_encoding.json").write_text("{unfinished", encoding="utf-8")
    scan = store.scan()
    assert scan.latest_sequence == 4
    assert scan.invalid
    integrity = store.inspect_integrity(scan=scan)
    assert integrity["verdict"] == "normal"
    assert store.marker_path.read_bytes() == marker_before

    # Two checksum-valid records sharing sequence 5 are also ignored as duplicates.
    projection_raw = json.loads((store.checkpoint_dir / "00000002-projection_alignment.json").read_text(encoding="utf-8"))
    fusion_raw = json.loads((store.checkpoint_dir / "00000003-fusion_scoring.json").read_text(encoding="utf-8"))
    for raw in (projection_raw, fusion_raw):
        body = dict(raw)
        body.pop("record_sha256", None)
        body["sequence"] = 5
        body["record_sha256"] = digest_json(body)
        stage = body["stage"]
        (store.checkpoint_dir / f"00000005-{stage}.json").write_text(
            json.dumps(body, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    duplicate_scan = store.scan()
    assert duplicate_scan.latest_sequence == 4
    assert 5 in duplicate_scan.duplicate_sequences

    dataset = built_service._load()[1]
    changed = list(dataset.image_map()["i01"]["feature"])
    changed[0] = float(changed[0]) + 0.001
    built_service.edit_image("i01", {"feature": changed})
    result = built_service.build()
    assert result["status"] == "completed"
    assert sorted(path.name for path in store.checkpoint_dir.glob("00000005-*.json")) == [
        "00000005-projection_alignment.json"
    ]
    assert store.scan().latest_sequence == 7


def test_snapshot_rollback_is_adjudicated(built_service):
    store = built_service.store
    latest = max(store.checkpoint_dir.glob("*.json")).name
    (store.checkpoint_dir / latest).unlink()
    integrity = store.inspect_integrity()
    assert integrity["verdict"] == "rollback"
    assert integrity["reason"] == "marker_tip_checkpoint_missing"
    with pytest.raises(AppError) as captured:
        built_service.build()
    assert captured.value.code == "E_STALE_CHECKPOINT"
    assert captured.value.diagnostics["integrity"]["verdict"] == "rollback"


def test_snapshot_artifact_tamper_is_adjudicated(built_service):
    store = built_service.store
    stage = store.scan().stages["aggregation"]
    artifact_path = store.workspace / stage["artifact_path"]
    artifact_path.write_bytes(artifact_path.read_bytes() + b" ")
    integrity = store.inspect_integrity()
    assert integrity["verdict"] == "tamper"
    assert integrity["reason"] == "marker_tip_record_duplicate_or_corrupt"
    with pytest.raises(AppError) as captured:
        built_service.build()
    assert captured.value.code == "E_STALE_CHECKPOINT"


def test_snapshot_fingerprint_mismatch_is_tamper(built_service):
    store = built_service.store
    marker = json.loads(store.marker_path.read_text(encoding="utf-8"))
    marker["stages"]["aggregation"]["input_fingerprint"] = "f" * 64
    body = dict(marker)
    body.pop("marker_sha256", None)
    marker["marker_sha256"] = digest_json(body)
    store.marker_path.write_text(json.dumps(marker, ensure_ascii=False, indent=2), encoding="utf-8")
    integrity = store.inspect_integrity()
    assert integrity["verdict"] == "tamper"
    assert integrity["reason"] == "stage_fingerprint_or_digest_mismatch"


def test_normal_advance_is_detected_and_reconciled(built_service):
    store = built_service.store
    old_marker = store.marker_path.read_bytes()
    dataset = built_service._load()[1]
    changed = list(dataset.image_map()["i01"]["feature"])
    changed[0] = float(changed[0]) + 0.001
    built_service.edit_image("i01", {"feature": changed})
    built_service.build()
    assert store.scan().latest_sequence == 7
    # Simulate a crash after checkpoint publication but before marker update.
    store.marker_path.write_bytes(old_marker)
    integrity = store.inspect_integrity()
    assert integrity["verdict"] == "normal_advance"
    assert integrity["reason"] == "valid_checkpoint_chain_ahead_of_marker"
    built_service.build()
    assert store.inspect_integrity()["verdict"] == "normal"


def test_corrupt_tip_after_marker_is_tamper(built_service):
    store = built_service.store
    latest = sorted(store.checkpoint_dir.glob("*.json"))[-1]
    latest.write_text("{truncated", encoding="utf-8")
    integrity = store.inspect_integrity()
    assert integrity["verdict"] == "tamper"


def _concurrent_publish_worker(workspace: str, artifact: dict, queue) -> None:
    store = CheckpointStore(workspace)
    try:
        result = store.publish(
            artifact,
            input_fingerprint="concurrent-stage-2",
            components={"test": "concurrency"},
            config_version="concurrent-config",
        )
        queue.put(("ok", result["sequence"]))
    except AppError as error:
        queue.put((error.code, error.diagnostics.get("sequence")))


def test_multiprocess_publication_serializes_and_rejects_duplicate(built_service):
    store = built_service.store
    stage2 = store.scan().stages["projection_alignment"]
    artifact = json.loads(json.dumps(stage2["artifact"]))
    artifact["summary"]["similarity_mean"] = 0.123456789
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    processes = [
        context.Process(target=_concurrent_publish_worker, args=(str(store.workspace), artifact, queue))
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    outcomes = [queue.get(timeout=20) for _ in processes]
    for process in processes:
        process.join(timeout=20)
        assert process.exitcode == 0
    assert sorted(item[0] for item in outcomes) == ["E_STALE_CHECKPOINT", "ok"]
    assert store.scan().latest_sequence == 5

