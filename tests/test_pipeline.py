from __future__ import annotations

import hashlib
import json
import multiprocessing
from pathlib import Path

import pytest
from PIL import Image

from panopipe.checkpoints import CheckpointStore
from panopipe.errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, PipelineError
from panopipe.pipeline import Pipeline
from panopipe.workspace import Workspace


def _snapshot(root: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for directory in ("shards", "checkpoints"):
        for path in sorted((root / directory).rglob("*")):
            if path.is_file() and not path.name.startswith("."):
                result[path.relative_to(root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def test_full_build_checkpoints_and_validation(built_workspace) -> None:
    workspace, result = built_workspace
    assert result["checkpoint_sequence"] == 16
    assert len(result["completed_shards"]) == 4
    records = workspace.checkpoints.list_records()
    assert [record["sequence"] for record in records] == list(range(1, 17))
    assert len({record["shard_id"] for record in records}) == 4
    for record in records:
        assert record["input_fingerprint"]
        assert record["manifest_version_fingerprint"]
        assert record["artifact_digest"]
        assert record["artifact_files"]
        assert record["record_digest"]
    for shard_id, shard_result in result["shards"].items():
        validation = shard_result["validation"]
        assert validation["ok"] is True
        assert validation["pixel_count_sum"] == 3072
        assert {chain["chain"] for chain in validation["chains"]} == {
            "png_palette",
            "rle_json",
            "coco_json",
        }
        assert all(chain["labels_equal"] and chain["segments_equal"] for chain in validation["chains"])


def test_stop_and_resume_are_byte_identical(built_workspace, tmp_path: Path, demo_dir: Path) -> None:
    full_workspace, full_result = built_workspace
    resumed_root = tmp_path / "resumed"
    resumed = Workspace.create(resumed_root)
    resumed.import_manifest(demo_dir / "manifest_v2.json")
    first = Pipeline(resumed).build(stop_after_shards=2)
    assert first["stopped"] is True
    assert first["completed_shards"] == ["shard_1_indexed", "shard_2_rgb"]
    assert first["checkpoint_sequence"] == 8
    second = Pipeline(resumed).build(resume=True)
    assert second["ok"] is True
    assert second["summary_digest"] == full_result["summary_digest"]
    assert _snapshot(full_workspace.root) == _snapshot(resumed.root)


def test_stop_file_blocks_until_resume(imported_workspace: Workspace) -> None:
    imported_workspace.request_stop(reason="test")
    result = Pipeline(imported_workspace).build()
    assert result["stopped"] is True
    assert result["checkpoint_sequence"] == 0
    resumed = Pipeline(imported_workspace).build(resume=True)
    assert resumed["ok"] is True
    assert imported_workspace.stop_requested() is False


def test_invalidation_propagation_starts_at_merge(built_workspace) -> None:
    workspace, _ = built_workspace
    manifest_path = workspace.manifest_path
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    data["shards"][0]["min_instance_area"] = 10
    manifest_path.write_text(json.dumps(data, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    status = Pipeline(workspace).status(shard_ids=["shard_1_indexed"])
    pending = status["shards"]["shard_1_indexed"]["pending_invalidation"]
    assert pending["earliest_stage"] == "merge"
    assert pending["invalidated_stages"] == ["merge", "encode", "validate"]
    assert pending["reasons"][0]["code"] == "area_threshold_changed"
    result = Pipeline(workspace).build(shard_ids=["shard_1_indexed"])
    assert result["ok"] is True
    assert [item["stage"] for item in result["reused_stages"]] == ["decode"]
    assert [item["stage"] for item in result["rebuilt_stages"]] == ["merge", "encode", "validate"]
    assert result["checkpoint_sequence"] == 19
    event = result["invalidations"]["shard_1_indexed"]
    assert event["earliest_stage"] == "merge"
    assert event["active"] is True
    final_status = Pipeline(workspace).status(shard_ids=["shard_1_indexed"])
    assert final_status["invalidations"]["shard_1_indexed"]["resolved"] is True


def test_format_preference_only_invalidates_encode(built_workspace) -> None:
    workspace, _ = built_workspace
    data = json.loads(workspace.manifest_path.read_text(encoding="utf-8"))
    data["shards"][0]["mask_format"] = "coco_json"
    workspace.manifest_path.write_text(json.dumps(data, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    status = Pipeline(workspace).status(shard_ids=["shard_1_indexed"])
    pending = status["shards"]["shard_1_indexed"]["pending_invalidation"]
    assert pending["earliest_stage"] == "encode"
    assert pending["invalidated_stages"] == ["encode", "validate"]
    result = Pipeline(workspace).build(shard_ids=["shard_1_indexed"])
    assert [item["stage"] for item in result["reused_stages"]] == ["decode", "merge"]
    assert [item["stage"] for item in result["rebuilt_stages"]] == ["encode", "validate"]


def test_category_mapping_change_invalidates_decode(built_workspace) -> None:
    workspace, _ = built_workspace
    data = json.loads(workspace.manifest_path.read_text(encoding="utf-8"))
    data["shards"][0]["categories"][0]["name"] = "background_renamed"
    workspace.manifest_path.write_text(json.dumps(data, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    status = Pipeline(workspace).status(shard_ids=["shard_1_indexed"])
    pending = status["shards"]["shard_1_indexed"]["pending_invalidation"]
    assert pending["earliest_stage"] == "decode"
    assert any(reason["code"] == "category_mapping_changed" for reason in pending["reasons"])
    result = Pipeline(workspace).build(shard_ids=["shard_1_indexed"])
    assert [item["stage"] for item in result["rebuilt_stages"]] == ["decode", "merge", "encode", "validate"]


@pytest.mark.parametrize(
    ("field", "value", "expected_earliest", "reason"),
    [
        ("instance_start_id", 200, "merge", "instance_start_id_changed"),
        ("confidence_threshold", 0.7, "merge", "confidence_threshold_changed"),
    ],
)
def test_merge_configuration_changes_invalidate_from_merge(
    built_workspace, field: str, value, expected_earliest: str, reason: str
) -> None:
    workspace, _ = built_workspace
    data = json.loads(workspace.manifest_path.read_text(encoding="utf-8"))
    data["shards"][0][field] = value
    workspace.manifest_path.write_text(json.dumps(data, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    status = Pipeline(workspace).status(shard_ids=["shard_1_indexed"])
    pending = status["shards"]["shard_1_indexed"]["pending_invalidation"]
    assert pending["earliest_stage"] == expected_earliest
    assert any(item["code"] == reason for item in pending["reasons"])
    result = Pipeline(workspace).build(shard_ids=["shard_1_indexed"])
    assert [item["stage"] for item in result["reused_stages"]] == ["decode"]


def test_schema_version_change_and_source_pixel_change_invalidate_decode(built_workspace) -> None:
    workspace, _ = built_workspace
    data = json.loads(workspace.manifest_path.read_text(encoding="utf-8"))
    data["import_metadata"]["schema_version"] = 1
    workspace.manifest_path.write_text(json.dumps(data, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    status = Pipeline(workspace).status(shard_ids=["shard_1_indexed"])
    assert status["shards"]["shard_1_indexed"]["pending_invalidation"]["earliest_stage"] == "decode"

    image_path = workspace.root / "inputs" / "shard_2_rgb.png"
    with Image.open(image_path) as image:
        rgb = image.convert("RGB")
    rgb.putpixel((0, 0), (80, 80, 80))
    rgb.save(image_path, format="PNG", optimize=False)
    status = Pipeline(workspace).status(shard_ids=["shard_2_rgb"])
    pending = status["shards"]["shard_2_rgb"]["pending_invalidation"]
    assert pending["earliest_stage"] == "decode"
    assert any(item["code"] == "source_pixel_content_changed" for item in pending["reasons"])


def test_palette_cap_is_recorded_as_pipeline_degradation(built_workspace) -> None:
    workspace, _ = built_workspace
    config = json.loads(workspace.config_path.read_text(encoding="utf-8"))
    config["palette_max_colors"] = 2
    workspace.config_path.write_text(json.dumps(config, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    status = Pipeline(workspace).status(shard_ids=["shard_1_indexed"])
    pending = status["shards"]["shard_1_indexed"]["pending_invalidation"]
    assert pending["earliest_stage"] == "encode"
    result = Pipeline(workspace).build(shard_ids=["shard_1_indexed"])
    assert result["ok"] is False
    validation = result["shards"]["shard_1_indexed"]["validation"]
    assert "E_PALETTE_CAPACITY" in validation["error_codes"]
    assert any(item["code"] == "D_PALETTE_CAPACITY_EXCEEDED" for item in validation["degradations"])
    assert any(chain["ok"] for chain in validation["chains"] if chain["format"] != "png_palette")
    assert not (workspace.shard_dir("shard_1_indexed", "encode") / "mask.png").exists()


def test_graceful_stage_jump_fails_without_writes(imported_workspace: Workspace) -> None:
    with pytest.raises(PipelineError) as exc_info:
        Pipeline(imported_workspace).build(stages=["encode"], shard_ids=["shard_1_indexed"])
    assert exc_info.value.code == E_INVALID_STAGE
    assert imported_workspace.checkpoints.last_sequence() == 0
    assert not imported_workspace.shard_dir("shard_1_indexed").exists()
    with pytest.raises(PipelineError) as exc_info:
        Pipeline(imported_workspace).build(stages=["decode", "encode"], shard_ids=["shard_1_indexed"])
    assert exc_info.value.code == E_INVALID_STAGE
    assert imported_workspace.checkpoints.last_sequence() == 0


def test_invalid_import_writes_no_workspace_files(tmp_path: Path) -> None:
    manifest_path = tmp_path / "empty_categories.json"
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "shards": [
                    {
                        "id": "s",
                        "image": "unused.png",
                        "categories": [],
                        "instance_start_id": 100,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    workspace = Workspace(tmp_path / "never-created")
    with pytest.raises(PipelineError) as exc_info:
        workspace.import_manifest(manifest_path)
    assert exc_info.value.code == "E_INVALID_CONFIG"
    assert not workspace.root.exists()


def test_unknown_color_diagnostic_and_no_checkpoint(tmp_path: Path, demo_dir: Path) -> None:
    workspace = Workspace.create(tmp_path / "unknown")
    workspace.import_manifest(demo_dir / "errors" / "unknown_color_manifest.json")
    with pytest.raises(PipelineError) as exc_info:
        Pipeline(workspace).build()
    assert exc_info.value.code == "E_UNKNOWN_COLOR"
    assert exc_info.value.details["unknown_pixel_count"] == 1
    assert exc_info.value.details["unknown_colors"][0]["color"] == "#FF00FF"
    assert workspace.checkpoints.last_sequence() == 0
    diagnostics = list((workspace.root / "diagnostics").glob("unknown_color.*.json"))
    assert diagnostics
    diagnostic = json.loads(diagnostics[0].read_text(encoding="utf-8"))
    assert diagnostic["error"]["code"] == "E_UNKNOWN_COLOR"
    assert diagnostic["error"]["details"]["unknown_colors"][0]["pixel_count"] == 1


def test_segment_overflow_records_degradation(tmp_path: Path, demo_dir: Path) -> None:
    workspace = Workspace.create(tmp_path / "overflow")
    workspace.import_manifest(demo_dir / "errors" / "segment_overflow_manifest.json")
    result = Pipeline(workspace).build()
    assert result["ok"] is False
    validation = result["shards"]["segment_overflow"]["validation"]
    assert validation["ok"] is False
    assert validation["degraded"] is True
    assert validation["error_codes"] == ["E_SEGMENT_ID_OVERFLOW"]
    assert all(not chain["ok"] for chain in validation["chains"])
    assert any(item["code"] == "D_SEGMENT_ID_BITS_EXCEEDED" for item in validation["degradations"])
    for shard_dir in (workspace.root / "shards" / "segment_overflow").glob("encode/*"):
        assert shard_dir.suffix not in {".png"}


def _publish_worker(root: str, record: dict, queue) -> None:
    try:
        published = CheckpointStore(Path(root)).publish(record, expected_sequence=17)
        queue.put(("ok", published["sequence"], record["shard_id"]))
    except PipelineError as exc:
        queue.put((exc.code, exc.details, record["shard_id"]))


def test_multiprocess_publish_rejects_stale_or_duplicate(built_workspace) -> None:
    workspace, _ = built_workspace
    base = {
        "input_fingerprint": "a" * 64,
        "manifest_version_fingerprint": "b" * 64,
        "artifact_digest": "c" * 64,
        "stage_fingerprint": "d" * 64,
        "artifact_files": [{"path": "shards/x/decode/labels.npy", "sha256": "e" * 64}],
        "configuration_axes": {"schema_version": 2},
        "pipeline_format": "PANO_CHECKPOINT_V1",
    }
    records = [
        {**base, "shard_id": "worker_a", "stage": "decode"},
        {**base, "shard_id": "worker_b", "stage": "decode", "stage_fingerprint": "f" * 64},
    ]
    context = multiprocessing.get_context("fork")
    queue = context.Queue()
    processes = [context.Process(target=_publish_worker, args=(str(workspace.root), record, queue)) for record in records]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0
    outcomes = [queue.get(timeout=2) for _ in processes]
    assert sorted(item[0] for item in outcomes) == sorted(["ok", E_STALE_CHECKPOINT])
    assert workspace.checkpoints.last_sequence() == 17
    successful_shard = next(item[2] for item in outcomes if item[0] == "ok")
    successful_record = next(record for record in records if record["shard_id"] == successful_shard)
    with pytest.raises(PipelineError) as exc_info:
        workspace.checkpoints.publish(successful_record, expected_sequence=18)
    assert exc_info.value.code == E_STALE_CHECKPOINT
