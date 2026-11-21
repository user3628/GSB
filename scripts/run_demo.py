"""End-to-end deterministic acceptance demo."""

from __future__ import annotations

import hashlib
import json
import shutil
import sys
import threading
import urllib.request
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pose_pipeline.config import PipelineConfig
from pose_pipeline.constants import (
    ERROR_INVALID_PARAM,
    ERROR_INVALID_STAGE,
    ERROR_POSE_INVALID,
    ERROR_STALE_CHECKPOINT,
)
from pose_pipeline.engine import PipelineEngine
from pose_pipeline.errors import PipelineError
from pose_pipeline.generator import generate_dataset
from pose_pipeline.http_api import create_server
from pose_pipeline.util import atomic_write_pretty_json, read_json_file


def run_demo(
    out: str | Path = "demo_workspace",
    *,
    seed: int = 20261122,
    quiet: bool = False,
) -> dict[str, Any]:
    root = Path(out).resolve()
    root.mkdir(parents=True, exist_ok=True)
    data_dir = root / "data"
    clean_dir = root / "clean"
    resumed_dir = root / "resumed"
    for managed in (data_dir, clean_dir, resumed_dir, root / "errors", root / "strict"):
        if managed.exists():
            shutil.rmtree(managed)

    _log(quiet, "1/8 generating deterministic samples")
    generated = generate_dataset(data_dir, seed=seed)
    manifest_path = data_dir / "manifest.json"

    _log(quiet, "2/8 running the full baseline build")
    clean = PipelineEngine(clean_dir)
    clean.import_samples(manifest_path)
    clean_build = clean.build()
    clean_report_bytes = (clean.store.exports_dir / "report.json").read_bytes()
    clean_poses_bytes = (clean.store.exports_dir / "poses.json").read_bytes()
    report = clean.report()
    clean_head = clean.store.scan().head
    checks: dict[str, bool] = {
        "baseline_complete": bool(clean_build["pipeline_complete"]),
        "baseline_hit_rate_is_one": report["totals"]["hit_rate"] == 1.0,
        "baseline_has_five_shards": report["totals"]["shards"] == 5,
        "baseline_has_multi_instance": _shard_instance_count(clean, "shard_03") == 2,
        "baseline_has_invisible_keypoint": _has_invisible_point(clean, "shard_02"),
    }

    _log(quiet, "3/8 stopping after seven atomic units, then resuming")
    resumed = PipelineEngine(resumed_dir)
    resumed.import_samples(manifest_path)
    partial = resumed.build(stop_after_units=7)
    partial_head = resumed.store.scan().head
    resumed_build = resumed.resume()
    checks["stop_completed_partial_units"] = len(partial["completed_units"]) == 7
    checks["stop_left_pipeline_incomplete"] = not partial["pipeline_complete"]
    checks["resume_completed_pipeline"] = bool(resumed_build["pipeline_complete"])
    checks["resume_report_bytes_equal"] = (
        clean_report_bytes == (resumed.store.exports_dir / "report.json").read_bytes()
    )
    checks["resume_poses_bytes_equal"] = (
        clean_poses_bytes == (resumed.store.exports_dir / "poses.json").read_bytes()
    )
    checks["resume_checkpoint_chain_continuous"] = [
        record["sequence"] for record in resumed.store.scan().records
    ] == list(range(1, 21))

    _log(quiet, "4/8 applying a skeleton-only change")
    incremental = PipelineEngine(clean_dir)
    pre_before = {
        shard_id: incremental.store.latest_by_stage()[(shard_id, "preprocess")]["sequence"]
        for shard_id in ["shard_01", "shard_02", "shard_03", "shard_04", "shard_05"]
    }
    decode_before = {
        shard_id: incremental.store.latest_by_stage()[(shard_id, "decode")]["sequence"]
        for shard_id in ["shard_01", "shard_02", "shard_03", "shard_04", "shard_05"]
    }
    manifest = read_json_file(incremental.store.inputs_dir / "manifest.json")
    manifest["skeleton"].append(["left_shoulder", "right_shoulder"])
    for shard in manifest["shards"]:
        shard["skeleton"] = list(manifest["skeleton"])
    atomic_write_pretty_json(incremental.store.inputs_dir / "manifest.json", manifest)
    skeleton_build = incremental.build()
    checks["skeleton_earliest_stage_is_assembly"] = (
        skeleton_build["invalidation"]["earliest_stage"] == "assembly"
    )
    checks["skeleton_rebuilds_ten_units"] = skeleton_build["planned_unit_count"] == 10
    checks["skeleton_reuses_preprocess"] = all(
        pre_before[shard_id]
        == incremental.store.latest_by_stage()[(shard_id, "preprocess")]["sequence"]
        for shard_id in pre_before
    )
    checks["skeleton_reuses_decode"] = all(
        decode_before[shard_id]
        == incremental.store.latest_by_stage()[(shard_id, "decode")]["sequence"]
        for shard_id in decode_before
    )

    _log(quiet, "5/8 applying confidence-interval and ground-truth changes")
    manifest = read_json_file(incremental.store.inputs_dir / "manifest.json")
    for shard in manifest["shards"]:
        shard["expected_pose_confidence"] = {"min": 0.995, "max": 1.0}
        for instance in shard["instances"]:
            instance["expected_pose_confidence"] = {"min": 0.995, "max": 1.0}
    atomic_write_pretty_json(incremental.store.inputs_dir / "manifest.json", manifest)
    confidence_build = incremental.build()
    updated_report = incremental.report()
    checks["confidence_earliest_stage_is_validation"] = (
        confidence_build["invalidation"]["earliest_stage"] == "validation"
    )
    checks["confidence_rebuilds_only_five_units"] = confidence_build["planned_unit_count"] == 5
    checks["confidence_marks_out_of_range"] = (
        updated_report["totals"]["confidence_out_of_interval_instances"] >= 1
    )
    manifest = read_json_file(incremental.store.inputs_dir / "manifest.json")
    head_truth = manifest["shards"][0]["instances"][0]["keypoints"]["head"]
    head_truth["x"] = round(float(head_truth["x"]) + 0.001, 8)
    atomic_write_pretty_json(incremental.store.inputs_dir / "manifest.json", manifest)
    truth_build = incremental.build()
    checks["truth_earliest_stage_is_preprocess"] = (
        truth_build["invalidation"]["earliest_stage"] == "preprocess"
    )
    checks["truth_rebuilds_only_four_units"] = truth_build["planned_unit_count"] == 4

    _log(quiet, "6/8 injecting invalid parameters and skeleton definitions")
    errors_dir = root / "errors"
    negative = PipelineEngine(errors_dir / "negative", PipelineConfig(gaussian_radius=-1))
    negative.import_samples(manifest_path)
    negative_error = _capture_error(negative.build)
    heatmap = PipelineEngine(
        errors_dir / "heatmap", PipelineConfig(gaussian_radius=2.2, heatmap_width=64)
    )
    heatmap.import_samples(manifest_path)
    heatmap_error = _capture_error(heatmap.build)
    unknown = PipelineEngine(errors_dir / "unknown_skeleton")
    unknown.import_samples(manifest_path)
    unknown.build()
    unknown_manifest = read_json_file(unknown.store.inputs_dir / "manifest.json")
    unknown_manifest["skeleton"].append(["head", "not_a_joint"])
    for shard in unknown_manifest["shards"]:
        shard["skeleton"] = list(unknown_manifest["skeleton"])
    atomic_write_pretty_json(unknown.store.inputs_dir / "manifest.json", unknown_manifest)
    unknown_error = _capture_error(unknown.build)
    conflict = PipelineEngine(
        errors_dir / "multi_instance_conflict",
        PipelineConfig(association_max_edge_fraction=0.75),
    )
    conflict.import_samples(manifest_path)
    conflict_error = _capture_error(conflict.build)
    strict = PipelineEngine(root / "strict")
    strict.import_samples(manifest_path)
    stage_error = _capture_error(
        lambda: strict.build(strict_shard_id="shard_01", strict_stage="validation")
    )
    next_sequence = resumed.store.next_sequence()
    stale_error = _capture_error(
        lambda: resumed.store.publish(
            shard_id="shard_01",
            stage="preprocess",
            input_fingerprint="0" * 64,
            config_version="1" * 64,
            artifact_digest="2" * 64,
            upstream_digest=None,
            expected_sequence=next_sequence - 1,
        )
    )
    checks["negative_gaussian_returns_invalid_param"] = (
        negative_error.get("code") == ERROR_INVALID_PARAM
        and negative_error.get("stage") == "preprocess"
    )
    checks["heatmap_size_mismatch_returns_invalid_param"] = (
        heatmap_error.get("code") == ERROR_INVALID_PARAM
    )
    checks["unknown_skeleton_returns_pose_invalid"] = (
        unknown_error.get("code") == ERROR_POSE_INVALID
    )
    checks["multi_instance_conflict_returns_pose_invalid"] = (
        conflict_error.get("code") == ERROR_POSE_INVALID
        and conflict_error.get("details", {}).get("reason") == "multi_instance_conflict"
    )
    checks["invalid_stage_jump_returns_invalid_stage"] = (
        stage_error.get("code") == ERROR_INVALID_STAGE
    )
    checks["stale_publication_returns_stale_checkpoint"] = (
        stale_error.get("code") == ERROR_STALE_CHECKPOINT
    )

    _log(quiet, "7/8 exercising the local HTTP API")
    current_incremental_report = incremental.report()
    server = create_server(clean_dir, host="127.0.0.1", port=0, quiet=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        health = _http_json("GET", base + "/health")
        status = _http_json("GET", base + "/api/status")
        exported_report = _http_json("GET", base + "/api/report")
        checks["http_health"] = bool(health.get("ok"))
        checks["http_status_complete"] = bool(status.get("pipeline_complete"))
        checks["http_report_matches_digest"] = (
            exported_report.get("report_digest")
            == current_incremental_report.get("report_digest")
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    all_passed = all(checks.values())
    result = {
        "ok": all_passed,
        "seed": seed,
        "data_dir": str(data_dir),
        "dataset_digest": generated["dataset_digest"],
        "baseline": {
            "report_digest": report["report_digest"],
            "report_sha256": _sha256_bytes(clean_report_bytes),
            "poses_digest": report["poses_export_digest"],
            "poses_sha256": _sha256_bytes(clean_poses_bytes),
            "totals": report["totals"],
            "checkpoint_head_sequence": clean_head["sequence"] if clean_head else None,
        },
        "stop_resume": {
            "stopped_after_units": 7,
            "stopped_checkpoint_head_sequence": partial_head["sequence"] if partial_head else None,
            "resumed_planned_units": resumed_build["planned_unit_count"],
            "report_bytes_equal": checks["resume_report_bytes_equal"],
            "poses_bytes_equal": checks["resume_poses_bytes_equal"],
        },
        "incremental": {
            "skeleton_change": {
                "earliest_stage": skeleton_build["invalidation"]["earliest_stage"],
                "planned_units": skeleton_build["planned_unit_count"],
                "affected_shards": skeleton_build["invalidation"]["affected_shard_ids"],
                "preprocess_reused": checks["skeleton_reuses_preprocess"],
                "decode_reused": checks["skeleton_reuses_decode"],
            },
            "confidence_interval_change": {
                "earliest_stage": confidence_build["invalidation"]["earliest_stage"],
                "planned_units": confidence_build["planned_unit_count"],
                "out_of_interval_instances": updated_report["totals"][
                    "confidence_out_of_interval_instances"
                ],
            },
            "ground_truth_change": {
                "earliest_stage": truth_build["invalidation"]["earliest_stage"],
                "planned_units": truth_build["planned_unit_count"],
            },
        },
        "errors": {
            "negative_gaussian": negative_error,
            "heatmap_size_mismatch": heatmap_error,
            "unknown_skeleton": unknown_error,
            "multi_instance_conflict": conflict_error,
            "invalid_stage_jump": stage_error,
            "stale_checkpoint": stale_error,
        },
        "checks": checks,
        "all_passed": all_passed,
    }
    atomic_write_pretty_json(root / "results.json", result)
    _log(quiet, "8/8 demo complete" + (" successfully" if all_passed else " with failures"))
    if not all_passed:
        raise RuntimeError(f"demo checks failed: {[name for name, ok in checks.items() if not ok]}")
    return result


def _capture_error(callable_value: Any) -> dict[str, Any]:
    try:
        callable_value()
    except PipelineError as exc:
        return exc.to_dict()
    return {"ok": False, "code": None, "message": "expected an error but none was raised"}


def _shard_instance_count(engine: PipelineEngine, shard_id: str) -> int:
    record = engine.store.latest_by_stage()[(shard_id, "assembly")]
    artifact = engine.store.load_artifact(record)
    return int(artifact["instance_count"])


def _has_invisible_point(engine: PipelineEngine, shard_id: str) -> bool:
    record = engine.store.latest_by_stage()[(shard_id, "preprocess")]
    artifact = engine.store.load_artifact(record)
    return bool(artifact["invisible_keypoints"])


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _http_json(method: str, url: str) -> dict[str, Any]:
    request = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def _log(quiet: bool, message: str) -> None:
    if not quiet:
        print(message, flush=True)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="demo_workspace")
    parser.add_argument("--seed", type=int, default=20261122)
    parser.add_argument("--quiet", action="store_true")
    arguments = parser.parse_args()
    print(json.dumps(run_demo(arguments.out, seed=arguments.seed, quiet=arguments.quiet), ensure_ascii=False, indent=2))
