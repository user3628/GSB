from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

from .canonical import atomic_write_json, digest_file, read_json
from .errors import PipelineError
from .pipeline import PipelineRunner
from .stage_io import stage_directory


def _pixel_sha256(array: np.ndarray) -> str:
    data = np.ascontiguousarray(array, dtype=np.uint8).tobytes(order="C")
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _validate_timeline(schedule: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    frames = schedule["frames"]
    expected_global = 0
    previous_end_us = 0
    for frame in frames:
        if frame["global_frame"] != expected_global:
            errors.append(
                f"global frame sequence break at {frame['global_frame']}, expected {expected_global}"
            )
        if frame["start_us"] < previous_end_us:
            errors.append(f"frame {frame['global_frame']} starts before previous frame ended")
        if frame["end_us"] <= frame["start_us"]:
            errors.append(f"frame {frame['global_frame']} has non-positive duration")
        if frame["start_ms"] > frame["end_ms"]:
            errors.append(f"frame {frame['global_frame']} has reversed millisecond bounds")
        previous_end_us = frame["end_us"]
        expected_global += 1
    for shot in schedule["shots"]:
        records = [
            frame
            for frame in frames
            if frame["global_frame"] >= shot["global_frame_start"]
            and frame["global_frame"] < shot["global_frame_end"]
        ]
        if len(records) != shot["duration_frames"]:
            errors.append(f"shot {shot['shot_id']} frame count mismatch")
        if records and records[0]["shot_id"] != shot["shot_id"]:
            errors.append(f"shot {shot['shot_id']} start attribution mismatch")
        if records and records[-1]["shot_id"] != shot["shot_id"]:
            errors.append(f"shot {shot['shot_id']} end attribution mismatch")
    return errors


def validate_workspace(workspace: str | Path) -> dict[str, Any]:
    workspace = Path(workspace).resolve()
    runner = PipelineRunner(workspace)
    inspection = runner.inspect()
    if not inspection.get("ok"):
        raise PipelineError(
            inspection["error"]["code"],
            inspection["error"]["message"],
            stage=inspection["error"].get("stage"),
        )
    incomplete = [
        stage
        for stage, info in inspection["stages"].items()
        if info["status"] != "complete"
    ]
    if incomplete:
        raise PipelineError(
            "E_INVALID_STAGE",
            "workspace is not fully built",
            details={"incomplete_stages": incomplete},
        )
    schedule_path = workspace / inspection["stages"]["schedule"]["artifact_path"]
    schedule = read_json(schedule_path)
    manifest = runner.export_manifest()
    output_dir = stage_directory(workspace, "encode") / "output"
    errors = _validate_timeline(schedule)
    pixel_errors: list[dict[str, Any]] = []
    for frame in manifest["frames"]:
        path = output_dir / frame["file"]
        if not path.exists():
            pixel_errors.append({"global_frame": frame["global_frame"], "error": "missing file"})
            continue
        png_digest = digest_file(path)
        if png_digest != frame["png_sha256"]:
            pixel_errors.append(
                {"global_frame": frame["global_frame"], "error": "png digest mismatch"}
            )
        with Image.open(path) as image:
            array = np.asarray(image.convert("RGB"), dtype=np.uint8)
        if _pixel_sha256(array) != frame["pixel_sha256"]:
            pixel_errors.append(
                {"global_frame": frame["global_frame"], "error": "pixel digest mismatch"}
            )
    actual_tree_digest = runner.verify_encoded_output()
    state: dict[str, Any] = {}
    for stage in ("compose", "transition", "encode"):
        path = workspace / "state" / "execution" / f"{stage}.json"
        state[stage] = read_json(path) if path.exists() else None
    return {
        "workspace": str(workspace),
        "ok": not errors and not pixel_errors and actual_tree_digest["ok"],
        "timeline_errors": errors,
        "pixel_errors": pixel_errors,
        "tree_check": actual_tree_digest,
        "shot_count": len(schedule["shots"]),
        "frame_count": schedule["total_frames"],
        "timeline_digest": schedule["timeline_digest"],
        "overall_fingerprint": inspection["overall_fingerprint"],
        "cache_strategy": inspection["cache_strategy"],
        "cache_plan": inspection["cache_plan"],
        "execution": state,
        "statistics": manifest["statistics"],
        "deterministic_summary_sha256": manifest["deterministic_summary_sha256"],
        "output_tree_digest": manifest["output_tree_digest"],
        "first_frame": manifest["frames"][0],
        "last_frame": manifest["frames"][-1],
    }


def compare_scales(
    small_workspace: str | Path,
    large_workspace: str | Path,
    output: str | Path | None = None,
) -> dict[str, Any]:
    small = validate_workspace(small_workspace)
    large = validate_workspace(large_workspace)
    comparison = {
        "small": {
            "shot_count": small["shot_count"],
            "frame_count": small["frame_count"],
            "timeline_digest": small["timeline_digest"],
            "deterministic_summary_sha256": small["deterministic_summary_sha256"],
            "output_tree_digest": small["output_tree_digest"],
            "cache_strategy": small["cache_strategy"],
            "statistics": small["statistics"],
        },
        "large": {
            "shot_count": large["shot_count"],
            "frame_count": large["frame_count"],
            "timeline_digest": large["timeline_digest"],
            "deterministic_summary_sha256": large["deterministic_summary_sha256"],
            "output_tree_digest": large["output_tree_digest"],
            "cache_strategy": large["cache_strategy"],
            "statistics": large["statistics"],
        },
        "timeline_digests_differ": small["timeline_digest"] != large["timeline_digest"],
        "output_summaries_differ": small["deterministic_summary_sha256"]
        != large["deterministic_summary_sha256"],
        "frame_count_delta": large["frame_count"] - small["frame_count"],
        "shot_count_delta": large["shot_count"] - small["shot_count"],
    }
    report = {
        "ok": bool(small["ok"] and large["ok"]),
        "comparison": comparison,
        "details": {"small": small, "large": large},
    }
    if output is not None:
        atomic_write_json(Path(output), report)
    return report
