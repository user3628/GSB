from __future__ import annotations

import json
from pathlib import Path

from storypipe.pipeline import PipelineRunner
from storypipe.verify import compare_scales, validate_workspace


def test_two_scale_pipeline_is_complete(small_run, large_run) -> None:
    small = small_run["result"]
    large = large_run["result"]
    assert small["ok"] and large["ok"]
    assert small["cache_strategy"] == "per_frame_recompute"
    assert large["cache_strategy"] == "segment_cache"
    assert all(info["status"] == "complete" for info in small["stages"].values())
    assert all(info["status"] == "complete" for info in large["stages"].values())
    assert small["timeline_digest"] != large["timeline_digest"]


def test_cache_peak_respects_budget(small_run, large_run) -> None:
    for item in (small_run, large_run):
        workspace = item["workspace"]
        budget = json.loads((workspace / "input" / "config.json").read_text())[
            "cache"
        ]["memory_budget_bytes"]
        for stage in ("compose", "transition", "encode"):
            stats = json.loads(
                (workspace / "state" / "execution" / f"{stage}.json").read_text()
            )
            assert stats["budget_respected"]
            peak_key = (
                "peak_frame_buffer_bytes"
                if "peak_frame_buffer_bytes" in stats
                else "peak_transition_buffer_bytes"
            )
            assert stats[peak_key] <= budget


def test_manifest_and_crossfade_fields(small_run) -> None:
    workspace = small_run["workspace"]
    manifest = PipelineRunner(workspace).export_manifest()
    frames = manifest["frames"]
    assert [frame["global_frame"] for frame in frames] == list(range(manifest["frame_count"]))
    crossfade = [
        frame
        for frame in frames
        if frame["transition"] is not None
        and frame["transition"]["type"] == "crossfade"
    ]
    hard_cut = [
        frame
        for frame in frames
        if frame["transition"] is not None
        and frame["transition"]["type"] == "hard_cut"
    ]
    assert crossfade
    assert hard_cut
    for frame in hard_cut:
        assert len(frame["blend_sources"]) == 1
        assert frame["blend_sources"][0]["weight"] == 1.0
    for frame in crossfade:
        assert frame["transition"]["type"] == "crossfade"
        assert len(frame["blend_sources"]) == 2
        assert abs(sum(source["weight"] for source in frame["blend_sources"]) - 1.0) < 1e-12
        assert frame["pixel_sha256"].startswith("sha256:")
        assert frame["png_sha256"].startswith("sha256:")
    stats = manifest["statistics"]
    assert sum(stats["camera_move_frame_counts"].values()) == manifest["frame_count"]
    assert stats["transition_frame_counts"]["crossfade"] == len(crossfade)
    assert stats["transition_frame_counts"]["hard_cut"] == len(hard_cut)
    assert sum(stats["transition_frame_counts"].values()) == len(crossfade) + len(hard_cut)
    summary_path = (
        workspace
        / "stages"
        / "04_encode"
        / "output"
        / "frame_summaries.jsonl"
    )
    rows = [
        json.loads(line)
        for line in summary_path.read_text(encoding="utf-8").splitlines()
        if line
    ]
    assert len(rows) == manifest["frame_count"]
    assert [row["global_frame"] for row in rows] == list(range(manifest["frame_count"]))


def test_verification_and_scale_comparison(small_run, large_run) -> None:
    small = validate_workspace(small_run["workspace"])
    large = validate_workspace(large_run["workspace"])
    assert small["ok"] and large["ok"]
    assert not small["timeline_errors"] and not large["timeline_errors"]
    assert not small["pixel_errors"] and not large["pixel_errors"]
    report = compare_scales(small_run["workspace"], large_run["workspace"])
    assert report["ok"]
    assert report["comparison"]["frame_count_delta"] > 0
    assert report["comparison"]["shot_count_delta"] > 0
    assert report["comparison"]["timeline_digests_differ"]


def test_rerun_without_changes_reuses_checkpoints(small_run) -> None:
    workspace: Path = small_run["workspace"]
    runner = PipelineRunner(workspace)
    before = json.loads((workspace / "stages" / "04_encode" / "output" / "manifest.json").read_text())
    before_sequences = {
        stage: runner.checkpoints.read(stage)["sequence"]
        for stage in ("schedule", "compose", "transition", "encode")
    }
    result = runner.run()
    after = json.loads((workspace / "stages" / "04_encode" / "output" / "manifest.json").read_text())
    after_sequences = {
        stage: runner.checkpoints.read(stage)["sequence"]
        for stage in ("schedule", "compose", "transition", "encode")
    }
    assert result["ok"]
    assert before["deterministic_summary_sha256"] == after["deterministic_summary_sha256"]
    assert before["output_tree_digest"] == after["output_tree_digest"]
    assert before_sequences == after_sequences
