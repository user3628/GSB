from __future__ import annotations

import json
import shutil
from pathlib import Path

import numpy as np
import pytest
import soundfile as sf

from sonic_sep.errors import AppError
from sonic_sep.manifest import load_manifest
from sonic_sep.pipeline import SeparationPipeline
from sonic_sep.workspace import Workspace

from conftest import imported_workspace, write_config


def test_full_build_is_deterministic_and_preserves_sample_counts(dataset_root: Path, tmp_path: Path) -> None:
    first_workspace = imported_workspace(dataset_root, tmp_path / "first")
    second_workspace = imported_workspace(dataset_root, tmp_path / "second")
    first = SeparationPipeline(first_workspace).build_slice("slice-001")
    second = SeparationPipeline(second_workspace).build_slice("slice-001")
    assert first["result_digest"] == second["result_digest"]
    assert first["mask_digest"] == second["mask_digest"]
    assert first["round_changes_digest"] == second["round_changes_digest"]
    assert set(first["stage_products"]) == {"stft", "features", "mask_init", "mask_refine", "reconstruct"}
    manifest = load_manifest(dataset_root / "samples" / "manifest.json")
    truth = manifest.get("slice-001")
    assert first["source_sample_counts"] == [truth.n_samples] * truth.source_count
    for output in first["outputs"]:
        audio, sample_rate = sf.read(first_workspace.output_dir("slice-001") / output["file"], dtype="int16")
        assert sample_rate == 16_000
        assert audio.size == truth.n_samples
    assert SeparationPipeline(first_workspace).verify_slice("slice-001")["fresh_full_match"] is True


def test_incremental_band_update_is_equal_to_full_recompute(dataset_root: Path, tmp_path: Path) -> None:
    workspace = imported_workspace(dataset_root, tmp_path / "state")
    pipeline = SeparationPipeline(workspace)
    baseline = pipeline.build_slice("slice-001")
    source = workspace.effective_manifest().get("slice-001").sources[0]
    result = pipeline.incremental_band_update(
        "slice-001",
        source_id=0,
        band_start=2,
        band_end=5,
        f0_min_hz=source.f0_min_hz - 10.0,
        f0_max_hz=source.f0_max_hz + 10.0,
    )
    assert result["full_equivalent"] is True
    assert result["incremental_verified"] is True
    assert result["stage_metrics"]["stft"]["status"] == "reused"
    assert result["stage_metrics"]["features"]["status"] == "reused"
    assert 0 < len(result["affected_bands"]) < 16
    assert result["mask_digest"] != baseline["mask_digest"]
    assert result["result_digest"] != baseline["result_digest"]
    assert result["clipping"]["total_clipped_samples"] >= 0
    assert pipeline.verify_slice("slice-001")["fresh_full_match"] is True
    invalidations = pipeline.invalidations(slice_id="slice-001")["slices"][0]["records"]
    assert invalidations[-1]["earliest_stage"] == "mask_init"
    assert invalidations[-1]["affected_bands"] == result["affected_bands"]


def test_graceful_stop_and_resume_match_a_single_full_run(dataset_root: Path, tmp_path: Path) -> None:
    stopped_workspace = imported_workspace(dataset_root, tmp_path / "stopped")
    full_workspace = imported_workspace(dataset_root, tmp_path / "full")
    stopped_pipeline = SeparationPipeline(stopped_workspace)
    stopped = stopped_pipeline.build_all(slice_ids=["slice-001"], stop_after_units=9)
    assert stopped["status"] == "stopped"
    assert stopped["resume_required"] is True
    assert stopped_workspace.stop_requested() is True
    assert not (stopped_workspace.slice_dir("slice-001") / "summary.json").exists()

    resumed = stopped_pipeline.resume(slice_ids=["slice-001"])
    full = SeparationPipeline(full_workspace).build_slice("slice-001")
    assert resumed["status"] == "complete"
    assert resumed["results"][0]["result_digest"] == full["result_digest"]
    assert SeparationPipeline(stopped_workspace).verify_slice("slice-001")["fresh_full_match"] is True

    checkpoint_status = stopped_pipeline.status(slice_id="slice-001")["checkpoints"]
    sequences = [record["sequence"] for record in checkpoint_status["records"]]
    assert sequences == list(range(1, len(sequences) + 1))
    assert checkpoint_status["recoverable_sequence"] == checkpoint_status["latest_sequence"]
    assert checkpoint_status["diagnostics"] == []


def test_mask_iteration_change_invalidates_from_refinement(dataset_root: Path, tmp_path: Path) -> None:
    workspace = imported_workspace(dataset_root, tmp_path / "state")
    pipeline = SeparationPipeline(workspace)
    pipeline.build_slice("slice-001")
    config_path = write_config(tmp_path / "configs" / "iterations.yaml", {"mask_iterations": 5})
    workspace.import_dataset(dataset_root / "samples" / "manifest.json", config_path, preserve_overrides=True)
    rebuilt = pipeline.build_slice("slice-001")
    assert rebuilt["invalidation"]["earliest_stage"] == "mask_refine"
    assert rebuilt["invalidation"]["affected_bands"] == list(range(16))
    assert rebuilt["stage_metrics"]["stft"]["status"] == "reused"
    assert rebuilt["stage_metrics"]["features"]["status"] == "reused"
    assert rebuilt["stage_metrics"]["mask_init"]["status"] == "reused"
    assert rebuilt["stage_metrics"]["mask_refine"]["status"] == "recomputed"
    version = pipeline.version(slice_id="slice-001")["slices"][0]
    assert version["matches_persisted"] is True


def test_band_partition_change_invalidates_features_but_reuses_stft(dataset_root: Path, tmp_path: Path) -> None:
    workspace = imported_workspace(dataset_root, tmp_path / "state")
    pipeline = SeparationPipeline(workspace)
    pipeline.build_slice("slice-001")
    config_path = write_config(tmp_path / "configs" / "bands.yaml", {"band_count": 8})
    workspace.import_dataset(dataset_root / "samples" / "manifest.json", config_path, preserve_overrides=True)
    rebuilt = pipeline.build_slice("slice-001")
    assert rebuilt["invalidation"]["earliest_stage"] == "features"
    assert rebuilt["stage_metrics"]["stft"]["status"] == "reused"
    assert rebuilt["stage_metrics"]["features"]["status"] == "recomputed"


def test_overlap_change_invalidates_only_reconstruction(dataset_root: Path, tmp_path: Path) -> None:
    workspace = imported_workspace(dataset_root, tmp_path / "state")
    pipeline = SeparationPipeline(workspace)
    pipeline.build_slice("slice-001")
    config_path = write_config(tmp_path / "configs" / "overlap.yaml", {"block_overlap_percent": 50})
    workspace.import_dataset(dataset_root / "samples" / "manifest.json", config_path, preserve_overrides=True)
    rebuilt = pipeline.build_slice("slice-001")
    assert rebuilt["invalidation"]["earliest_stage"] == "reconstruct"
    assert rebuilt["stage_metrics"]["stft"]["status"] == "reused"
    assert rebuilt["stage_metrics"]["features"]["status"] == "reused"
    assert rebuilt["stage_metrics"]["mask_init"]["status"] == "reused"
    assert rebuilt["stage_metrics"]["mask_refine"]["status"] == "reused"
    assert rebuilt["stage_metrics"]["reconstruct"]["status"] == "recomputed"


def test_sample_content_change_invalidates_stft(dataset_root: Path, tmp_path: Path) -> None:
    project = tmp_path / "project"
    shutil.copytree(dataset_root / "samples", project / "samples")
    shutil.copytree(dataset_root / "configs", project / "configs")
    workspace = Workspace(project, state_root=tmp_path / "state")
    workspace.import_dataset(project / "samples" / "manifest.json", project / "configs" / "default.yaml")
    pipeline = SeparationPipeline(workspace)
    pipeline.build_slice("slice-001")
    wav_path = project / "samples" / "slice-001.wav"
    audio, sample_rate = sf.read(wav_path, dtype="int16")
    audio[100] = np.int16(123 if audio[100] != 123 else 124)
    sf.write(wav_path, audio, sample_rate, subtype="PCM_16", format="WAV")
    rebuilt = pipeline.build_slice("slice-001")
    assert rebuilt["invalidation"]["earliest_stage"] == "stft"
    assert rebuilt["stage_metrics"]["stft"]["status"] == "recomputed"
    assert rebuilt["stage_metrics"]["features"]["status"] == "recomputed"


def test_incremental_mismatch_has_coordinates(monkeypatch: pytest.MonkeyPatch, dataset_root: Path, tmp_path: Path) -> None:
    workspace = imported_workspace(dataset_root, tmp_path / "state")
    pipeline = SeparationPipeline(workspace)
    pipeline.build_slice("slice-001")

    original = pipeline._compute_reference

    def corrupted_reference(*args, **kwargs):
        result = original(*args, **kwargs)
        result["masks"] = np.array(result["masks"], copy=True)
        result["masks"][0, 0, 0] += 0.25
        return result

    monkeypatch.setattr(pipeline, "_compute_reference", corrupted_reference)
    source = workspace.effective_manifest().get("slice-001").sources[0]
    with pytest.raises(AppError) as caught:
        pipeline.incremental_band_update(
            "slice-001",
            source_id=0,
            band_start=2,
            band_end=3,
            f0_min_hz=source.f0_min_hz - 5.0,
            f0_max_hz=source.f0_max_hz + 5.0,
        )
    assert caught.value.code == "E_INCREMENTAL_MISMATCH"
    assert {"band_index", "block_index"}.issubset(caught.value.details)

