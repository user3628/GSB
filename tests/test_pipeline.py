from __future__ import annotations

import pytest

from qoplib.checkpoint import CheckpointStore
from qoplib.errors import (
    E_INVALID_STAGE,
    E_NUMERIC_INPUT,
    E_NUMERIC_MISMATCH,
    InvalidStageError,
    NumericMismatchError,
)
from qoplib.manifest import load_manifest
from qoplib.pipeline import (
    expected_stage_fingerprints,
    require_prerequisite,
    run_shard_pipeline,
)
from qoplib.io_util import load_npz
from qoplib.runner import run_fingerprint


def _store(tmp_path):
    return CheckpointStore(tmp_path / "run")


def test_healthy_shard_completes_all_stages(demo_data, tmp_path):
    manifest = load_manifest(demo_data / "manifest.yaml")
    store = _store(tmp_path)
    shard = manifest.shard_by_id("shard_00")
    result = run_shard_pipeline(
        shard, demo_data, manifest, store,
        run_fingerprint(manifest, demo_data))
    assert result.status == "completed"
    assert [stage.stage for stage in result.stages] == \
        ["validate", "quantize", "linear", "verify"]
    assert result.final_summary["output_shape"] == shard.expected_output_shape
    assert result.final_summary["tightest_margin_layer"] is not None


def test_nan_inf_shard_reports_numeric_input(demo_data, tmp_path):
    manifest = load_manifest(demo_data / "manifest.yaml")
    store = _store(tmp_path)
    shard = manifest.shard_by_id("shard_03")
    result = run_shard_pipeline(
        shard, demo_data, manifest, store,
        run_fingerprint(manifest, demo_data))
    assert result.status == "rejected_input"
    assert result.error["code"] == E_NUMERIC_INPUT
    detail = result.error["details"]["non_finite"]["activation"]
    assert detail["nan_count"] == 1
    assert detail["pos_inf_count"] == 1
    assert detail["neg_inf_count"] == 1


def test_tolerance_breach_raises_numeric_mismatch(demo_data, tmp_path):
    manifest = load_manifest(demo_data / "manifest.yaml")
    store = _store(tmp_path)
    shard = manifest.shard_by_id("shard_01")
    for layer in shard.layers:
        object.__setattr__(layer, "rel_tol", 1e-9)
        object.__setattr__(layer, "abs_tol", 1e-12)
    with pytest.raises(NumericMismatchError) as exc:
        run_shard_pipeline(shard, demo_data, manifest, store,
                           run_fingerprint(manifest, demo_data))
    assert exc.value.code == E_NUMERIC_MISMATCH
    assert exc.value.details["shard_id"] == "shard_01"
    assert "max_rel_error" in exc.value.details


def test_stage_fingerprints_change_with_bitwidth(demo_data):
    manifest = load_manifest(demo_data / "manifest.yaml")
    shard = manifest.shard_by_id("shard_01")
    arrays = load_npz(demo_data / shard.npz_file)
    before = expected_stage_fingerprints(
        shard, arrays, "manifest-fp")
    changed_layer = shard.layers[0]
    object.__setattr__(changed_layer, "bitwidth", 8)
    after = expected_stage_fingerprints(shard, arrays, "manifest-fp")
    assert before["validate"] == after["validate"]
    assert before["quantize"] != after["quantize"]
    assert before["linear"] != after["linear"]
    assert before["verify"] != after["verify"]


def test_invalidation_rebuilds_downstream_and_reuses_upstream(
        demo_data, tmp_path):
    manifest = load_manifest(demo_data / "manifest.yaml")
    store = _store(tmp_path)
    shard = manifest.shard_by_id("shard_01")
    run_fp = run_fingerprint(manifest, demo_data)
    first = run_shard_pipeline(shard, demo_data, manifest, store, run_fp)
    assert all(not stage.reused for stage in first.stages)

    # Re-run unchanged: everything reused.
    second = run_shard_pipeline(shard, demo_data, manifest, store, run_fp)
    assert all(stage.reused for stage in second.stages)
    assert second.invalidations == []

    # Change bitwidth: validate stays reusable, downstream invalidated.
    object.__setattr__(shard.layers[0], "bitwidth", 8)
    third = run_shard_pipeline(shard, demo_data, manifest, store, run_fp)
    reused = {stage.stage: stage.reused for stage in third.stages}
    assert reused["validate"] is True
    assert reused["quantize"] is False
    invalidated_stages = {item["stage"] for item in third.invalidations}
    assert "quantize" in invalidated_stages
    assert "linear" in invalidated_stages
    assert "verify" in invalidated_stages
    for item in third.invalidations:
        assert item["old_fingerprint"] != item["new_fingerprint"]


def test_require_prerequisite_guard(demo_data, tmp_path):
    store = _store(tmp_path)
    with pytest.raises(InvalidStageError) as exc:
        require_prerequisite(store, "shard_01", "verify")
    assert exc.value.code == E_INVALID_STAGE
    assert exc.value.details["missing_stage"] == "validate"


def test_clamp_statistics_are_audited(demo_data, tmp_path):
    manifest = load_manifest(demo_data / "manifest.yaml")
    store = _store(tmp_path)
    shard = manifest.shard_by_id("shard_00")
    result = run_shard_pipeline(
        shard, demo_data, manifest, store,
        run_fingerprint(manifest, demo_data))
    quant = result.final_summary["quant_summary"]
    assert quant["clamp_total"] >= 0
    for layer in quant["layers"]:
        assert set(["layer", "clamp_count", "clamp_positions",
                    "low_clamps", "high_clamps"]).issubset(layer)
