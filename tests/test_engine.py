from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from quant_audit.canonical import canonical_json
from quant_audit.config import load_config
from quant_audit.data import load_calibration, load_weights
from quant_audit.engine import AuditEngine
from quant_audit.errors import E_ATTRIBUTION_GAP, E_INCREMENTAL_MISMATCH, AuditError


def _engine(
    data: Path,
    config_name: str = "config.yaml",
    calibration_name: str = "calibration_clean.npy",
) -> AuditEngine:
    weights = load_weights(data / "weights.npz")
    config = load_config(data / config_name, weights.layer_names)
    calibration = load_calibration(data / calibration_name, config.calibration_ratio)
    return AuditEngine(config, weights, calibration)


def test_full_run_is_deterministic_and_records_explicit_errors(demo_paths: dict[str, Path]) -> None:
    data = demo_paths["data"]
    first = _engine(data).run_full()
    second = _engine(data).run_full()

    assert canonical_json(first.result) == canonical_json(second.result)
    assert len(first.result["layers"]) == 6
    assert first.result["budget"]["usable"] is True
    for layer in first.result["layers"]:
        error = layer["error"]
        assert error["absolute_max"] >= 0.0
        assert error["relative_l2"] >= 0.0
        assert error["propagated_abs_error_bound"] + 1e-12 >= error["absolute_max"]
        assert layer["name"].startswith("linear_")


def test_attribution_closes_and_marks_rank_disagreements(demo_paths: dict[str, Path]) -> None:
    result = _engine(demo_paths["data"]).run_full().result
    attribution = result["attribution"]
    total = attribution["end_to_end_l2_error"]
    allocated = sum(layer["contribution_l2"] for layer in attribution["layers"])
    tolerance = attribution["closure_tolerance"]

    assert abs(allocated - total) <= max(1e-12, total * 1e-12)
    assert attribution["closure"]["first_order_gap_ratio"] <= tolerance
    assert attribution["closure"]["replay_gap_ratio"] <= tolerance
    assert attribution["closure"]["passed"] is True
    for layer in attribution["layers"]:
        if layer["needs_review"]:
            assert layer["first_order_rank"] != layer["replay_rank"]
            assert layer["review_reason"]
            assert layer["deviation_source"]


def test_budget_links_upstream_share_to_action(demo_paths: dict[str, Path]) -> None:
    engine = _engine(
        demo_paths["data"],
        config_name="config_outlier_over_budget.yaml",
        calibration_name="calibration_outlier.npy",
    )
    result = engine.run_full().result
    budget = result["budget"]

    assert budget["usable"] is False
    assert budget["over_budget_layers"]
    assert result["attribution"]["review_required_layers"]
    reviewed = [
        layer for layer in result["attribution"]["layers"] if layer["needs_review"]
    ]
    assert reviewed
    assert all(layer["review_reason"] and layer["deviation_source"] for layer in reviewed)
    assert any(
        diagnosis["cannot_fix_by_raising_bits_alone"]
        for diagnosis in budget["diagnostics"]
    )
    assert budget["closest_to_budget"] is not None
    assert "margin" in budget["closest_to_budget"]


def test_attribution_gap_returns_business_code(demo_paths: dict[str, Path]) -> None:
    engine = _engine(
        demo_paths["data"],
        config_name="config_attribution_gap.yaml",
        calibration_name="calibration_outlier.npy",
    )
    with pytest.raises(AuditError) as caught:
        engine.run_full()
    assert caught.value.code == E_ATTRIBUTION_GAP
    assert caught.value.details["first_order_gap_ratio"] > 0.0


def test_incremental_reuses_prefix_and_matches_full(demo_paths: dict[str, Path]) -> None:
    workspace = demo_paths["workspace"]
    workspace.calibrate(mode="full")
    previous_record, previous_arrays = workspace.store.load_stage(1)
    incremental_engine = _engine(demo_paths["data"], config_name="config_incremental.yaml")

    incremental = incremental_engine.run_incremental(previous_record, previous_arrays)
    full = incremental_engine.run_full()

    assert incremental.earliest_recomputed_layer == 3
    assert incremental.reused_layers == ("linear_00", "linear_01", "linear_02")
    assert incremental.recomputed_layers == ("linear_03", "linear_04", "linear_05")
    assert canonical_json(incremental.result) == canonical_json(full.result)
    for index in range(6):
        assert np.array_equal(incremental.arrays[f"quant_out_{index}"], full.arrays[f"quant_out_{index}"])
        assert np.array_equal(incremental.arrays[f"ref_out_{index}"], full.arrays[f"ref_out_{index}"])


def test_incremental_cache_mismatch_returns_business_code(demo_paths: dict[str, Path]) -> None:
    workspace = demo_paths["workspace"]
    workspace.calibrate(mode="full")
    previous_record, previous_arrays = workspace.store.load_stage(1)
    engine = _engine(demo_paths["data"], config_name="config_incremental.yaml")
    tampered = {key: value.copy() for key, value in previous_arrays.items()}
    tampered["quant_out_0"] = tampered["quant_out_0"] + 0.01

    with pytest.raises(AuditError) as caught:
        engine.run_incremental(previous_record, tampered)
    assert caught.value.code == E_INCREMENTAL_MISMATCH
    assert "linear_00" in caught.value.details["layers"]

