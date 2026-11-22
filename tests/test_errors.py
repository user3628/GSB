from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from multiscale_fusion.engine import FusionEngine
from multiscale_fusion.errors import FusionError
from multiscale_fusion.manifest import Manifest
from multiscale_fusion.quantization import quantize_symmetric

from .conftest import layer_dict, manifest_dict, write_manifest_dict


def test_attention_length_error_has_layer_and_index(workspace: Path) -> None:
    data = manifest_dict(workspace)
    layer_dict(data, "p4")["attention"]["weights"].pop()
    write_manifest_dict(workspace, data)
    engine = FusionEngine(workspace)
    with pytest.raises(FusionError) as excinfo:
        engine.run("demo_fpn")
    error = excinfo.value
    assert error.code == "E_INVALID_PARAM"
    assert error.layer == "p4"
    assert error.index == 6
    assert error.details["expected_length"] == 7
    assert error.details["actual_length"] == 6


def test_non_finite_attention_reports_exact_index(workspace: Path) -> None:
    data = manifest_dict(workspace)
    layer_dict(data, "p5")["attention"]["weights"][2] = float("nan")
    (workspace / "input" / "manifest.json").write_text(
        json.dumps(data, indent=2, sort_keys=True, allow_nan=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(FusionError) as excinfo:
        FusionEngine(workspace)
    error = excinfo.value
    assert error.code == "E_INVALID_PARAM"
    assert error.layer == "p5"
    assert error.index == 2
    assert "NaN" in error.details["value"] or "nan" in error.details["value"].lower()


def test_all_zero_fusion_coefficients_are_rejected(workspace: Path) -> None:
    data = manifest_dict(workspace)
    layer_dict(data, "p3")["fusion"]["coeffs"] = {"lateral": 0.0, "topdown": 0.0}
    write_manifest_dict(workspace, data)
    with pytest.raises(FusionError) as excinfo:
        FusionEngine(workspace)
    error = excinfo.value
    assert error.code == "E_INVALID_PARAM"
    assert error.layer == "p3"
    assert error.index == 0
    assert "zero" in error.details["reason"]


def test_zero_attention_sum_is_invalid(workspace: Path) -> None:
    data = manifest_dict(workspace)
    layer_dict(data, "p3")["attention"]["weights"] = [1.0, -1.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    write_manifest_dict(workspace, data)
    with pytest.raises(FusionError) as excinfo:
        FusionEngine(workspace).run("demo_fpn")
    error = excinfo.value
    assert error.code == "E_INVALID_PARAM"
    assert error.layer == "p3"
    assert error.details["reason"] == "normalization denominator is zero"


def test_declared_shape_mismatch_has_expected_and_actual(workspace: Path) -> None:
    data = manifest_dict(workspace)
    layer_dict(data, "p5")["expected_output_shape"] = [2, 7, 4, 6]
    write_manifest_dict(workspace, data)
    with pytest.raises(FusionError) as excinfo:
        FusionEngine(workspace).run("demo_fpn")
    error = excinfo.value
    assert error.code == "E_SHAPE_MISMATCH"
    assert error.layer == "p5"
    assert error.details["expected_shape"] == [2, 7, 4, 6]
    assert error.details["actual_shape"] == [2, 7, 4, 5]
    assert error.details["stage"] == "lateral_shape_plan"


def test_upsample_alignment_mismatch_has_layer(workspace: Path) -> None:
    data = manifest_dict(workspace)
    layer_dict(data, "p3")["upsample"]["scale"] = 3
    write_manifest_dict(workspace, data)
    with pytest.raises(FusionError) as excinfo:
        FusionEngine(workspace).run("demo_fpn")
    error = excinfo.value
    assert error.code == "E_SHAPE_MISMATCH"
    assert error.layer == "p3"
    assert error.details["stage"] == "upsample_plan_alignment"


def test_allowed_range_violation_is_numeric_error(workspace: Path) -> None:
    data = manifest_dict(workspace)
    layer_dict(data, "p5")["allowed_range"] = [-1.0, 1.0]
    write_manifest_dict(workspace, data)
    with pytest.raises(FusionError) as excinfo:
        FusionEngine(workspace).run("demo_fpn")
    error = excinfo.value
    assert error.code == "E_NUMERIC"
    assert error.layer == "p5"
    assert error.details["reason"] == "allowed_range_violation"
    assert error.details["violating_count"] > 0


def test_zero_scale_and_all_saturated_quantization_errors() -> None:
    zeros = np.zeros((1, 2, 2, 2), dtype=np.float64)
    with pytest.raises(FusionError) as excinfo:
        quantize_symmetric(zeros, 8, layer="zero", reference=zeros)
    assert excinfo.value.code == "E_NUMERIC"
    assert excinfo.value.details["reason"] == "zero_scale"

    values = np.array([[[[100.0, -100.0]]]], dtype=np.float64)
    with pytest.raises(FusionError) as excinfo:
        quantize_symmetric(values, 8, layer="sat", reference=values, scale_override=1e-9)
    error = excinfo.value
    assert error.code == "E_NUMERIC"
    assert error.details["reason"] == "all_quantized_values_saturated"
    assert error.details["saturation_count"] == values.size
