from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from multiscale_fusion.engine import FusionEngine, StageMaterial
from multiscale_fusion.manifest import Manifest
from multiscale_fusion.pipeline import run_fusion
from multiscale_fusion.util import load_npy, read_json

from .conftest import file_sha, key_artifacts, layer_dict, manifest_dict, write_manifest_dict


def test_fusion_matches_scalar_high_precision_reference(engine: FusionEngine, workspace: Path) -> None:
    result = engine.run("demo_fpn")
    assert result["status"] == "completed"
    manifest = Manifest.load(workspace / "input" / "manifest.json")
    task = manifest.by_id("demo_fpn")
    for layer in task.layers:
        reference = load_npy(workspace / "input" / layer.reference)
        output = load_npy(workspace / "outputs" / "demo_fpn" / f"{layer.name}.npy")
        assert output.shape == layer.expected_output_shape
        assert np.allclose(output, reference, rtol=0.0, atol=1e-11)
        relative = np.linalg.norm((output - reference).ravel()) / np.linalg.norm(reference.ravel())
        assert relative < 1e-12
    final = load_npy(workspace / "outputs" / "demo_fpn" / "final.npy")
    finest = load_npy(workspace / "outputs" / "demo_fpn" / f"{task.finest_layer.name}.npy")
    assert np.array_equal(final, finest)


def test_quantization_reports_scales_bounds_and_effective_bits(engine: FusionEngine, workspace: Path) -> None:
    result = engine.run("demo_fpn")
    report = result["quantization"]
    assert set(report["layers"]) == {"p3", "p4", "p5"}
    for name in ("p3", "p4", "p5"):
        item = report["layers"][name]
        assert item["scale"] > 0
        assert item["saturation_count"] == 0
        assert 0 < item["effective_bits"] <= item["bits"]
        assert item["reference_error_within_bound"] is True
        bound = load_npy(workspace / "outputs" / "demo_fpn" / item["error_bound_artifact"])
        actual = load_npy(workspace / "outputs" / "demo_fpn" / item["abs_reference_error_artifact"])
        assert np.all(actual <= bound)
        assert np.allclose(bound, item["error_bound_with_reference_slack"], rtol=0.0, atol=0.0)
    final = report["final"]
    assert final["scale"] > 0
    assert final["reference_error_within_bound"] is True
    assert final["max_reference_error"] <= final["error_bound_with_reference_slack"]
    assert 0 < final["effective_bits"] <= final["bits"]


def test_attention_normalization_and_stage_metadata(engine: FusionEngine, workspace: Path) -> None:
    result = engine.run("demo_fpn")
    store = engine.store("demo_fpn")
    state = store.scan()
    record = state.latest_for_layer("p3")
    assert record is not None
    metadata = read_json(store.load_artifact(record, "metadata.json"))
    attention = metadata["diagnostics"]["attention"]
    assert attention["channels"] == 7
    assert attention["raw_sum"] != 1.0
    assert attention["normalization_applied"] is True
    assert abs(sum(attention["effective_weights"]) - 1.0) < 1e-12
    assert metadata["diagnostics"]["upsample"]["tail_rows"] == 1
    assert metadata["diagnostics"]["upsample"]["tail_cols"] == 1
    assert metadata["diagnostics"]["lateral"]["odd_input_channels"] is True
    assert result["sensitivity"]["baseline_final_sha256"] == result["final_sha256"]


def test_l1_attention_normalization_rule(workspace: Path) -> None:
    data = manifest_dict(workspace)
    layer = layer_dict(data, "p5")
    layer["attention"]["normalization"] = "l1"
    write_manifest_dict(workspace, data)
    engine = FusionEngine(workspace)
    result = engine.run("demo_fpn")
    assert result["status"] == "completed"
    store = engine.store("demo_fpn")
    record = store.scan().latest_for_layer("p5")
    assert record is not None
    metadata = read_json(store.load_artifact(record, "metadata.json"))
    attention = metadata["diagnostics"]["attention"]
    assert attention["normalization"] == "l1"
    assert attention["normalization_applied"] is True
    assert abs(sum(abs(value) for value in attention["effective_weights"]) - 1.0) < 1e-12


def test_second_run_reuses_every_layer_byte_identically(engine: FusionEngine, workspace: Path) -> None:
    first = engine.run("demo_fpn")
    hashes = key_artifacts(workspace)
    second = engine.run("demo_fpn")
    assert second["status"] == "completed"
    assert second["reused_layers"] == ["p5", "p4", "p3"]
    assert second["computed_layers"] == []
    assert second["head_sequence"] == first["head_sequence"] == 3
    assert key_artifacts(workspace) == hashes


def test_layered_pipeline_can_be_replayed_from_plan(engine: FusionEngine, workspace: Path) -> None:
    task = engine._resolve_task("demo_fpn")
    plan = engine._build_plan(task)
    inputs = {name: plan.layers[name].input_array for name in plan.processing_order}
    conv = {name: plan.layers[name].conv_params for name in plan.processing_order}
    replay_a = run_fusion(task, inputs, conv)
    replay_b = run_fusion(task, inputs, conv)
    assert replay_a.final_sha256 == replay_b.final_sha256
    for name in plan.processing_order:
        assert replay_a.layer_outputs[name].raw_sha256 == replay_b.layer_outputs[name].raw_sha256
    result = engine.run("demo_fpn")
    assert result["final_sha256"] == replay_a.final_sha256
