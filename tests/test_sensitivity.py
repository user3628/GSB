from __future__ import annotations

import numpy as np

from multiscale_fusion.engine import FusionEngine
from multiscale_fusion.pipeline import relative_l2_deviation, run_fusion


def test_sensitivity_replay_ranking_and_additivity(engine: FusionEngine) -> None:
    result = engine.run("demo_fpn")
    report = result["sensitivity"]
    assert len(report["layers"]) == 3
    assert set(report["replay_ranking"]) == {"p3", "p4", "p5"}
    assert set(report["first_order_ranking"]) == {"p3", "p4", "p5"}
    assert report["additivity_gap"] <= report["additivity_tolerance"]
    assert report["additivity_ok"] is True
    assert abs(
        report["sum_single_layer_relative_deviation"] - report["all_layers_relative_deviation"]
    ) == report["additivity_gap"]
    replay_rank = {name: index + 1 for index, name in enumerate(report["replay_ranking"])}
    first_rank = {name: index + 1 for index, name in enumerate(report["first_order_ranking"])}
    expected_review = sorted(name for name in replay_rank if replay_rank[name] != first_rank[name])
    assert sorted(report["review_required_layers"]) == expected_review
    for item in report["layers"]:
        assert item["replay_relative_deviation"] >= 0
        assert item["first_order_relative_deviation"] >= 0
        assert item["review_required"] == (replay_rank[item["layer"]] != first_rank[item["layer"]])
        assert item["perturbation"]["amplitude"] == report["amplitude"]
        assert len(item["perturbation"]["attention_pattern"]) == 7


def test_single_layer_perturbation_changes_end_to_end_output(engine: FusionEngine) -> None:
    task = engine._resolve_task("demo_fpn")
    plan = engine._build_plan(task)
    inputs = {name: plan.layers[name].input_array for name in plan.processing_order}
    conv = {name: plan.layers[name].conv_params for name in plan.processing_order}
    baseline = run_fusion(task, inputs, conv)
    layer = task.layer_by_name("p4")
    attention = list(layer.attention.weights)
    attention[0] += 0.002
    perturbed = run_fusion(task, inputs, conv, attention_overrides={"p4": tuple(attention)})
    deviation = relative_l2_deviation(baseline.final, perturbed.final)
    assert deviation > 0
    assert np.any(perturbed.final != baseline.final)
