from __future__ import annotations

import math

from matgen.config import normalize_config
from matgen.diagnostics import build_diagnostics
from matgen.noise import compose_noise
from matgen.palette import build_palette_artifacts, load_palette
from matgen.prompt import generate_control_vector


def build_chain(config):
    recipe = {
        "material_id": "diagnostic_material",
        "description": "粗糙 陶土 Terracotta 暖色",
        "base_color": [0.72, 0.34, 0.22],
        "roughness": 0.82,
        "metalness": 0.01,
        "resolution": [10, 8],
        "noise": {
            "seed": 104729,
            "base_resolution": [14, 10],
            "octaves": [
                {"frequency": 1.0, "amplitude": 0.6},
                {"frequency": 2.0, "amplitude": 0.3},
                {"frequency": 4.0, "amplitude": 0.1},
            ],
        },
    }
    control = generate_control_vector(
        recipe["description"], material_id=recipe["material_id"], hash_seed=config["hash_seed"]
    )
    noise = compose_noise(recipe, config)
    palette = load_palette(recipe, cards_path=None)
    palette_artifact = build_palette_artifacts(recipe, control, noise, palette)
    diagnostics = build_diagnostics(
        recipe=recipe,
        config=config,
        control_artifact=control,
        noise_artifact=noise,
        palette_artifact=palette_artifact,
    )
    return diagnostics


def test_error_report_has_explicit_samples_references_and_finite_bounds() -> None:
    diagnostics = build_chain(normalize_config({"sample_count": 8}))
    assert len(diagnostics["octaves"]) == 3
    assert diagnostics["reference_method"]["numeric_type"] == "decimal"
    assert diagnostics["sample_positions"]["base"]
    assert diagnostics["sample_positions"]["target"]
    assert diagnostics["palette_quantization"]["samples"]
    for octave in diagnostics["octaves"]:
        assert octave["samples"]
        assert octave["accumulation_error_bound"] >= octave["measured_absolute_error"]
        assert octave["accumulation_error_bound"] >= 0.0
        assert math.isfinite(octave["relative_error_bound"])
        assert 0 <= octave["effective_digits"] <= 15
        for sample in octave["samples"]:
            assert sample["reference_decimal"]
            assert sample["position"]["x"] >= 0 and sample["position"]["y"] >= 0
    for parameter in diagnostics["material_parameters"].values():
        assert parameter["reference_decimal"]
        assert parameter["error_bound"] >= parameter["absolute_error"]
        assert 0 <= parameter["significant_digits"] <= 15


def test_budget_overflow_and_closest_layer_are_reported() -> None:
    config = normalize_config(
        {
            "sample_count": 8,
            "octave_error_budget": 1e-20,
            "total_error_budget": 1e-20,
        }
    )
    diagnostics = build_chain(config)
    assert diagnostics["error_budget"]["over_budget_octaves"] == [0, 1, 2]
    assert diagnostics["error_budget"]["closest_octave"]["over_budget"] is True
    assert diagnostics["error_budget"]["total_over_budget"] is True
    assert diagnostics["error_budget"]["total_budget_margin"] < 0.0


def test_attribution_cross_check_marks_rank_mismatches() -> None:
    diagnostics = build_chain(normalize_config({"sample_count": 8}))
    attribution = diagnostics["attribution"]
    assert attribution["contribution_order"]
    assert attribution["perturbation_replay_order"]
    contribution_rank = {
        octave_index: rank for rank, octave_index in enumerate(attribution["contribution_order"])
    }
    replay_rank = {
        octave_index: rank for rank, octave_index in enumerate(attribution["perturbation_replay_order"])
    }
    expected_mismatch = {
        octave_index
        for octave_index in contribution_rank
        if contribution_rank[octave_index] != replay_rank[octave_index]
    }
    assert {item["octave_index"] for item in attribution["review_items"]} == expected_mismatch
    for item in attribution["review_items"]:
        assert item["review_required"] is True
        assert item["deviation_source"]

