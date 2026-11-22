from __future__ import annotations

from typing import Any, Mapping

import numpy as np

from .errors import FusionError
from .manifest import LayerSpec, TaskSpec
from .pipeline import FusionResult, relative_l2_deviation, run_fusion


def _attention_perturbation(layer: LayerSpec, delta: float) -> tuple[float, ...]:
    # Alternating signs retain a near-zero raw sum for even channel counts and
    # explicitly include the odd-channel tail.  The final normalization rule,
    # if any, is then applied by the normal pipeline path.
    n = len(layer.attention.weights)
    pattern = np.ones(n, dtype=np.float64)
    pattern[1::2] = -1.0
    return tuple(float(value + delta * direction) for value, direction in zip(layer.attention.weights, pattern))


def _fusion_perturbation(layer: LayerSpec, delta: float) -> dict[str, float]:
    pattern = {"lateral": 1.0, "topdown": -1.0}
    return {key: float(value + delta * pattern.get(key, 1.0)) for key, value in layer.fusion.coeffs.items()}


def _overrides_for_layers(
    layers: list[LayerSpec],
    delta: float,
) -> tuple[dict[str, tuple[float, ...]], dict[str, dict[str, float]]]:
    attention = {layer.name: _attention_perturbation(layer, delta) for layer in layers}
    fusion = {layer.name: _fusion_perturbation(layer, delta) for layer in layers}
    return attention, fusion


def _run_with_delta(
    task: TaskSpec,
    inputs: Mapping[str, np.ndarray],
    conv_params: Mapping[str, tuple[np.ndarray, np.ndarray, dict[str, Any]]],
    layers: list[LayerSpec],
    delta: float,
    *,
    baseline: np.ndarray,
) -> tuple[FusionResult, float, float]:
    attention, fusion = _overrides_for_layers(layers, delta)
    result = run_fusion(task, inputs, conv_params, attention_overrides=attention, fusion_overrides=fusion)
    deviation = relative_l2_deviation(baseline, result.final)
    max_abs_delta = float(np.max(np.abs(result.final - baseline))) if baseline.size else 0.0
    return result, deviation, max_abs_delta


def _attention_pattern(layer: LayerSpec) -> tuple[float, ...]:
    n = len(layer.attention.weights)
    pattern = np.ones(n, dtype=np.float64)
    pattern[1::2] = -1.0
    return tuple(float(value) for value in pattern)


def _fusion_pattern(layer: LayerSpec) -> dict[str, float]:
    pattern = {"lateral": 1.0, "topdown": -1.0}
    return {key: float(pattern.get(key, 1.0)) for key in layer.fusion.coeffs}


def compute_sensitivity_report(
    task: TaskSpec,
    inputs: Mapping[str, np.ndarray],
    conv_params: Mapping[str, tuple[np.ndarray, np.ndarray, dict[str, Any]]],
    baseline: FusionResult,
) -> dict[str, Any]:
    """Replay one-layer perturbations and compare with a central-difference estimate."""

    amplitude = task.sensitivity.amplitude
    probe = task.sensitivity.first_order_probe
    tolerance = task.sensitivity.tolerance
    base = baseline.final
    baseline_norm = max(float(np.linalg.norm(base.ravel())), 1e-30)
    records: list[dict[str, Any]] = []
    all_layers = list(task.layers)

    for layer in all_layers:
        _, replay_deviation, max_abs_delta = _run_with_delta(
            task, inputs, conv_params, [layer], amplitude, baseline=base
        )
        plus, _, _ = _run_with_delta(task, inputs, conv_params, [layer], probe, baseline=base)
        minus, _, _ = _run_with_delta(task, inputs, conv_params, [layer], -probe, baseline=base)
        derivative = (plus.final - minus.final) / (2.0 * probe)
        first_order_deviation = float(np.linalg.norm(derivative.ravel())) * amplitude / baseline_norm
        records.append(
            {
                "layer": layer.name,
                "replay_relative_deviation": replay_deviation,
                "replay_max_abs_delta": max_abs_delta,
                "first_order_relative_deviation": first_order_deviation,
                "perturbation": {
                    "amplitude": amplitude,
                    "attention_pattern": list(_attention_pattern(layer)),
                    "fusion_pattern": _fusion_pattern(layer),
                    "model": "additive_uniform_direction",
                },
            }
        )

    _, all_deviation, all_max_abs_delta = _run_with_delta(
        task, inputs, conv_params, all_layers, amplitude, baseline=base
    )
    replay_sum = float(sum(record["replay_relative_deviation"] for record in records))
    additivity_gap = abs(replay_sum - all_deviation)
    additivity_ok = additivity_gap <= tolerance

    replay_order = [record["layer"] for record in sorted(records, key=lambda item: (-item["replay_relative_deviation"], item["layer"]))]
    first_order_order = [
        record["layer"]
        for record in sorted(records, key=lambda item: (-item["first_order_relative_deviation"], item["layer"]))
    ]
    replay_rank = {name: index + 1 for index, name in enumerate(replay_order)}
    first_order_rank = {name: index + 1 for index, name in enumerate(first_order_order)}
    review_layers = [
        record["layer"]
        for record in records
        if replay_rank[record["layer"]] != first_order_rank[record["layer"]]
    ]
    for record in records:
        name = record["layer"]
        record["replay_rank"] = replay_rank[name]
        record["first_order_rank"] = first_order_rank[name]
        record["review_required"] = replay_rank[name] != first_order_rank[name]

    report = {
        "task_id": task.id,
        "baseline_final_sha256": baseline.final_sha256,
        "baseline_final_shape": list(base.shape),
        "baseline_final_norm": baseline_norm,
        "amplitude": amplitude,
        "first_order_probe": probe,
        "additivity_tolerance": tolerance,
        "layers": records,
        "replay_ranking": replay_order,
        "first_order_ranking": first_order_order,
        "ranking_consistent": not review_layers,
        "review_required_layers": review_layers,
        "sum_single_layer_relative_deviation": replay_sum,
        "all_layers_relative_deviation": all_deviation,
        "all_layers_max_abs_delta": all_max_abs_delta,
        "additivity_gap": additivity_gap,
        "additivity_ok": additivity_ok,
    }
    return report


def assert_sensitivity_report_valid(report: Mapping[str, Any]) -> None:
    if not report.get("additivity_ok", False):
        raise FusionError(
            "E_NUMERIC",
            "sensitivity additivity deviation exceeds configured tolerance",
            details={
                "reason": "sensitivity_additivity_exceeds_tolerance",
                "sum_single_layer_relative_deviation": report.get("sum_single_layer_relative_deviation"),
                "all_layers_relative_deviation": report.get("all_layers_relative_deviation"),
                "additivity_gap": report.get("additivity_gap"),
                "additivity_tolerance": report.get("additivity_tolerance"),
            },
        )
