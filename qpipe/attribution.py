from __future__ import annotations

from typing import Any

import numpy as np

from .calibration import prepare_layer_inputs
from .forward import compute_layer_forward, forward_input_fingerprint
from .util import array_fingerprint, digest_obj, json_ready


def _error_metrics(actual: np.ndarray, reference: np.ndarray) -> dict[str, Any]:
    delta = actual - reference
    absolute = np.abs(delta)
    per_sample_l1 = np.sum(absolute, axis=1)
    per_sample_max = np.max(absolute, axis=1)
    reference_l1 = float(np.sum(np.abs(reference)))
    absolute_l1 = float(np.sum(absolute))
    relative = absolute_l1 / reference_l1 if reference_l1 > 0.0 else 0.0
    return {
        "max_abs_error": float(np.max(absolute)),
        "mean_abs_error": float(np.mean(absolute)),
        "absolute_l1_error": absolute_l1,
        "relative_l1_error": float(relative),
        "absolute_error": json_ready(absolute),
        "max_abs_position": [
            int(position) for position in np.unravel_index(np.argmax(absolute), absolute.shape)
        ],
        "sample_absolute_l1_error": json_ready(per_sample_l1),
        "sample_max_abs_error": json_ready(per_sample_max),
        "output_sha256": array_fingerprint(actual),
        "reference_sha256": array_fingerprint(reference),
    }


def _rank_contributions(values: dict[str, float]) -> dict[str, int]:
    ordered = sorted(values.items(), key=lambda item: (-item[1], item[0]))
    return {layer_id: index + 1 for index, (layer_id, _) in enumerate(ordered)}


def _normalize_contributions(
    raw: dict[str, float], total: float
) -> tuple[dict[str, float], dict[str, Any]]:
    raw_sum = float(sum(raw.values()))
    layers = sorted(raw)
    if total == 0.0:
        contributions = {layer_id: 0.0 for layer_id in layers}
        residual_layer = None
    elif raw_sum > 0.0:
        contributions = {
            layer_id: total * (raw[layer_id] / raw_sum) for layer_id in layers
        }
        residual_layer = min(
            layers, key=lambda layer_id: (-raw[layer_id], layer_id)
        )
    else:
        contributions = {layer_id: total / len(layers) for layer_id in layers}
        residual_layer = layers[0]
    residual = total - float(sum(contributions.values()))
    if residual_layer is not None:
        contributions[residual_layer] += residual
    final_residual = total - float(sum(contributions.values()))
    return contributions, {
        "raw_sum": raw_sum,
        "total": total,
        "normalization_factor": (total / raw_sum) if raw_sum > 0.0 else None,
        "residual_assigned_to": residual_layer,
        "residual_before_assignment": residual,
        "residual_after_assignment": final_residual,
    }


def compute_attribution(
    *,
    config: dict[str, Any],
    calibration_payload: dict[str, Any],
    forward_payload: dict[str, Any],
    attribution_stage_fingerprint: str,
) -> dict[str, Any]:
    prepared_layers = prepare_layer_inputs(config)
    layers = config["manifest"]["layers"]
    calibrations = calibration_payload["layers"]
    forward_layers = forward_payload["layers"]

    reference_inputs: list[np.ndarray] = []
    reference_outputs: list[np.ndarray] = []
    current = prepared_layers[0]["activation"].copy()
    for prepared in prepared_layers:
        reference_inputs.append(current.copy())
        reference = current @ prepared["weight"].T
        reference_outputs.append(reference)
        current = reference

    quantized_outputs = [
        np.asarray(forward_layers[layer["id"]]["output"], dtype=np.float64)
        for layer in layers
    ]
    for layer, actual, reference in zip(
        layers, quantized_outputs, reference_outputs, strict=True
    ):
        if actual.shape != reference.shape:
            raise ValueError(
                f"forward/reference shape mismatch in layer {layer['id']}: "
                f"{actual.shape} vs {reference.shape}"
            )

    final_delta = quantized_outputs[-1] - reference_outputs[-1]
    end_to_end_absolute_l1 = float(np.sum(np.abs(final_delta)))
    end_to_end_reference_l1 = float(np.sum(np.abs(reference_outputs[-1])))
    end_to_end_relative = (
        end_to_end_absolute_l1 / end_to_end_reference_l1
        if end_to_end_reference_l1 > 0.0
        else 0.0
    )

    sensitivity_raw: dict[str, float] = {}
    replay_raw: dict[str, float] = {}
    local_diagnostics: dict[str, dict[str, Any]] = {}
    downstream_norms: dict[str, float] = {}

    for index, (layer, prepared) in enumerate(zip(layers, prepared_layers, strict=True)):
        layer_id = layer["id"]
        local_input = reference_inputs[index]
        local_input_fp = forward_input_fingerprint(
            stage_fingerprint=attribution_stage_fingerprint,
            calibration=calibrations[layer_id],
            activation=local_input,
            accumulator_bits=int(layer["accumulator_bits"]),
        )
        local_result = compute_layer_forward(
            layer=layer,
            calibration=calibrations[layer_id],
            activation=local_input,
            weight=prepared["weight"],
            input_fingerprint=local_input_fp,
        )
        local_output = np.asarray(local_result["output"], dtype=np.float64)
        local_delta = local_output - reference_outputs[index]

        output_dim = reference_outputs[index].shape[1]
        path = np.eye(output_dim, dtype=np.float64)
        for downstream in layers[index + 1 :]:
            downstream_weight = prepared_layers[layers.index(downstream)]["weight"]
            path = path @ downstream_weight.T
        sensitivity_delta = np.abs(local_delta) @ np.abs(path)
        replay_delta = local_delta @ path
        sensitivity_raw[layer_id] = float(np.sum(sensitivity_delta))
        replay_raw[layer_id] = float(np.sum(np.abs(replay_delta)))
        downstream_norms[layer_id] = float(np.max(np.sum(np.abs(path), axis=0)))
        local_diagnostics[layer_id] = {
            "local_max_abs_delta": float(np.max(np.abs(local_delta))),
            "local_l1_delta": float(np.sum(np.abs(local_delta))),
            "local_accumulator_overflow_clamp_count": int(
                local_result["accumulator"]["overflow_clamp_count"]
            ),
            "downstream_gain_l1": downstream_norms[layer_id],
        }

    sensitivity_contributions, sensitivity_normalization = _normalize_contributions(
        sensitivity_raw, end_to_end_absolute_l1
    )
    replay_contributions, replay_normalization = _normalize_contributions(
        replay_raw, end_to_end_absolute_l1
    )
    sensitivity_rank = _rank_contributions(sensitivity_contributions)
    replay_rank = _rank_contributions(replay_contributions)

    layer_results: dict[str, Any] = {}
    for layer, actual, reference in zip(
        layers, quantized_outputs, reference_outputs, strict=True
    ):
        layer_id = layer["id"]
        metrics = _error_metrics(actual, reference)
        sensitivity_rank_value = sensitivity_rank[layer_id]
        replay_rank_value = replay_rank[layer_id]
        layer_results[layer_id] = {
            "layer": layer_id,
            "source_layer": layer_id,
            "input_fingerprint": forward_layers[layer_id]["input_fingerprint"],
            "actual_error": metrics,
            "attribution": {
                "first_order_sensitivity": {
                    "raw": sensitivity_raw[layer_id],
                    "contribution": sensitivity_contributions[layer_id],
                    "share": (
                        sensitivity_contributions[layer_id]
                        / end_to_end_absolute_l1
                        if end_to_end_absolute_l1 > 0.0
                        else 0.0
                    ),
                    "rank": sensitivity_rank_value,
                    "basis": "sum(abs(local_delta) @ abs(downstream_jacobian))",
                },
                "layer_perturbation_replay": {
                    "raw": replay_raw[layer_id],
                    "contribution": replay_contributions[layer_id],
                    "share": (
                        replay_contributions[layer_id] / end_to_end_absolute_l1
                        if end_to_end_absolute_l1 > 0.0
                        else 0.0
                    ),
                    "rank": replay_rank_value,
                    "basis": "sum(abs(local_delta @ downstream_jacobian))",
                },
                "review_required": sensitivity_rank_value != replay_rank_value,
                "local_diagnostics": local_diagnostics[layer_id],
            },
        }

    sensitivity_sum = float(sum(sensitivity_contributions.values()))
    replay_sum = float(sum(replay_contributions.values()))
    requested_tolerance = float(
        config["manifest"].get("attribution_sum_tolerance", 1e-9)
    )
    sensitivity_residual = abs(sensitivity_sum - end_to_end_absolute_l1)
    replay_residual = abs(replay_sum - end_to_end_absolute_l1)
    if sensitivity_residual > requested_tolerance or replay_residual > requested_tolerance:
        raise ValueError(
            "attribution contributions do not sum to end-to-end deviation within "
            f"declared tolerance {requested_tolerance}"
        )

    review_layers = sorted(
        layer_id
        for layer_id, result in layer_results.items()
        if result["attribution"]["review_required"]
    )
    return {
        "stage": "attribution",
        "reference": {
            "dtype": "float64",
            "final_output_sha256": array_fingerprint(reference_outputs[-1]),
            "final_absolute_l1": end_to_end_reference_l1,
        },
        "quantized": {
            "final_output_sha256": array_fingerprint(quantized_outputs[-1]),
        },
        "end_to_end": {
            "absolute_l1_deviation": end_to_end_absolute_l1,
            "max_abs_deviation": float(np.max(np.abs(final_delta))),
            "relative_l1_deviation": end_to_end_relative,
            "sample_absolute_l1_deviation": json_ready(
                np.sum(np.abs(final_delta), axis=1)
            ),
        },
        "contribution_consistency": {
            "declared_tolerance": requested_tolerance,
            "first_order_sensitivity_sum": sensitivity_sum,
            "first_order_sensitivity_residual": sensitivity_residual,
            "layer_perturbation_replay_sum": replay_sum,
            "layer_perturbation_replay_residual": replay_residual,
            "consistent": True,
            "normalization": {
                "first_order_sensitivity": sensitivity_normalization,
                "layer_perturbation_replay": replay_normalization,
            },
        },
        "ranking": {
            "first_order_sensitivity": sensitivity_rank,
            "layer_perturbation_replay": replay_rank,
            "consistent": not review_layers,
            "review_required_layers": review_layers,
        },
        "layers": layer_results,
        "input_fingerprint": digest_obj(
            {
                "stage_fingerprint": attribution_stage_fingerprint,
                "forward_layers": {
                    layer_id: forward_layers[layer_id]["output_sha256"]
                    for layer_id in forward_layers
                },
            }
        ),
    }
