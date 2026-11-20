"""Layer-wise quantization, forward replay, attribution and budget evaluation."""

from __future__ import annotations

import copy
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import numpy as np

from .canonical import array_digest, canonical_digest, canonical_json
from .config import LayerQuantConfig, QuantConfig
from .data import (
    CalibrationBundle,
    LayerWeights,
    WeightBundle,
    ensure_calibration_finite,
)
from .errors import (
    E_ATTRIBUTION_GAP,
    E_INCREMENTAL_MISMATCH,
    E_INVALID_STAGE,
    E_QUANT_INVALID,
    AuditError,
)
from .quantization import QuantizedTensor, quantize_weight_and_bias


@dataclass(frozen=True)
class PreparedLayer:
    index: int
    weights: LayerWeights
    config: LayerQuantConfig
    weight_q: QuantizedTensor
    bias_q: QuantizedTensor

    @property
    def fingerprint(self) -> str:
        return canonical_digest(
            {
                "parameters": self.weights.fingerprint,
                "quantization": self.config.as_dict(),
            }
        )


@dataclass(frozen=True)
class EngineResult:
    result: dict[str, Any]
    arrays: dict[str, np.ndarray]
    layer_fingerprints: dict[str, str]
    calibration_fingerprint: str
    mode: str
    earliest_recomputed_layer: int
    reused_layers: tuple[str, ...]
    recomputed_layers: tuple[str, ...]

    @property
    def result_summary(self) -> str:
        return canonical_digest(self.result)


class AuditEngine:
    def __init__(
        self,
        config: QuantConfig,
        weights: WeightBundle,
        calibration: CalibrationBundle,
    ) -> None:
        self.config = config
        self.weights = weights
        self.calibration = calibration
        self._validate_inputs()
        self.prepared = self._prepare_layers()

    def _validate_inputs(self) -> None:
        names = self.weights.layer_names
        config_names = tuple(layer.name for layer in self.config.layers)
        if names != config_names:
            raise AuditError(
                E_INVALID_STAGE,
                "quantization configuration and weight layer order differ",
                configured=list(config_names),
                weights=list(names),
            )
        input_width = self.weights.layers[0].weight.shape[1]
        if self.calibration.samples.shape[1] != input_width:
            raise AuditError(
                E_INVALID_STAGE,
                "calibration tensor width does not match first layer input",
                calibration_width=int(self.calibration.samples.shape[1]),
                model_width=int(input_width),
            )
        ensure_calibration_finite(self.calibration.samples, names[0])

    def _prepare_layers(self) -> tuple[PreparedLayer, ...]:
        prepared: list[PreparedLayer] = []
        for index, (weights, layer_config) in enumerate(zip(self.weights.layers, self.config.layers)):
            weight_q, bias_q = quantize_weight_and_bias(weights.weight, weights.bias, layer_config)
            prepared.append(
                PreparedLayer(
                    index=index,
                    weights=weights,
                    config=layer_config,
                    weight_q=weight_q,
                    bias_q=bias_q,
                )
            )
        return tuple(prepared)

    @property
    def layer_names(self) -> tuple[str, ...]:
        return self.weights.layer_names

    @property
    def layer_fingerprints(self) -> dict[str, str]:
        return {layer.weights.name: layer.fingerprint for layer in self.prepared}

    def run_full(self) -> EngineResult:
        return self._run_full_impl()

    def run_incremental(
        self,
        previous_record: dict[str, Any],
        previous_arrays: dict[str, np.ndarray],
    ) -> EngineResult:
        earliest = self.earliest_affected_layer(previous_record)
        incremental = self._run_incremental_impl(previous_record, previous_arrays, earliest)
        full = self._run_full_impl()
        differing = _compare_results(full.result, incremental.result)
        differing_arrays = _compare_layer_arrays(full.arrays, incremental.arrays, self.layer_names)
        if differing or differing_arrays:
            layer_names = sorted(set(differing) | set(differing_arrays))
            raise AuditError(
                E_INCREMENTAL_MISMATCH,
                "incremental result differs from a clean full calibration",
                layers=layer_names,
                differing_result_layers=differing,
                differing_array_layers=differing_arrays,
            )
        return incremental

    def earliest_affected_layer(self, previous_record: dict[str, Any]) -> int:
        inputs = previous_record.get("inputs", {})
        if inputs.get("layer_names") != list(self.layer_names):
            return 0
        previous_calibration = inputs.get("calibration_fingerprint")
        if previous_calibration != self.calibration.fingerprint:
            return 0
        previous_fingerprints = previous_record.get("layer_fingerprints", {})
        for index, layer in enumerate(self.prepared):
            if previous_fingerprints.get(layer.weights.name) != layer.fingerprint:
                return index
        return len(self.prepared)

    def _run_full_impl(self) -> EngineResult:
        layer_results: list[dict[str, Any]] = []
        layer_arrays: dict[str, np.ndarray] = {}
        ref_states: list[np.ndarray] = [self.calibration.samples]
        quant_states: list[np.ndarray] = [self.calibration.samples]
        local_errors: list[np.ndarray] = []
        previous_bound = 0.0

        for layer in self.prepared:
            layer_result, arrays, ref_out, quant_out, local_error, previous_bound = self._compute_layer(
                layer,
                ref_states[-1],
                quant_states[-1],
                previous_bound,
            )
            ref_states.append(ref_out)
            quant_states.append(quant_out)
            local_errors.append(local_error)
            layer_results.append(layer_result)
            layer_arrays.update(arrays)

        prefix_outputs = self._full_prefix_outputs()
        for index, value in enumerate(prefix_outputs):
            layer_arrays[f"p_{index}"] = value

        result = self._finalize_result(layer_results, ref_states[-1], quant_states[-1], local_errors, prefix_outputs)
        return EngineResult(
            result=result,
            arrays=layer_arrays,
            layer_fingerprints=self.layer_fingerprints,
            calibration_fingerprint=self.calibration.fingerprint,
            mode="full",
            earliest_recomputed_layer=0,
            reused_layers=(),
            recomputed_layers=self.layer_names,
        )

    def _run_incremental_impl(
        self,
        previous_record: dict[str, Any],
        previous_arrays: dict[str, np.ndarray],
        earliest: int,
    ) -> EngineResult:
        if earliest == 0:
            result = self._run_full_impl()
            return EngineResult(
                result=result.result,
                arrays=result.arrays,
                layer_fingerprints=result.layer_fingerprints,
                calibration_fingerprint=result.calibration_fingerprint,
                mode="incremental",
                earliest_recomputed_layer=0,
                reused_layers=(),
                recomputed_layers=self.layer_names,
            )

        previous_layers = previous_record.get("result", {}).get("layers", [])
        if len(previous_layers) != len(self.prepared):
            raise AuditError(
                E_INVALID_STAGE,
                "previous audit record does not contain the complete layer prefix",
                expected_layers=len(self.prepared),
                actual_layers=len(previous_layers),
            )
        if not previous_record.get("stage_archive"):
            raise AuditError(E_INVALID_STAGE, "previous audit record has no stage archive")

        required: list[str] = []
        for index in range(earliest):
            required.extend(
                [
                    f"ref_out_{index}",
                    f"quant_out_{index}",
                    f"local_out_{index}",
                    f"local_error_{index}",
                ]
            )
        required.extend(f"p_{index}" for index in range(earliest + 1))
        missing = [key for key in required if key not in previous_arrays]
        if missing:
            raise AuditError(
                E_INVALID_STAGE,
                "previous audit record is missing prerequisite layer results",
                missing=missing,
                reusable_prefix=earliest,
            )

        layer_results: list[dict[str, Any]] = []
        layer_arrays: dict[str, np.ndarray] = {}
        ref_states: list[np.ndarray] = []
        quant_states: list[np.ndarray] = []
        local_errors: list[np.ndarray] = []
        previous_bound = 0.0

        for index in range(earliest):
            layer = self.prepared[index]
            arrays = {
                key: np.ascontiguousarray(previous_arrays[key])
                for key in (
                    f"ref_in_{index}",
                    f"ref_out_{index}",
                    f"quant_in_{index}",
                    f"quant_out_{index}",
                    f"local_out_{index}",
                    f"local_error_{index}",
                    f"weight_dequant_{index}",
                    f"bias_dequant_{index}",
                )
                if key in previous_arrays
            }
            required_arrays = (
                f"ref_in_{index}",
                f"ref_out_{index}",
                f"quant_in_{index}",
                f"quant_out_{index}",
            )
            missing_arrays = [key for key in required_arrays if key not in arrays]
            if missing_arrays:
                raise AuditError(
                    E_INVALID_STAGE,
                    "previous stage archive is missing a reused layer tensor",
                    layers=[layer.weights.name],
                    missing=missing_arrays,
                )
            ref_in = arrays[f"ref_in_{index}"]
            quant_in = arrays[f"quant_in_{index}"]
            ref_out = arrays[f"ref_out_{index}"]
            quant_out = arrays[f"quant_out_{index}"]
            local_out = arrays[f"local_out_{index}"]
            local_error = np.ascontiguousarray(local_out - ref_out)
            arrays[f"local_error_{index}"] = local_error
            arrays.setdefault(f"weight_dequant_{index}", layer.weight_q.dequantized)
            arrays.setdefault(f"bias_dequant_{index}", layer.bias_q.dequantized)

            prior_result = copy.deepcopy(previous_layers[index])
            if prior_result.get("name") != layer.weights.name:
                raise AuditError(
                    E_INVALID_STAGE,
                    "previous stage layer order does not match the current model",
                    index=index,
                    expected=layer.weights.name,
                    actual=prior_result.get("name"),
                )
            layer_results.append(prior_result)
            layer_arrays.update(arrays)
            if index == 0:
                ref_states.append(ref_in)
                quant_states.append(quant_in)
            ref_states.append(ref_out)
            quant_states.append(quant_out)
            local_errors.append(local_error)
            previous_bound = float(prior_result["error"]["propagated_abs_error_bound"])

        prefix_outputs: dict[int, np.ndarray] = {
            index: np.ascontiguousarray(previous_arrays[f"p_{index}"])
            for index in range(earliest + 1)
        }
        for index in range(earliest, len(self.prepared)):
            layer = self.prepared[index]
            layer_result, arrays, ref_out, quant_out, local_error, previous_bound = self._compute_layer(
                layer,
                ref_states[index],
                quant_states[index],
                previous_bound,
            )
            ref_states.append(ref_out)
            quant_states.append(quant_out)
            local_errors.append(local_error)
            layer_results.append(layer_result)
            layer_arrays.update(arrays)

        for index in range(earliest + 1, len(self.prepared) + 1):
            prefix_outputs[index] = self._prefix_output(index, earliest, ref_states, quant_states)

        ordered_prefix = [prefix_outputs[index] for index in range(len(self.prepared) + 1)]
        for index, value in enumerate(ordered_prefix):
            layer_arrays[f"p_{index}"] = value

        result = self._finalize_result(
            layer_results,
            ref_states[-1],
            quant_states[-1],
            local_errors,
            ordered_prefix,
        )
        return EngineResult(
            result=result,
            arrays=layer_arrays,
            layer_fingerprints=self.layer_fingerprints,
            calibration_fingerprint=self.calibration.fingerprint,
            mode="incremental",
            earliest_recomputed_layer=earliest,
            reused_layers=self.layer_names[:earliest],
            recomputed_layers=self.layer_names[earliest:],
        )

    def _compute_layer(
        self,
        layer: PreparedLayer,
        ref_input: np.ndarray,
        quant_input: np.ndarray,
        previous_bound: float,
    ) -> tuple[dict[str, Any], dict[str, np.ndarray], np.ndarray, np.ndarray, np.ndarray, float]:
        ensure_calibration_finite(ref_input, layer.weights.name)
        ensure_calibration_finite(quant_input, layer.weights.name)

        weight = layer.weights.weight
        bias = layer.weights.bias
        weight_q = layer.weight_q
        bias_q = layer.bias_q

        ref_out = np.ascontiguousarray(ref_input @ weight.T + bias)
        local_out = np.ascontiguousarray(ref_input @ weight_q.dequantized.T + bias_q.dequantized)
        quant_out = np.ascontiguousarray(quant_input @ weight_q.dequantized.T + bias_q.dequantized)
        if not np.all(np.isfinite(ref_out)) or not np.all(np.isfinite(quant_out)):
            raise AuditError(
                E_QUANT_INVALID,
                "forward pass produced NaN or Inf",
                layer=layer.weights.name,
            )

        local_error = np.ascontiguousarray(local_out - ref_out)
        output_error = np.ascontiguousarray(quant_out - ref_out)
        metrics = _error_metrics(output_error, ref_out)
        local_metrics = _error_metrics(local_error, ref_out)

        delta_weight = weight_q.dequantized - weight
        delta_bias = bias_q.dequantized - bias
        local_bound_matrix = np.abs(ref_input) @ np.abs(delta_weight).T + np.abs(delta_bias)
        local_bound = float(np.max(local_bound_matrix)) if local_bound_matrix.size else 0.0
        weight_inf_norm = float(np.max(np.sum(np.abs(weight_q.dequantized), axis=1)))
        propagated_bound = local_bound + weight_inf_norm * previous_bound
        reference_norm = max(float(np.max(np.abs(ref_out))), 1e-15)

        clamp_events = []
        for parameter, tensor in (("weight", weight_q), ("bias", bias_q)):
            if tensor.clamp_count:
                clamp_events.append(
                    {
                        "layer": layer.weights.name,
                        "parameter": parameter,
                        "count": tensor.clamp_count,
                        "positions": [list(position) for position in tensor.clamp_positions],
                    }
                )

        layer_result = {
            "index": layer.index,
            "name": layer.weights.name,
            "quantization": {
                "bits": layer.config.bits,
                "mode": layer.config.mode,
                "zero_point_policy": layer.config.zero_point,
                "weight": _tensor_metadata(weight_q),
                "bias": _tensor_metadata(bias_q),
            },
            "clamp_events": clamp_events,
            "error": {
                **metrics,
                "local_absolute_mean": local_metrics["absolute_mean"],
                "local_absolute_max": local_metrics["absolute_max"],
                "local_relative_l2": local_metrics["relative_l2"],
                "local_abs_error_bound": local_bound,
                "propagated_abs_error_bound": propagated_bound,
                "relative_error_bound": propagated_bound / reference_norm,
            },
        }
        arrays = {
            f"ref_in_{layer.index}": np.ascontiguousarray(ref_input),
            f"ref_out_{layer.index}": ref_out,
            f"quant_in_{layer.index}": np.ascontiguousarray(quant_input),
            f"quant_out_{layer.index}": quant_out,
            f"local_out_{layer.index}": local_out,
            f"local_error_{layer.index}": local_error,
            f"weight_dequant_{layer.index}": weight_q.dequantized,
            f"bias_dequant_{layer.index}": bias_q.dequantized,
        }
        return layer_result, arrays, ref_out, quant_out, local_error, propagated_bound

    def _full_prefix_outputs(self) -> list[np.ndarray]:
        outputs: list[np.ndarray] = []
        for quantized_prefix in range(len(self.prepared) + 1):
            value = self.calibration.samples
            for index, layer in enumerate(self.prepared):
                if index < quantized_prefix:
                    value = value @ layer.weight_q.dequantized.T + layer.bias_q.dequantized
                else:
                    value = value @ layer.weights.weight.T + layer.weights.bias
            outputs.append(np.ascontiguousarray(value))
        return outputs

    def _prefix_output(
        self,
        count: int,
        earliest: int,
        ref_states: list[np.ndarray],
        quant_states: list[np.ndarray],
    ) -> np.ndarray:
        value = self.calibration.samples if earliest == 0 else quant_states[earliest]
        for index in range(earliest, count):
            layer = self.prepared[index]
            value = value @ layer.weight_q.dequantized.T + layer.bias_q.dequantized
        for index in range(count, len(self.prepared)):
            layer = self.prepared[index]
            value = value @ layer.weights.weight.T + layer.weights.bias
        return np.ascontiguousarray(value)

    def _finalize_result(
        self,
        layer_results: list[dict[str, Any]],
        reference_output: np.ndarray,
        quantized_output: np.ndarray,
        local_errors: list[np.ndarray],
        prefix_outputs: list[np.ndarray],
    ) -> dict[str, Any]:
        end_to_end = _end_to_end_metrics(reference_output, quantized_output)
        attribution = self._attribute(reference_output, quantized_output, local_errors, prefix_outputs)
        budget = self._evaluate_budget(layer_results, attribution, end_to_end)
        return {
            "schema_version": 1,
            "layers": layer_results,
            "end_to_end": end_to_end,
            "attribution": attribution,
            "budget": budget,
        }

    def _attribute(
        self,
        reference_output: np.ndarray,
        quantized_output: np.ndarray,
        local_errors: list[np.ndarray],
        prefix_outputs: list[np.ndarray],
    ) -> dict[str, Any]:
        deviation = np.ascontiguousarray(quantized_output - reference_output)
        total_l2 = float(np.linalg.norm(deviation.ravel(order="C")))

        first_order_deltas: list[np.ndarray] = []
        for index, local_error in enumerate(local_errors):
            delta = local_error
            for downstream in range(index + 1, len(self.prepared)):
                delta = delta @ self.prepared[downstream].weights.weight.T
            first_order_deltas.append(np.ascontiguousarray(delta))

        replay_deltas = [
            np.ascontiguousarray(prefix_outputs[index + 1] - prefix_outputs[index])
            for index in range(len(self.prepared))
        ]
        first_order_sum = np.sum(np.stack(first_order_deltas, axis=0), axis=0) if first_order_deltas else deviation
        replay_sum = np.sum(np.stack(replay_deltas, axis=0), axis=0) if replay_deltas else deviation
        denominator = max(total_l2, 1e-15)
        first_order_gap = float(np.linalg.norm((first_order_sum - deviation).ravel(order="C")) / denominator)
        replay_gap = float(np.linalg.norm((replay_sum - deviation).ravel(order="C")) / denominator)
        tolerance = self.config.closure_tolerance
        if first_order_gap > tolerance + 1e-12 or replay_gap > tolerance + 1e-12:
            raise AuditError(
                E_ATTRIBUTION_GAP,
                "attribution contributions do not close against the end-to-end deviation",
                tolerance=tolerance,
                first_order_gap_ratio=first_order_gap,
                replay_gap_ratio=replay_gap,
                end_to_end_l2_error=total_l2,
            )

        first_order_scores = [float(np.linalg.norm(delta.ravel(order="C"))) for delta in first_order_deltas]
        replay_scores = [float(np.linalg.norm(delta.ravel(order="C"))) for delta in replay_deltas]
        first_order_ranks = _ranks(first_order_scores)
        replay_ranks = _ranks(replay_scores)
        first_order_sum_scores = sum(first_order_scores)
        replay_sum_scores = sum(replay_scores)
        first_order_shares = [
            score / first_order_sum_scores if first_order_sum_scores > 0.0 else 0.0
            for score in first_order_scores
        ]
        replay_shares = [
            score / replay_sum_scores if replay_sum_scores > 0.0 else 0.0
            for score in replay_scores
        ]
        primary_contributions = [share * total_l2 for share in replay_shares]
        first_order_contributions = [share * total_l2 for share in first_order_shares]

        layer_entries = []
        review_layers = []
        for index, layer in enumerate(self.prepared):
            rank_differs = first_order_ranks[index] != replay_ranks[index]
            review_reason = None
            if rank_differs:
                review_reason = (
                    "first-order and replay contribution ranks differ; first-order ignores "
                    "higher-order interactions between this layer's noise and downstream quantization"
                )
                review_layers.append(layer.weights.name)
            layer_entries.append(
                {
                    "name": layer.weights.name,
                    "first_order_score": first_order_scores[index],
                    "replay_score": replay_scores[index],
                    "first_order_rank": first_order_ranks[index],
                    "replay_rank": replay_ranks[index],
                    "first_order_share": first_order_shares[index],
                    "replay_share": replay_shares[index],
                    "first_order_contribution_l2": first_order_contributions[index],
                    "contribution_l2": primary_contributions[index],
                    "contribution_share": replay_shares[index],
                    "deviation_source": f"local weight/bias quantization noise at {layer.weights.name}",
                    "needs_review": rank_differs,
                    "review_reason": review_reason,
                }
            )
        allocated_sum = float(sum(entry["contribution_l2"] for entry in layer_entries))
        return {
            "end_to_end_l2_error": total_l2,
            "closure_tolerance": tolerance,
            "closure": {
                "first_order_sum_l2": float(np.linalg.norm(first_order_sum.ravel(order="C"))),
                "first_order_gap_ratio": first_order_gap,
                "replay_sum_l2": float(np.linalg.norm(replay_sum.ravel(order="C"))),
                "replay_gap_ratio": replay_gap,
                "allocated_sum_l2": allocated_sum,
                "allocated_gap_ratio": abs(allocated_sum - total_l2) / denominator,
                "passed": True,
            },
            "ranking_consistent": not review_layers,
            "review_required_layers": review_layers,
            "layers": layer_entries,
        }

    def _evaluate_budget(
        self,
        layer_results: list[dict[str, Any]],
        attribution: dict[str, Any],
        end_to_end: dict[str, Any],
    ) -> dict[str, Any]:
        metric = self.config.budget_metric
        total_error_value = _metric_value(end_to_end, metric)
        total_budget = self.config.total_budget
        total_ratio = total_error_value / total_budget if total_budget > 0.0 else (0.0 if total_error_value == 0.0 else float("inf"))
        total_over = total_error_value > total_budget
        total_l2 = attribution["end_to_end_l2_error"]

        entries: list[dict[str, Any]] = []
        over_layers: list[str] = []
        for index, layer in enumerate(layer_results):
            name = layer["name"]
            error_value = _metric_value(layer["error"], metric)
            budget = self.config.per_layer_budget[name]
            ratio = error_value / budget if budget > 0.0 else (0.0 if error_value == 0.0 else float("inf"))
            over = error_value > budget
            upstream_share = 0.0
            if total_l2 > 0.0:
                upstream_share = float(
                    sum(item["contribution_l2"] for item in attribution["layers"][:index]) / total_l2
                )
            diagnosis = None
            if over:
                over_layers.append(name)
                cannot_fix = upstream_share >= self.config.upstream_share_threshold
                if cannot_fix:
                    action = (
                        "Do not only raise this layer's bit width. First reduce propagated error from "
                        "upstream layers or recalibrate on a more representative sample, then reassess this layer."
                    )
                else:
                    action = "Evaluate a higher bit width, a different zero-point policy, or a tighter calibration set for this layer."
                diagnosis = {
                    "layer": name,
                    "cannot_fix_by_raising_bits_alone": cannot_fix,
                    "upstream_contribution_share": upstream_share,
                    "upstream_share_threshold": self.config.upstream_share_threshold,
                    "suggested_action": action,
                }
            entries.append(
                {
                    "name": name,
                    "metric": metric,
                    "error": error_value,
                    "budget": budget,
                    "occupancy_ratio": ratio,
                    "margin": budget - error_value,
                    "over_budget": over,
                    "upstream_contribution_share": upstream_share,
                    "diagnosis": diagnosis,
                }
            )

        within = [entry for entry in entries if not entry["over_budget"]]
        if within:
            closest = max(within, key=lambda item: item["occupancy_ratio"])
        elif entries:
            closest = min(entries, key=lambda item: item["occupancy_ratio"])
        else:
            closest = None

        diagnostics = [entry["diagnosis"] for entry in entries if entry["diagnosis"] is not None]
        return {
            "metric": metric,
            "per_layer": entries,
            "total": {
                "error": total_error_value,
                "budget": total_budget,
                "occupancy_ratio": total_ratio,
                "margin": total_budget - total_error_value,
                "over_budget": total_over,
            },
            "over_budget_layers": over_layers,
            "closest_to_budget": None
            if closest is None
            else {
                "name": closest["name"],
                "occupancy_ratio": closest["occupancy_ratio"],
                "margin": closest["margin"],
            },
            "diagnostics": diagnostics,
            "usable": not over_layers and not total_over,
        }


def _tensor_metadata(tensor: QuantizedTensor) -> dict[str, Any]:
    return {
        "scale": tensor.scale,
        "zero_point": tensor.zero_point,
        "qmin": tensor.qmin,
        "qmax": tensor.qmax,
        "clamp_count": tensor.clamp_count,
        "clamp_positions": [list(position) for position in tensor.clamp_positions],
    }


def _error_metrics(error: np.ndarray, reference: np.ndarray) -> dict[str, float]:
    absolute = np.abs(error)
    reference_l2 = float(np.linalg.norm(reference.ravel(order="C")))
    error_l2 = float(np.linalg.norm(error.ravel(order="C")))
    absolute_max = float(np.max(absolute)) if absolute.size else 0.0
    reference_inf = max(float(np.max(np.abs(reference))) if reference.size else 0.0, 1e-15)
    return {
        "absolute_mean": float(np.mean(absolute)) if absolute.size else 0.0,
        "absolute_max": absolute_max,
        "l2_error": error_l2,
        "relative_l2": error_l2 / max(reference_l2, 1e-15),
        "relative_max": absolute_max / reference_inf,
    }


def _end_to_end_metrics(reference: np.ndarray, quantized: np.ndarray) -> dict[str, float]:
    metrics = _error_metrics(quantized - reference, reference)
    metrics["reference_l2"] = float(np.linalg.norm(reference.ravel(order="C")))
    metrics["reference_absolute_max"] = float(np.max(np.abs(reference))) if reference.size else 0.0
    return metrics


def _metric_value(metrics: dict[str, Any], metric: str) -> float:
    return float(metrics[metric])


def _ranks(scores: list[float]) -> list[int]:
    order = sorted(range(len(scores)), key=lambda index: (-scores[index], index))
    ranks = [0] * len(scores)
    for rank, index in enumerate(order, start=1):
        ranks[index] = rank
    return ranks


def _compare_results(full: dict[str, Any], incremental: dict[str, Any]) -> list[str]:
    differing: list[str] = []
    full_layers = full.get("layers", [])
    incremental_layers = incremental.get("layers", [])
    for index in range(max(len(full_layers), len(incremental_layers))):
        if index >= len(full_layers) or index >= len(incremental_layers):
            differing.append(f"layer_{index:02d}")
            continue
        if canonical_json(full_layers[index]) != canonical_json(incremental_layers[index]):
            differing.append(str(full_layers[index].get("name", f"layer_{index:02d}")))
    if canonical_json(full.get("attribution")) != canonical_json(incremental.get("attribution")):
        differing.extend(
            item["name"]
            for item in full.get("attribution", {}).get("layers", [])
            if item["name"] not in differing
        )
    if canonical_json(full.get("budget")) != canonical_json(incremental.get("budget")):
        for item in full.get("budget", {}).get("per_layer", []):
            if item["name"] not in differing:
                differing.append(item["name"])
    if canonical_json(full.get("end_to_end")) != canonical_json(incremental.get("end_to_end")):
        differing.append("end_to_end")
    return sorted(set(differing))


def _compare_layer_arrays(
    full_arrays: dict[str, np.ndarray],
    incremental_arrays: dict[str, np.ndarray],
    layer_names: Iterable[str],
) -> list[str]:
    differing: list[str] = []
    for index, name in enumerate(layer_names):
        for prefix in ("ref_out", "quant_out", "local_out", "local_error"):
            key = f"{prefix}_{index}"
            if key not in full_arrays or key not in incremental_arrays:
                differing.append(name)
                break
            if array_digest(full_arrays[key]) != array_digest(incremental_arrays[key]):
                differing.append(name)
                break
    for key in sorted(set(full_arrays) & set(incremental_arrays)):
        if key.startswith("p_") and array_digest(full_arrays[key]) != array_digest(incremental_arrays[key]):
            differing.append(f"prefix_{key}")
    return sorted(set(differing))

