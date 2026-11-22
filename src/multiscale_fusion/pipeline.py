from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import numpy as np

from .attention import apply_channel_attention, normalize_attention, validate_fusion_coeffs
from .errors import FusionError
from .kernels import conv2d_nchw, resize_nchw
from .manifest import AttentionSpec, FusionSpec, LayerSpec, TaskSpec
from .util import sha256_array


@dataclass
class LayerOutput:
    name: str
    manifest_index: int
    raw: np.ndarray
    lateral: np.ndarray
    attention_weighted: np.ndarray
    attention_meta: dict[str, Any]
    fusion_coeffs: dict[str, float]
    conv_meta: dict[str, Any]
    upsample_meta: dict[str, Any] | None
    range_observed: tuple[float, float]
    raw_sha256: str

    def diagnostics(self) -> dict[str, Any]:
        return {
            "layer": self.name,
            "manifest_index": self.manifest_index,
            "shape": list(self.raw.shape),
            "dtype": str(self.raw.dtype),
            "raw_sha256": self.raw_sha256,
            "attention": self.attention_meta,
            "fusion_coeffs": dict(sorted(self.fusion_coeffs.items())),
            "lateral": self.conv_meta,
            "upsample": self.upsample_meta,
            "range_observed": [self.range_observed[0], self.range_observed[1]],
        }


@dataclass
class FusionResult:
    task_id: str
    layer_outputs: dict[str, LayerOutput]
    processing_order: tuple[str, ...]
    final: np.ndarray
    final_sha256: str

    def raw_arrays(self) -> dict[str, np.ndarray]:
        return {name: output.raw for name, output in self.layer_outputs.items()}

    def summary(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "processing_order": list(self.processing_order),
            "final_sha256": self.final_sha256,
            "layer_summaries": {
                name: {
                    "shape": list(output.raw.shape),
                    "raw_sha256": output.raw_sha256,
                    "range_observed": list(output.range_observed),
                }
                for name, output in sorted(self.layer_outputs.items())
            },
        }


def _validate_input_array(layer: str, array: np.ndarray) -> np.ndarray:
    if array.ndim != 4:
        raise FusionError.shape_mismatch(layer, [0, 0, 0, 0], array.shape, stage="input")
    if not np.issubdtype(array.dtype, np.number):
        raise FusionError.invalid_param(layer, None, "input.dtype", "feature shard must be numeric", dtype=str(array.dtype))
    converted = np.asarray(array, dtype=np.float64)
    finite = np.isfinite(converted)
    if not np.all(finite):
        first = int(np.flatnonzero(~finite.ravel())[0])
        raise FusionError.numeric(layer, "non_finite_input", index=first, non_finite_count=int(np.count_nonzero(~finite)))
    return converted


def compute_layer_output(
    task: TaskSpec,
    layer: LayerSpec,
    x: np.ndarray,
    conv_spec: tuple[np.ndarray, np.ndarray, dict[str, Any]],
    previous: np.ndarray | None,
    *,
    attention_values: tuple[float, ...] | None = None,
    fusion_coeffs_override: Mapping[str, float] | None = None,
) -> LayerOutput:
    """Compute one indivisible pyramid layer from an already materialized predecessor."""

    x = _validate_input_array(layer.name, x)
    weights, bias, conv_seed_meta = conv_spec
    lateral, conv_meta = conv2d_nchw(x, weights, bias, layer.lateral.stride, layer.lateral.padding, layer=layer.name)
    conv_meta = {**conv_seed_meta, **conv_meta}
    raw_attention = tuple(attention_values) if attention_values is not None else layer.attention.weights
    attention_spec = AttentionSpec(weights=raw_attention, normalization=layer.attention.normalization)
    effective_attention, attention_meta = normalize_attention(attention_spec, lateral.shape[1], layer=layer.name)
    attention_weighted = apply_channel_attention(lateral, effective_attention, layer=layer.name)
    coeffs_source = dict(fusion_coeffs_override if fusion_coeffs_override is not None else layer.fusion.coeffs)
    coeffs = validate_fusion_coeffs(coeffs_source, layer=layer.name, require_topdown=previous is not None)
    upsample_meta: dict[str, Any] | None = None
    if previous is None:
        fused = coeffs["lateral"] * attention_weighted
    else:
        if layer.upsample is None:
            raise FusionError.invalid_param(layer.name, None, "upsample", "missing top-down connection")
        aligned, upsample_meta = resize_nchw(previous, attention_weighted.shape[2:], layer.upsample, layer=layer.name)
        if aligned.shape != attention_weighted.shape:
            raise FusionError.shape_mismatch(
                layer.name,
                attention_weighted.shape,
                aligned.shape,
                stage="fusion_alignment",
                message=(
                    f"layer {layer.name!r} top-down tensor shape {list(aligned.shape)} "
                    f"does not match lateral tensor shape {list(attention_weighted.shape)}"
                ),
            )
        fused = coeffs["lateral"] * attention_weighted + coeffs["topdown"] * aligned
    if tuple(fused.shape) != layer.expected_output_shape:
        raise FusionError.shape_mismatch(layer.name, layer.expected_output_shape, fused.shape, stage="declared_output")
    if not np.all(np.isfinite(fused)):
        count = int(np.count_nonzero(~np.isfinite(fused)))
        raise FusionError.numeric(layer.name, "non_finite_fusion_output", non_finite_count=count)
    observed_min = float(np.min(fused))
    observed_max = float(np.max(fused))
    low, high = layer.allowed_range
    if observed_min < low or observed_max > high:
        bad = fused < low
        bad |= fused > high
        first = int(np.flatnonzero(bad.ravel())[0])
        raise FusionError.numeric(
            layer.name,
            "allowed_range_violation",
            index=first,
            observed_min=observed_min,
            observed_max=observed_max,
            allowed_range=[low, high],
            violating_count=int(np.count_nonzero(bad)),
        )
    return LayerOutput(
        name=layer.name,
        manifest_index=layer.manifest_index,
        raw=fused,
        lateral=lateral,
        attention_weighted=attention_weighted,
        attention_meta=attention_meta,
        fusion_coeffs=dict(coeffs),
        conv_meta=conv_meta,
        upsample_meta=upsample_meta,
        range_observed=(observed_min, observed_max),
        raw_sha256=sha256_array(fused),
    )


def run_fusion(
    task: TaskSpec,
    inputs: Mapping[str, np.ndarray],
    conv_params: Mapping[str, tuple[np.ndarray, np.ndarray, dict[str, Any]]],
    *,
    attention_overrides: Mapping[str, tuple[float, ...]] | None = None,
    fusion_overrides: Mapping[str, Mapping[str, float]] | None = None,
) -> FusionResult:
    """Run the top-down pyramid fusion without checkpoint or quantization side effects."""

    attention_overrides = attention_overrides or {}
    fusion_overrides = fusion_overrides or {}
    outputs: dict[str, LayerOutput] = {}
    previous: np.ndarray | None = None
    processing_order: list[str] = []
    for layer in task.processing_layers:
        if layer.name not in inputs:
            raise FusionError.invalid_stage(f"input shard for layer {layer.name!r} was not loaded", layer=layer.name)
        if layer.name not in conv_params:
            raise FusionError.invalid_stage(f"convolution parameters for layer {layer.name!r} are missing", layer=layer.name)
        output = compute_layer_output(
            task,
            layer,
            inputs[layer.name],
            conv_params[layer.name],
            previous,
            attention_values=attention_overrides.get(layer.name),
            fusion_coeffs_override=fusion_overrides.get(layer.name),
        )
        outputs[layer.name] = output
        processing_order.append(layer.name)
        previous = output.raw
    finest = task.finest_layer.name
    final = outputs[finest].raw
    return FusionResult(
        task_id=task.id,
        layer_outputs=outputs,
        processing_order=tuple(processing_order),
        final=final,
        final_sha256=sha256_array(final),
    )

def relative_l2_deviation(reference: np.ndarray, candidate: np.ndarray, *, floor: float = 1e-30) -> float:
    delta = np.asarray(candidate, dtype=np.float64) - np.asarray(reference, dtype=np.float64)
    denominator = float(np.linalg.norm(np.asarray(reference, dtype=np.float64).ravel()))
    numerator = float(np.linalg.norm(delta.ravel()))
    return numerator / max(denominator, floor)
