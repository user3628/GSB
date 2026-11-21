from __future__ import annotations

import hashlib
from typing import Any

import numpy as np

from .calibration import dequantize_array, quantize_array
from .util import array_fingerprint, digest_obj, json_ready


def _parameter_vector(
    parameter: Any,
    *,
    granularity: str,
    axis: int | None,
    size: int,
    dtype: Any,
) -> np.ndarray:
    if granularity == "per_tensor":
        return np.asarray(parameter, dtype=dtype).reshape(1)
    if int(axis) != 0:
        raise ValueError("parameter vector requested for non-leading channel axis")
    values = np.asarray(parameter, dtype=dtype).reshape(-1)
    if values.shape[0] != size:
        raise ValueError("channel parameter length does not match array dimension")
    return values


def _weight_scale_for_k(weight_params: dict[str, Any], k: int, out_channels: int) -> np.ndarray:
    if weight_params["granularity"] == "per_tensor":
        return np.full(out_channels, float(weight_params["scale"]), dtype=np.float64)
    axis = int(weight_params["channel_axis"])
    if axis != 0:
        raise ValueError("factorized path requires weight channel_axis=0")
    return np.asarray(weight_params["scale"], dtype=np.float64)


def _accumulator_digest(accumulator: np.ndarray) -> str:
    hasher = hashlib.sha256()
    header = digest_obj(
        {"dtype": "<i8", "shape": list(accumulator.shape), "order": "C"}
    ).encode("ascii")
    hasher.update(header)
    hasher.update(np.ascontiguousarray(accumulator, dtype="<i8").tobytes())
    return hasher.hexdigest()


def _factorized_quantized_linear(
    activation: np.ndarray,
    weight: np.ndarray,
    activation_params: dict[str, Any],
    weight_params: dict[str, Any],
    accumulator_bits: int,
) -> tuple[np.ndarray, dict[str, Any]]:
    activation_quantized, activation_saturation, _, _ = quantize_array(
        activation,
        scale=activation_params["scale"],
        zero_point=activation_params["zero_point"],
        bits=activation_params["bits"],
        mode=activation_params["mode"],
        granularity=activation_params["granularity"],
        channel_axis=activation_params["channel_axis"],
    )
    weight_quantized, weight_saturation, _, _ = quantize_array(
        weight,
        scale=weight_params["scale"],
        zero_point=weight_params["zero_point"],
        bits=weight_params["bits"],
        mode=weight_params["mode"],
        granularity=weight_params["granularity"],
        channel_axis=weight_params["channel_axis"],
    )
    batch_size, in_features = activation.shape
    out_channels = weight.shape[0]
    if activation_params["granularity"] == "per_tensor":
        activation_scale_term = np.asarray(
            float(activation_params["scale"]), dtype=np.float64
        )
        shifted_activation = activation_quantized - int(
            activation_params["zero_point"]
        )
    else:
        activation_scale = _parameter_vector(
            activation_params["scale"],
            granularity="per_channel",
            axis=activation_params["channel_axis"],
            size=batch_size,
            dtype=np.float64,
        )
        activation_zero = _parameter_vector(
            activation_params["zero_point"],
            granularity="per_channel",
            axis=activation_params["channel_axis"],
            size=batch_size,
            dtype=np.int64,
        )
        activation_scale_term = activation_scale.reshape(batch_size, 1)
        shifted_activation = activation_quantized - activation_zero.reshape(
            batch_size, 1
        )
    if weight_params["granularity"] == "per_tensor":
        weight_scale_term = np.asarray(
            float(weight_params["scale"]), dtype=np.float64
        )
        shifted_weight = weight_quantized - int(weight_params["zero_point"])
    else:
        weight_scale = _parameter_vector(
            weight_params["scale"],
            granularity="per_channel",
            axis=weight_params["channel_axis"],
            size=out_channels,
            dtype=np.float64,
        )
        weight_zero = _parameter_vector(
            weight_params["zero_point"],
            granularity="per_channel",
            axis=weight_params["channel_axis"],
            size=out_channels,
            dtype=np.int64,
        )
        weight_scale_term = weight_scale.reshape(1, out_channels)
        shifted_weight = weight_quantized - weight_zero.reshape(out_channels, 1)
    accumulator_min = -(1 << (accumulator_bits - 1))
    accumulator_max = (1 << (accumulator_bits - 1)) - 1
    accumulator = np.zeros((batch_size, out_channels), dtype=np.int64)
    clamp_count = 0
    clamped_positions: list[list[int]] = []
    for feature in range(in_features):
        term = (
            shifted_activation[:, feature].reshape(batch_size, 1)
            * shifted_weight[:, feature].reshape(1, out_channels)
        )
        candidate = accumulator + term
        clipped = np.clip(candidate, accumulator_min, accumulator_max)
        changed = candidate != clipped
        changed_count = int(np.count_nonzero(changed))
        if changed_count:
            clamp_count += changed_count
            if len(clamped_positions) < 16:
                for position in np.argwhere(changed)[: 16 - len(clamped_positions)]:
                    clamped_positions.append([int(position[0]), int(position[1])])
        accumulator = clipped

    output = accumulator.astype(np.float64) * weight_scale_term * activation_scale_term
    diagnostics = {
        "accumulator_bits": accumulator_bits,
        "accumulator_mode": "fixed_width_integer",
        "accumulator_min": accumulator_min,
        "accumulator_max": accumulator_max,
        "clamp_count": clamp_count,
        "clamped_positions": clamped_positions,
        "accumulator_min_observed": int(np.min(accumulator)),
        "accumulator_max_observed": int(np.max(accumulator)),
        "accumulator_max_abs": int(np.max(np.abs(accumulator))),
        "accumulator_sum": int(np.sum(accumulator, dtype=np.int64)),
        "accumulator_sha256": _accumulator_digest(accumulator),
        "weight_saturation_count": int(weight_saturation),
        "activation_saturation_count": int(activation_saturation),
    }
    return output, diagnostics


def _mixed_scale_quantized_linear(
    activation: np.ndarray,
    weight: np.ndarray,
    activation_params: dict[str, Any],
    weight_params: dict[str, Any],
    accumulator_bits: int,
) -> tuple[np.ndarray, dict[str, Any]]:
    """Correct mixed-scale reduction for non-leading channel-axis configurations.

    The standard integer-accumulator path is used whenever scales can be factored
    out of the reduction (both operands are per-tensor or channel axis 0). This
    fallback keeps integer products but applies the scale chain per reduction
    term; it is reported explicitly rather than pretending to be a single GEMM.
    """

    activation_quantized, activation_saturation, _, _ = quantize_array(
        activation,
        scale=activation_params["scale"],
        zero_point=activation_params["zero_point"],
        bits=activation_params["bits"],
        mode=activation_params["mode"],
        granularity=activation_params["granularity"],
        channel_axis=activation_params["channel_axis"],
    )
    weight_quantized, weight_saturation, _, _ = quantize_array(
        weight,
        scale=weight_params["scale"],
        zero_point=weight_params["zero_point"],
        bits=weight_params["bits"],
        mode=weight_params["mode"],
        granularity=weight_params["granularity"],
        channel_axis=weight_params["channel_axis"],
    )
    batch_size, in_features = activation.shape
    out_channels = weight.shape[0]
    output = np.zeros((batch_size, out_channels), dtype=np.float64)
    integer_product_max_abs = 0
    for feature in range(in_features):
        if activation_params["granularity"] == "per_tensor":
            activation_scale = float(activation_params["scale"])
            activation_zero = int(activation_params["zero_point"])
        else:
            activation_scale = float(
                np.asarray(activation_params["scale"])[
                    _channel_index(activation_params["channel_axis"], feature, activation.ndim)
                ]
            )
            activation_zero = int(
                np.asarray(activation_params["zero_point"])[
                    _channel_index(activation_params["channel_axis"], feature, activation.ndim)
                ]
            )
        if weight_params["granularity"] == "per_tensor":
            weight_scale = np.full(
                out_channels, float(weight_params["scale"]), dtype=np.float64
            )
            weight_zero = np.full(
                out_channels, int(weight_params["zero_point"]), dtype=np.int64
            )
        else:
            weight_axis = int(weight_params["channel_axis"])
            if weight_axis == 0:
                weight_scale = np.asarray(weight_params["scale"], dtype=np.float64)
                weight_zero = np.asarray(weight_params["zero_point"], dtype=np.int64)
            else:
                index = _channel_index(weight_axis, feature, weight.ndim)
                weight_scale = np.asarray(
                    [float(np.asarray(weight_params["scale"])[index])]
                )
                weight_zero = np.asarray(
                    [int(np.asarray(weight_params["zero_point"])[index])]
                )
        term = (
            activation_quantized[:, feature].reshape(batch_size, 1)
            - activation_zero
        ) * (
            weight_quantized[:, feature].reshape(1, out_channels)
            - weight_zero.reshape(1, -1)
        )
        integer_product_max_abs = max(integer_product_max_abs, int(np.max(np.abs(term))))
        output += (
            term.astype(np.float64)
            * activation_scale
            * weight_scale.reshape(1, out_channels)
        )
    diagnostics = {
        "accumulator_bits": accumulator_bits,
        "accumulator_mode": "integer_products_with_mixed_scale_reduction",
        "clamp_count": 0,
        "clamped_positions": [],
        "integer_product_max_abs": integer_product_max_abs,
        "accumulator_sha256": array_fingerprint(output),
        "weight_saturation_count": int(weight_saturation),
        "activation_saturation_count": int(activation_saturation),
    }
    return output, diagnostics


def _channel_index(axis: int, reduction_index: int, ndim: int) -> int:
    normalized = axis % ndim
    if normalized == 1:
        return reduction_index
    return 0


def _factorizable(activation_params: dict[str, Any], weight_params: dict[str, Any]) -> bool:
    for params in (activation_params, weight_params):
        if params["granularity"] == "per_channel" and int(params["channel_axis"]) != 0:
            return False
    return True


def compute_layer_forward(
    *,
    layer: dict[str, Any],
    calibration: dict[str, Any],
    activation: np.ndarray,
    weight: np.ndarray,
    input_fingerprint: str,
) -> dict[str, Any]:
    accumulator_bits = int(layer["accumulator_bits"])
    activation_params = calibration["activation"]
    weight_params = calibration["weight"]
    if _factorizable(activation_params, weight_params):
        output, diagnostics = _factorized_quantized_linear(
            activation,
            weight,
            activation_params,
            weight_params,
            accumulator_bits,
        )
    else:
        output, diagnostics = _mixed_scale_quantized_linear(
            activation,
            weight,
            activation_params,
            weight_params,
            accumulator_bits,
        )
    if not np.all(np.isfinite(output)):
        from .errors import NumericInput

        raise NumericInput(
            "quantized forward produced a non-finite output",
            layer=layer["id"],
            parameter="output",
        )
    if diagnostics["clamp_count"]:
        diagnostics["overflow_clamped"] = True
        diagnostics["overflow_clamp_count"] = int(diagnostics["clamp_count"])
    else:
        diagnostics["overflow_clamped"] = False
        diagnostics["overflow_clamp_count"] = 0
    layer_result = {
        "layer": layer["id"],
        "source_layer": layer["id"],
        "input_shape": list(activation.shape),
        "weight_shape": list(weight.shape),
        "output_shape": list(output.shape),
        "input_fingerprint": input_fingerprint,
        "input_sha256": array_fingerprint(activation),
        "output_sha256": array_fingerprint(output),
        "accumulator": diagnostics,
        "output": json_ready(output),
    }
    return layer_result


def forward_input_fingerprint(
    *,
    stage_fingerprint: str,
    calibration: dict[str, Any],
    activation: np.ndarray,
    accumulator_bits: int,
) -> str:
    return digest_obj(
        {
            "stage_fingerprint": stage_fingerprint,
            "layer": calibration["layer"],
            "calibration_input_fingerprint": calibration["input_fingerprint"],
            "activation_sha256": array_fingerprint(activation),
            "accumulator_bits": accumulator_bits,
        }
    )
