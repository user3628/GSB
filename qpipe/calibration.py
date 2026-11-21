from __future__ import annotations

from typing import Any

import numpy as np

from .config import load_shard_array, shard_path
from .errors import InvalidParam
from .util import array_fingerprint, digest_file, digest_obj, ensure_finite

QUANT_MODES = ("symmetric", "asymmetric")
QUANT_GRANULARITIES = ("per_tensor", "per_channel")


def _check_quant_settings(layer: dict[str, Any], role: str) -> dict[str, Any]:
    settings = layer[role]["quantization"]
    layer_id = layer["id"]
    if settings.get("bits") not in (4, 8):
        raise InvalidParam(
            "unsupported quantization bit width",
            layer=layer_id,
            parameter=f"{role}.quantization.bits",
            details={"bits": settings.get("bits")},
        )
    if settings.get("mode") not in QUANT_MODES:
        raise InvalidParam(
            "unsupported quantization mode",
            layer=layer_id,
            parameter=f"{role}.quantization.mode",
        )
    if settings.get("granularity") not in QUANT_GRANULARITIES:
        raise InvalidParam(
            "unsupported quantization granularity",
            layer=layer_id,
            parameter=f"{role}.quantization.granularity",
        )
    return settings


def _quant_bounds(bits: int, mode: str) -> tuple[int, int]:
    if mode == "symmetric":
        limit = (1 << (bits - 1)) - 1
        return -limit, limit
    return 0, (1 << bits) - 1


def _round_half_up(values: np.ndarray | float) -> np.ndarray | float:
    return np.floor(np.asarray(values) + 0.5)


def _channel_values(values: np.ndarray, axis: int) -> tuple[np.ndarray, tuple[int, ...]]:
    normalized_axis = axis % values.ndim
    moved = np.moveaxis(values, normalized_axis, 0)
    return moved.reshape(moved.shape[0], -1), moved.shape[1:]


def _compute_scale_zero_point(
    minimum: float,
    maximum: float,
    *,
    bits: int,
    mode: str,
    layer_id: str,
    parameter: str,
    channel: int | None = None,
) -> tuple[float, int, int, int]:
    qmin, qmax = _quant_bounds(bits, mode)
    min_value = float(minimum)
    max_value = float(maximum)
    if mode == "symmetric":
        magnitude = max(abs(min_value), abs(max_value))
        scale = magnitude / float(qmax)
        zero_point = 0
    else:
        value_range = max_value - min_value
        # Reserve one code for rounding, and also allow zero-point clamping to
        # consume an endpoint code for all-negative or all-positive channels.
        # The resulting interval always encloses both observed extrema.
        denominator = max((qmax - qmin) - 1, 1)
        scale = max(
            value_range / float(denominator),
            abs(min_value) / float(qmax),
            abs(max_value) / float(qmax),
        )
        shift = int(np.ceil(-min_value / scale)) if scale > 0 else 0
        zero_point = int(np.clip(shift, qmin, qmax))
    if not np.isfinite(scale) or scale <= 0.0:
        details: dict[str, Any] = {
            "min": min_value,
            "max": max_value,
            "scale": scale,
        }
        if channel is not None:
            details["channel"] = channel
        raise InvalidParam(
            "quantization scale is zero or non-finite",
            layer=layer_id,
            parameter=f"{parameter}.scale",
            details=details,
        )
    return float(scale), zero_point, qmin, qmax


def compute_qparams(
    values: np.ndarray,
    *,
    layer_id: str,
    role: str,
    settings: dict[str, Any],
) -> dict[str, Any]:
    bits = int(settings["bits"])
    mode = str(settings["mode"])
    granularity = str(settings["granularity"])
    parameter = f"{role}.quantization"
    if granularity == "per_tensor":
        minimum = float(np.min(values))
        maximum = float(np.max(values))
        scale, zero_point, qmin, qmax = _compute_scale_zero_point(
            minimum,
            maximum,
            bits=bits,
            mode=mode,
            layer_id=layer_id,
            parameter=parameter,
        )
        scale_value: Any = scale
        zero_value: Any = zero_point
        minima: Any = minimum
        maxima: Any = maximum
        channel_count = 1
        channel_axis = None
    else:
        channel_axis = int(settings["channel_axis"])
        if not -values.ndim <= channel_axis < values.ndim:
            raise InvalidParam(
                "channel_axis is outside array dimensions",
                layer=layer_id,
                parameter=f"{parameter}.channel_axis",
            )
        channel_axis %= values.ndim
        rows, _ = _channel_values(values, channel_axis)
        minima_array = np.min(rows, axis=1)
        maxima_array = np.max(rows, axis=1)
        scales: list[float] = []
        zero_points: list[int] = []
        qmin = qmax = 0
        for channel, (minimum, maximum) in enumerate(
            zip(minima_array, maxima_array, strict=True)
        ):
            scale, zero_point, qmin, qmax = _compute_scale_zero_point(
                float(minimum),
                float(maximum),
                bits=bits,
                mode=mode,
                layer_id=layer_id,
                parameter=parameter,
                channel=channel,
            )
            scales.append(scale)
            zero_points.append(zero_point)
        scale_value = scales
        zero_value = zero_points
        minima = [float(item) for item in minima_array]
        maxima = [float(item) for item in maxima_array]
        channel_count = len(scales)

    quantized, saturation_count, observed_min, observed_max = quantize_array(
        values,
        scale=scale_value,
        zero_point=zero_value,
        bits=bits,
        mode=mode,
        granularity=granularity,
        channel_axis=channel_axis if granularity == "per_channel" else None,
    )
    if granularity == "per_tensor":
        representable_min = (qmin - int(zero_value)) * float(scale_value)
        representable_max = (qmax - int(zero_value)) * float(scale_value)
    else:
        representable_min = [
            (qmin - zero_point) * scale
            for scale, zero_point in zip(scale_value, zero_value, strict=True)
        ]
        representable_max = [
            (qmax - zero_point) * scale
            for scale, zero_point in zip(scale_value, zero_value, strict=True)
        ]
    return {
        "bits": bits,
        "mode": mode,
        "granularity": granularity,
        "channel_axis": channel_axis if granularity == "per_channel" else None,
        "channel_count": channel_count,
        "qmin": int(qmin),
        "qmax": int(qmax),
        "scale": scale_value,
        "zero_point": zero_value,
        "observed_min": minima,
        "observed_max": maxima,
        "representable_min": representable_min,
        "representable_max": representable_max,
        "saturation_count": int(saturation_count),
        "quantized_min": int(observed_min),
        "quantized_max": int(observed_max),
    }


def _broadcast_params(
    values: np.ndarray,
    *,
    scale: Any,
    zero_point: Any,
    granularity: str,
    channel_axis: int | None,
) -> tuple[np.ndarray, np.ndarray]:
    if granularity == "per_tensor":
        return (
            np.asarray(scale, dtype=np.float64),
            np.asarray(zero_point, dtype=np.int64),
        )
    axis = int(channel_axis)
    shape = [1] * values.ndim
    shape[axis] = values.shape[axis]
    return (
        np.asarray(scale, dtype=np.float64).reshape(shape),
        np.asarray(zero_point, dtype=np.int64).reshape(shape),
    )


def quantize_array(
    values: np.ndarray,
    *,
    scale: Any,
    zero_point: Any,
    bits: int,
    mode: str,
    granularity: str,
    channel_axis: int | None,
) -> tuple[np.ndarray, int, int, int]:
    qmin, qmax = _quant_bounds(bits, mode)
    scale_array, zero_array = _broadcast_params(
        values,
        scale=scale,
        zero_point=zero_point,
        granularity=granularity,
        channel_axis=channel_axis,
    )
    if np.any(scale_array <= 0) or not np.all(np.isfinite(scale_array)):
        raise InvalidParam("quantization scale is invalid", parameter="scale")
    raw = _round_half_up(values / scale_array) + zero_array
    clipped = np.clip(raw, qmin, qmax)
    saturation_count = int(np.count_nonzero(raw != clipped))
    quantized = clipped.astype(np.int64)
    return quantized, saturation_count, int(np.min(quantized)), int(np.max(quantized))


def dequantize_array(
    quantized: np.ndarray,
    *,
    scale: Any,
    zero_point: Any,
    granularity: str,
    channel_axis: int | None,
) -> np.ndarray:
    scale_array, zero_array = _broadcast_params(
        quantized,
        scale=scale,
        zero_point=zero_point,
        granularity=granularity,
        channel_axis=channel_axis,
    )
    return (quantized.astype(np.float64) - zero_array) * scale_array


def prepare_layer_inputs(config: dict[str, Any]) -> list[dict[str, Any]]:
    layers = config["manifest"]["layers"]
    declarations = config["shapes"]["layers"]
    prepared: list[dict[str, Any]] = []
    previous_output: tuple[int, ...] | None = None
    for layer in layers:
        layer_id = layer["id"]
        _check_quant_settings(layer, "weight")
        _check_quant_settings(layer, "activation")
        accumulator_bits = int(layer["accumulator_bits"])
        if accumulator_bits < 2 or accumulator_bits > 62:
            raise InvalidParam(
                "accumulator_bits must be in [2, 62]",
                layer=layer_id,
                parameter="accumulator_bits",
            )
        weight = load_shard_array(
            config,
            layer["weight"]["shard"],
            layer["weight"]["key"],
            role="weight",
        )
        activation = load_shard_array(
            config,
            layer["activation"]["shard"],
            layer["activation"]["key"],
            role="activation",
        )
        ensure_finite(weight, layer=layer_id, parameter="weight")
        ensure_finite(activation, layer=layer_id, parameter="activation")
        if weight.ndim != 2 or activation.ndim != 2:
            raise InvalidParam(
                "linear layer shards must be rank-2 arrays",
                layer=layer_id,
                parameter="shape",
                details={
                    "weight_shape": list(weight.shape),
                    "activation_shape": list(activation.shape),
                },
            )
        declaration = declarations.get(layer_id, {})
        expected_weight_shape = declaration.get("weight_shape")
        expected_input_shape = declaration.get("input_shape")
        expected_output_shape = declaration.get("output_shape")
        actual_output_shape = [activation.shape[0], weight.shape[0]]
        if list(weight.shape) != expected_weight_shape:
            raise InvalidParam(
                "weight shape does not match declaration",
                layer=layer_id,
                parameter="shape.weight",
                details={
                    "actual": list(weight.shape),
                    "declared": expected_weight_shape,
                },
            )
        if list(activation.shape) != expected_input_shape:
            raise InvalidParam(
                "activation shape does not match declaration",
                layer=layer_id,
                parameter="shape.input",
                details={
                    "actual": list(activation.shape),
                    "declared": expected_input_shape,
                },
            )
        if actual_output_shape != expected_output_shape:
            raise InvalidParam(
                "declared output shape is inconsistent with operands",
                layer=layer_id,
                parameter="shape.output",
                details={
                    "actual": actual_output_shape,
                    "declared": expected_output_shape,
                },
            )
        if weight.shape[1] != activation.shape[1]:
            raise InvalidParam(
                "linear layer contraction dimensions differ",
                layer=layer_id,
                parameter="shape",
                details={
                    "weight_shape": list(weight.shape),
                    "activation_shape": list(activation.shape),
                },
            )
        if previous_output is not None and tuple(expected_input_shape) != previous_output:
            raise InvalidParam(
                "operator chain shape mismatch",
                layer=layer_id,
                parameter="shape.input",
                details={
                    "previous_output": list(previous_output),
                    "declared_input": expected_input_shape,
                },
            )
        previous_output = tuple(actual_output_shape)
        prepared.append(
            {
                "layer": layer,
                "layer_id": layer_id,
                "weight": weight,
                "activation": activation,
                "weight_fingerprint": array_fingerprint(weight),
                "activation_fingerprint": array_fingerprint(activation),
                "weight_file_digest": digest_file(
                    shard_path(config, layer["weight"]["shard"])
                ),
                "activation_file_digest": digest_file(
                    shard_path(config, layer["activation"]["shard"])
                ),
            }
        )
    return prepared


def compute_layer_calibration(
    prepared: dict[str, Any], *, config: dict[str, Any], stage_fingerprint: str
) -> dict[str, Any]:
    layer = prepared["layer"]
    layer_id = prepared["layer_id"]
    weight_params = compute_qparams(
        prepared["weight"],
        layer_id=layer_id,
        role="weight",
        settings=layer["weight"]["quantization"],
    )
    activation_params = compute_qparams(
        prepared["activation"],
        layer_id=layer_id,
        role="activation",
        settings=layer["activation"]["quantization"],
    )
    input_fingerprint = digest_obj(
        {
            "stage_fingerprint": stage_fingerprint,
            "layer": layer_id,
            "weight_file": prepared["weight_file_digest"],
            "activation_file": prepared["activation_file_digest"],
            "weight_quantization": layer["weight"]["quantization"],
            "activation_quantization": layer["activation"]["quantization"],
        }
    )
    return {
        "layer": layer_id,
        "source_layer": layer_id,
        "input_fingerprint": input_fingerprint,
        "weight": weight_params,
        "activation": activation_params,
        "rounding": "round_half_up_then_clip",
    }
