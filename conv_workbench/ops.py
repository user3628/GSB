"""Deterministic NumPy convolution and interpolation operators."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

import numpy as np

from .canonical import object_fingerprint, sha256_bytes, canonical_bytes
from .errors import InvalidStageError


_CONV2D_NAMES = {"conv2d", "conv_2d", "convolution2d", "convolution_2d"}
_CONV1D_NAMES = {"conv1d", "conv_1d", "convolution1d", "convolution_1d"}
_INTERPOLATE_NAMES = {"interpolate", "resize", "interpolation"}
_INTERPOLATE_MODES = {"nearest", "bilinear", "bicubic"}


@dataclass
class OperatorSpec:
    name: str
    params: dict[str, Any]
    weights: np.ndarray | None = None
    bias: np.ndarray | None = None
    operator_index: int = 0
    shard_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def fingerprint(self) -> str:
        return object_fingerprint({"name": self.name, "params": self.params})

    @property
    def summary(self) -> dict[str, Any]:
        result = {"name": self.name, **self.params}
        if self.weights is not None:
            result["weights_shape"] = list(self.weights.shape)
        return result


@dataclass(frozen=True)
class ExecOptions:
    block_h: int = 2
    block_w: int = 3
    out_channel_block: int = 2

    def validate(self) -> None:
        if min(self.block_h, self.block_w, self.out_channel_block) < 1:
            raise ValueError("all block dimensions must be positive")


def _stage_error(spec: OperatorSpec, reason: str, **detail: Any) -> InvalidStageError:
    return InvalidStageError(
        f"operator {spec.operator_index} ({spec.name or 'unknown'}) is invalid: {reason}",
        shard_id=spec.shard_id,
        operator_index=spec.operator_index,
        operator_name=spec.name,
        reason=reason,
        **detail,
    )


def _as_int(value: Any, name: str, spec: OperatorSpec) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, np.integer)):
        raise _stage_error(spec, f"parameter {name!r} must be an integer")
    return int(value)


def _as_int_pair(value: Any, name: str, spec: OperatorSpec) -> tuple[int, int]:
    if isinstance(value, (int, np.integer)) and not isinstance(value, bool):
        pair = (int(value), int(value))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)) and len(value) == 2:
        pair = tuple(_as_int(item, name, spec) for item in value)
    else:
        raise _stage_error(spec, f"parameter {name!r} must be an integer or a pair of integers")
    if min(pair) < 1:
        raise _stage_error(spec, f"parameter {name!r} must be positive")
    return pair  # type: ignore[return-value]


def _as_float(value: Any, name: str, spec: OperatorSpec) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, np.integer, np.floating)):
        raise _stage_error(spec, f"parameter {name!r} must be a finite number")
    result = float(value)
    if not math.isfinite(result):
        raise _stage_error(spec, f"parameter {name!r} must be finite")
    return result


def _as_scale(value: Any, spec: OperatorSpec) -> list[float]:
    if isinstance(value, (int, float, np.integer, np.floating)) and not isinstance(value, bool):
        values = [float(value), float(value)]
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)) and len(value) == 2:
        values = [_as_float(item, "scale", spec) for item in value]
    else:
        raise _stage_error(spec, "parameter 'scale' must be a number or a pair of numbers")
    for item in values:
        if not (1.0 / 64.0 <= item <= 64.0):
            raise _stage_error(
                spec,
                "scale is out of bounds; each axis must be in [1/64, 64]",
                scale=values,
                allowed_range=[1.0 / 64.0, 64.0],
            )
    return values


def _as_array(value: Any, name: str, spec: OperatorSpec) -> np.ndarray:
    try:
        array = np.asarray(value, dtype=np.float64)
    except (TypeError, ValueError) as exc:
        raise _stage_error(spec, f"parameter {name!r} must be a numeric array") from exc
    if not np.all(np.isfinite(array)):
        raise _stage_error(spec, f"parameter {name!r} contains NaN or infinity")
    return array


def _array_digest(array: np.ndarray) -> str:
    payload = {
        "dtype": "float64",
        "shape": list(array.shape),
        "values": array.tolist(),
    }
    return sha256_bytes(canonical_bytes(payload))


def _extract_params(raw: dict[str, Any], spec: OperatorSpec) -> dict[str, Any]:
    if "params" in raw:
        params = raw["params"]
        if not isinstance(params, dict):
            raise _stage_error(spec, "field 'params' must be an object")
        return dict(params)
    return {key: value for key, value in raw.items() if key not in {"name", "op", "index"}}


def normalize_operator(raw: Any, shard_id: str, operator_index: int) -> OperatorSpec:
    """Validate and normalize one manifest operator.

    Normalization is side-effect free.  The engine calls this for the complete
    chain before executing anything, preventing partial execution of an invalid
    chain.
    """

    if not isinstance(raw, dict):
        spec = OperatorSpec("", {}, operator_index=operator_index, shard_id=shard_id)
        raise _stage_error(spec, "operator description must be an object")

    names = [value for value in (raw.get("name"), raw.get("op")) if value is not None]
    if not names or not isinstance(names[0], str):
        spec = OperatorSpec("", {}, operator_index=operator_index, shard_id=shard_id)
        raise _stage_error(spec, "missing string field 'name'")
    if len(set(str(item).lower() for item in names)) != 1:
        spec = OperatorSpec(str(names[0]).lower(), {}, operator_index=operator_index, shard_id=shard_id)
        raise _stage_error(spec, "fields 'name' and 'op' disagree")

    name = str(names[0]).strip().lower()
    spec = OperatorSpec(name, {}, operator_index=operator_index, shard_id=shard_id)
    params = _extract_params(raw, spec)

    if name in _CONV2D_NAMES:
        spec.name = "conv2d"
        return _normalize_conv2d(spec, params)
    if name in _CONV1D_NAMES:
        spec.name = "conv1d"
        return _normalize_conv1d(spec, params)
    if name in _INTERPOLATE_NAMES:
        return _normalize_interpolate(spec, params)
    raise _stage_error(spec, f"unknown operator name {name!r}")


def _normalize_conv2d(spec: OperatorSpec, params: dict[str, Any]) -> OperatorSpec:
    if "weights" not in params:
        raise _stage_error(spec, "missing required parameter 'weights'")
    weights = _as_array(params["weights"], "weights", spec)
    if weights.ndim != 4:
        raise _stage_error(spec, "conv2d weights must have shape [kh, kw, cin, cout]")
    kh, kw, cin, cout = weights.shape
    if min(weights.shape) < 1:
        raise _stage_error(spec, "conv2d weights may not contain an empty dimension")

    kernel_shape = params.get("kernel_shape")
    if kernel_shape is not None and list(_as_int_pair(kernel_shape, "kernel_shape", spec)) != [kh, kw]:
        raise _stage_error(spec, "kernel_shape does not match weights", kernel_shape=kernel_shape, weights_shape=[kh, kw, cin, cout])
    stride = _as_int_pair(params.get("stride", 1), "stride", spec)
    dilation = _as_int_pair(params.get("dilation", 1), "dilation", spec)
    padding = params.get("padding")
    if not isinstance(padding, str) or padding.lower() not in {"same", "valid"}:
        raise _stage_error(spec, "parameter 'padding' must be 'same' or 'valid'")
    padding = padding.lower()

    if params.get("bias") is None:
        bias = np.zeros((cout,), dtype=np.float64)
    else:
        bias = _as_array(params["bias"], "bias", spec)
        if bias.shape != (cout,):
            raise _stage_error(spec, f"bias must have shape [{cout}]")

    spec.params = {
        "kernel_shape": [kh, kw],
        "in_channels": int(cin),
        "out_channels": int(cout),
        "stride": list(stride),
        "dilation": list(dilation),
        "padding": padding,
        "weights_sha256": _array_digest(weights),
        "bias": bias.tolist(),
    }
    spec.weights = np.ascontiguousarray(weights)
    spec.bias = np.ascontiguousarray(bias)
    return spec


def _normalize_conv1d(spec: OperatorSpec, params: dict[str, Any]) -> OperatorSpec:
    if "weights" not in params:
        raise _stage_error(spec, "missing required parameter 'weights'")
    weights = _as_array(params["weights"], "weights", spec)
    if weights.ndim != 3:
        raise _stage_error(spec, "conv1d weights must have shape [k, cin, cout]")
    kernel_size, cin, cout = weights.shape
    if min(weights.shape) < 1:
        raise _stage_error(spec, "conv1d weights may not contain an empty dimension")

    declared_kernel = params.get("kernel_size")
    if declared_kernel is not None and _as_int(declared_kernel, "kernel_size", spec) != kernel_size:
        raise _stage_error(spec, "kernel_size does not match weights")
    stride = _as_int(params.get("stride", 1), "stride", spec)
    dilation = _as_int(params.get("dilation", 1), "dilation", spec)
    if stride < 1 or dilation < 1:
        raise _stage_error(spec, "stride and dilation must be positive")
    padding = params.get("padding")
    if not isinstance(padding, str) or padding.lower() not in {"same", "valid"}:
        raise _stage_error(spec, "parameter 'padding' must be 'same' or 'valid'")
    padding = padding.lower()

    if params.get("bias") is None:
        bias = np.zeros((cout,), dtype=np.float64)
    else:
        bias = _as_array(params["bias"], "bias", spec)
        if bias.shape != (cout,):
            raise _stage_error(spec, f"bias must have shape [{cout}]")

    spec.params = {
        "kernel_size": int(kernel_size),
        "in_channels": int(cin),
        "out_channels": int(cout),
        "stride": int(stride),
        "dilation": int(dilation),
        "padding": padding,
        "weights_sha256": _array_digest(weights),
        "bias": bias.tolist(),
    }
    spec.weights = np.ascontiguousarray(weights)
    spec.bias = np.ascontiguousarray(bias)
    return spec


def _normalize_interpolate(spec: OperatorSpec, params: dict[str, Any]) -> OperatorSpec:
    mode = params.get("mode")
    if not isinstance(mode, str) or mode.lower() not in _INTERPOLATE_MODES:
        raise _stage_error(spec, "parameter 'mode' must be nearest, bilinear, or bicubic")
    mode = mode.lower()
    if "scale" not in params:
        raise _stage_error(spec, "missing required parameter 'scale'")
    scale = _as_scale(params["scale"], spec)

    if "align_corners" in params:
        align = params["align_corners"]
        if not isinstance(align, bool):
            raise _stage_error(spec, "parameter 'align_corners' must be a boolean")
        if "align_mode" in params:
            mode_align = _align_from_string(params["align_mode"], spec)
            if mode_align != align:
                raise _stage_error(spec, "align_corners and align_mode disagree")
    elif "align_mode" in params:
        align = _align_from_string(params["align_mode"], spec)
    else:
        raise _stage_error(spec, "missing required parameter 'align_corners' (or 'align_mode')")

    spec.params = {
        "mode": mode,
        "scale": scale,
        "align_corners": bool(align),
        "align_mode": "align_corners" if align else "half_pixel",
    }
    return spec


def _align_from_string(value: Any, spec: OperatorSpec) -> bool:
    if not isinstance(value, str) or value.lower() not in {"half_pixel", "align_corners"}:
        raise _stage_error(spec, "align_mode must be 'half_pixel' or 'align_corners'")
    return value.lower() == "align_corners"


def infer_output_shape(spec: OperatorSpec, input_shape: Iterable[int]) -> tuple[int, ...]:
    shape = tuple(int(item) for item in input_shape)
    if len(shape) != 4:
        raise _stage_error(spec, "input must be an NHWC rank-4 tensor", input_shape=list(shape))
    if min(shape) < 1:
        raise _stage_error(spec, "NHWC dimensions must all be positive", input_shape=list(shape))
    n, h, w, channels = shape

    if spec.name == "conv2d":
        assert spec.weights is not None
        kh, kw, cin, _cout = spec.weights.shape
        if channels != cin:
            raise _stage_error(
                spec,
                "input channel count does not match convolution weights",
                input_channels=channels,
                weight_in_channels=int(cin),
            )
        sh, sw = spec.params["stride"]
        dh, dw = spec.params["dilation"]
        try:
            out_h = _conv_dimension(h, kh, sh, dh, spec.params["padding"])
            out_w = _conv_dimension(w, kw, sw, dw, spec.params["padding"])
        except ValueError as exc:
            raise _stage_error(spec, str(exc), input_shape=list(shape), kernel_shape=[kh, kw]) from exc
        return (n, out_h, out_w, int(spec.weights.shape[3]))

    if spec.name == "conv1d":
        assert spec.weights is not None
        kernel, cin, cout = spec.weights.shape
        if channels != cin:
            raise _stage_error(
                spec,
                "input channel count does not match convolution weights",
                input_channels=channels,
                weight_in_channels=int(cin),
            )
        try:
            out_w = _conv_dimension(w, kernel, spec.params["stride"], spec.params["dilation"], spec.params["padding"])
        except ValueError as exc:
            raise _stage_error(spec, str(exc), input_shape=list(shape), kernel_size=int(kernel)) from exc
        return (n, h, out_w, int(cout))

    if spec.name == "interpolate":
        out_h = math.floor(h * spec.params["scale"][0])
        out_w = math.floor(w * spec.params["scale"][1])
        if out_h < 1 or out_w < 1:
            raise _stage_error(
                spec,
                "scale produces an empty spatial output",
                input_shape=list(shape),
                output_shape=[out_h, out_w],
            )
        return (n, out_h, out_w, channels)

    raise _stage_error(spec, f"unsupported normalized operator {spec.name!r}")


def _conv_dimension(size: int, kernel: int, stride: int, dilation: int, padding: str) -> int:
    effective = (kernel - 1) * dilation + 1
    if padding == "same":
        return int(math.ceil(size / stride))
    output = (size - effective) // stride + 1
    if output < 1:
        raise ValueError("valid convolution produces an empty dimension")
    return int(output)


def _same_padding(size: int, kernel: int, stride: int, dilation: int) -> tuple[int, int]:
    output = int(math.ceil(size / stride))
    effective = (kernel - 1) * dilation + 1
    total = max((output - 1) * stride + effective - size, 0)
    before = total // 2
    return before, total - before


def _padding_for(size: int, kernel: int, stride: int, dilation: int, mode: str) -> tuple[int, int, int]:
    if mode == "same":
        before, after = _same_padding(size, kernel, stride, dilation)
        return _conv_dimension(size, kernel, stride, dilation, mode), before, after
    return _conv_dimension(size, kernel, stride, dilation, mode), 0, 0


def execute_operator(
    spec: OperatorSpec,
    tensor: np.ndarray,
    options: ExecOptions | None = None,
) -> tuple[np.ndarray, dict[str, Any]]:
    options = options or ExecOptions()
    options.validate()
    array = np.asarray(tensor, dtype=np.float64)
    infer_output_shape(spec, array.shape)
    if spec.name == "conv2d":
        return conv2d_nhwc(array, spec, options)
    if spec.name == "conv1d":
        return conv1d_nhwc(array, spec, options)
    if spec.name == "interpolate":
        return interpolate_nhwc(array, spec)
    raise _stage_error(spec, "unsupported operator")


def conv2d_nhwc(
    tensor: np.ndarray,
    spec: OperatorSpec,
    options: ExecOptions | None = None,
) -> tuple[np.ndarray, dict[str, Any]]:
    options = options or ExecOptions()
    options.validate()
    assert spec.weights is not None and spec.bias is not None
    weights = spec.weights
    bias = spec.bias
    kh, kw, _cin, cout = weights.shape
    sh, sw = spec.params["stride"]
    dh, dw = spec.params["dilation"]
    mode = spec.params["padding"]
    out_shape = infer_output_shape(spec, tensor.shape)
    _, out_h, out_w, _ = out_shape
    _, top, bottom = _padding_for(tensor.shape[1], kh, sh, dh, mode)
    _, left, right = _padding_for(tensor.shape[2], kw, sw, dw, mode)
    padded = np.pad(tensor, ((0, 0), (top, bottom), (left, right), (0, 0)), mode="constant")
    output = np.empty(out_shape, dtype=np.float64)
    blocks: list[dict[str, Any]] = []

    for oh0 in range(0, out_h, options.block_h):
        bh = min(options.block_h, out_h - oh0)
        rows = oh0 * sh + np.arange(bh, dtype=np.int64) * sh
        for ow0 in range(0, out_w, options.block_w):
            bw = min(options.block_w, out_w - ow0)
            cols = ow0 * sw + np.arange(bw, dtype=np.int64) * sw
            for oc0 in range(0, cout, options.out_channel_block):
                bc = min(options.out_channel_block, cout - oc0)
                acc = np.broadcast_to(bias[oc0 : oc0 + bc], (tensor.shape[0], bh, bw, bc)).copy()
                for rk in range(kh):
                    current_rows = rows + rk * dh
                    for ck in range(kw):
                        current_cols = cols + ck * dw
                        patch = padded[:, current_rows[:, None], current_cols[None, :], :]
                        for ci in range(weights.shape[2]):
                            acc += patch[..., ci, None] * weights[rk, ck, ci, oc0 : oc0 + bc]
                output[:, oh0 : oh0 + bh, ow0 : ow0 + bw, oc0 : oc0 + bc] = acc
                blocks.append(
                    {
                        "output_rows": [int(oh0), int(oh0 + bh)],
                        "output_cols": [int(ow0), int(ow0 + bw)],
                        "output_channels": [int(oc0), int(oc0 + bc)],
                    }
                )

    return output, {
        "operator": "conv2d",
        "layout": "NHWC",
        "padding": {"top": top, "bottom": bottom, "left": left, "right": right},
        "blocks": blocks,
        "block_count": len(blocks),
    }


def conv1d_nhwc(
    tensor: np.ndarray,
    spec: OperatorSpec,
    options: ExecOptions | None = None,
) -> tuple[np.ndarray, dict[str, Any]]:
    options = options or ExecOptions()
    options.validate()
    assert spec.weights is not None and spec.bias is not None
    weights = spec.weights
    bias = spec.bias
    kernel, _cin, cout = weights.shape
    stride = spec.params["stride"]
    dilation = spec.params["dilation"]
    mode = spec.params["padding"]
    out_shape = infer_output_shape(spec, tensor.shape)
    _, _, out_w, _ = out_shape
    _, left, right = _padding_for(tensor.shape[2], kernel, stride, dilation, mode)
    padded = np.pad(tensor, ((0, 0), (0, 0), (left, right), (0, 0)), mode="constant")
    batch_rows = tensor.shape[0] * tensor.shape[1]
    flat = padded.reshape(batch_rows, padded.shape[2], padded.shape[3])
    flat_output = np.empty((batch_rows, out_w, cout), dtype=np.float64)
    blocks: list[dict[str, Any]] = []

    for ow0 in range(0, out_w, options.block_w):
        bw = min(options.block_w, out_w - ow0)
        cols = ow0 * stride + np.arange(bw, dtype=np.int64) * stride
        for oc0 in range(0, cout, options.out_channel_block):
            bc = min(options.out_channel_block, cout - oc0)
            acc = np.broadcast_to(bias[oc0 : oc0 + bc], (batch_rows, bw, bc)).copy()
            for k in range(kernel):
                current_cols = cols + k * dilation
                patch = flat[:, current_cols, :]
                for ci in range(weights.shape[1]):
                    acc += patch[..., ci, None] * weights[k, ci, oc0 : oc0 + bc]
            flat_output[:, ow0 : ow0 + bw, oc0 : oc0 + bc] = acc
            blocks.append(
                {
                    "output_rows": [0, int(tensor.shape[1])],
                    "output_cols": [int(ow0), int(ow0 + bw)],
                    "output_channels": [int(oc0), int(oc0 + bc)],
                }
            )

    output = flat_output.reshape(tensor.shape[0], tensor.shape[1], out_w, cout)
    return output, {
        "operator": "conv1d",
        "layout": "NHWC",
        "axis": "W",
        "padding": {"left": left, "right": right},
        "blocks": blocks,
        "block_count": len(blocks),
    }


def _coordinate_grid(size_in: int, size_out: int, align_corners: bool) -> np.ndarray:
    indices = np.arange(size_out, dtype=np.float64)
    if align_corners:
        if size_out > 1:
            coordinates = indices * (size_in - 1) / (size_out - 1)
        else:
            coordinates = np.zeros(1, dtype=np.float64)
    else:
        effective_scale = size_in / size_out
        coordinates = (indices + 0.5) * effective_scale - 0.5
    return np.clip(coordinates, 0.0, float(size_in - 1))


def _cubic_weight(distance: np.ndarray, a: float = -0.75) -> np.ndarray:
    x = np.abs(distance)
    result = np.zeros_like(x, dtype=np.float64)
    inside_one = x <= 1.0
    between = (x > 1.0) & (x < 2.0)
    result[inside_one] = (a + 2.0) * x[inside_one] ** 3 - (a + 3.0) * x[inside_one] ** 2 + 1.0
    result[between] = (
        a * x[between] ** 3
        - 5.0 * a * x[between] ** 2
        + 8.0 * a * x[between]
        - 4.0 * a
    )
    return result


def interpolate_nhwc(tensor: np.ndarray, spec: OperatorSpec) -> tuple[np.ndarray, dict[str, Any]]:
    if spec.name != "interpolate":
        raise _stage_error(spec, "expected interpolation operator")
    out_shape = infer_output_shape(spec, tensor.shape)
    _, out_h, out_w, _ = out_shape
    in_h, in_w = tensor.shape[1], tensor.shape[2]
    align = bool(spec.params["align_corners"])
    mode = spec.params["mode"]
    y = _coordinate_grid(in_h, out_h, align)
    x = _coordinate_grid(in_w, out_w, align)

    if mode == "nearest":
        yi = np.clip(np.floor(y + 0.5).astype(np.int64), 0, in_h - 1)
        xi = np.clip(np.floor(x + 0.5).astype(np.int64), 0, in_w - 1)
        output = tensor[:, yi[:, None], xi[None, :], :]
    elif mode == "bilinear":
        y0 = np.floor(y).astype(np.int64)
        x0 = np.floor(x).astype(np.int64)
        y1 = np.clip(y0 + 1, 0, in_h - 1)
        x1 = np.clip(x0 + 1, 0, in_w - 1)
        y0 = np.clip(y0, 0, in_h - 1)
        x0 = np.clip(x0, 0, in_w - 1)
        wy = (y - y0)[:, None]
        wx = (x - x0)[None, :]
        top_left = tensor[:, y0[:, None], x0[None, :], :]
        top_right = tensor[:, y0[:, None], x1[None, :], :]
        bottom_left = tensor[:, y1[:, None], x0[None, :], :]
        bottom_right = tensor[:, y1[:, None], x1[None, :], :]
        weights = wy * wx
        output = (
            top_left * ((1.0 - wy) * (1.0 - wx))[:, :, None]
            + top_right * ((1.0 - wy) * wx)[:, :, None]
            + bottom_left * (wy * (1.0 - wx))[:, :, None]
            + bottom_right * weights[:, :, None]
        )
    elif mode == "bicubic":
        y0 = np.floor(y).astype(np.int64)
        x0 = np.floor(x).astype(np.int64)
        y_offsets = np.arange(-1, 3, dtype=np.int64)
        x_offsets = np.arange(-1, 3, dtype=np.int64)
        wy = _cubic_weight(y[:, None] - (y0[:, None] + y_offsets[None, :]))
        wx = _cubic_weight(x[:, None] - (x0[:, None] + x_offsets[None, :]))
        output = np.zeros(out_shape, dtype=np.float64)
        for iy, delta_y in enumerate(y_offsets):
            yi = np.clip(y0 + delta_y, 0, in_h - 1)
            for ix, delta_x in enumerate(x_offsets):
                xi = np.clip(x0 + delta_x, 0, in_w - 1)
                weight = (wy[:, iy][:, None] * wx[:, ix][None, :])[:, :, None]
                output += tensor[:, yi[:, None], xi[None, :], :] * weight
    else:  # pragma: no cover - normalization prevents this
        raise _stage_error(spec, f"unsupported interpolation mode {mode!r}")

    output = np.ascontiguousarray(output, dtype=np.float64)
    return output, {
        "operator": "interpolate",
        "mode": mode,
        "align_mode": spec.params["align_mode"],
        "scale": list(spec.params["scale"]),
        "effective_scale": [in_h / out_h, in_w / out_w],
        "blocks": [
            {
                "output_rows": [0, int(out_h)],
                "output_cols": [0, int(out_w)],
                "output_channels": [0, int(tensor.shape[3])],
            }
        ],
        "block_count": 1,
        "coordinates": {
            "y": [float(y[0]), float(y[-1])],
            "x": [float(x[0]), float(x[-1])],
            "clipped_to": {"y": [0.0, float(in_h - 1)], "x": [0.0, float(in_w - 1)]},
        },
    }

