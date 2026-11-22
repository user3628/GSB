"""Deterministic CPU-only image editing operators.

All sampling and color arithmetic use explicit integer/fixed-point rules.
No operation depends on ambient RNG state, worker scheduling, or floating point
sampling order.
"""

from __future__ import annotations

import math
from typing import Any, Mapping, Sequence

import numpy as np

from .errors import invalid_param


_FIXED_BITS = 16
_FIXED_ONE = 1 << _FIXED_BITS
_MAX_RESIZE_SIDE = 8192
_MAX_RESIZE_PIXELS = 16_777_216


def _validate_image(image: Any) -> np.ndarray:
    array = np.asarray(image)
    if array.dtype != np.uint8 or array.ndim != 3 or array.shape[2] != 3:
        raise invalid_param("image", "Operator input must be an HxWx3 uint8 RGB array", shape=list(array.shape), dtype=str(array.dtype))
    if array.shape[0] <= 0 or array.shape[1] <= 0:
        raise invalid_param("image", "Operator input image must be non-empty", shape=list(array.shape))
    return np.ascontiguousarray(array, dtype=np.uint8)


def _int_value(operator: str, params: Mapping[str, Any], key: str) -> int:
    if key not in params:
        raise invalid_param(operator, f"Missing parameter {key!r}", parameter=key)
    value = params[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise invalid_param(operator, f"Parameter {key!r} must be an integer", parameter=key, value=value)
    return value


def _check_param_keys(operator: str, params: Mapping[str, Any], allowed: set[str]) -> None:
    unknown = sorted(set(params) - allowed)
    if unknown:
        raise invalid_param(operator, f"Unknown parameter(s): {', '.join(unknown)}", unknown_parameters=unknown)


def crop(image: np.ndarray, params: Mapping[str, Any]) -> np.ndarray:
    _check_param_keys("crop", params, {"x", "y", "w", "h"})
    source = _validate_image(image)
    x = _int_value("crop", params, "x")
    y = _int_value("crop", params, "y")
    width = _int_value("crop", params, "w")
    height = _int_value("crop", params, "h")
    source_height, source_width = source.shape[:2]
    if x < 0 or y < 0 or width <= 0 or height <= 0:
        raise invalid_param(
            "crop",
            "crop requires x,y >= 0 and w,h > 0",
            x=x,
            y=y,
            w=width,
            h=height,
        )
    if x + width > source_width or y + height > source_height:
        raise invalid_param(
            "crop",
            "crop rectangle exceeds image bounds",
            x=x,
            y=y,
            w=width,
            h=height,
            image_width=source_width,
            image_height=source_height,
        )
    return np.array(source[y : y + height, x : x + width, :], dtype=np.uint8, copy=True, order="C")


def _nearest_indices(source_length: int, target_length: int) -> np.ndarray:
    target = np.arange(target_length, dtype=np.int64)
    indices = ((2 * target + 1) * source_length) // (2 * target_length)
    return np.clip(indices, 0, source_length - 1)


def _bilinear_axis(source_length: int, target_length: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    target = np.arange(target_length, dtype=np.int64)
    # Pixel-center mapping in Q16 fixed point, with explicit floor and clamp.
    positions = ((2 * target + 1) * source_length - target_length) * _FIXED_ONE
    positions = positions // (2 * target_length)
    positions = np.clip(positions, 0, (source_length - 1) * _FIXED_ONE)
    lower = positions >> _FIXED_BITS
    fraction = positions & (_FIXED_ONE - 1)
    upper = np.minimum(lower + 1, source_length - 1)
    return lower, upper, fraction


def resize(image: np.ndarray, params: Mapping[str, Any]) -> np.ndarray:
    _check_param_keys("resize", params, {"w", "h", "mode"})
    source = _validate_image(image)
    width = _int_value("resize", params, "w")
    height = _int_value("resize", params, "h")
    mode = params.get("mode", "nearest")
    if width <= 0 or height <= 0:
        raise invalid_param("resize", "resize width and height must be positive", w=width, h=height)
    if width > _MAX_RESIZE_SIDE or height > _MAX_RESIZE_SIDE or width * height > _MAX_RESIZE_PIXELS:
        raise invalid_param(
            "resize",
            "resize output exceeds the kernel safety limit",
            w=width,
            h=height,
            max_side=_MAX_RESIZE_SIDE,
            max_pixels=_MAX_RESIZE_PIXELS,
        )
    if mode not in {"nearest", "bilinear"}:
        raise invalid_param("resize", "mode must be 'nearest' or 'bilinear'", mode=mode)
    source_height, source_width = source.shape[:2]
    if mode == "nearest":
        x_indices = _nearest_indices(source_width, width)
        y_indices = _nearest_indices(source_height, height)
        return np.ascontiguousarray(source[y_indices[:, None], x_indices[None, :], :], dtype=np.uint8)

    x0, x1, fx = _bilinear_axis(source_width, width)
    y0, y1, fy = _bilinear_axis(source_height, height)
    wx0 = (_FIXED_ONE - fx).astype(np.int64)
    wx1 = fx.astype(np.int64)
    wy0 = (_FIXED_ONE - fy).astype(np.int64)
    wy1 = fy.astype(np.int64)
    v00 = source[y0[:, None], x0[None, :], :].astype(np.int64)
    v01 = source[y0[:, None], x1[None, :], :].astype(np.int64)
    v10 = source[y1[:, None], x0[None, :], :].astype(np.int64)
    v11 = source[y1[:, None], x1[None, :], :].astype(np.int64)
    weights = (
        wy0[:, None, None] * wx0[None, :, None],
        wy0[:, None, None] * wx1[None, :, None],
        wy1[:, None, None] * wx0[None, :, None],
        wy1[:, None, None] * wx1[None, :, None],
    )
    accumulator = v00 * weights[0] + v01 * weights[1] + v10 * weights[2] + v11 * weights[3]
    rounded = (accumulator + (1 << (2 * _FIXED_BITS - 1))) >> (2 * _FIXED_BITS)
    return np.clip(rounded, 0, 255).astype(np.uint8)


def rotate(image: np.ndarray, params: Mapping[str, Any]) -> np.ndarray:
    _check_param_keys("rotate", params, {"angle"})
    source = _validate_image(image)
    angle = _int_value("rotate", params, "angle")
    if angle < -360 or angle > 360:
        raise invalid_param("rotate", "angle must be in [-360, 360]", angle=angle)
    if angle % 360 == 0:
        return np.array(source, dtype=np.uint8, copy=True, order="C")

    height, width = source.shape[:2]
    radians = math.radians(float(angle))
    cos_q = int(round(math.cos(radians) * _FIXED_ONE))
    sin_q = int(round(math.sin(radians) * _FIXED_ONE))
    center_x_q = (width - 1) * _FIXED_ONE // 2
    center_y_q = (height - 1) * _FIXED_ONE // 2
    dx = np.arange(width, dtype=np.int64) * _FIXED_ONE - center_x_q
    dy = np.arange(height, dtype=np.int64) * _FIXED_ONE - center_y_q
    relative_x = dx[None, :]
    relative_y = dy[:, None]
    source_x_q = (cos_q * relative_x + sin_q * relative_y) // _FIXED_ONE + center_x_q
    source_y_q = (-sin_q * relative_x + cos_q * relative_y) // _FIXED_ONE + center_y_q
    source_x = (source_x_q + _FIXED_ONE // 2) // _FIXED_ONE
    source_y = (source_y_q + _FIXED_ONE // 2) // _FIXED_ONE
    valid = (source_x >= 0) & (source_x < width) & (source_y >= 0) & (source_y < height)
    clipped_x = np.clip(source_x, 0, width - 1)
    clipped_y = np.clip(source_y, 0, height - 1)
    sampled = source[clipped_y, clipped_x, :]
    sampled[~valid, :] = 0
    return np.ascontiguousarray(sampled, dtype=np.uint8)


def color_shift(image: np.ndarray, params: Mapping[str, Any]) -> np.ndarray:
    _check_param_keys("color_shift", params, {"dr", "dg", "db"})
    source = _validate_image(image)
    deltas = [_int_value("color_shift", params, key) for key in ("dr", "dg", "db")]
    for key, delta in zip(("dr", "dg", "db"), deltas):
        if delta < -255 or delta > 255:
            raise invalid_param("color_shift", f"{key} must be in [-255, 255]", parameter=key, value=delta)
    shifted = source.astype(np.int16) + np.asarray(deltas, dtype=np.int16)[None, None, :]
    return np.clip(shifted, 0, 255).astype(np.uint8)


def _box_axis(pixels: np.ndarray, radius: int, axis: int) -> np.ndarray:
    if radius == 0:
        return pixels.astype(np.int64, copy=True)
    pad_width = [(0, 0), (0, 0), (0, 0)]
    pad_width[axis] = (radius, radius)
    padded = np.pad(pixels.astype(np.int64), pad_width, mode="edge")
    cumulative = np.cumsum(padded, axis=axis, dtype=np.int64)
    zeros_shape = list(cumulative.shape)
    zeros_shape[axis] = 1
    prefix = np.concatenate([np.zeros(zeros_shape, dtype=np.int64), cumulative], axis=axis)
    window = 2 * radius + 1
    source_length = pixels.shape[axis]
    upper_index = [slice(None), slice(None), slice(None)]
    lower_index = [slice(None), slice(None), slice(None)]
    upper_index[axis] = slice(window, window + source_length)
    lower_index[axis] = slice(0, source_length)
    sums = prefix[tuple(upper_index)] - prefix[tuple(lower_index)]
    return (sums + window // 2) // window


def blur(image: np.ndarray, params: Mapping[str, Any]) -> np.ndarray:
    _check_param_keys("blur", params, {"radius"})
    source = _validate_image(image)
    radius = _int_value("blur", params, "radius")
    if radius < 0 or radius > 32:
        raise invalid_param("blur", "radius must be in [0, 32]", radius=radius)
    if radius == 0:
        return np.array(source, dtype=np.uint8, copy=True, order="C")
    horizontal = _box_axis(source, radius, axis=1)
    vertical = _box_axis(horizontal, radius, axis=0)
    return np.clip(vertical, 0, 255).astype(np.uint8)


_KERNELS = {
    "crop": crop,
    "resize": resize,
    "rotate": rotate,
    "color_shift": color_shift,
    "blur": blur,
}


def apply_operator(image: np.ndarray, name: str, params: Mapping[str, Any], *, task_id: str | None = None) -> np.ndarray:
    kernel = _KERNELS.get(name)
    if kernel is None:
        raise invalid_param(name, f"Unsupported operator {name!r}", task_id=task_id)
    try:
        return kernel(image, params)
    except Exception as exc:
        from .errors import PipelineError

        if isinstance(exc, PipelineError):
            if exc.task_id is None:
                exc.task_id = task_id
            raise
        raise


def _ceil_div(numerator: int, denominator: int) -> int:
    return max(1, (numerator + denominator - 1) // denominator)


def operator_steps(name: str, params: Mapping[str, Any], input_size: tuple[int, int]) -> int:
    width, height = int(input_size[0]), int(input_size[1])
    area = width * height
    if name == "crop":
        return _ceil_div(int(params["w"]) * int(params["h"]), 64)
    if name == "resize":
        return _ceil_div(int(params["w"]) * int(params["h"]), 32)
    if name == "rotate":
        return _ceil_div(area, 32)
    if name == "color_shift":
        return _ceil_div(area, 64)
    if name == "blur":
        radius = int(params.get("radius", 0))
        return _ceil_div(area * (2 * radius + 1), 128)
    raise invalid_param(name, f"Unsupported operator {name!r}")


def estimate_chain_steps(operators: Sequence[Any], input_size: tuple[int, int]) -> tuple[int, list[dict[str, Any]]]:
    width, height = int(input_size[0]), int(input_size[1])
    total = 0
    chunks: list[dict[str, Any]] = []
    for index, operator in enumerate(operators, start=1):
        name = operator.name
        params = operator.params
        steps = operator_steps(name, params, (width, height))
        total += steps
        if name == "crop":
            width, height = int(params["w"]), int(params["h"])
        elif name == "resize":
            width, height = int(params["w"]), int(params["h"])
        chunks.append(
            {
                "index": index,
                "operator": name,
                "estimated_steps": steps,
                "output_size": [width, height],
            }
        )
    return total, chunks


def apply_chain(image: np.ndarray, operators: Sequence[Any], *, task_id: str | None = None) -> np.ndarray:
    result = _validate_image(image)
    for operator in operators:
        result = apply_operator(result, operator.name, operator.params, task_id=task_id)
    return result
