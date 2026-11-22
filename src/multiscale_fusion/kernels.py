from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np

from .errors import FusionError
from .manifest import LateralSpec, UpsampleSpec
from .util import sha256_array, stable_rng


def conv_output_hw(input_hw: tuple[int, int], kernel: tuple[int, int], stride: tuple[int, int], padding: str) -> tuple[int, int]:
    h, w = input_hw
    kh, kw = kernel
    sh, sw = stride
    if padding == "same":
        return (int(math.ceil(h / sh)), int(math.ceil(w / sw)))
    oh = (h - kh) // sh + 1
    ow = (w - kw) // sw + 1
    if oh <= 0 or ow <= 0:
        return (max(0, oh), max(0, ow))
    return (oh, ow)


def _same_padding(input_hw: tuple[int, int], kernel: tuple[int, int], stride: tuple[int, int]) -> tuple[int, int, int, int]:
    h, w = input_hw
    kh, kw = kernel
    sh, sw = stride
    out_h = int(math.ceil(h / sh))
    out_w = int(math.ceil(w / sw))
    pad_h = max((out_h - 1) * sh + kh - h, 0)
    pad_w = max((out_w - 1) * sw + kw - w, 0)
    top = pad_h // 2
    left = pad_w // 2
    return (top, pad_h - top, left, pad_w - left)


def build_conv_params(
    layer_name: str,
    in_channels: int,
    lateral: LateralSpec,
    task_seed: int,
    *,
    material_suffix: str = "",
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    """Build deterministic lateral weights from manifest parameters.

    Weight values are part of the stage fingerprint.  Keeping the weights
    generated rather than hidden in an opaque framework checkpoint makes the
    example fully local and byte-reproducible.
    """

    kh, kw = lateral.kernel_size
    out_channels = lateral.out_channels
    fan_in = max(1, in_channels * kh * kw)
    seed_material = "|".join(
        [
            str(task_seed),
            layer_name,
            str(lateral.kernel_seed if lateral.kernel_seed is not None else "auto"),
            str(in_channels),
            str(out_channels),
            f"{kh}x{kw}",
            str(lateral.stride),
            lateral.padding,
            material_suffix,
        ]
    )
    rng = stable_rng(seed_material)
    std = math.sqrt(2.0 / fan_in) * lateral.weight_scale
    weight = rng.standard_normal((out_channels, in_channels, kh, kw)).astype(np.float64) * std
    bias = rng.standard_normal((out_channels,)).astype(np.float64) * (0.01 * lateral.weight_scale)
    metadata = {
        "seed_material": seed_material,
        "weight_shape": list(weight.shape),
        "weight_sha256": sha256_array(weight),
        "bias_sha256": sha256_array(bias),
        "fan_in": fan_in,
        "weight_scale": lateral.weight_scale,
    }
    return weight, bias, metadata


def conv2d_nchw(
    x: np.ndarray,
    weight: np.ndarray,
    bias: np.ndarray,
    stride: tuple[int, int],
    padding: str,
    *,
    layer: str,
) -> tuple[np.ndarray, dict[str, Any]]:
    if x.ndim != 4:
        raise FusionError.shape_mismatch(layer, [0, 0, 0, 0], x.shape, stage="lateral_conv")
    if weight.ndim != 4:
        raise FusionError.invalid_param(layer, None, "lateral.weight", "weight must be 4-D")
    n, c, h, w = x.shape
    out_c, weight_c, kh, kw = weight.shape
    if weight_c != c:
        raise FusionError.shape_mismatch(layer, [n, weight_c, h, w], x.shape, stage="lateral_weight_channels")
    if bias.shape != (out_c,):
        raise FusionError.shape_mismatch(layer, [out_c], bias.shape, stage="lateral_bias")
    sh, sw = stride
    oh, ow = conv_output_hw((h, w), (kh, kw), stride, padding)
    if oh <= 0 or ow <= 0:
        raise FusionError.shape_mismatch(layer, [n, out_c, 1, 1], [n, out_c, oh, ow], stage="lateral_conv_window")
    if padding == "same":
        pt, pb, pl, pr = _same_padding((h, w), (kh, kw), stride)
        padded = np.pad(x, ((0, 0), (0, 0), (pt, pb), (pl, pr)), mode="constant")
    elif padding == "valid":
        pt = pb = pl = pr = 0
        padded = x
    else:
        raise FusionError.invalid_param(layer, None, "lateral.padding", f"unsupported padding {padding!r}")

    out = np.empty((n, out_c, oh, ow), dtype=np.float64)
    # Explicit output-position loops deliberately include the final (possibly
    # partially padded) row/column and every odd channel.  This is the tail
    # handling required for odd NCHW shapes and avoids implicit cropping.
    for ni in range(n):
        for oi in range(out_c):
            kernel_plane = weight[oi]
            bias_value = float(bias[oi])
            for y in range(oh):
                y0 = y * sh
                for xidx in range(ow):
                    x0 = xidx * sw
                    patch = padded[ni, :, y0 : y0 + kh, x0 : x0 + kw]
                    value = float(np.sum(patch * kernel_plane, axis=(0, 1, 2), dtype=np.float64)) + bias_value
                    out[ni, oi, y, xidx] = value
    if not np.all(np.isfinite(out)):
        count = int(np.count_nonzero(~np.isfinite(out)))
        raise FusionError.numeric(layer, "non_finite_conv_output", non_finite_count=count)
    meta = {
        "input_shape": list(x.shape),
        "output_shape": list(out.shape),
        "kernel_size": [kh, kw],
        "stride": [sh, sw],
        "padding": padding,
        "padding_tb_lr": [pt, pb, pl, pr],
        "odd_input_channels": bool(c % 2),
        "odd_input_height": bool(h % 2),
        "odd_input_width": bool(w % 2),
        "last_window_rows": [min((oh - 1) * sh, h - 1), min((oh - 1) * sh + kh - 1, h - 1)],
        "last_window_cols": [min((ow - 1) * sw, w - 1), min((ow - 1) * sw + kw - 1, w - 1)],
        "tail_padded_rows": int(max(0, (oh - 1) * sh + kh - h)),
        "tail_padded_cols": int(max(0, (ow - 1) * sw + kw - w)),
    }
    return out, meta


def resize_nchw(
    x: np.ndarray,
    target_hw: tuple[int, int],
    spec: UpsampleSpec,
    *,
    layer: str,
) -> tuple[np.ndarray, dict[str, Any]]:
    if x.ndim != 4:
        raise FusionError.shape_mismatch(layer, [0, 0, 0, 0], x.shape, stage="upsample")
    n, c, src_h, src_w = x.shape
    target_h, target_w = int(target_hw[0]), int(target_hw[1])
    base_h = src_h * spec.scale
    base_w = src_w * spec.scale
    tail_h = target_h - base_h
    tail_w = target_w - base_w
    if abs(tail_h) > spec.max_tail or abs(tail_w) > spec.max_tail:
        raise FusionError.shape_mismatch(
            layer,
            [n, c, target_h, target_w],
            [n, c, base_h, base_w],
            stage="upsample_alignment",
            message=(
                f"layer {layer!r} top-down upsample would produce {[n, c, base_h, base_w]}, "
                f"cannot align to required {[n, c, target_h, target_w]} with max_tail={spec.max_tail}"
            ),
        )
    if spec.mode == "nearest":
        out = np.empty((n, c, target_h, target_w), dtype=np.float64)
        src_rows = np.minimum(np.arange(target_h) // spec.scale, src_h - 1)
        src_cols = np.minimum(np.arange(target_w) // spec.scale, src_w - 1)
        for row in range(target_h):
            sy = int(src_rows[row])
            for col in range(target_w):
                sx = int(src_cols[col])
                out[:, :, row, col] = x[:, :, sy, sx]
    elif spec.mode == "bilinear":
        out = np.empty((n, c, target_h, target_w), dtype=np.float64)
        if spec.align_corners:
            if target_h == 1:
                rows = np.zeros(1, dtype=np.float64)
            else:
                rows = np.arange(target_h, dtype=np.float64) * (src_h - 1) / max(target_h - 1, 1)
            if target_w == 1:
                cols = np.zeros(1, dtype=np.float64)
            else:
                cols = np.arange(target_w, dtype=np.float64) * (src_w - 1) / max(target_w - 1, 1)
        else:
            rows = (np.arange(target_h, dtype=np.float64) + 0.5) / spec.scale - 0.5
            cols = (np.arange(target_w, dtype=np.float64) + 0.5) / spec.scale - 0.5
            rows = np.clip(rows, 0.0, max(src_h - 1, 0))
            cols = np.clip(cols, 0.0, max(src_w - 1, 0))
        y0 = np.floor(rows).astype(np.int64)
        x0 = np.floor(cols).astype(np.int64)
        y1 = np.minimum(y0 + 1, src_h - 1)
        x1 = np.minimum(x0 + 1, src_w - 1)
        wy = (rows - y0)[None, None, :, None]
        wx = (cols - x0)[None, None, None, :]
        for row in range(target_h):
            y0i = int(y0[row])
            y1i = int(y1[row])
            top_left = x[:, :, y0i, :][:, :, x0]
            top_right = x[:, :, y0i, :][:, :, x1]
            bottom_left = x[:, :, y1i, :][:, :, x0]
            bottom_right = x[:, :, y1i, :][:, :, x1]
            value = (
                top_left * (1.0 - wx)
                + top_right * wx
                + (bottom_left * (1.0 - wx) + bottom_right * wx - top_left * (1.0 - wx) - top_right * wx)
                * wy[:, :, row, :]
            )
            out[:, :, row, :] = value
    else:
        raise FusionError.invalid_param(layer, None, "upsample.mode", f"unsupported mode {spec.mode!r}")
    if not np.all(np.isfinite(out)):
        count = int(np.count_nonzero(~np.isfinite(out)))
        raise FusionError.numeric(layer, "non_finite_upsample_output", non_finite_count=count)
    meta = {
        "source_shape": list(x.shape),
        "target_shape": list(out.shape),
        "scale": spec.scale,
        "mode": spec.mode,
        "align_corners": spec.align_corners,
        "base_shape": [n, c, base_h, base_w],
        "tail_rows": int(tail_h),
        "tail_cols": int(tail_w),
        "tail_elements": int(max(0, tail_h) * target_w + max(0, tail_w) * target_h - max(0, tail_h) * max(0, tail_w)),
        "edge_replicated": bool(tail_h > 0 or tail_w > 0),
        "cropped": bool(tail_h < 0 or tail_w < 0),
    }
    return out, meta
