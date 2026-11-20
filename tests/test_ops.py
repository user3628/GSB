from __future__ import annotations

import math

import numpy as np
import pytest

from conv_workbench.canonical import npy_bytes
from conv_workbench.ops import (
    ExecOptions,
    execute_operator,
    normalize_operator,
)


def _conv_dim(size: int, kernel: int, stride: int, dilation: int, padding: str) -> int:
    effective = (kernel - 1) * dilation + 1
    if padding == "same":
        return math.ceil(size / stride)
    return (size - effective) // stride + 1


def _pad_same(size: int, kernel: int, stride: int, dilation: int) -> tuple[int, int]:
    out = math.ceil(size / stride)
    effective = (kernel - 1) * dilation + 1
    total = max((out - 1) * stride + effective - size, 0)
    return total // 2, total - total // 2


def _naive_conv2d(x, w, b, stride, dilation, padding):
    _, h, width, _ = x.shape
    kh, kw, _, cout = w.shape
    sh, sw = stride
    dh, dw = dilation
    oh = _conv_dim(h, kh, sh, dh, padding)
    ow = _conv_dim(width, kw, sw, dw, padding)
    if padding == "same":
        top, bottom = _pad_same(h, kh, sh, dh)
        left, right = _pad_same(width, kw, sw, dw)
    else:
        top = bottom = left = right = 0
    padded = np.pad(x, ((0, 0), (top, bottom), (left, right), (0, 0)))
    out = np.empty((x.shape[0], oh, ow, cout), dtype=np.float64)
    for n in range(x.shape[0]):
        for iy in range(oh):
            for ix in range(ow):
                for oc in range(cout):
                    total = float(b[oc])
                    for ky in range(kh):
                        for kx in range(kw):
                            py = iy * sh + ky * dh
                            px = ix * sw + kx * dw
                            total += float(np.sum(padded[n, py, px, :] * w[ky, kx, :, oc]))
                    out[n, iy, ix, oc] = total
    return out


def _naive_conv1d(x, w, b, stride, dilation, padding):
    _, h, width, _ = x.shape
    kernel, _, cout = w.shape
    ow = _conv_dim(width, kernel, stride, dilation, padding)
    if padding == "same":
        left, right = _pad_same(width, kernel, stride, dilation)
    else:
        left = right = 0
    padded = np.pad(x, ((0, 0), (0, 0), (left, right), (0, 0)))
    out = np.empty((x.shape[0], h, ow, cout), dtype=np.float64)
    for n in range(x.shape[0]):
        for row in range(h):
            for ix in range(ow):
                for oc in range(cout):
                    total = float(b[oc])
                    for k in range(kernel):
                        px = ix * stride + k * dilation
                        total += float(np.sum(padded[n, row, px, :] * w[k, :, oc]))
                    out[n, row, ix, oc] = total
    return out


@pytest.mark.parametrize(
    "stride,padding,dilation",
    [
        ([1, 1], "same", [1, 1]),
        ([2, 1], "same", [2, 1]),
        ([1, 2], "valid", [1, 2]),
        ([2, 2], "valid", [1, 1]),
    ],
)
def test_conv2d_matches_naive_and_is_block_invariant(stride, padding, dilation):
    rng = np.random.Generator(np.random.PCG64(1234))
    x = rng.normal(size=(2, 8, 9, 3))
    w = rng.normal(size=(3, 2, 3, 5))
    b = rng.normal(size=(5,))
    raw = {
        "name": "conv2d",
        "params": {
            "weights": w.tolist(),
            "bias": b.tolist(),
            "stride": stride,
            "padding": padding,
            "dilation": dilation,
        },
    }
    spec = normalize_operator(raw, "conv-case", 0)
    exact, info = execute_operator(spec, x, ExecOptions(block_h=2, block_w=3, out_channel_block=2))
    tiny, _ = execute_operator(spec, x, ExecOptions(block_h=1, block_w=1, out_channel_block=1))
    expected = _naive_conv2d(x, w, b, stride, dilation, padding)
    assert exact.shape == expected.shape
    np.testing.assert_allclose(exact, expected, rtol=1e-14, atol=1e-14)
    assert npy_bytes(exact) == npy_bytes(tiny)
    assert info["block_count"] >= 1
    assert all("output_rows" in block for block in info["blocks"])


@pytest.mark.parametrize(
    "stride,padding,dilation",
    [(1, "same", 1), (2, "same", 2), (1, "valid", 2), (3, "valid", 1)],
)
def test_conv1d_matches_naive_and_uses_nhwc_width_axis(stride, padding, dilation):
    rng = np.random.Generator(np.random.PCG64(5678))
    x = rng.normal(size=(2, 4, 11, 3))
    w = rng.normal(size=(4, 3, 5))
    b = rng.normal(size=(5,))
    spec = normalize_operator(
        {
            "name": "conv1d",
            "params": {
                "weights": w.tolist(),
                "bias": b.tolist(),
                "stride": stride,
                "padding": padding,
                "dilation": dilation,
            },
        },
        "conv1d-case",
        0,
    )
    exact, _ = execute_operator(spec, x, ExecOptions(block_w=2, out_channel_block=2))
    tiny, _ = execute_operator(spec, x, ExecOptions(block_w=1, out_channel_block=1))
    expected = _naive_conv1d(x, w, b, stride, dilation, padding)
    np.testing.assert_allclose(exact, expected, rtol=1e-14, atol=1e-14)
    assert exact.shape[1] == x.shape[1]
    assert npy_bytes(exact) == npy_bytes(tiny)


def _coordinate(index: int, size_in: int, size_out: int, align_corners: bool) -> float:
    if align_corners:
        source = 0.0 if size_out == 1 else index * (size_in - 1) / (size_out - 1)
    else:
        source = (index + 0.5) * (size_in / size_out) - 0.5
    return min(max(source, 0.0), float(size_in - 1))


def _cubic(distance: float) -> float:
    a = -0.75
    x = abs(distance)
    if x <= 1.0:
        return (a + 2.0) * x**3 - (a + 3.0) * x**2 + 1.0
    if x < 2.0:
        return a * x**3 - 5.0 * a * x**2 + 8.0 * a * x - 4.0 * a
    return 0.0


def _scalar_interpolate(x: np.ndarray, mode: str, scale: tuple[float, float], align: bool) -> np.ndarray:
    n, h, w, c = x.shape
    oh = math.floor(h * scale[0])
    ow = math.floor(w * scale[1])
    out = np.empty((n, oh, ow, c), dtype=np.float64)
    for oy in range(oh):
        sy = _coordinate(oy, h, oh, align)
        y0 = math.floor(sy)
        for ox in range(ow):
            sx = _coordinate(ox, w, ow, align)
            x0 = math.floor(sx)
            for batch in range(n):
                for channel in range(c):
                    if mode == "nearest":
                        iy = min(max(math.floor(sy + 0.5), 0), h - 1)
                        ix = min(max(math.floor(sx + 0.5), 0), w - 1)
                        value = x[batch, iy, ix, channel]
                    elif mode == "bilinear":
                        fy, fx = sy - y0, sx - x0
                        y1, x1 = min(y0 + 1, h - 1), min(x0 + 1, w - 1)
                        value = (
                            x[batch, y0, x0, channel] * (1 - fy) * (1 - fx)
                            + x[batch, y0, x1, channel] * (1 - fy) * fx
                            + x[batch, y1, x0, channel] * fy * (1 - fx)
                            + x[batch, y1, x1, channel] * fy * fx
                        )
                    else:
                        value = 0.0
                        for dy in range(-1, 3):
                            iy = min(max(y0 + dy, 0), h - 1)
                            wy = _cubic(sy - (y0 + dy))
                            for dx in range(-1, 3):
                                ix = min(max(x0 + dx, 0), w - 1)
                                wx = _cubic(sx - (x0 + dx))
                                value += x[batch, iy, ix, channel] * wy * wx
                    out[batch, oy, ox, channel] = value
    return out


@pytest.mark.parametrize("mode", ["nearest", "bilinear", "bicubic"])
@pytest.mark.parametrize("align", [False, True])
@pytest.mark.parametrize("scale", [(1.75, 0.5), (0.6, 2.25)])
def test_interpolation_matches_scalar_reference(mode, align, scale):
    rng = np.random.Generator(np.random.PCG64(991))
    x = rng.normal(size=(1, 7, 6, 3))
    spec = normalize_operator(
        {
            "name": "interpolate",
            "params": {"mode": mode, "scale": list(scale), "align_corners": align},
        },
        "resize-case",
        0,
    )
    actual, info = execute_operator(spec, x)
    expected = _scalar_interpolate(x, mode, scale, align)
    np.testing.assert_allclose(actual, expected, rtol=1e-13, atol=1e-13)
    assert actual.shape == expected.shape
    assert info["align_mode"] == ("align_corners" if align else "half_pixel")
    assert npy_bytes(actual) == npy_bytes(execute_operator(spec, x)[0])


def test_nearest_boundary_coordinates_clip_instead_of_wrapping():
    x = np.arange(9, dtype=np.float64).reshape(1, 3, 3, 1)
    spec = normalize_operator(
        {
            "name": "interpolate",
            "params": {"mode": "nearest", "scale": [2.0, 2.0], "align_corners": False},
        },
        "clip-case",
        0,
    )
    output, info = execute_operator(spec, x)
    assert output[0, 0, 0, 0] == x[0, 0, 0, 0]
    assert output[0, -1, -1, 0] == x[0, -1, -1, 0]
    assert output[0, 0, -1, 0] == x[0, 0, -1, 0]
    assert info["coordinates"]["clipped_to"] == {"y": [0.0, 2.0], "x": [0.0, 2.0]}

