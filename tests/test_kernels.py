from __future__ import annotations

import numpy as np
import pytest

from multiscale_fusion.errors import FusionError
from multiscale_fusion.kernels import build_conv_params, conv2d_nchw, resize_nchw
from multiscale_fusion.manifest import LateralSpec, UpsampleSpec


def test_conv2d_odd_channels_and_asymmetric_stride() -> None:
    rng = np.random.default_rng(7)
    x = rng.standard_normal((1, 3, 5, 7))
    weight = rng.standard_normal((4, 3, 3, 3))
    bias = rng.standard_normal((4,))
    out, meta = conv2d_nchw(x, weight, bias, (2, 2), "same", layer="odd")
    assert out.shape == (1, 4, 3, 4)
    assert meta["odd_input_channels"] is True
    assert meta["odd_input_height"] is True
    assert meta["odd_input_width"] is True
    assert meta["tail_padded_rows"] >= 0
    assert meta["tail_padded_cols"] >= 0
    # Directly recompute one output position, including the final odd channel.
    pt = meta["padding_tb_lr"][0]
    pl = meta["padding_tb_lr"][2]
    padded = np.pad(x, ((0, 0), (0, 0), (pt, meta["padding_tb_lr"][1]), (pl, meta["padding_tb_lr"][3])))
    expected = np.sum(padded[0, :, 0:3, 0:3] * weight[0], axis=(0, 1, 2)) + bias[0]
    assert out[0, 0, 0, 0] == pytest.approx(expected, rel=0, abs=1e-14)
    # Tail channel is read explicitly: zeroing channel 2 changes output.
    x2 = x.copy()
    x2[:, 2, :, :] = 0
    out2, _ = conv2d_nchw(x2, weight, bias, (2, 2), "same", layer="odd")
    assert not np.array_equal(out2, out)


def test_nearest_upsample_handles_odd_tail_and_crop() -> None:
    x = np.arange(1 * 2 * 3 * 5, dtype=np.float64).reshape(1, 2, 3, 5)
    spec = UpsampleSpec(scale=2, mode="nearest", max_tail=1)
    out, meta = resize_nchw(x, (7, 9), spec, layer="tail")
    assert out.shape == (1, 2, 7, 9)
    assert meta["tail_rows"] == 1
    assert meta["tail_cols"] == -1
    assert meta["edge_replicated"] is True
    assert meta["cropped"] is True
    expanded = np.repeat(np.repeat(x, 2, axis=2), 2, axis=3)
    expected = np.concatenate([expanded[:, :, :6, :9], expanded[:, :, 5:6, :9]], axis=2)
    assert np.array_equal(out, expected)
    assert np.array_equal(out[:, :, 6, :], expanded[:, :, 5, :9])


def test_bilinear_upsample_shape_and_finiteness() -> None:
    rng = np.random.default_rng(3)
    x = rng.standard_normal((1, 3, 4, 5))
    spec = UpsampleSpec(scale=2, mode="bilinear", align_corners=False, max_tail=1)
    out, meta = resize_nchw(x, (9, 11), spec, layer="bi")
    assert out.shape == (1, 3, 9, 11)
    assert np.all(np.isfinite(out))
    assert meta["tail_rows"] == 1
    assert meta["tail_cols"] == 1


def test_upsample_alignment_mismatch_reports_shapes() -> None:
    x = np.zeros((1, 2, 3, 5), dtype=np.float64)
    with pytest.raises(FusionError) as excinfo:
        resize_nchw(x, (10, 10), UpsampleSpec(scale=2, mode="nearest", max_tail=1), layer="bad")
    error = excinfo.value
    assert error.code == "E_SHAPE_MISMATCH"
    assert error.layer == "bad"
    assert error.details["expected_shape"] == [1, 2, 10, 10]
    assert error.details["actual_shape"] == [1, 2, 6, 10]


def test_derived_conv_weights_are_deterministic() -> None:
    spec = LateralSpec(kernel_size=(3, 3), stride=(1, 1), out_channels=7, kernel_seed=9)
    w1, b1, m1 = build_conv_params("p3", 5, spec, 11)
    w2, b2, m2 = build_conv_params("p3", 5, spec, 11)
    assert np.array_equal(w1, w2)
    assert np.array_equal(b1, b2)
    assert m1 == m2
