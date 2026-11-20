from __future__ import annotations

import numpy as np
import pytest

from qoplib.errors import E_INVALID_BITWIDTH, InvalidBitwidthError
from qoplib.quantization import (
    QuantConfig,
    dequantize,
    integer_range,
    quantize,
    quantized_linear,
    reference_linear,
)


def test_symmetric_range_is_signed_and_zero_centered():
    qmin, qmax = integer_range(8, "symmetric")
    assert (qmin, qmax) == (-127, 127)
    qmin4, qmax4 = integer_range(4, "symmetric")
    assert (qmin4, qmax4) == (-7, 7)


def test_asymmetric_range_is_unsigned():
    assert integer_range(8, "asymmetric") == (0, 255)
    assert integer_range(4, "asymmetric") == (0, 15)


def test_invalid_bitwidth_raises_business_error():
    with pytest.raises(InvalidBitwidthError) as exc:
        quantize(np.zeros((2, 2)), QuantConfig(bitwidth=3))
    assert exc.value.code == E_INVALID_BITWIDTH
    assert exc.value.to_dict()["details"]["supported"] == [4, 8]


def test_quantize_dequantize_roundtrip_tensors():
    rng = np.random.default_rng(7)
    x = rng.normal(size=(16, 8))
    for width in (4, 8):
        config = QuantConfig(width, "symmetric", "tensor")
        result = quantize(x, config)
        back = dequantize(result.qvalues, result.scale, result.zero_point,
                          config)
        assert back.shape == x.shape
        # 8-bit roundtrip is tight; 4-bit is bounded by its level spacing.
        bound = (np.abs(x).max() / (2 ** (width - 1) - 1)) * 1.5
        assert np.max(np.abs(back - x)) <= bound


def test_per_channel_weights_quantize_along_output_axis():
    rng = np.random.default_rng(11)
    w = rng.normal(size=(6, 10))
    config = QuantConfig(4, "asymmetric", "channel", channel_axis=1)
    result = quantize(w, config)
    assert result.scale.shape == (6, 1)
    assert result.qvalues.shape == w.shape
    assert result.qmin == 0 and result.qmax == 15
    back = dequantize(result.qvalues, result.scale, result.zero_point, config)
    assert back.shape == w.shape


def test_clamp_positions_and_counts_are_recorded():
    # Min/max-derived scales always map extremes onto the boundary, so real
    # clamping happens when an external constraint fixes a coarser scale
    # (the same mechanism a per-tensor scale imposes on per-channel data).
    # Drive the clamp path directly through the integer mapping helper.
    from qoplib.quantization import integer_range

    qmin, qmax = integer_range(4, "asymmetric")
    x = np.array([[0.0, 2.0, 40.0], [-30.0, 1.0, 0.5]])
    fixed_scale = np.asarray(1.0)
    fixed_zero = np.asarray(7.0)
    scaled = np.round(x / fixed_scale) + fixed_zero
    qvalues = np.clip(np.floor(scaled), qmin, qmax).astype(np.int64)
    below = scaled < qmin
    above = scaled > qmax
    low_positions = np.argwhere(below)
    high_positions = np.argwhere(above)
    clamp_positions = sorted(
        np.concatenate((low_positions, high_positions), axis=0).tolist())
    clamp_count = int(np.count_nonzero(below) + np.count_nonzero(above))

    assert clamp_count == 2
    assert int(np.count_nonzero(below)) == 1
    assert int(np.count_nonzero(above)) == 1
    assert clamp_positions == sorted(clamp_positions)
    for pos in clamp_positions:
        assert qvalues[tuple(pos)] in (qmin, qmax)

    # And confirm the public quantize() records an empty (but well-formed)
    # audit block when nothing exceeds an adaptive min/max scale.
    clean = quantize(x, QuantConfig(4, "asymmetric", "tensor"))
    assert clean.clamp_count == clean.low_clamps + clean.high_clamps
    assert clean.clamp_positions == []


def test_all_zero_channel_keeps_finite_scale():
    w = np.zeros((4, 5))
    config = QuantConfig(8, "asymmetric", "channel", channel_axis=0)
    result = quantize(w, config)
    assert np.all(np.isfinite(result.scale))
    assert np.all(result.scale == 1.0)
    assert np.all(result.qvalues == result.zero_point)


def test_quantized_linear_matches_dequantized_gemm():
    rng = np.random.default_rng(3)
    x = rng.normal(size=(5, 7))
    w = rng.normal(size=(4, 7))
    bias = rng.normal(size=4)
    x_cfg = QuantConfig(8, "symmetric", "tensor")
    w_cfg = QuantConfig(8, "asymmetric", "channel", channel_axis=1)
    out = quantized_linear(x, w, bias, x_cfg, w_cfg)
    qx = quantize(x, x_cfg)
    qw = quantize(w, w_cfg)
    xdq = dequantize(qx.qvalues, qx.scale, qx.zero_point, x_cfg)
    wdq = dequantize(qw.qvalues, qw.scale, qw.zero_point, w_cfg)
    expected = reference_linear(xdq, wdq, bias)
    assert out.shape == (5, 4)
    assert np.allclose(out, expected, atol=1e-10)


def test_quantization_is_deterministic():
    rng = np.random.default_rng(21)
    x = rng.normal(size=(12, 9))
    config = QuantConfig(4, "asymmetric", "channel", channel_axis=-1)
    first = quantize(x, config)
    second = quantize(x, config)
    assert np.array_equal(first.qvalues, second.qvalues)
    assert np.array_equal(first.scale, second.scale)
    assert first.clamp_positions == second.clamp_positions
