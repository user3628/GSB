from __future__ import annotations

import numpy as np
import pytest

from quant_audit.config import LayerQuantConfig
from quant_audit.errors import E_QUANT_INVALID, AuditError
from quant_audit.quantization import quantize_tensor


def test_symmetric_quantization_stays_in_range() -> None:
    values = np.linspace(-1.25, 1.75, num=64, dtype=np.float64).reshape(8, 8)
    config = LayerQuantConfig(name="layer_x", bits=8, mode="symmetric", zero_point="exact")
    result = quantize_tensor(values, config, "layer_x", "weight")

    assert result.zero_point == 0
    assert result.scale > 0.0
    assert result.qmin == -128
    assert result.qmax == 127
    assert result.quantized.min() >= result.qmin
    assert result.quantized.max() <= result.qmax
    assert result.clamp_count == 0
    assert np.allclose(result.dequantized, result.quantized * result.scale)


def test_asymmetric_quantization_computes_zero_point() -> None:
    values = np.array([[-2.0, -1.0], [0.5, 3.0]], dtype=np.float64)
    config = LayerQuantConfig(name="layer_x", bits=4, mode="asymmetric", zero_point="exact")
    result = quantize_tensor(values, config, "layer_x", "weight")

    assert result.qmin == 0
    assert result.qmax == 15
    assert 0 <= result.zero_point <= 15
    assert result.clamp_count == 0
    assert result.quantized.min() >= 0
    assert result.quantized.max() <= 15


def test_forced_zero_point_records_clamp_count_and_position() -> None:
    values = np.array([-2.0, 1.0], dtype=np.float64)
    config = LayerQuantConfig(name="layer_clamp", bits=4, mode="asymmetric", zero_point="zero")
    result = quantize_tensor(values, config, "layer_clamp", "weight")

    assert result.zero_point == 0
    assert result.clamp_count == 1
    assert result.clamp_positions == ((0,),)
    assert result.quantized[0] == 0


def test_zero_scale_returns_quant_invalid_with_layer() -> None:
    config = LayerQuantConfig(name="layer_zero", bits=8, mode="symmetric", zero_point="exact")
    with pytest.raises(AuditError) as caught:
        quantize_tensor(np.zeros((2, 2)), config, "layer_zero", "weight")
    assert caught.value.code == E_QUANT_INVALID
    assert caught.value.details["layer"] == "layer_zero"
    assert caught.value.details["parameter"] == "weight"


def test_nan_returns_quant_invalid_with_layer() -> None:
    config = LayerQuantConfig(name="layer_nan", bits=8, mode="symmetric", zero_point="exact")
    values = np.array([[1.0, np.nan]], dtype=np.float64)
    with pytest.raises(AuditError) as caught:
        quantize_tensor(values, config, "layer_nan", "bias")
    assert caught.value.code == E_QUANT_INVALID
    assert caught.value.details["layer"] == "layer_nan"

