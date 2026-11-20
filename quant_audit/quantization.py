"""Affine quantization and dequantization primitives."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from .config import LayerQuantConfig
from .errors import E_QUANT_INVALID, AuditError


@dataclass(frozen=True)
class QuantizedTensor:
    scale: float
    zero_point: int
    qmin: int
    qmax: int
    quantized: np.ndarray
    dequantized: np.ndarray
    clamp_count: int
    clamp_positions: tuple[tuple[int, ...], ...]

    def audit_dict(self, layer: str, parameter: str) -> dict[str, Any]:
        return {
            "layer": layer,
            "parameter": parameter,
            "scale": self.scale,
            "zero_point": self.zero_point,
            "qmin": self.qmin,
            "qmax": self.qmax,
            "clamp_count": self.clamp_count,
            "clamp_positions": [list(position) for position in self.clamp_positions],
        }


def quantize_tensor(
    values: np.ndarray,
    layer_config: LayerQuantConfig,
    layer_name: str,
    parameter: str,
) -> QuantizedTensor:
    values = np.asarray(values, dtype=np.float64)
    if not np.all(np.isfinite(values)):
        raise AuditError(
            E_QUANT_INVALID,
            "tensor contains NaN or Inf",
            layer=layer_name,
            parameter=parameter,
        )

    bits = layer_config.bits
    qmax = (1 << bits) - 1
    qmin = -(1 << (bits - 1))
    if layer_config.mode == "symmetric":
        max_abs = float(np.max(np.abs(values)))
        scale = max_abs / float((1 << (bits - 1)) - 1)
        zero_point = 0
        qmin = -(1 << (bits - 1))
        qmax = (1 << (bits - 1)) - 1
    else:
        minimum = float(np.min(values))
        maximum = float(np.max(values))
        qmin, qmax = 0, (1 << bits) - 1
        if layer_config.zero_point == "zero":
            scale = max(abs(minimum), abs(maximum)) / float(qmax)
            zero_point = 0
        else:
            scale = (maximum - minimum) / float(qmax)
            if scale > 0.0:
                zero_point = int(np.rint(-minimum / scale))
            else:
                zero_point = 0
            zero_point = min(qmax, max(qmin, zero_point))

    if not np.isfinite(scale) or scale == 0.0:
        raise AuditError(
            E_QUANT_INVALID,
            "quantization scale is zero or non-finite",
            layer=layer_name,
            parameter=parameter,
            scale=float(scale),
        )

    raw = np.rint(values / scale) + float(zero_point)
    outside = (raw < qmin) | (raw > qmax)
    positions_array = np.argwhere(outside)
    clamp_count = int(positions_array.shape[0])
    clamp_positions = tuple(tuple(int(item) for item in row) for row in positions_array[:32])
    quantized = np.clip(raw, qmin, qmax).astype(np.float64)
    dequantized = np.ascontiguousarray((quantized - float(zero_point)) * scale)
    return QuantizedTensor(
        scale=float(scale),
        zero_point=int(zero_point),
        qmin=int(qmin),
        qmax=int(qmax),
        quantized=np.ascontiguousarray(quantized),
        dequantized=dequantized,
        clamp_count=clamp_count,
        clamp_positions=clamp_positions,
    )


def quantize_weight_and_bias(
    weight: np.ndarray,
    bias: np.ndarray,
    layer_config: LayerQuantConfig,
) -> tuple[QuantizedTensor, QuantizedTensor]:
    weight_q = quantize_tensor(weight, layer_config, layer_config.name, "weight")
    bias_q = quantize_tensor(bias, layer_config, layer_config.name, "bias")
    return weight_q, bias_q

