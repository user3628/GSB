"""Low-precision quantize / dequantize and quantized-domain linear ops.

Supports 4-bit and 8-bit integers, symmetric and asymmetric modes and
per-tensor / per-channel granularity.  Every public function is deterministic:
the same array and configuration always produce identical outputs and audit
statistics.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import numpy as np

from .errors import InvalidBitwidthError

SUPPORTED_BITWIDTHS = (4, 8)
VALID_GRANULARITIES = ("tensor", "channel")
VALID_MODES = ("symmetric", "asymmetric")


@dataclass(frozen=True)
class QuantConfig:
    bitwidth: int
    mode: str = "symmetric"
    granularity: str = "tensor"
    channel_axis: int = -1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "bitwidth": self.bitwidth,
            "mode": self.mode,
            "granularity": self.granularity,
            "channel_axis": self.channel_axis,
        }


@dataclass
class QuantResult:
    qvalues: np.ndarray
    scale: np.ndarray
    zero_point: np.ndarray
    qmin: int
    qmax: int
    clamp_count: int
    clamp_positions: List[List[int]]
    low_clamps: int
    high_clamps: int

    def to_audit(self) -> Dict[str, Any]:
        return {
            "qmin": self.qmin,
            "qmax": self.qmax,
            "clamp_count": int(self.clamp_count),
            "low_clamps": int(self.low_clamps),
            "high_clamps": int(self.high_clamps),
            "clamp_positions": self.clamp_positions,
            "scale_fingerprint_len": int(np.asarray(self.scale).size),
        }


def validate_config(config: QuantConfig) -> None:
    if config.bitwidth not in SUPPORTED_BITWIDTHS:
        raise InvalidBitwidthError(
            f"unsupported bitwidth {config.bitwidth!r}; expected one of "
            f"{SUPPORTED_BITWIDTHS}",
            details={"bitwidth": config.bitwidth,
                     "supported": list(SUPPORTED_BITWIDTHS)},
        )
    if config.mode not in VALID_MODES:
        raise ValueError(f"unsupported quantization mode {config.mode!r}")
    if config.granularity not in VALID_GRANULARITIES:
        raise ValueError(f"unsupported granularity {config.granularity!r}")


def integer_range(bitwidth: int, mode: str = "asymmetric") -> Tuple[int, int]:
    if bitwidth not in SUPPORTED_BITWIDTHS:
        raise InvalidBitwidthError(
            f"unsupported bitwidth {bitwidth!r}; expected one of "
            f"{SUPPORTED_BITWIDTHS}",
            details={"bitwidth": bitwidth, "supported": list(SUPPORTED_BITWIDTHS)},
        )
    levels = 1 << bitwidth
    if mode == "symmetric":
        # Signed symmetric range with zero point fixed at zero.
        bound = levels // 2 - 1
        return -bound, bound
    return 0, levels - 1


def _channel_views(array: np.ndarray, axis: int) -> Tuple[np.ndarray, np.ndarray]:
    """Return (min, max) arrays per channel along ``axis`` (axis kept)."""

    axis = axis % array.ndim
    mins = np.amin(array, axis=axis, keepdims=True)
    maxs = np.amax(array, axis=axis, keepdims=True)
    return mins, maxs


def compute_qparams(array: np.ndarray, config: QuantConfig
                    ) -> Tuple[np.ndarray, np.ndarray, int, int]:
    """Compute scale and zero point for an array."""

    validate_config(config)
    qmin, qmax = integer_range(config.bitwidth, config.mode)
    if config.granularity == "tensor":
        x_min = np.asarray(float(np.min(array)))
        x_max = np.asarray(float(np.max(array)))
    else:
        x_min, x_max = _channel_views(array, config.channel_axis)

    if config.mode == "symmetric":
        # Signed symmetric range centred on zero.
        abs_max = np.maximum(np.abs(x_min), np.abs(x_max))
        scale = np.where(abs_max > 0.0, abs_max / qmax, 1.0)
        zero_point = np.zeros_like(scale)
    else:
        span = x_max - x_min
        scale = np.where(span > 0.0, span / (qmax - qmin), 1.0)
        zero_point = qmin - np.round(x_min / scale)
        zero_point = np.clip(zero_point, qmin, qmax)
    return np.asarray(scale, dtype=np.float64), \
        np.asarray(zero_point, dtype=np.float64), qmin, qmax


def _expand_for_broadcast(params: np.ndarray, shape: Tuple[int, ...],
                          axis: int, granularity: str = "tensor"
                          ) -> np.ndarray:
    params = np.asarray(params)
    if params.ndim == 0 or params.size == 1 or granularity == "tensor":
        return params.reshape((1,) * len(shape))
    # Per-channel parameters keep the reduced axis from np.amin(keepdims=True).
    if params.ndim == len(shape):
        return params
    axis = axis % len(shape)
    target_shape = [1] * len(shape)
    target_shape[axis] = params.size
    return params.reshape(target_shape)


def quantize(array: np.ndarray, config: QuantConfig) -> QuantResult:
    """Quantize a float array and record clamp statistics."""

    array = np.asarray(array, dtype=np.float64)
    validate_config(config)
    scale, zero_point, qmin, qmax = compute_qparams(array, config)
    scale_b = _expand_for_broadcast(scale, array.shape,
                                    config.channel_axis, config.granularity)
    zero_b = _expand_for_broadcast(zero_point, array.shape,
                                   config.channel_axis, config.granularity)

    scaled = np.round(array / scale_b) + zero_b
    qvalues = np.clip(np.floor(scaled), qmin, qmax).astype(np.int64)

    below = scaled < qmin
    above = scaled > qmax
    low_positions = np.argwhere(below)
    high_positions = np.argwhere(above)
    clamp_positions = np.concatenate(
        (low_positions, high_positions), axis=0
    ).tolist() if (low_positions.size or high_positions.size) else []
    clamp_positions.sort()

    return QuantResult(
        qvalues=qvalues,
        scale=scale,
        zero_point=zero_point,
        qmin=qmin,
        qmax=qmax,
        clamp_count=int(np.count_nonzero(below) + np.count_nonzero(above)),
        clamp_positions=[[int(v) for v in pos] for pos in clamp_positions],
        low_clamps=int(np.count_nonzero(below)),
        high_clamps=int(np.count_nonzero(above)),
    )


def dequantize(qvalues: np.ndarray, scale: np.ndarray,
               zero_point: np.ndarray, config: QuantConfig) -> np.ndarray:
    """Inverse of :func:`quantize`."""

    qvalues = np.asarray(qvalues, dtype=np.float64)
    scale = np.asarray(scale, dtype=np.float64)
    zero_point = np.asarray(zero_point, dtype=np.float64)
    if config.granularity == "tensor":
        return (qvalues - float(zero_point.reshape(-1)[0])) * \
            float(scale.reshape(-1)[0])
    if scale.ndim == qvalues.ndim:
        scale_b, zero_b = scale, zero_point
    else:
        axis = config.channel_axis % qvalues.ndim
        target_shape = [1] * qvalues.ndim
        target_shape[axis] = scale.size
        scale_b = scale.reshape(target_shape)
        zero_b = zero_point.reshape(target_shape)
    return (qvalues - zero_b) * scale_b


def reference_linear(x: np.ndarray, w: np.ndarray,
                     bias: np.ndarray | None = None) -> np.ndarray:
    """High precision float64 reference linear op: ``x @ w.T + bias``."""

    out = np.matmul(np.asarray(x, dtype=np.float64),
                    np.asarray(w, dtype=np.float64).T)
    if bias is not None:
        out = out + np.asarray(bias, dtype=np.float64)
    return out


def quantized_linear(x: np.ndarray, w: np.ndarray,
                     bias: np.ndarray | None,
                     x_config: QuantConfig,
                     w_config: QuantConfig) -> np.ndarray:
    """Linear op executed in the quantized domain.

    ``w`` is quantized per output channel (last axis) while ``x`` may be
    quantized per tensor or per feature channel.  The matrix multiplication
    accumulates integer values and the floating point scales are applied
    afterwards, so the output is correctly restored to float.
    """

    validate_config(x_config)
    validate_config(w_config)
    qx = quantize(x, x_config)
    qw = quantize(w, w_config)

    qi = qx.qvalues.astype(np.int64) - qx.zero_point.reshape(
        _broadcast_shape(qx.zero_point, x.shape, x_config)).astype(np.int64)
    qj = qw.qvalues.astype(np.int64) - qw.zero_point.reshape(
        _broadcast_shape(qw.zero_point, w.shape, w_config)).astype(np.int64)

    if x_config.granularity == "tensor":
        integer_product = np.matmul(qi.astype(np.float64),
                                    qj.T.astype(np.float64))
        sx = float(np.asarray(qx.scale).reshape(-1)[0])
        if w_config.granularity == "tensor":
            sw = float(np.asarray(qw.scale).reshape(-1)[0])
            out = integer_product * (sx * sw)
        else:
            # One scale per output feature: broadcast over the columns.
            sw = np.asarray(qw.scale, dtype=np.float64).reshape(1, -1)
            out = integer_product * (sx * sw)
    else:
        # Per-channel activation scale: apply each scale term-by-term so the
        # weighted sum is restored to float without assuming a uniform scale.
        sx_b = np.asarray(qx.scale, dtype=np.float64).reshape(
            _broadcast_shape(qx.scale, x.shape, x_config))
        sw_b = np.asarray(qw.scale, dtype=np.float64).reshape(
            _broadcast_shape(qw.scale, w.shape, w_config))
        out = np.matmul(qi.astype(np.float64) * sx_b,
                        (qj.astype(np.float64) * sw_b).T)
    if bias is not None:
        out = out + np.asarray(bias, dtype=np.float64)
    return out


def _broadcast_shape(params: np.ndarray, shape: Tuple[int, ...],
                     config: QuantConfig) -> Tuple[int, ...]:
    params = np.asarray(params)
    if params.ndim == 0 or params.size == 1 or config.granularity == "tensor":
        return (1,) * len(shape)
    if params.ndim == len(shape):
        return tuple(params.shape)
    axis = config.channel_axis % len(shape)
    target = [1] * len(shape)
    target[axis] = params.size
    return tuple(target)


def has_non_finite(array: np.ndarray) -> bool:
    return bool(np.any(~np.isfinite(np.asarray(array, dtype=np.float64))))


def non_finite_summary(array: np.ndarray) -> Dict[str, int]:
    array = np.asarray(array, dtype=np.float64)
    return {
        "nan_count": int(np.count_nonzero(np.isnan(array))),
        "pos_inf_count": int(np.count_nonzero(array == np.inf)),
        "neg_inf_count": int(np.count_nonzero(array == -np.inf)),
    }
