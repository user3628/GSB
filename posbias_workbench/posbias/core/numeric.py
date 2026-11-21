"""Fixed-point quantized domain and log-domain arithmetic.

All position-bias artifacts are stored as fixed-point int32 arrays. Products and
ratios of base frequencies and scaling factors are evaluated in log domain so
that the pipeline never accumulates float drift. Every guard carries scheme and
bin identifiers and raises ``E_NUMERIC`` with diagnostic context.
"""

from __future__ import annotations

import math

import numpy as np

from ..errors import NumericError

INT32_MIN = np.iinfo(np.int32).min
INT32_MAX = np.iinfo(np.int32).max
HEADROOM = 1.5


def log_domain_product(*, base_freq, scale, stage, scheme, bin_id=None,
                       context=None):
    """Return ``base_freq * scale`` computed entirely in log domain."""
    ctx = {"base_freq": base_freq, "scale": scale}
    if context:
        ctx.update(context)
    if not np.isfinite(base_freq) or not np.isfinite(scale):
        raise NumericError(
            "non-finite operand in log-domain product",
            stage=stage, scheme=scheme, bin_id=bin_id, context=ctx)
    if base_freq == 0.0:
        raise NumericError(
            "zero base frequency", stage=stage, scheme=scheme,
            bin_id=bin_id, context=ctx)
    if scale == 0.0:
        raise NumericError(
            "zero scaling coefficient", stage=stage, scheme=scheme,
            bin_id=bin_id, context=ctx)
    if base_freq < 0.0 or scale < 0.0:
        raise NumericError(
            "negative operand in log-domain product", stage=stage,
            scheme=scheme, bin_id=bin_id, context=ctx)
    log_value = math.log(base_freq) + math.log(scale)
    value = math.exp(log_value)
    if not math.isfinite(value):
        raise NumericError(
            "log-domain product produced non-finite result",
            stage=stage, scheme=scheme, bin_id=bin_id, context=ctx)
    return value


def log_domain_ratio(*, numerator, denominator, stage, scheme, bin_id=None,
                     context=None):
    """Return ``numerator / denominator`` computed entirely in log domain."""
    ctx = {"numerator": numerator, "denominator": denominator}
    if context:
        ctx.update(context)
    if not np.isfinite(numerator) or not np.isfinite(denominator):
        raise NumericError(
            "non-finite operand in log-domain ratio",
            stage=stage, scheme=scheme, bin_id=bin_id, context=ctx)
    if denominator == 0.0:
        raise NumericError(
            "zero denominator in log-domain ratio", stage=stage,
            scheme=scheme, bin_id=bin_id, context=ctx)
    if numerator < 0.0 or denominator < 0.0:
        raise NumericError(
            "negative operand in log-domain ratio", stage=stage,
            scheme=scheme, bin_id=bin_id, context=ctx)
    log_value = math.log(numerator) - math.log(denominator)
    value = math.exp(log_value)
    if not math.isfinite(value):
        raise NumericError(
            "log-domain ratio produced non-finite result",
            stage=stage, scheme=scheme, bin_id=bin_id, context=ctx)
    return value


class QuantizedBias:
    """Fixed-point int32 representation of a position-bias matrix."""

    def __init__(self, qvalues, scale, *, scheme, bin_id=None, shape=None):
        qvalues = np.asarray(qvalues, dtype=np.int32)
        if not np.isfinite(scale) or scale <= 0.0:
            raise NumericError(
                "quantization scale must be a positive finite number",
                stage="bias", scheme=scheme, bin_id=bin_id,
                context={"scale": scale})
        self.qvalues = qvalues
        self.scale = float(scale)
        self.scheme = scheme
        self.bin_id = bin_id
        self.shape = shape or tuple(qvalues.shape)

    @classmethod
    def from_float(cls, values, *, scheme, bin_id=None):
        values = np.asarray(values, dtype=np.float64)
        if not np.all(np.isfinite(values)):
            raise NumericError(
                "bias matrix contains NaN/Inf", stage="bias",
                scheme=scheme, bin_id=bin_id,
                context={"non_finite": int(np.size(values) -
                                           np.count_nonzero(
                                               np.isfinite(values)))})
        peak = float(np.max(np.abs(values))) if values.size else 0.0
        if peak == 0.0:
            scale = 1.0
        else:
            scale = HEADROOM * peak / float(-INT32_MIN)
        if not np.isfinite(scale) or scale <= 0.0:
            raise NumericError(
                "invalid quantization scale factor", stage="bias",
                scheme=scheme, bin_id=bin_id, context={"peak": peak})
        qvalues = np.round(values / scale).astype(np.int32)
        clipped = int(np.count_nonzero(np.abs(values / scale) > -INT32_MIN))
        return cls(qvalues, scale, scheme=scheme, bin_id=bin_id,
                   shape=tuple(values.shape)), clipped

    def dequantize(self):
        return self.qvalues.astype(np.float64) * self.scale

    def to_dict(self):
        return {
            "scheme": self.scheme,
            "bin_id": self.bin_id,
            "scale": self.scale,
            "dtype": "int32",
            "shape": list(self.shape),
            "q_min": int(np.min(self.qvalues)) if self.qvalues.size else 0,
            "q_max": int(np.max(self.qvalues)) if self.qvalues.size else 0,
        }


def finite_guard(values, *, stage, scheme, bin_id=None, context=None):
    """Raise ``E_NUMERIC`` when an array contains NaN/Inf."""
    arr = np.asarray(values, dtype=np.float64)
    if not np.all(np.isfinite(arr)):
        bad = int(arr.size - np.count_nonzero(np.isfinite(arr)))
        ctx = {"non_finite": bad}
        if context:
            ctx.update(context)
        raise NumericError(
            "non-finite value detected", stage=stage, scheme=scheme,
            bin_id=bin_id, context=ctx)
