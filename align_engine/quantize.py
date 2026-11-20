"""Symmetric per-dimension integer quantization with recorded scale factors.

Vectors are stored as int32 arrays plus one float64 scale per dimension and
the declared bit-width.  ``dequantize(quantize(v))`` restores vectors to the
precision declared by the bit-width (max abs error <= scale / 2 per element).
"""

import numpy as np


def quantize(vectors, bits):
    if bits < 2 or bits > 32:
        raise ValueError("bits must be in [2, 32]")
    matrix = np.asarray(vectors, dtype=np.float64)
    if matrix.ndim != 2:
        raise ValueError("vectors must be a 2-D matrix")
    qmax = float(2 ** (bits - 1) - 1)
    if matrix.shape[0] == 0:
        max_abs = np.zeros(matrix.shape[1], dtype=np.float64)
    else:
        max_abs = np.max(np.abs(matrix), axis=0)
    scales = np.where(max_abs > 0.0, max_abs / qmax, 1.0)
    quantized = np.rint(matrix / scales).clip(-qmax, qmax).astype(np.int32)
    return quantized, [float(s) for s in scales]


def dequantize(quantized, scales):
    q = np.asarray(quantized, dtype=np.float64)
    s = np.asarray(scales, dtype=np.float64)
    return q * s
