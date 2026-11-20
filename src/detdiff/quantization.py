"""Explicitly clamped float tensor quantization to deterministic 8-bit PNG."""

from __future__ import annotations

import binascii
import math
import struct
import zlib
from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class QuantizedImage:
    png_bytes: bytes
    array: np.ndarray
    pre_clamp_count: int
    post_clamp_count: int


def quantize_to_png(tensor: np.ndarray, *, compression_level: int = 9) -> QuantizedImage:
    values = np.asarray(tensor, dtype=np.float64)
    if values.ndim != 3 or values.shape[2] != 3:
        raise ValueError("expected an HxWx3 tensor")
    finite = np.isfinite(values)
    pre_clamp_count = int(np.count_nonzero((~finite) | (values < -1.0) | (values > 1.0)))
    sanitized = np.nan_to_num(values, nan=0.0, posinf=1.0, neginf=-1.0)
    clamped = np.clip(sanitized, -1.0, 1.0)
    scaled = np.rint((clamped + 1.0) * 127.5)
    post_clamp_count = int(np.count_nonzero((scaled < 0.0) | (scaled > 255.0)))
    final = np.clip(scaled, 0.0, 255.0).astype(np.uint8)
    png_bytes = encode_png_rgb8(final, compression_level=compression_level)
    return QuantizedImage(
        png_bytes=png_bytes,
        array=final,
        pre_clamp_count=pre_clamp_count,
        post_clamp_count=post_clamp_count,
    )


def encode_png_rgb8(array: np.ndarray, *, compression_level: int = 9) -> bytes:
    if array.ndim != 3 or array.shape[2] != 3 or array.dtype != np.uint8:
        raise ValueError("expected HxWx3 uint8 array")
    height, width, _ = array.shape
    scanlines = b"".join(b"\x00" + array[row].tobytes(order="C") for row in range(height))
    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    idat = zlib.compress(scanlines, level=compression_level)
    return b"".join(
        (
            b"\x89PNG\r\n\x1a\n",
            _png_chunk(b"IHDR", ihdr),
            _png_chunk(b"IDAT", idat),
            _png_chunk(b"IEND", b""),
        )
    )


def _png_chunk(kind: bytes, data: bytes) -> bytes:
    crc = binascii.crc32(kind + data) & 0xFFFFFFFF
    return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", crc)


def summary_stats(tensor: np.ndarray) -> dict[str, float]:
    flat = np.asarray(tensor, dtype=np.float64).reshape(-1)
    mean = math.fsum(float(value) for value in flat) / int(flat.size)
    variance = math.fsum((float(value) - mean) ** 2 for value in flat) / int(flat.size)
    return {"mean": round(mean, 12), "variance": round(variance, 12)}

