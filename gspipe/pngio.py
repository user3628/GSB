"""Strict RGB PNG decoding plus a byte-deterministic PNG encoder."""

from __future__ import annotations

import io
import struct
import zlib
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, UnidentifiedImageError

from .canonical import sha256_bytes
from .errors import E_INVALID_INPUT, PipelineError


PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def _png_chunk(kind: bytes, payload: bytes) -> bytes:
    crc = zlib.crc32(kind)
    crc = zlib.crc32(payload, crc) & 0xFFFFFFFF
    return struct.pack(">I", len(payload)) + kind + payload + struct.pack(">I", crc)


def encode_rgb_png(image: np.ndarray) -> bytes:
    """Encode an HxWx3 uint8 array without timestamps or ancillary metadata.

    Each row uses PNG filter type 0 and zlib level 9.  This makes the byte
    representation independent of Pillow's encoder settings and keeps repeated
    executions byte-for-byte identical.
    """

    array = np.asarray(image)
    if array.dtype != np.uint8 or array.ndim != 3 or array.shape[2] != 3:
        raise PipelineError(
            E_INVALID_INPUT,
            "PNG encoder expects an HxWx3 uint8 RGB array",
            details={"dtype": str(array.dtype), "shape": list(array.shape)},
        )
    height, width = int(array.shape[0]), int(array.shape[1])
    if height <= 0 or width <= 0:
        raise PipelineError(E_INVALID_INPUT, "Cannot encode an empty image")

    rows = bytearray()
    contiguous = np.ascontiguousarray(array, dtype=np.uint8)
    for row in contiguous:
        rows.append(0)
        rows.extend(row.tobytes(order="C"))
    compressed = zlib.compress(bytes(rows), level=9)
    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    return b"".join(
        [
            PNG_SIGNATURE,
            _png_chunk(b"IHDR", ihdr),
            _png_chunk(b"IDAT", compressed),
            _png_chunk(b"IEND", b""),
        ]
    )


def decode_rgb_png(data: bytes, *, source: str = "<bytes>") -> np.ndarray:
    """Decode a PNG and reject non-8-bit RGB inputs.

    The input contract is deliberately strict: accepting palette or alpha
    images would silently change the algorithm's pixel domain.
    """

    if not isinstance(data, (bytes, bytearray)):
        raise PipelineError(E_INVALID_INPUT, "PNG data must be bytes", details={"source": source})
    try:
        with Image.open(io.BytesIO(bytes(data))) as image:
            if image.format != "PNG":
                raise PipelineError(
                    E_INVALID_INPUT,
                    "Input must be a PNG image",
                    details={"source": source, "format": image.format},
                )
            if image.mode != "RGB":
                raise PipelineError(
                    E_INVALID_INPUT,
                    "Input PNG must use 8-bit RGB mode",
                    details={"source": source, "mode": image.mode},
                )
            image.load()
            pixels = np.asarray(image, dtype=np.uint8)
    except PipelineError:
        raise
    except (UnidentifiedImageError, OSError, ValueError) as exc:
        raise PipelineError(
            E_INVALID_INPUT,
            f"Unable to decode PNG: {exc}",
            details={"source": source},
        ) from exc
    if pixels.ndim != 3 or pixels.shape[2] != 3:
        raise PipelineError(
            E_INVALID_INPUT,
            "Decoded PNG is not an RGB pixel array",
            details={"source": source, "shape": list(pixels.shape)},
        )
    return np.array(pixels, dtype=np.uint8, copy=True, order="C")


def decode_rgb_png_file(path: str | Path) -> np.ndarray:
    file_path = Path(path)
    try:
        data = file_path.read_bytes()
    except OSError as exc:
        raise PipelineError(
            E_INVALID_INPUT,
            f"Unable to read input PNG: {exc}",
            details={"source": str(file_path)},
        ) from exc
    return decode_rgb_png(data, source=str(file_path))


def pixel_fingerprint(image: np.ndarray) -> str:
    array = np.ascontiguousarray(image, dtype=np.uint8)
    height, width, channels = array.shape
    header = f"RGB8:{width}x{height}:{channels}:".encode("ascii")
    return sha256_bytes(header + array.tobytes(order="C"))


def image_info(image: np.ndarray) -> dict[str, Any]:
    array = np.ascontiguousarray(image, dtype=np.uint8)
    return {
        "width": int(array.shape[1]),
        "height": int(array.shape[0]),
        "channels": int(array.shape[2]),
        "pixel_sha256": pixel_fingerprint(array),
    }
