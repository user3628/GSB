"""RGB image I/O and canonical deterministic PNG encoding."""

from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from PIL import Image

from .errors import E_INPUT_MISSING, WorkbenchError
from .util import sha256_bytes


@dataclass(frozen=True)
class LoadedImage:
    pixels: np.ndarray
    width: int
    height: int
    content_digest: str


def _png_chunk(kind: bytes, payload: bytes) -> bytes:
    checksum = zlib.crc32(kind + payload) & 0xFFFFFFFF
    return struct.pack(">I", len(payload)) + kind + payload + struct.pack(">I", checksum)


def encode_png_rgb(pixels: np.ndarray) -> bytes:
    array = np.asarray(pixels, dtype=np.uint8)
    if array.ndim != 3 or array.shape[2] != 3:
        raise ValueError("expected an HxWx3 RGB uint8 array")
    height, width, _ = array.shape
    if width <= 0 or height <= 0:
        raise ValueError("image dimensions must be positive")
    contiguous = np.ascontiguousarray(array)
    raw = b"".join(b"\x00" + row.tobytes() for row in contiguous)
    header = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    return b"".join(
        (
            b"\x89PNG\r\n\x1a\n",
            _png_chunk(b"IHDR", header),
            _png_chunk(b"IDAT", zlib.compress(raw, level=9)),
            _png_chunk(b"IEND", b""),
        )
    )


def pixel_digest(pixels: np.ndarray) -> str:
    array = np.ascontiguousarray(np.asarray(pixels, dtype=np.uint8))
    height, width, _ = array.shape
    return sha256_bytes(
        b"RGB8\0" + struct.pack(">II", width, height) + array.tobytes(order="C")
    )


def load_rgb(path: Path) -> LoadedImage:
    path = Path(path)
    if not path.is_file():
        raise WorkbenchError(
            E_INPUT_MISSING,
            f"input shard not found: {path.name}",
            stage="parsed",
            reason="INPUT_FILE_MISSING",
            details={"path": path.name},
        )
    try:
        with Image.open(path) as image:
            converted = image.convert("RGB")
            pixels = np.asarray(converted, dtype=np.uint8).copy()
    except Exception as exc:
        raise WorkbenchError(
            E_INPUT_MISSING,
            f"cannot decode RGB input shard {path.name}: {exc}",
            stage="parsed",
            reason="INPUT_DECODE_FAILED",
            details={"path": path.name},
        ) from exc
    height, width, _ = pixels.shape
    return LoadedImage(
        pixels=pixels,
        width=width,
        height=height,
        content_digest=pixel_digest(pixels),
    )
