from __future__ import annotations

import struct
import zlib
from pathlib import Path

import numpy as np


class PNGError(ValueError):
    pass


def encode_rgb_png(pixels: np.ndarray) -> bytes:
    arr = np.asarray(pixels, dtype=np.uint8)
    if arr.ndim != 3 or arr.shape[2] != 3:
        raise PNGError("RGB PNG input must have shape (height, width, 3)")
    height, width, _ = arr.shape
    rows = [b"\x00" + arr[row].tobytes() for row in range(height)]
    raw = b"".join(rows)

    sig = b"\x89PNG\r\n\x1a\n"

    def chunk(kind: bytes, data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)

    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    return sig + chunk(b"IHDR", ihdr) + chunk(b"IDAT", zlib.compress(raw, 9)) + chunk(b"IEND", b"")


def decode_rgb_png(path: Path) -> np.ndarray:
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise PNGError(f"cannot read PNG: {exc}") from exc
    return decode_rgb_png_bytes(data)


def decode_rgb_png_bytes(data: bytes) -> np.ndarray:
    if not data.startswith(b"\x89PNG\r\n\x1a\n"):
        raise PNGError("invalid PNG signature")
    pos = 8
    width = height = depth = color_type = interlace = None
    idat = bytearray()
    palette: list[tuple[int, int, int]] = []
    while pos < len(data):
        if pos + 12 > len(data):
            raise PNGError("truncated PNG chunk")
        length = struct.unpack(">I", data[pos : pos + 4])[0]
        kind = data[pos + 4 : pos + 8]
        payload = data[pos + 8 : pos + 8 + length]
        crc = struct.unpack(">I", data[pos + 8 + length : pos + 12 + length])[0]
        if zlib.crc32(kind + payload) & 0xFFFFFFFF != crc:
            raise PNGError(f"PNG CRC mismatch in {kind!r}")
        pos += 12 + length
        if kind == b"IHDR":
            if length != 13:
                raise PNGError("invalid IHDR")
            width, height, depth, color_type, _compression, _filter, interlace = struct.unpack(">IIBBBBB", payload)
        elif kind == b"PLTE":
            palette = [tuple(payload[offset : offset + 3]) for offset in range(0, length, 3)]
        elif kind == b"IDAT":
            idat.extend(payload)
        elif kind == b"IEND":
            break

    if width is None or height is None:
        raise PNGError("PNG has no IHDR")
    if depth != 8:
        raise PNGError("only 8-bit PNG is supported")
    if interlace != 0:
        raise PNGError("interlaced PNG is not supported")
    if color_type not in (0, 2, 3, 6):
        raise PNGError(f"unsupported PNG color type {color_type}")
    channels = {0: 1, 2: 3, 3: 1, 6: 4}[color_type]
    bpp = channels
    stride = width * channels
    try:
        raw = zlib.decompress(bytes(idat))
    except zlib.error as exc:
        raise PNGError(f"invalid PNG IDAT: {exc}") from exc
    expected = height * (stride + 1)
    if len(raw) != expected:
        raise PNGError("unexpected PNG scanline size")

    out = np.empty((height, width, 3), dtype=np.uint8)
    prev = np.zeros(stride, dtype=np.int16)
    offset = 0
    for row in range(height):
        filter_type = raw[offset]
        offset += 1
        src = np.frombuffer(raw[offset : offset + stride], dtype=np.uint8).astype(np.int16)
        offset += stride
        dst = np.zeros(stride, dtype=np.int16)
        for idx in range(stride):
            left = int(dst[idx - bpp]) if idx >= bpp else 0
            up = int(prev[idx])
            up_left = int(prev[idx - bpp]) if idx >= bpp else 0
            if filter_type == 0:
                pred = 0
            elif filter_type == 1:
                pred = left
            elif filter_type == 2:
                pred = up
            elif filter_type == 3:
                pred = (left + up) // 2
            elif filter_type == 4:
                predictor = left + up - up_left
                pa = abs(predictor - left)
                pb = abs(predictor - up)
                pc = abs(predictor - up_left)
                pred = left if pa <= pb and pa <= pc else up if pb <= pc else up_left
            else:
                raise PNGError(f"unsupported PNG filter {filter_type}")
            dst[idx] = (int(src[idx]) + pred) & 0xFF
        if color_type == 0:
            gray = dst.astype(np.uint8)
            out[row] = np.repeat(gray[:, None], 3, axis=1)
        elif color_type == 2:
            out[row] = dst.reshape(width, 3).astype(np.uint8)
        elif color_type == 3:
            indices = dst.astype(np.uint8)
            if not palette:
                raise PNGError("indexed PNG missing PLTE")
            palette_arr = np.asarray(palette, dtype=np.uint8)
            if indices.max(initial=0) >= len(palette_arr):
                raise PNGError("indexed PNG palette overflow")
            out[row] = palette_arr[indices]
        else:
            rgba = dst.reshape(width, 4).astype(np.int16)
            alpha = rgba[:, 3:4]
            rgb = rgba[:, :3]
            out[row] = ((rgb * alpha + 255 * (255 - alpha)) // 255).astype(np.uint8)
        prev = dst
    return out
