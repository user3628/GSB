"""确定性序列化、内容指纹、原子写入、PNG 与文件锁原语。

* 所有 JSON 统一走 :func:`dumps`，键排序、无空白、浮点固定有效位，
  保证同一逻辑内容逐字节一致（跨平台、跨运行）。
* :func:`atomic_write` 使用 ``tmp -> fsync -> os.replace`` 模式，
  写入中断只会留下孤立临时文件，不会出现半写目标文件。
* PNG 仅依赖标准库（zlib/struct），避免引入额外图像依赖。
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import struct
import zlib
from typing import Any

FLOAT_SIG = 10
"""浮点数保留的有效位数；同时用于序列化与坐标比较。"""


def round_float(value: float, sig: int = FLOAT_SIG) -> float:
    if value == 0 or not isinstance(value, (int, float)):
        return float(value)
    return float(f"{float(value):.{sig}g}")


def _normalize(obj: Any) -> Any:
    if isinstance(obj, float):
        if obj != obj:  # NaN 不允许进入确定性产物
            raise ValueError("NaN cannot be serialized deterministically")
        return round_float(obj)
    if isinstance(obj, (int, bool, str)) or obj is None:
        return obj
    if isinstance(obj, dict):
        return {str(k): _normalize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_normalize(v) for v in obj]
    raise TypeError(f"non-canonical type: {type(obj)!r}")


def dumps(obj: Any) -> bytes:
    """规范化 JSON：排序键、紧凑分隔符、固定浮点精度，返回 UTF-8 字节。"""
    return json.dumps(
        _normalize(obj),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def dump_bytes(obj: Any) -> bytes:
    return dumps(obj)


def loads(data: bytes | str) -> Any:
    return json.loads(data)


def fingerprint(obj: Any) -> str:
    """对规范化字节计算 SHA-256 十六进制摘要。"""
    return hashlib.sha256(dumps(obj)).hexdigest()


def fingerprint_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def atomic_write(path: str, data: bytes) -> None:
    """原子写入：同目录临时文件 + fsync + 原子 rename。"""
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def write_json(path: str, obj: Any) -> str:
    data = dumps(obj)
    atomic_write(path, data)
    return fingerprint_bytes(data)


def read_json(path: str) -> Any:
    with open(path, "rb") as handle:
        return loads(handle.read())


@contextlib.contextmanager
def file_lock(path: str):
    """跨进程排他文件锁（POSIX flock），同进程可重入由 fcntl 语义保证。"""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    handle = open(path, "a+b")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield handle
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


# --------------------------------------------------------------------------- #
# 极简 8 位灰度 PNG（标准库实现，确定性输出，无第三方图像库依赖）
# --------------------------------------------------------------------------- #
def _png_chunk(tag: bytes, payload: bytes) -> bytes:
    chunk = tag + payload
    return struct.pack(">I", len(payload)) + chunk + struct.pack(">I", zlib.crc32(chunk) & 0xFFFFFFFF)


def write_png(path: str, image) -> None:
    import numpy as np

    arr = np.asarray(image, dtype=np.uint8)
    if arr.ndim != 2:
        raise ValueError("only 2-D grayscale images are supported")
    height, width = arr.shape
    # 每行前置 filter type 0
    raw = b"".join(b"\x00" + arr[y].tobytes() for y in range(height))
    png = b"\x89PNG\r\n\x1a\n"
    ihdr = struct.pack(">IIBBBBB", width, height, 8, 0, 0, 0, 0)  # 8bit, grayscale
    png += _png_chunk(b"IHDR", ihdr)
    png += _png_chunk(b"IDAT", zlib.compress(raw, level=9))
    png += _png_chunk(b"IEND", b"")
    atomic_write(path, png)


def read_png(path: str):
    import numpy as np

    with open(path, "rb") as handle:
        blob = handle.read()
    if blob[:8] != b"\x89PNG\r\n\x1a\n":
        raise ValueError(f"not a PNG file: {path}")
    pos = 8
    width = height = bit_depth = color_type = 0
    idata = b""
    while pos < len(blob):
        (length,) = struct.unpack(">I", blob[pos : pos + 4])
        tag = blob[pos + 4 : pos + 8]
        payload = blob[pos + 8 : pos + 8 + length]
        pos += 12 + length
        if tag == b"IHDR":
            width, height, bit_depth, color_type = struct.unpack(">IIBB", payload[:10])
        elif tag == b"IDAT":
            idata += payload
        elif tag == b"IEND":
            break
    if bit_depth != 8 or color_type != 0:
        raise ValueError("only 8-bit grayscale PNG is supported")
    raw = zlib.decompress(idata)
    stride = width
    out = np.zeros((height, width), dtype=np.uint8)
    offset = 0
    for y in range(height):
        filter_type = raw[offset]
        offset += 1
        row = np.frombuffer(raw[offset : offset + stride], dtype=np.uint8).copy()
        offset += stride
        if filter_type == 1:  # Sub
            for x in range(1, width):
                row[x] = (row[x] + row[x - 1]) & 0xFF
        elif filter_type == 2:  # Up
            if y:
                row = (row + out[y - 1]) & 0xFF
        elif filter_type == 3:  # Average
            left = np.concatenate(([0], row[:-1]))
            up = out[y - 1] if y else np.zeros(width, dtype=np.uint8)
            row = (row + ((left.astype(np.uint16) + up.astype(np.uint16)) // 2)).astype(np.uint8)
        elif filter_type == 4:  # Paeth
            left = np.concatenate(([0], row[:-1]))
            up = out[y - 1] if y else np.zeros(width, dtype=np.uint8)
            ul = np.concatenate(([0], up[:-1]))
            base = left.astype(np.int16) + up.astype(np.int16) - ul.astype(np.int16)
            dl, du, dul = np.abs(base - left), np.abs(base - up), np.abs(base - ul)
            pred = np.where((dl <= du) & (dl <= dul), left, np.where(du <= dul, up, ul))
            row = (row.astype(np.int16) + pred) & 0xFF
        out[y] = row
    return out
