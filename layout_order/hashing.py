"""确定性序列化与内容指纹工具。

为保证「同输入 + 同配置」逐字节一致，所有 JSON 落盘均经
:func:`canonical_json` 规范化（键排序、无键间空白差异、固定换行），
指纹统一使用 SHA-256 十六进制摘要。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

_DUMPS_KWARGS = {"sort_keys": True, "ensure_ascii": False, "separators": (",", ":")}


def canonical_json(obj: Any) -> str:
    """返回规范化 JSON 字符串（无尾随换行）。"""
    return json.dumps(_to_native(obj), **_DUMPS_KWARGS)


def canonical_bytes(obj: Any) -> bytes:
    return canonical_json(obj).encode("utf-8")


def digest(obj: Any) -> str:
    """对任意可 JSON 化对象计算内容指纹。"""
    return hashlib.sha256(canonical_bytes(obj)).hexdigest()[:16]


def digest_full(obj: Any) -> str:
    return hashlib.sha256(canonical_bytes(obj)).hexdigest()


def digest_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _to_native(obj: Any) -> Any:
    """把 NumPy 标量/数组递归转为 Python 原生类型。"""
    try:
        import numpy as np
    except ImportError:  # pragma: no cover - numpy 为声明依赖
        np = None

    if np is not None:
        if isinstance(obj, np.generic):
            return obj.item()
        if isinstance(obj, np.ndarray):
            return [_to_native(v) for v in obj.tolist()]
    if isinstance(obj, dict):
        return {str(k): _to_native(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_native(v) for v in obj]
    return obj
