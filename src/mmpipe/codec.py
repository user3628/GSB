"""确定性序列化与版本指纹。

canonical_dumps 保证同一逻辑对象在任何进程、任何机器上逐字节一致：
- 字典键排序；
- 分隔符固定、无多余空白；
- 浮点采用 Python repr（往返一致，numpy 浮点先转内置 float）；
- 不允许集合、字节等不确定类型。

指纹采用 BLAKE2b（256 位摘要），纯标准库实现、跨平台稳定。
"""

import hashlib
import json
from typing import Any


def _to_native(obj: Any) -> Any:
    try:
        import numpy as np
    except ImportError:  # pragma: no cover
        np = None
    if np is not None:
        if isinstance(obj, np.ndarray):
            return [_to_native(v) for v in obj.tolist()]
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, (np.integer,)):
            return int(obj)
    if isinstance(obj, dict):
        return {str(k): _to_native(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_native(v) for v in obj]
    if isinstance(obj, float):
        if obj != obj or obj in (float("inf"), float("-inf")):
            raise ValueError("non-finite float cannot be serialized canonically")
        return obj
    if obj is None or isinstance(obj, (str, int, bool)):
        return obj
    raise TypeError(f"cannot canonicalize type {type(obj)!r}")


def canonical_dumps(obj: Any) -> bytes:
    return json.dumps(
        _to_native(obj),
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def fingerprint(obj: Any) -> str:
    """返回对象的 BLAKE2b-256 版本指纹（十六进制字符串）。"""
    return hashlib.blake2b(canonical_dumps(obj), digest_size=32).hexdigest()


def digest_bytes(data: bytes) -> str:
    return hashlib.blake2b(data, digest_size=32).hexdigest()


def short(fp: str, n: int = 12) -> str:
    return fp[:n]
