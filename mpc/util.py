from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from typing import Any

import numpy as np

from .errors import BusinessError


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def json_digest(value: Any) -> str:
    return sha256_hex(canonical_json_bytes(value))


def float_token(value: float) -> str:
    """Stable binary identity for a float used by derivation digests."""

    return float(value).hex()


def array_digest(array: np.ndarray, *, declared_dtype: str | None = None) -> str:
    """Hash a canonical, platform-stable array representation.

    The dtype and shape are included so equal bytes with different shapes do not
    collide. Windows x86-64 and the WSL/Linux test environment are both
    little-endian; explicit little-endian dtype strings make this contract clear.
    """

    arr = np.asarray(array)
    if arr.dtype.byteorder not in ("<", "=", "|"):
        arr = arr.astype(arr.dtype.newbyteorder("<"), copy=False)
    if declared_dtype is not None and str(arr.dtype) != declared_dtype:
        arr = arr.astype(np.dtype(declared_dtype), copy=False)
    arr = np.ascontiguousarray(arr)
    header = canonical_json_bytes(
        {"dtype": arr.dtype.str, "shape": list(arr.shape), "order": "C"}
    )
    h = hashlib.sha256()
    h.update(header)
    h.update(b"\0")
    h.update(arr.tobytes(order="C"))
    return h.hexdigest()


def ensure_finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise BusinessError("E_INVALID_REQUEST", f"{field} must be a number")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise BusinessError("E_INVALID_REQUEST", f"{field} must be a number") from exc
    if not math.isfinite(number):
        raise BusinessError("E_INVALID_REQUEST", f"{field} must be finite")
    return number


def normalize_precision(value: Any) -> str:
    if not isinstance(value, str):
        raise BusinessError("E_INVALID_PRECISION", "precision must be FP16 or FP32")
    precision = value.strip().upper()
    if precision not in {"FP16", "FP32"}:
        raise BusinessError(
            "E_INVALID_PRECISION",
            "precision must be FP16 or FP32",
            details={"received": value},
        )
    return precision


def require_mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise BusinessError("E_INVALID_REQUEST", f"{field} must be an object")
    return value


def parse_precision_assignments(value: Any, *, field: str = "shards") -> dict[str, str]:
    """Accept both the compact map form and a list of shard objects."""

    if isinstance(value, dict):
        return {str(shard_id): normalize_precision(precision) for shard_id, precision in value.items()}
    if isinstance(value, list):
        result: dict[str, str] = {}
        for item in value:
            if isinstance(item, str):
                if ":" not in item:
                    raise BusinessError(
                        "E_INVALID_POLICY",
                        f"{field} string entries must use 'shard_id:PRECISION'",
                    )
                shard_id, precision = item.split(":", 1)
                result[shard_id] = normalize_precision(precision)
                continue
            if not isinstance(item, dict):
                raise BusinessError("E_INVALID_POLICY", f"{field} entries must be objects")
            shard_id = item.get("shard_id", item.get("id"))
            precision = item.get("precision")
            if shard_id is None or precision is None:
                raise BusinessError(
                    "E_INVALID_POLICY", f"{field} entries require shard_id and precision"
                )
            result[str(shard_id)] = normalize_precision(precision)
        return result
    raise BusinessError("E_INVALID_POLICY", f"{field} must be an object or list")


def canonical_float(value: float) -> float:
    # JSON numbers are still finite and stable for the ranges used by this app.
    if not math.isfinite(float(value)):
        raise ValueError("non-finite float cannot be serialized")
    return float(value)
