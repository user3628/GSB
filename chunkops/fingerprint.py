"""Canonical hashing and deterministic tensor fingerprints."""

from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Iterable

import numpy as np


def canonical_json(value: Any) -> str:
    """Canonical JSON used for digests and version chains."""

    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def sha256_file(path: str | os.PathLike[str], block_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            block = handle.read(block_size)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def sha256_array(array: np.ndarray) -> str:
    digest = hashlib.sha256()
    digest.update(str(array.dtype).encode("ascii"))
    digest.update(repr(tuple(array.shape)).encode("ascii"))
    digest.update(np.ascontiguousarray(array).tobytes(order="C"))
    return digest.hexdigest()


def digest_object(value: Any) -> str:
    return sha256_text(canonical_json(value))


def shape_fingerprint(shape: Iterable[int]) -> str:
    normalized = [int(dim) for dim in shape]
    return digest_object({"kind": "shape-v1", "shape": normalized})


def _safe_number(value: float) -> float | str:
    value = float(value)
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    return value


def numeric_range(array: np.ndarray) -> dict[str, Any]:
    arr = np.asarray(array)
    finite = np.isfinite(arr)
    finite_values = arr[finite]
    result: dict[str, Any] = {
        "dtype": str(arr.dtype),
        "shape": [int(x) for x in arr.shape],
        "element_count": int(arr.size),
        "finite_count": int(finite_values.size),
        "nan_count": int(np.count_nonzero(np.isnan(arr))),
        "pos_inf_count": int(np.count_nonzero(np.isposinf(arr))),
        "neg_inf_count": int(np.count_nonzero(np.isneginf(arr))),
    }
    if finite_values.size:
        result.update(
            {
                "min": _safe_number(np.min(finite_values)),
                "max": _safe_number(np.max(finite_values)),
                "mean": _safe_number(np.mean(finite_values, dtype=np.float64)),
                "abs_max": _safe_number(np.max(np.abs(finite_values.astype(np.float64)))),
            }
        )
    else:
        result.update(
            {"min": None, "max": None, "mean": None, "abs_max": None}
        )
    result["anomaly_ratio"] = float(1.0 - finite_values.size / max(1, arr.size))
    return result


def numeric_fingerprint(array: np.ndarray) -> str:
    """Compact numeric-range fingerprint independent of the full payload digest."""

    rng = numeric_range(array)
    return digest_object({"kind": "numeric-range-v1", **rng})


def value_fingerprint(array: np.ndarray) -> str:
    """Full payload fingerprint, used to prevent stale cache hits."""

    return digest_object(
        {
            "kind": "tensor-values-v1",
            "dtype": str(array.dtype),
            "shape": [int(x) for x in array.shape],
            "sha256": sha256_array(array),
        }
    )


def write_canonical_json(path: str | os.PathLike[str], value: Any) -> None:
    Path(path).write_text(canonical_json(value) + "\n", encoding="utf-8")
