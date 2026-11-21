"""Canonical hashing helpers used by input and dependency fingerprints."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from typing import Any

import numpy as np


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def update_array_hash(hasher: "hashlib._Hash", label: str, array: np.ndarray) -> None:
    contiguous = np.ascontiguousarray(array)
    hasher.update(label.encode("utf-8"))
    hasher.update(b"\0")
    hasher.update(contiguous.dtype.str.encode("ascii"))
    hasher.update(b"\0")
    hasher.update(canonical_json(list(contiguous.shape)).encode("ascii"))
    hasher.update(b"\0")
    hasher.update(contiguous.tobytes(order="C"))
    hasher.update(b"\0")


def sha256_arrays(named_arrays: Mapping[str, np.ndarray] | Iterable[tuple[str, np.ndarray]]) -> str:
    hasher = hashlib.sha256()
    items = named_arrays.items() if isinstance(named_arrays, Mapping) else named_arrays
    for label, array in sorted(items, key=lambda item: item[0]):
        update_array_hash(hasher, label, array)
    return hasher.hexdigest()


def array_digest(array: np.ndarray) -> str:
    return sha256_arrays({"array": array})

