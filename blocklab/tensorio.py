"""Deterministic NumPy tensor loading, serialization, and hashing."""

from __future__ import annotations

import io
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .errors import E_INPUT, BusinessError
from .util import digest_bytes


@dataclass(frozen=True)
class TensorInput:
    array: np.ndarray
    key: str
    digest: str


def validate_layout(shape: tuple[int, ...]) -> None:
    if len(shape) not in (2, 4):
        raise BusinessError(
            E_INPUT,
            "tensor must be 2-D or [batch, heads, sequence, head_dim] 4-D",
            shape=list(shape),
        )
    if any(int(dim) <= 0 for dim in shape):
        raise BusinessError(E_INPUT, "all dimensions must be positive", shape=list(shape))


def array_digest(array: np.ndarray) -> str:
    contiguous = np.ascontiguousarray(array)
    header = f"{contiguous.dtype.str}|{','.join(str(dim) for dim in contiguous.shape)}|".encode(
        "ascii"
    )
    return digest_bytes(header + contiguous.tobytes(order="C"))


def npy_bytes(array: np.ndarray) -> bytes:
    buffer = io.BytesIO()
    np.lib.format.write_array(buffer, np.ascontiguousarray(array), allow_pickle=False)
    return buffer.getvalue()


def npy_from_bytes(data: bytes) -> np.ndarray:
    array = np.lib.format.read_array(io.BytesIO(data), allow_pickle=False)
    return np.ascontiguousarray(array)


def load_npz_tensor(path: Path) -> TensorInput:
    if not path.exists():
        raise BusinessError(E_INPUT, "shard file not found", path=str(path))
    try:
        with np.load(path, allow_pickle=False) as archive:
            candidates: list[tuple[str, np.ndarray]] = []
            for key in archive.files:
                value = archive[key]
                if isinstance(value, np.ndarray) and value.ndim in (2, 4):
                    candidates.append((key, value))
            if not candidates:
                raise BusinessError(
                    E_INPUT,
                    "NPZ contains no 2-D or 4-D tensor",
                    keys=list(archive.files),
                )
            preferred = ["tensor", "data", "shard", "matrix", "x"]
            selected = next(
                ((key, value) for key in preferred for candidate_key, value in candidates if candidate_key == key),
                candidates[0],
            )
    except BusinessError:
        raise
    except Exception as exc:
        raise BusinessError(E_INPUT, f"cannot read NPZ: {exc}", path=str(path)) from exc

    key, value = selected
    validate_layout(tuple(int(dim) for dim in value.shape))
    if not np.issubdtype(value.dtype, np.floating) and not np.issubdtype(
        value.dtype, np.integer
    ):
        raise BusinessError(
            E_INPUT,
            "tensor dtype must be real numeric",
            dtype=str(value.dtype),
        )
    array = np.ascontiguousarray(value, dtype=np.float64)
    return TensorInput(array=array, key=key, digest=array_digest(array))


def tensor_info(array: np.ndarray) -> dict[str, Any]:
    finite = np.isfinite(array)
    return {
        "shape": [int(dim) for dim in array.shape],
        "dtype": "float64",
        "element_count": int(array.size),
        "finite_count": int(np.count_nonzero(finite)),
        "nonfinite_count": int(array.size - np.count_nonzero(finite)),
        "digest": array_digest(array),
    }

