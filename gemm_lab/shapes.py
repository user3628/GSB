from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np

from .errors import BusinessCode, GemmError, fail


SUPPORTED_DTYPES = (np.dtype("float32"), np.dtype("float64"))


def normalize_transpose(flag: str | bool, name: str) -> str:
    if isinstance(flag, (bool, np.bool_)):
        return "T" if flag else "N"
    if not isinstance(flag, str):
        fail(
            BusinessCode.E_INVALID_SHAPE,
            f"{name} transpose flag must be 'N', 'T', or bool",
            parameter=name,
            value=repr(flag),
        )
    normalized = flag.strip().upper()
    aliases = {"N": "N", "NO": "N", "FALSE": "N", "T": "T", "YES": "T", "TRUE": "T"}
    if normalized not in aliases:
        fail(
            BusinessCode.E_INVALID_SHAPE,
            f"{name} transpose flag must be 'N', 'T', or bool",
            parameter=name,
            value=flag,
        )
    return aliases[normalized]


def _validate_array(array: np.ndarray, name: str) -> np.ndarray:
    if not isinstance(array, np.ndarray):
        fail(
            BusinessCode.E_INVALID_SHAPE,
            f"{name} must be a NumPy ndarray",
            parameter=name,
            value_type=type(array).__name__,
        )
    if array.dtype not in SUPPORTED_DTYPES:
        fail(
            BusinessCode.E_INVALID_SHAPE,
            f"{name} dtype must be float32 or float64",
            parameter=name,
            dtype=str(array.dtype),
        )
    return array


@dataclass(frozen=True)
class MatrixShape:
    batch: int
    m: int
    n: int
    k: int
    trans_a: str
    trans_b: str
    a_shape: tuple[int, ...]
    b_shape: tuple[int, ...]
    dtype: str
    is_batched: bool

    @property
    def itemsize(self) -> int:
        return np.dtype(self.dtype).itemsize

    @property
    def output_shape(self) -> tuple[int, ...]:
        if self.is_batched:
            return (self.batch, self.m, self.n)
        return (self.m, self.n)

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch": self.batch,
            "m": self.m,
            "n": self.n,
            "k": self.k,
            "trans_a": self.trans_a,
            "trans_b": self.trans_b,
            "a_shape": list(self.a_shape),
            "b_shape": list(self.b_shape),
            "dtype": self.dtype,
            "batched": self.is_batched,
        }

    @property
    def fingerprint(self) -> str:
        return shape_fingerprint(
            batch=self.batch,
            m=self.m,
            n=self.n,
            k=self.k,
            trans_a=self.trans_a,
            trans_b=self.trans_b,
            dtype=self.dtype,
            is_batched=self.is_batched,
        )


def validate_operands(
    a: np.ndarray,
    b: np.ndarray,
    *,
    trans_a: str | bool = "N",
    trans_b: str | bool = "N",
) -> MatrixShape:
    """Validate GEMM/BMM operands before any output allocation.

    A 3-D pair is BMM; a 2-D pair is GEMM.  Batch mismatch is reported before
    per-matrix shape conflicts so callers receive the distinct stable code.
    """

    _validate_array(a, "A")
    _validate_array(b, "B")
    ta = normalize_transpose(trans_a, "A")
    tb = normalize_transpose(trans_b, "B")

    if a.ndim not in (2, 3) or b.ndim not in (2, 3):
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "A and B must both be 2-D (GEMM) or both be 3-D (BMM)",
            a_shape=list(a.shape),
            b_shape=list(b.shape),
        )
    if a.ndim != b.ndim:
        fail(
            BusinessCode.E_BATCH_MISMATCH,
            "GEMM/BMM operands must have matching rank",
            a_shape=list(a.shape),
            b_shape=list(b.shape),
        )

    is_batched = a.ndim == 3
    if is_batched:
        batch = int(a.shape[0])
        if int(b.shape[0]) != batch:
            fail(
                BusinessCode.E_BATCH_MISMATCH,
                "A and B batch dimensions differ",
                batch_a=batch,
                batch_b=int(b.shape[0]),
            )
        a2 = a.shape[-2:]
        b2 = b.shape[-2:]
    else:
        batch = 1
        a2 = a.shape
        b2 = b.shape

    a_m, a_k = (int(a2[0]), int(a2[1])) if ta == "N" else (int(a2[1]), int(a2[0]))
    b_k, b_n = (int(b2[0]), int(b2[1])) if tb == "N" else (int(b2[1]), int(b2[0]))
    if a_k != b_k:
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "logical K dimensions do not agree after transpose normalization",
            trans_a=ta,
            trans_b=tb,
            logical_k_a=a_k,
            logical_k_b=b_k,
            a_shape=list(a.shape),
            b_shape=list(b.shape),
        )
    if a_m <= 0 or b_n <= 0 or a_k <= 0 or batch <= 0:
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "all logical matrix dimensions must be positive",
            batch=batch,
            m=a_m,
            n=b_n,
            k=a_k,
        )
    if a.dtype != b.dtype:
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "A and B must use the same dtype",
            a_dtype=str(a.dtype),
            b_dtype=str(b.dtype),
        )

    return MatrixShape(
        batch=batch,
        m=a_m,
        n=b_n,
        k=a_k,
        trans_a=ta,
        trans_b=tb,
        a_shape=tuple(int(x) for x in a.shape),
        b_shape=tuple(int(x) for x in b.shape),
        dtype=a.dtype.name,
        is_batched=is_batched,
    )


def shape_fingerprint(
    *,
    batch: int,
    m: int,
    n: int,
    k: int,
    trans_a: str,
    trans_b: str,
    dtype: str,
    is_batched: bool,
) -> str:
    payload = {
        "batch": int(batch),
        "m": int(m),
        "n": int(n),
        "k": int(k),
        "trans_a": trans_a,
        "trans_b": trans_b,
        "dtype": dtype,
        "batched": bool(is_batched),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def array_digest(arrays: Iterable[np.ndarray]) -> str:
    digest = hashlib.sha256()
    for array in arrays:
        contiguous = np.ascontiguousarray(array)
        digest.update(str(contiguous.dtype).encode("ascii"))
        digest.update(json.dumps(list(contiguous.shape)).encode("ascii"))
        digest.update(contiguous.tobytes(order="C"))
    return digest.hexdigest()


def logical_views(
    a: np.ndarray,
    b: np.ndarray,
    shape: MatrixShape,
) -> tuple[np.ndarray, np.ndarray]:
    if shape.trans_a == "T":
        a = np.swapaxes(a, -1, -2)
    if shape.trans_b == "T":
        b = np.swapaxes(b, -1, -2)
    return a, b


def jsonable_shape(shape: MatrixShape) -> dict[str, Any]:
    return shape.to_dict()

