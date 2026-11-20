"""CSR loading, validation, and stable fingerprints."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from .errors import SpMMError, fail
from .util import array_digest, canonical_json, sha256_hex


SUPPORTED_DTYPES = (np.dtype("float32"), np.dtype("float64"))


def _is_integer_dtype(dtype: np.dtype) -> bool:
    return np.issubdtype(dtype, np.integer)


def _location_array_slice(array_name: str, position: int, row: int | None = None) -> dict[str, Any]:
    location: dict[str, Any] = {"array": array_name, "position": int(position)}
    if row is not None:
        location["row"] = int(row)
    return location


@dataclass
class CSRMatrix:
    """Validated CSR arrays.

    The class deliberately owns copies of the three input arrays. Callers may
    safely mutate their originals after construction without changing a kernel
    run or its fingerprints.
    """

    indptr: np.ndarray
    indices: np.ndarray
    data: np.ndarray
    shape: tuple[int, int]

    @classmethod
    def from_arrays(
        cls,
        indptr: np.ndarray | Iterable[int],
        indices: np.ndarray | Iterable[int],
        data: np.ndarray | Iterable[float],
        shape: tuple[int, int],
    ) -> "CSRMatrix":
        try:
            rows, cols = (int(shape[0]), int(shape[1]))
        except (TypeError, ValueError, IndexError) as exc:
            raise SpMMError(
                "E_SHAPE_INVALID",
                "shape must be a two-element integer sequence",
                {"received": repr(shape)},
            ) from exc
        if rows <= 0 or cols <= 0:
            fail(
                "E_SHAPE_INVALID",
                "CSR matrix dimensions must both be positive",
                details={"rows": rows, "cols": cols},
                location={"shape": [rows, cols]},
            )

        ptr = np.asarray(indptr)
        idx = np.asarray(indices)
        values = np.asarray(data)
        if ptr.ndim != 1 or idx.ndim != 1 or values.ndim != 1:
            fail(
                "E_BAD_CSR",
                "indptr, indices, and data must be one-dimensional arrays",
                details={"indptr_ndim": ptr.ndim, "indices_ndim": idx.ndim, "data_ndim": values.ndim},
            )
        if not _is_integer_dtype(ptr.dtype):
            fail("E_BAD_CSR", "indptr must use an integer dtype", details={"dtype": str(ptr.dtype)})
        if not _is_integer_dtype(idx.dtype):
            fail("E_BAD_CSR", "indices must use an integer dtype", details={"dtype": str(idx.dtype)})
        if values.dtype not in SUPPORTED_DTYPES:
            fail(
                "E_DTYPE_UNSUPPORTED",
                "data must be float32 or float64",
                details={"dtype": str(values.dtype), "supported": [str(x) for x in SUPPORTED_DTYPES]},
            )

        ptr = np.ascontiguousarray(ptr, dtype=np.int64).copy()
        idx = np.ascontiguousarray(idx, dtype=np.int64).copy()
        values = np.ascontiguousarray(values).copy()
        ptr.setflags(write=False)
        idx.setflags(write=False)
        values.setflags(write=False)

        if ptr.size != rows + 1:
            fail(
                "E_BAD_CSR",
                "indptr length must equal rows + 1",
                details={"indptr_length": int(ptr.size), "rows": rows, "expected_length": rows + 1},
                location={"array": "indptr", "position": 0},
            )
        if int(ptr[0]) != 0:
            fail(
                "E_BAD_CSR",
                "indptr[0] must be zero",
                details={"value": int(ptr[0])},
                location={"array": "indptr", "position": 0},
            )
        if ptr.size and int(ptr[-1]) != int(idx.size):
            fail(
                "E_BAD_CSR",
                "indptr[-1] must equal the number of stored nonzeros",
                details={"indptr_last": int(ptr[-1]), "nnz": int(idx.size)},
                location={"array": "indptr", "position": int(ptr.size - 1)},
            )
        if ptr.size > 1:
            bad = np.flatnonzero(np.diff(ptr) < 0)
            if bad.size:
                pos = int(bad[0] + 1)
                fail(
                    "E_BAD_CSR",
                    "indptr must be monotonically nondecreasing",
                    details={"current": int(ptr[pos]), "previous": int(ptr[pos - 1])},
                    location={"array": "indptr", "position": pos, "row": pos - 1},
                )
        if idx.size != values.size:
            fail(
                "E_BAD_CSR",
                "indices and data must have equal lengths",
                details={"indices_length": int(idx.size), "data_length": int(values.size)},
            )
        if idx.size:
            bad = np.flatnonzero((idx < 0) | (idx >= cols))
            if bad.size:
                pos = int(bad[0])
                row = int(np.searchsorted(ptr, pos, side="right") - 1)
                row = min(max(row, 0), rows - 1)
                fail(
                    "E_BAD_CSR",
                    "column index is outside the CSR shape",
                    details={"column_index": int(idx[pos]), "cols": cols},
                    location={"array": "indices", "position": pos, "row": row},
                )

        return cls(ptr, idx, values, (rows, cols))

    @property
    def rows(self) -> int:
        return self.shape[0]

    @property
    def cols(self) -> int:
        return self.shape[1]

    @property
    def nnz(self) -> int:
        return int(self.indices.size)

    @property
    def dtype(self) -> np.dtype:
        return self.data.dtype

    @property
    def itemsize(self) -> int:
        return int(self.data.dtype.itemsize)

    def row_counts(self) -> np.ndarray:
        return np.diff(self.indptr)

    def row_nnz(self, row: int) -> int:
        return int(self.indptr[row + 1] - self.indptr[row])

    def features(self) -> dict[str, Any]:
        counts = self.row_counts()
        nnz = self.nnz
        empty_rows = int(np.count_nonzero(counts == 0))
        explicit_zeros = int(np.count_nonzero(self.data == 0.0))
        duplicate_count = 0
        max_duplicate_extra = 0
        for row in range(self.rows):
            start, end = int(self.indptr[row]), int(self.indptr[row + 1])
            if end - start < 2:
                continue
            _, unique_counts = np.unique(self.indices[start:end], return_counts=True)
            if unique_counts.size:
                max_extra = int(unique_counts.max() - 1)
                max_duplicate_extra = max(max_duplicate_extra, max_extra)
                duplicate_count += int((unique_counts - 1).sum())
        return {
            "rows": self.rows,
            "cols": self.cols,
            "nnz": nnz,
            "density": float(nnz / (self.rows * self.cols)),
            "avg_nnz_per_row": float(nnz / self.rows),
            "empty_rows": empty_rows,
            "empty_row_ratio": float(empty_rows / self.rows),
            "explicit_zero_count": explicit_zeros,
            "duplicate_count": duplicate_count,
            "max_duplicate_extra_per_index": max_duplicate_extra,
            "dtype": str(self.dtype),
            "has_nonfinite_data": bool(np.any(~np.isfinite(self.data))),
            "shape_fingerprint": self.shape_fingerprint(),
            "structure_fingerprint": self.structure_fingerprint(),
            "value_fingerprint": self.value_fingerprint(),
        }

    def shape_fingerprint(self) -> str:
        return sha256_hex(canonical_json({"rows": self.rows, "cols": self.cols}))

    def structure_fingerprint(self) -> str:
        header = canonical_json(
            {
                "indptr_length": int(self.indptr.size),
                "indices_length": int(self.indices.size),
                "shape": [self.rows, self.cols],
            }
        )
        return sha256_hex(
            header
            + b"\0"
            + self.indptr.astype("<i8", copy=False).tobytes()
            + self.indices.astype("<i8", copy=False).tobytes()
        )

    def value_fingerprint(self) -> str:
        return array_digest(self.data)

    def fingerprints(self) -> dict[str, str]:
        return {
            "shape": self.shape_fingerprint(),
            "structure": self.structure_fingerprint(),
            "values": self.value_fingerprint(),
        }

    def validate_duplicates(self, tolerance: int) -> dict[str, Any]:
        """Validate repeated column indices.

        ``tolerance`` is the maximum allowed number of *extra* occurrences of
        one (row, column) pair. Zero therefore rejects every duplicate.
        """

        if tolerance < 0:
            fail("E_USAGE", "duplicate_tolerance must be nonnegative", details={"value": tolerance})
        max_extra = 0
        duplicate_count = 0
        first_violation: dict[str, Any] | None = None
        for row in range(self.rows):
            start, end = int(self.indptr[row]), int(self.indptr[row + 1])
            if end - start < 2:
                continue
            columns = self.indices[start:end]
            unique, inverse, counts = np.unique(columns, return_inverse=True, return_counts=True)
            if counts.size == 0:
                continue
            max_extra = max(max_extra, int(counts.max() - 1))
            duplicate_count += int((counts - 1).sum())
            if first_violation is None:
                for unique_index in np.flatnonzero(counts - 1 > tolerance):
                    occurrences = np.flatnonzero(inverse == unique_index) + start
                    first_violation = {
                        "row": row,
                        "column_index": int(unique[unique_index]),
                        "occurrences": int(counts[unique_index]),
                        "allowed_extra": int(tolerance),
                        "nnz_positions": [int(x) for x in occurrences.tolist()],
                    }
                    break
        if first_violation is not None:
            details = {
                "tolerance": int(tolerance),
                "duplicate_count": duplicate_count,
                "max_extra_per_index": max_extra,
                "violation": first_violation,
            }
            fail(
                "E_DUP_INDEX",
                "duplicate column-index occurrences exceed the configured tolerance",
                details=details,
                location={
                    "row": first_violation["row"],
                    "column_index": first_violation["column_index"],
                    "nnz_positions": first_violation["nnz_positions"],
                },
            )
        return {
            "tolerance": int(tolerance),
            "duplicate_count": duplicate_count,
            "max_extra_per_index": max_extra,
            "passed": True,
        }


def load_csr_npz(path: str | Path, *, shape: tuple[int, int] | None = None) -> CSRMatrix:
    file_path = Path(path)
    try:
        with np.load(file_path, allow_pickle=False) as archive:
            keys = set(archive.files)
            missing = {"indptr", "indices", "data"} - keys
            if missing:
                fail(
                    "E_BAD_CSR",
                    "NPZ file is missing required CSR arrays",
                    details={"missing": sorted(missing), "path": str(file_path)},
                )
            indptr = archive["indptr"]
            indices = archive["indices"]
            data = archive["data"]
            embedded_shape: tuple[int, int] | None = None
            if "shape" in keys:
                raw_shape = np.asarray(archive["shape"]).reshape(-1)
                if raw_shape.size != 2:
                    fail(
                        "E_SHAPE_INVALID",
                        "embedded shape must have two elements",
                        details={"shape": raw_shape.tolist()},
                        location={"path": str(file_path)},
                    )
                embedded_shape = (int(raw_shape[0]), int(raw_shape[1]))
    except SpMMError:
        raise
    except (OSError, ValueError) as exc:
        raise SpMMError("E_IO", f"cannot load CSR NPZ: {file_path}", {"path": str(file_path), "cause": str(exc)}) from exc

    if shape is not None and embedded_shape is not None and tuple(shape) != embedded_shape:
        fail(
            "E_SHAPE_INVALID",
            "caller-provided shape conflicts with the NPZ embedded shape",
            details={"caller_shape": list(shape), "embedded_shape": list(embedded_shape)},
        )
    resolved_shape = tuple(shape) if shape is not None else embedded_shape
    if resolved_shape is None:
        rows = int(np.asarray(indptr).size - 1)
        cols = int(np.max(indices)) + 1 if np.asarray(indices).size else 0
        resolved_shape = (rows, cols)
    return CSRMatrix.from_arrays(indptr, indices, data, resolved_shape)


def load_dense_npy(path: str | Path, *, dtype: np.dtype | str | None = None) -> np.ndarray:
    file_path = Path(path)
    try:
        array = np.load(file_path, allow_pickle=False)
    except (OSError, ValueError) as exc:
        raise SpMMError("E_IO", f"cannot load dense NPY: {file_path}", {"path": str(file_path), "cause": str(exc)}) from exc
    if array.ndim != 2:
        fail(
            "E_SHAPE_INVALID",
            "dense right-hand matrix must be two-dimensional",
            details={"ndim": int(array.ndim), "path": str(file_path)},
        )
    if array.dtype not in SUPPORTED_DTYPES:
        fail(
            "E_DTYPE_UNSUPPORTED",
            "dense matrix must use float32 or float64",
            details={"dtype": str(array.dtype), "supported": [str(x) for x in SUPPORTED_DTYPES]},
        )
    if dtype is not None and array.dtype != np.dtype(dtype):
        fail(
            "E_DTYPE_MISMATCH",
            "dense matrix dtype does not match the requested compute dtype",
            details={"dense_dtype": str(array.dtype), "requested": str(np.dtype(dtype))},
        )
    return np.ascontiguousarray(array)

