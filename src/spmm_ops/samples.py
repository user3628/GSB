"""Deterministic local sample and high-precision reference generation."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, localcontext
import json
import os
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from .csr import CSRMatrix
from .util import array_digest, atomic_write_bytes, atomic_write_json, canonical_array_bytes, json_digest, sha256_hex


REFERENCE_PRECISION = 80
REFERENCE_DISPLAY_DIGITS = 40


@dataclass(frozen=True)
class SampleSpec:
    name: str
    rows: int
    cols: int
    density: float
    seed: int
    dense_cols: int = 24
    kind: str = "regular"


SPECS: tuple[SampleSpec, ...] = (
    SampleSpec("square_1pct", 96, 96, 0.01, 1101, 24),
    SampleSpec("square_10pct", 96, 96, 0.10, 1110, 24),
    SampleSpec("square_40pct", 96, 96, 0.40, 1140, 24),
    SampleSpec("tall_1pct", 160, 64, 0.01, 2101, 20),
    SampleSpec("tall_10pct", 160, 64, 0.10, 2110, 20),
    SampleSpec("tall_40pct", 160, 64, 0.40, 2140, 20),
    SampleSpec("wide_1pct", 64, 160, 0.01, 3101, 32),
    SampleSpec("wide_10pct", 64, 160, 0.10, 3110, 32),
    SampleSpec("wide_40pct", 64, 160, 0.40, 3140, 32),
    SampleSpec("degenerate_struct", 48, 40, 0.12, 4101, 16, "degenerate"),
    SampleSpec("anomaly_nan_inf", 24, 18, 0.20, 5101, 12, "anomaly"),
)


def _row_counts(rng: np.random.Generator, rows: int, nnz: int) -> np.ndarray:
    probabilities = np.full(rows, 1.0 / rows, dtype=np.float64)
    return rng.multinomial(nnz, probabilities).astype(np.int64)


def _generate_regular(spec: SampleSpec) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    rng = np.random.default_rng(spec.seed)
    target_nnz = max(1, int(round(spec.rows * spec.cols * spec.density)))
    counts = _row_counts(rng, spec.rows, target_nnz)
    indptr = np.zeros(spec.rows + 1, dtype=np.int64)
    indptr[1:] = np.cumsum(counts)
    indices = np.empty(target_nnz, dtype=np.int64)
    data = rng.uniform(-1.0, 1.0, size=target_nnz).astype(np.float64)
    for row, count in enumerate(counts):
        count_int = int(count)
        start = int(indptr[row])
        if count_int:
            selected = np.sort(rng.choice(spec.cols, size=count_int, replace=False)).astype(np.int64)
            indices[start : start + count_int] = selected
    dense_rng = np.random.default_rng(spec.seed + 10_000)
    dense = dense_rng.uniform(-0.75, 0.75, size=(spec.cols, spec.dense_cols)).astype(np.float64)
    return indptr, indices, data, dense


def _generate_degenerate(spec: SampleSpec) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    rng = np.random.default_rng(spec.seed)
    target_nnz = max(1, int(round(spec.rows * spec.cols * spec.density)))
    counts = _row_counts(rng, spec.rows, target_nnz)
    for row in (3,):
        counts[row] = 0
    counts[17] = 3
    for row in range(28, 32):
        counts[row] = 0
    indptr = np.zeros(spec.rows + 1, dtype=np.int64)
    indptr[1:] = np.cumsum(counts)
    nnz = int(indptr[-1])
    indices = np.empty(nnz, dtype=np.int64)
    data = rng.uniform(-1.0, 1.0, size=nnz).astype(np.float64)
    for row, count in enumerate(counts):
        count_int = int(count)
        start = int(indptr[row])
        if count_int:
            selected = rng.choice(spec.cols, size=count_int, replace=False).astype(np.int64)
            indices[start : start + count_int] = selected
    # Deliberately unsorted rows exercise stable ordering in the kernel.
    for row in range(spec.rows):
        start, end = int(indptr[row]), int(indptr[row + 1])
        if end - start > 1:
            permutation = rng.permutation(end - start)
            indices[start:end] = indices[start:end][permutation]
            data[start:end] = data[start:end][permutation]
    # Explicitly stored zeros, including one negative zero, remain positions.
    for position in range(5, nnz, 17):
        data[position] = 0.0
    if nnz > 3:
        data[3] = -0.0
    # Create repeated column indices without changing nnz.
    for row in (0, 7, 19, 33):
        start, end = int(indptr[row]), int(indptr[row + 1])
        if end - start >= 3:
            indices[start + 2] = indices[start + 1]
    # A deliberately order-sensitive row: block boundaries change the rounded
    # sequential sum while the Decimal reference remains exactly 1.
    cancellation_start = int(indptr[17])
    indices[cancellation_start : cancellation_start + 3] = np.asarray([2, 26, 4], dtype=np.int64)
    data[cancellation_start : cancellation_start + 3] = np.asarray([1.0e16, 1.0, -1.0e16], dtype=np.float64)
    dense_rng = np.random.default_rng(spec.seed + 10_000)
    dense = dense_rng.uniform(-0.75, 0.75, size=(spec.cols, spec.dense_cols)).astype(np.float64)
    return indptr, indices, data, dense


def _generate_anomaly(spec: SampleSpec) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    indptr, indices, data, dense = _generate_regular(spec)
    first_nonzero_rows = np.flatnonzero(np.diff(indptr) > 0)
    if first_nonzero_rows.size:
        row = int(first_nonzero_rows[0])
        data[int(indptr[row])] = np.nan
    if first_nonzero_rows.size > 1:
        row = int(first_nonzero_rows[-1])
        data[int(indptr[row + 1] - 1)] = np.inf
    dense[5, 3] = np.nan
    dense[9, 7] = np.inf
    dense[12, 1] = -np.inf
    return indptr, indices, data, dense


def _decimal_value(value: float) -> tuple[str, Decimal | int | None]:
    if np.isnan(value):
        return "nan", None
    if np.isposinf(value):
        return "inf", 1
    if np.isneginf(value):
        return "inf", -1
    return "finite", Decimal.from_float(float(value))


def _decimal_product(
    left: tuple[str, Decimal | int | None], right: tuple[str, Decimal | int | None]
) -> tuple[str, Decimal | int | None]:
    left_kind, left_value = left
    right_kind, right_value = right
    if left_kind == "nan" or right_kind == "nan":
        return "nan", None
    if left_kind == "inf" and right_kind == "inf":
        return "inf", int(left_value) * int(right_value)  # type: ignore[arg-type]
    if left_kind == "inf":
        finite = right_value  # type: ignore[assignment]
        if finite == 0:
            return "nan", None
        return "inf", int(left_value) * (1 if finite > 0 else -1)  # type: ignore[operator]
    if right_kind == "inf":
        finite = left_value  # type: ignore[assignment]
        if finite == 0:
            return "nan", None
        return "inf", int(right_value) * (1 if finite > 0 else -1)  # type: ignore[operator]
    return "finite", left_value * right_value  # type: ignore[operator]


def _decimal_add(
    left: tuple[str, Decimal | int | None], right: tuple[str, Decimal | int | None]
) -> tuple[str, Decimal | int | None]:
    left_kind, left_value = left
    right_kind, right_value = right
    if left_kind == "nan" or right_kind == "nan":
        return "nan", None
    if left_kind == "inf" and right_kind == "inf":
        if int(left_value) != int(right_value):  # type: ignore[arg-type]
            return "nan", None
        return "inf", int(left_value)  # type: ignore[arg-type]
    if left_kind == "inf":
        return "inf", int(left_value)  # type: ignore[arg-type]
    if right_kind == "inf":
        return "inf", int(right_value)  # type: ignore[arg-type]
    return "finite", left_value + right_value  # type: ignore[operator]


def _format_decimal(value: tuple[str, Decimal | int | None]) -> str:
    kind, payload = value
    if kind == "nan":
        return "NAN"
    if kind == "inf":
        return "INF" if int(payload) > 0 else "-INF"
    assert isinstance(payload, Decimal)
    return format(payload, f".{REFERENCE_DISPLAY_DIGITS}e")


def compute_high_precision_reference(matrix: CSRMatrix, dense: np.ndarray) -> np.ndarray:
    """Compute every output with Decimal arithmetic and IEEE special classes."""

    with localcontext() as context:
        context.prec = REFERENCE_PRECISION
        dense_decimal = [[_decimal_value(float(value)) for value in row] for row in dense]
        strings = np.empty((matrix.rows, dense.shape[1]), dtype=f"<U{REFERENCE_DISPLAY_DIGITS + 12}")
        for row in range(matrix.rows):
            start, end = int(matrix.indptr[row]), int(matrix.indptr[row + 1])
            accumulators: list[tuple[str, Decimal | int | None]] = [
                ("finite", Decimal(0)) for _ in range(dense.shape[1])
            ]
            for position in range(start, end):
                coefficient = _decimal_value(float(matrix.data[position]))
                dense_row = dense_decimal[int(matrix.indices[position])]
                for column in range(dense.shape[1]):
                    product = _decimal_product(coefficient, dense_row[column])
                    accumulators[column] = _decimal_add(accumulators[column], product)
            for column, value in enumerate(accumulators):
                strings[row, column] = _format_decimal(value)
        return strings


def _sample_record(
    spec: SampleSpec,
    out_dir: Path,
    indptr: np.ndarray,
    indices: np.ndarray,
    data: np.ndarray,
    dense: np.ndarray,
) -> dict[str, Any]:
    matrix = CSRMatrix.from_arrays(indptr, indices, data, (spec.rows, spec.cols))
    features = matrix.features()
    reference = compute_high_precision_reference(matrix, dense)
    sparse_path = out_dir / f"{spec.name}.npz"
    dense_path = out_dir / f"{spec.name}.dense.npy"
    reference_path = out_dir / f"{spec.name}.ref.npz"
    np.savez_compressed(
        sparse_path,
        indptr=matrix.indptr,
        indices=matrix.indices,
        data=matrix.data,
        shape=np.asarray(matrix.shape, dtype=np.int64),
    )
    np.save(dense_path, dense, allow_pickle=False)
    np.savez_compressed(
        reference_path,
        values=reference,
        precision=np.asarray(REFERENCE_PRECISION, dtype=np.int64),
        display_digits=np.asarray(REFERENCE_DISPLAY_DIGITS, dtype=np.int64),
        format_version=np.asarray(1, dtype=np.int64),
    )
    return {
        "name": spec.name,
        "kind": spec.kind,
        "sparse_file": sparse_path.name,
        "dense_file": dense_path.name,
        "reference_file": reference_path.name,
        "seed": spec.seed,
        "shape": [spec.rows, spec.cols],
        "dense_shape": [spec.cols, spec.dense_cols],
        "target_density": spec.density,
        "actual_density": features["density"],
        "nnz": features["nnz"],
        "empty_rows": features["empty_rows"],
        "empty_row_ratio": features["empty_row_ratio"],
        "duplicate_count": features["duplicate_count"],
        "max_duplicate_extra_per_index": features["max_duplicate_extra_per_index"],
        "explicit_zero_count": features["explicit_zero_count"],
        "has_nonfinite_sparse_data": features["has_nonfinite_data"],
        "has_nonfinite_dense_data": bool(np.any(~np.isfinite(dense))),
        "dtype": features["dtype"],
        "fingerprints": {
            "shape": features["shape_fingerprint"],
            "structure": features["structure_fingerprint"],
            "values": features["value_fingerprint"],
            "dense": array_digest(dense),
            "reference": array_digest(reference),
        },
        "logical_array_digests": {
            "indptr": sha256_hex(canonical_array_bytes(matrix.indptr)),
            "indices": sha256_hex(canonical_array_bytes(matrix.indices)),
            "data": sha256_hex(canonical_array_bytes(matrix.data)),
            "dense": sha256_hex(canonical_array_bytes(dense)),
            "reference": sha256_hex(canonical_array_bytes(reference)),
        },
    }


def generate_samples(
    out_dir: str | os.PathLike[str] = "samples",
    *,
    specs: Iterable[SampleSpec] = SPECS,
) -> dict[str, Any]:
    destination = Path(out_dir)
    destination.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, Any]] = []
    for spec in specs:
        if spec.kind == "regular":
            arrays = _generate_regular(spec)
        elif spec.kind == "degenerate":
            arrays = _generate_degenerate(spec)
        elif spec.kind == "anomaly":
            arrays = _generate_anomaly(spec)
        else:
            raise ValueError(f"unknown sample kind: {spec.kind}")
        records.append(_sample_record(spec, destination, *arrays))
    payload: dict[str, Any] = {
        "format": "spmm-local-samples",
        "format_version": 1,
        "generator": "numpy PCG64 + Decimal high-precision reference",
        "reference_precision": REFERENCE_PRECISION,
        "reference_display_digits": REFERENCE_DISPLAY_DIGITS,
        "samples": records,
    }
    payload["manifest_digest"] = json_digest(payload)
    manifest_path = destination / "manifest.json"
    encoded = json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False).encode("utf-8") + b"\n"
    atomic_write_bytes(manifest_path, encoded)
    return payload


def load_manifest(path: str | os.PathLike[str] = "samples/manifest.json") -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    stored_digest = manifest.get("manifest_digest")
    content = {key: value for key, value in manifest.items() if key != "manifest_digest"}
    if not stored_digest or stored_digest != json_digest(content):
        raise ValueError("sample manifest digest mismatch")
    return manifest

