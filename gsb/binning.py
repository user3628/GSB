"""Deterministic quantile binning shared by training and batch prediction."""

from __future__ import annotations

import base64
import math
from dataclasses import dataclass
from typing import Any

import numpy as np

from .errors import WorkbenchError
from .util import sha256_bytes, sha256_json


def _quantile_sorted(values: np.ndarray, q: float) -> float:
    if values.size == 0:
        return 0.0
    position = (values.size - 1) * q
    lower = int(math.floor(position))
    upper = min(lower + 1, values.size - 1)
    fraction = position - lower
    result = float(values[lower] * (1.0 - fraction) + values[upper] * fraction)
    return 0.0 if result == 0.0 else result


def _normalize_edges(edges: list[float]) -> list[float]:
    output: list[float] = []
    previous: float | None = None
    for value in edges:
        value = 0.0 if value == 0.0 else float(value)
        if previous is None or value > previous:
            output.append(value)
            previous = value
    return output


def _encode_matrix(binned: np.ndarray) -> str:
    return base64.b64encode(np.asarray(binned, dtype="<u2").tobytes(order="C")).decode("ascii")


def decode_matrix(encoded: str, rows: int, cols: int) -> np.ndarray:
    data = base64.b64decode(encoded.encode("ascii"), validate=True)
    expected = rows * cols * 2
    if len(data) != expected:
        raise WorkbenchError(
            "E_INVALID_STAGE",
            "binned matrix payload has invalid length",
            {"expected_bytes": expected, "actual_bytes": len(data)},
        )
    return np.frombuffer(data, dtype="<u2").reshape(rows, cols).astype(np.uint16, copy=True)


@dataclass(frozen=True)
class BinSpec:
    edges: tuple[tuple[float, ...], ...]
    bins: int

    @property
    def missing_bin(self) -> int:
        return 0

    @property
    def histogram_bins(self) -> int:
        return self.bins + 1

    def transform(self, X: np.ndarray) -> np.ndarray:
        X = np.asarray(X, dtype=np.float64)
        if X.ndim != 2 or X.shape[1] != len(self.edges):
            raise WorkbenchError(
                "E_INVALID_PARAM",
                "feature matrix does not match bin specification",
                {"shape": list(X.shape), "expected_columns": len(self.edges)},
            )
        finite_mask = np.isfinite(X)
        invalid = np.argwhere(np.isinf(X))
        if invalid.size:
            row, column = map(int, invalid[0])
            raise WorkbenchError(
                "E_NUMERIC",
                "feature contains Inf during binning",
                {"sample_index": row, "column": column, "stage": "binning"},
            )
        binned = np.zeros(X.shape, dtype=np.uint16)
        for column, column_edges in enumerate(self.edges):
            if column_edges:
                values = X[:, column]
                bins = np.searchsorted(np.asarray(column_edges, dtype=np.float64), values, side="right") + 1
                bins = np.where(finite_mask[:, column], bins, 0)
                binned[:, column] = bins.astype(np.uint16)
            else:
                binned[:, column] = np.where(finite_mask[:, column], 1, 0).astype(np.uint16)
        return binned

    def to_payload(self) -> dict[str, Any]:
        return {"bins": self.bins, "edges": [list(column) for column in self.edges], "missing_bin": self.missing_bin}

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "BinSpec":
        edges = tuple(tuple(float(value) for value in column) for column in payload.get("edges", []))
        return cls(edges=edges, bins=int(payload["bins"]))


def fit_bins(X: np.ndarray, bins: int) -> BinSpec:
    if not isinstance(bins, int) or bins <= 0:
        raise WorkbenchError("E_INVALID_PARAM", "bins must be a positive integer", {"bins": bins})
    X = np.asarray(X, dtype=np.float64)
    if X.ndim != 2:
        raise WorkbenchError("E_INVALID_PARAM", "X must be two-dimensional", {"shape": list(X.shape)})
    invalid = np.argwhere(np.isinf(X))
    if invalid.size:
        row, column = map(int, invalid[0])
        raise WorkbenchError(
            "E_NUMERIC",
            "feature contains Inf during histogram binning",
            {"sample_index": row, "column": column, "stage": "binning"},
        )
    all_edges: list[tuple[float, ...]] = []
    for column in range(X.shape[1]):
        values = np.sort(X[np.isfinite(X[:, column]), column], kind="mergesort")
        if values.size == 0 or bins == 1:
            all_edges.append(tuple())
            continue
        candidates = [_quantile_sorted(values, index / float(bins)) for index in range(1, bins)]
        all_edges.append(tuple(_normalize_edges(candidates)))
    return BinSpec(edges=tuple(all_edges), bins=bins)


def build_binning_payload(X: np.ndarray, bins: int) -> tuple[dict[str, Any], BinSpec, np.ndarray]:
    spec = fit_bins(X, bins)
    binned = spec.transform(X)
    matrix_sha = sha256_bytes(binned.tobytes(order="C"))
    payload = {
        "schema": 1,
        "algorithm": "deterministic-quantile-bins-v1",
        "rows": int(X.shape[0]),
        "cols": int(X.shape[1]),
        "spec": spec.to_payload(),
        "binned_matrix_b64": _encode_matrix(binned),
        "binned_matrix_sha256": matrix_sha,
        "missing_counts": [int(np.isnan(X[:, column]).sum()) for column in range(X.shape[1])],
        "bin_counts": [int(len(column_edges)) + 1 for column_edges in spec.edges],
        "content_digest": sha256_json({"spec": spec.to_payload(), "binned_matrix_sha256": matrix_sha}),
    }
    return payload, spec, binned


def binned_from_payload(payload: dict[str, Any]) -> tuple[BinSpec, np.ndarray]:
    spec = BinSpec.from_payload(payload["spec"])
    binned = decode_matrix(payload["binned_matrix_b64"], int(payload["rows"]), int(payload["cols"]))
    if sha256_bytes(binned.tobytes(order="C")) != payload["binned_matrix_sha256"]:
        raise WorkbenchError("E_INVALID_STAGE", "binned matrix digest does not match payload", {"stage": "binning"})
    return spec, binned
