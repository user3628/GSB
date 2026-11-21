"""Deterministic reduction and density helpers."""

from __future__ import annotations

from collections.abc import Iterable

import numpy as np


def kahan_reduce(values: Iterable[np.ndarray]) -> np.ndarray:
    """Vectorized Kahan compensated accumulation in iterable order."""

    iterator = iter(values)
    try:
        total = np.array(next(iterator), dtype=np.float64, copy=True)
    except StopIteration:
        return np.empty(0, dtype=np.float64)
    compensation = np.zeros_like(total)
    for value in iterator:
        item = np.asarray(value, dtype=np.float64)
        adjusted = item - compensation
        updated = total + adjusted
        compensation = (updated - total) - adjusted
        total = updated
    return total


def pairwise_reduce(values: Iterable[np.ndarray]) -> np.ndarray:
    """Pairwise reduction in iterable order, independent of NumPy's internals."""

    level = [np.array(value, dtype=np.float64, copy=True) for value in values]
    if not level:
        return np.empty(0, dtype=np.float64)
    while len(level) > 1:
        next_level: list[np.ndarray] = []
        for index in range(0, len(level) - 1, 2):
            next_level.append(level[index] + level[index + 1])
        if len(level) % 2:
            next_level.append(level[-1])
        level = next_level
    return level[0]


def reduce_chunks(chunks: list[np.ndarray], method: str) -> np.ndarray:
    if method == "kahan":
        return kahan_reduce(chunks)
    if method == "pairwise":
        return pairwise_reduce(chunks)
    raise ValueError(f"unsupported reduction method: {method}")


def trapezoid(y: np.ndarray, x: np.ndarray) -> float:
    """NumPy-1.26-compatible trapezoidal integral for 1-D arrays."""

    y_values = np.asarray(y, dtype=np.float64)
    x_values = np.asarray(x, dtype=np.float64)
    if y_values.shape != x_values.shape or y_values.ndim != 1:
        raise ValueError("trapezoid requires equally shaped one-dimensional arrays")
    if y_values.size < 2:
        return 0.0
    return float(np.sum((y_values[:-1] + y_values[1:]) * 0.5 * np.diff(x_values)))


def canonical_column(values: np.ndarray) -> np.ndarray:
    """Sort a finite, imputed column into a platform-stable canonical order."""

    return np.sort(np.asarray(values, dtype=np.float64), kind="mergesort")
