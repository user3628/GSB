"""Byte-reproducible local NPZ sample generation for both scale tiers."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import numpy as np

from .canonical import atomic_write_json, write_deterministic_npz


BASE_SEED = 20261121
SCALE_SPECS = {
    "small": {"rows": 1500, "columns": 8, "missing_columns": [2, 6], "scale_id": 11},
    "large": {"rows": 15000, "columns": 24, "missing_columns": [2, 6, 11, 18], "scale_id": 29},
}

_KERNEL_LIKE_KINDS = [
    "normal",
    "uniform",
    "lognormal",
    "student_t",
    "normal_wide",
    "gamma",
    "exponential",
    "student_t4",
    "normal",
    "beta",
    "normal_wide",
    "lognormal",
]


def _column_kind(index: int) -> str:
    return _KERNEL_LIKE_KINDS[index % len(_KERNEL_LIKE_KINDS)]


def _column_scale(index: int) -> float:
    return 0.8 + 0.07 * (index % 7)


def _draw_column(kind: str, size: int, rng: np.random.Generator, index: int) -> np.ndarray:
    scale = _column_scale(index)
    if kind == "normal":
        return rng.normal(0.0, scale, size=size)
    if kind == "normal_wide":
        return rng.normal(0.0, scale * 2.1, size=size)
    if kind == "uniform":
        return rng.uniform(-2.0 * scale, 2.0 * scale, size=size)
    if kind == "lognormal":
        return rng.lognormal(mean=0.0, sigma=0.65, size=size) - 1.0
    if kind == "student_t":
        return rng.standard_t(df=3.0, size=size) * scale
    if kind == "student_t4":
        return rng.standard_t(df=4.5, size=size) * scale
    if kind == "gamma":
        return rng.gamma(shape=2.2, scale=scale, size=size) - 2.2 * scale
    if kind == "exponential":
        return rng.exponential(scale=scale, size=size) - scale
    if kind == "beta":
        return rng.beta(a=2.0, b=5.0, size=size) * 4.0 - 2.0
    raise ValueError(f"unknown generator kind: {kind}")


def _shift_for_column(index: int) -> float:
    if index % 4 == 2:
        return 1.05 * _column_scale(index)
    if index % 7 == 3:
        return 0.80 * _column_scale(index)
    return 0.0


def _inject_missing(
    matrix: np.ndarray,
    missing_columns: list[int],
    *,
    count_per_column: int,
    seed: int,
) -> np.ndarray:
    result = np.array(matrix, dtype=np.float64, copy=True)
    for column in missing_columns:
        rng = np.random.Generator(np.random.PCG64(seed + 1009 * column))
        rows = rng.choice(result.shape[0], size=count_per_column, replace=False)
        result[rows, column] = np.nan
    return result


def _expected_output(scale: str, config_points: int = 128) -> dict[str, Any]:
    return {
        "aggregation": "per_column",
        "density": {
            "grid": "common_linspace_per_column_over_union_range",
            "points": config_points,
            "normalization": "trapezoid_unit_area",
            "reduction": ["kahan", "pairwise"],
            "precision_reference": "numpy_longdouble_when_more_precise_than_float64",
        },
        "bandwidth": {
            "sample_std_ddof": 1,
            "sample_size_power": "-1/5",
            "silverman": "1.06*std*n**(-1/5)",
            "scott": "1.0592*std*n**(-1/5)",
            "fixed": "explicit_user_value",
        },
        "mmd": {
            "estimator": "unbiased_one_dimensional_kernel_mmd_squared",
            "sample_size_reported": ["n_reference", "n_current", "evaluated_n_reference", "evaluated_n_current"],
            "kernel_parameter_reported": ["kernel", "comparison_bandwidth"],
            "sample_size_policy": "all rows when <=2048; deterministic evenly spaced indices above 2048",
        },
        "kl": {
            "estimator": "trapezoid_grid_approximation",
            "formula": "sum(p*log((p+epsilon)/(q+epsilon)))*dx",
            "epsilon": 1e-12,
            "direction": "reference_to_current",
        },
        "drift": {
            "rule": "score>threshold AND p_value<=alpha",
            "minimum_bootstrap_columns": 2,
            "null": "fixed_seed_pooled_bootstrap_with_replacement",
            "score": "unbiased_kernel_mmd_squared",
        },
    }


def _generate_tier(spec: dict[str, Any], scale: str, seed: int) -> tuple[
    np.ndarray, np.ndarray, np.ndarray, np.ndarray, list[str], list[str]
]:
    rows = int(spec["rows"])
    columns = int(spec["columns"])
    scale_id = int(spec["scale_id"])
    kinds = [_column_kind(index) for index in range(columns)]
    column_names = [f"{kind}_{index:02d}" for index, kind in enumerate(kinds)]

    reference = np.empty((rows, columns), dtype=np.float64)
    current = np.empty((rows, columns), dtype=np.float64)
    for column, kind in enumerate(kinds):
        reference_rng = np.random.Generator(np.random.PCG64(seed + scale_id * 100003 + column * 17))
        current_rng = np.random.Generator(np.random.PCG64(seed + scale_id * 200003 + column * 19 + 1))
        reference[:, column] = _draw_column(kind, rows, reference_rng, column)
        current[:, column] = _draw_column(kind, rows, current_rng, column)
        current[:, column] += _shift_for_column(column)

    missing_columns = list(spec["missing_columns"])
    missing_per_column = max(12, rows // 90)
    reference = _inject_missing(
        reference,
        missing_columns,
        count_per_column=missing_per_column,
        seed=seed + scale_id * 31 + 7,
    )
    current = _inject_missing(
        current,
        missing_columns,
        count_per_column=missing_per_column,
        seed=seed + scale_id * 47 + 13,
    )
    reference_labels = np.full(rows, "reference", dtype="<U9")
    current_labels = np.full(rows, "current", dtype="<U9")
    return reference, current, reference_labels, current_labels, column_names, kinds


def _declaration(
    *,
    scale: str,
    spec: dict[str, Any],
    seed: int,
    column_names: list[str],
    kinds: list[str],
) -> dict[str, Any]:
    rows = int(spec["rows"])
    columns = int(spec["columns"])
    missing_columns = list(spec["missing_columns"])
    return {
        "schema_version": 1,
        "dataset": scale,
        "seed": seed,
        "generator": "kde_drift.generator",
        "row_count": rows,
        "column_count": columns,
        "column_names": column_names,
        "column_kinds": kinds,
        "missing_columns": missing_columns,
        "missing_column_names": [column_names[index] for index in missing_columns],
        "missing_count_per_column": max(12, rows // 90),
        "reference": {
            "path": "reference.npz",
            "rows": rows,
            "columns": columns,
            "column_names": column_names,
            "features_key": "features",
            "labels_key": "source_labels",
        },
        "current": {
            "path": "current.npz",
            "rows": rows,
            "columns": columns,
            "column_names": column_names,
            "features_key": "features",
            "labels_key": "source_labels",
        },
        "expected_output": _expected_output(scale),
    }


def generate_dataset(
    output_dir: str | Path,
    *,
    seed: int = BASE_SEED,
    scales: tuple[str, ...] = ("small", "large"),
) -> dict[str, Any]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    index: dict[str, Any] = {
        "schema_version": 1,
        "seed": seed,
        "datasets": {},
    }
    for scale in scales:
        if scale not in SCALE_SPECS:
            raise ValueError(f"unknown scale: {scale}")
        spec = copy.deepcopy(SCALE_SPECS[scale])
        reference, current, ref_labels, cur_labels, names, kinds = _generate_tier(spec, scale, seed)
        target = output / scale
        target.mkdir(parents=True, exist_ok=True)
        write_deterministic_npz(
            target / "reference.npz",
            {"features": reference, "source_labels": ref_labels},
        )
        write_deterministic_npz(
            target / "current.npz",
            {"features": current, "source_labels": cur_labels},
        )
        declaration = _declaration(
            scale=scale,
            spec=spec,
            seed=seed,
            column_names=names,
            kinds=kinds,
        )
        atomic_write_json(target / "declaration.json", declaration)
        index["datasets"][scale] = {
            "declaration": f"{scale}/declaration.json",
            "rows": spec["rows"],
            "columns": spec["columns"],
            "missing_columns": spec["missing_columns"],
        }
    atomic_write_json(output / "index.json", index)
    return index
