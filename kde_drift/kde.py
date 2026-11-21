"""Deterministic per-column KDE, bandwidth estimation, and precision envelopes."""

from __future__ import annotations

import math
from typing import Any

import numpy as np

from .canonical import canonical_json_text, digest_object
from .config import get_path
from .data import Dataset
from .errors import WorkbenchError, numeric_error
from .numeric import canonical_column, reduce_chunks, trapezoid
from .stop import StopController


def _json_number(value: float) -> float | str:
    if math.isnan(value):
        return "nan"
    if math.isinf(value):
        return "inf" if value > 0 else "-inf"
    return float(value)


def prepare_column(
    values: np.ndarray,
    *,
    method: str,
    column_name: str,
) -> tuple[np.ndarray, dict[str, Any]]:
    array = np.asarray(values, dtype=np.float64)
    if array.ndim != 1:
        raise numeric_error(
            "column feature values must be one-dimensional",
            stage="bandwidth",
            column=column_name,
            location="kde.prepare_column",
        )
    missing_mask = np.isnan(array)
    observed = array[~missing_mask]
    if observed.size == 0:
        raise numeric_error(
            "column has no observed values after missing-value handling",
            stage="bandwidth",
            column=column_name,
            location="kde.imputation.fill_value",
            details={"missing_count": int(missing_mask.size), "method": method},
        )
    if method == "zero":
        fill_value = 0.0
    elif method == "mean":
        fill_value = float(np.mean(np.sort(observed, kind="mergesort"), dtype=np.float64))
    elif method == "median":
        fill_value = float(np.median(np.sort(observed, kind="mergesort")))
    else:
        raise numeric_error(
            "unsupported missing-value imputation method",
            stage="bandwidth",
            column=column_name,
            location="kde.imputation.method",
            details={"method": method},
        )
    if not math.isfinite(fill_value):
        raise numeric_error(
            "imputation fill value is not finite",
            stage="bandwidth",
            column=column_name,
            location="kde.imputation.fill_value",
            details={"method": method, "fill_value": _json_number(fill_value)},
        )
    filled = array.copy()
    filled[missing_mask] = fill_value
    if np.isnan(filled).any() or np.isinf(filled).any():
        raise numeric_error(
            "numeric values remain non-finite after imputation",
            stage="bandwidth",
            column=column_name,
            location="kde.imputation.reestimate",
        )
    missing_indices = np.flatnonzero(missing_mask)
    diagnostics = {
        "missing_count": int(missing_indices.size),
        "missing_ratio": float(missing_indices.size / max(filled.size, 1)),
        "fill_method": method,
        "fill_value": float(fill_value),
        "reestimated": bool(missing_indices.size),
    }
    return canonical_column(filled), diagnostics


def _sample_std(values: np.ndarray, *, column_name: str) -> float:
    if values.size < 2:
        raise numeric_error(
            "at least two observations are required to estimate a sample standard deviation",
            stage="bandwidth",
            column=column_name,
            location="kde.bandwidth.sample_size",
            details={"sample_size": int(values.size)},
        )
    std = float(np.std(values, ddof=1))
    if not math.isfinite(std):
        raise numeric_error(
            "sample standard deviation is not finite",
            stage="bandwidth",
            column=column_name,
            location="kde.bandwidth.sample_std",
            details={"sample_std": _json_number(std), "sample_size": int(values.size)},
        )
    return std


def _choose_bandwidth(
    *,
    standard_deviation: float,
    sample_size: int,
    rule: str,
    fixed_value: Any,
    column_name: str,
) -> tuple[float, dict[str, Any]]:
    if rule == "fixed":
        try:
            bandwidth = float(fixed_value)
        except (TypeError, ValueError):
            bandwidth = math.nan
        factor: float | str = "fixed"
    else:
        factor = 1.06 if rule == "silverman" else 1.0592
        bandwidth = float(factor) * standard_deviation * (sample_size ** (-0.2))
    if not math.isfinite(bandwidth):
        raise numeric_error(
            "kernel bandwidth parameter is not finite",
            stage="bandwidth",
            column=column_name,
            location="kde.bandwidth.value",
            details={
                "rule": rule,
                "bandwidth": _json_number(bandwidth),
                "sample_std": standard_deviation,
                "sample_size": sample_size,
            },
        )
    if bandwidth <= 0.0:
        raise numeric_error(
            "kernel bandwidth must be strictly positive",
            stage="bandwidth",
            column=column_name,
            location="kde.bandwidth.value",
            details={
                "rule": rule,
                "bandwidth": bandwidth,
                "sample_std": standard_deviation,
                "sample_size": sample_size,
            },
        )
    return bandwidth, {
        "factor": factor,
        "sample_size_power": -0.2 if rule != "fixed" else None,
    }


def compute_bandwidths(
    dataset: Dataset,
    config: dict[str, Any],
    *,
    stop: StopController,
) -> tuple[dict[str, Any], dict[str, dict[str, np.ndarray]]]:
    rule = config["bandwidth"]["rule"]
    fixed_value = config["bandwidth"]["value"]
    kernel = config["kernel"]
    method = config["imputation"]["method"]
    columns: list[dict[str, Any]] = []
    prepared: dict[str, dict[str, np.ndarray]] = {}
    for column_index, column_name in enumerate(dataset.column_names):
        reference_values, reference_diag = prepare_column(
            dataset.reference_features[:, column_index],
            method=method,
            column_name=column_name,
        )
        current_values, current_diag = prepare_column(
            dataset.current_features[:, column_index],
            method=method,
            column_name=column_name,
        )
        reference_std = _sample_std(reference_values, column_name=column_name)
        current_std = _sample_std(current_values, column_name=column_name)
        reference_bandwidth, reference_rule_detail = _choose_bandwidth(
            standard_deviation=reference_std,
            sample_size=int(reference_values.size),
            rule=rule,
            fixed_value=fixed_value,
            column_name=column_name,
        )
        current_bandwidth, current_rule_detail = _choose_bandwidth(
            standard_deviation=current_std,
            sample_size=int(current_values.size),
            rule=rule,
            fixed_value=fixed_value,
            column_name=column_name,
        )
        columns.append(
            {
                "index": column_index,
                "name": column_name,
                "kernel": kernel,
                "bandwidth_rule": rule,
                "rule_detail": {
                    "reference": reference_rule_detail,
                    "current": current_rule_detail,
                },
                "reference": {
                    "sample_size": int(reference_values.size),
                    "sample_std": reference_std,
                    "bandwidth": reference_bandwidth,
                    "imputation": reference_diag,
                },
                "current": {
                    "sample_size": int(current_values.size),
                    "sample_std": current_std,
                    "bandwidth": current_bandwidth,
                    "imputation": current_diag,
                },
            }
        )
        prepared[column_name] = {
            "reference": reference_values,
            "current": current_values,
        }
        stop.tick(stage="bandwidth", location=f"bandwidth.column[{column_index}]")
    payload = {
        "stage": "bandwidth",
        "kernel": kernel,
        "bandwidth_rule": rule,
        "fixed_value": fixed_value,
        "imputation_method": method,
        "columns": columns,
        "diagnostics": {
            "columns_with_missing": [
                item["name"]
                for item in columns
                if item["reference"]["imputation"]["missing_count"]
                or item["current"]["imputation"]["missing_count"]
            ],
            "all_columns_have_positive_finite_bandwidth": True,
        },
    }
    return payload, prepared


def _kernel_values(distance: np.ndarray, kernel: str) -> np.ndarray:
    if kernel == "gaussian":
        return np.exp(-0.5 * distance * distance) / math.sqrt(2.0 * math.pi)
    if kernel == "epanechnikov":
        inside = np.abs(distance) <= 1.0
        weights = np.zeros_like(distance, dtype=np.float64)
        weights[inside] = 0.75 * (1.0 - distance[inside] * distance[inside])
        return weights
    raise ValueError(f"unsupported kernel: {kernel}")


def _pairwise_depth(size: int) -> int:
    if size <= 1:
        return 1
    return int(math.ceil(math.log2(size))) + 1


def _kde_curve(
    values: np.ndarray,
    grid: np.ndarray,
    bandwidth: float,
    *,
    kernel: str,
    chunk_size: int,
    summation: str,
    column_name: str,
    stop: StopController,
    unit_prefix: str,
) -> tuple[np.ndarray, dict[str, Any]]:
    if not math.isfinite(bandwidth) or bandwidth <= 0.0:
        raise numeric_error(
            "kernel bandwidth must be finite and positive",
            stage="density",
            column=column_name,
            location=f"{unit_prefix}.bandwidth",
            details={"bandwidth": _json_number(bandwidth)},
        )
    if values.size < 1:
        raise numeric_error(
            "kernel density estimate requires at least one sample",
            stage="density",
            column=column_name,
            location=f"{unit_prefix}.sample",
        )
    if np.isnan(values).any() or np.isinf(values).any():
        raise numeric_error(
            "sample values are non-finite at density evaluation",
            stage="density",
            column=column_name,
            location=f"{unit_prefix}.sample",
        )
    if np.isnan(grid).any() or np.isinf(grid).any():
        raise numeric_error(
            "query grid contains non-finite values",
            stage="density",
            column=column_name,
            location=f"{unit_prefix}.grid",
        )

    chunks: list[np.ndarray] = []
    absolute_terms = np.zeros(grid.shape[0], dtype=np.float64)
    high_precision = np.zeros(grid.shape[0], dtype=np.longdouble)
    chunk_count = 0
    for start in range(0, values.size, chunk_size):
        sample_chunk = values[start : start + chunk_size]
        distance = (grid[:, None] - sample_chunk[None, :]) / bandwidth
        weights = _kernel_values(distance, kernel) / bandwidth
        chunk_sum = np.sum(weights, axis=1, dtype=np.float64)
        if np.isnan(chunk_sum).any() or np.isinf(chunk_sum).any():
            raise numeric_error(
                "non-finite kernel contribution encountered",
                stage="density",
                column=column_name,
                location=f"{unit_prefix}.chunk[{chunk_count}]",
                details={"chunk_start": start, "chunk_size": int(sample_chunk.size)},
            )
        chunks.append(chunk_sum)
        absolute_terms += np.sum(np.abs(weights), axis=1, dtype=np.float64)
        high_precision += np.sum(
            weights.astype(np.longdouble), axis=1, dtype=np.longdouble
        )
        chunk_count += 1
        stop.tick(
            stage="density",
            location=f"{unit_prefix}.chunk[{chunk_count - 1}]",
            next_unit=chunk_count,
        )

    if not chunks:
        raise numeric_error(
            "density accumulator is empty",
            stage="density",
            column=column_name,
            location=f"{unit_prefix}.accumulator",
        )
    reduced = reduce_chunks(chunks, summation)
    if reduced.shape != grid.shape or np.isnan(reduced).any() or np.isinf(reduced).any():
        raise numeric_error(
            "density reduction produced an invalid curve",
            stage="density",
            column=column_name,
            location=f"{unit_prefix}.reduction",
        )
    unnormalized = reduced / float(values.size)
    integral = trapezoid(unnormalized, grid)
    if not math.isfinite(integral) or integral <= 0.0:
        raise numeric_error(
            "density curve has a non-positive or non-finite area",
            stage="density",
            column=column_name,
            location=f"{unit_prefix}.normalization",
            details={"integral": _json_number(integral)},
        )
    curve = unnormalized / integral
    high_curve = (high_precision / np.longdouble(values.size)) / np.longdouble(integral)
    if np.isnan(curve).any() or np.isinf(curve).any():
        raise numeric_error(
            "normalized density curve contains non-finite values",
            stage="density",
            column=column_name,
            location=f"{unit_prefix}.normalization",
        )
    observed_error = float(np.max(np.abs(curve - np.asarray(high_curve, dtype=np.float64))))
    normalized_absolute_terms = absolute_terms / float(values.size) / integral
    unit_roundoff = float(np.finfo(np.float64).eps) / 2.0
    inner_depth = _pairwise_depth(int(min(chunk_size, values.size)))
    outer_depth = _pairwise_depth(chunk_count)
    if summation == "kahan":
        theoretical = unit_roundoff * (inner_depth + 2) * normalized_absolute_terms
    else:
        theoretical = unit_roundoff * (inner_depth + outer_depth) * normalized_absolute_terms
    theoretical_bound = float(np.max(theoretical)) if theoretical.size else 0.0
    verified_bound = max(theoretical_bound, observed_error)
    high_precision_improved = bool(
        np.finfo(np.longdouble).eps < np.finfo(np.float64).eps
        and np.dtype(np.longdouble).itemsize > np.dtype(np.float64).itemsize
    )
    diagnostics = {
        "summation": summation,
        "chunk_size": int(chunk_size),
        "chunks": chunk_count,
        "sample_size": int(values.size),
        "bandwidth": float(bandwidth),
        "kernel": kernel,
        "theoretical_upper_bound": theoretical_bound,
        "observed_high_precision_error": observed_error,
        "verified_upper_bound": verified_bound,
        "meets_theoretical_bound": bool(observed_error <= theoretical_bound * (1.0 + 1e-12) + 1e-300),
        "high_precision_reference": "longdouble" if high_precision_improved else "float64",
        "integral_before_renormalization": float(integral),
        "integral_after_renormalization": trapezoid(curve, grid),
    }
    return curve, diagnostics


def compute_densities(
    dataset: Dataset,
    bandwidth_payload: dict[str, Any],
    config: dict[str, Any],
    *,
    stop: StopController,
) -> tuple[dict[str, Any], dict[str, np.ndarray]]:
    method = config["imputation"]["method"]
    kernel = config["kernel"]
    chunk_size = int(config["chunk_size"])
    summation = config["summation"]
    grid_points = int(config["grid"]["points"])
    columns: list[dict[str, Any]] = []
    grids: dict[str, np.ndarray] = {}
    bandwidth_by_name = {item["name"]: item for item in bandwidth_payload["columns"]}
    for column_index, column_name in enumerate(dataset.column_names):
        reference_values, reference_diag = prepare_column(
            dataset.reference_features[:, column_index],
            method=method,
            column_name=column_name,
        )
        current_values, current_diag = prepare_column(
            dataset.current_features[:, column_index],
            method=method,
            column_name=column_name,
        )
        union = np.concatenate([reference_values, current_values])
        lower = float(np.min(union))
        upper = float(np.max(union))
        if not math.isfinite(lower) or not math.isfinite(upper) or upper <= lower:
            raise numeric_error(
                "query grid range is invalid or degenerate",
                stage="density",
                column=column_name,
                location="kde.grid",
                details={"lower": _json_number(lower), "upper": _json_number(upper)},
            )
        grid = np.linspace(lower, upper, grid_points, dtype=np.float64)
        bandwidth_info = bandwidth_by_name[column_name]
        reference_bandwidth = float(bandwidth_info["reference"]["bandwidth"])
        current_bandwidth = float(bandwidth_info["current"]["bandwidth"])
        reference_curve, reference_diagnostics = _kde_curve(
            reference_values,
            grid,
            reference_bandwidth,
            kernel=kernel,
            chunk_size=chunk_size,
            summation=summation,
            column_name=column_name,
            stop=stop,
            unit_prefix=f"density.column[{column_index}].reference",
        )
        current_curve, current_diagnostics = _kde_curve(
            current_values,
            grid,
            current_bandwidth,
            kernel=kernel,
            chunk_size=chunk_size,
            summation=summation,
            column_name=column_name,
            stop=stop,
            unit_prefix=f"density.column[{column_index}].current",
        )
        columns.append(
            {
                "index": column_index,
                "name": column_name,
                "grid": [float(value) for value in grid.tolist()],
                "reference_density": [float(value) for value in reference_curve.tolist()],
                "current_density": [float(value) for value in current_curve.tolist()],
                "reference_diagnostics": reference_diagnostics,
                "current_diagnostics": current_diagnostics,
                "imputation": {
                    "reference": reference_diag,
                    "current": current_diag,
                },
                "kernel_parameters": {
                    "kernel": kernel,
                    "reference_bandwidth": reference_bandwidth,
                    "current_bandwidth": current_bandwidth,
                },
            }
        )
        grids[column_name] = grid
    payload = {
        "stage": "density",
        "kernel": kernel,
        "grid_points": grid_points,
        "chunk_size": chunk_size,
        "summation": summation,
        "columns": columns,
        "diagnostics": {
            "curve_digest": "",
            "all_curves_finite": True,
            "all_curves_unit_area": all(
                abs(
                    trapezoid(
                        np.asarray(column["reference_density"], dtype=np.float64),
                        np.asarray(column["grid"], dtype=np.float64),
                    )
                    - 1.0
                )
                < 1e-10
                and abs(
                    trapezoid(
                        np.asarray(column["current_density"], dtype=np.float64),
                        np.asarray(column["grid"], dtype=np.float64),
                    )
                    - 1.0
                )
                < 1e-10
                for column in columns
            ),
            "columns_with_imputation": [
                column["name"]
                for column in columns
                if column["imputation"]["reference"]["missing_count"]
                or column["imputation"]["current"]["missing_count"]
            ],
        },
    }
    payload["diagnostics"]["curve_digest"] = digest_object(columns)
    return payload, grids
