"""Chunked kernel MMD and trapezoid KL comparison metrics."""

from __future__ import annotations

import math
from typing import Any

import numpy as np

from .config import get_path
from .data import Dataset
from .errors import numeric_error
from .kde import prepare_column
from .numeric import trapezoid
from .stop import StopController


def _select_evenly(values: np.ndarray, limit: int) -> tuple[np.ndarray, np.ndarray]:
    size = int(values.size)
    if size <= limit:
        indices = np.arange(size, dtype=np.int64)
    else:
        indices = np.unique(np.rint(np.linspace(0, size - 1, limit)).astype(np.int64))
    return values[indices], indices


def _kernel_pair_chunk(
    distance: np.ndarray,
    kernel: str,
) -> np.ndarray:
    if kernel == "gaussian":
        return np.exp(-0.5 * distance * distance)
    if kernel == "epanechnikov":
        inside = np.abs(distance) <= 1.0
        result = np.zeros_like(distance, dtype=np.float64)
        result[inside] = 0.75 * (1.0 - distance[inside] * distance[inside])
        return result
    raise ValueError(f"unsupported MMD kernel: {kernel}")


def _kernel_pair_sum(
    left: np.ndarray,
    right: np.ndarray,
    *,
    kernel: str,
    bandwidth: float,
    chunk_size: int,
) -> tuple[float, int]:
    total = 0.0
    compensation = 0.0
    blocks = 0
    for start in range(0, left.size, chunk_size):
        chunk = left[start : start + chunk_size]
        distance = (chunk[:, None] - right[None, :]) / bandwidth
        block = float(np.sum(_kernel_pair_chunk(distance, kernel), dtype=np.float64))
        adjusted = block - compensation
        updated = total + adjusted
        compensation = (updated - total) - adjusted
        total = updated
        blocks += 1
    return total, blocks


def _unbiased_mmd_from_sums(
    sum_xx: float,
    sum_yy: float,
    sum_xy: float,
    n_x: int,
    n_y: int,
) -> tuple[float, float]:
    if n_x < 2 or n_y < 2:
        return math.nan, math.nan
    mmd_squared = (
        (sum_xx - float(n_x)) / float(n_x * (n_x - 1))
        + (sum_yy - float(n_y)) / float(n_y * (n_y - 1))
        - 2.0 * sum_xy / float(n_x * n_y)
    )
    return float(mmd_squared), max(0.0, float(mmd_squared))


def mmd_squared(
    reference: np.ndarray,
    current: np.ndarray,
    *,
    kernel: str,
    bandwidth: float,
    chunk_size: int,
) -> dict[str, Any]:
    if reference.size < 2 or current.size < 2:
        raise ValueError("MMD requires at least two observations per side")
    sum_xx, blocks_xx = _kernel_pair_sum(
        reference,
        reference,
        kernel=kernel,
        bandwidth=bandwidth,
        chunk_size=chunk_size,
    )
    sum_yy, blocks_yy = _kernel_pair_sum(
        current,
        current,
        kernel=kernel,
        bandwidth=bandwidth,
        chunk_size=chunk_size,
    )
    sum_xy, blocks_xy = _kernel_pair_sum(
        reference,
        current,
        kernel=kernel,
        bandwidth=bandwidth,
        chunk_size=chunk_size,
    )
    raw, score = _unbiased_mmd_from_sums(
        sum_xx,
        sum_yy,
        sum_xy,
        int(reference.size),
        int(current.size),
    )
    return {
        "mmd_squared_raw": raw,
        "score": score,
        "kernel_sums": {
            "reference_reference": sum_xx,
            "current_current": sum_yy,
            "reference_current": sum_xy,
        },
        "kernel_blocks": {
            "reference_reference": blocks_xx,
            "current_current": blocks_yy,
            "reference_current": blocks_xy,
        },
    }


def compute_metrics(
    dataset: Dataset,
    bandwidth_payload: dict[str, Any],
    density_payload: dict[str, Any],
    config: dict[str, Any],
    *,
    stop: StopController,
) -> dict[str, Any]:
    method = config["imputation"]["method"]
    mmd_config = config["mmd"]
    kl_epsilon = float(config["kl"]["epsilon"])
    bandwidth_by_name = {item["name"]: item for item in bandwidth_payload["columns"]}
    density_by_name = {item["name"]: item for item in density_payload["columns"]}
    columns: list[dict[str, Any]] = []
    for column_index, column_name in enumerate(dataset.column_names):
        reference_values, _ = prepare_column(
            dataset.reference_features[:, column_index],
            method=method,
            column_name=column_name,
        )
        current_values, _ = prepare_column(
            dataset.current_features[:, column_index],
            method=method,
            column_name=column_name,
        )
        reference_eval, reference_indices = _select_evenly(
            reference_values, int(mmd_config["max_eval_samples"])
        )
        current_eval, current_indices = _select_evenly(
            current_values, int(mmd_config["max_eval_samples"])
        )
        bandwidth_info = bandwidth_by_name[column_name]
        reference_bandwidth = float(bandwidth_info["reference"]["bandwidth"])
        current_bandwidth = float(bandwidth_info["current"]["bandwidth"])
        comparison_bandwidth = math.sqrt(
            0.5 * (reference_bandwidth * reference_bandwidth + current_bandwidth * current_bandwidth)
        ) * float(mmd_config["bandwidth_scale"])
        if not math.isfinite(comparison_bandwidth) or comparison_bandwidth <= 0.0:
            raise numeric_error(
                "MMD comparison bandwidth is not finite and positive",
                stage="metrics",
                column=column_name,
                location="metrics.mmd.comparison_bandwidth",
                details={"comparison_bandwidth": comparison_bandwidth},
            )
        mmd_result = mmd_squared(
            reference_eval,
            current_eval,
            kernel=mmd_config["kernel"],
            bandwidth=comparison_bandwidth,
            chunk_size=int(mmd_config["chunk_size"]),
        )
        if not math.isfinite(float(mmd_result["score"])) or not math.isfinite(
            float(mmd_result["mmd_squared_raw"])
        ):
            raise numeric_error(
                "MMD calculation produced a non-finite score",
                stage="metrics",
                column=column_name,
                location="metrics.mmd.score",
            )
        density_info = density_by_name[column_name]
        grid = np.asarray(density_info["grid"], dtype=np.float64)
        p = np.asarray(density_info["reference_density"], dtype=np.float64)
        q = np.asarray(density_info["current_density"], dtype=np.float64)
        ratio = (p + kl_epsilon) / (q + kl_epsilon)
        if (ratio <= 0).any() or np.isnan(ratio).any() or np.isinf(ratio).any():
            raise numeric_error(
                "KL ratio is non-positive or non-finite",
                stage="metrics",
                column=column_name,
                location="metrics.kl.ratio",
            )
        kl_terms = p * np.log(ratio)
        kl_value = trapezoid(kl_terms, grid)
        if not math.isfinite(kl_value):
            raise numeric_error(
                "KL approximation is not finite",
                stage="metrics",
                column=column_name,
                location="metrics.kl.integral",
            )
        kl_value = max(0.0, float(kl_value))
        columns.append(
            {
                "index": column_index,
                "name": column_name,
                "mmd": {
                    **mmd_result,
                    "estimator": "unbiased_1d_kernel_mmd_squared",
                    "kernel": mmd_config["kernel"],
                    "kernel_parameter": {
                        "comparison_bandwidth": comparison_bandwidth,
                        "reference_bandwidth": reference_bandwidth,
                        "current_bandwidth": current_bandwidth,
                        "bandwidth_scale": float(mmd_config["bandwidth_scale"]),
                    },
                    "sample_sizes": {
                        "n_reference": int(reference_values.size),
                        "n_current": int(current_values.size),
                        "evaluated_n_reference": int(reference_eval.size),
                        "evaluated_n_current": int(current_eval.size),
                    },
                    "evaluated_index_ranges": {
                        "reference": [int(reference_indices[0]), int(reference_indices[-1])],
                        "current": [int(current_indices[0]), int(current_indices[-1])],
                    },
                    "selection_policy": (
                        "all"
                        if reference_values.size <= int(mmd_config["max_eval_samples"])
                        and current_values.size <= int(mmd_config["max_eval_samples"])
                        else "deterministic_evenly_spaced_indices"
                    ),
                    "chunk_size": int(mmd_config["chunk_size"]),
                },
                "kl": {
                    "value": kl_value,
                    "epsilon": kl_epsilon,
                    "direction": "reference_to_current",
                    "formula": "sum(p*log((p+epsilon)/(q+epsilon)))*dx",
                    "sample_sizes": {
                        "n_reference": int(reference_values.size),
                        "n_current": int(current_values.size),
                    },
                    "kernel_parameters": {
                        "kernel": density_info["kernel_parameters"]["kernel"],
                        "reference_bandwidth": reference_bandwidth,
                        "current_bandwidth": current_bandwidth,
                    },
                },
            }
        )
        stop.tick(stage="metrics", location=f"metrics.column[{column_index}]")
    payload = {
        "stage": "metrics",
        "columns": columns,
        "diagnostics": {
            "mmd_estimator": "unbiased_1d_kernel_mmd_squared",
            "kl_estimator": "trapezoid_grid_approximation",
            "all_metrics_finite": all(
                math.isfinite(column["mmd"]["score"]) and math.isfinite(column["kl"]["value"])
                for column in columns
            ),
        },
    }
    return payload
