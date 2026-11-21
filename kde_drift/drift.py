"""Deterministic fixed-seed bootstrap drift adjudication."""

from __future__ import annotations

import math
from typing import Any

import numpy as np

from .canonical import array_digest, digest_object
from .config import get_path
from .data import Dataset
from .errors import numeric_error
from .kde import prepare_column
from .metrics import mmd_squared
from .stop import StopController


def compute_drift(
    dataset: Dataset,
    metrics_payload: dict[str, Any],
    config: dict[str, Any],
    *,
    config_version: str,
    stop: StopController,
) -> dict[str, Any]:
    method = config["imputation"]["method"]
    mmd_config = config["mmd"]
    drift_config = config["drift"]
    threshold = float(drift_config["threshold"])
    alpha = float(drift_config["alpha"])
    resamples = int(drift_config["bootstrap_resamples"])
    seed = int(drift_config["seed"])
    bootstrap_size = int(mmd_config["bootstrap_sample_size"])
    chunk_size = int(mmd_config["chunk_size"])
    metrics_by_name = {item["name"]: item for item in metrics_payload["columns"]}
    columns: list[dict[str, Any]] = []

    if len(dataset.column_names) < 2:
        raise numeric_error(
            "drift adjudication requires at least two columns",
            stage="drift",
            column=dataset.column_names[0] if dataset.column_names else "unknown",
            location="drift.columns",
            details={"column_count": len(dataset.column_names)},
        )

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
        metric = metrics_by_name[column_name]
        score = float(metric["mmd"]["score"])
        bandwidth = float(metric["mmd"]["kernel_parameter"]["comparison_bandwidth"])
        kernel = metric["mmd"]["kernel"]
        pooled = np.sort(np.concatenate([reference_values, current_values]))
        seed_sequence = np.random.SeedSequence(
            [seed, column_index, int(reference_values.size), int(current_values.size), bootstrap_size]
        )
        rng = np.random.Generator(np.random.PCG64(seed_sequence))
        null_scores = np.empty(resamples, dtype=np.float64)
        draw_digest = np.empty(resamples * bootstrap_size * 2, dtype=np.int64)
        cursor = 0
        for resample_index in range(resamples):
            reference_draw = rng.integers(0, pooled.size, size=bootstrap_size, dtype=np.int64)
            current_draw = rng.integers(0, pooled.size, size=bootstrap_size, dtype=np.int64)
            draw_digest[cursor : cursor + bootstrap_size] = reference_draw
            cursor += bootstrap_size
            draw_digest[cursor : cursor + bootstrap_size] = current_draw
            cursor += bootstrap_size
            reference_sample = np.sort(pooled[reference_draw])
            current_sample = np.sort(pooled[current_draw])
            try:
                null_result = mmd_squared(
                    reference_sample,
                    current_sample,
                    kernel=kernel,
                    bandwidth=bandwidth,
                    chunk_size=chunk_size,
                )
            except ValueError as exc:
                raise numeric_error(
                    "bootstrap MMD calculation failed",
                    stage="drift",
                    column=column_name,
                    location=f"drift.column[{column_index}].resample[{resample_index}]",
                    details={"reason": str(exc)},
                ) from exc
            null_scores[resample_index] = float(null_result["score"])
            stop.tick(
                stage="drift",
                location=f"drift.column[{column_index}].resample[{resample_index}]",
            )
        exceedances = int(np.count_nonzero(null_scores >= score))
        p_value = float((1 + exceedances) / (resamples + 1))
        exceeds_threshold = bool(score > threshold)
        significant = bool(p_value <= alpha)
        is_drift = bool(exceeds_threshold and significant)
        columns.append(
            {
                "index": column_index,
                "name": column_name,
                "score": score,
                "score_kind": "unbiased_kernel_mmd_squared",
                "threshold": threshold,
                "alpha": alpha,
                "p_value": p_value,
                "significant": significant,
                "exceeds_threshold": exceeds_threshold,
                "drift": is_drift,
                "decision_rule": "score>threshold AND p_value<=alpha",
                "significance": {
                    "method": "pooled_bootstrap_with_replacement",
                    "resamples": resamples,
                    "null_scores_digest": array_digest(null_scores),
                    "null_scores_min": float(np.min(null_scores)),
                    "null_scores_max": float(np.max(null_scores)),
                    "exceedances": exceedances,
                    "alpha": alpha,
                    "seed": seed,
                    "seed_derivation": {
                        "components": [
                            seed,
                            column_index,
                            int(reference_values.size),
                            int(current_values.size),
                            bootstrap_size,
                        ],
                        "generator": "PCG64(SeedSequence(...))",
                    },
                },
                "resampling": {
                    "reference_index_range": [0, int(reference_values.size) - 1],
                    "current_index_range": [0, int(current_values.size) - 1],
                    "pooled_index_range": [0, int(pooled.size) - 1],
                    "draws_per_resample": bootstrap_size,
                    "total_draws": int(draw_digest.size),
                    "draw_index_min": int(np.min(draw_digest)),
                    "draw_index_max": int(np.max(draw_digest)),
                    "draw_index_digest": array_digest(draw_digest),
                },
                "mmd_dependency": {
                    "kernel": kernel,
                    "comparison_bandwidth": bandwidth,
                    "n_reference": int(reference_values.size),
                    "n_current": int(current_values.size),
                    "bootstrap_sample_size": bootstrap_size,
                },
                "config_version": config_version,
                "decision_config": {
                    "threshold": threshold,
                    "alpha": alpha,
                    "bootstrap_resamples": resamples,
                    "seed": seed,
                },
            }
        )
    drift_names = [column["name"] for column in columns if column["drift"]]
    payload = {
        "stage": "drift",
        "config_version": config_version,
        "columns": columns,
        "summary": {
            "drift_count": len(drift_names),
            "drift_columns": drift_names,
            "column_count": len(columns),
            "bootstrap_columns": len(columns),
            "drift_digest": digest_object(columns),
        },
    }
    return payload
