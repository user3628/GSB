from __future__ import annotations

import hashlib
import math
import os
from typing import Any

import numpy as np

from .dataset import ShardSpec, Table, is_missing, load_table, parse_number
from .errors import WorkbenchError
from .util import (
    digest_object,
    sha256_bytes,
    stable_float,
    stable_matrix_digest,
    stable_vector_digest,
    to_jsonable,
)


def _numeric_fill_value(column: dict[str, Any], config: dict[str, Any]) -> tuple[str, float]:
    method = config["encoding"]["missing"]["numeric"]
    if method == "mean":
        return method, float(column.get("mean", 0.0))
    if method == "median":
        return method, float(column.get("median", 0.0))
    if method == "mode":
        return method, float(column.get("mode", 0.0))
    if method == "zero":
        return method, 0.0
    constant = config["encoding"]["missing"]["constant"]
    try:
        return method, float(str(constant))
    except ValueError as exc:
        raise WorkbenchError(
            "E_CONFIG_INVALID",
            "Numeric constant fill requires a numeric encoding.missing.constant",
            {"constant": constant, "column": column.get("name")},
        ) from exc


def _categorical_fill_value(column: dict[str, Any], config: dict[str, Any]) -> tuple[str, str]:
    method = config["encoding"]["missing"]["categorical"]
    if method == "mode":
        value = column.get("mode")
        if value is None:
            value = config["encoding"]["missing"]["constant"]
        return method, str(value)
    return method, str(config["encoding"]["missing"]["constant"])


def resolve_fill_plan(tier_schema: dict[str, Any], config: dict[str, Any]) -> list[dict[str, Any]]:
    plan: list[dict[str, Any]] = []
    for column in tier_schema["source_columns"]:
        if column["dtype"] == "label":
            continue
        if column["dtype"] == "numeric":
            method, value = _numeric_fill_value(column, config)
            plan.append(
                {
                    "index": column["index"],
                    "name": column["name"],
                    "dtype": "numeric",
                    "method": method,
                    "value": stable_float(value),
                }
            )
        elif column["dtype"] == "categorical":
            method, value = _categorical_fill_value(column, config)
            plan.append(
                {
                    "index": column["index"],
                    "name": column["name"],
                    "dtype": "categorical",
                    "method": method,
                    "value": value,
                }
            )
    return plan


def _target_encode(
    values: list[str],
    labels: np.ndarray,
    *,
    column_name: str,
    column_index: int,
    config: dict[str, Any],
) -> tuple[np.ndarray, dict[str, Any]]:
    target_config = config["encoding"]["target"]
    folds = int(target_config["folds"])
    smoothing = float(target_config["smoothing"])
    fold_seed = int(target_config["fold_seed"])
    count = len(values)
    if folds > count:
        raise WorkbenchError(
            "E_FOLD_INVALID",
            "Target encoding requested more folds than rows",
            {
                "column": column_name,
                "column_index": column_index,
                "folds": folds,
                "rows": count,
            },
        )
    rng = np.random.default_rng(fold_seed)
    permutation = rng.permutation(count)
    fold_indices = np.array_split(permutation, folds)
    fold_of_row = np.full(count, -1, dtype=np.int64)
    for fold_index, indices in enumerate(fold_indices):
        fold_of_row[indices] = fold_index

    categories = sorted(set(values))
    category_index = {value: index for index, value in enumerate(categories)}
    encoded = np.zeros(count, dtype=np.float64)
    diagnostics_folds: list[dict[str, Any]] = []
    category_rows: dict[str, dict[str, Any]] = {
        value: {
            "value": value,
            "folds": [],
        }
        for value in categories
    }

    for fold_index, validation_indices in enumerate(fold_indices):
        validation_mask = np.zeros(count, dtype=bool)
        validation_mask[validation_indices] = True
        train_indices = np.nonzero(~validation_mask)[0]
        train_labels = labels[train_indices]
        classes, class_counts = np.unique(train_labels, return_counts=True)
        if len(classes) < 2:
            raise WorkbenchError(
                "E_FOLD_INVALID",
                "Target encoding fold has only one class in its training partition",
                {
                    "column": column_name,
                    "column_index": column_index,
                    "fold": fold_index,
                    "classes": [stable_float(float(value)) for value in classes.tolist()],
                    "class_counts": [int(value) for value in class_counts.tolist()],
                    "training_rows": int(train_indices.size),
                },
            )
        prior = float(np.mean(train_labels))
        category_sums = np.zeros(len(categories), dtype=np.float64)
        category_counts = np.zeros(len(categories), dtype=np.int64)
        for row_index in train_indices:
            cat_index = category_index[values[int(row_index)]]
            category_counts[cat_index] += 1
            category_sums[cat_index] += float(labels[int(row_index)])
        encoded_by_category = np.empty(len(categories), dtype=np.float64)
        for cat_index, category in enumerate(categories):
            count_value = int(category_counts[cat_index])
            if count_value == 0:
                estimate = prior
            else:
                estimate = (float(category_sums[cat_index]) + smoothing * prior) / (
                    float(count_value) + smoothing
                )
            encoded_by_category[cat_index] = estimate
            category_rows[category]["folds"].append(
                {
                    "fold": fold_index,
                    "count": count_value,
                    "sum_label": stable_float(float(category_sums[cat_index])),
                    "seen_in_training": count_value > 0,
                    "encoded": stable_float(estimate),
                    "fallback": count_value == 0,
                }
            )
        for row_index in validation_indices:
            cat_index = category_index[values[int(row_index)]]
            encoded[int(row_index)] = encoded_by_category[cat_index]
        diagnostics_folds.append(
            {
                "fold": fold_index,
                "validation_rows": int(validation_indices.size),
                "training_rows": int(train_indices.size),
                "class_counts": {
                    str(int(value)): int(count_value)
                    for value, count_value in zip(classes.tolist(), class_counts.tolist())
                },
                "training_global_mean": stable_float(prior),
                "fallback_global_mean": stable_float(prior),
            }
        )

    diagnostics = {
        "column": column_name,
        "column_index": column_index,
        "method": "out_of_fold_smoothed_mean",
        "fold_assignment_seed": fold_seed,
        "fold_count": folds,
        "smoothing": smoothing,
        "fallback": "current_training_fold_global_mean",
        "fold_assignments": [int(value) for value in fold_of_row.tolist()],
        "folds": diagnostics_folds,
        "categories": [category_rows[value] for value in categories],
    }
    return encoded, diagnostics


def _hash_bucket(column_name: str, value: str, buckets: int) -> int:
    digest = hashlib.blake2b(
        f"{column_name}\x1f{value}".encode("utf-8"),
        digest_size=8,
        person=b"featwb",
    ).digest()
    return int.from_bytes(digest, "little", signed=False) % buckets


def _global_categorical_categories(
    column: dict[str, Any],
    fill_value: str,
    observed_values: list[str],
) -> list[str]:
    categories = set(str(value) for value in column.get("domain", []))
    if int(column.get("missing_count", 0)) > 0:
        categories.add(str(fill_value))
    categories.update(observed_values)
    return sorted(categories)


def encode_table(
    table: Table,
    tier_schema: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    strategy = config["encoding"]["strategy"]
    fill_plan = resolve_fill_plan(tier_schema, config)
    fill_by_index = {entry["index"]: entry for entry in fill_plan}
    columns_by_index = {
        int(column["index"]): column for column in tier_schema["source_columns"]
    }
    label_index = int(tier_schema["label_column"]["index"])
    labels = np.asarray(
        [int(float(table.rows[row][label_index])) for row in range(table.row_count)],
        dtype=np.int64,
    )
    feature_blocks: list[np.ndarray] = []
    feature_names: list[str] = []
    mapping: list[dict[str, Any]] = []
    source_domains: list[dict[str, Any]] = []
    target_diagnostics: list[dict[str, Any]] = []
    fill_summary: list[dict[str, Any]] = []

    for column_index in range(table.column_count):
        column = columns_by_index[column_index]
        column_name = column["name"]
        dtype = column["dtype"]
        if dtype == "label":
            continue
        raw_values = [table.rows[row][column_index] for row in range(table.row_count)]
        if dtype == "numeric":
            fill = fill_by_index[column_index]
            fill_summary.append(
                {
                    "column": column_name,
                    "column_index": column_index,
                    "value": fill["value"],
                    "method": fill["method"],
                    "filled_count": sum(1 for value in raw_values if is_missing(value)),
                }
            )
            numeric_values = np.empty(table.row_count, dtype=np.float64)
            for row_index, value in enumerate(raw_values):
                if is_missing(value):
                    numeric_values[row_index] = float(fill["value"])
                else:
                    numeric_values[row_index] = parse_number(
                        value,
                        column=column_name,
                        path=table.path,
                        index=column_index,
                        row_number=row_index + 2,
                    )
            feature_blocks.append(numeric_values.reshape(-1, 1))
            feature_names.append(column_name)
            mapping.append(
                {
                    "feature": column_name,
                    "source_column": column_name,
                    "source_index": column_index,
                    "kind": "numeric",
                    "value": None,
                }
            )
            source_domains.append(
                {
                    "column": column_name,
                    "column_index": column_index,
                    "kind": "numeric",
                    "domain": column.get("domain"),
                    "fill_value": fill["value"],
                }
            )
            continue

        if dtype != "categorical":
            raise WorkbenchError(
                "E_COLUMN_MISMATCH",
                "Unsupported column type reached the encoder",
                {
                    "column": column_name,
                    "column_index": column_index,
                    "declared": dtype,
                    "observed": "numeric-or-categorical",
                },
            )
        fill = fill_by_index[column_index]
        missing_count = sum(1 for value in raw_values if is_missing(value))
        values = [str(fill["value"]) if is_missing(value) else value for value in raw_values]
        categories = _global_categorical_categories(column, str(fill["value"]), values)
        fill_summary.append(
            {
                "column": column_name,
                "column_index": column_index,
                "value": fill["value"],
                "method": fill["method"],
                "filled_count": missing_count,
            }
        )
        source_domains.append(
            {
                "column": column_name,
                "column_index": column_index,
                "kind": "categorical",
                "domain": categories,
                "declared_domain": column.get("domain", []),
                "fill_value": fill["value"],
            }
        )

        if strategy == "one_hot":
            for category in categories:
                feature_name = f"ohe:{column_name}={category}"
                block = np.asarray([1.0 if value == category else 0.0 for value in values], dtype=np.float64)
                feature_blocks.append(block.reshape(-1, 1))
                feature_names.append(feature_name)
                mapping.append(
                    {
                        "feature": feature_name,
                        "source_column": column_name,
                        "source_index": column_index,
                        "kind": "one_hot",
                        "value": category,
                    }
                )
        elif strategy == "target":
            encoded, diagnostics = _target_encode(
                values,
                labels,
                column_name=column_name,
                column_index=column_index,
                config=config,
            )
            feature_name = f"te:{column_name}"
            feature_blocks.append(encoded.reshape(-1, 1))
            feature_names.append(feature_name)
            mapping.append(
                {
                    "feature": feature_name,
                    "source_column": column_name,
                    "source_index": column_index,
                    "kind": "target",
                    "value": None,
                }
            )
            target_diagnostics.append(diagnostics)
        elif strategy == "hash":
            buckets = int(config["encoding"]["hash"]["n_features"])
            block = np.zeros((table.row_count, buckets), dtype=np.float64)
            for row_index, value in enumerate(values):
                block[row_index, _hash_bucket(column_name, value, buckets)] = 1.0
            feature_blocks.append(block)
            for bucket in range(buckets):
                feature_name = f"hash:{column_name}:{bucket:04d}"
                feature_names.append(feature_name)
                mapping.append(
                    {
                        "feature": feature_name,
                        "source_column": column_name,
                        "source_index": column_index,
                        "kind": "hash",
                        "value": bucket,
                    }
                )
        else:
            raise WorkbenchError(
                "E_CONFIG_INVALID",
                "Unsupported encoding strategy",
                {"strategy": strategy},
            )

    if feature_blocks:
        matrix = np.concatenate(feature_blocks, axis=1)
        matrix = np.ascontiguousarray(matrix, dtype=np.float64)
    else:
        matrix = np.empty((table.row_count, 0), dtype=np.float64)

    feature_domains: list[dict[str, Any]] = []
    for column_index in range(matrix.shape[1]):
        values = matrix[:, column_index]
        feature_domains.append(
            {
                "feature": feature_names[column_index],
                "min": stable_float(float(np.min(values))) if values.size else 0.0,
                "max": stable_float(float(np.max(values))) if values.size else 0.0,
                "unique_count": int(np.unique(values).size),
            }
        )

    matrix_digest = stable_matrix_digest(matrix)
    label_digest = stable_vector_digest(labels)
    summary: dict[str, Any] = {
        "schema_version": 1,
        "shard_index": table.shard_index,
        "split_index": table.split_index,
        "tier": table.tier,
        "file": table.path.name,
        "input_sha256": table.content_sha256,
        "schema_digest": tier_schema["schema_digest"],
        "encoding_strategy": strategy,
        "rows": int(matrix.shape[0]),
        "source_columns": len(tier_schema["source_columns"]),
        "encoded_feature_count": int(matrix.shape[1]),
        "feature_names": feature_names,
        "matrix_digest": matrix_digest,
        "label_digest": label_digest,
        "mapping": mapping,
        "source_domains": source_domains,
        "feature_domains": feature_domains,
        "missing_fill": fill_summary,
        "target_encoding_diagnostics": target_diagnostics,
        "label_counts": {
            "0": int(np.sum(labels == 0)),
            "1": int(np.sum(labels == 1)),
        },
    }
    summary = to_jsonable(summary)
    summary["encoding_digest"] = digest_object(summary)
    return {
        "summary": summary,
        "matrix": matrix,
        "labels": labels,
    }


def encode_worker(task: dict[str, Any]) -> dict[str, Any]:
    spec: ShardSpec = task["spec"]
    fail_shard = os.environ.get("FEATURE_WORKBENCH_FAIL_SHARD")
    try:
        if fail_shard and fail_shard in {str(spec.shard_index), spec.file, spec.tier}:
            raise RuntimeError(f"injected worker failure for shard {spec.shard_index}")
        table = load_table(spec)
        result = encode_table(table, task["tier_schema"], task["config"])
        return {
            "ok": True,
            "shard_index": spec.shard_index,
            "summary": result["summary"],
            "matrix": result["matrix"],
            "labels": result["labels"],
        }
    except WorkbenchError as exc:
        return {
            "ok": False,
            "shard_index": spec.shard_index,
            "error": exc.to_dict()["error"],
        }
    except BaseException as exc:  # Process boundaries must never leak opaque exceptions.
        return {
            "ok": False,
            "shard_index": spec.shard_index,
            "error": {
                "code": "E_WORKER_FAILED",
                "message": "Worker failed while encoding a shard",
                "details": {
                    "shard_index": spec.shard_index,
                    "file": spec.file,
                    "error_type": type(exc).__name__,
                    "reason": str(exc),
                },
            },
        }

