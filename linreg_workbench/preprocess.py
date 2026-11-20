"""Deterministic preprocessing stage and preprocessing fingerprints."""

from __future__ import annotations

import math
from collections import Counter
from typing import Any

import numpy as np

from .canonical import digest, sha256_file
from .data import DatasetSpec, read_csv
from .errors import EMPTY_FEATURE_CODE, InvalidData, NumericError

_MISSING_TOKENS = {"", "NA", "N/A", "null", "NULL", "NaN", "nan"}


def preprocessing_fingerprint(spec: DatasetSpec, config: dict[str, Any]) -> str:
    return digest(
        {
            "kind": "preprocess-v1",
            "data_sha256": spec.sha256,
            "dataset_semantics": spec.semantics_digest,
            "target": spec.target,
            "features": spec.features,
            "config": config["preprocessing"],
        }
    )


def _is_missing(value: str) -> bool:
    return value.strip() in _MISSING_TOKENS


def _parse_continuous(values: list[str], column: str) -> tuple[np.ndarray, np.ndarray, list[int]]:
    result = np.zeros(len(values), dtype=np.float64)
    observed = np.zeros(len(values), dtype=bool)
    invalid: list[int] = []
    for index, raw in enumerate(values):
        if _is_missing(raw):
            continue
        try:
            number = float(raw)
        except ValueError:
            invalid.append(index)
            continue
        if not math.isfinite(number):
            invalid.append(index)
            continue
        result[index] = number
        observed[index] = True
    if invalid:
        raise InvalidData(
            "Continuous feature contains non-finite or non-numeric values",
            details={"column": column, "row_indices": invalid[:20], "count": len(invalid)},
        )
    return result, observed, invalid


def preprocess(spec: DatasetSpec, config: dict[str, Any]) -> dict[str, Any]:
    fp = preprocessing_fingerprint(spec, config)
    header, rows = read_csv(spec.file_path)
    if len(rows) != spec.rows:
        raise InvalidData(
            "Declared row count does not match CSV",
            details={"expected": spec.rows, "actual": len(rows)},
        )
    if len(header) != spec.columns:
        raise InvalidData(
            "Declared column count does not match CSV",
            details={"expected": spec.columns, "actual": len(header)},
        )
    if sha256_file(spec.file_path) != spec.sha256:
        raise InvalidData("Dataset checksum changed after declaration was loaded")
    index = {name: position for position, name in enumerate(header)}
    target_name = spec.target["name"]
    if target_name not in index:
        raise InvalidData("Target column is absent from CSV", details={"target": target_name})

    target_raw = [row[index[target_name]].strip() for row in rows]
    if any(_is_missing(value) for value in target_raw):
        raise InvalidData("Target column contains missing values", details={"target": target_name})
    target_values = sorted(set(target_raw))
    if len(target_values) != 2:
        raise InvalidData(
            "Target must contain exactly two classes",
            details={"target": target_name, "classes": target_values},
        )
    declared_classes = spec.target.get("classes")
    if declared_classes is not None and set(map(str, declared_classes)) != set(target_values):
        raise InvalidData(
            "Observed target classes differ from declaration",
            details={
                "target": target_name,
                "declared": list(map(str, declared_classes)),
                "observed": target_values,
            },
        )
    positive_raw = spec.target.get("positive_value", target_values[-1])
    positive = str(positive_raw)
    if positive not in target_values:
        positive = target_values[-1]
    y = np.asarray([0 if value != positive else 1 for value in target_raw], dtype=np.float64)
    if int(y.sum()) == 0 or int((1 - y).sum()) == 0:
        raise InvalidData("Target has only one class after encoding")
    class_counts = {
        str(value): Counter(target_raw)[value] for value in target_values
    }

    prep = config["preprocessing"]
    feature_names: list[str] = []
    matrix_columns: list[np.ndarray] = []
    diagnostics: list[dict[str, Any]] = []
    numeric_meta: dict[str, Any] = {}
    categorical_meta: dict[str, Any] = {}
    observed_missing_columns: list[dict[str, Any]] = []
    continuous_columns: list[str] = []
    categorical_columns: list[str] = []
    used_features: list[dict[str, Any]] = []

    declared_names = {item["name"] for item in spec.features}
    for feature in spec.features:
        name = feature["name"]
        role = feature.get("role") or "continuous"
        if name not in index:
            diagnostics.append(
                {
                    "code": "E_DECLARED_COLUMN_MISSING",
                    "column": name,
                    "message": "Declared feature is absent from the CSV and was excluded.",
                }
            )
            continue
        raw_values = [row[index[name]].strip() for row in rows]
        missing_count = sum(1 for value in raw_values if _is_missing(value))
        if missing_count:
            observed_missing_columns.append(
                {
                    "column": name,
                    "role": role,
                    "missing_count": missing_count,
                    "missing_rate": missing_count / len(rows),
                }
            )
        if missing_count == len(rows) or name.strip() == "":
            diagnostics.append(
                {
                    "code": EMPTY_FEATURE_CODE,
                    "column": name,
                    "role": role,
                    "missing_count": missing_count,
                    "message": "Empty or fully missing feature was excluded; other columns were processed.",
                }
            )
            continue
        if role == "continuous":
            values, observed, _ = _parse_continuous(raw_values, name)
            if not np.any(observed):
                diagnostics.append(
                    {
                        "code": EMPTY_FEATURE_CODE,
                        "column": name,
                        "role": role,
                        "missing_count": len(rows),
                        "message": "Feature has no finite observations and was excluded.",
                    }
                )
                continue
            impute = prep["numeric_imputation"]
            observed_values = values[observed]
            if impute == "mean":
                fill = float(np.mean(observed_values))
            elif impute == "median":
                fill = float(np.median(observed_values))
            else:
                fill = 0.0
            filled = values.copy()
            filled[~observed] = fill
            normalization = prep["numeric_normalization"]
            if normalization == "standard":
                center = float(np.mean(observed_values))
                scale = float(np.std(observed_values))
                if not math.isfinite(scale) or scale <= 1e-12:
                    scale = 1.0
            elif normalization == "minmax":
                center = float(np.min(observed_values))
                max_value = float(np.max(observed_values))
                scale = max_value - center
                if not math.isfinite(scale) or scale <= 1e-12:
                    scale = 1.0
            else:
                center = 0.0
                scale = 1.0
            transformed = (filled - center) / scale
            matrix_columns.append(transformed)
            feature_names.append(name)
            continuous_columns.append(name)
            used_features.append(dict(feature))
            numeric_meta[name] = {
                "imputation": impute,
                "fill_value": fill,
                "normalization": normalization,
                "center": center,
                "scale": scale,
                "observed_count": int(observed.sum()),
                "missing_count": int((~observed).sum()),
                "min": float(np.min(observed_values)),
                "max": float(np.max(observed_values)),
                "mean": float(np.mean(observed_values)),
                "std": float(np.std(observed_values)),
            }
        elif role == "categorical":
            non_missing = [value for value in raw_values if not _is_missing(value)]
            if not non_missing:
                diagnostics.append(
                    {
                        "code": EMPTY_FEATURE_CODE,
                        "column": name,
                        "role": role,
                        "missing_count": len(rows),
                        "message": "Fully missing categorical feature was excluded.",
                    }
                )
                continue
            counts = Counter(non_missing)
            mode = sorted(counts.items(), key=lambda pair: (-pair[1], pair[0]))[0][0]
            imputation = prep["categorical_imputation"]
            fill = mode if imputation == "mode" else "__MISSING__"
            cleaned = [fill if _is_missing(value) else value for value in raw_values]
            declared_categories = feature.get("categories")
            categories = (
                [str(value) for value in declared_categories]
                if isinstance(declared_categories, list) and declared_categories
                else sorted(set(cleaned))
            )
            if "__MISSING__" in cleaned and "__MISSING__" not in categories:
                categories.append("__MISSING__")
            unknown = sorted(set(cleaned) - set(categories))
            if unknown:
                categories.extend(unknown)
                diagnostics.append(
                    {
                        "code": "E_UNDECLARED_CATEGORY",
                        "column": name,
                        "categories": unknown,
                        "message": "Undeclared categories were retained deterministically.",
                    }
                )
            if prep["categorical_encoding"] == "onehot":
                for category in categories:
                    encoded = np.asarray(
                        [1.0 if value == category else 0.0 for value in cleaned],
                        dtype=np.float64,
                    )
                    matrix_columns.append(encoded)
                    feature_names.append(f"{name}={category}")
                categorical_meta[name] = {
                    "encoding": "onehot",
                    "imputation": imputation,
                    "fill_value": fill,
                    "categories": categories,
                    "missing_count": missing_count,
                }
            else:
                smoothing = float(prep["target_encoding_smoothing"])
                prior = float(np.mean(y))
                mappings: dict[str, float] = {}
                for category in categories:
                    selected = np.asarray([value == category for value in cleaned])
                    positives = float(np.sum(y[selected]))
                    count = int(np.sum(selected))
                    mappings[category] = (positives + smoothing * prior) / (count + smoothing)
                encoded = np.asarray([mappings[value] for value in cleaned], dtype=np.float64)
                matrix_columns.append(encoded)
                feature_names.append(name)
                categorical_meta[name] = {
                    "encoding": "target",
                    "imputation": imputation,
                    "fill_value": fill,
                    "smoothing": smoothing,
                    "prior": prior,
                    "mappings": mappings,
                    "missing_count": missing_count,
                }
            categorical_columns.append(name)
            used_features.append(dict(feature))
        else:
            raise InvalidData(
                "Unsupported feature role in declaration",
                details={"column": name, "role": role},
            )

    for name in header:
        if name == target_name or name in declared_names:
            continue
        raw_values = [row[index[name]].strip() for row in rows]
        if all(_is_missing(value) for value in raw_values):
            diagnostics.append(
                {
                    "code": EMPTY_FEATURE_CODE,
                    "column": name,
                    "role": "inferred",
                    "missing_count": len(rows),
                    "message": "Undeclared empty column was excluded.",
                }
            )
        else:
            diagnostics.append(
                {
                    "code": "E_UNDECLARED_COLUMN",
                    "column": name,
                    "message": "Undeclared columns are not used by the training matrix.",
                }
            )

    if not matrix_columns:
        raise InvalidData(
            "No usable feature columns remain after preprocessing",
            details={"diagnostics": diagnostics},
        )
    X = np.column_stack(matrix_columns).astype(np.float64, copy=False)
    if not np.all(np.isfinite(X)):
        raise NumericError(
            "Preprocessing produced NaN or Inf",
            details={"reason": "non-finite design matrix"},
        )
    if not np.all(np.isfinite(y)):
        raise NumericError("Target encoding produced NaN or Inf")
    return {
        "artifact_type": "preprocess",
        "fingerprint": fp,
        "data_sha256": spec.sha256,
        "dataset_semantics_digest": spec.semantics_digest,
        "dataset_id": spec.dataset_id,
        "target_name": target_name,
        "target_classes": target_values,
        "positive_class": positive,
        "row_count": len(rows),
        "feature_count": X.shape[1],
        "feature_names": feature_names,
        "X": X.tolist(),
        "y": y.tolist(),
        "row_ids": list(range(len(rows))),
        "continuous_columns": continuous_columns,
        "categorical_columns": categorical_columns,
        "observed_missing_columns": observed_missing_columns,
        "diagnostics": diagnostics,
        "empty_feature_errors": [
            item for item in diagnostics if item["code"] == EMPTY_FEATURE_CODE
        ],
        "numeric_metadata": numeric_meta,
        "categorical_metadata": categorical_meta,
        "class_counts": class_counts,
        "used_features": used_features,
        "preprocessing_config": prep,
    }
