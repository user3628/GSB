from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.preprocessing import OneHotEncoder

from .config import stage_config
from .errors import invalid_config, invalid_stage
from .stage import ArtifactBundle
from .util import (
    array_bytes,
    canonical_json_bytes,
    load_array,
    read_json,
    sha256_bytes,
    sha256_file,
)


@dataclass
class PreprocessedData:
    X: np.ndarray
    feature_names: list[str]
    metadata: dict[str, Any]


def load_manifest(manifest_path: Path) -> dict[str, Any]:
    if not manifest_path.exists():
        raise invalid_stage("Sample manifest not found", {"path": str(manifest_path)})
    manifest = read_json(manifest_path)
    if manifest.get("schema_version") != "sample-manifest-v1":
        raise invalid_stage(
            "Unsupported sample manifest schema", {"schema_version": manifest.get("schema_version")}
        )
    return manifest


def get_tier_item(manifest: dict[str, Any], tier: str) -> dict[str, Any]:
    for item in manifest.get("files", []):
        if item.get("tier") == tier:
            return item
    raise invalid_stage("Tier is not present in sample manifest", {"tier": tier})


def preprocess_input_fingerprint(
    manifest_path: Path,
    manifest: dict[str, Any],
    tier: str,
    config: dict[str, Any],
) -> str:
    item = get_tier_item(manifest, tier)
    payload = {
        "manifest_sha256": sha256_file(manifest_path),
        "tier_item": item,
        "preprocess": stage_config(config, "preprocess"),
    }
    return sha256_bytes(canonical_json_bytes(payload))


def _read_rows(csv_path: Path, expected_columns: list[str]) -> list[dict[str, str]]:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != expected_columns:
            raise invalid_stage(
                "CSV header does not match manifest column order",
                {"expected": expected_columns, "actual": reader.fieldnames},
            )
        rows = list(reader)
    return rows


def _mode(values: list[str]) -> str:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def compute_preprocess(
    manifest_path: Path,
    tier: str,
    config: dict[str, Any],
) -> ArtifactBundle:
    manifest = load_manifest(manifest_path)
    item = get_tier_item(manifest, tier)
    csv_path = manifest_path.parent / item["file"]
    if not csv_path.exists():
        raise invalid_stage("Tier CSV not found", {"path": str(csv_path), "tier": tier})
    if sha256_file(csv_path) != item["sha256"]:
        raise invalid_stage(
            "Tier CSV digest does not match manifest",
            {"path": str(csv_path), "tier": tier},
        )
    semantics = item.get("column_semantics", [])
    expected_columns = [column["name"] for column in semantics]
    rows = _read_rows(csv_path, expected_columns)
    if len(rows) != item["rows"]:
        raise invalid_stage(
            "Tier row count does not match manifest",
            {"tier": tier, "expected": item["rows"], "actual": len(rows)},
        )

    pre = config["preprocess"]
    max_cardinality = pre["max_onehot_cardinality"]
    continuous_columns: list[dict[str, Any]] = []
    categorical_columns: list[dict[str, Any]] = []
    ignored_columns: list[dict[str, Any]] = []
    column_audit: list[dict[str, Any]] = []
    missing_indicators: list[tuple[str, np.ndarray]] = []
    total_missing = 0
    total_cells = 0

    for semantic in semantics:
        name = semantic["name"]
        kind = semantic["kind"]
        raw_values = [row[name] for row in rows]
        non_empty = [value for value in raw_values if value != ""]
        missing_count = len(raw_values) - len(non_empty)
        total_missing += missing_count
        total_cells += len(raw_values)
        audit: dict[str, Any] = {
            "name": name,
            "declared_kind": kind,
            "missing_count": missing_count,
            "missing_ratio": missing_count / max(len(raw_values), 1),
            "non_empty_count": len(non_empty),
            "redundant_of": semantic.get("redundant_of"),
        }
        if kind == "identifier" or semantic.get("role") == "identifier":
            audit["detected_kind"] = "identifier"
            audit["action"] = "excluded_from_features"
            ignored_columns.append(audit)
            column_audit.append(audit)
            continue
        if semantic.get("redundant_of"):
            audit["redundancy_declared"] = True
        if kind == "continuous":
            numeric_values: list[float] = []
            for value in non_empty:
                try:
                    number = float(value)
                except ValueError as exc:
                    raise invalid_stage(
                        "Continuous column contains a non-numeric non-empty value",
                        {"tier": tier, "column": name, "value": value},
                    ) from exc
                if not np.isfinite(number):
                    raise invalid_stage(
                        "Continuous column contains a non-finite value",
                        {"tier": tier, "column": name, "value": value},
                    )
                numeric_values.append(number)
            parse_ratio = len(numeric_values) / max(len(non_empty), 1)
            if parse_ratio < 0.98:
                raise invalid_stage(
                    "Manifest declares a continuous column but statistics are not numeric",
                    {"tier": tier, "column": name, "numeric_parse_ratio": parse_ratio},
                )
            if non_empty:
                fill_value = {
                    "median": float(np.median(np.asarray(numeric_values, dtype=np.float64))),
                    "mean": float(np.mean(np.asarray(numeric_values, dtype=np.float64))),
                    "zero": 0.0,
                }[pre["numeric_impute"]]
            else:
                fill_value = 0.0
            array = np.asarray(
                [
                    fill_value if value == "" else float(value)
                    for value in raw_values
                ],
                dtype=np.float64,
            )
            continuous_columns.append(
                {
                    "name": name,
                    "fill_value": fill_value,
                    "non_empty": len(non_empty),
                    "redundant_of": semantic.get("redundant_of"),
                }
            )
            audit.update(
                {
                    "detected_kind": "continuous",
                    "numeric_parse_ratio": parse_ratio,
                    "fill_value": fill_value,
                    "fill_strategy": pre["numeric_impute"],
                    "observed_min": float(np.min(array)) if array.size else None,
                    "observed_max": float(np.max(array)) if array.size else None,
                }
            )
        elif kind == "categorical":
            observed = sorted(set(non_empty))
            if len(observed) > max_cardinality:
                raise invalid_config(
                    "Categorical column exceeds max_onehot_cardinality",
                    {
                        "tier": tier,
                        "column": name,
                        "cardinality": len(observed),
                        "max_onehot_cardinality": max_cardinality,
                    },
                )
            if pre["categorical_impute"] == "most_frequent":
                fill_value = _mode(non_empty) if non_empty else "__MISSING__"
            else:
                fill_value = "__MISSING__"
            categories = sorted(set(observed) | {fill_value})
            array = np.asarray(
                [fill_value if value == "" else value for value in raw_values],
                dtype=object,
            )
            categorical_columns.append(
                {
                    "name": name,
                    "fill_value": fill_value,
                    "categories": categories,
                    "redundant_of": semantic.get("redundant_of"),
                }
            )
            audit.update(
                {
                    "detected_kind": "categorical",
                    "observed_cardinality": len(observed),
                    "observed_categories": observed,
                    "fill_value": fill_value,
                    "fill_strategy": pre["categorical_impute"],
                }
            )
        else:
            raise invalid_stage(
                "Unsupported column kind in manifest", {"tier": tier, "column": name, "kind": kind}
            )
        if missing_count and pre["add_missing_indicators"]:
            missing_indicators.append(
                (
                    f"missing__{name}",
                    np.asarray([1.0 if value == "" else 0.0 for value in raw_values]),
                )
            )
        column_audit.append(audit)

    feature_blocks: list[np.ndarray] = []
    feature_names: list[str] = []
    feature_audit: list[dict[str, Any]] = []
    transforms: list[dict[str, Any]] = []

    if continuous_columns:
        continuous_matrix = np.column_stack(
            [
                np.asarray(
                    [
                        column["fill_value"] if row[column["name"]] == "" else float(row[column["name"]])
                        for row in rows
                    ],
                    dtype=np.float64,
                )
                for column in continuous_columns
            ]
        )
    else:
        continuous_matrix = np.empty((len(rows), 0), dtype=np.float64)
    if continuous_columns:
        means = np.mean(continuous_matrix, axis=0, dtype=np.float64)
        scales = np.std(continuous_matrix, axis=0, dtype=np.float64)
        scales = np.where(scales < 1e-12, 1.0, scales)
        standardized = (continuous_matrix - means) / scales
        feature_blocks.append(standardized)
        for index, column in enumerate(continuous_columns):
            feature_name = f"num__{column['name']}"
            feature_names.append(feature_name)
            feature_audit.append(
                {
                    "name": feature_name,
                    "source": column["name"],
                    "kind": "continuous_standardized",
                    "mean": float(means[index]),
                    "scale": float(scales[index]),
                }
            )
        transforms.append(
            {
                "kind": "standard_scaler",
                "columns": [column["name"] for column in continuous_columns],
                "means": [float(value) for value in means],
                "scales": [float(value) for value in scales],
            }
        )
    else:
        means = np.empty(0, dtype=np.float64)
        scales = np.empty(0, dtype=np.float64)

    if categorical_columns:
        categorical_matrix = np.column_stack(
            [
                np.asarray(
                    [column["fill_value"] if row[column["name"]] == "" else row[column["name"]] for row in rows],
                    dtype=object,
                )
                for column in categorical_columns
            ]
        )
        encoder = OneHotEncoder(
            categories=[column["categories"] for column in categorical_columns],
            handle_unknown="ignore",
            sparse_output=False,
            dtype=np.float64,
        )
        encoded = encoder.fit_transform(categorical_matrix)
        feature_blocks.append(np.asarray(encoded, dtype=np.float64))
        for column_index, column in enumerate(categorical_columns):
            for category in column["categories"]:
                feature_name = f"cat__{column['name']}__{category}"
                feature_names.append(feature_name)
                feature_audit.append(
                    {
                        "name": feature_name,
                        "source": column["name"],
                        "kind": "one_hot",
                        "category": category,
                    }
                )
        transforms.append(
            {
                "kind": "one_hot",
                "columns": [column["name"] for column in categorical_columns],
                "categories": [column["categories"] for column in categorical_columns],
            }
        )

    if missing_indicators:
        indicator_matrix = np.column_stack([indicator for _, indicator in missing_indicators])
        feature_blocks.append(indicator_matrix)
        for feature_name, _ in missing_indicators:
            feature_names.append(feature_name)
            feature_audit.append(
                {
                    "name": feature_name,
                    "source": feature_name.removeprefix("missing__"),
                    "kind": "missing_indicator",
                }
            )
        transforms.append(
            {
                "kind": "missing_indicators",
                "columns": [name.removeprefix("missing__") for name, _ in missing_indicators],
            }
        )

    if feature_blocks:
        X = np.column_stack(feature_blocks).astype(np.float64, copy=False)
    else:
        raise invalid_stage("No feature columns remain after preprocessing", {"tier": tier})

    dropped_features: list[dict[str, Any]] = []
    if pre["drop_exact_duplicate_features"]:
        seen: dict[bytes, str] = {}
        keep_indexes: list[int] = []
        for index, feature_name in enumerate(feature_names):
            key = np.ascontiguousarray(X[:, index], dtype="<f8").tobytes()
            if key in seen:
                dropped_features.append(
                    {"name": feature_name, "duplicate_of": seen[key]}
                )
                continue
            seen[key] = feature_name
            keep_indexes.append(index)
        X = X[:, keep_indexes]
        feature_names = [feature_names[index] for index in keep_indexes]
        feature_audit = [feature_audit[index] for index in keep_indexes]

    missing_ratio = total_missing / max(total_cells, 1)
    metadata: dict[str, Any] = {
        "schema_version": "preprocess-v1",
        "tier": tier,
        "rows": len(rows),
        "raw_columns": item["columns"],
        "encoded_dimension": int(X.shape[1]),
        "missing_ratio": missing_ratio,
        "total_missing_cells": total_missing,
        "continuous_columns": [column["name"] for column in continuous_columns],
        "categorical_columns": [column["name"] for column in categorical_columns],
        "ignored_columns": [column["name"] for column in ignored_columns],
        "missing_columns": [
            audit["name"] for audit in column_audit if audit["missing_count"] > 0
        ],
        "redundant_columns": [
            audit["name"] for audit in column_audit if audit.get("redundant_of")
        ],
        "column_audit": column_audit,
        "feature_names": feature_names,
        "feature_audit": feature_audit,
        "transforms": transforms,
        "dropped_exact_duplicate_features": dropped_features,
        "input_csv_sha256": item["sha256"],
        "preprocess_config": config["preprocess"],
    }
    X_canonical = np.ascontiguousarray(X, dtype="<f8")
    artifacts = {
        "X.npy": array_bytes(X_canonical),
        "metadata.json": canonical_json_bytes(metadata),
    }
    return ArtifactBundle(artifacts=artifacts, metadata=metadata)


def load_preprocess_artifacts(paths: dict[str, Path]) -> PreprocessedData:
    required = {"X.npy", "metadata.json"}
    missing = sorted(required - set(paths))
    if missing:
        raise invalid_stage("Preprocess artifacts are incomplete", {"missing": missing})
    metadata = json.loads(paths["metadata.json"].read_text(encoding="utf-8"))
    X = np.asarray(load_array(paths["X.npy"]), dtype=np.float64)
    if X.shape != (int(metadata["rows"]), int(metadata["encoded_dimension"])):
        raise invalid_stage(
            "Preprocess matrix shape does not match metadata",
            {"shape": list(X.shape), "metadata": metadata},
        )
    if X.shape[1] != len(metadata["feature_names"]):
        raise invalid_stage(
            "Preprocess feature-name count does not match matrix",
            {"features": len(metadata["feature_names"]), "columns": X.shape[1]},
        )
    return PreprocessedData(
        X=X,
        feature_names=list(metadata["feature_names"]),
        metadata=metadata,
    )
