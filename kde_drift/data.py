"""Dataset registry, declaration validation, and column-schema checks."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import numpy as np

from .canonical import dataset_arrays_digest, read_json
from .errors import WorkbenchError


def compare_column_schemas(
    reference_names: Sequence[str],
    current_names: Sequence[str],
    reference_shape: tuple[int, int],
    current_shape: tuple[int, int],
) -> None:
    reference = list(reference_names)
    current = list(current_names)
    differences: list[dict[str, Any]] = []
    if int(reference_shape[1]) != int(current_shape[1]):
        differences.append(
            {
                "kind": "column_count",
                "reference": int(reference_shape[1]),
                "current": int(current_shape[1]),
            }
        )
    if len(reference) != len(current):
        differences.append({"kind": "column_name_count", "reference": len(reference), "current": len(current)})
    for index in range(max(len(reference), len(current))):
        left = reference[index] if index < len(reference) else None
        right = current[index] if index < len(current) else None
        if left != right:
            differences.append(
                {"kind": "column_name", "index": index, "reference": left, "current": right}
            )
    if differences:
        raise WorkbenchError(
            "E_COLUMN_MISMATCH",
            "reference and current column schemas differ",
            stage="schema_validation",
            location="column_names",
            details={"differences": differences, "difference_count": len(differences)},
        )


@dataclass(frozen=True)
class Dataset:
    name: str
    declaration_path: Path
    declaration: dict[str, Any]
    reference_features: np.ndarray
    current_features: np.ndarray
    reference_labels: np.ndarray
    current_labels: np.ndarray
    column_names: list[str]
    dataset_fingerprint: str
    reference_fingerprint: str
    current_fingerprint: str
    missing_columns: list[int]

    @property
    def rows_reference(self) -> int:
        return int(self.reference_features.shape[0])

    @property
    def rows_current(self) -> int:
        return int(self.current_features.shape[0])

    @property
    def columns(self) -> int:
        return int(self.reference_features.shape[1])

    def summary(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "rows_reference": self.rows_reference,
            "rows_current": self.rows_current,
            "columns": self.columns,
            "column_names": self.column_names,
            "missing_columns": self.missing_columns,
            "missing_column_names": [self.column_names[index] for index in self.missing_columns],
            "dataset_fingerprint": self.dataset_fingerprint,
            "reference_fingerprint": self.reference_fingerprint,
            "current_fingerprint": self.current_fingerprint,
        }


def _load_npz_pair(
    declaration: dict[str, Any], declaration_path: Path
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    reference_spec = declaration.get("reference", {})
    current_spec = declaration.get("current", {})
    features_key = reference_spec.get("features_key", "features")
    labels_key = reference_spec.get("labels_key", "source_labels")
    reference_path = declaration_path.parent / reference_spec.get("path", "reference.npz")
    current_path = declaration_path.parent / current_spec.get("path", "current.npz")
    for path in (reference_path, current_path):
        if not path.exists():
            raise WorkbenchError(
                "E_NOT_FOUND",
                f"dataset file not found: {path}",
                stage="dataset_import",
                location=str(path),
            )
    try:
        reference_npz = np.load(reference_path, allow_pickle=False)
        current_npz = np.load(current_path, allow_pickle=False)
        reference = np.asarray(reference_npz[features_key], dtype=np.float64)
        current = np.asarray(current_npz[features_key], dtype=np.float64)
        reference_labels = np.asarray(reference_npz[labels_key])
        current_labels = np.asarray(current_npz[labels_key])
    except (KeyError, ValueError, OSError) as exc:
        raise WorkbenchError(
            "E_INVALID_STAGE",
            "unable to import dataset arrays",
            stage="dataset_import",
            location=str(declaration_path),
            details={"reason": str(exc)},
        ) from exc
    if reference.ndim != 2 or current.ndim != 2:
        raise WorkbenchError(
            "E_INVALID_STAGE",
            "feature samples must be two-dimensional matrices",
            stage="dataset_import",
            location="features",
        )
    if reference_labels.ndim != 1 or current_labels.ndim != 1:
        raise WorkbenchError(
            "E_INVALID_STAGE",
            "source labels must be one-dimensional",
            stage="dataset_import",
            location="source_labels",
        )
    if reference_labels.shape[0] != reference.shape[0] or current_labels.shape[0] != current.shape[0]:
        raise WorkbenchError(
            "E_INVALID_STAGE",
            "source label length does not match the feature rows",
            stage="dataset_import",
            location="source_labels",
        )
    if np.isinf(reference).any() or np.isinf(current).any():
        raise WorkbenchError(
            "E_NUMERIC",
            "dataset contains infinite feature values",
            stage="dataset_import",
            location="features",
        )
    return reference, current, reference_labels, current_labels


def load_dataset(data_dir: str | Path, name: str) -> Dataset:
    root = Path(data_dir)
    declaration_path = root / name / "declaration.json"
    if not declaration_path.exists():
        raise WorkbenchError(
            "E_NOT_FOUND",
            f"dataset declaration not found: {declaration_path}",
            stage="dataset_import",
            location=str(declaration_path),
        )
    declaration = read_json(declaration_path)
    declared_column_names = [str(value) for value in declaration.get("column_names", [])]
    reference_spec = declaration.get("reference", {})
    current_spec = declaration.get("current", {})
    reference_names = [
        str(value) for value in reference_spec.get("column_names", declared_column_names)
    ]
    current_names = [
        str(value) for value in current_spec.get("column_names", declared_column_names)
    ]
    if not declared_column_names and not reference_names:
        raise WorkbenchError(
            "E_INVALID_STAGE",
            "declaration does not define column_names",
            stage="dataset_import",
            location=str(declaration_path),
        )
    reference, current, reference_labels, current_labels = _load_npz_pair(
        declaration, declaration_path
    )
    compare_column_schemas(
        reference_names,
        current_names,
        reference.shape,
        current.shape,
    )
    column_names = reference_names
    declared_rows = int(declaration.get("row_count", -1))
    declared_columns = int(declaration.get("column_count", -1))
    if reference.shape[0] != declared_rows or current.shape[0] != declared_rows:
        raise WorkbenchError(
            "E_INVALID_STAGE",
            "declared row_count does not match the NPZ samples",
            stage="dataset_import",
            location="row_count",
            details={
                "declared": declared_rows,
                "reference": int(reference.shape[0]),
                "current": int(current.shape[0]),
            },
        )
    if reference.shape[1] != declared_columns or len(column_names) != declared_columns:
        raise WorkbenchError(
            "E_INVALID_STAGE",
            "declared column_count does not match the samples or column_names",
            stage="dataset_import",
            location="column_count",
            details={"declared": declared_columns, "actual": int(reference.shape[1])},
        )
    missing_columns = [int(value) for value in declaration.get("missing_columns", [])]
    if any(index < 0 or index >= declared_columns for index in missing_columns):
        raise WorkbenchError(
            "E_INVALID_STAGE",
            "declaration contains an out-of-range missing column index",
            stage="dataset_import",
            location="missing_columns",
            details={"missing_columns": missing_columns, "columns": declared_columns},
        )
    reference_fp = dataset_arrays_digest(
        reference, reference_labels, column_names, canonical_columns=True
    )
    current_fp = dataset_arrays_digest(
        current, current_labels, column_names, canonical_columns=True
    )
    fingerprint = dataset_arrays_digest(
        np.concatenate([reference, current], axis=0),
        np.concatenate([reference_labels, current_labels], axis=0),
        column_names,
        canonical_columns=True,
    )
    return Dataset(
        name=name,
        declaration_path=declaration_path,
        declaration=declaration,
        reference_features=np.ascontiguousarray(reference, dtype=np.float64),
        current_features=np.ascontiguousarray(current, dtype=np.float64),
        reference_labels=reference_labels,
        current_labels=current_labels,
        column_names=column_names,
        dataset_fingerprint=fingerprint,
        reference_fingerprint=reference_fp,
        current_fingerprint=current_fp,
        missing_columns=missing_columns,
    )


class DatasetRegistry:
    def __init__(self, data_dir: str | Path) -> None:
        self.data_dir = Path(data_dir)

    def names(self) -> list[str]:
        index_path = self.data_dir / "index.json"
        if index_path.exists():
            index = read_json(index_path)
            names = sorted(index.get("datasets", {}))
            if names:
                return names
        return sorted(
            path.parent.name for path in self.data_dir.glob("*/declaration.json")
        )

    def load(self, name: str) -> Dataset:
        return load_dataset(self.data_dir, name)

    def summaries(self) -> list[dict[str, Any]]:
        values: list[dict[str, Any]] = []
        for name in self.names():
            dataset = self.load(name)
            declaration = dataset.declaration
            values.append(
                {
                    "name": name,
                    "rows_reference": dataset.rows_reference,
                    "rows_current": dataset.rows_current,
                    "columns": dataset.columns,
                    "missing_columns": dataset.missing_columns,
                    "column_names": dataset.column_names,
                    "seed": declaration.get("seed"),
                    "expected_output": declaration.get("expected_output", {}),
                }
            )
        return values
