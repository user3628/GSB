from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from .errors import PlanError


INT64 = "int64"
FLOAT64 = "float64"
BOOL = "bool"
DTYPES = (INT64, FLOAT64, BOOL)
DTYPE_BYTES = {INT64: 8, FLOAT64: 8, BOOL: 1}
NUMPY_DTYPES = {INT64: np.dtype("int64"), FLOAT64: np.dtype("float64"), BOOL: np.dtype("bool")}
INT64_MIN = -(1 << 63)
INT64_MAX = (1 << 63) - 1


@dataclass(frozen=True)
class FieldSpec:
    name: str
    dtype: str

    def __post_init__(self) -> None:
        if self.dtype not in DTYPES:
            raise PlanError(f"unsupported dtype {self.dtype!r}", details={"column": self.name})

    def to_dict(self) -> dict[str, str]:
        return {"name": self.name, "dtype": self.dtype}


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    row_count: int
    columns: tuple[FieldSpec, ...]
    file_name: str

    def __post_init__(self) -> None:
        if self.row_count < 0:
            raise PlanError("dataset row_count must be non-negative")
        names = [column.name for column in self.columns]
        if len(names) != len(set(names)):
            raise PlanError(f"dataset {self.name!r} contains duplicate column names")

    @property
    def schema(self) -> dict[str, str]:
        return {column.name: column.dtype for column in self.columns}

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "row_count": self.row_count,
            "file": self.file_name,
            "columns": [column.to_dict() for column in self.columns],
        }


@dataclass
class Column:
    name: str
    dtype: str
    values: np.ndarray
    valid: np.ndarray

    def __post_init__(self) -> None:
        if self.dtype not in DTYPES:
            raise ValueError(f"unsupported column dtype {self.dtype!r}")
        expected = NUMPY_DTYPES[self.dtype]
        if self.values.dtype != expected:
            raise ValueError(f"column {self.name!r} has dtype {self.values.dtype}, expected {expected}")
        if self.valid.dtype != np.dtype("bool"):
            raise ValueError(f"column {self.name!r} null bitmap must be bool")
        if self.values.shape != self.valid.shape or self.values.ndim != 1:
            raise ValueError(f"column {self.name!r} values and null bitmap shapes differ")

    def __len__(self) -> int:
        return int(self.values.shape[0])

    def value_at(self, row: int) -> Any:
        if not bool(self.valid[row]):
            return None
        value = self.values[row]
        if self.dtype == INT64:
            return int(value)
        if self.dtype == FLOAT64:
            return float(value)
        return bool(value)

    def take(self, mask: np.ndarray) -> "Column":
        return Column(self.name, self.dtype, self.values[mask], self.valid[mask])


@dataclass
class ColumnBatch:
    columns: dict[str, Column]
    row_count: int

    def __post_init__(self) -> None:
        if self.row_count < 0:
            raise ValueError("batch row_count must be non-negative")
        for column in self.columns.values():
            if len(column) != self.row_count:
                raise ValueError(f"column {column.name!r} length differs from batch row_count")

    def take(self, mask: np.ndarray) -> "ColumnBatch":
        if mask.dtype != np.dtype("bool") or mask.shape != (self.row_count,):
            raise ValueError("batch selection mask must be a bool vector matching row_count")
        return ColumnBatch(
            {name: column.take(mask) for name, column in self.columns.items()},
            int(mask.sum()),
        )


@dataclass
class Dataset:
    spec: DatasetSpec
    arrays: dict[str, Column] = field(default_factory=dict)

    @property
    def name(self) -> str:
        return self.spec.name

    @property
    def row_count(self) -> int:
        return self.spec.row_count

    @property
    def columns(self) -> tuple[FieldSpec, ...]:
        return self.spec.columns

    @property
    def schema(self) -> dict[str, str]:
        return self.spec.schema

    def batch(self, start: int, size: int) -> ColumnBatch:
        if start < 0 or size < 0 or start + size > self.row_count:
            raise ValueError("batch range is outside the dataset")
        end = start + size
        return ColumnBatch(
            {
                column.name: Column(
                    column.name,
                    column.dtype,
                    column.values[start:end],
                    column.valid[start:end],
                )
                for column in self.arrays.values()
            },
            size,
        )


class DatasetRegistry:
    """Loads only dataset metadata up front and caches arrays on first use."""

    def __init__(self, data_dir: str | Path) -> None:
        self.data_dir = Path(data_dir).resolve()
        self._lock = threading.RLock()
        self._specs: dict[str, DatasetSpec] = {}
        self._loaded: dict[str, Dataset] = {}
        self._load_manifest()

    def _load_manifest(self) -> None:
        manifest_path = self.data_dir / "manifest.json"
        if not manifest_path.exists():
            raise PlanError(
                f"sample manifest not found: {manifest_path}",
                details={"hint": "run scripts/generate_samples.py first"},
            )
        with manifest_path.open("r", encoding="utf-8") as handle:
            manifest = json.load(handle)
        datasets = manifest.get("datasets")
        if not isinstance(datasets, dict) or not datasets:
            raise PlanError("sample manifest contains no datasets")
        for name, raw in datasets.items():
            try:
                row_count = int(raw["rows"])
                file_name = str(raw["file"])
                columns = tuple(FieldSpec(str(item["name"]), str(item["dtype"])) for item in raw["columns"])
            except (KeyError, TypeError, ValueError) as exc:
                raise PlanError(f"invalid manifest entry for dataset {name!r}") from exc
            self._specs[name] = DatasetSpec(name, row_count, columns, file_name)

    def names(self) -> list[str]:
        return sorted(self._specs)

    def specs(self) -> list[DatasetSpec]:
        return [self._specs[name] for name in self.names()]

    def get_spec(self, name: str) -> DatasetSpec:
        try:
            return self._specs[name]
        except KeyError as exc:
            raise PlanError(
                f"unknown dataset {name!r}",
                details={"available_datasets": self.names()},
            ) from exc

    def load(self, name: str) -> Dataset:
        spec = self.get_spec(name)
        with self._lock:
            cached = self._loaded.get(name)
            if cached is not None:
                return cached
            archive_path = (self.data_dir / spec.file_name).resolve()
            if self.data_dir not in archive_path.parents:
                raise PlanError("dataset file escapes the configured data directory")
            if not archive_path.exists():
                raise PlanError(
                    f"dataset file not found: {archive_path}",
                    details={"dataset": name},
                )
            with np.load(archive_path, allow_pickle=False) as archive:
                arrays: dict[str, Column] = {}
                for field_spec in spec.columns:
                    values_key = f"{field_spec.name}__values"
                    valid_key = f"{field_spec.name}__valid"
                    if values_key not in archive or valid_key not in archive:
                        raise PlanError(
                            f"dataset {name!r} is missing arrays for column {field_spec.name!r}"
                        )
                    values = np.array(archive[values_key], copy=True)
                    valid = np.array(archive[valid_key], dtype=np.bool_, copy=True)
                    if values.shape != (spec.row_count,) or valid.shape != (spec.row_count,):
                        raise PlanError(
                            f"dataset {name!r} column {field_spec.name!r} has an invalid shape"
                        )
                    arrays[field_spec.name] = Column(field_spec.name, field_spec.dtype, values, valid)
            dataset = Dataset(spec, arrays)
            self._loaded[name] = dataset
            return dataset


def as_numpy_array(values: Iterable[Any], dtype: str) -> np.ndarray:
    return np.asarray(list(values), dtype=NUMPY_DTYPES[dtype])
