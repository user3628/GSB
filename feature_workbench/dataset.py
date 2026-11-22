from __future__ import annotations

import csv
import math
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from .errors import WorkbenchError
from .util import canonical_json_bytes, digest_object, file_sha256, stable_float


MISSING_TOKENS = {"", "na", "null", "none", "nan"}


@dataclass(frozen=True)
class Table:
    path: Path
    tier: str
    shard_index: int
    split_index: int
    header: list[str]
    rows: list[list[str]]
    content_sha256: str

    @property
    def row_count(self) -> int:
        return len(self.rows)

    @property
    def column_count(self) -> int:
        return len(self.header)


@dataclass(frozen=True)
class ShardSpec:
    file: str
    path: Path
    tier: str
    shard_index: int
    split_index: int
    declared_rows: int
    declared_columns: int
    declared_column_names: list[str] | None
    declared_types: dict[int, str]
    categorical_cardinalities: dict[int, int]
    categorical_names: dict[int, str]
    missing_columns: list[int]
    label_column: int
    content_sha256: str

    def as_public_dict(self) -> dict[str, Any]:
        return {
            "file": self.file,
            "path": str(self.path),
            "tier": self.tier,
            "shard_index": self.shard_index,
            "split_index": self.split_index,
            "declared_rows": self.declared_rows,
            "declared_columns": self.declared_columns,
            "missing_columns": list(self.missing_columns),
            "categorical_cardinalities": {
                str(index): value
                for index, value in sorted(self.categorical_cardinalities.items())
            },
            "label_column": self.label_column,
            "content_sha256": self.content_sha256,
        }


def _require_int(mapping: dict[str, Any], key: str, *, minimum: int = 0) -> int:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise WorkbenchError(
            "E_INPUT_INVALID",
            f"Manifest field {key!r} must be an integer >= {minimum}",
            {"value": value, "entry": mapping.get("file")},
        )
    return value


def _safe_data_path(data_dir: Path, name: str) -> Path:
    if not isinstance(name, str) or not name:
        raise WorkbenchError("E_INPUT_INVALID", "Shard file name is missing")
    data_root = data_dir.resolve()
    path = (data_root / name).resolve()
    try:
        path.relative_to(data_root)
    except ValueError as exc:
        raise WorkbenchError(
            "E_INPUT_INVALID",
            "Shard path escapes the data directory",
            {"file": name, "data_dir": str(data_root)},
        ) from exc
    if not path.is_file():
        raise WorkbenchError(
            "E_NOT_FOUND",
            "Shard CSV file was not found",
            {"file": name, "path": str(path)},
        )
    return path


def load_manifest(path: Path) -> dict[str, Any]:
    import json

    path = Path(path)
    if not path.is_file():
        raise WorkbenchError(
            "E_NOT_FOUND",
            "Sample declaration file was not found",
            {"path": str(path)},
        )
    try:
        with path.open("r", encoding="utf-8") as handle:
            manifest = json.load(handle)
    except (OSError, ValueError) as exc:
        raise WorkbenchError(
            "E_INPUT_INVALID",
            "Sample declaration JSON is invalid",
            {"path": str(path), "reason": str(exc)},
        ) from exc
    if not isinstance(manifest, dict):
        raise WorkbenchError("E_INPUT_INVALID", "Manifest root must be an object")
    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        raise WorkbenchError("E_INPUT_INVALID", "Manifest files must be a non-empty list")
    return manifest


def _parse_categorical_columns(entry: dict[str, Any]) -> tuple[dict[int, int], dict[int, str]]:
    cardinalities: dict[int, int] = {}
    names: dict[int, str] = {}
    raw = entry.get("categorical_columns", [])
    if isinstance(raw, dict):
        for key, value in raw.items():
            index = int(key)
            if isinstance(value, dict):
                cardinalities[index] = int(value.get("cardinality"))
                if value.get("name"):
                    names[index] = str(value["name"])
            else:
                cardinalities[index] = int(value)
    elif isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                index = int(item.get("index"))
                if "cardinality" not in item:
                    raise WorkbenchError(
                        "E_INPUT_INVALID",
                        "Categorical column declaration requires cardinality",
                        {"entry": entry.get("file"), "column_index": index},
                    )
                cardinalities[index] = int(item["cardinality"])
                if item.get("name"):
                    names[index] = str(item["name"])
            else:
                cardinalities[int(item)] = -1
    else:
        raise WorkbenchError(
            "E_INPUT_INVALID",
            "categorical_columns must be a list or mapping",
            {"entry": entry.get("file")},
        )
    return cardinalities, names


def _parse_declared_types(
    entry: dict[str, Any],
    count: int,
    categorical_indices: set[int],
    label_index: int,
) -> dict[int, str]:
    raw = entry.get("column_types")
    declared: dict[int, str] = {}
    if isinstance(raw, list):
        if len(raw) != count:
            raise WorkbenchError(
                "E_COLUMN_MISMATCH",
                "Declared column_types length does not match the declared column count",
                {
                    "column": "<column_types>",
                    "declared": len(raw),
                    "observed": count,
                    "file": entry.get("file"),
                },
            )
        declared = {index: str(value) for index, value in enumerate(raw)}
    elif isinstance(raw, dict):
        for key, value in raw.items():
            declared[int(key)] = str(value)
    elif raw is not None:
        raise WorkbenchError(
            "E_INPUT_INVALID",
            "column_types must be a list or mapping",
            {"entry": entry.get("file")},
        )
    for index in range(count):
        declared.setdefault(
            index,
            "label" if index == label_index else ("categorical" if index in categorical_indices else "numeric"),
        )
    if label_index in declared:
        declared[label_index] = "label"
    return declared


def build_shard_specs(manifest: dict[str, Any], data_dir: Path) -> list[ShardSpec]:
    data_dir = Path(data_dir)
    prepared: list[tuple[tuple[int, int, str], dict[str, Any], Path, str]] = []
    tier_order = {str(tier): index for index, tier in enumerate(manifest.get("tiers", {}))}
    for entry in manifest["files"]:
        if not isinstance(entry, dict):
            raise WorkbenchError("E_INPUT_INVALID", "Each manifest file entry must be an object")
        file_name = entry.get("file")
        path = _safe_data_path(data_dir, file_name)
        tier = str(entry.get("tier", "default"))
        split_index = int(entry.get("split_index", 0))
        order_key = (tier_order.get(tier, len(tier_order)), split_index, str(file_name))
        prepared.append((order_key, entry, path, tier))
    prepared.sort(key=lambda item: item[0])

    specs: list[ShardSpec] = []
    for shard_index, (_, entry, path, tier) in enumerate(prepared):
        declared_columns = _require_int(entry, "columns", minimum=1)
        declared_rows = _require_int(entry, "rows", minimum=0)
        split_index = int(entry.get("split_index", shard_index))
        label_index = int(entry.get("label_column", declared_columns - 1))
        if not (0 <= label_index < declared_columns):
            raise WorkbenchError(
                "E_INPUT_INVALID",
                "label_column is outside the declared column range",
                {"file": entry.get("file"), "label_column": label_index},
            )
        categorical_cardinalities, categorical_names = _parse_categorical_columns(entry)
        for index, cardinality in categorical_cardinalities.items():
            if not (0 <= index < declared_columns):
                raise WorkbenchError(
                    "E_COLUMN_MISMATCH",
                    "Categorical column index is outside the declared range",
                    {
                        "column": categorical_names.get(index, index),
                        "column_index": index,
                        "declared": index,
                        "observed": declared_columns,
                        "file": entry.get("file"),
                    },
                )
            if cardinality < 1:
                raise WorkbenchError(
                    "E_INPUT_INVALID",
                    "Categorical cardinality must be positive",
                    {"file": entry.get("file"), "column_index": index, "cardinality": cardinality},
                )
        missing_columns = [int(index) for index in entry.get("missing_columns", [])]
        for index in missing_columns:
            if not (0 <= index < declared_columns):
                raise WorkbenchError(
                    "E_COLUMN_MISMATCH",
                    "Missing column index is outside the declared range",
                    {
                        "column": str(index),
                        "column_index": index,
                        "declared": index,
                        "observed": declared_columns,
                        "file": entry.get("file"),
                    },
                )
        declared_types = _parse_declared_types(
            entry,
            declared_columns,
            set(categorical_cardinalities),
            label_index,
        )
        names = entry.get("column_names")
        if names is not None and (not isinstance(names, list) or len(names) != declared_columns):
            raise WorkbenchError(
                "E_INPUT_INVALID",
                "column_names must match the declared column count",
                {"file": entry.get("file")},
            )
        specs.append(
            ShardSpec(
                file=str(entry["file"]),
                path=path,
                tier=tier,
                shard_index=shard_index,
                split_index=split_index,
                declared_rows=declared_rows,
                declared_columns=declared_columns,
                declared_column_names=[str(value) for value in names] if names else None,
                declared_types=declared_types,
                categorical_cardinalities=categorical_cardinalities,
                categorical_names=categorical_names,
                missing_columns=missing_columns,
                label_column=label_index,
                content_sha256=file_sha256(path),
            )
        )
    return specs


def read_csv_table(path: Path) -> tuple[list[str], list[list[str]]]:
    path = Path(path)
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            try:
                header = next(reader)
            except StopIteration as exc:
                raise WorkbenchError(
                    "E_INPUT_INVALID",
                    "CSV file is empty",
                    {"path": str(path)},
                ) from exc
            if not header or any(not name for name in header):
                raise WorkbenchError(
                    "E_INPUT_INVALID",
                    "CSV header contains an empty column name",
                    {"path": str(path), "header": header},
                )
            if len(header) != len(set(header)):
                raise WorkbenchError(
                    "E_INPUT_INVALID",
                    "CSV header contains duplicate column names",
                    {"path": str(path), "header": header},
                )
            rows: list[list[str]] = []
            for line_number, row in enumerate(reader, start=2):
                if len(row) != len(header):
                    raise WorkbenchError(
                        "E_INPUT_INVALID",
                        "CSV row has a different field count from the header",
                        {
                            "path": str(path),
                            "line": line_number,
                            "expected": len(header),
                            "observed": len(row),
                        },
                    )
                rows.append(row)
    except OSError as exc:
        raise WorkbenchError(
            "E_INPUT_INVALID",
            "CSV file could not be read",
            {"path": str(path), "reason": str(exc)},
        ) from exc
    return header, rows


def is_missing(value: str) -> bool:
    return value.strip().lower() in MISSING_TOKENS


def parse_number(value: str, *, column: str, path: Path, index: int, row_number: int) -> float:
    try:
        parsed = float(value.strip())
    except (TypeError, ValueError) as exc:
        raise WorkbenchError(
            "E_COLUMN_MISMATCH",
            "Column declaration conflicts with observed non-numeric values",
            {
                "column": column,
                "column_index": index,
                "declared": "numeric",
                "observed": [value],
                "file": str(path),
                "row": row_number,
            },
        ) from exc
    if not math.isfinite(parsed):
        raise WorkbenchError(
            "E_COLUMN_MISMATCH",
            "Column declaration conflicts with observed non-finite values",
            {
                "column": column,
                "column_index": index,
                "declared": "numeric",
                "observed": [value],
                "file": str(path),
                "row": row_number,
            },
        )
    return parsed


def load_table(spec: ShardSpec) -> Table:
    header, rows = read_csv_table(spec.path)
    if len(header) != spec.declared_columns:
        raise WorkbenchError(
            "E_COLUMN_MISMATCH",
            "Observed column count conflicts with the declaration",
            {
                "column": "<column_count>",
                "declared": spec.declared_columns,
                "observed": len(header),
                "file": spec.file,
                "header": header,
            },
        )
    if spec.declared_column_names is not None and header != spec.declared_column_names:
        mismatch_index = next(
            (
                index
                for index, (declared, observed) in enumerate(zip(spec.declared_column_names, header))
                if declared != observed
            ),
            0,
        )
        raise WorkbenchError(
            "E_COLUMN_MISMATCH",
            "Observed column names conflict with the declaration",
            {
                "column": str(mismatch_index),
                "column_index": mismatch_index,
                "declared": spec.declared_column_names[mismatch_index],
                "observed": header[mismatch_index],
                "declared_columns": spec.declared_column_names,
                "observed_columns": header,
                "file": spec.file,
            },
        )
    if len(rows) != spec.declared_rows:
        raise WorkbenchError(
            "E_INPUT_INVALID",
            "Observed row count conflicts with the declaration",
            {
                "file": spec.file,
                "declared_rows": spec.declared_rows,
                "observed_rows": len(rows),
            },
        )
    table = Table(
        path=spec.path,
        tier=spec.tier,
        shard_index=spec.shard_index,
        split_index=spec.split_index,
        header=header,
        rows=rows,
        content_sha256=spec.content_sha256,
    )
    validate_table_against_declaration(table, spec)
    return table


def validate_table_against_declaration(table: Table, spec: ShardSpec) -> None:
    label_index = spec.label_column
    for index, column_name in enumerate(table.header):
        declared_type = spec.declared_types.get(index, "numeric")
        values = [row[index] for row in table.rows if not is_missing(row[index])]
        if declared_type == "label":
            if len(values) != table.row_count:
                raise WorkbenchError(
                    "E_INPUT_INVALID",
                    "Label column contains missing values",
                    {"column": column_name, "column_index": index, "file": spec.file},
                )
            invalid = []
            for value in values:
                try:
                    numeric_label = float(value.strip())
                except ValueError:
                    invalid.append(value)
                    continue
                if numeric_label not in {0.0, 1.0}:
                    invalid.append(value)
            if invalid:
                raise WorkbenchError(
                    "E_COLUMN_MISMATCH",
                    "Label column declaration conflicts with observed values",
                    {
                        "column": column_name,
                        "column_index": index,
                        "declared": "binary 0/1 label",
                        "observed": sorted(set(invalid))[:20],
                        "file": spec.file,
                    },
                )
        elif declared_type == "numeric":
            for row_number, row in enumerate(table.rows, start=2):
                if not is_missing(row[index]):
                    parse_number(
                        row[index],
                        column=column_name,
                        path=spec.path,
                        index=index,
                        row_number=row_number,
                    )
        elif declared_type == "categorical":
            declared_cardinality = spec.categorical_cardinalities.get(index)
            if declared_cardinality is not None:
                observed_values = sorted(set(values))
                if len(observed_values) != declared_cardinality:
                    raise WorkbenchError(
                        "E_COLUMN_MISMATCH",
                        "Categorical cardinality conflicts with the declaration",
                        {
                            "column": column_name,
                            "column_index": index,
                            "declared_cardinality": declared_cardinality,
                            "observed_cardinality": len(observed_values),
                            "declared": declared_cardinality,
                            "observed": observed_values,
                            "file": spec.file,
                        },
                    )
        else:
            raise WorkbenchError(
                "E_COLUMN_MISMATCH",
                "Unknown declared column type",
                {
                    "column": column_name,
                    "column_index": index,
                    "declared": declared_type,
                    "observed": sorted(set(values))[:20],
                    "file": spec.file,
                },
            )
    if spec.missing_columns:
        for index in spec.missing_columns:
            values = [row[index] for row in table.rows]
            has_missing = any(is_missing(value) for value in values)
            if not has_missing:
                # A shard can legitimately contain no missing value for a nullable column.
                continue
    if label_index < 0 or label_index >= table.column_count:
        raise WorkbenchError(
            "E_COLUMN_MISMATCH",
            "Declared label column does not exist",
            {
                "column": str(label_index),
                "declared": label_index,
                "observed": table.column_count,
                "file": spec.file,
            },
        )


def manifest_digest(manifest: dict[str, Any]) -> str:
    return digest_object(manifest)


def combined_shard_fingerprint(specs: Iterable[ShardSpec]) -> str:
    return digest_object(
        [
            {
                "file": spec.file,
                "tier": spec.tier,
                "shard_index": spec.shard_index,
                "content_sha256": spec.content_sha256,
            }
            for spec in sorted(specs, key=lambda item: item.shard_index)
        ]
    )


def _mode(values: list[str]) -> str:
    counts = Counter(values)
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def infer_type_schema(
    manifest: dict[str, Any],
    specs: list[ShardSpec],
    tables: dict[int, Table],
) -> dict[str, Any]:
    tiers: dict[str, Any] = {}
    by_tier: dict[str, list[ShardSpec]] = {}
    for spec in specs:
        by_tier.setdefault(spec.tier, []).append(spec)

    for tier, tier_specs in sorted(by_tier.items()):
        tier_specs = sorted(tier_specs, key=lambda item: item.shard_index)
        first_table = tables[tier_specs[0].shard_index]
        reference_header = first_table.header
        reference_label = tier_specs[0].label_column
        for spec in tier_specs[1:]:
            table = tables[spec.shard_index]
            if table.header != reference_header:
                mismatch_index = next(
                    (
                        index
                        for index, (left, right) in enumerate(zip(reference_header, table.header))
                        if left != right
                    ),
                    0,
                )
                raise WorkbenchError(
                    "E_COLUMN_MISMATCH",
                    "Column layout differs between shards in the same tier",
                    {
                        "column": reference_header[mismatch_index],
                        "column_index": mismatch_index,
                        "declared": reference_header[mismatch_index],
                        "observed": table.header[mismatch_index],
                        "tier": tier,
                        "file": spec.file,
                    },
                )
            if spec.label_column != reference_label:
                raise WorkbenchError(
                    "E_COLUMN_MISMATCH",
                    "Label column differs between shards in the same tier",
                    {
                        "column": reference_header[reference_label],
                        "column_index": reference_label,
                        "declared": reference_label,
                        "observed": spec.label_column,
                        "tier": tier,
                        "file": spec.file,
                    },
                )

        source_columns: list[dict[str, Any]] = []
        for index, column_name in enumerate(reference_header):
            all_values: list[str] = []
            missing_count = 0
            per_shard_cardinalities: dict[str, int] = {}
            for spec in tier_specs:
                table = tables[spec.shard_index]
                values = [row[index] for row in table.rows]
                present = [value for value in values if not is_missing(value)]
                missing_count += len(values) - len(present)
                all_values.extend(present)
                per_shard_cardinalities[spec.file] = len(set(present))

            declared_types = {spec.declared_types.get(index, "numeric") for spec in tier_specs}
            if len(declared_types) != 1:
                sorted_types = sorted(declared_types)
                raise WorkbenchError(
                    "E_COLUMN_MISMATCH",
                    "Column type is inconsistent across shards",
                    {
                        "column": column_name,
                        "column_index": index,
                        "declared": sorted_types[0],
                        "observed": sorted_types[1],
                        "declared_types": sorted_types,
                        "tier": tier,
                    },
                )
            declared_type = declared_types.pop()
            if declared_type == "label":
                dtype = "label"
            elif declared_type == "categorical":
                dtype = "categorical"
            elif declared_type == "numeric":
                dtype = "numeric"
            else:
                raise WorkbenchError(
                    "E_COLUMN_MISMATCH",
                    "Unknown declared column type",
                    {
                        "column": column_name,
                        "column_index": index,
                        "declared": declared_type,
                        "observed": "numeric-or-categorical",
                        "tier": tier,
                    },
                )

            entry: dict[str, Any] = {
                "index": index,
                "name": column_name,
                "dtype": dtype,
                "missing_count": missing_count,
                "observed_non_missing": len(all_values),
                "declared_cardinality": tier_specs[0].categorical_cardinalities.get(index),
                "per_shard_cardinalities": dict(sorted(per_shard_cardinalities.items())),
            }
            if dtype == "categorical":
                domain = sorted(set(all_values))
                entry["domain"] = domain
                entry["observed_cardinality"] = len(domain)
                entry["mode"] = _mode(all_values) if all_values else None
            elif dtype == "numeric":
                numbers = [float(value.strip()) for value in all_values]
                if numbers:
                    array = np.asarray(numbers, dtype=np.float64)
                    entry["domain"] = {
                        "min": stable_float(float(np.min(array))),
                        "max": stable_float(float(np.max(array))),
                        "mean": stable_float(float(np.mean(array))),
                    }
                    entry["mean"] = stable_float(float(np.mean(array)))
                    entry["median"] = stable_float(float(np.median(array)))
                    mode_counts = Counter(numbers)
                    mode_value = sorted(mode_counts.items(), key=lambda item: (-item[1], item[0]))[0][0]
                    entry["mode"] = stable_float(mode_value)
                else:
                    entry["domain"] = {"min": None, "max": None, "mean": None}
                    entry["mean"] = 0.0
                    entry["median"] = 0.0
                    entry["mode"] = 0.0
            else:
                entry["domain"] = [0, 1]
            source_columns.append(entry)

        label_name = reference_header[reference_label]
        tier_rows = sum(tables[spec.shard_index].row_count for spec in tier_specs)
        tiers[tier] = {
            "tier": tier,
            "rows": tier_rows,
            "columns": len(reference_header),
            "shards": [spec.shard_index for spec in tier_specs],
            "shard_files": [spec.file for spec in tier_specs],
            "label_column": {"index": reference_label, "name": label_name},
            "source_columns": source_columns,
        }

    schema = {
        "schema_version": 1,
        "manifest_seed": manifest.get("seed"),
        "tiers": tiers,
    }
    schema["schema_digest"] = digest_object(schema)
    return schema


def schema_bytes(schema: dict[str, Any]) -> bytes:
    return canonical_json_bytes(schema)

