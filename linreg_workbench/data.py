"""Dataset declaration loading, validation and local sample import."""

from __future__ import annotations

import csv
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .canonical import atomic_write_json, digest, read_json, sha256_file
from .errors import InvalidData, NotFound

_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


@dataclass(frozen=True)
class DatasetSpec:
    dataset_id: str
    file_path: Path
    declaration: dict[str, Any]
    declaration_path: Path

    @property
    def sha256(self) -> str:
        return self.declaration["sha256"]

    @property
    def rows(self) -> int:
        return int(self.declaration["rows"])

    @property
    def columns(self) -> int:
        return int(self.declaration["columns"])

    @property
    def target(self) -> dict[str, Any]:
        return self.declaration["target"]

    @property
    def features(self) -> list[dict[str, Any]]:
        return self.declaration["features"]

    @property
    def semantics_digest(self) -> str:
        return digest(
            {
                "dataset_id": self.dataset_id,
                "columns": self.declaration.get("column_names"),
                "target": self.target,
                "features": self.features,
            }
        )

    def public_summary(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "file": str(self.file_path),
            "sha256": self.sha256,
            "rows": self.rows,
            "columns": self.columns,
            "target": self.target,
            "feature_count": len(self.features),
            "continuous_count": sum(1 for item in self.features if item["role"] == "continuous"),
            "categorical_count": sum(1 for item in self.features if item["role"] == "categorical"),
            "missing_columns": self.declaration.get("missing_columns", []),
            "redundant_columns": self.declaration.get("redundant_columns", []),
            "class_counts": self.declaration.get("class_counts", {}),
            "notes": self.declaration.get("notes", []),
        }


def read_csv(path: Path) -> tuple[list[str], list[list[str]]]:
    try:
        with open(path, "r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            rows = list(reader)
    except (OSError, UnicodeError, csv.Error) as exc:
        raise InvalidData(
            "Could not read CSV dataset", details={"path": str(path), "reason": str(exc)}
        ) from exc
    if not rows:
        raise InvalidData("CSV dataset is empty", details={"path": str(path)})
    header = [item.strip() for item in rows[0]]
    if len(set(header)) != len(header):
        raise InvalidData("CSV header contains duplicate names")
    body = rows[1:]
    for index, row in enumerate(body, start=2):
        if len(row) != len(header):
            raise InvalidData(
                "CSV row has the wrong number of columns",
                details={"line": index, "expected": len(header), "actual": len(row)},
            )
    return header, body


class DatasetRegistry:
    """Bundled declaration plus imported datasets stored in the workspace."""

    def __init__(self, data_dir: str | Path, workspace_dir: str | Path) -> None:
        self.data_dir = Path(data_dir).resolve()
        self.workspace_dir = Path(workspace_dir).resolve()
        self.declaration_path = self.data_dir / "dataset_declaration.json"
        self.imports_dir = self.workspace_dir / "imports"
        self.import_catalog = self.imports_dir / "catalog.json"

    def _bundled(self) -> dict[str, dict[str, Any]]:
        if not self.declaration_path.exists():
            return {}
        payload = read_json(self.declaration_path)
        result: dict[str, dict[str, Any]] = {}
        for item in payload.get("datasets", []):
            entry = dict(item)
            entry["_base"] = self.data_dir
            entry["_declaration_path"] = self.declaration_path
            result[item["dataset_id"]] = entry
        return result

    def _imported(self) -> dict[str, dict[str, Any]]:
        if not self.import_catalog.exists():
            return {}
        payload = read_json(self.import_catalog)
        result: dict[str, dict[str, Any]] = {}
        for item in payload.get("datasets", []):
            entry = dict(item)
            entry["_base"] = self.workspace_dir
            entry["_declaration_path"] = self.imports_dir / item["dataset_id"] / "declaration.json"
            result[item["dataset_id"]] = entry
        return result

    def list(self) -> list[DatasetSpec]:
        entries = self._bundled()
        entries.update(self._imported())
        return [self._to_spec(item) for _, item in sorted(entries.items())]

    def list_public(self) -> list[dict[str, Any]]:
        return [item.public_summary() for item in self.list()]

    def get(self, dataset_id: str) -> DatasetSpec:
        if not isinstance(dataset_id, str) or not _ID_RE.match(dataset_id):
            raise InvalidData(
                "Invalid dataset id", details={"dataset_id": str(dataset_id)}
            )
        entries = self._bundled()
        entries.update(self._imported())
        if dataset_id not in entries:
            raise NotFound(
                "Dataset is not registered in this workspace",
                details={"dataset_id": dataset_id, "available": sorted(entries)},
            )
        return self._to_spec(entries[dataset_id])

    def _to_spec(self, entry: dict[str, Any]) -> DatasetSpec:
        file_name = entry.get("file")
        base = Path(entry["_base"])
        path = Path(file_name)
        if not path.is_absolute():
            path = base / path
        path = path.resolve()
        declaration = {key: value for key, value in entry.items() if not key.startswith("_")}
        decl_path = Path(entry["_declaration_path"]).resolve()
        if not path.exists():
            raise InvalidData(
                "Declared dataset file does not exist",
                details={"dataset_id": entry["dataset_id"], "path": str(path)},
            )
        actual_sha = sha256_file(path)
        if actual_sha != declaration.get("sha256"):
            raise InvalidData(
                "Dataset content does not match its declaration checksum",
                details={
                    "dataset_id": entry["dataset_id"],
                    "expected_sha256": declaration.get("sha256"),
                    "actual_sha256": actual_sha,
                },
            )
        return DatasetSpec(
            dataset_id=entry["dataset_id"],
            file_path=path,
            declaration=declaration,
            declaration_path=decl_path,
        )

    def import_dataset(
        self,
        csv_path: str | Path,
        dataset_id: str,
        *,
        target: str | None = None,
        features: list[dict[str, Any]] | None = None,
        override: bool = False,
    ) -> DatasetSpec:
        if not isinstance(dataset_id, str) or not _ID_RE.match(dataset_id):
            raise InvalidData("Invalid dataset id", details={"dataset_id": dataset_id})
        source = Path(csv_path).expanduser().resolve()
        if not source.exists():
            raise NotFound("CSV file was not found", details={"path": str(source)})
        existing = {item.dataset_id for item in self.list()}
        if dataset_id in existing and not override:
            raise InvalidData(
                "Dataset id already exists",
                details={"dataset_id": dataset_id, "hint": "use override=true"},
            )
        header, rows = read_csv(source)
        if target is None:
            if "label" in header:
                target = "label"
            elif "target" in header:
                target = "target"
            else:
                target = header[-1]
        if target not in header:
            raise InvalidData(
                "Target column is not present", details={"target": target, "columns": header}
            )
        if not rows:
            raise InvalidData("Dataset contains no data rows")
        declared = {item["name"]: item for item in (features or [])}
        generated_features: list[dict[str, Any]] = []
        for name in header:
            if name == target:
                continue
            supplied = declared.get(name)
            if supplied is not None:
                if supplied.get("role") not in {"continuous", "categorical"}:
                    raise InvalidData(
                        "Imported feature role must be continuous or categorical",
                        details={"column": name, "role": supplied.get("role")},
                    )
                generated_features.append(dict(supplied))
                continue
            values = [row[header.index(name)].strip() for row in rows if row[header.index(name)].strip()]
            numeric = True
            for value in values[:200]:
                try:
                    float(value)
                except ValueError:
                    numeric = False
                    break
            unique = sorted(set(values))
            item: dict[str, Any] = {
                "name": name,
                "role": "continuous" if numeric else "categorical",
                "dtype": "float" if numeric else "string",
                "semantic": "Inferred during local import.",
            }
            if not numeric:
                item["categories"] = unique
            generated_features.append(item)
        target_values = sorted({row[header.index(target)].strip() for row in rows})
        if len(target_values) != 2:
            raise InvalidData(
                "Target must have exactly two observed classes",
                details={"target": target, "classes": target_values},
            )
        target_decl = {
            "name": target,
            "role": "target",
            "dtype": "binary",
            "classes": target_values,
            "negative_value": target_values[0],
            "positive_value": target_values[1],
            "semantic": "Imported binary target.",
        }
        if len(generated_features) == 0:
            raise InvalidData("Dataset must contain at least one feature")
        destination_dir = self.imports_dir / dataset_id
        destination_dir.mkdir(parents=True, exist_ok=True)
        destination_csv = destination_dir / "data.csv"
        shutil.copyfile(source, destination_csv)
        sha = sha256_file(destination_csv)
        missing_columns = []
        for index, name in enumerate(header):
            count = sum(1 for row in rows if row[index].strip() == "")
            if count:
                role = "target" if name == target else next(
                    (item["role"] for item in generated_features if item["name"] == name),
                    "feature",
                )
                missing_columns.append(
                    {
                        "column": name,
                        "role": role,
                        "missing_count": count,
                        "missing_rate": count / len(rows),
                    }
                )
        declaration = {
            "dataset_id": dataset_id,
            "file": str(destination_csv.relative_to(self.workspace_dir)),
            "sha256": sha,
            "rows": len(rows),
            "columns": len(header),
            "column_names": header,
            "target": target_decl,
            "features": generated_features,
            "missing_columns": missing_columns,
            "redundant_columns": [],
            "class_counts": {
                value: sum(1 for row in rows if row[header.index(target)].strip() == value)
                for value in target_values
            },
            "seed": None,
            "notes": ["Imported locally; checksum captured at import time."],
        }
        atomic_write_json(destination_dir / "declaration.json", declaration)
        catalog = {"schema_version": 1, "datasets": []}
        if self.import_catalog.exists():
            catalog = read_json(self.import_catalog)
        catalog["datasets"] = [
            item for item in catalog.get("datasets", []) if item.get("dataset_id") != dataset_id
        ]
        catalog["datasets"].append(declaration)
        catalog["datasets"].sort(key=lambda item: item["dataset_id"])
        atomic_write_json(self.import_catalog, catalog)
        return self.get(dataset_id)
