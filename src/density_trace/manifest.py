from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .canonical import canonical_json_bytes, read_json, sha256_bytes, sha256_file
from .errors import (
    E_EMPTY_BATCH,
    E_INPUT_CONTRACT,
    E_INVALID_STAGE,
    E_MANIFEST_INVALID,
    E_NUMERIC,
    business_error,
)


@dataclass(frozen=True)
class BatchSpec:
    batch_id: int
    file: str
    rows: int
    cols: int
    valid_columns: tuple[int, ...]
    source_label: str
    feature_key: str = "features"
    label_key: str = "source_batch"

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "file": self.file,
            "rows": self.rows,
            "cols": self.cols,
            "valid_columns": list(self.valid_columns),
            "source_label": self.source_label,
            "feature_key": self.feature_key,
            "label_key": self.label_key,
        }


@dataclass(frozen=True)
class Manifest:
    path: Path
    schema_version: int
    batches: tuple[BatchSpec, ...]

    def by_id(self, batch_id: int) -> BatchSpec:
        if batch_id < 0 or batch_id >= len(self.batches):
            raise business_error(
                E_INVALID_STAGE,
                "batch id is outside the manifest",
                batch_id=batch_id,
                available_batch_ids=[spec.batch_id for spec in self.batches],
            )
        spec = self.batches[batch_id]
        if spec.batch_id != batch_id:
            raise business_error(
                E_INVALID_STAGE,
                "manifest batch ids must be contiguous and zero based",
                batch_id=batch_id,
                manifest_batch_id=spec.batch_id,
            )
        return spec

    def resolve(self, spec: BatchSpec) -> Path:
        path = Path(spec.file)
        if not path.is_absolute():
            path = self.path.parent / path
        return path.resolve()

    def input_digest(self, spec: BatchSpec) -> str:
        file_digest = sha256_file(self.resolve(spec))
        payload = {"file_digest": file_digest, "manifest_entry": spec.to_dict()}
        return sha256_bytes(canonical_json_bytes(payload))

    def file_digest(self) -> str:
        return sha256_file(self.path)


@dataclass(frozen=True)
class LoadedBatch:
    spec: BatchSpec
    file_path: Path
    input_digest: str
    raw_shape: tuple[int, int]
    effective_samples: np.ndarray
    dropped_rows: int
    source_labels: tuple[str, ...]


def load_manifest(path: str | Path) -> Manifest:
    manifest_path = Path(path).resolve()
    try:
        raw = read_json(manifest_path)
        schema_version = int(raw.get("schema_version", 1))
        entries = raw["batches"]
        batches: list[BatchSpec] = []
        for expected_id, entry in enumerate(entries):
            batch_id = int(entry.get("batch_id", expected_id))
            valid_columns = tuple(int(index) for index in entry["valid_columns"])
            spec = BatchSpec(
                batch_id=batch_id,
                file=str(entry["file"]),
                rows=int(entry["rows"]),
                cols=int(entry["cols"]),
                valid_columns=valid_columns,
                source_label=str(entry.get("source_label", f"batch-{batch_id}")),
                feature_key=str(entry.get("feature_key", "features")),
                label_key=str(entry.get("label_key", "source_batch")),
            )
            if spec.batch_id != expected_id:
                raise business_error(
                    E_MANIFEST_INVALID,
                    "manifest batch ids must be contiguous and zero based",
                    batch_id=expected_id,
                    found=spec.batch_id,
                )
            if spec.rows < 0 or spec.cols < 0:
                raise business_error(
                    E_MANIFEST_INVALID,
                    "rows and cols must be nonnegative",
                    batch_id=spec.batch_id,
                )
            if any(index < 0 or index >= spec.cols for index in spec.valid_columns):
                raise business_error(
                    E_MANIFEST_INVALID,
                    "valid_columns contains an out-of-range index",
                    batch_id=spec.batch_id,
                    valid_columns=list(spec.valid_columns),
                    cols=spec.cols,
                )
            batches.append(spec)
        if not batches:
            raise business_error(
                E_MANIFEST_INVALID, "manifest must contain at least one batch"
            )
        manifest = Manifest(manifest_path, schema_version, tuple(batches))
        for spec in manifest.batches:
            if not manifest.resolve(spec).is_file():
                raise business_error(
                    E_MANIFEST_INVALID,
                    "batch file does not exist",
                    batch_id=spec.batch_id,
                    file=str(manifest.resolve(spec)),
                )
        return manifest
    except (KeyError, TypeError, ValueError) as exc:
        raise business_error(E_MANIFEST_INVALID, f"invalid manifest: {exc}") from exc


def load_batch(manifest: Manifest, batch_id: int) -> LoadedBatch:
    spec = manifest.by_id(batch_id)
    path = manifest.resolve(spec)
    try:
        with np.load(path, allow_pickle=False) as archive:
            if spec.feature_key not in archive:
                raise business_error(
                    E_MANIFEST_INVALID,
                    "feature array is missing from NPZ",
                    batch_id=batch_id,
                    feature_key=spec.feature_key,
                )
            features = np.asarray(archive[spec.feature_key], dtype=np.float64)
            labels = archive.get(spec.label_key, None)
    except business_error:
        raise
    except Exception as exc:
        raise business_error(
            E_MANIFEST_INVALID,
            f"failed to read batch NPZ: {exc}",
            batch_id=batch_id,
            file=str(path),
        ) from exc
    if features.ndim != 2:
        raise business_error(
            E_INPUT_CONTRACT,
            "features must be a two-dimensional array",
            batch_id=batch_id,
        )
    if features.shape != (spec.rows, spec.cols):
        raise business_error(
            E_INPUT_CONTRACT,
            "NPZ shape does not match manifest declaration",
            batch_id=batch_id,
            expected=[spec.rows, spec.cols],
            actual=list(features.shape),
        )
    if spec.rows == 0 or not spec.valid_columns:
        raise business_error(
            E_EMPTY_BATCH,
            "batch has no rows or no valid columns",
            batch_id=batch_id,
            rows=spec.rows,
            valid_columns=list(spec.valid_columns),
        )
    selected = features[:, spec.valid_columns]
    inf_mask = np.isinf(selected)
    if np.any(inf_mask):
        bad_rows, bad_columns = np.where(inf_mask)
        raise business_error(
            E_NUMERIC,
            "batch contains an infinite value in a valid column",
            batch_id=batch_id,
            anomaly=f"Inf at row={int(bad_rows[0])} column={int(spec.valid_columns[int(bad_columns[0])])}",
            grid_interval=[0, 0],
        )
    finite_rows = np.all(np.isfinite(selected), axis=1)
    effective = selected[finite_rows]
    if effective.shape[0] == 0:
        raise business_error(
            E_EMPTY_BATCH,
            "batch has no finite samples in its valid columns",
            batch_id=batch_id,
            rows=spec.rows,
            valid_columns=list(spec.valid_columns),
        )
    if effective.shape[1] != 2:
        raise business_error(
            E_INPUT_CONTRACT,
            "density grid is two-dimensional and requires exactly two valid columns",
            batch_id=batch_id,
            valid_columns=list(spec.valid_columns),
        )
    if labels is None:
        label_values = (spec.source_label,)
    else:
        label_array = np.asarray(labels).reshape(-1)
        if label_array.shape[0] != spec.rows:
            raise business_error(
                E_INPUT_CONTRACT,
                "source label length does not match rows",
                batch_id=batch_id,
                labels=int(label_array.shape[0]),
                rows=spec.rows,
            )
        label_values = tuple(sorted({str(value) for value in label_array.tolist()}))
    if not np.all(np.isfinite(effective)):
        raise business_error(
            E_NUMERIC,
            "non-finite samples remained after manifest-driven cleaning",
            batch_id=batch_id,
            anomaly="NaN or Inf",
            grid_interval=[0, 0],
        )
    return LoadedBatch(
        spec=spec,
        file_path=path,
        input_digest=manifest.input_digest(spec),
        raw_shape=(spec.rows, spec.cols),
        effective_samples=effective.astype(np.float64, copy=False),
        dropped_rows=int(spec.rows - effective.shape[0]),
        source_labels=label_values,
    )
