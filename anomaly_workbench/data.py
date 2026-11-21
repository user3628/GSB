"""Deterministic local demo data, NPZ import and dataset fingerprints."""

from __future__ import annotations

import io
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from .errors import WorkbenchError
from .util import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    copy_atomic,
    digest_object,
    file_lock,
    read_json,
    sha256_bytes,
)

DEMO_SEED = 20261122
TIER_SPECS: dict[str, dict[str, Any]] = {
    "small": {
        "rows": 800,
        "columns": 10,
        "shard_rows": [200, 200, 200, 200],
        "missing_columns": [2, 7],
        "heavy_tail_columns": [3, 8],
        "redundant_groups": [[0, 1], [4, 9]],
        "seed_offset": 101,
        "source_count": 5,
    },
    "medium": {
        "rows": 6000,
        "columns": 30,
        "shard_rows": [1200, 1200, 1200, 1200, 1200],
        "missing_columns": [4, 13, 22],
        "heavy_tail_columns": [5, 11, 24],
        "redundant_groups": [[0, 2], [6, 18], [10, 29]],
        "seed_offset": 202,
        "source_count": 7,
    },
}

DECLARED_STATISTICS = {
    "center": "median",
    "scale": "interquartile_range / 1.349",
    "quantile_method": "linear",
    "missing_values": "filled per configured strategy before scaling",
    "normalization_basis": "per-detector training-score median and IQR in log/logit domain",
    "threshold_basis": "empirical quantile of the combined score",
}


def _validate_tier_name(tier: str) -> None:
    if not isinstance(tier, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", tier):
        raise WorkbenchError(
            "E_DATA",
            "tier must be a short local identifier",
            stage="import",
            details={"tier": tier},
        )


def _rng_for_tier(seed: int, tier: str) -> np.random.Generator:
    spec = TIER_SPECS[tier]
    return np.random.Generator(np.random.PCG64(seed + int(spec["seed_offset"])))


def make_arrays(
    tier: str, *, seed: int = DEMO_SEED
) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict[str, Any]]:
    """Build one deterministic tier before it is split into batch shards."""

    if tier not in TIER_SPECS:
        raise WorkbenchError("E_DATA", f"unknown demo tier: {tier}", stage="import")
    spec = TIER_SPECS[tier]
    rows = int(spec["rows"])
    columns = int(spec["columns"])
    rng = _rng_for_tier(seed, tier)
    scales = 0.75 + rng.random(columns) * 3.5
    offsets = rng.normal(0.0, 5.0, size=columns)
    features = rng.standard_normal((rows, columns)) * scales + offsets

    for column in spec["heavy_tail_columns"]:
        features[:, column] = (
            rng.standard_t(df=3.0, size=rows) * scales[column] + offsets[column]
        )

    for base, duplicate in spec["redundant_groups"]:
        noise = rng.normal(0.0, 0.015 * scales[base], size=rows)
        features[:, duplicate] = features[:, base] + noise

    outlier_count = max(5, int(rows * 0.025))
    outlier_rows = rng.choice(rows, size=outlier_count, replace=False)
    for row in outlier_rows:
        count = 1 + int(rng.integers(0, 2))
        chosen = rng.choice(columns, size=count, replace=False)
        for column in chosen:
            magnitude = (6.0 + float(rng.random()) * 5.0) * scales[column]
            sign = -1.0 if float(rng.random()) < 0.5 else 1.0
            features[int(row), int(column)] += sign * magnitude

    for column in spec["missing_columns"]:
        missing_count = max(3, int(rows * 0.012))
        indexes = rng.choice(rows, size=missing_count, replace=False)
        features[indexes, column] = np.nan

    source_ids = np.asarray(
        [f"source-{int(value):02d}" for value in rng.integers(0, spec["source_count"], size=rows)],
        dtype="<U16",
    )
    batch_ids = np.empty(rows, dtype=np.int64)
    cursor = 0
    shard_entries: list[dict[str, Any]] = []
    for batch_id, shard_rows in enumerate(spec["shard_rows"]):
        batch_ids[cursor : cursor + shard_rows] = batch_id
        shard_entries.append(
            {
                "batch_id": batch_id,
                "file": f"batch-{batch_id:03d}.npz",
                "rows": int(shard_rows),
            }
        )
        cursor += int(shard_rows)

    declaration = {
        "schema_version": 1,
        "tier": tier,
        "seed": int(seed),
        "rows": rows,
        "columns": columns,
        "missing_column_indices": list(spec["missing_columns"]),
        "heavy_tail_column_indices": list(spec["heavy_tail_columns"]),
        "redundant_column_groups": [list(group) for group in spec["redundant_groups"]],
        "continuous_column_indices": list(range(columns)),
        "expected_statistics": dict(DECLARED_STATISTICS),
        "shards": shard_entries,
    }
    return features, source_ids, batch_ids, declaration


def _npz_bytes(features: np.ndarray, source_ids: np.ndarray, batch_ids: np.ndarray) -> bytes:
    buffer = io.BytesIO()
    np.savez(
        buffer,
        features=np.asarray(features, dtype=np.float64),
        source_ids=np.asarray(source_ids, dtype="<U16"),
        batch_ids=np.asarray(batch_ids, dtype=np.int64),
    )
    return buffer.getvalue()


def _write_npz(
    path: Path, features: np.ndarray, source_ids: np.ndarray, batch_ids: np.ndarray
) -> None:
    atomic_write_bytes(path, _npz_bytes(features, source_ids, batch_ids))


def generate_demo(root: str | os.PathLike[str], *, seed: int = DEMO_SEED, force: bool = False) -> dict[str, Any]:
    """Create both deterministic tiers, declarations and a manifest."""

    root_path = Path(root)
    manifest_path = root_path / "manifest.json"
    if manifest_path.exists() and not force:
        raise WorkbenchError(
            "E_DATA",
            "workspace already contains a manifest; pass force=True to regenerate",
            stage="import",
        )
    manifest: dict[str, Any] = {"schema_version": 1, "seed": int(seed), "tiers": {}}
    for tier in TIER_SPECS:
        features, source_ids, batch_ids, declaration = make_arrays(tier, seed=seed)
        tier_dir = root_path / "data" / tier
        cursor = 0
        shard_records: list[dict[str, Any]] = []
        for entry in declaration["shards"]:
            rows = int(entry["rows"])
            path = tier_dir / entry["file"]
            _write_npz(
                path,
                features[cursor : cursor + rows],
                source_ids[cursor : cursor + rows],
                batch_ids[cursor : cursor + rows],
            )
            shard_records.append(
                {
                    "path": str(path.relative_to(root_path)),
                    "batch_id": int(entry["batch_id"]),
                    "rows": rows,
                }
            )
            cursor += rows
        atomic_write_json(tier_dir / "declaration.json", declaration)
        manifest["tiers"][tier] = {
            "declaration": str((tier_dir / "declaration.json").relative_to(root_path)),
            "shards": shard_records,
        }
    atomic_write_json(manifest_path, manifest)
    return manifest


def _load_npz(path: Path) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    try:
        with np.load(path, allow_pickle=False) as archive:
            feature_key = "features" if "features" in archive.files else "X"
            if feature_key not in archive.files:
                raise WorkbenchError("E_DATA", "NPZ shard is missing features/X", stage="import")
            if "source_ids" not in archive.files or "batch_ids" not in archive.files:
                raise WorkbenchError(
                    "E_DATA", "NPZ shard must contain source_ids and batch_ids", stage="import"
                )
            features = np.asarray(archive[feature_key], dtype=np.float64)
            source_ids = np.asarray(archive["source_ids"])
            batch_ids = np.asarray(archive["batch_ids"])
    except WorkbenchError:
        raise
    except Exception as exc:
        raise WorkbenchError("E_DATA", f"cannot read NPZ shard: {exc}", stage="import") from exc
    if features.ndim != 2:
        raise WorkbenchError("E_DATA", "features must be a 2-D matrix", stage="import")
    if source_ids.ndim != 1 or batch_ids.ndim != 1:
        raise WorkbenchError("E_DATA", "source_ids and batch_ids must be 1-D", stage="import")
    if not (len(features) == len(source_ids) == len(batch_ids)):
        raise WorkbenchError("E_DATA", "shard array lengths do not match", stage="import")
    if np.any(np.isinf(features)):
        raise WorkbenchError("E_DATA", "features contain Inf; only NaN is accepted as missing", stage="import")
    if len(batch_ids) and len(set(int(value) for value in batch_ids.tolist())) != 1:
        raise WorkbenchError("E_DATA", "each shard must contain exactly one batch_id", stage="import")
    return features, source_ids.astype(str), batch_ids.astype(np.int64)


def _array_digest(array: np.ndarray, label: str) -> str:
    contiguous = np.ascontiguousarray(array)
    header = canonical_json_bytes(
        {"label": label, "dtype": contiguous.dtype.str, "shape": list(contiguous.shape)}
    )
    return sha256_bytes(header + b"\0" + contiguous.tobytes(order="C"))


def validate_declaration(
    declaration: Any,
    *,
    tier: str,
    shards: Iterable[tuple[dict[str, Any], np.ndarray, np.ndarray, np.ndarray]],
) -> None:
    if not isinstance(declaration, dict):
        raise WorkbenchError("E_DATA", "declaration must be a JSON object", stage="import")
    if declaration.get("tier") not in {None, tier}:
        raise WorkbenchError(
            "E_DATA",
            "declaration tier does not match import tier",
            stage="import",
            details={"declaration_tier": declaration.get("tier"), "tier": tier},
        )
    rows = declaration.get("rows")
    columns = declaration.get("columns")
    if not isinstance(rows, int) or not isinstance(columns, int) or rows < 1 or columns < 1:
        raise WorkbenchError("E_DATA", "declaration rows/columns must be positive integers", stage="import")
    shard_rows = 0
    observed_missing: set[int] = set()
    for metadata, features, _, _ in shards:
        shard_rows += len(features)
        if features.shape[1] != columns:
            raise WorkbenchError(
                "E_DATA",
                "shard column count differs from declaration",
                stage="import",
                details={"expected": columns, "actual": int(features.shape[1]), "shard": metadata.get("path")},
            )
        missing = np.flatnonzero(np.any(np.isnan(features), axis=0)).tolist()
        observed_missing.update(int(value) for value in missing)
    if shard_rows != rows:
        raise WorkbenchError(
            "E_DATA",
            "declared row count differs from imported shards",
            stage="import",
            details={"declared": rows, "observed": shard_rows},
        )
    declared_missing_raw = declaration.get("missing_column_indices", [])
    if not isinstance(declared_missing_raw, list):
        raise WorkbenchError("E_DATA", "missing_column_indices must be a list", stage="import")
    declared_missing = {int(value) for value in declared_missing_raw}
    if any(value < 0 or value >= columns for value in declared_missing):
        raise WorkbenchError("E_DATA", "missing column index outside matrix", stage="import")
    if not observed_missing.issubset(declared_missing):
        raise WorkbenchError(
            "E_DATA",
            "observed missing columns are not declared",
            stage="import",
            details={
                "declared": sorted(declared_missing),
                "observed": sorted(observed_missing),
                "undeclared": sorted(observed_missing - declared_missing),
            },
        )
    for field in ("heavy_tail_column_indices", "continuous_column_indices"):
        values = declaration.get(field, [])
        if not isinstance(values, list) or any(int(value) < 0 or int(value) >= columns for value in values):
            raise WorkbenchError("E_DATA", f"invalid {field}", stage="import")
    groups = declaration.get("redundant_column_groups", [])
    if not isinstance(groups, list):
        raise WorkbenchError("E_DATA", "redundant_column_groups must be a list", stage="import")
    for group in groups:
        if not isinstance(group, list) or len(group) != 2:
            raise WorkbenchError("E_DATA", "redundant groups must be pairs", stage="import")
        if any(int(value) < 0 or int(value) >= columns for value in group):
            raise WorkbenchError("E_DATA", "redundant column index outside matrix", stage="import")


@dataclass(frozen=True)
class TierData:
    tier: str
    features: np.ndarray
    source_ids: np.ndarray
    batch_ids: np.ndarray
    declaration: dict[str, Any]
    declaration_digest: str
    data_fingerprint: str
    shard_digests: list[dict[str, Any]]
    shard_paths: list[str]

    @property
    def row_count(self) -> int:
        return int(self.features.shape[0])

    @property
    def column_count(self) -> int:
        return int(self.features.shape[1])

    def observation(self) -> dict[str, Any]:
        return {
            "tier": self.tier,
            "rows": self.row_count,
            "columns": self.column_count,
            "data_fingerprint": self.data_fingerprint,
            "declaration_digest": self.declaration_digest,
            "missing_column_indices": self.declaration.get("missing_column_indices", []),
            "heavy_tail_column_indices": self.declaration.get("heavy_tail_column_indices", []),
            "shard_count": len(self.shard_digests),
            "shards": self.shard_digests,
            "expected_statistics": self.declaration.get("expected_statistics", {}),
        }


def load_tier(root: str | os.PathLike[str], tier: str) -> TierData:
    _validate_tier_name(tier)
    root_path = Path(root)
    manifest_path = root_path / "manifest.json"
    if not manifest_path.exists():
        raise WorkbenchError("E_NOT_FOUND", "workspace manifest.json is missing", stage="import")
    manifest = read_json(manifest_path)
    entry = manifest.get("tiers", {}).get(tier)
    if not entry:
        raise WorkbenchError("E_NOT_FOUND", f"tier {tier!r} is not imported", stage="import")
    declaration_path = root_path / entry["declaration"]
    declaration = read_json(declaration_path)
    declaration_digest = sha256_bytes(canonical_json_bytes(declaration))
    shard_entries = sorted(entry.get("shards", []), key=lambda item: (int(item["batch_id"]), item["path"]))
    loaded: list[tuple[dict[str, Any], np.ndarray, np.ndarray, np.ndarray]] = []
    shard_digests: list[dict[str, Any]] = []
    paths: list[str] = []
    for metadata in shard_entries:
        path = root_path / metadata["path"]
        features, sources, batches = _load_npz(path)
        observed_batch = int(batches[0]) if len(batches) else int(metadata["batch_id"])
        if observed_batch != int(metadata["batch_id"]):
            raise WorkbenchError(
                "E_DATA",
                "manifest batch_id differs from shard contents",
                stage="import",
                details={"manifest": metadata["batch_id"], "observed": observed_batch},
            )
        metadata = {**metadata, "path": metadata["path"]}
        loaded.append((metadata, features, sources, batches))
        shard_digests.append(
            {
                "batch_id": int(metadata["batch_id"]),
                "rows": int(len(features)),
                "features_digest": _array_digest(features, "features"),
                "source_ids_digest": _array_digest(sources, "source_ids"),
                "batch_ids_digest": _array_digest(batches, "batch_ids"),
            }
        )
        paths.append(str(path))
    validate_declaration(declaration, tier=tier, shards=loaded)
    features = np.concatenate([item[1] for item in loaded], axis=0)
    source_ids = np.concatenate([item[2] for item in loaded], axis=0)
    batch_ids = np.concatenate([item[3] for item in loaded], axis=0)
    data_fingerprint = digest_object(
        {
            "tier": tier,
            "declaration_digest": declaration_digest,
            "shards": shard_digests,
        }
    )
    return TierData(
        tier=tier,
        features=np.asarray(features, dtype=np.float64),
        source_ids=np.asarray(source_ids, dtype=str),
        batch_ids=np.asarray(batch_ids, dtype=np.int64),
        declaration=declaration,
        declaration_digest=declaration_digest,
        data_fingerprint=data_fingerprint,
        shard_digests=shard_digests,
        shard_paths=paths,
    )


def workspace_observation(root: str | os.PathLike[str], tiers: Iterable[str]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for tier in tiers:
        try:
            result[tier] = load_tier(root, tier).observation()
        except WorkbenchError as exc:
            result[tier] = {"error": exc.to_dict()}
    return result


def _read_import_material(
    declaration_source: str | os.PathLike[str],
    shard_sources: Iterable[str | os.PathLike[str]],
    tier: str,
    *,
    validate_against_declaration: bool = True,
) -> tuple[dict[str, Any], list[tuple[Path, np.ndarray, np.ndarray, np.ndarray]]]:
    declaration_path = Path(declaration_source)
    try:
        declaration = read_json(declaration_path)
    except Exception as exc:
        raise WorkbenchError("E_DATA", f"cannot read declaration: {exc}", stage="import") from exc
    materials: list[tuple[Path, np.ndarray, np.ndarray, np.ndarray]] = []
    for source in shard_sources:
        path = Path(source)
        features, sources, batches = _load_npz(path)
        materials.append((path, features, sources, batches))
    validation_shards = [
        ({"path": str(path)}, features, sources, batches)
        for path, features, sources, batches in materials
    ]
    if validate_against_declaration:
        validate_declaration(declaration, tier=tier, shards=validation_shards)
    else:
        declared_columns = declaration.get("columns")
        for metadata, features, _, batches in validation_shards:
            if not isinstance(declared_columns, int) or features.shape[1] != declared_columns:
                raise WorkbenchError(
                    "E_DATA",
                    "incoming shard column count differs from declaration",
                    stage="import",
                    details={
                        "declared": declared_columns,
                        "observed": int(features.shape[1]),
                        "shard": metadata["path"],
                    },
                )
            if len(set(int(value) for value in batches.tolist())) != 1:
                raise WorkbenchError(
                    "E_DATA", "incoming shard must contain exactly one batch", stage="import"
                )
    return declaration, materials


def import_dataset(
    root: str | os.PathLike[str],
    tier: str,
    declaration_source: str | os.PathLike[str],
    shard_sources: Iterable[str | os.PathLike[str]],
    *,
    replace: bool = True,
) -> dict[str, Any]:
    """Copy local declaration/shards into a workspace and update the manifest."""

    _validate_tier_name(tier)
    root_path = Path(root)
    declaration, materials = _read_import_material(
        declaration_source,
        shard_sources,
        tier,
        validate_against_declaration=False,
    )
    with file_lock(root_path / "locks" / "data.lock"):
        manifest_path = root_path / "manifest.json"
        if manifest_path.exists():
            manifest = read_json(manifest_path)
        else:
            manifest = {"schema_version": 1, "seed": None, "tiers": {}}
        tier_entry = manifest["tiers"].get(
            tier, {"declaration": f"data/{tier}/declaration.json", "shards": []}
        )
        existing = sorted(tier_entry.get("shards", []), key=lambda item: (int(item["batch_id"]), item["path"]))
        incoming_batches = {int(batches[0]) for _, _, _, batches in materials if len(batches)}
        if not replace:
            duplicates = incoming_batches & {int(item["batch_id"]) for item in existing}
            if duplicates:
                raise WorkbenchError(
                    "E_DATA",
                    "import would duplicate existing batch ids; use replace",
                    stage="import",
                    details={"batch_ids": sorted(duplicates)},
                )
        retained = [
            item for item in existing if int(item["batch_id"]) not in incoming_batches
        ]
        incoming_by_batch: dict[int, tuple[Path, np.ndarray, np.ndarray, np.ndarray]] = {}
        for source, features, sources, batches in materials:
            batch_id = int(batches[0]) if len(batches) else 0
            incoming_by_batch[batch_id] = (source, features, sources, batches)

        candidate_shards: list[tuple[dict[str, Any], np.ndarray, np.ndarray, np.ndarray]] = []
        for item in retained:
            features, sources, batches = _load_npz(root_path / item["path"])
            candidate_shards.append(({"path": item["path"]}, features, sources, batches))
        for batch_id, (_, features, sources, batches) in sorted(incoming_by_batch.items()):
            candidate_shards.append(
                ({"path": f"incoming:batch-{batch_id:03d}"}, features, sources, batches)
            )
        validate_declaration(declaration, tier=tier, shards=candidate_shards)

        copied: list[dict[str, Any]] = []
        for batch_id, (source, features, _, _) in sorted(incoming_by_batch.items()):
            destination = root_path / "data" / tier / "shards" / f"batch-{batch_id:03d}.npz"
            copy_atomic(source, destination)
            copied.append(
                {
                    "path": str(destination.relative_to(root_path)),
                    "batch_id": batch_id,
                    "rows": int(len(features)),
                }
            )
        declaration_destination = root_path / tier_entry["declaration"]
        atomic_write_json(declaration_destination, declaration)
        all_shards = sorted(retained + copied, key=lambda item: (int(item["batch_id"]), item["path"]))
        tier_entry["shards"] = all_shards
        manifest["tiers"][tier] = tier_entry
        atomic_write_json(manifest_path, manifest)
        observation = load_tier(root_path, tier).observation()
    return {"tier": tier, "replaced_batches": sorted(incoming_batches), "observation": observation}


def replace_shard(
    root: str | os.PathLike[str],
    tier: str,
    shard_source: str | os.PathLike[str],
    declaration_source: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    """Atomically replace exactly one existing batch shard."""

    _validate_tier_name(tier)
    replacement_path = Path(shard_source)
    features, sources, batches = _load_npz(replacement_path)
    if not len(batches):
        raise WorkbenchError("E_DATA", "replacement shard is empty", stage="import")
    batch_id = int(batches[0])
    root_path = Path(root)
    with file_lock(root_path / "locks" / "data.lock"):
        manifest_path = root_path / "manifest.json"
        if not manifest_path.exists():
            raise WorkbenchError("E_NOT_FOUND", "workspace manifest.json is missing", stage="import")
        manifest = read_json(manifest_path)
        tier_entry = manifest.get("tiers", {}).get(tier)
        if not tier_entry:
            raise WorkbenchError("E_NOT_FOUND", f"tier {tier!r} is not imported", stage="import")
        matches = [item for item in tier_entry["shards"] if int(item["batch_id"]) == batch_id]
        if len(matches) != 1:
            raise WorkbenchError(
                "E_DATA",
                "replacement shard batch_id must match exactly one existing shard",
                stage="import",
                details={"batch_id": batch_id, "matches": len(matches)},
            )
        destination = root_path / matches[0]["path"]
        copy_atomic(replacement_path, destination)
        if declaration_source is not None:
            declaration = read_json(declaration_source)
            shards_for_validation: list[tuple[dict[str, Any], np.ndarray, np.ndarray, np.ndarray]] = []
            for item in tier_entry["shards"]:
                source_path = (
                    replacement_path
                    if int(item["batch_id"]) == batch_id
                    else root_path / item["path"]
                )
                item_features, item_sources, item_batches = _load_npz(source_path)
                shards_for_validation.append(
                    ({"path": str(source_path)}, item_features, item_sources, item_batches)
                )
            validate_declaration(declaration, tier=tier, shards=shards_for_validation)
            atomic_write_json(root_path / tier_entry["declaration"], declaration)
        else:
            # Validate the whole dataset (including the replaced file) against
            # the existing declaration before reporting success.
            load_tier(root_path, tier)
        observation = load_tier(root_path, tier).observation()
    return {"tier": tier, "replaced_batch": batch_id, "observation": observation}


def write_shard_file(
    path: str | os.PathLike[str],
    features: np.ndarray,
    source_ids: np.ndarray,
    batch_ids: np.ndarray,
) -> None:
    _write_npz(Path(path), features, source_ids, batch_ids)


def mutate_shard_copy(
    root: str | os.PathLike[str], tier: str, batch_id: int, destination: str | os.PathLike[str]
) -> dict[str, Any]:
    """Create a deterministic replacement shard for the incremental demo."""

    observation_manifest = read_json(Path(root) / "manifest.json")
    match = None
    for item in observation_manifest["tiers"][tier]["shards"]:
        if int(item["batch_id"]) == batch_id:
            match = item
            break
    if match is None:
        raise WorkbenchError("E_NOT_FOUND", f"batch {batch_id} not found in tier {tier}", stage="import")
    features, sources, batches = _load_npz(Path(root) / match["path"])
    mutated = np.array(features, copy=True)
    finite = np.isfinite(mutated)
    scale = np.nanstd(mutated, axis=0)
    scale[~np.isfinite(scale) | (scale <= 0)] = 1.0
    shifted_rows = np.arange(0, len(mutated), max(1, len(mutated) // 7))
    shifted_columns = np.arange(len(scale)) % max(1, min(4, len(scale)))
    mutated[np.ix_(shifted_rows, shifted_columns)] += 0.35 * scale[shifted_columns]
    write_shard_file(destination, mutated, sources, batches)
    return {
        "tier": tier,
        "batch_id": batch_id,
        "destination": str(destination),
        "mutated_rows": int(len(shifted_rows)),
        "finite_cells": int(finite.sum()),
    }
