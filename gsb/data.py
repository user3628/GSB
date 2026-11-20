"""Deterministic NPZ dataset generation and manifest validation."""

from __future__ import annotations

import io
import math
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .errors import WorkbenchError
from .util import (
    atomic_write_bytes,
    atomic_write_json,
    atomic_write_text,
    canonical_json_text,
    ensure_within,
    read_json,
    sha256_bytes,
    sha256_file,
    sha256_json,
)


GENERATOR_VERSION = "deterministic-synthetic-v1"
MANIFEST_NAME = "manifest.json"
CHECKSUMS_NAME = "checksums.json"


@dataclass(frozen=True)
class Dataset:
    key: str
    npz_path: Path
    manifest_path: Path
    X: np.ndarray
    y: np.ndarray
    sample_ids: np.ndarray
    manifest: dict[str, Any]
    manifest_sha256: str
    data_fingerprint: str

    @property
    def rows(self) -> int:
        return int(self.X.shape[0])

    @property
    def cols(self) -> int:
        return int(self.X.shape[1])

    def summary(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "npz_path": self.npz_path.name,
            "rows": self.rows,
            "cols": self.cols,
            "missing_columns": list(self.manifest.get("missing_columns", [])),
            "positive_rate": float(np.mean(self.y)),
            "manifest_positive_rate": float(self.manifest.get("positive_rate", -1.0)),
            "data_fingerprint": self.data_fingerprint,
            "manifest_sha256": self.manifest_sha256,
        }


def _sigmoid(values: np.ndarray) -> np.ndarray:
    values = np.asarray(values, dtype=np.float64)
    output = np.empty_like(values)
    positive = values >= 0.0
    output[positive] = 1.0 / (1.0 + np.exp(-values[positive]))
    exp_values = np.exp(values[~positive])
    output[~positive] = exp_values / (1.0 + exp_values)
    return output


def _calibrate_intercept(logit_without_intercept: np.ndarray, target_rate: float) -> float:
    low, high = -12.0, 12.0
    for _ in range(80):
        middle = (low + high) / 2.0
        rate = float(np.mean(_sigmoid(middle + logit_without_intercept)))
        if rate < target_rate:
            low = middle
        else:
            high = middle
    return (low + high) / 2.0


def _make_small(seed: int) -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    n, p = 800, 12
    z = rng.standard_normal((n, 4))
    X = np.empty((n, p), dtype=np.float64)
    X[:, 0] = z[:, 0] + 0.18 * rng.standard_normal(n)
    X[:, 1] = z[:, 1] + 0.20 * rng.standard_normal(n)
    X[:, 2] = 0.97 * z[:, 0] + 0.07 * rng.standard_normal(n)
    X[:, 3] = -0.84 * z[:, 1] + 0.12 * rng.standard_normal(n)
    X[:, 4] = z[:, 2] + 0.12 * rng.standard_normal(n)
    X[:, 5] = z[:, 3] + 0.15 * rng.standard_normal(n)
    X[:, 6] = 0.78 * z[:, 2] + 0.35 * rng.standard_normal(n)
    X[:, 7] = z[:, 1] * z[:, 2] + 0.20 * rng.standard_normal(n)
    X[:, 8] = rng.uniform(-2.5, 2.5, n)
    X[:, 9] = rng.gamma(2.2, 0.8, n)
    X[:, 10] = 0.58 * z[:, 3] + 0.42 * rng.standard_normal(n)
    X[:, 11] = np.sin(z[:, 0]) + 0.25 * rng.standard_normal(n)
    latent = (
        1.20 * z[:, 0]
        - 0.95 * z[:, 1]
        + 0.72 * z[:, 2]
        + 0.32 * z[:, 1] * z[:, 2]
        + 0.30 * np.sin(2.0 * z[:, 3])
        + 0.32 * rng.standard_normal(n)
    )
    intercept = _calibrate_intercept(latent, 0.36)
    probability = _sigmoid(intercept + latent)
    y = (rng.random(n) < probability).astype(np.uint8)
    X[2, 2] = np.nan
    X[17, 7] = np.nan
    missing_2 = rng.choice(n, size=44, replace=False)
    missing_7 = rng.choice(n, size=37, replace=False)
    X[missing_2, 2] = np.nan
    X[missing_7, 7] = np.nan
    return X, y


def _make_medium(seed: int) -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    n, p = 8000, 32
    z = rng.standard_normal((n, 6))
    X = np.empty((n, p), dtype=np.float64)
    X[:, 0] = z[:, 0] + 0.15 * rng.standard_normal(n)
    X[:, 1] = z[:, 1] + 0.18 * rng.standard_normal(n)
    X[:, 2] = z[:, 2] + 0.20 * rng.standard_normal(n)
    X[:, 3] = 0.96 * z[:, 0] + 0.08 * rng.standard_normal(n)
    X[:, 4] = -0.89 * z[:, 1] + 0.10 * rng.standard_normal(n)
    X[:, 5] = 0.81 * z[:, 2] + 0.15 * rng.standard_normal(n)
    X[:, 6] = z[:, 3] + 0.12 * rng.standard_normal(n)
    X[:, 7] = z[:, 4] + 0.17 * rng.standard_normal(n)
    X[:, 8] = z[:, 5] + 0.19 * rng.standard_normal(n)
    X[:, 9] = z[:, 0] * z[:, 1] + 0.18 * rng.standard_normal(n)
    X[:, 10] = np.sin(z[:, 2]) + 0.22 * rng.standard_normal(n)
    X[:, 11] = np.cos(z[:, 3]) + 0.24 * rng.standard_normal(n)
    X[:, 12] = rng.uniform(-3.0, 3.0, n)
    X[:, 13] = rng.gamma(2.0, 0.9, n)
    X[:, 14] = rng.lognormal(0.0, 0.35, n)
    X[:, 15] = 0.55 * z[:, 4] + 0.45 * rng.standard_normal(n)
    X[:, 16] = 0.67 * z[:, 5] + 0.33 * rng.standard_normal(n)
    X[:, 17] = z[:, 1] * z[:, 5] + 0.16 * rng.standard_normal(n)
    X[:, 18] = 0.73 * z[:, 0] + 0.27 * rng.standard_normal(n)
    X[:, 19] = -0.64 * z[:, 2] + 0.36 * rng.standard_normal(n)
    X[:, 20] = z[:, 3] * z[:, 4] + 0.20 * rng.standard_normal(n)
    X[:, 21] = np.tanh(z[:, 0] + z[:, 2]) + 0.12 * rng.standard_normal(n)
    X[:, 22] = rng.normal(2.0, 1.4, n)
    X[:, 23] = rng.beta(2.0, 4.0, n)
    X[:, 24] = 0.48 * z[:, 3] + 0.52 * rng.standard_normal(n)
    X[:, 25] = z[:, 4] * z[:, 5] + 0.18 * rng.standard_normal(n)
    X[:, 26] = np.sin(1.5 * z[:, 1]) + 0.21 * rng.standard_normal(n)
    X[:, 27] = np.cos(1.7 * z[:, 2]) + 0.23 * rng.standard_normal(n)
    X[:, 28] = 0.59 * z[:, 0] + 0.41 * rng.standard_normal(n)
    X[:, 29] = 0.61 * z[:, 5] + 0.39 * rng.standard_normal(n)
    X[:, 30] = rng.uniform(0.0, 1.0, n)
    X[:, 31] = 0.70 * z[:, 1] + 0.30 * rng.standard_normal(n)
    latent = (
        1.08 * z[:, 0]
        - 0.82 * z[:, 1]
        + 0.66 * z[:, 2]
        + 0.44 * z[:, 3]
        - 0.36 * z[:, 4]
        + 0.29 * z[:, 5]
        + 0.26 * z[:, 0] * z[:, 3]
        + 0.23 * np.sin(1.8 * z[:, 2])
        + 0.22 * np.cos(1.3 * z[:, 5])
        + 0.34 * rng.standard_normal(n)
    )
    intercept = _calibrate_intercept(latent, 0.41)
    probability = _sigmoid(intercept + latent)
    y = (rng.random(n) < probability).astype(np.uint8)
    missing_columns = (3, 9, 17, 25)
    rates = (0.055, 0.043, 0.036, 0.048)
    for column, rate in zip(missing_columns, rates):
        chosen = rng.choice(n, size=int(round(n * rate)), replace=False)
        X[chosen, column] = np.nan
    return X, y


def _npy_bytes(array: np.ndarray) -> bytes:
    buffer = io.BytesIO()
    np.save(buffer, array, allow_pickle=False)
    return buffer.getvalue()


def write_npz_deterministic(path: Path, arrays: dict[str, np.ndarray]) -> None:
    """Write a byte-stable NPZ (fixed ZIP metadata, no timestamps)."""

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_STORED, allowZip64=True) as archive:
        for name in sorted(arrays):
            info = zipfile.ZipInfo(f"{name}.npy", date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_STORED
            info.create_system = 0
            info.external_attr = 0
            archive.writestr(info, _npy_bytes(np.ascontiguousarray(arrays[name])))
    atomic_write_bytes(path, buffer.getvalue())


def generate_datasets(data_dir: Path, *, seed_base: int = 20261121, force: bool = False) -> dict[str, Any]:
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = data_dir / MANIFEST_NAME
    if manifest_path.exists() and not force:
        return verify_generated_data(data_dir)

    entries: dict[str, Any] = {}
    specifications = (
        ("small", 800, 12, seed_base + 1, (2, 7), _make_small),
        ("medium", 8000, 32, seed_base + 2, (3, 9, 17, 25), _make_medium),
    )
    for key, rows, cols, seed, missing_columns, maker in specifications:
        X, y = maker(seed)
        if X.shape != (rows, cols):
            raise RuntimeError(f"generator shape mismatch for {key}: {X.shape}")
        sample_ids = np.arange(rows, dtype=np.uint32)
        npz_path = data_dir / f"{key}.npz"
        write_npz_deterministic(
            npz_path,
            {
                "X": np.asarray(X, dtype="<f8"),
                "y": np.asarray(y, dtype="u1"),
                "sample_id": sample_ids,
            },
        )
        entries[key] = {
            "npz": npz_path.name,
            "rows": rows,
            "cols": cols,
            "seed": seed,
            "missing_columns": list(missing_columns),
            "positive_rate": float(np.mean(y)),
            "sha256": sha256_file(npz_path),
            "feature_kind": "continuous",
            "correlated_redundant_pairs": (
                [[0, 2], [1, 3]]
                if key == "small"
                else [[0, 3], [1, 4], [2, 5], [7, 15], [8, 16]]
            ),
        }

    manifest = {
        "schema": 1,
        "generator_version": GENERATOR_VERSION,
        "seed_base": seed_base,
        "datasets": entries,
        "observations": {
            "note": "rows, cols, missing column indices and positive rates are explicit declarations",
            "categorical_columns": [],
        },
    }
    atomic_write_text(manifest_path, canonical_json_text(manifest, pretty=True))
    checksums = {
        "schema": 1,
        "files": {
            **{entry["npz"]: entry["sha256"] for entry in entries.values()},
            MANIFEST_NAME: sha256_file(manifest_path),
        },
    }
    atomic_write_json(data_dir / CHECKSUMS_NAME, checksums, pretty=True)
    return verify_generated_data(data_dir)


def verify_generated_data(data_dir: Path) -> dict[str, Any]:
    data_dir = Path(data_dir)
    manifest_path = data_dir / MANIFEST_NAME
    if not manifest_path.exists():
        raise WorkbenchError("E_INVALID_PARAM", "manifest.json is missing", {"data_dir": str(data_dir)})
    manifest = read_json(manifest_path)
    checksums_path = data_dir / CHECKSUMS_NAME
    checksums = read_json(checksums_path) if checksums_path.exists() else {"files": {}}
    expected_manifest = checksums.get("files", {}).get(MANIFEST_NAME)
    if expected_manifest is not None and expected_manifest != sha256_file(manifest_path):
        raise WorkbenchError(
            "E_INVALID_PARAM",
            "checksums.json mismatch: manifest.json",
            {"expected": expected_manifest, "actual": sha256_file(manifest_path)},
        )
    verified: dict[str, Any] = {}
    for key, entry in manifest.get("datasets", {}).items():
        npz_path = data_dir / entry["npz"]
        if not npz_path.exists():
            raise WorkbenchError("E_INVALID_PARAM", f"dataset file missing: {npz_path.name}", {"key": key})
        actual = sha256_file(npz_path)
        if actual != entry.get("sha256"):
            raise WorkbenchError(
                "E_INVALID_PARAM",
                f"dataset checksum mismatch: {npz_path.name}",
                {"key": key, "expected": entry.get("sha256"), "actual": actual},
            )
        if checksums.get("files", {}).get(npz_path.name, actual) != actual:
            raise WorkbenchError(
                "E_INVALID_PARAM",
                f"checksums.json mismatch: {npz_path.name}",
                {"key": key, "actual": actual},
            )
        with np.load(npz_path, allow_pickle=False) as loaded:
            X = np.asarray(loaded["X"], dtype=np.float64)
            y = np.asarray(loaded["y"], dtype=np.uint8)
        if X.shape != (entry["rows"], entry["cols"]) or y.shape != (entry["rows"],):
            raise WorkbenchError(
                "E_INVALID_PARAM",
                "manifest dimensions do not match NPZ contents",
                {"key": key, "shape": list(X.shape), "expected": [entry["rows"], entry["cols"]]},
            )
        if not np.all(np.isfinite(X[~np.isnan(X)])):
            raise WorkbenchError("E_NUMERIC", "generated data contains Inf", {"key": key, "stage": "data"})
        observed_missing = [int(index) for index in np.flatnonzero(np.any(np.isnan(X), axis=0))]
        if observed_missing != list(entry["missing_columns"]):
            raise WorkbenchError(
                "E_INVALID_PARAM",
                "missing column declaration does not match NPZ contents",
                {"key": key, "declared": entry["missing_columns"], "observed": observed_missing},
            )
        observed_rate = float(np.mean(y))
        if abs(observed_rate - float(entry["positive_rate"])) > 1e-12:
            raise WorkbenchError(
                "E_INVALID_PARAM",
                "positive rate declaration does not match labels",
                {"key": key, "declared": entry["positive_rate"], "observed": observed_rate},
            )
        verified[key] = {
            **entry,
            "observed_positive_rate": observed_rate,
            "observed_missing_columns": observed_missing,
            "manifest_sha256": sha256_file(manifest_path),
        }
    return {
        "schema": 1,
        "manifest": str(manifest_path),
        "manifest_sha256": sha256_file(manifest_path),
        "checksums": checksums.get("files", {}),
        "datasets": verified,
    }


def list_datasets(data_dir: Path) -> dict[str, Any]:
    data_dir = Path(data_dir)
    manifest_path = data_dir / MANIFEST_NAME
    if not manifest_path.exists():
        return {"manifest": None, "datasets": {}}
    manifest = read_json(manifest_path)
    return {"manifest": str(manifest_path), "manifest_sha256": sha256_file(manifest_path), "datasets": manifest.get("datasets", {})}


def load_dataset(key: str, *, project_root: Path, data_dir: Path) -> Dataset:
    project_root = Path(project_root).resolve()
    data_dir = ensure_within(project_root, data_dir, label="data_dir")
    manifest_path = data_dir / MANIFEST_NAME
    if not manifest_path.exists():
        raise WorkbenchError("E_INVALID_PARAM", "manifest.json is missing", {"data_dir": str(data_dir)})
    manifest_all = read_json(manifest_path)
    checksums_path = data_dir / CHECKSUMS_NAME
    if checksums_path.exists():
        checksums = read_json(checksums_path).get("files", {})
        expected_manifest = checksums.get(MANIFEST_NAME)
        if expected_manifest is not None and expected_manifest != sha256_file(manifest_path):
            raise WorkbenchError(
                "E_INVALID_PARAM",
                "manifest checksum does not match checksums.json",
                {"expected": expected_manifest, "actual": sha256_file(manifest_path)},
            )
    datasets = manifest_all.get("datasets", {})
    if key not in datasets:
        raise WorkbenchError("E_INVALID_PARAM", f"unknown dataset: {key}", {"key": key, "available": sorted(datasets)})
    entry = datasets[key]
    npz_path = ensure_within(project_root, data_dir / entry["npz"], label="npz_path")
    if not npz_path.exists():
        raise WorkbenchError("E_INVALID_PARAM", f"dataset file missing: {npz_path.name}", {"key": key})
    manifest_sha = sha256_file(manifest_path)
    if entry.get("sha256") and sha256_file(npz_path) != entry["sha256"]:
        raise WorkbenchError(
            "E_INVALID_PARAM",
            "dataset checksum does not match manifest declaration",
            {"key": key, "path": str(npz_path)},
        )
    with np.load(npz_path, allow_pickle=False) as loaded:
        required = {"X", "y"}
        if not required.issubset(loaded.files):
            raise WorkbenchError("E_INVALID_PARAM", "NPZ must contain X and y", {"keys": loaded.files})
        try:
            X = np.asarray(loaded["X"], dtype=np.float64)
        except (TypeError, ValueError) as exc:
            raise WorkbenchError("E_INVALID_PARAM", "X cannot be converted to float64", {"stage": "data"}) from exc
        y = np.asarray(loaded["y"])
        if "sample_id" in loaded.files:
            sample_ids = np.asarray(loaded["sample_id"])
        else:
            sample_ids = np.arange(X.shape[0], dtype=np.int64)
    if X.ndim != 2:
        raise WorkbenchError("E_INVALID_PARAM", "X must be two-dimensional", {"shape": list(X.shape)})
    if y.ndim != 1 or y.shape[0] != X.shape[0]:
        raise WorkbenchError(
            "E_INVALID_PARAM",
            "y must be one-dimensional with one label per row",
            {"X_shape": list(X.shape), "y_shape": list(y.shape)},
        )
    if sample_ids.ndim != 1 or sample_ids.shape[0] != X.shape[0]:
        raise WorkbenchError("E_INVALID_PARAM", "sample_id must have one value per row", {"shape": list(sample_ids.shape)})
    if np.unique(sample_ids).size != sample_ids.size:
        raise WorkbenchError("E_INVALID_PARAM", "sample_id values must be unique", {"stage": "data"})
    try:
        y_numeric = np.asarray(y, dtype=np.float64)
    except (TypeError, ValueError) as exc:
        raise WorkbenchError("E_INVALID_PARAM", "labels must be binary", {"stage": "data"}) from exc
    if not np.all(np.isfinite(y_numeric)) or not np.all((y_numeric == 0.0) | (y_numeric == 1.0)):
        raise WorkbenchError("E_INVALID_PARAM", "labels must be exactly 0 or 1", {"stage": "data"})
    y = y_numeric.astype(np.uint8)
    invalid = np.argwhere(np.isinf(X))
    if invalid.size:
        row, column = map(int, invalid[0])
        raise WorkbenchError(
            "E_NUMERIC",
            "features contain Inf",
            {"sample_index": row, "column": column, "stage": "data"},
        )
    declared_rows = int(entry["rows"])
    declared_cols = int(entry["cols"])
    if X.shape != (declared_rows, declared_cols):
        raise WorkbenchError(
            "E_INVALID_PARAM",
            "manifest dimensions do not match NPZ contents",
            {"key": key, "shape": list(X.shape), "expected": [declared_rows, declared_cols]},
        )
    observed_missing = [int(index) for index in np.flatnonzero(np.any(np.isnan(X), axis=0))]
    if observed_missing != list(entry["missing_columns"]):
        raise WorkbenchError(
            "E_INVALID_PARAM",
            "missing column declaration does not match NPZ contents",
            {"key": key, "declared": entry["missing_columns"], "observed": observed_missing},
        )
    observed_rate = float(np.mean(y))
    if abs(observed_rate - float(entry["positive_rate"])) > 1e-12:
        raise WorkbenchError(
            "E_INVALID_PARAM",
            "positive rate declaration does not match labels",
            {"key": key, "declared": entry["positive_rate"], "observed": observed_rate},
        )
    X = np.ascontiguousarray(X, dtype=np.float64)
    y = np.ascontiguousarray(y, dtype=np.uint8)
    sample_ids = np.ascontiguousarray(sample_ids)
    fingerprint = sha256_json(
        {
            "manifest_sha256": manifest_sha,
            "rows": int(X.shape[0]),
            "cols": int(X.shape[1]),
            "X_sha256": sha256_bytes(np.asarray(X, dtype="<f8").tobytes(order="C")),
            "y_sha256": sha256_bytes(np.asarray(y, dtype="u1").tobytes(order="C")),
            "sample_id_sha256": sha256_bytes(np.ascontiguousarray(sample_ids).tobytes(order="C")),
        }
    )
    return Dataset(
        key=key,
        npz_path=npz_path,
        manifest_path=manifest_path,
        X=X,
        y=y,
        sample_ids=sample_ids,
        manifest={**entry, "manifest_schema": manifest_all.get("schema")},
        manifest_sha256=manifest_sha,
        data_fingerprint=fingerprint,
    )
