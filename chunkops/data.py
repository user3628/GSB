"""Deterministic local .npz fragment generation and loading."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from .errors import ChunkOpsError, E_MISSING_FRAGMENT, E_INVALID_SHAPE
from .fingerprint import (
    numeric_fingerprint,
    numeric_range,
    shape_fingerprint,
    sha256_file,
    value_fingerprint,
)
from .ops import find_zero_rows, scan_nonfinite, validate_matrix

TIER_SPECS = {
    "small": (128, 512),
    "medium": (2048, 1024),
    "large": (8192, 4096),
}


def _atomic_savez(path: Path, **arrays: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp.npz", dir=path.parent)
    os.close(fd)
    try:
        np.savez(temporary, **arrays)
        with open(temporary, "rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _base_values(rows: int, cols: int, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return rng.standard_normal((rows, cols), dtype=np.float32) * np.float32(1.75)


def _apply_anomalies(x: np.ndarray, seed: int) -> dict[str, Any]:
    rows, cols = x.shape
    rng = np.random.default_rng(seed ^ 0xA5A5A5A5)

    extreme_rows = sorted(
        set(
            [
                0,
                max(0, rows // 7),
                max(0, rows // 2),
                max(0, rows - 1),
            ]
        )
    )
    extreme_cols = sorted(
        set([0, 1, max(0, cols // 2), max(0, cols - 2), max(0, cols - 1)])
    )
    sign = 1.0
    for row in extreme_rows:
        for col in extreme_cols:
            x[row, col] = np.float32(1.0e4 if sign > 0 else -1.0e4)
            sign = -sign

    zero_rows = sorted(
        set(
            [
                min(rows - 1, 3),
                min(rows - 1, max(4, rows // 5)),
                min(rows - 1, max(5, rows // 3)),
            ]
        )
    )
    for row in zero_rows:
        if row not in extreme_rows:
            x[row, :] = 0.0

    occupied = set(extreme_rows) | set(zero_rows)
    candidates = [row for row in range(rows) if row not in occupied]
    if not candidates:
        candidates = [0]
    chosen = rng.choice(
        np.asarray(candidates, dtype=np.int64),
        size=min(3, len(candidates)),
        replace=False,
    )
    special_rows = [int(value) for value in chosen]
    nan_col = int(cols // 4)
    pos_col = int(cols // 2) if cols > 2 else 0
    neg_col = int(cols - 3) if cols > 4 else cols - 1
    seeded_positions = {
        "nan": [special_rows[0], nan_col],
        "pos_inf": [special_rows[1], pos_col],
        "neg_inf": [special_rows[2], neg_col],
    }
    x[special_rows[0], nan_col] = np.float32(np.nan)
    x[special_rows[1], pos_col] = np.float32(np.inf)
    x[special_rows[2], neg_col] = np.float32(-np.inf)
    protected = {
        (row, col)
        for row, col in (
            (special_rows[0], nan_col),
            (special_rows[1], pos_col),
            (special_rows[2], neg_col),
        )
    }

    # Add a sparse deterministic anomaly sprinkling so large fragments exercise
    # chunk boundaries rather than only one early block.
    extra = max(2, min(64, x.size // 250_000))
    for _ in range(extra):
        row = int(rng.integers(0, rows))
        col = int(rng.integers(0, cols))
        if row in zero_rows or (row, col) in protected:
            continue
        kind = int(rng.integers(0, 3))
        x[row, col] = np.float32(
            np.nan if kind == 0 else (np.inf if kind == 1 else -np.inf)
        )
    return {
        "extreme_rows": extreme_rows,
        "extreme_cols": extreme_cols,
        "zero_rows": zero_rows,
        "seeded_nonfinite": seeded_positions,
    }


def _fragment_record(
    *,
    path: Path,
    fragment_id: str,
    tier: str,
    seed: int,
    anomaly: bool,
    x: np.ndarray,
    gamma: np.ndarray,
    beta: np.ndarray,
    anomaly_positions: dict[str, Any] | None,
) -> dict[str, Any]:
    nonfinite = scan_nonfinite(x, fragment_id=fragment_id)
    zero_rows = find_zero_rows(x)
    ranges = numeric_range(x)
    return {
        "fragment_id": fragment_id,
        "tier": tier,
        "path": str(path.resolve()),
        "file_size_bytes": int(path.stat().st_size),
        "file_sha256": sha256_file(path),
        "seed": seed,
        "anomaly_fragment": bool(anomaly),
        "shape": [int(v) for v in x.shape],
        "dtype": str(x.dtype),
        "shape_fingerprint": shape_fingerprint(x.shape),
        "value_fingerprint": value_fingerprint(x),
        "numeric_fingerprint": numeric_fingerprint(x),
        "numeric_range": ranges,
        "nonfinite": nonfinite,
        "zero_row_indices": zero_rows,
        "seeded_anomaly_positions": anomaly_positions,
        "auxiliary_tensors": {
            "gamma": {"shape": list(gamma.shape), "dtype": str(gamma.dtype)},
            "beta": {"shape": list(beta.shape), "dtype": str(beta.dtype)},
        },
        "operations": [
            {"op": "softmax", "axis": 1, "description": "row-direction Softmax"},
            {"op": "layernorm", "axis": 0, "description": "column-direction LayerNorm"},
        ],
    }


def generate_datasets(
    root: str | os.PathLike[str] = "data",
    *,
    seed: int = 20261122,
    include_large: bool = True,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Generate base and anomalous fragments for all configured tiers."""

    root_path = Path(root)
    fragments_dir = root_path / "fragments"
    fragments_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = root_path / "manifest.json"
    tiers = ["small", "medium"] + (["large"] if include_large else [])
    required_ids = {
        f"{tier}_{'anomaly' if anomaly else 'base'}"
        for tier in tiers
        for anomaly in (False, True)
    }
    if manifest_path.exists() and not overwrite:
        existing_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        existing_records = {
            record["fragment_id"]: record
            for record in existing_manifest.get("fragments", [])
        }
        if required_ids <= set(existing_records) and all(
            Path(existing_records[fragment_id]["path"]).exists()
            for fragment_id in required_ids
        ):
            return existing_manifest

    records: list[dict[str, Any]] = []
    for tier_index, tier in enumerate(tiers):
        rows, cols = TIER_SPECS[tier]
        for anomaly in (False, True):
            fragment_seed = seed + 1009 * tier_index + (17 if anomaly else 0)
            fragment_id = f"{tier}_{'anomaly' if anomaly else 'base'}"
            path = fragments_dir / f"{fragment_id}.npz"
            if path.exists() and not overwrite:
                existing = np.load(path, allow_pickle=False)
                try:
                    x = existing["x"]
                finally:
                    existing.close()
                anomaly_positions = None
            else:
                x = _base_values(rows, cols, fragment_seed)
                anomaly_positions = _apply_anomalies(x, fragment_seed) if anomaly else None
                gamma = (
                    np.float32(1.0)
                    + np.float32(0.08)
                    * np.random.default_rng(fragment_seed + 3).standard_normal(
                        (rows,), dtype=np.float32
                    )
                )
                beta = (
                    np.float32(0.02)
                    * np.random.default_rng(fragment_seed + 5).standard_normal(
                        (cols,), dtype=np.float32
                    )
                )
                _atomic_savez(path, x=x, gamma=gamma, beta=beta)
            loaded = np.load(path, allow_pickle=False)
            try:
                x = loaded["x"]
                gamma = loaded["gamma"]
                beta = loaded["beta"]
            finally:
                loaded.close()
            records.append(
                _fragment_record(
                    path=path,
                    fragment_id=fragment_id,
                    tier=tier,
                    seed=fragment_seed,
                    anomaly=anomaly,
                    x=x,
                    gamma=gamma,
                    beta=beta,
                    anomaly_positions=anomaly_positions,
                )
            )

    manifest = {
        "schema_version": 1,
        "generator": "chunkops.data.generate_datasets",
        "generator_version": 1,
        "seed": int(seed),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "tiers": {
            tier: {"rows": shape[0], "cols": shape[1]}
            for tier, shape in TIER_SPECS.items()
        },
        "coverage": {
            "tiers": tiers,
            "ops": ["softmax-axis1-row", "layernorm-axis0-column"],
            "anomalies": ["extreme_1e4_mix", "all_zero_rows", "nan", "pos_inf", "neg_inf"],
        },
        "shape_and_range_catalog": {
            record["fragment_id"]: {
                "shape": record["shape"],
                "dtype": record["dtype"],
                "numeric_range": record["numeric_range"],
                "nonfinite_first_positions": {
                    key: record["nonfinite"][key]
                    for key in ("first_any", "first_nan", "first_pos_inf", "first_neg_inf")
                },
                "zero_rows": record["zero_row_indices"],
            }
            for record in records
        },
        "fragments": records,
    }
    temporary = manifest_path.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False)
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, manifest_path)
    return manifest


def load_manifest(root: str | os.PathLike[str] = "data") -> dict[str, Any]:
    path = Path(root) / "manifest.json"
    if not path.exists():
        raise ChunkOpsError(
            E_MISSING_FRAGMENT,
            "dataset manifest is missing",
            {"path": str(path.resolve()), "position": "manifest"},
        )
    return json.loads(path.read_text(encoding="utf-8"))


def load_fragment(
    path: str | os.PathLike[str],
    *,
    fragment_id: str | None = None,
) -> tuple[np.ndarray, np.ndarray | None, np.ndarray | None, dict[str, Any]]:
    """Load x/gamma/beta with stable missing-fragment business errors."""

    source = Path(path)
    if not source.exists():
        raise ChunkOpsError(
            E_MISSING_FRAGMENT,
            "fragment file is missing",
            {
                "fragment_id": fragment_id,
                "path": str(source.resolve()),
                "position": "fragment_path",
            },
        )
    try:
        archive = np.load(source, allow_pickle=False)
    except Exception as exc:
        raise ChunkOpsError(
            E_INVALID_SHAPE,
            "fragment is not a readable .npz archive",
            {
                "fragment_id": fragment_id,
                "path": str(source.resolve()),
                "position": "fragment_path",
                "cause": type(exc).__name__,
            },
        ) from exc
    try:
        if "x" not in archive.files:
            raise ChunkOpsError(
                E_MISSING_FRAGMENT,
                "fragment archive does not contain tensor key 'x'",
                {
                    "fragment_id": fragment_id,
                    "path": str(source.resolve()),
                    "keys": sorted(archive.files),
                    "position": "x",
                },
            )
        x = archive["x"]
        gamma = archive["gamma"] if "gamma" in archive.files else None
        beta = archive["beta"] if "beta" in archive.files else None
    finally:
        archive.close()
    try:
        validate_matrix(x, fragment_id=fragment_id)
    except ChunkOpsError as exc:
        exc.details.update(
            {"path": str(source.resolve()), "position": exc.details.get("position", "x")}
        )
        raise
    metadata = {
        "fragment_id": fragment_id,
        "path": str(source.resolve()),
        "shape": [int(v) for v in x.shape],
        "dtype": str(x.dtype),
        "shape_fingerprint": shape_fingerprint(x.shape),
        "value_fingerprint": value_fingerprint(x),
        "numeric_range": numeric_range(x),
    }
    return x, gamma, beta, metadata


def choose_fragment(manifest: dict[str, Any], fragment_id: str) -> dict[str, Any]:
    for record in manifest.get("fragments", []):
        if record["fragment_id"] == fragment_id:
            return record
    raise ChunkOpsError(
        E_MISSING_FRAGMENT,
        "fragment id is not present in manifest",
        {"fragment_id": fragment_id, "position": "manifest.fragments"},
    )
