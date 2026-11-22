"""Deterministic local .npy sample generation and manifest loading."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .checkpoint import atomic_write_json, atomic_save_npy
from .errors import BlockGemmError, E_INVALID_SHAPE, invalid_shape
from .fingerprint import array_payload_digest, shape_fingerprint
from .kernel import ShapeExpectation


GENERATOR_VERSION = "blockgemm-samples-v1"


@dataclass(frozen=True)
class SampleDefinition:
    case_id: str
    kind: str
    seed: int
    m: int
    n: int
    k: int
    batch: int = 1
    a_transpose: bool = False
    b_transpose: bool = False
    tags: tuple[str, ...] = ()
    anomaly: str | None = None

    def stored_a_shape(self) -> tuple[int, ...]:
        if self.kind == "gemm":
            return (self.k, self.m) if self.a_transpose else (self.m, self.k)
        prefix = (self.batch,)
        return prefix + ((self.k, self.m) if self.a_transpose else (self.m, self.k))

    def stored_b_shape(self) -> tuple[int, ...]:
        if self.kind == "gemm":
            return (self.n, self.k) if self.b_transpose else (self.k, self.n)
        prefix = (self.batch,)
        return prefix + ((self.n, self.k) if self.b_transpose else (self.k, self.n))

    def output_shape(self) -> tuple[int, ...]:
        if self.kind == "gemm":
            return (self.m, self.n)
        return (self.batch, self.m, self.n)


SAMPLE_DEFINITIONS: tuple[SampleDefinition, ...] = (
    SampleDefinition(
        "small_gemm", "gemm", 1001, 64, 64, 64, tags=("small", "gemm", "square")
    ),
    SampleDefinition(
        "medium_gemm", "gemm", 1002, 512, 256, 384, tags=("medium", "gemm", "non_square")
    ),
    SampleDefinition(
        "large_gemm", "gemm", 1003, 1024, 1024, 1024, tags=("large", "gemm", "square")
    ),
    SampleDefinition(
        "transpose_gemm",
        "gemm",
        1004,
        96,
        72,
        48,
        a_transpose=True,
        b_transpose=True,
        tags=("gemm", "transpose_a", "transpose_b", "non_square", "tail"),
    ),
    SampleDefinition(
        "bmm_batch3",
        "bmm",
        1005,
        96,
        64,
        80,
        batch=3,
        tags=("bmm", "batch_gt_1", "non_square", "tail"),
    ),
    SampleDefinition(
        "bmm_transpose_batch2",
        "bmm",
        1006,
        72,
        56,
        48,
        batch=2,
        a_transpose=True,
        b_transpose=True,
        tags=("bmm", "batch_gt_1", "transpose_a", "transpose_b", "non_square", "tail"),
    ),
    SampleDefinition(
        "bmm_a_transpose_batch2",
        "bmm",
        1007,
        80,
        48,
        64,
        batch=2,
        a_transpose=True,
        tags=("bmm", "batch_gt_1", "transpose_a", "non_square"),
    ),
    SampleDefinition(
        "bmm_b_transpose_batch2",
        "bmm",
        1008,
        64,
        72,
        40,
        batch=2,
        b_transpose=True,
        tags=("bmm", "batch_gt_1", "transpose_b", "non_square", "tail"),
    ),
    SampleDefinition(
        "nan_gemm",
        "gemm",
        1009,
        64,
        64,
        64,
        tags=("gemm", "anomaly", "nan"),
        anomaly="nan",
    ),
    SampleDefinition(
        "inf_bmm_batch2",
        "bmm",
        1010,
        48,
        40,
        32,
        batch=2,
        tags=("bmm", "batch_gt_1", "anomaly", "inf", "non_square"),
        anomaly="inf",
    ),
)


def _generate_matrices(definition: SampleDefinition) -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(definition.seed)
    a_shape = definition.stored_a_shape()
    b_shape = definition.stored_b_shape()
    a = rng.standard_normal(a_shape).astype(np.float64, copy=False)
    b = rng.standard_normal(b_shape).astype(np.float64, copy=False)
    # Keep magnitudes moderate so high-precision tolerance checks are meaningful.
    a *= 0.25
    b *= 0.25

    if definition.anomaly == "nan":
        if definition.kind == "gemm":
            a[3, 5] = np.nan
            b[8, 9] = np.nan
        else:
            a[0, 3, 5] = np.nan
            b[0, 8, 9] = np.nan
    elif definition.anomaly == "inf":
        if definition.kind == "gemm":
            a[4, 6] = np.inf
            b[10, 11] = -np.inf
        else:
            a[1, 2, 3] = np.inf
            b[1, 5, 6] = -np.inf
    return a, b


def _nonfinite_metadata(array: np.ndarray) -> dict[str, Any]:
    mask = ~np.isfinite(array)
    indices = np.argwhere(mask)
    locations = [
        {"index": [int(value) for value in index], "value": str(array[tuple(index)])}
        for index in indices[:20]
    ]
    return {"count": int(indices.shape[0]), "locations": locations, "truncated": bool(indices.shape[0] > 20)}


def _case_manifest_entry(
    definition: SampleDefinition,
    *,
    a_path: str,
    b_path: str,
    a: np.ndarray,
    b: np.ndarray,
    a_file_sha256: str,
    b_file_sha256: str,
) -> dict[str, Any]:
    shape_fp = shape_fingerprint(
        kind=definition.kind,
        m=definition.m,
        n=definition.n,
        k=definition.k,
        batch=definition.batch,
        dtype=a.dtype.str,
        a_transpose=definition.a_transpose,
        b_transpose=definition.b_transpose,
    )
    return {
        "id": definition.case_id,
        "kind": definition.kind,
        "seed": definition.seed,
        "tags": list(definition.tags),
        "dtype": a.dtype.str,
        "M": definition.m,
        "N": definition.n,
        "K": definition.k,
        "batch": definition.batch,
        "a_transpose": definition.a_transpose,
        "b_transpose": definition.b_transpose,
        "a_file": a_path,
        "b_file": b_path,
        "a_stored_shape": list(definition.stored_a_shape()),
        "b_stored_shape": list(definition.stored_b_shape()),
        "effective_a_shape": [definition.m, definition.k],
        "effective_b_shape": [definition.k, definition.n],
        "output_shape": list(definition.output_shape()),
        "shape_fingerprint": shape_fp,
        "a_array_digest": array_payload_digest(a),
        "b_array_digest": array_payload_digest(b),
        "a_file_sha256": a_file_sha256,
        "b_file_sha256": b_file_sha256,
        "anomaly": definition.anomaly,
        "nonfinite": {
            "a": _nonfinite_metadata(a),
            "b": _nonfinite_metadata(b),
            "affected_batches": _affected_batches(definition, a, b),
        },
    }


def _affected_batches(definition: SampleDefinition, a: np.ndarray, b: np.ndarray) -> list[int]:
    if definition.kind == "gemm":
        return [0] if ((~np.isfinite(a)).any() or (~np.isfinite(b)).any()) else []
    affected = []
    for index in range(definition.batch):
        if (~np.isfinite(a[index])).any() or (~np.isfinite(b[index])).any():
            affected.append(index)
    return affected


def shape_list_entry(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": entry["id"],
        "kind": entry["kind"],
        "tags": entry["tags"],
        "a_stored_shape": entry["a_stored_shape"],
        "b_stored_shape": entry["b_stored_shape"],
        "effective_a_shape": entry["effective_a_shape"],
        "effective_b_shape": entry["effective_b_shape"],
        "M": entry["M"],
        "N": entry["N"],
        "K": entry["K"],
        "batch": entry["batch"],
        "a_transpose": entry["a_transpose"],
        "b_transpose": entry["b_transpose"],
        "output_shape": entry["output_shape"],
        "dtype": entry["dtype"],
        "shape_fingerprint": entry["shape_fingerprint"],
        "anomaly": entry["anomaly"],
    }


def generate_samples(output_dir: str | os.PathLike[str], *, force: bool = False) -> dict[str, Any]:
    output = Path(output_dir)
    manifest_path = output / "manifest.json"
    if manifest_path.exists() and not force:
        raise BlockGemmError(
            "E_INVALID_CONFIG",
            f"sample manifest already exists at {manifest_path}; pass --force to regenerate",
            reason="sample_manifest_exists",
            details={"manifest": str(manifest_path)},
        )
    matrix_dir = output / "matrices"
    matrix_dir.mkdir(parents=True, exist_ok=True)
    entries: list[dict[str, Any]] = []
    for definition in SAMPLE_DEFINITIONS:
        a, b = _generate_matrices(definition)
        a_name = f"{definition.case_id}_a.npy"
        b_name = f"{definition.case_id}_b.npy"
        a_path = matrix_dir / a_name
        b_path = matrix_dir / b_name
        a_file_digest = atomic_save_npy(a_path, a)
        b_file_digest = atomic_save_npy(b_path, b)
        entries.append(
            _case_manifest_entry(
                definition,
                a_path=f"matrices/{a_name}",
                b_path=f"matrices/{b_name}",
                a=a,
                b=b,
                a_file_sha256=a_file_digest,
                b_file_sha256=b_file_digest,
            )
        )
    shapes = {
        "schema_version": 1,
        "generator_version": GENERATOR_VERSION,
        "dtype_policy": "float64",
        "seed_policy": "numpy.random.default_rng(seed); values scaled by 0.25",
        "cases": [shape_list_entry(entry) for entry in entries],
    }
    manifest = {
        "schema_version": 1,
        "generator_version": GENERATOR_VERSION,
        "seed_policy": shapes["seed_policy"],
        "shape_list_file": "shapes.json",
        "cases": entries,
    }
    atomic_write_json(output / "shapes.json", shapes)
    atomic_write_json(manifest_path, manifest)
    return manifest


def load_manifest(manifest_path: str | os.PathLike[str]) -> dict[str, Any]:
    path = Path(manifest_path)
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise BlockGemmError(
            E_INVALID_SHAPE,
            f"sample manifest does not exist: {path}",
            reason="manifest_missing",
            details={"manifest": str(path)},
        ) from error
    except Exception as error:
        raise BlockGemmError(
            E_INVALID_SHAPE,
            f"sample manifest is not valid JSON: {path}",
            reason="manifest_invalid",
            details={"manifest": str(path), "error": str(error)},
        ) from error
    if not isinstance(manifest, dict) or not isinstance(manifest.get("cases"), list):
        raise BlockGemmError(
            E_INVALID_SHAPE,
            "sample manifest does not contain a cases list",
            reason="manifest_invalid",
            details={"manifest": str(path)},
        )
    manifest["_manifest_path"] = str(path.resolve())
    manifest["_manifest_dir"] = str(path.resolve().parent)
    return manifest


@dataclass(frozen=True)
class LoadedSample:
    entry: dict[str, Any]
    a: np.ndarray
    b: np.ndarray
    expected: ShapeExpectation
    a_path: Path
    b_path: Path


def case_entry(manifest: dict[str, Any], case_id: str) -> dict[str, Any]:
    for entry in manifest.get("cases", []):
        if entry.get("id") == case_id:
            return entry
    raise BlockGemmError(
        E_INVALID_SHAPE,
        f"sample case not found: {case_id}",
        reason="sample_case_missing",
        details={"case_id": case_id, "available": [e.get("id") for e in manifest.get("cases", [])]},
    )


def load_case(manifest_path: str | os.PathLike[str], case_id: str) -> LoadedSample:
    manifest = load_manifest(manifest_path)
    entry = case_entry(manifest, case_id)
    base = Path(manifest["_manifest_dir"])
    a_path = base / entry["a_file"]
    b_path = base / entry["b_file"]
    try:
        a = np.load(a_path, allow_pickle=False)
        b = np.load(b_path, allow_pickle=False)
    except FileNotFoundError as error:
        raise invalid_shape(
            f"sample matrix file is missing for case {case_id}",
            shard_id="gemm" if entry.get("kind") == "gemm" else "bmm",
            reason="sample_file_missing",
            details={"case_id": case_id, "error": str(error)},
        ) from error
    except Exception as error:
        raise invalid_shape(
            f"sample matrix file cannot be read for case {case_id}",
            shard_id="gemm" if entry.get("kind") == "gemm" else "bmm",
            reason="sample_file_invalid",
            details={"case_id": case_id, "error": str(error)},
        ) from error
    expected = ShapeExpectation(
        m=int(entry["M"]),
        n=int(entry["N"]),
        k=int(entry["K"]),
        batch=int(entry["batch"]),
        dtype=str(entry["dtype"]),
        kind=str(entry["kind"]),
        a_transpose=bool(entry["a_transpose"]),
        b_transpose=bool(entry["b_transpose"]),
        case_id=case_id,
    )
    # Shape-list validation is deliberately done by the kernel; content changes are
    # allowed so the invalidation path can observe them.
    if list(a.shape) != list(entry["a_stored_shape"]):
        raise invalid_shape(
            "stored A shape differs from the shape manifest",
            shard_id="gemm" if entry["kind"] == "gemm" else "batch-0000",
            reason="shape_manifest_mismatch",
            details={"case_id": case_id, "expected": entry["a_stored_shape"], "actual": list(a.shape)},
        )
    if list(b.shape) != list(entry["b_stored_shape"]):
        raise invalid_shape(
            "stored B shape differs from the shape manifest",
            shard_id="gemm" if entry["kind"] == "gemm" else "batch-0000",
            reason="shape_manifest_mismatch",
            details={"case_id": case_id, "expected": entry["b_stored_shape"], "actual": list(b.shape)},
        )
    return LoadedSample(entry=entry, a=a, b=b, expected=expected, a_path=a_path, b_path=b_path)


def ensure_samples(manifest_path: str | os.PathLike[str]) -> dict[str, Any]:
    path = Path(manifest_path)
    if path.is_file():
        return load_manifest(path)
    generate_samples(path.parent, force=False)
    return load_manifest(path)
