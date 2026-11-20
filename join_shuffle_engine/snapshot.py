from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from .errors import EngineError


SNAPSHOT_FORMAT = "join-shuffle-columnar-v1"
KEY_DTYPE = "<i8"
MEASURE_DTYPE = "<f8"
NULL_DTYPE = "|u1"
ALLOWED_DIMENSIONS = {"dimensionless"}
ALLOWED_UNITS = {"score", "ratio", "count"}
REQUIRED_ARRAYS = ("keys", "measures", "nulls")


@dataclass(frozen=True)
class Snapshot:
    snapshot_id: str
    side: str
    version: int
    generated_at: str
    path: Path
    content_sha256: str
    keys: np.ndarray
    measures: np.ndarray
    nulls: np.ndarray
    measure_dimension: str
    measure_unit: str

    @property
    def row_count(self) -> int:
        return int(self.keys.shape[0])

    def metadata(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "side": self.side,
            "version": self.version,
            "generated_at": self.generated_at,
            "path": str(self.path),
            "content_sha256": self.content_sha256,
            "row_count": self.row_count,
            "key_dtype": KEY_DTYPE,
            "measure_dtype": MEASURE_DTYPE,
            "null_bitmap_dtype": NULL_DTYPE,
            "measure_dimension": self.measure_dimension,
            "measure_unit": self.measure_unit,
            "null_count": int(np.count_nonzero(self.nulls)),
        }


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _content_digest(core: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(core)).hexdigest()


def _snapshot_error(reason: str, path: Path | str, **details: Any) -> EngineError:
    payload = {"reason": reason, "path": str(path)}
    payload.update(details)
    return EngineError("E_SNAPSHOT_INVALID", f"snapshot validation failed: {reason}", payload)


def _validate_generated_at(value: Any, path: Path) -> str:
    if not isinstance(value, str) or not value:
        raise _snapshot_error("generated_at_missing", path)
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise _snapshot_error("generated_at_invalid", path, value=value) from exc
    return value


def _validate_arrays(
    keys: np.ndarray,
    measures: np.ndarray,
    nulls: np.ndarray,
    path: Path | str,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    keys = np.asarray(keys)
    measures = np.asarray(measures)
    nulls = np.asarray(nulls)
    if keys.ndim != 1 or measures.ndim != 1 or nulls.ndim != 1:
        raise _snapshot_error("column_not_one_dimensional", path)
    if keys.shape[0] != measures.shape[0] or keys.shape[0] != nulls.shape[0]:
        raise _snapshot_error(
            "column_length_mismatch",
            path,
            keys=int(keys.shape[0]),
            measures=int(measures.shape[0]),
            nulls=int(nulls.shape[0]),
        )
    if keys.dtype.kind != "i" or keys.dtype.itemsize != 8:
        raise _snapshot_error("invalid_key_dtype", path, dtype=str(keys.dtype))
    if measures.dtype.kind != "f" or measures.dtype.itemsize != 8:
        raise _snapshot_error("invalid_measure_dtype", path, dtype=str(measures.dtype))
    if nulls.dtype.kind not in {"u", "i", "b"} or nulls.dtype.itemsize != 1:
        raise _snapshot_error("invalid_null_bitmap_dtype", path, dtype=str(nulls.dtype))
    if np.any((nulls != 0) & (nulls != 1)):
        bad = int(np.flatnonzero((nulls != 0) & (nulls != 1))[0])
        raise _snapshot_error("null_bitmap_value_invalid", path, position=bad)
    null_bool = nulls.astype(bool, copy=False)
    if np.any(measures[null_bool] != 0.0):
        bad = int(np.flatnonzero(null_bool & (measures != 0.0))[0])
        raise _snapshot_error(
            "null_position_measure_not_zero", path, position=bad
        )
    if np.any(~np.isfinite(measures[~null_bool])):
        bad = int(np.flatnonzero((~null_bool) & ~np.isfinite(measures))[0])
        raise _snapshot_error("non_null_measure_not_finite", path, position=bad)
    return (
        np.ascontiguousarray(keys, dtype="<i8"),
        np.ascontiguousarray(measures, dtype="<f8"),
        np.ascontiguousarray(null_bool, dtype="|u1"),
    )


def save_snapshot(
    directory: str | Path,
    *,
    snapshot_id: str,
    side: str,
    version: int,
    generated_at: str,
    keys: np.ndarray,
    measures: np.ndarray,
    nulls: np.ndarray,
    measure_dimension: str = "dimensionless",
    measure_unit: str = "score",
) -> Snapshot:
    """Write an immutable snapshot directory atomically within its parent."""

    target = Path(directory)
    if target.exists():
        raise _snapshot_error("snapshot_directory_already_exists", target)
    if side not in {"left", "right"}:
        raise _snapshot_error("invalid_side", target, side=side)
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise _snapshot_error("invalid_version", target, version=version)
    if not isinstance(snapshot_id, str) or not snapshot_id:
        raise _snapshot_error("snapshot_id_missing", target)
    if measure_dimension not in ALLOWED_DIMENSIONS:
        raise _snapshot_error(
            "invalid_measure_dimension",
            target,
            dimension=measure_dimension,
            allowed=sorted(ALLOWED_DIMENSIONS),
        )
    if measure_unit not in ALLOWED_UNITS:
        raise _snapshot_error(
            "invalid_measure_unit",
            target,
            unit=measure_unit,
            allowed=sorted(ALLOWED_UNITS),
        )
    generated_at = _validate_generated_at(generated_at, target)
    keys, measures, nulls = _validate_arrays(keys, measures, nulls, target)
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        files = {
            "keys": ("keys.bin", keys),
            "measures": ("measures.bin", measures),
            "nulls": ("nulls.bin", nulls),
        }
        array_meta: dict[str, Any] = {}
        for name, (filename, array) in files.items():
            file_path = staging / filename
            array.tofile(file_path)
            with file_path.open("rb+") as handle:
                os.fsync(handle.fileno())
            dtype = {"keys": KEY_DTYPE, "measures": MEASURE_DTYPE, "nulls": NULL_DTYPE}[name]
            array_meta[name] = {
                "file": filename,
                "dtype": dtype,
                "bytes": int(array.nbytes),
                "sha256": _sha256_file(file_path),
            }
        core = {
            "format": SNAPSHOT_FORMAT,
            "snapshot_id": snapshot_id,
            "side": side,
            "version": version,
            "generated_at": generated_at,
            "row_count": int(keys.shape[0]),
            "byte_order": "little",
            "key_dtype": KEY_DTYPE,
            "measure_dtype": MEASURE_DTYPE,
            "null_bitmap_dtype": NULL_DTYPE,
            "measure_dimension": measure_dimension,
            "measure_unit": measure_unit,
            "arrays": array_meta,
        }
        manifest = dict(core)
        manifest["content_sha256"] = _content_digest(core)
        manifest_path = staging / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        with manifest_path.open("rb+") as handle:
            os.fsync(handle.fileno())
        staging.replace(target)
        return load_snapshot(target)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def load_snapshot(directory: str | Path) -> Snapshot:
    path = Path(directory)
    manifest_path = path / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise _snapshot_error("manifest_missing", path) from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise _snapshot_error("manifest_unreadable", path, error=str(exc)) from exc
    if not isinstance(manifest, dict):
        raise _snapshot_error("manifest_not_object", path)
    if manifest.get("format") != SNAPSHOT_FORMAT:
        raise _snapshot_error("format_unsupported", path, format=manifest.get("format"))
    supplied_digest = manifest.get("content_sha256")
    if not isinstance(supplied_digest, str):
        raise _snapshot_error("content_digest_missing", path)
    core = dict(manifest)
    core.pop("content_sha256", None)
    expected_digest = _content_digest(core)
    if supplied_digest != expected_digest:
        raise _snapshot_error(
            "content_digest_mismatch",
            path,
            expected=expected_digest,
            actual=supplied_digest,
        )
    format_fields = {
        "format": SNAPSHOT_FORMAT,
        "byte_order": "little",
        "key_dtype": KEY_DTYPE,
        "measure_dtype": MEASURE_DTYPE,
        "null_bitmap_dtype": NULL_DTYPE,
    }
    for field, expected in format_fields.items():
        if manifest.get(field) != expected:
            raise _snapshot_error(f"{field}_unsupported", path, value=manifest.get(field))
    snapshot_id = manifest.get("snapshot_id")
    side = manifest.get("side")
    version = manifest.get("version")
    generated_at = _validate_generated_at(manifest.get("generated_at"), path)
    if not isinstance(snapshot_id, str) or not snapshot_id:
        raise _snapshot_error("snapshot_id_missing", path)
    if side not in {"left", "right"}:
        raise _snapshot_error("invalid_side", path, side=side)
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise _snapshot_error("invalid_version", path, version=version)
    dimension = manifest.get("measure_dimension")
    if dimension not in ALLOWED_DIMENSIONS:
        raise _snapshot_error(
            "invalid_measure_dimension",
            path,
            dimension=dimension,
            allowed=sorted(ALLOWED_DIMENSIONS),
        )
    unit = manifest.get("measure_unit")
    if unit not in ALLOWED_UNITS:
        raise _snapshot_error(
            "invalid_measure_unit", path, unit=unit, allowed=sorted(ALLOWED_UNITS)
        )
    row_count = manifest.get("row_count")
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
        raise _snapshot_error("invalid_row_count", path, row_count=row_count)
    arrays_meta = manifest.get("arrays")
    if not isinstance(arrays_meta, dict) or set(arrays_meta) != set(REQUIRED_ARRAYS):
        raise _snapshot_error("array_manifest_invalid", path)
    arrays: dict[str, np.ndarray] = {}
    dtype_map = {
        "keys": np.dtype(KEY_DTYPE),
        "measures": np.dtype(MEASURE_DTYPE),
        "nulls": np.dtype(NULL_DTYPE),
    }
    expected_dtype_text = {
        "keys": KEY_DTYPE,
        "measures": MEASURE_DTYPE,
        "nulls": NULL_DTYPE,
    }
    for name in REQUIRED_ARRAYS:
        meta = arrays_meta.get(name)
        if not isinstance(meta, dict):
            raise _snapshot_error("array_metadata_invalid", path, array=name)
        if meta.get("dtype") != expected_dtype_text[name]:
            raise _snapshot_error(
                "array_dtype_mismatch", path, array=name, dtype=meta.get("dtype")
            )
        filename = meta.get("file")
        if not isinstance(filename, str) or Path(filename).name != filename:
            raise _snapshot_error("array_filename_invalid", path, array=name)
        file_path = path / filename
        expected_bytes = int(row_count * dtype_map[name].itemsize)
        expected_meta_bytes = meta.get("bytes")
        if expected_meta_bytes != expected_bytes:
            raise _snapshot_error(
                "array_size_mismatch",
                path,
                array=name,
                expected=expected_bytes,
                actual=expected_meta_bytes,
            )
        try:
            if file_path.stat().st_size != expected_bytes:
                raise _snapshot_error(
                    "array_file_size_mismatch",
                    path,
                    array=name,
                    expected=expected_bytes,
                    actual=file_path.stat().st_size,
                )
            actual_hash = _sha256_file(file_path)
        except FileNotFoundError as exc:
            raise _snapshot_error("array_file_missing", path, array=name) from exc
        except OSError as exc:
            raise _snapshot_error(
                "array_file_unreadable", path, array=name, error=str(exc)
            ) from exc
        if actual_hash != meta.get("sha256"):
            raise _snapshot_error(
                "array_digest_mismatch",
                path,
                array=name,
                expected=meta.get("sha256"),
                actual=actual_hash,
            )
        arrays[name] = np.fromfile(file_path, dtype=dtype_map[name], count=row_count)
    keys, measures, nulls = _validate_arrays(
        arrays["keys"], arrays["measures"], arrays["nulls"], path
    )
    return Snapshot(
        snapshot_id=snapshot_id,
        side=side,
        version=version,
        generated_at=generated_at,
        path=path.resolve(),
        content_sha256=supplied_digest,
        keys=keys,
        measures=measures,
        nulls=nulls,
        measure_dimension=dimension,
        measure_unit=unit,
    )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
