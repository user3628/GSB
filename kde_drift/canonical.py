"""Canonical serialization, hashing, and deterministic file helpers."""

from __future__ import annotations

import hashlib
import io
import json
import math
import os
import struct
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Iterable

import numpy as np


def _json_default(value: Any) -> Any:
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"not JSON serializable: {type(value)!r}")


def canonical_json_bytes(value: Any, *, strict: bool = False) -> bytes:
    """Return the one canonical JSON encoding used for fingerprints/artifacts."""

    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=not strict,
        default=_json_default,
    ).encode("utf-8")


def canonical_json_text(value: Any, *, strict: bool = False) -> str:
    return canonical_json_bytes(value, strict=strict).decode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_object(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def array_digest(array: np.ndarray) -> str:
    """Hash dtype, shape, and C-order payload without pickle or ZIP metadata."""

    contiguous = np.ascontiguousarray(array)
    h = hashlib.sha256()
    h.update(contiguous.dtype.str.encode("ascii"))
    h.update(b"\0")
    h.update(struct.pack("<I", contiguous.ndim))
    for dim in contiguous.shape:
        h.update(struct.pack("<Q", int(dim)))
    h.update(contiguous.tobytes(order="C"))
    return h.hexdigest()


def dataset_arrays_digest(
    features: np.ndarray,
    labels: np.ndarray,
    column_names: Iterable[str],
    *,
    canonical_columns: bool = False,
) -> str:
    import numpy as np

    matrix = np.asarray(features, dtype=np.float64)
    if canonical_columns and matrix.ndim == 2:
        matrix = np.sort(matrix, axis=0, kind="mergesort")
    label_array = np.asarray(labels)
    if canonical_columns and label_array.ndim == 1:
        label_array = np.sort(label_array, kind="mergesort")
    return digest_object(
        {
            "features": array_digest(matrix),
            "labels": array_digest(label_array),
            "columns": list(column_names),
        }
    )


def file_sha256(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def fsync_directory(path: str | Path) -> None:
    try:
        fd = os.open(Path(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path: str | Path, data: bytes, *, mode: int = 0o644) -> None:
    """Publish bytes atomically in the destination directory."""

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.tmp-", dir=destination.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, destination)
        fsync_directory(destination.parent)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def atomic_write_json(path: str | Path, value: Any, *, strict: bool = True) -> None:
    atomic_write_bytes(path, canonical_json_bytes(value, strict=strict))


def deterministic_npz_bytes(arrays: dict[str, np.ndarray]) -> bytes:
    """Create a byte-reproducible NPZ using fixed ZIP timestamps/metadata."""

    output = io.BytesIO()
    with zipfile.ZipFile(
        output,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6,
        allowZip64=True,
    ) as archive:
        for name in sorted(arrays):
            payload = io.BytesIO()
            np.lib.format.write_array(payload, np.asarray(arrays[name]), allow_pickle=False)
            info = zipfile.ZipInfo(f"{name}.npy", date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o600 << 16
            archive.writestr(info, payload.getvalue(), compresslevel=6)
    return output.getvalue()


def write_deterministic_npz(path: str | Path, arrays: dict[str, np.ndarray]) -> None:
    atomic_write_bytes(path, deterministic_npz_bytes(arrays))


def json_safe(value: Any) -> Any:
    """Convert non-finite floats into stable strings for HTTP/CLI responses."""

    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, np.ndarray):
        return json_safe(value.tolist())
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        if math.isnan(value):
            return "nan"
        return "inf" if value > 0 else "-inf"
    return value


def read_json(path: str | Path) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)
