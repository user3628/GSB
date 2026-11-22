from __future__ import annotations

import hashlib
import io
import json
import os
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np


ZIP_EPOCH = (1980, 1, 1, 0, 0, 0)


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def sha256_file(path: str | os.PathLike[str]) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def array_digest(array: np.ndarray) -> str:
    contiguous = np.asarray(array)
    if contiguous.ndim > 0:
        contiguous = np.ascontiguousarray(contiguous)
    digest = hashlib.sha256()
    digest.update(contiguous.dtype.str.encode("ascii"))
    digest.update(b"\0")
    digest.update(canonical_json_bytes(list(contiguous.shape)))
    digest.update(b"\0")
    digest.update(contiguous.tobytes(order="C"))
    return digest.hexdigest()


def digests_combined(parts: Mapping[str, str] | Sequence[tuple[str, str]]) -> str:
    items = parts.items() if isinstance(parts, Mapping) else parts
    return sha256_json({str(key): str(value) for key, value in sorted(items)})


def _npy_bytes(array: np.ndarray) -> bytes:
    contiguous = np.asarray(array)
    if contiguous.ndim > 0:
        contiguous = np.ascontiguousarray(contiguous)
    buffer = io.BytesIO()
    np.lib.format.write_array(buffer, contiguous, allow_pickle=False)
    return buffer.getvalue()


def deterministic_npz_bytes(arrays: Mapping[str, np.ndarray]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_STORED) as archive:
        for key in sorted(arrays):
            payload = _npy_bytes(np.asarray(arrays[key]))
            info = zipfile.ZipInfo(filename=f"{key}.npy", date_time=ZIP_EPOCH)
            info.compress_type = zipfile.ZIP_STORED
            info.external_attr = (0o600 & 0xFFFF) << 16
            archive.writestr(info, payload)
    return buffer.getvalue()


def deterministic_npz_write(path: str | os.PathLike[str], arrays: Mapping[str, np.ndarray]) -> str:
    data = deterministic_npz_bytes(arrays)
    atomic_write_bytes(path, data)
    return sha256_bytes(data)


def load_npz(path: str | os.PathLike[str]) -> dict[str, np.ndarray]:
    with np.load(path, allow_pickle=False) as archive:
        return {key: archive[key] for key in archive.files}


def json_scalar(value: np.ndarray | np.generic | Any) -> Any:
    if isinstance(value, np.ndarray):
        if value.shape != ():
            return value.tolist()
        value = value.item()
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        try:
            os.fsync(descriptor)
        except OSError:
            pass
    finally:
        os.close(descriptor)


def atomic_write_bytes(path: str | os.PathLike[str], data: bytes) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=target.parent,
    )
    temporary_path = Path(temporary)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, target)
        _fsync_directory(target.parent)
    except BaseException:
        try:
            temporary_path.unlink()
        except OSError:
            pass
        raise


def atomic_write_json(path: str | os.PathLike[str], value: Any) -> str:
    data = canonical_json_bytes(value)
    atomic_write_bytes(path, data)
    return sha256_bytes(data)


def read_json(path: str | os.PathLike[str]) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)
