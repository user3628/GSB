from __future__ import annotations

import hashlib
import io
import json
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any, Mapping

import numpy as np


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def pretty_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            indent=2,
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def sha256_file(path: str | os.PathLike[str]) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def array_bytes(array: np.ndarray) -> bytes:
    buffer = io.BytesIO()
    np.save(buffer, np.ascontiguousarray(array), allow_pickle=False)
    return buffer.getvalue()


def sha256_array(array: np.ndarray) -> str:
    return sha256_bytes(array_bytes(array))


def write_bytes(path: str | os.PathLike[str], data: bytes, *, atomic: bool = True) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not atomic:
        path.write_bytes(data)
        return
    fd, tmp_name = tempfile.mkstemp(prefix=f".tmp-{path.name}-", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
        fsync_dir(path.parent)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def write_json(path: str | os.PathLike[str], value: Any, *, pretty: bool = True) -> None:
    data = pretty_json_bytes(value) if pretty else canonical_json_bytes(value)
    write_bytes(path, data)


def write_npy(path: str | os.PathLike[str], array: np.ndarray) -> None:
    write_bytes(path, array_bytes(array))


def read_json(path: str | os.PathLike[str]) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def load_npy(path: str | os.PathLike[str], *, layer: str | None = None) -> np.ndarray:
    from .errors import FusionError

    try:
        array = np.load(path, allow_pickle=False)
    except Exception as exc:  # noqa: BLE001 - normalized into public error shape
        raise FusionError.numeric(layer or "input", "npy_load_failed", path=str(path), reason_detail=str(exc))
    if not isinstance(array, np.ndarray):
        raise FusionError.numeric(layer or "input", "npy_not_array", path=str(path))
    return array


def fsync_dir(path: str | os.PathLike[str]) -> None:
    try:
        fd = os.open(os.fspath(path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def fsync_file(path: str | os.PathLike[str]) -> None:
    with open(path, "rb") as handle:
        os.fsync(handle.fileno())


def safe_relative_path(value: str, *, field_name: str = "path") -> Path:
    from .errors import FusionError

    candidate = Path(value)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise FusionError("E_INVALID_PARAM", f"{field_name} must be a relative path inside the input directory", details={"field": field_name, "value": value})
    return candidate


def resolve_under(base: Path, relative: str, *, field_name: str = "path") -> Path:
    return (base / safe_relative_path(relative, field_name=field_name)).resolve()


def stable_rng(material: str | bytes) -> np.random.Generator:
    digest = hashlib.sha256(material.encode("utf-8") if isinstance(material, str) else material).digest()
    seed = int.from_bytes(digest[:16], "little", signed=False)
    return np.random.Generator(np.random.PCG64(seed))


def new_tmp_name(prefix: str = "tmp") -> str:
    return f".{prefix}-{os.getpid()}-{uuid.uuid4().hex}"


def is_json_primitive(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def ensure_mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    from .errors import FusionError

    if not isinstance(value, Mapping):
        raise FusionError("E_INVALID_PARAM", f"{field_name} must be an object", details={"field": field_name})
    return value


def ensure_finite_float(value: Any, field_name: str, *, index: int | None = None) -> float:
    from .errors import FusionError

    try:
        result = float(value)
    except (TypeError, ValueError):
        raise FusionError("E_INVALID_PARAM", f"{field_name} must be numeric", index=index, details={"field": field_name, "value": repr(value)})
    if not np.isfinite(result):
        raise FusionError("E_INVALID_PARAM", f"{field_name} must be finite", index=index, details={"field": field_name, "value": repr(value)})
    return result
