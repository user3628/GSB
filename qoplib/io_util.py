"""Atomic file writes, deterministic JSON and deterministic .npz storage."""

from __future__ import annotations

import hashlib
import io
import os
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Mapping

import numpy as np

from .fingerprints import canonical_json_text

# Fixed ZIP member timestamp so identical arrays always produce identical .npz
# bytes (ZIP headers embed modification times otherwise).
_FIXED_ZIP_DATE = (1980, 1, 1, 0, 0, 0)


def ensure_dir(path: os.PathLike[str] | str) -> Path:
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def atomic_write_bytes(path: os.PathLike[str] | str, data: bytes) -> Path:
    """Write ``data`` atomically.

    The file is written to a temporary name in the same directory, flushed,
    fsynced and then renamed with :func:`os.replace`, which is atomic on both
    POSIX and Windows.  A crash at any point therefore leaves either the old
    file or the new file, never a half-written record.
    """

    path = Path(path)
    ensure_dir(path.parent)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp",
                                    dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        _fsync_directory(path.parent)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    return path


def atomic_write_text(path: os.PathLike[str] | str, text: str) -> Path:
    return atomic_write_bytes(path, text.encode("utf-8"))


def atomic_write_json(path: os.PathLike[str] | str, obj: Any) -> Path:
    return atomic_write_text(path, canonical_json_text(obj) + "\n")


def _fsync_directory(directory: Path) -> None:
    try:
        fd = os.open(str(directory), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def save_npz_deterministic(path: os.PathLike[str] | str,
                           arrays: Mapping[str, np.ndarray]) -> Path:
    """Save arrays to a byte-reproducible .npz (zip of .npy) archive."""

    buffer = io.BytesIO()
    # Deterministic member order, fixed timestamp and fixed compression level.
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED,
                         compresslevel=6) as archive:
        for name in sorted(arrays):
            array = np.ascontiguousarray(arrays[name])
            npy_buffer = io.BytesIO()
            np.lib.format.write_array(npy_buffer, array, allow_pickle=False)
            info = zipfile.ZipInfo(f"{name}.npy", date_time=_FIXED_ZIP_DATE)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o600 << 16
            archive.writestr(info, npy_buffer.getvalue())
    return atomic_write_bytes(path, buffer.getvalue())


def load_npz(path: os.PathLike[str] | str) -> dict[str, np.ndarray]:
    with np.load(Path(path), allow_pickle=False) as loaded:
        return {name: loaded[name] for name in loaded.files}


def hash_file(path: os.PathLike[str] | str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_file(path: os.PathLike[str] | str) -> str:
    return hash_file(path)
