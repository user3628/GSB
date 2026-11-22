from __future__ import annotations

import contextlib
import hashlib
import io
import json
import math
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Iterator

import numpy as np

from .errors import WorkbenchError


def to_jsonable(value: Any) -> Any:
    if isinstance(value, np.ndarray):
        return [to_jsonable(item) for item in value.tolist()]
    if isinstance(value, np.generic):
        return to_jsonable(value.item())
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(item) for item in value]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Canonical JSON does not permit NaN or infinity")
        if value == 0.0:
            return 0.0
        return float(f"{value:.15g}")
    return value


def canonical_json_bytes(value: Any) -> bytes:
    normalized = to_jsonable(value)
    text = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return text.encode("utf-8")


def canonical_json_text(value: Any) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def digest_object(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path, data: bytes, mode: int = 0o644) -> None:
    """Write a complete file or leave the previous file untouched."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.chmod(temp_path, mode)
        except OSError:
            pass
        os.replace(temp_path, path)
        _fsync_directory(path.parent)
    except BaseException:
        with contextlib.suppress(OSError):
            temp_path.unlink()
        raise


def atomic_write_text(path: Path, text: str, mode: int = 0o644) -> None:
    atomic_write_bytes(path, text.encode("utf-8"), mode=mode)


def atomic_write_json(path: Path, value: Any, trailing_newline: bool = True) -> None:
    data = canonical_json_bytes(value)
    if trailing_newline:
        data += b"\n"
    atomic_write_bytes(path, data)


def read_json(path: Path) -> Any:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def atomic_write_npz(path: Path, **arrays: np.ndarray) -> None:
    buffer = io.BytesIO()
    np.savez(buffer, **arrays)
    atomic_write_bytes(path, buffer.getvalue())


def load_npz(path: Path) -> dict[str, np.ndarray]:
    with np.load(path, allow_pickle=False) as loaded:
        return {name: loaded[name] for name in loaded.files}


def stable_float(value: float, digits: int = 12) -> float:
    if not math.isfinite(float(value)):
        raise ValueError("non-finite float")
    if float(value) == 0.0:
        return 0.0
    return float(f"{float(value):.{digits}g}")


def stable_matrix_digest(matrix: np.ndarray) -> str:
    array = np.ascontiguousarray(matrix, dtype="<f8")
    digest = hashlib.sha256()
    digest.update(canonical_json_bytes(list(array.shape)))
    digest.update(array.tobytes(order="C"))
    return digest.hexdigest()


def stable_vector_digest(vector: np.ndarray) -> str:
    array = np.ascontiguousarray(vector, dtype="<i8")
    digest = hashlib.sha256()
    digest.update(canonical_json_bytes(list(array.shape)))
    digest.update(array.tobytes(order="C"))
    return digest.hexdigest()


class FileLockTimeout(WorkbenchError):
    def __init__(self, path: Path, timeout: float | None) -> None:
        super().__init__(
            "E_STATE_INVALID",
            "Timed out while acquiring the checkpoint file lock",
            {"path": str(path), "timeout_seconds": timeout},
        )


class FileLock:
    """A real OS file lock using fcntl on POSIX and msvcrt on Windows."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._handle: io.IOBase | None = None

    def acquire(self, timeout: float | None = None) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.touch(exist_ok=True)
        handle = self.path.open("r+b")
        start = time.monotonic()
        while True:
            try:
                self._lock_handle(handle)
                self._handle = handle
                return self
            except OSError:
                if timeout is not None and time.monotonic() - start >= timeout:
                    handle.close()
                    raise FileLockTimeout(self.path, timeout)
                time.sleep(0.01)

    @staticmethod
    def _lock_handle(handle: io.IOBase) -> None:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            if handle.read(1) == b"":
                handle.seek(0)
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def release(self) -> None:
        if self._handle is None:
            return
        handle = self._handle
        self._handle = None
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()

    def __enter__(self) -> "FileLock":
        return self.acquire()

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.release()


@contextlib.contextmanager
def atomic_directory_publish(temp_dir: Path, final_dir: Path) -> Iterator[Path]:
    """Publish a directory tree by a single rename where the platform allows it."""

    temp_dir = Path(temp_dir)
    final_dir = Path(final_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)
    yield temp_dir
    if final_dir.exists():
        old_dir = final_dir.with_name(f".{final_dir.name}.old")
        if old_dir.exists():
            import shutil

            shutil.rmtree(old_dir)
        os.replace(final_dir, old_dir)
        os.replace(temp_dir, final_dir)
        import shutil

        shutil.rmtree(old_dir, ignore_errors=True)
    else:
        os.replace(temp_dir, final_dir)
    _fsync_directory(final_dir.parent)

