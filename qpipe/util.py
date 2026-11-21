from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import shutil
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Any, Iterator

import numpy as np

from .errors import QPipeIOError


def canonical_json_bytes(value: Any, *, pretty: bool = False) -> bytes:
    if pretty:
        text = json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            indent=2,
            allow_nan=False,
            separators=(",", ": "),
        )
    else:
        text = json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    return (text + "\n").encode("utf-8")


def json_ready(value: Any) -> Any:
    if isinstance(value, np.ndarray):
        return json_ready(value.tolist())
    if isinstance(value, np.generic):
        return json_ready(value.item())
    if isinstance(value, dict):
        return {str(key): json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(item) for item in value]
    if isinstance(value, float):
        if not np.isfinite(value):
            raise ValueError("non-finite float cannot be serialized")
        return value
    return value


def digest_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_obj(value: Any) -> str:
    return digest_bytes(canonical_json_bytes(json_ready(value)))


def digest_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def array_fingerprint(values: np.ndarray) -> str:
    array = np.ascontiguousarray(values)
    header = canonical_json_bytes(
        {"dtype": array.dtype.str, "shape": list(array.shape), "order": "C"}
    )
    return digest_bytes(header + array.tobytes(order="C"))


def ensure_finite(values: np.ndarray, *, layer: str, parameter: str) -> None:
    finite = np.isfinite(values)
    if bool(np.all(finite)):
        return
    from .errors import NumericInput

    bad = np.argwhere(~finite)
    position = [int(item) for item in bad[0]]
    raise NumericInput(
        f"non-finite value detected in {parameter}",
        layer=layer,
        parameter=parameter,
        details={
            "position": position,
            "non_finite_count": int(bad.shape[0]),
        },
    )


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_name = f".tmp-{path.name}-{os.getpid()}-{time.time_ns()}"
    temp_path = path.parent / temp_name
    try:
        with temp_path.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        _fsync_directory(path.parent)
    except OSError as exc:
        with contextlib.suppress(OSError):
            temp_path.unlink()
        raise QPipeIOError(f"atomic write failed for {path}: {exc}") from exc


def atomic_write_json(path: Path, value: Any, *, pretty: bool = True) -> None:
    atomic_write_bytes(path, canonical_json_bytes(value, pretty=pretty))


def load_json(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise QPipeIOError(f"cannot read JSON {path}: {exc}") from exc


def clean_stale_temp_files(root: Path, *, minimum_age_seconds: int = 3600) -> None:
    if not root.exists():
        return
    now = time.time()
    for path in root.rglob(".tmp-*"):
        with contextlib.suppress(OSError):
            if now - path.stat().st_mtime >= minimum_age_seconds:
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()


class FileLock:
    """Exclusive advisory lock backed by a real OS file lock."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._handle: io.BufferedRandom | None = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        handle.seek(0)
        handle.write(b"0")
        handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            while True:
                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
                    break
                except OSError:
                    time.sleep(0.01)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        self._handle = handle
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        handle = self._handle
        self._handle = None
        if handle is None:
            return
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                with contextlib.suppress(OSError):
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def deterministic_npz_bytes(arrays: dict[str, np.ndarray]) -> bytes:
    """Create a ZIP-based .npz with fixed metadata and stable array bytes."""

    output = io.BytesIO()
    with zipfile.ZipFile(
        output, mode="w", compression=zipfile.ZIP_STORED, allowZip64=True
    ) as archive:
        for name in sorted(arrays):
            array_buffer = io.BytesIO()
            np.lib.format.write_array(
                array_buffer,
                np.ascontiguousarray(arrays[name]),
                allow_pickle=False,
            )
            info = zipfile.ZipInfo(f"{name}.npy", date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_STORED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, array_buffer.getvalue())
    return output.getvalue()


def atomic_write_npz(path: Path, arrays: dict[str, np.ndarray]) -> None:
    atomic_write_bytes(path, deterministic_npz_bytes(arrays))


def copy_atomic(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_name = f".tmp-{destination.name}-{os.getpid()}-{time.time_ns()}"
    temp_path = destination.parent / temp_name
    try:
        with source.open("rb") as source_handle, temp_path.open("xb") as target:
            shutil.copyfileobj(source_handle, target, length=1024 * 1024)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temp_path, destination)
        _fsync_directory(destination.parent)
    except OSError as exc:
        with contextlib.suppress(OSError):
            temp_path.unlink()
        raise QPipeIOError(f"atomic copy failed {source} -> {destination}: {exc}") from exc


def exclusive_directory_publish(temp_dir: Path, final_dir: Path) -> bool:
    """Publish a fully written directory without overwriting an existing one."""

    final_dir.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.rename(temp_dir, final_dir)
        _fsync_directory(final_dir.parent)
        return True
    except FileExistsError:
        return False
    except OSError as exc:
        if final_dir.exists():
            return False
        raise QPipeIOError(f"directory publish failed {final_dir}: {exc}") from exc


@contextlib.contextmanager
def temporary_directory(parent: Path, prefix: str) -> Iterator[Path]:
    parent.mkdir(parents=True, exist_ok=True)
    path = Path(tempfile.mkdtemp(prefix=prefix, dir=parent))
    try:
        yield path
    finally:
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
