"""Deterministic serialization, hashing, atomic files and cross-process locks."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_json_text(value: Any, *, pretty: bool = False) -> str:
    if pretty:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        ) + "\n"
    return canonical_json_bytes(value).decode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write via a temporary file in the destination directory and os.replace.

    The temporary file is deliberately created beside the destination so that
    the final replace is on the same volume on Windows as well as POSIX.
    """

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp_path = Path(temporary)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        if os.name != "nt":
            os.chmod(temp_path, 0o644)
        os.replace(temp_path, path)
        _fsync_directory(path.parent)
    except BaseException:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass
        raise


def _fsync_directory(directory: Path) -> None:
    if os.name == "nt":
        return
    try:
        descriptor = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write_json(path: Path, value: Any, *, pretty: bool = False) -> None:
    text = canonical_json_text(value, pretty=pretty)
    atomic_write_bytes(path, text.encode("utf-8"))


def atomic_write_text(path: Path, text: str) -> None:
    atomic_write_bytes(path, text.encode("utf-8"))


def read_json(path: Path) -> Any:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def ensure_within(root: Path, candidate: Path, *, label: str = "path") -> Path:
    root = Path(root).resolve()
    resolved = Path(candidate).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        from .errors import WorkbenchError

        raise WorkbenchError(
            "E_INVALID_PARAM",
            f"{label} must stay inside the project directory",
            {"label": label, "project_root": str(root), "path": str(resolved)},
        ) from exc
    return resolved


def atomic_temp_is_same_directory(path: Path) -> bool:
    """Operational self-check for the Windows same-volume replacement rule."""

    path = Path(path)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        temp_path = Path(temporary)
        if os.name == "nt":
            return os.path.splitdrive(temp_path.resolve())[0].lower() == os.path.splitdrive(path.resolve())[0].lower()
        return temp_path.parent.resolve() == path.parent.resolve()
    finally:
        os.close(fd)
        try:
            Path(temporary).unlink()
        except FileNotFoundError:
            pass


class FileLock:
    """Small cross-process advisory lock using fcntl or msvcrt."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._handle = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            self._handle = self.path.open("r+b")
        else:
            self._handle = self.path.open("w+b")
            self._handle.write(b"0")
            self._handle.flush()
        if os.name == "nt":  # pragma: no cover - exercised on Windows
            import msvcrt

            self._handle.seek(0)
            msvcrt.locking(self._handle.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl

            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self._handle is None:
            return
        try:
            if os.name == "nt":  # pragma: no cover - exercised on Windows
                import msvcrt

                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None


def float_to_hex(value: float) -> str:
    return float(value).hex()


def floats_to_hex(values) -> list[str]:
    return [float(value).hex() for value in values]


def hex_to_floats(values) -> list[float]:
    return [float.fromhex(value) for value in values]


def stage_index(stage: str) -> int:
    order = ("binning", "boosting", "calibration", "diagnostics")
    try:
        return order.index(stage)
    except ValueError as exc:
        from .errors import WorkbenchError

        raise WorkbenchError(
            "E_INVALID_STAGE",
            f"unknown stage: {stage}",
            {"stage": stage, "valid_stages": list(order)},
        ) from exc
