"""Atomic local writes, cross-platform advisory file locks, and snapshots."""

from __future__ import annotations

import json
import os
import shutil
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import numpy as np

from .canonical import canonical_json, digest_file, digest_json
from .errors import EngineError, E_CONFLICT, snapshot_conflict


_THREAD_LOCKS: dict[str, threading.Lock] = {}
_THREAD_LOCKS_GUARD = threading.Lock()


def _thread_lock(path: Path) -> threading.Lock:
    key = str(path.resolve())
    with _THREAD_LOCKS_GUARD:
        lock = _THREAD_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _THREAD_LOCKS[key] = lock
        return lock


class FileLock:
    """Exclusive lock that works on POSIX and Windows.

    POSIX uses ``flock``; Windows uses ``msvcrt.locking``. A process-local
    threading lock prevents two threads in one process from bypassing the
    platform lock semantics. The lock file is never used as the publication
    record itself.
    """

    def __init__(self, path: str | Path, timeout: float = 30.0) -> None:
        self.path = Path(path)
        self.timeout = timeout
        self._handle: Any = None
        self._thread_lock = _thread_lock(self.path)

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self._thread_lock.acquire(timeout=self.timeout):
            raise EngineError(
                E_CONFLICT,
                "timed out waiting for in-process workspace lock",
                {"path": str(self.path), "timeout": self.timeout},
            )
        try:
            self._handle = open(self.path, "a+b")
            self._acquire_platform()
        except Exception:
            if self._handle is not None:
                self._handle.close()
                self._handle = None
            self._thread_lock.release()
            raise
        return self

    def _acquire_platform(self) -> None:
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                if os.name == "nt":
                    import msvcrt

                    self._handle.seek(0)
                    if self._handle.read(1) == b"":
                        self._handle.seek(0)
                        self._handle.write(b"0")
                        self._handle.flush()
                    self._handle.seek(0)
                    msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(
                        self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB
                    )
                return
            except (BlockingIOError, OSError) as exc:
                if time.monotonic() >= deadline:
                    raise EngineError(
                        E_CONFLICT,
                        "timed out waiting for workspace file lock",
                        {"path": str(self.path), "timeout": self.timeout},
                    ) from exc
                time.sleep(0.01)

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        try:
            if self._handle is not None:
                if os.name == "nt":
                    import msvcrt

                    self._handle.seek(0)
                    try:
                        msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
                    except OSError:
                        pass
                else:
                    import fcntl

                    try:
                        fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
                    except OSError:
                        pass
                self._handle.close()
                self._handle = None
        finally:
            self._thread_lock.release()


def fsync_directory(path: str | Path) -> None:
    """Best-effort directory fsync (unsupported on Windows)."""

    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _temporary_sibling(path: Path) -> Path:
    suffix = f".tmp.{os.getpid()}.{threading.get_ident()}.{uuid.uuid4().hex}"
    return path.with_name(path.name + suffix)


def atomic_write_bytes(path: str | Path, data: bytes) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = _temporary_sibling(target)
    try:
        with open(temp, "xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, target)
        fsync_directory(target.parent)
    finally:
        if temp.exists():
            try:
                temp.unlink()
            except OSError:
                pass


def atomic_write_text(path: str | Path, text: str) -> None:
    atomic_write_bytes(path, text.encode("utf-8"))


def atomic_write_json(
    path: str | Path, value: Any, *, canonical: bool = False, indent: int = 2
) -> None:
    if canonical:
        text = canonical_json(value) + "\n"
    else:
        text = json.dumps(value, ensure_ascii=False, sort_keys=True, indent=indent) + "\n"
    atomic_write_text(path, text)


def atomic_write_npy(path: str | Path, array: np.ndarray) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = _temporary_sibling(target)
    try:
        with open(temp, "wb") as handle:
            np.save(handle, np.asarray(array), allow_pickle=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, target)
        fsync_directory(target.parent)
    finally:
        if temp.exists():
            try:
                temp.unlink()
            except OSError:
                pass


def atomic_write_npz(path: str | Path, **arrays: np.ndarray) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = _temporary_sibling(target)
    try:
        with open(temp, "wb") as handle:
            np.savez(handle, **arrays)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, target)
        fsync_directory(target.parent)
    finally:
        if temp.exists():
            try:
                temp.unlink()
            except OSError:
                pass


def move_atomically(source: str | Path, target: str | Path) -> None:
    src = Path(source)
    dst = Path(target)
    dst.parent.mkdir(parents=True, exist_ok=True)
    os.replace(src, dst)
    fsync_directory(dst.parent)
    if src.parent != dst.parent:
        try:
            fsync_directory(src.parent)
        except OSError:
            pass


def copy_atomically(source: str | Path, target: str | Path) -> None:
    src = Path(source)
    dst = Path(target)
    dst.parent.mkdir(parents=True, exist_ok=True)
    temp = _temporary_sibling(dst)
    try:
        with open(src, "rb") as reader, open(temp, "xb") as writer:
            shutil.copyfileobj(reader, writer)
            writer.flush()
            os.fsync(writer.fileno())
        os.replace(temp, dst)
        fsync_directory(dst.parent)
    finally:
        if temp.exists():
            try:
                temp.unlink()
            except OSError:
                pass


def read_json(path: str | Path) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def canonical_document_digest(value: Any) -> str:
    return digest_json(value)


def artifact_file_digest(path: str | Path) -> str:
    return digest_file(path)


def export_snapshot(workspace: str | Path, destination: str | Path) -> dict[str, Any]:
    """Export a deterministic JSON snapshot and its digest.

    Import verifies both the payload digest and the generation, so replacing a
    newer snapshot with an older bundle is reported as a version rollback.
    """

    root = Path(workspace)
    files: dict[str, str] = {}
    payloads: dict[str, Any] = {}
    candidates = [
        root / "workspace_meta.json",
        root / "registry.json",
        root / "request_state.json",
    ]
    if root.exists():
        for path in sorted(root.rglob("*.json")):
            if ".history" in path.parts or "staging" in path.parts:
                continue
            relative = path.relative_to(root).as_posix()
            files[relative] = digest_file(path)
            if relative in {
                "workspace_meta.json",
                "registry.json",
                "request_state.json",
            }:
                payloads[relative] = read_json(path)
    meta = payloads.get("workspace_meta.json", {})
    snapshot = {
        "snapshot_schema": 1,
        "workspace_id": meta.get("workspace_id"),
        "generation": int(meta.get("generation", 0)),
        "files": files,
        "state_files": payloads,
    }
    snapshot["snapshot_digest"] = digest_json(snapshot)
    atomic_write_json(destination, snapshot)
    return snapshot


def import_snapshot(
    snapshot_path: str | Path,
    current_workspace: str | Path,
    *,
    allow_rollback: bool = False,
) -> dict[str, Any]:
    snapshot = read_json(snapshot_path)
    if not isinstance(snapshot, dict):
        raise snapshot_conflict("snapshot root must be an object", reason="invalid_snapshot")
    expected = snapshot.pop("snapshot_digest", None)
    actual = digest_json(snapshot)
    if expected != actual:
        raise snapshot_conflict(
            "snapshot digest does not match its content",
            reason="digest_mismatch",
            expected=expected,
            actual=actual,
        )
    meta_path = Path(current_workspace) / "workspace_meta.json"
    if meta_path.exists():
        current = read_json(meta_path)
        current_generation = int(current.get("generation", 0))
        incoming_generation = int(snapshot.get("generation", 0))
        if incoming_generation < current_generation and not allow_rollback:
            raise snapshot_conflict(
                "snapshot generation would move the workspace backwards",
                reason="version_rollback",
                current_generation=current_generation,
                incoming_generation=incoming_generation,
            )
    snapshot["snapshot_digest"] = expected
    return snapshot


@contextmanager
def staging_path(request_dir: str | Path, filename: str) -> Iterator[Path]:
    folder = Path(request_dir) / "staging"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{filename}.{os.getpid()}.{threading.get_ident()}.{uuid.uuid4().hex}"
    try:
        yield path
    finally:
        if path.exists():
            try:
                path.unlink()
            except OSError:
                pass
