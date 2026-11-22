"""Cross-process file locking, atomic writes and deterministic serialization.

The implementations rely only on the Python standard library and behave
consistently on POSIX and Windows:

* :func:`atomic_write_bytes` writes through a unique temp file in the same
  directory then ``os.replace``\\ s it into place, so a crash at any point can
  never leave a half-written target file.
* :class:`FileLock` uses ``fcntl`` on POSIX and ``msvcrt`` on Windows.
* JSON is always emitted with ``\\n`` newlines, ``ensure_ascii=False`` and
  sorted keys, which makes byte-identical outputs reproducible across machines
  and platforms (Windows CRLF vs POSIX LF differences are avoided).
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
from contextlib import contextmanager
from typing import Any, Dict, Iterator

IS_WINDOWS = sys.platform.startswith("win")

if IS_WINDOWS:  # pragma: no cover - exercised only on Windows
    import msvcrt  # type: ignore
else:
    import fcntl  # type: ignore


def canonical_json(obj: Any) -> str:
    """Return a deterministic JSON string with LF line endings."""
    return json.dumps(
        obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    )


def canonical_pretty_json(obj: Any) -> str:
    """Deterministic, human-readable JSON (LF endings, sorted keys)."""
    return json.dumps(
        obj, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False,
    ) + "\n"


def digest_json(obj: Any) -> str:
    """SHA-256 content fingerprint of a deterministically serialized object."""
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


def digest_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def atomic_write_bytes(path: str, data: bytes) -> None:
    """Atomically write ``data`` to ``path`` (no half-written target file)."""
    directory = os.path.dirname(os.path.abspath(path)) or "."
    ensure_dir(directory)
    fd, tmp_path = tempfile.mkstemp(
        prefix=".tmp-" + os.path.basename(path) + "-", dir=directory,
    )
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            try:
                os.fsync(handle.fileno())
            except OSError:  # pragma: no cover - some filesystems lack fsync
                pass
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def atomic_write_text(path: str, text: str) -> None:
    # Normalize any incoming CRLF so on-disk records are byte-identical on
    # Windows and POSIX.
    atomic_write_bytes(path, text.replace("\r\n", "\n").replace("\r", "\n")
                       .encode("utf-8"))


def write_json(path: str, obj: Any, *, pretty: bool = False) -> None:
    text = canonical_pretty_json(obj) if pretty else canonical_json(obj) + "\n"
    atomic_write_text(path, text)


def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8", newline="\n") as handle:
        return json.load(handle)


def read_jsonl(path: str) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8", newline="\n") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def write_jsonl(path: str, rows: list[Any]) -> None:
    lines = [canonical_json(row) for row in rows]
    atomic_write_text(path, "\n".join(lines) + ("\n" if lines else ""))


class FileLock:
    """An advisory, cross-process exclusive lock backed by a real OS lock."""

    def __init__(self, lock_path: str) -> None:
        self.lock_path = lock_path
        ensure_dir(os.path.dirname(os.path.abspath(lock_path)) or ".")
        self._handle = None

    def acquire(self) -> "FileLock":
        self._handle = open(self.lock_path, "a+b")
        try:
            if IS_WINDOWS:  # pragma: no cover - Windows only
                msvcrt.locking(  # type: ignore[name-defined]
                    self._handle.fileno(), msvcrt.LK_LOCK, 1,
                )
            else:
                fcntl.flock(  # type: ignore[name-defined]
                    self._handle.fileno(), fcntl.LOCK_EX,
                )
        except BaseException:
            self._handle.close()
            self._handle = None
            raise
        return self

    def release(self) -> None:
        if self._handle is None:
            return
        try:
            if IS_WINDOWS:  # pragma: no cover - Windows only
                self._handle.seek(0)
                msvcrt.locking(  # type: ignore[name-defined]
                    self._handle.fileno(), msvcrt.LK_UNLCK, 1,
                )
            else:
                fcntl.flock(  # type: ignore[name-defined]
                    self._handle.fileno(), fcntl.LOCK_UN,
                )
        finally:
            self._handle.close()
            self._handle = None

    def __enter__(self) -> "FileLock":
        return self.acquire()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


@contextmanager
def file_lock(lock_path: str) -> Iterator[None]:
    lock = FileLock(lock_path)
    lock.acquire()
    try:
        yield
    finally:
        lock.release()
