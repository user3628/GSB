from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_json_text(value: Any) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def digest_obj(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def fsync_dir(path: Path) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        fsync_dir(path.parent)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            tmp.unlink()
        raise


def atomic_write_json(path: Path, value: Any, *, pretty: bool = False) -> None:
    if pretty:
        data = (
            json.dumps(
                value,
                sort_keys=True,
                ensure_ascii=False,
                indent=2,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    else:
        data = canonical_json_bytes(value)
    atomic_write_bytes(path, data)


def read_json(path: Path) -> Any:
    with path.open("rb") as f:
        return json.loads(f.read().decode("utf-8"))


class FileLock:
    """Advisory POSIX lock shared by CLI and HTTP workers."""

    def __init__(self, path: Path, *, shared: bool = False) -> None:
        self.path = path
        self.shared = shared
        self._file = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("a+", encoding="utf-8")
        fcntl.flock(
            self._file.fileno(),
            fcntl.LOCK_SH if self.shared else fcntl.LOCK_EX,
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        if self._file is not None:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
            self._file.close()
            self._file = None


def safe_component(value: str) -> str:
    # Shard ids are used in paths. Keep readable ASCII and append a short hash so
    # two ids that sanitize to the same text never alias each other.
    readable = "".join(c if c.isalnum() or c in "._-" else "_" for c in value)[:48]
    suffix = hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
    return f"{readable}-{suffix}"


def ensure_within(root: Path, candidate: Path) -> Path:
    resolved_root = root.resolve()
    resolved = candidate.resolve()
    if resolved != resolved_root and resolved_root not in resolved.parents:
        raise ValueError(f"path escapes root: {candidate}")
    return resolved
