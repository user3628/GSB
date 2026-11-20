"""Shared deterministic serialization, hashing and atomic filesystem helpers."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import tempfile
import unicodedata
from pathlib import Path
from typing import Any, Iterator


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_prompt(prompt: str) -> str:
    normalized = unicodedata.normalize("NFKC", prompt)
    return " ".join(normalized.strip().split()).casefold()


def stable_hash(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def atomic_write_bytes(path: Path, data: bytes, *, mode: int = 0o644) -> None:
    """Write a file atomically in its destination directory.

    Incomplete ``.tmp-*`` siblings are intentionally never considered valid by
    readers, so process death at any write position leaves the previous file
    untouched and recovery can ignore the temporary file.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=".tmp-", dir=str(path.parent))
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temp_path, mode)
        os.replace(temp_path, path)
        _fsync_directory(path.parent)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temp_path.unlink()


def atomic_write_json(path: Path, value: Any) -> None:
    atomic_write_bytes(path, canonical_json_bytes(value) + b"\n")


def read_json(path: Path) -> Any:
    with path.open("rb") as handle:
        return json.load(handle)


def _fsync_directory(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@contextlib.contextmanager
def exclusive_file_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def tree_manifest(root: Path, *, include_suffixes: tuple[str, ...] | None = None) -> dict[str, str]:
    """Return a stable relative-path to SHA-256 manifest without modifying root."""

    result: dict[str, str] = {}
    if not root.exists():
        return result
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        if path.name.startswith(".tmp-"):
            continue
        if include_suffixes is not None and path.suffix not in include_suffixes:
            continue
        result[path.relative_to(root).as_posix()] = sha256_file(path)
    return result

