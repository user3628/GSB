"""Small deterministic I/O, hashing, and serialization helpers."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Iterable, Sequence


def canonical_json_bytes(value: Any) -> bytes:
    """Return the one canonical JSON representation used by every artifact.

    Python's JSON encoder emits deterministic key order when ``sort_keys`` is
    enabled.  Float formatting is also stable for the values used here.  A
    trailing newline makes text artifacts pleasant to inspect and hash.
    """

    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=True,
        ).encode("utf-8")
        + b"\n"
    )


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write and atomically replace ``path`` in its own directory."""

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        _fsync_directory(path.parent)
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def atomic_write_json(path: Path, value: Any) -> None:
    atomic_write_bytes(path, canonical_json_bytes(value))


def _fsync_directory(directory: Path) -> None:
    """Best-effort directory fsync (not supported on Windows)."""

    try:
        descriptor = os.open(str(directory), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def load_json(path: Path, default: Any = None) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


def digest_files(base: Path, paths: Iterable[Path]) -> str:
    """Hash a set of files, including sorted relative names and lengths."""

    resolved = sorted((path.resolve() for path in paths), key=lambda item: str(item))
    digest = hashlib.sha256()
    for path in resolved:
        relative = path.relative_to(base.resolve()).as_posix().encode("utf-8")
        digest.update(b"file\0")
        digest.update(relative)
        digest.update(b"\0")
        digest.update(str(path.stat().st_size).encode("ascii"))
        digest.update(b"\0")
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def digest_named_files(paths: Iterable[Path]) -> str:
    """Hash files by basename and content, independent of their parent path."""

    resolved = sorted((path.resolve() for path in paths), key=lambda item: item.name)
    digest = hashlib.sha256()
    for path in resolved:
        digest.update(b"named-file\0")
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(path.stat().st_size).encode("ascii"))
        digest.update(b"\0")
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def digest_file_list(files: Sequence[tuple[str, int, bytes]]) -> str:
    """Hash ``(name, size, content)`` triples in the supplied order."""

    digest = hashlib.sha256()
    for name, size, content in files:
        digest.update(b"record\0")
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(size).encode("ascii"))
        digest.update(b"\0")
        digest.update(content)
    return digest.hexdigest()


def get_nested(mapping: Any, dotted: str, default: Any = None) -> Any:
    current = mapping
    for part in dotted.split("."):
        if not isinstance(current, dict) or part not in current:
            return default
        current = current[part]
    return current


def flatten_json(value: Any, prefix: str = "") -> dict[str, Any]:
    result: dict[str, Any] = {}
    if isinstance(value, dict):
        for key in sorted(value):
            child = f"{prefix}.{key}" if prefix else str(key)
            result.update(flatten_json(value[key], child))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            child = f"{prefix}[{index}]" if prefix else f"[{index}]"
            result.update(flatten_json(item, child))
    else:
        result[prefix] = value
    return result


def changed_paths(old: Any, new: Any) -> list[str]:
    """Return sorted leaf paths whose scalar/list values differ."""

    old_flat = flatten_json(old, "")
    new_flat = flatten_json(new, "")
    return sorted(
        path
        for path in set(old_flat) | set(new_flat)
        if old_flat.get(path, object()) != new_flat.get(path, object())
    )
