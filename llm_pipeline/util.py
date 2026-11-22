"""Small deterministic I/O, hashing, and locking utilities."""

from __future__ import annotations

import contextlib
import copy
import errno
import fcntl
import json
import os
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Iterator

from .errors import E_LOCK_TIMEOUT, PipelineError


def canonical_json_bytes(value: Any) -> bytes:
    """Canonical compact JSON, UTF-8 encoded, without a trailing newline."""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_json_line(value: Any) -> bytes:
    return canonical_json_bytes(value) + b"\n"


def jsonl_bytes(rows: list[Any]) -> bytes:
    return b"".join(canonical_json_line(row) for row in rows)


def sha256_bytes(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def sha256_obj(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        try:
            os.fsync(fd)
        except OSError:
            # Some local filesystems do not support fsync on directories.
            pass
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path, data: bytes, *, mode: int = 0o644) -> None:
    """Write a file atomically and durably; no final partial file is visible."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.tmp-", dir=str(path.parent)
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp_path, mode)
        os.replace(tmp_path, path)
        fsync_dir(path.parent)
    finally:
        if tmp_path.exists():
            with contextlib.suppress(OSError):
                tmp_path.unlink()


def atomic_write_json(path: Path, value: Any) -> None:
    atomic_write_bytes(path, canonical_json_line(value))


def read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default


@contextlib.contextmanager
def file_lock(path: Path, *, timeout: float = 30.0) -> Iterator[None]:
    """Exclusive advisory flock held for the duration of the context.

    ``flock`` is a real OS file lock, so separate Python processes contend for
    the same lock even when they do not share memory.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    deadline = time.monotonic() + max(0.0, timeout)
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise PipelineError(
                        E_LOCK_TIMEOUT,
                        f"timed out waiting for file lock: {path}",
                        {"lock": str(path)},
                        http_status=503,
                    )
                time.sleep(0.01)
        os.ftruncate(fd, 0)
        os.write(fd, f"pid={os.getpid()}\n".encode("ascii"))
        os.fsync(fd)
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def deep_merge(base: Any, patch: Any) -> Any:
    """Return a deep-merged copy. Dicts merge recursively, other values replace."""
    if isinstance(base, dict) and isinstance(patch, dict):
        result = copy.deepcopy(base)
        for key, value in patch.items():
            if key in result:
                result[key] = deep_merge(result[key], value)
            else:
                result[key] = copy.deepcopy(value)
        return result
    return copy.deepcopy(patch)


def set_dotted(target: dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = dotted_key.split(".")
    if not parts or any(not part for part in parts):
        raise ValueError(f"invalid dotted key: {dotted_key!r}")
    cursor = target
    for part in parts[:-1]:
        child = cursor.get(part)
        if child is None:
            child = {}
            cursor[part] = child
        if not isinstance(child, dict):
            raise ValueError(f"cannot descend into non-object field: {part!r}")
        cursor = child
    cursor[parts[-1]] = value


def unique_quarantine_path(directory: Path, original: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    token = uuid.uuid4().hex
    return directory / f"{time.time_ns()}-{token}-{original.name}"


def move_to_quarantine(path: Path, quarantine_dir: Path) -> Path:
    target = unique_quarantine_path(quarantine_dir, path)
    try:
        os.replace(path, target)
    except FileNotFoundError:
        pass
    return target


def ensure_no_path_traversal(name: str) -> str:
    candidate = Path(name)
    if (
        not name
        or candidate.is_absolute()
        or candidate.name != name
        or name in {".", ".."}
        or "/" in name
        or "\\" in name
    ):
        raise ValueError(f"invalid file name: {name!r}")
    return name


def first_difference(
    left_rows: list[Any],
    right_rows: list[Any],
    *,
    product: str,
) -> dict[str, Any] | None:
    limit = min(len(left_rows), len(right_rows))
    for index in range(limit):
        if left_rows[index] != right_rows[index]:
            left = left_rows[index]
            right = right_rows[index]
            identifier = None
            kind = "row"
            if isinstance(left, dict):
                identifier = left.get("id") or left.get("layer_id")
            if isinstance(right, dict) and identifier is None:
                identifier = right.get("id") or right.get("layer_id")
            if isinstance(left, dict):
                kind = "layer" if "layer_id" in left else "sample"
            return {
                "product": product,
                "index": index,
                "kind": kind,
                "id": identifier,
                "incremental": left,
                "full": right,
            }
    if len(left_rows) != len(right_rows):
        extra = left_rows[limit] if len(left_rows) > limit else right_rows[limit]
        identifier = extra.get("id") or extra.get("layer_id") if isinstance(extra, dict) else None
        return {
            "product": product,
            "index": limit,
            "kind": "length",
            "id": identifier,
            "incremental_length": len(left_rows),
            "full_length": len(right_rows),
        }
    return None


def errno_name(exc: OSError) -> str:
    return errno.errorcode.get(exc.errno or 0, "EIO")
