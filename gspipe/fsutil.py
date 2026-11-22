"""Filesystem primitives shared by checkpoints, caches, and exports."""

from __future__ import annotations

import json
import os
import secrets
import shutil
import time
from pathlib import Path
from typing import Any

from .canonical import canonical_json_bytes
from .errors import E_IO, PipelineError


def ensure_dir(path: str | Path) -> Path:
    target = Path(path)
    target.mkdir(parents=True, exist_ok=True)
    return target


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _replace_with_retry(source: Path, target: Path) -> None:
    delay = 0.01
    last_error: OSError | None = None
    for _ in range(20):
        try:
            os.replace(str(source), str(target))
            return
        except PermissionError as exc:
            last_error = exc
            time.sleep(delay)
            delay = min(delay * 2, 0.25)
    if last_error is not None:
        raise last_error


def atomic_write_bytes(path: str | Path, data: bytes) -> Path:
    """Write a complete file and atomically replace ``path``.

    A unique same-directory temporary file is fsynced before replacement.  If
    the process or machine stops at any point, readers observe either the old
    complete file or the new complete file, never a partially written target.
    """

    target = Path(path)
    ensure_dir(target.parent)
    token = f"{os.getpid()}.{secrets.token_hex(8)}"
    temporary = target.with_name(f".{target.name}.{token}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        _replace_with_retry(temporary, target)
        _fsync_directory(target.parent)
        return target
    except OSError as exc:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise PipelineError(
            E_IO,
            f"Failed to atomically write {target}: {exc}",
            details={"path": str(target)},
        ) from exc


def atomic_write_json(path: str | Path, value: Any) -> Path:
    return atomic_write_bytes(path, canonical_json_bytes(value) + b"\n")


def read_json(path: str | Path) -> Any:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def remove_path(path: str | Path) -> None:
    target = Path(path)
    if target.is_dir():
        shutil.rmtree(target, ignore_errors=True)
    else:
        target.unlink(missing_ok=True)


def safe_relative_path(root: Path, relative: str) -> Path:
    candidate = (root / relative).resolve()
    resolved_root = root.resolve()
    try:
        candidate.relative_to(resolved_root)
    except ValueError as exc:
        raise PipelineError(
            E_IO,
            "Artifact path escapes the workspace",
            details={"path": relative},
        ) from exc
    return candidate
