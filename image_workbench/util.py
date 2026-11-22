"""Canonical serialization, hashing and atomic filesystem helpers."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import uuid
from pathlib import Path
from typing import Any


SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_json_text(value: Any) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def pretty_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    ensure_dir(path.parent)
    tmp = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    try:
        with tmp.open("wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        fsync_directory(path.parent)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def atomic_write_json(path: Path, value: Any) -> str:
    data = pretty_json_bytes(value)
    atomic_write_bytes(path, data)
    return sha256_bytes(data)


def atomic_copy_file(source: Path, target: Path) -> str:
    data = source.read_bytes()
    atomic_write_bytes(target, data)
    return sha256_bytes(data)


def read_json_file(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def validate_safe_name(value: str, kind: str = "name") -> str:
    if not SAFE_NAME_RE.fullmatch(value) or value in {".", ".."}:
        raise ValueError(f"unsafe {kind}: {value!r}")
    return value


def safe_name(value: str, kind: str = "name") -> str:
    return validate_safe_name(value, kind)


def reset_runtime_dirs(workspace: Path) -> None:
    for name in ("checkpoints", "artifacts", "reports", "metadata", "control", ".locks"):
        target = workspace / name
        if target.exists():
            shutil.rmtree(target)


def remove_file_if_exists(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return


def path_within(root: Path, candidate: Path) -> bool:
    try:
        candidate.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def relative_posix(root: Path, path: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def temp_path_near(path: Path, suffix: str = ".tmp") -> Path:
    ensure_dir(path.parent)
    return path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}{suffix}"
