"""Deterministic helpers: canonical JSON, hashing, atomic file writes."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path


def canonical_json(obj) -> str:
    """Canonical JSON used for fingerprints and on-disk records (byte stable)."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def pretty_json(obj) -> str:
    return json.dumps(obj, sort_keys=True, indent=2, ensure_ascii=True) + "\n"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def sha256_file(path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write_bytes(path, data: bytes) -> None:
    """Write a file atomically: temp file in the same directory + os.replace.

    A crash at any point leaves either the old file or the new file, never a
    half-written record. Leftover temp files use the ``.tmp-`` suffix and are
    ignored by readers.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp-{os.getpid()}")
    try:
        with open(tmp, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass
    _fsync_dir(path.parent)


def atomic_write_text(path, text: str) -> None:
    atomic_write_bytes(path, text.encode("utf-8"))


def _fsync_dir(dir_path) -> None:
    if os.name == "nt":
        return
    try:
        fd = os.open(str(dir_path), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def round6(value: float) -> float:
    """Deterministic rounding for summary ratios."""
    return round(float(value) + 0.0, 6)
