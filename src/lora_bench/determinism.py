"""Determinism helpers: fixed-seed RNGs, fingerprints, atomic writes and locks.

The module deliberately exposes pure functions so worker processes can import
them under ``spawn`` (Windows) as well as ``fork`` (Linux/macOS) start methods.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from contextlib import contextmanager
from typing import Any, Iterator

import numpy as np
import torch


def set_global_seed(seed: int) -> None:
    """Pin every RNG we rely on to ``seed``."""
    os.environ["PYTHONHASHSEED"] = str(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():  # pragma: no cover - CPU only workbench
        torch.cuda.manual_seed_all(seed)


def torch_rng(seed: int) -> torch.Generator:
    gen = torch.Generator(device="cpu")
    gen.manual_seed(seed)
    return gen


def stable_json(obj: Any) -> str:
    """Canonical JSON used for fingerprints and byte-identical summaries."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_file(path: str, chunk: int = 1 << 16) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(chunk), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_write(path: str, data: bytes) -> None:
    """Write ``data`` to ``path`` atomically.

    The file is first materialised in the same directory (``os.rename`` is
    atomic within one filesystem) and fsynced before the rename, so a crash at
    any point leaves either the previous file or the complete new file.
    """
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp-", dir=directory)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        dir_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except BaseException:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def atomic_write_json(path: str, obj: Any) -> None:
    atomic_write(path, (stable_json(obj) + "\n").encode("utf-8"))


class FileLock:
    """Cross-platform advisory file lock (``fcntl`` on POSIX, ``msvcrt`` on Windows)."""

    def __init__(self, lock_path: str) -> None:
        self.lock_path = lock_path
        os.makedirs(os.path.dirname(os.path.abspath(lock_path)), exist_ok=True)
        self._handle = open(lock_path, "a+")
        self._locked = False

    def acquire(self) -> None:
        if os.name == "nt":  # pragma: no cover - exercised on Windows
            import msvcrt

            msvcrt.locking(self._handle.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl

            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX)
        self._locked = True

    def release(self) -> None:
        if not self._locked:
            return
        if os.name == "nt":  # pragma: no cover
            import msvcrt

            self._handle.seek(0)
            msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        self._locked = False

    def close(self) -> None:
        try:
            self.release()
        finally:
            self._handle.close()

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


@contextmanager
def file_lock(lock_path: str) -> Iterator[None]:
    lock = FileLock(lock_path)
    try:
        yield
    finally:
        lock.close()


def tensor_digest(tensor: torch.Tensor) -> str:
    """Deterministic sha256 over a tensor's raw float bytes."""
    contiguous = tensor.detach().cpu().contiguous()
    return hashlib.sha256(contiguous.numpy().tobytes()).hexdigest()
