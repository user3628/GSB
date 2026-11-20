"""Deterministic serialization, hashing, atomic writes and file locks."""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from contextlib import contextmanager


def canonical_dumps(obj) -> bytes:
    """Byte-stable JSON: sorted keys, no whitespace surprises, UTF-8, LF newline."""
    text = json.dumps(
        obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    return (text + "\n").encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).digest().hex()


def digest_obj(obj) -> str:
    return sha256_hex(canonical_dumps(obj))


def digest_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def atomic_write(path: str, payload: bytes) -> None:
    """Write via temp file + fsync + os.replace; no half-written target ever."""
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp-", dir=directory)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
        try:
            dir_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


@contextmanager
def file_lock(lock_path: str):
    """Advisory exclusive lock shared across processes on the same workdir."""
    os.makedirs(os.path.dirname(os.path.abspath(lock_path)) or ".", exist_ok=True)
    fh = open(lock_path, "a+b")
    try:
        import fcntl

        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        import fcntl

        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        fh.close()


def read_json(path: str):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def write_json(path: str, obj) -> str:
    payload = canonical_dumps(obj)
    atomic_write(path, payload)
    return sha256_hex(payload)
