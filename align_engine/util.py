"""Deterministic serialization, hashing, atomic writes and file locks."""

import contextlib
import hashlib
import json
import os
import tempfile

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX fallback
    fcntl = None


def canonical_json_bytes(obj):
    """Deterministic JSON serialization (sorted keys, tight separators)."""
    text = json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return (text + "\n").encode("utf-8")


def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fingerprint_obj(*parts):
    """Order-sensitive fingerprint over byte/str parts."""
    digest = hashlib.sha256()
    for part in parts:
        if isinstance(part, str):
            part = part.encode("utf-8")
        digest.update(len(part).to_bytes(8, "big"))
        digest.update(part)
    return digest.hexdigest()


def atomic_write(path, data):
    """Write bytes atomically: temp file in same dir + fsync + os.replace.

    An interruption at any point never leaves a half-written record at
    ``path``; at most an orphaned ``.tmp-*`` file remains.
    """
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=".part")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


@contextlib.contextmanager
def file_lock(path):
    """Exclusive advisory file lock (POSIX flock; no-op fallback)."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    handle = open(path, "a+b")
    try:
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield handle
    finally:
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()
