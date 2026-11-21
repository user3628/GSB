from __future__ import annotations

import hashlib
import hmac
import json
import os
import tempfile
from pathlib import Path
from typing import Any


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_bytes(data: bytes) -> str:
    return "sha256:" + sha256_hex(data)


def digest_object(value: Any) -> str:
    return digest_bytes(canonical_json_bytes(value))


def derive_stage_seed(policy_seed: int, stage_name: str, input_digest: str) -> bytes:
    message = b"prep-pipeline/v1\0" + stage_name.encode("utf-8") + b"\0" + input_digest.encode("ascii")
    return hmac.new(str(policy_seed).encode("ascii"), message, hashlib.sha256).digest()


def hmac_rank(seed: bytes, namespace: str, material: str) -> str:
    message = namespace.encode("utf-8") + b"\0" + material.encode("utf-8")
    return hmac.new(seed, message, hashlib.sha256).hexdigest()


def stable_shard_id(shard_key: str, shard_count: int, salt: str) -> int:
    material = salt.encode("utf-8") + b"\0" + shard_key.encode("utf-8")
    value = int.from_bytes(hashlib.sha256(material).digest()[:8], "big")
    return value % shard_count


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
        try:
            directory_fd = os.open(path.parent, getattr(os, "O_DIRECTORY", 0))
        except OSError:
            directory_fd = None
        if directory_fd is not None:
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    except BaseException:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def atomic_write_json(path: Path, value: Any) -> bytes:
    data = canonical_json_bytes(value) + b"\n"
    atomic_write_bytes(path, data)
    return data
