from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_text(value: Any) -> str:
    return canonical_bytes(value).decode("utf-8")


def digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def digest_obj(value: Any) -> str:
    return digest_bytes(canonical_bytes(value))


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _fsync_dir(path: Path) -> None:
    if os.name == "nt":
        return
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.tmp-", dir=path.parent)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        _fsync_dir(path.parent)
    except BaseException:
        try:
            tmp_path.unlink(missing_ok=True)
        finally:
            raise


def atomic_write_json(path: Path, value: Any) -> None:
    atomic_write_bytes(path, canonical_bytes(value) + b"\n")


def atomic_write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    payload = b"".join(canonical_bytes(row) + b"\n" for row in rows)
    atomic_write_bytes(path, payload)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at line {line_no}: {exc}") from exc
            if not isinstance(row, dict):
                raise ValueError(f"JSONL line {line_no} is not an object")
            rows.append(row)
    return rows


def ensure_path_within(base: Path, candidate: Path) -> Path:
    base = base.resolve()
    candidate = candidate.resolve()
    if base not in candidate.parents and candidate != base:
        raise ValueError(f"path escapes workspace: {candidate}")
    return candidate
