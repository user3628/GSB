"""Canonical serialization, digests, JSONL I/O, and atomic file writes."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Iterable, Iterator

from .errors import BusinessError, E_INVALID_INPUT, E_INVALID_RECORD


def canonical_json_bytes(value: Any) -> bytes:
    text = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return (text + "\n").encode("utf-8")


def pretty_json_bytes(value: Any) -> bytes:
    text = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
        allow_nan=False,
    )
    return (text + "\n").encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_json(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def digest_text(value: str) -> str:
    return sha256_bytes(value.encode("utf-8"))


def atomic_write_bytes(path: str | Path, data: bytes, *, fsync: bool = True) -> None:
    """Write a complete file or leave the previous file untouched."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{target.name}.tmp-",
        dir=str(target.parent),
    )
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            if fsync:
                os.fsync(handle.fileno())
        os.replace(tmp_name, target)
        if fsync and os.name == "posix":
            try:
                dir_fd = os.open(str(target.parent), os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError:
                pass
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def atomic_write_json(path: str | Path, value: Any, *, pretty: bool = False) -> bytes:
    data = pretty_json_bytes(value) if pretty else canonical_json_bytes(value)
    atomic_write_bytes(path, data)
    return data


def read_json(path: str | Path, *, default: Any = None) -> Any:
    target = Path(path)
    if not target.exists():
        return default
    try:
        with target.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise BusinessError(
            E_INVALID_INPUT,
            f"cannot read JSON file: {target}",
            {"path": str(target), "cause": str(exc)},
        ) from exc


def read_bytes(path: str | Path) -> bytes:
    target = Path(path)
    try:
        return target.read_bytes()
    except OSError as exc:
        raise BusinessError(
            E_INVALID_INPUT,
            f"cannot read input file: {target}",
            {"path": str(target), "cause": str(exc)},
        ) from exc


def read_jsonl(path: str | Path) -> Iterator[tuple[int, dict[str, Any]]]:
    target = Path(path)
    try:
        raw = target.read_bytes()
    except OSError as exc:
        raise BusinessError(
            E_INVALID_INPUT,
            f"cannot read JSONL shard: {target}",
            {"path": str(target), "cause": str(exc)},
        ) from exc
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise BusinessError(
            E_INVALID_RECORD,
            f"shard is not valid UTF-8: {target}",
            {"path": str(target), "offset": exc.start},
        ) from exc
    for line_no, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            raise BusinessError(
                E_INVALID_RECORD,
                f"invalid JSON at line {line_no}",
                {"path": str(target), "line": line_no, "cause": exc.msg},
            ) from exc
        if not isinstance(item, dict):
            raise BusinessError(
                E_INVALID_RECORD,
                f"JSONL record at line {line_no} must be an object",
                {"path": str(target), "line": line_no},
            )
        yield line_no, item


def write_jsonl(path: str | Path, rows: Iterable[dict[str, Any]]) -> bytes:
    chunks = [canonical_json_bytes(row) for row in rows]
    data = b"".join(chunks)
    atomic_write_bytes(path, data)
    return data


def shard_id_for(path: str | Path) -> str:
    return Path(path).name


def canonical_shards(paths: Iterable[str | Path]) -> list[Path]:
    resolved: list[Path] = []
    names: set[str] = set()
    for raw in paths:
        path = Path(raw).expanduser().resolve()
        if not path.is_file():
            raise BusinessError(
                E_INVALID_INPUT,
                f"shard does not exist: {raw}",
                {"path": str(raw)},
            )
        name = path.name
        if name in names:
            raise BusinessError(
                E_INVALID_INPUT,
                "shard basenames must be unique",
                {"duplicate": name},
            )
        names.add(name)
        resolved.append(path)
    resolved.sort(key=lambda p: p.name)
    if not resolved:
        raise BusinessError(E_INVALID_INPUT, "at least one shard is required")
    return resolved

