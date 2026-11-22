from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Iterable


def canonical_bytes(value: Any) -> bytes:
    """Stable UTF-8 JSON encoding used by artifacts and fingerprints."""
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        + b"\n"
    )


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_obj(value: Any) -> str:
    return sha256_bytes(canonical_bytes(value))


def ensure_dir(path: str | Path) -> Path:
    target = Path(path)
    target.mkdir(parents=True, exist_ok=True)
    return target


def atomic_write_bytes(path: str | Path, data: bytes) -> None:
    """Write a complete file and atomically replace the destination."""
    target = Path(path)
    ensure_dir(target.parent)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{target.name}.tmp-", dir=str(target.parent)
    )
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, target)
        try:
            directory_fd = os.open(str(target.parent), os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            pass
    except BaseException:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def atomic_write_json(path: str | Path, value: Any) -> None:
    atomic_write_bytes(path, canonical_bytes(value))


def read_json(path: str | Path) -> Any:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                value = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at line {line_number}: {exc}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"JSONL line {line_number} is not an object")
            records.append(value)
    return records


def write_jsonl(path: str | Path, records: Iterable[dict[str, Any]]) -> None:
    lines = [canonical_bytes(record).decode("utf-8") for record in records]
    atomic_write_bytes(path, "".join(lines).encode("utf-8"))


def copy_tree_files(source: str | Path, destination: str | Path) -> None:
    """Copy regular files only; verification uses this for isolated workspaces."""
    import shutil

    source_path = Path(source)
    destination_path = Path(destination)
    ensure_dir(destination_path)
    for item in sorted(source_path.rglob("*")):
        if not item.is_file():
            continue
        relative = item.relative_to(source_path)
        target = destination_path / relative
        ensure_dir(target.parent)
        shutil.copy2(item, target)
