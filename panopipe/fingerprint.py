"""Deterministic hashing helpers."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def digest_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def digest_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def digest_files(root: Path, relative_paths: Iterable[str]) -> tuple[str, list[dict[str, str]]]:
    """Hash a sorted list of files and return an artifact manifest."""

    entries: list[dict[str, str]] = []
    h = hashlib.sha256()
    for rel in sorted(relative_paths):
        path = root / rel
        if not path.is_file():
            raise FileNotFoundError(path)
        file_digest = digest_file(path)
        entries.append({"path": rel, "sha256": file_digest})
        h.update(rel.encode("utf-8"))
        h.update(b"\0")
        h.update(file_digest.encode("ascii"))
        h.update(b"\n")
    return h.hexdigest(), entries
