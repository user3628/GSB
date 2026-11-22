"""Canonical serialization, hashing, and Unicode normalization helpers."""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Iterator


_WHITESPACE_RE = re.compile(r"\s+", re.UNICODE)


def normalize_instruction(text: str) -> str:
    """Apply NFKC, remove a BOM/zero-width markers, and collapse whitespace.

    NFKC normalizes full-width Latin letters/digits and compatibility forms to
    their ordinary half-width equivalents before tokenization.
    """

    if not isinstance(text, str):
        raise TypeError("instruction must be a string")
    normalized = unicodedata.normalize("NFKC", text)
    normalized = normalized.replace("\ufeff", "").replace("\u200b", "")
    return _WHITESPACE_RE.sub(" ", normalized).strip()


def canonical_json_text(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_json_bytes(value: Any) -> bytes:
    return canonical_json_text(value).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def sha256_json(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def iter_json_lines(path: str | Path) -> Iterator[Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                yield json.loads(line)
