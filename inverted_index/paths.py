from __future__ import annotations

import re
from collections.abc import Iterable

from .errors import bad_path


# Deliberately conservative: paths are dot-separated identifiers. Arrays are
# expanded automatically while traversing a document, so no bracket syntax is
# accepted.
_SEGMENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]*$")


def split_field_path(path: str) -> tuple[str, ...]:
    if not isinstance(path, str) or not path:
        raise bad_path(str(path), "path must be a non-empty string")
    if path.startswith(".") or path.endswith(".") or ".." in path:
        raise bad_path(path, "empty path segment")
    if any(ch.isspace() for ch in path):
        raise bad_path(path, "whitespace is not allowed")
    parts = tuple(path.split("."))
    for part in parts:
        if not _SEGMENT_RE.fullmatch(part):
            raise bad_path(path, f"invalid path segment {part!r}")
    return parts


def validate_field_paths(paths: Iterable[str]) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for path in paths:
        split_field_path(path)
        if path not in seen:
            seen.add(path)
            result.append(path)
    return tuple(result)
