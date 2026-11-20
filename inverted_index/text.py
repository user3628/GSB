from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

from .errors import BusinessError
from .paths import split_field_path


# Every run of Unicode letters/numbers is one token. This is the fixed
# separator rule: punctuation, underscores, hyphens, dots and whitespace all
# split tokens. NFKC + casefold makes normalization stable across platforms.
_TOKEN_RE = re.compile(r"[^\W_]+", re.UNICODE)


def normalize_text(value: str) -> str:
    return unicodedata.normalize("NFKC", value).casefold()


def tokenize(value: str) -> list[str]:
    return _TOKEN_RE.findall(normalize_text(value))


def normalize_term(value: str) -> str:
    if not isinstance(value, str):
        raise BusinessError("E_BAD_TERM", "A term must be a string")
    tokens = tokenize(value)
    if len(tokens) != 1:
        raise BusinessError(
            "E_BAD_TERM",
            "A term must normalize to exactly one token",
            details={"term": value, "normalized_tokens": tokens},
        )
    return tokens[0]


@dataclass(frozen=True)
class PositionedPosting:
    field_path: str
    term: str
    positions: tuple[int, ...]


@dataclass
class _PositionStream:
    cursor: int = 0
    occurrences: dict[tuple[str, str], list[int]] = field(default_factory=dict)

    def add_tokens(self, field_path: str, value: str) -> None:
        for token in tokenize(value):
            self.occurrences.setdefault((field_path, token), []).append(self.cursor)
            self.cursor += 1

    def boundary(self) -> None:
        # A gap prevents phrases from matching across two array elements while
        # preserving globally ascending, unique integer positions.
        self.cursor += 1


def _scalar_text(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return None


def _walk(value: Any, parts: tuple[str, ...], depth: int, stream: _PositionStream) -> None:
    if depth == len(parts):
        if isinstance(value, list):
            for element in value:
                _walk(element, parts, depth, stream)
                stream.boundary()
            return
        if isinstance(value, dict):
            for key in sorted(value):
                _walk(value[key], parts, depth, stream)
                stream.boundary()
            return
        text = _scalar_text(value)
        if text is not None:
            stream.add_tokens(".".join(parts), text)
        return

    part = parts[depth]
    if isinstance(value, list):
        for element in value:
            _walk(element, parts, depth, stream)
            stream.boundary()
        return
    if isinstance(value, dict) and part in value:
        _walk(value[part], parts, depth + 1, stream)


def extract_postings(document: dict[str, Any], field_paths: tuple[str, ...]) -> dict[tuple[str, str], tuple[int, ...]]:
    """Extract ``(field path, term) -> positions`` for one document.

    Nested arrays are traversed by array index. A one-position gap is inserted
    after each array element (at every nesting level), so adjacency is stable
    and phrases do not cross element boundaries.
    """

    occurrences: dict[tuple[str, str], list[int]] = {}
    for path in field_paths:
        # Positions are local to a field path so the same value in two fields
        # cannot accidentally share a phrase coordinate space.
        stream = _PositionStream()
        parts = split_field_path(path)
        _walk(document, parts, 0, stream)
        occurrences.update(stream.occurrences)
    return {key: tuple(positions) for key, positions in occurrences.items()}


def iter_path_values(value: Any, path: str) -> Iterator[Any]:
    """Yield scalar leaves at a path in deterministic document order."""

    parts = split_field_path(path)

    def walk(node: Any, depth: int) -> Iterator[Any]:
        if depth == len(parts):
            if isinstance(node, list):
                for element in node:
                    yield from walk(element, depth)
            elif isinstance(node, dict):
                for key in sorted(node):
                    yield from walk(node[key], depth)
            else:
                yield node
            return
        part = parts[depth]
        if isinstance(node, list):
            for element in node:
                yield from walk(element, depth)
        elif isinstance(node, dict) and part in node:
            yield from walk(node[part], depth + 1)

    yield from walk(value, 0)
