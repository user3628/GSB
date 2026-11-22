"""A deliberately small YAML subset parser used when PyYAML is unavailable.

It supports the governance configuration shape used by this project: nested
mappings/lists, quoted scalars, comments, inline lists/maps, and JSON scalars.
It is not intended to be a general YAML implementation.
"""

from __future__ import annotations

import json
from typing import Any

from .errors import BusinessError, E_INVALID_CONFIG


def _strip_comment(line: str) -> str:
    quote: str | None = None
    escaped = False
    for idx, char in enumerate(line):
        if escaped:
            escaped = False
            continue
        if char == "\\" and quote == '"':
            escaped = True
            continue
        if quote:
            if char == quote:
                quote = None
            continue
        if char in ("'", '"'):
            quote = char
        elif char == "#" and (idx == 0 or line[idx - 1].isspace()):
            return line[:idx]
    return line


def _split_top_level(text: str, delimiter: str = ",") -> list[str]:
    parts: list[str] = []
    start = 0
    depth = 0
    quote: str | None = None
    escaped = False
    for idx, char in enumerate(text):
        if escaped:
            escaped = False
            continue
        if char == "\\" and quote == '"':
            escaped = True
            continue
        if quote:
            if char == quote:
                quote = None
            continue
        if char in ("'", '"'):
            quote = char
        elif char in "[{(":
            depth += 1
        elif char in "]})":
            depth -= 1
        elif char == delimiter and depth == 0:
            parts.append(text[start:idx])
            start = idx + 1
    parts.append(text[start:])
    return parts


def _split_key_value(text: str) -> tuple[str, str]:
    quote: str | None = None
    escaped = False
    depth = 0
    for idx, char in enumerate(text):
        if escaped:
            escaped = False
            continue
        if char == "\\" and quote == '"':
            escaped = True
            continue
        if quote:
            if char == quote:
                quote = None
            continue
        if char in ("'", '"'):
            quote = char
        elif char in "[{(":
            depth += 1
        elif char in "]})":
            depth -= 1
        elif char == ":" and depth == 0:
            return text[:idx], text[idx + 1 :]
    raise BusinessError(E_INVALID_CONFIG, f"expected YAML mapping entry: {text}")


def _unquote(text: str) -> str:
    text = text.strip()
    if len(text) >= 2 and text[0] == text[-1] == '"':
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise BusinessError(E_INVALID_CONFIG, f"invalid quoted scalar: {text}") from exc
    if len(text) >= 2 and text[0] == text[-1] == "'":
        return text[1:-1].replace("''", "'")
    return text


def _parse_scalar(text: str) -> Any:
    text = text.strip()
    if not text:
        return None
    if text.startswith("[") and text.endswith("]"):
        inner = text[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(part) for part in _split_top_level(inner)]
    if text.startswith("{") and text.endswith("}"):
        inner = text[1:-1].strip()
        result: dict[str, Any] = {}
        if not inner:
            return result
        for part in _split_top_level(inner):
            key, value = _split_key_value(part)
            result[str(_parse_scalar(key))] = _parse_scalar(value)
        return result
    if text[0] in ("'", '"'):
        return _unquote(text)
    lowered = text.casefold()
    if lowered in {"null", "~"}:
        return None
    if lowered in {"true", "yes"}:
        return True
    if lowered in {"false", "no"}:
        return False
    try:
        if all(ch not in text for ch in ".eE"):
            return int(text, 10)
        return float(text)
    except ValueError:
        return text


def _logical_lines(text: str) -> list[tuple[int, str]]:
    lines: list[tuple[int, str]] = []
    for original in text.splitlines():
        if "\t" in original[: len(original) - len(original.lstrip())]:
            raise BusinessError(E_INVALID_CONFIG, "tab indentation is not supported")
        clean = _strip_comment(original).rstrip()
        if not clean.strip() or clean.lstrip().startswith("---"):
            continue
        indent = len(clean) - len(clean.lstrip(" "))
        lines.append((indent, clean[indent:]))
    return lines


class _Parser:
    def __init__(self, lines: list[tuple[int, str]]) -> None:
        self.lines = lines
        self.pos = 0

    def _peek(self) -> tuple[int, str] | None:
        return self.lines[self.pos] if self.pos < len(self.lines) else None

    def parse(self) -> Any:
        if not self.lines:
            return {}
        value = self._block(self.lines[0][0])
        if self.pos != len(self.lines):
            raise BusinessError(E_INVALID_CONFIG, "unexpected trailing YAML content")
        return value

    def _block(self, indent: int) -> Any:
        peek = self._peek()
        if peek is None or peek[0] < indent:
            return None
        if peek[1].startswith("- "):
            return self._list(indent)
        return self._mapping(indent)

    def _mapping(self, indent: int) -> dict[str, Any]:
        result: dict[str, Any] = {}
        while True:
            peek = self._peek()
            if peek is None or peek[0] < indent:
                break
            level, content = peek
            if level != indent or content.startswith("- "):
                break
            key_text, value_text = _split_key_value(content)
            key = str(_parse_scalar(key_text))
            self.pos += 1
            if value_text.strip():
                result[key] = _parse_scalar(value_text)
            else:
                next_peek = self._peek()
                if next_peek is not None and next_peek[0] > indent:
                    result[key] = self._block(next_peek[0])
                else:
                    result[key] = None
        return result

    def _list(self, indent: int) -> list[Any]:
        result: list[Any] = []
        while True:
            peek = self._peek()
            if peek is None or peek[0] < indent:
                break
            level, content = peek
            if level != indent or not content.startswith("- "):
                break
            item_text = content[2:].strip()
            self.pos += 1
            if not item_text:
                next_peek = self._peek()
                result.append(
                    self._block(next_peek[0])
                    if next_peek is not None and next_peek[0] > indent
                    else None
                )
                continue
            try:
                key_text, value_text = _split_key_value(item_text)
            except BusinessError:
                result.append(_parse_scalar(item_text))
                continue
            key = str(_parse_scalar(key_text))
            item: dict[str, Any] = {}
            if value_text.strip():
                item[key] = _parse_scalar(value_text)
            else:
                next_peek = self._peek()
                item[key] = (
                    self._block(next_peek[0])
                    if next_peek is not None and next_peek[0] > indent
                    else None
                )
            next_peek = self._peek()
            if next_peek is not None and next_peek[0] > indent:
                extra = self._block(next_peek[0])
                if isinstance(extra, dict):
                    item.update(extra)
            result.append(item)
        return result


def safe_load(text: str) -> Any:
    stripped = text.strip()
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            return json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise BusinessError(E_INVALID_CONFIG, "invalid JSON document") from exc
    return _Parser(_logical_lines(text)).parse()

