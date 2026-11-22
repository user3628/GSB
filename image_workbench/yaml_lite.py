"""Small, dependency-free YAML subset parser for local scheduler configs.

Supports nested mappings/lists, comments, quoted strings and JSON-style flow
collections. It intentionally does not implement anchors, tags or multi-document
streams.
"""

from __future__ import annotations

import ast
import json
import os
from typing import Any


class YamlLiteError(ValueError):
    pass


def _strip_comment(line: str) -> str:
    quote: str | None = None
    escaped = False
    for index, char in enumerate(line):
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
        if char in {"'", '"'}:
            quote = char
            continue
        if char == "#":
            return line[:index]
    return line


def _split_key_value(text: str) -> tuple[str, str]:
    quote: str | None = None
    escaped = False
    for index, char in enumerate(text):
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
        if char in {"'", '"'}:
            quote = char
            continue
        if char == ":":
            return text[:index].strip(), text[index + 1 :].strip()
    raise YamlLiteError(f"expected mapping entry: {text!r}")


def _unquote(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] == '"':
        return json.loads(value)
    if len(value) >= 2 and value[0] == value[-1] == "'":
        return value[1:-1].replace("''", "'")
    return value


def _scalar(value: str) -> Any:
    value = value.strip()
    if value == "":
        return None
    if value in {"null", "Null", "NULL", "~"}:
        return None
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    if (value.startswith("[") and value.endswith("]")) or (
        value.startswith("{") and value.endswith("}")
    ):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            try:
                return ast.literal_eval(value)
            except (ValueError, SyntaxError):
                pass
    if value[0] in {"'", '"'}:
        return _unquote(value)
    try:
        return int(value, 10)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


class _Parser:
    def __init__(self, lines: list[tuple[int, str, int]]):
        self.lines = lines

    def parse(self) -> Any:
        if not self.lines:
            return {}
        indent = self.lines[0][0]
        value, index = self._block(0, indent)
        if index != len(self.lines):
            _, text, line_no = self.lines[index]
            raise YamlLiteError(f"unexpected content at line {line_no}: {text!r}")
        return value

    def _block(self, index: int, indent: int) -> tuple[Any, int]:
        if index >= len(self.lines):
            return None, index
        if self.lines[index][0] != indent:
            raise YamlLiteError(f"bad indentation at line {self.lines[index][2]}")
        if self.lines[index][1].startswith("-"):
            return self._list(index, indent)
        return self._mapping(index, indent)

    def _mapping(self, index: int, indent: int) -> tuple[dict[str, Any], int]:
        result: dict[str, Any] = {}
        while index < len(self.lines):
            current_indent, text, line_no = self.lines[index]
            if current_indent < indent:
                break
            if current_indent > indent:
                raise YamlLiteError(f"unexpected indentation at line {line_no}")
            if text.startswith("-"):
                break
            key_text, rest = _split_key_value(text)
            key = _unquote(key_text)
            if not key:
                raise YamlLiteError(f"empty key at line {line_no}")
            index += 1
            if rest:
                result[key] = _scalar(rest)
            elif index < len(self.lines) and self.lines[index][0] > indent:
                result[key], index = self._block(index, self.lines[index][0])
            else:
                result[key] = None
        return result, index

    def _list(self, index: int, indent: int) -> tuple[list[Any], int]:
        result: list[Any] = []
        while index < len(self.lines):
            current_indent, text, line_no = self.lines[index]
            if current_indent < indent:
                break
            if current_indent > indent:
                raise YamlLiteError(f"unexpected indentation at line {line_no}")
            if not text.startswith("-"):
                break
            rest = text[1:].strip()
            index += 1
            if not rest:
                if index < len(self.lines) and self.lines[index][0] > indent:
                    item, index = self._block(index, self.lines[index][0])
                else:
                    item = None
                result.append(item)
                continue
            if ":" in rest and not rest.startswith(("{", "[", "'", '"')):
                key_text, value_text = _split_key_value(rest)
                item: dict[str, Any] = {_unquote(key_text): _scalar(value_text)}
                if index < len(self.lines) and self.lines[index][0] > indent:
                    child_indent = self.lines[index][0]
                    child, index = self._mapping(index, child_indent)
                    item.update(child)
                result.append(item)
            else:
                result.append(_scalar(rest))
                if index < len(self.lines) and self.lines[index][0] > indent:
                    raise YamlLiteError(
                        f"unexpected nested block after scalar at line {self.lines[index][2]}"
                    )
        return result, index


def loads(text: str) -> Any:
    lines: list[tuple[int, str, int]] = []
    for line_no, raw in enumerate(text.splitlines(), start=1):
        if "\t" in raw[: len(raw) - len(raw.lstrip())]:
            raise YamlLiteError(f"tabs are not allowed for indentation (line {line_no})")
        cleaned = _strip_comment(raw).rstrip()
        if not cleaned.strip():
            continue
        indent = len(cleaned) - len(cleaned.lstrip(" "))
        lines.append((indent, cleaned.lstrip(" "), line_no))
    return _Parser(lines).parse()


def load(path: str | "os.PathLike[str]") -> Any:
    from pathlib import Path

    return loads(Path(path).read_text(encoding="utf-8"))
