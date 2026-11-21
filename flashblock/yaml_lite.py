"""A deliberately small YAML subset reader.

The project has a hard dependency limit of the Python standard library plus
NumPy.  Operator configuration therefore only needs nested mappings with scalar
values, which is implemented here without adding PyYAML.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .errors import fail


_INT_RE = re.compile(r"^[+-]?(?:0|[1-9][0-9_]*)$")
_FLOAT_RE = re.compile(
    r"^[+-]?(?:(?:[0-9][0-9_]*)?\.[0-9_]+|[0-9][0-9_]*\.)(?:[eE][+-]?[0-9]+)?$"
    r"|^[+-]?[0-9][0-9_]*[eE][+-]?[0-9]+$"
)


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
        if char in ('"', "'"):
            if quote is None:
                quote = char
            elif quote == char:
                quote = None
            continue
        if char == "#" and quote is None:
            return line[:index]
    return line


def _parse_scalar(text: str, line_number: int) -> Any:
    text = text.strip()
    if not text:
        return ""
    lowered = text.lower()
    if lowered in {"true", "yes", "on"}:
        return True
    if lowered in {"false", "no", "off"}:
        return False
    if lowered in {"null", "~"}:
        return None
    if text.startswith(("'", '"')):
        if len(text) < 2 or text[-1] != text[0]:
            fail("E_CONFIG", f"Unterminated quoted scalar on YAML line {line_number}")
        if text[0] == '"':
            try:
                return json.loads(text)
            except json.JSONDecodeError as exc:
                fail("E_CONFIG", f"Invalid quoted scalar on YAML line {line_number}: {exc}")
        return text[1:-1].replace("''", "'")
    if text.startswith("[") or text.startswith("{"):
        try:
            return json.loads(text.replace("'", '"'))
        except json.JSONDecodeError:
            # The supported config format intentionally avoids complex inline
            # collections.  Keeping this error explicit prevents silent typos.
            fail("E_CONFIG", f"Unsupported inline collection on YAML line {line_number}")
    if _INT_RE.match(text):
        return int(text.replace("_", ""))
    if _FLOAT_RE.match(text):
        return float(text.replace("_", ""))
    return text


def loads(text: str) -> dict[str, Any]:
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]

    for line_number, raw_line in enumerate(text.splitlines(), 1):
        if "\t" in raw_line[: len(raw_line) - len(raw_line.lstrip())]:
            fail("E_CONFIG", f"Tabs are not supported for indentation (line {line_number})")
        line = _strip_comment(raw_line).rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        content = line.strip()
        if content.startswith("-"):
            fail("E_CONFIG", f"YAML lists are not supported (line {line_number})")
        if ":" not in content:
            fail("E_CONFIG", f"Expected 'key: value' on YAML line {line_number}")
        key, raw_value = content.split(":", 1)
        key = key.strip()
        if not key:
            fail("E_CONFIG", f"Empty key on YAML line {line_number}")

        while stack and stack[-1][0] >= indent:
            stack.pop()
        if not stack:
            fail("E_CONFIG", f"Invalid indentation on YAML line {line_number}")
        parent = stack[-1][1]
        if key in parent:
            fail("E_CONFIG", f"Duplicate key {key!r} on YAML line {line_number}")

        raw_value = raw_value.strip()
        if raw_value:
            parent[key] = _parse_scalar(raw_value, line_number)
        else:
            child: dict[str, Any] = {}
            parent[key] = child
            stack.append((indent, child))
    return root


def load(path: str | Path) -> dict[str, Any]:
    path = Path(path)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        fail("E_CONFIG", f"Unable to read config {path}: {exc}")
    return loads(text)

