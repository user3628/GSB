from __future__ import annotations

import json
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
        if char in {'"', "'"}:
            if quote is None:
                quote = char
            elif quote == char:
                quote = None
            continue
        if char == "#" and quote is None and (index == 0 or line[index - 1].isspace()):
            return line[:index]
    return line


def _parse_scalar(text: str) -> Any:
    value = text.strip()
    if value == "":
        return None
    if value.startswith('"'):
        return json.loads(value)
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(part) for part in _split_flow(inner)]
    if value.startswith("{") and value.endswith("}"):
        inner = value[1:-1].strip()
        if not inner:
            return {}
        result: dict[str, Any] = {}
        for part in _split_flow(inner):
            if ":" not in part:
                raise YamlLiteError(f"invalid flow mapping entry: {part}")
            key, raw_value = part.split(":", 1)
            parsed_key = _parse_scalar(key.strip())
            if not isinstance(parsed_key, str):
                parsed_key = str(parsed_key)
            result[parsed_key] = _parse_scalar(raw_value)
        return result
    if value.startswith("'"):
        if not value.endswith("'"):
            raise YamlLiteError(f"unterminated string: {value}")
        return value[1:-1].replace("''", "'")
    lowered = value.lower()
    if lowered in {"null", "~"}:
        return None
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    for converter in (int, float):
        try:
            return converter(value)
        except ValueError:
            pass
    return value


def _split_flow(text: str) -> list[str]:
    parts: list[str] = []
    start = 0
    depth = 0
    quote: str | None = None
    escaped = False
    for index, char in enumerate(text):
        if escaped:
            escaped = False
            continue
        if char == "\\" and quote == '"':
            escaped = True
            continue
        if char in {'"', "'"}:
            if quote is None:
                quote = char
            elif quote == char:
                quote = None
            continue
        if quote is not None:
            continue
        if char in "[{":
            depth += 1
        elif char in "]}":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append(text[start:index].strip())
            start = index + 1
    parts.append(text[start:].strip())
    return [part for part in parts if part]


def loads(text: str) -> Any:
    lines: list[tuple[int, str, int]] = []
    for number, raw in enumerate(text.splitlines(), 1):
        stripped = _strip_comment(raw).rstrip()
        if not stripped.strip():
            continue
        indent = len(stripped) - len(stripped.lstrip(" "))
        if "\t" in stripped[:indent]:
            raise YamlLiteError(f"tabs are not supported at line {number}")
        lines.append((indent, stripped[indent:], number))
    if not lines:
        return {}
    value, index = _parse_block(lines, 0, lines[0][0])
    if index != len(lines):
        raise YamlLiteError(f"unexpected content at line {lines[index][2]}")
    return value


def _parse_block(lines: list[tuple[int, str, int]], index: int, indent: int) -> tuple[Any, int]:
    if index >= len(lines):
        return {}, index
    if lines[index][0] != indent:
        raise YamlLiteError(f"bad indentation at line {lines[index][2]}")
    if lines[index][1].startswith("-"):
        return _parse_list(lines, index, indent)
    return _parse_map(lines, index, indent)


def _parse_map(lines: list[tuple[int, str, int]], index: int, indent: int) -> tuple[dict[str, Any], int]:
    result: dict[str, Any] = {}
    while index < len(lines):
        current_indent, content, number = lines[index]
        if current_indent < indent:
            break
        if current_indent > indent:
            raise YamlLiteError(f"bad indentation at line {number}")
        if content.startswith("-"):
            break
        if ":" not in content:
            raise YamlLiteError(f"expected mapping entry at line {number}")
        key, raw_value = content.split(":", 1)
        key = key.strip()
        if not key:
            raise YamlLiteError(f"empty key at line {number}")
        if key in result:
            raise YamlLiteError(f"duplicate key {key!r} at line {number}")
        if raw_value.strip():
            result[key] = _parse_scalar(raw_value)
            index += 1
            continue
        index += 1
        if index >= len(lines) or lines[index][0] <= indent:
            result[key] = {}
            continue
        child_indent = lines[index][0]
        child, index = _parse_block(lines, index, child_indent)
        result[key] = child
    return result, index


def _parse_list(lines: list[tuple[int, str, int]], index: int, indent: int) -> tuple[list[Any], int]:
    result: list[Any] = []
    while index < len(lines):
        current_indent, content, number = lines[index]
        if current_indent < indent:
            break
        if current_indent != indent or not content.startswith("-"):
            break
        item = content[1:]
        if item.startswith(" "):
            item = item[1:]
        if not item:
            index += 1
            if index >= len(lines) or lines[index][0] <= indent:
                result.append(None)
                continue
            child, index = _parse_block(lines, index, lines[index][0])
            result.append(child)
            continue
        if ":" in item and not item.startswith(('"', "'")):
            key, raw_value = item.split(":", 1)
            mapping: dict[str, Any] = {key.strip(): _parse_scalar(raw_value)}
            index += 1
            if index < len(lines) and lines[index][0] > indent:
                nested, index = _parse_map(lines, index, lines[index][0])
                mapping.update(nested)
            result.append(mapping)
            continue
        result.append(_parse_scalar(item))
        index += 1
    return result, index


def load(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return loads(handle.read())


def _quote(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def dumps(value: Any) -> str:
    lines: list[str] = []
    _dump(value, lines, 0)
    return "\n".join(lines) + "\n"


def _dump(value: Any, lines: list[str], indent: int) -> None:
    prefix = " " * indent
    if isinstance(value, dict):
        for key in value:
            item = value[key]
            if isinstance(item, (dict, list)):
                lines.append(f"{prefix}{key}:")
                _dump(item, lines, indent + 2)
            else:
                lines.append(f"{prefix}{key}: {_scalar(item)}")
        return
    if isinstance(value, list):
        for item in value:
            if isinstance(item, (dict, list)):
                lines.append(f"{prefix}-")
                _dump(item, lines, indent + 2)
            else:
                lines.append(f"{prefix}- {_scalar(item)}")
        return
    lines.append(f"{prefix}{_scalar(value)}")


def _scalar(value: Any) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, (int, float)):
        return repr(value)
    return _quote(str(value))


def dump(value: Any, path: str) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(dumps(value))
