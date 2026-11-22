"""A tiny deterministic YAML subset parser/dumper (standard library only).

The workbench configuration only needs nested mappings, sequences and scalar
values, so a full YAML dependency is unnecessary.  The supported grammar is
indentation based:

* ``key: value`` scalars, ``key:`` followed by an indented block,
* sequence items introduced by ``- `` (scalars or single-entry mappings),
* scalars: integers, floats, ``true``/``false``/``null`` and quoted/plain
  strings.

Both load and dump use LF newlines and stable ordering, so round-tripping a
configuration file is byte-identical when keys are inserted deterministically.
"""
from __future__ import annotations

from typing import Any, List, Tuple


class YamlError(ValueError):
    pass


def _parse_scalar(token: str) -> Any:
    token = token.strip()
    if token == "":
        return None
    if (len(token) >= 2 and token[0] == token[-1]
            and token[0] in ("'", '"')):
        inner = token[1:-1]
        if token[0] == '"':
            inner = inner.encode("utf-8").decode("unicode_escape")
        return inner
    low = token.lower()
    if low in ("true", "yes"):
        return True
    if low in ("false", "no"):
        return False
    if low in ("null", "~", ""):
        return None
    try:
        return int(token)
    except ValueError:
        pass
    try:
        return float(token)
    except ValueError:
        pass
    return token


def _split_lines(text: str) -> List[Tuple[int, str]]:
    out: List[Tuple[int, str]] = []
    for raw in text.replace("\r\n", "\n").split("\n"):
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        stripped = raw.lstrip(" ")
        indent = len(raw) - len(stripped)
        out.append((indent, stripped.rstrip()))
    return out


def _parse_block(lines: List[Tuple[int, str]], pos: int, indent: int):
    if pos >= len(lines):
        return None, pos
    if lines[pos][1].startswith("- "):
        seq: List[Any] = []
        while pos < len(lines) and lines[pos][0] == indent and \
                lines[pos][1].startswith("- "):
            body = lines[pos][1][2:]
            if ":" in body and not (
                body.startswith('"') or body.startswith("'")
            ):
                key, _, value = body.partition(":")
                item = {}
                value = value.strip()
                if value:
                    item[key.strip()] = _parse_scalar(value)
                    pos += 1
                else:
                    pos += 1
                    child, pos = _parse_block(lines, pos, lines[pos][0]) \
                        if pos < len(lines) and lines[pos][0] > indent else \
                        (None, pos)
                    item[key.strip()] = child
                # Additional keys belonging to this list mapping are written
                # at indent + 2 columns.
                while (pos < len(lines) and lines[pos][0] > indent
                       and not lines[pos][1].startswith("- ")):
                    sub_indent = lines[pos][0]
                    sub_map, pos = _parse_block(lines, pos, sub_indent)
                    item.update(sub_map)
                seq.append(item)
            else:
                seq.append(_parse_scalar(body))
                pos += 1
        return seq, pos

    mapping = {}
    while pos < len(lines) and lines[pos][0] == indent and not \
            lines[pos][1].startswith("- "):
        line = lines[pos][1]
        if ":" not in line:
            raise YamlError(f"invalid mapping line: {line!r}")
        key, _, value = line.partition(":")
        key = key.strip()
        value = value.strip()
        pos += 1
        if value:
            mapping[key] = _parse_scalar(value)
        else:
            if pos < len(lines) and lines[pos][0] > indent:
                child, pos = _parse_block(lines, pos, lines[pos][0])
                mapping[key] = child
            else:
                mapping[key] = None
    return mapping, pos


def loads(text: str) -> dict:
    lines = _split_lines(text)
    if not lines:
        return {}
    base_indent = lines[0][0]
    data, pos = _parse_block(lines, 0, base_indent)
    if pos != len(lines):
        raise YamlError(f"unparsed YAML content remaining at line {pos}")
    return data


def load(path: str) -> dict:
    with open(path, "r", encoding="utf-8", newline="\n") as handle:
        return loads(handle.read())


def _dump_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if text == "" or any(ch in text for ch in "\r\n"):
        import json
        return json.dumps(text, ensure_ascii=True)
    if any(ch in text for ch in ":#'\"") or text.strip() != text:
        return "'" + text.replace("'", "''") + "'"
    return text


def _dump(value: Any, indent: int) -> List[str]:
    pad = " " * indent
    lines: List[str] = []
    if isinstance(value, dict):
        for key in value:
            child = value[key]
            if isinstance(child, (dict, list)) and child:
                lines.append(f"{pad}{key}:")
                lines.extend(_dump(child, indent + 2))
            elif isinstance(child, (dict, list)):
                lines.append(f"{pad}{key}: {_dump_scalar(None)}")
            else:
                lines.append(f"{pad}{key}: {_dump_scalar(child)}")
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                entries = list(item.items())
                if not entries:
                    lines.append(f"{pad}-")
                    continue
                first_key, first_val = entries[0]
                if isinstance(first_val, (dict, list)) and first_val:
                    lines.append(f"{pad}- {first_key}:")
                    lines.extend(_dump(first_val, indent + 4))
                else:
                    lines.append(
                        f"{pad}- {first_key}: "
                        f"{'' if first_val is None and isinstance(first_val, dict) else _dump_scalar(first_val)}"
                        .rstrip()
                    )
                for extra_key, extra_val in entries[1:]:
                    if isinstance(extra_val, (dict, list)) and extra_val:
                        lines.append(f"{pad}  {extra_key}:")
                        lines.extend(_dump(extra_val, indent + 4))
                    else:
                        lines.append(
                            f"{pad}  {extra_key}: {_dump_scalar(extra_val)}"
                        )
            else:
                lines.append(f"{pad}- {_dump_scalar(item)}")
    else:  # pragma: no cover - guarded by callers
        lines.append(f"{pad}{_dump_scalar(value)}")
    return lines


def dumps(data: dict) -> str:
    return "\n".join(_dump(data, 0)) + "\n"


def dump(path: str, data: dict) -> None:
    from .io_utils import atomic_write_text
    atomic_write_text(path, dumps(data))
