from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Mapping

from .errors import WorkbenchError, wb_error

_UNRESERVED = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")
_HEX = frozenset("0123456789abcdefABCDEF")


def _invalid_key(reason: str, **details: object) -> WorkbenchError:
    return wb_error("E_KEY_INVALID", reason, reason=reason, **details)


def _normalize_percent_text(value: str, *, path: bool) -> str:
    if not isinstance(value, str):
        raise _invalid_key("URL component must be a string")
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value):
        raise _invalid_key("URL component contains a control character")
    if "\\" in value:
        raise _invalid_key("Backslashes are not valid in a normalized URI")
    if not path and "#" in value:
        raise _invalid_key("Raw fragment delimiter is not valid inside a query")

    output: list[str] = []
    index = 0
    while index < len(value):
        char = value[index]
        if char == "%":
            if index + 2 >= len(value) or value[index + 1] not in _HEX or value[index + 2] not in _HEX:
                raise _invalid_key("Malformed percent escape", offset=index)
            decoded = int(value[index + 1 : index + 3], 16)
            decoded_char = chr(decoded)
            if decoded_char in _UNRESERVED:
                output.append(decoded_char)
            else:
                output.append(f"%{decoded:02X}")
            index += 3
            continue
        if char == "+" and not path:
            output.append("%20")
        elif char == " ":
            raise _invalid_key("Raw spaces must be percent encoded")
        elif ord(char) > 0x7E:
            output.extend(f"%{byte:02X}" for byte in char.encode("utf-8"))
        else:
            output.append(char)
        index += 1
    return "".join(output)


def normalize_path(path: str) -> str:
    """Canonicalize a URI path without permitting traversal outside the root."""
    if not path.startswith("/"):
        raise _invalid_key("Path must start with '/'")
    normalized = _normalize_percent_text(path, path=True)
    raw_segments = normalized.split("/")
    stack: list[str] = []
    trailing = normalized.endswith("/") or raw_segments[-1] in (".", "..")
    for segment in raw_segments:
        if segment in ("", "."):
            continue
        if segment == "..":
            if stack:
                stack.pop()
            continue
        stack.append(segment)
    result = "/" + "/".join(stack)
    if trailing and result != "/":
        result += "/"
    return result


def _normalize_query_atom(atom: str) -> tuple[str, str]:
    if not atom:
        return "", ""
    if "=" in atom:
        name, value = atom.split("=", 1)
    else:
        name, value = atom, ""
    return (
        _normalize_percent_text(name, path=False),
        _normalize_percent_text(value, path=False),
    )


def normalize_query(query: str | None) -> str:
    """Sort query pairs and normalize their percent escapes.

    Repeated keys are preserved. Empty pairs are discarded. `+` is treated as
    the form-encoding representation of a space and emitted as %20.
    """
    if not query:
        return ""
    if query.startswith("?"):
        query = query[1:]
    pairs: list[tuple[str, str]] = []
    for atom in query.split("&"):
        if atom == "":
            continue
        pairs.append(_normalize_query_atom(atom))
    pairs.sort(key=lambda pair: (pair[0], pair[1]))
    return "&".join(f"{name}={value}" for name, value in pairs)


def _normalize_header_value(value: str | None) -> str:
    if value is None:
        return ""
    if any(ord(ch) < 0x20 and ch != "\t" for ch in value):
        raise _invalid_key("Vary header value contains a control character")
    return " ".join(value.strip().split())


@dataclass(frozen=True, slots=True)
class CacheKey:
    digest: str
    canonical: str
    path: str
    query: str
    vary: dict[str, str]

    def as_dict(self) -> dict[str, object]:
        return {
            "cache_key": self.digest,
            "key_canonical": self.canonical,
            "canonical_path": self.path,
            "canonical_query": self.query,
            "vary": self.vary,
        }


def build_cache_key(
    path: str,
    query: str | None,
    headers: Mapping[str, str] | None,
    vary: list[str] | tuple[str, ...],
) -> CacheKey:
    normalized_path = normalize_path(path)
    normalized_query = normalize_query(query)
    lowered_headers = {str(name).lower(): value for name, value in (headers or {}).items()}
    vary_values: dict[str, str] = {}
    for raw_name in vary:
        name = str(raw_name).strip().lower()
        if not name:
            raise _invalid_key("Vary contains an empty header name")
        vary_values[name] = _normalize_header_value(lowered_headers.get(name))
    canonical_payload = json.dumps(
        {
            "path": normalized_path,
            "query": normalized_query,
            "vary": vary_values,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    digest = hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()
    return CacheKey(
        digest=digest,
        canonical=canonical_payload,
        path=normalized_path,
        query=normalized_query,
        vary=vary_values,
    )

