from __future__ import annotations

from collections.abc import Iterable, Mapping


HOP_BY_HOP_HEADERS = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "proxy-connection",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)

REQUEST_BODY_HEADERS = frozenset({"content-length", "host"})
INTERNAL_HEADERS = frozenset(
    {
        "x-forward-host",
        "x-forward-port",
        "x-forward-scheme",
        "x-workbench-failpoint",
    }
)


def connection_tokens(values: Iterable[str]) -> set[str]:
    tokens: set[str] = set()
    for value in values:
        tokens.update(part.strip().lower() for part in value.split(",") if part.strip())
    return tokens


def filter_request_headers(headers: Mapping[str, str] | Iterable[tuple[str, str]]) -> dict[str, str]:
    """Drop hop-by-hop, framing, Host and workbench-internal request headers."""
    items = list(headers.items()) if isinstance(headers, Mapping) else list(headers)
    extra_hop = connection_tokens(
        value for name, value in items if name.lower() == "connection"
    )
    blocked = HOP_BY_HOP_HEADERS | REQUEST_BODY_HEADERS | INTERNAL_HEADERS | extra_hop
    result: dict[str, str] = {}
    for name, value in items:
        lowered = name.lower()
        if lowered in blocked:
            continue
        if lowered == "accept-encoding":
            continue
        result[name] = value
    result["accept-encoding"] = "identity"
    return result


def filter_response_headers(headers: Mapping[str, str] | Iterable[tuple[str, str]]) -> dict[str, str]:
    """Drop hop-by-hop/framing headers because Starlette reframes the response body."""
    items = list(headers.items()) if isinstance(headers, Mapping) else list(headers)
    extra_hop = connection_tokens(
        value for name, value in items if name.lower() == "connection"
    )
    blocked = HOP_BY_HOP_HEADERS | extra_hop | {
        "content-length",
        "content-encoding",
        "host",
    }
    return {name: value for name, value in items if name.lower() not in blocked}


def merge_headers(base: Mapping[str, str], overrides: Mapping[str, str]) -> dict[str, str]:
    merged: dict[str, str] = {}
    spelling: dict[str, str] = {}
    for name, value in base.items():
        lowered = name.lower()
        spelling[lowered] = name
        merged[lowered] = value
    for name, value in overrides.items():
        lowered = name.lower()
        if lowered in merged:
            merged[lowered] = value
        else:
            spelling[lowered] = name
            merged[lowered] = value
    return {spelling.get(name, name): value for name, value in merged.items()}
