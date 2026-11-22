"""Low-level text normalization shared by config and engine modules."""

from __future__ import annotations

import re
import unicodedata


_WHITESPACE_RE = re.compile(r"\s+", flags=re.UNICODE)


def nfkc(text: str) -> str:
    return unicodedata.normalize("NFKC", text)


def collapse_whitespace(text: str) -> str:
    return _WHITESPACE_RE.sub(" ", text).strip()


def normalize_label(text: str) -> str:
    return collapse_whitespace(nfkc(text)).casefold()


def normalize_token(text: str, comparison: str) -> str:
    normalized = collapse_whitespace(nfkc(text))
    if comparison == "nfkc_casefold":
        return normalized.casefold()
    return normalized

