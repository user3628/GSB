"""Stage 1: query normalization and tokenization.

Normalization: Unicode NFKC (which also unifies full/half width), lowercase,
whitespace folding. Tokenization: maximal runs of latin letters/digits form
one token; every CJK ideograph is its own token. Tokens carry character
offsets into the normalized string. The same input + config always yields
byte-identical token sequences.
"""

import unicodedata

from .errors import E_EMPTY_QUERY, WorkbenchError

TOKENIZER_VERSION = "1.0"

_STAGE = "normalize"
_LATIN = frozenset("abcdefghijklmnopqrstuvwxyz0123456789")


def _is_cjk(ch):
    o = ord(ch)
    return (
        0x4E00 <= o <= 0x9FFF
        or 0x3400 <= o <= 0x4DBF
        or 0xF900 <= o <= 0xFAFF
        or 0x20000 <= o <= 0x2A6DF
    )


def normalize_text(raw):
    """NFKC -> lowercase -> fold every whitespace run to a single space."""
    if not isinstance(raw, str):
        raw = "" if raw is None else str(raw)
    text = unicodedata.normalize("NFKC", raw)
    text = text.lower()
    return " ".join(text.split())


def tokenize(text):
    """Split normalized text into tokens with [start, end) char offsets."""
    tokens = []
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if ch in _LATIN:
            j = i + 1
            while j < n and text[j] in _LATIN:
                j += 1
            tokens.append({"text": text[i:j], "start": i, "end": j})
            i = j
        elif _is_cjk(ch):
            tokens.append({"text": ch, "start": i, "end": i + 1})
            i += 1
        else:
            i += 1
    return tokens


def normalize_config_section(cfg):
    return {
        "tokenizer_version": TOKENIZER_VERSION,
        "nfkc": bool(cfg.get("nfkc", True)),
        "lowercase": bool(cfg.get("lowercase", True)),
    }


def normalize_query(raw, query_id=None, config=None):
    """Normalize one raw query. Raises E_EMPTY_QUERY when no token remains."""
    normalized = normalize_text(raw)
    tokens = tokenize(normalized)
    if not tokens:
        raise WorkbenchError(
            E_EMPTY_QUERY,
            "query_has_no_tokens",
            stage=_STAGE,
            details={"query_id": query_id},
        )
    return normalized, tokens
