"""Deterministic tokenisation and POS tagging.

Tokens are either:
  * maximal runs of ASCII letters/digits (English words, numbers), or
  * individual CJK ideographs / kana / hangul characters,
punctuation and whitespace act as separators.
POS tags are derived from the token shape with a fixed rule table plus a
hash-based fallback, so identical text always yields identical tags.
"""
from __future__ import annotations

from dataclasses import dataclass

from .hashing import hash_parts

CJK_RANGES = (
    (0x3040, 0x30FF),   # kana
    (0x3400, 0x4DBF),   # CJK extension A
    (0x4E00, 0x9FFF),   # CJK unified
    (0xAC00, 0xD7AF),   # hangul syllables
    (0xF900, 0xFAFF),   # CJK compat
)


def _is_cjk(ch: str) -> bool:
    cp = ord(ch)
    return any(lo <= cp <= hi for lo, hi in CJK_RANGES)


def _is_word_char(ch: str) -> bool:
    return ch.isascii() and (ch.isalnum())


def tokenize(sentence: str) -> list[str]:
    tokens: list[str] = []
    buf: list[str] = []
    for ch in sentence:
        if _is_word_char(ch):
            buf.append(ch)
            continue
        if buf:
            tokens.append("".join(buf))
            buf = []
        if _is_cjk(ch):
            tokens.append(ch)
    if buf:
        tokens.append("".join(buf))
    return tokens


@dataclass(frozen=True)
class TaggedToken:
    text: str
    pos: str


def _rule_based_pos(token: str) -> str | None:
    if token.isdigit():
        return "NUM"
    if token.isascii() and token.isalpha():
        if token[0].isupper():
            return "PROPN"
        lowered = token.lower()
        if lowered in {"the", "a", "an", "this", "that"}:
            return "DET"
        if lowered in {"of", "in", "on", "at", "to", "for", "with", "by"}:
            return "ADP"
        if lowered in {"and", "or", "but"}:
            return "CCONJ"
        if lowered in {"is", "are", "was", "were", "be", "been"}:
            return "AUX"
        if lowered.endswith("ly"):
            return "ADV"
        if lowered.endswith("ing") or lowered.endswith("ed"):
            return "VERB"
        return "NOUN"
    if _is_cjk(token):
        return "CJK"
    return None


_POS_FALLBACK = ("NOUN", "VERB", "ADJ", "ADV")


def tag_token(token: str, index: int) -> str:
    rule = _rule_based_pos(token)
    if rule is not None:
        return rule
    bucket = hash_parts("pos", token, index) % len(_POS_FALLBACK)
    return _POS_FALLBACK[bucket]


def tag(tokens: list[str]) -> list[TaggedToken]:
    return [TaggedToken(text=t, pos=tag_token(t, i)) for i, t in enumerate(tokens)]


def count_tokens(sentence: str) -> int:
    return len(tokenize(sentence))
