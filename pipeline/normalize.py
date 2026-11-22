from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass


NORMALIZER_VERSION = "nfkc-space-lower-v1"
TOKENIZER_VERSION = "cjk-char-bigram-latin-run-v1"


@dataclass(frozen=True)
class Token:
    term: str
    start: int
    end: int


_WHITESPACE = re.compile(r"\s+")


def normalize_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text)
    normalized = _WHITESPACE.sub(" ", normalized)
    normalized = normalized.strip().lower()
    return normalized


def _is_cjk(char: str) -> bool:
    code = ord(char)
    return (
        0x3400 <= code <= 0x4DBF
        or 0x4E00 <= code <= 0x9FFF
        or 0xF900 <= code <= 0xFAFF
        or 0x20000 <= code <= 0x2FA1F
        or 0x30000 <= code <= 0x3134F
    )


def _is_latin_alnum(char: str) -> bool:
    return char.isascii() and char.isalnum()


def tokenize_normalized(text: str) -> list[Token]:
    tokens: list[Token] = []
    index = 0
    length = len(text)
    while index < length:
        char = text[index]
        if _is_cjk(char):
            end = index + 1
            while end < length and _is_cjk(text[end]):
                end += 1
            run = text[index:end]
            for offset, cell in enumerate(run):
                position = index + offset
                tokens.append(Token(cell, position, position + 1))
                if position + 1 < end:
                    tokens.append(Token(text[position : position + 2], position, position + 2))
            index = end
            continue
        if _is_latin_alnum(char):
            end = index + 1
            while end < length and _is_latin_alnum(text[end]):
                end += 1
            tokens.append(Token(text[index:end], index, end))
            index = end
            continue
        index += 1
    return tokens
