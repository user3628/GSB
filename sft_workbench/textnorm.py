"""Text normalization and tokenization.

Normalization pipeline: Unicode NFKC, case folding (lower), whitespace
collapse. Tokenization is deterministic and covers both latin words and
CJK characters; SimHash features use CJK bigrams for better sensitivity.
"""
from __future__ import annotations

import re
import unicodedata

WS_RE = re.compile(r"\s+")
WORD_RE = re.compile(r"[a-z0-9]+")
CJK_RE = re.compile(r"[㐀-䶿一-鿿豈-﫿]")
TOKEN_RE = re.compile(r"[a-z0-9]+|[㐀-䶿一-鿿豈-﫿]")


def normalize_text(text: str) -> str:
    """NFKC -> lowercase -> collapse all whitespace runs to a single space."""
    text = unicodedata.normalize("NFKC", text)
    text = text.lower()
    text = WS_RE.sub(" ", text)
    return text.strip()


def normalize_sample_text(instruction: str, input_text: str, output: str) -> str:
    joined = instruction + "\n" + input_text + "\n" + output
    return normalize_text(joined)


def has_cjk(text: str) -> bool:
    return bool(CJK_RE.search(text))


def word_tokens(normalized_text: str) -> list[str]:
    """Latin word tokens plus individual CJK characters, in order."""
    return [m.group(0) for m in TOKEN_RE.finditer(normalized_text)]


def simhash_features(normalized_text: str) -> list[str]:
    """Feature stream for SimHash: latin words, CJK bigrams (unigram fallback)."""
    features: list[str] = []
    features.extend(WORD_RE.findall(normalized_text))
    cjk_chars = CJK_RE.findall(normalized_text)
    if len(cjk_chars) == 1:
        features.append("cjk:" + cjk_chars[0])
    else:
        for idx in range(len(cjk_chars) - 1):
            features.append("cjk:" + cjk_chars[idx] + cjk_chars[idx + 1])
    return features
