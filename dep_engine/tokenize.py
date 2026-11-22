"""Deterministic whitespace-and-punctuation tokenization for mixed text."""

from __future__ import annotations

import re

# Runs of Unicode word characters, with internal apostrophes/hyphens retained.
# Underscores and all punctuation are separators.  This deliberately turns a
# punctuation-only sentence into an empty token sequence.
_TOKEN_RE = re.compile(r"[^\W_]+(?:[-'][^\W_]+)*", re.UNICODE)


def tokenize(text: str) -> list[str]:
    return _TOKEN_RE.findall(text)


def split_sentences(text: str) -> list[str]:
    """Split a shard into lines while preserving blank, non-final lines.

    ``str.splitlines`` intentionally does not add a phantom sentence for a final
    newline.  A blank interior line is a real empty sentence and is retained.
    """

    return text.splitlines()


def sentence_id(shard_index: int, sentence_index: int) -> str:
    return f"shard-{shard_index:04d}:sentence-{sentence_index:04d}"
