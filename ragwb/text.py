"""Deterministic multilingual text primitives.

No external NLP model is used.  Latin scripts are lower-cased and split on
word boundaries while CJK text is split into unigrams plus adjacent bigrams,
which gives stable overlap scoring for Chinese/Japanese style text without a
segmentation dictionary.
"""
from __future__ import annotations

import re
from typing import List, Set, Tuple

_LATIN_RE = re.compile(r"[a-z0-9]+")
_CJK_RE = re.compile(r"[\u4e00-\u9fff]")
_NUMBER_RE = re.compile(r"-?\d+(?:\.\d+)?")
_DATE_RE = re.compile(r"(\d{4})[-/年.](\d{1,2})(?:[-/月.](\d{1,2}))?")
_CJK_DIGIT = {
    "零": 0, "一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5,
    "六": 6, "七": 7, "八": 8, "九": 9,
}


def tokenize(text: str) -> List[str]:
    """Return a deterministic, order-preserving token list."""
    lowered = text.lower()
    tokens: List[str] = []
    tokens.extend(_LATIN_RE.findall(lowered))
    cjk = _CJK_RE.findall(lowered)
    tokens.extend(cjk)
    tokens.extend("".join(pair) for pair in zip(cjk, cjk[1:]))
    return tokens


def token_set(text: str) -> Set[str]:
    return set(tokenize(text))


def detect_language(text: str) -> str:
    """Coarse language label used for language-consistency scoring."""
    cjk = len(_CJK_RE.findall(text))
    latin = len(_LATIN_RE.findall(text.lower()))
    if cjk and latin:
        return "mixed"
    if cjk:
        return "zh"
    if latin:
        return "en"
    return "other"


def jaccard(left: Set[str], right: Set[str]) -> float:
    if not left or not right:
        return 0.0
    union = left | right
    return len(left & right) / len(union)


def extract_numbers(text: str) -> List[float]:
    values: List[float] = []
    for match in _NUMBER_RE.findall(text):
        values.append(float(match))
    return values


def _cjk_number(text: str) -> float | None:
    chars = [ch for ch in text if ch in _CJK_DIGIT]
    if not chars:
        return None
    total = 0
    for ch in chars:
        total = total * 10 + _CJK_DIGIT[ch]
    return float(total)


def extract_dates(text: str) -> List[str]:
    """Normalize every date-like expression to ``YYYY-MM-DD`` strings."""
    normalized: List[str] = []
    for match in _DATE_RE.finditer(text):
        year, month, day = match.group(1), match.group(2), match.group(3)
        normalized.append(
            f"{int(year):04d}-{int(month):02d}-{int(day or 1):02d}"
        )
    return normalized


def fact_signature(text: str) -> Tuple[Tuple[float, ...], Tuple[str, ...]]:
    """Deterministic (numbers, dates) signature used for conflict detection."""
    numbers = tuple(sorted(extract_numbers(text)))
    dates = tuple(sorted(extract_dates(text)))
    cjk_num = _cjk_number(text)
    if cjk_num is not None:
        numbers = tuple(sorted(set(numbers + (cjk_num,))))
    return numbers, dates


def find_span(haystack: str, needle: str) -> Tuple[int, int] | None:
    """Character offset of ``needle`` in ``haystack`` (first occurrence)."""
    index = haystack.find(needle)
    if index < 0:
        return None
    return index, index + len(needle)
