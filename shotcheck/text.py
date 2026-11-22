from __future__ import annotations

import re
import unicodedata
from typing import Any, Iterable


SUBJECT_DESCRIPTION = "subject_description"
SCENE_DESCRIPTION = "scene_description"
CONSTRAINT = "constraint"
DURATION = "duration"
UNIT_TYPES = (
    SUBJECT_DESCRIPTION,
    SCENE_DESCRIPTION,
    CONSTRAINT,
    DURATION,
)

_SEPARATOR_RE = re.compile(
    r"""
    (?:
        [,、;；。!?！？\n]
      | \s+(?:and|then|while|but|also)\s+
      | (?:并且|同时|但是|然而|而且|此外|以及|接着|然后|随后)
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

_DURATION_HINT_RE = re.compile(
    r"(?:\d+(?:\.\d+)?\s*(?:ms|毫秒|s|秒|m|分|帧|frames?|f)(?![A-Za-z])"
    r"|[零〇一二两三四五六七八九十百千万点半]+\s*(?:毫秒|秒|分|帧)"
    r"|(?:持续|时长|duration|lasts)\s*(?:约|大约|大概)?\s*"
    r"(?:\d|[零〇一二两三四五六七八九十百千万点]))",
    re.IGNORECASE,
)


def normalize_text(value: str) -> str:
    """NFKC normalization, full/half-width unification, whitespace folding."""
    normalized = unicodedata.normalize("NFKC", value)
    normalized = normalized.replace("\u3000", " ")
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def normalize_terms(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = normalize_text(str(value))
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def _fragment_spans(normalized_prompt: str) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    cursor = 0
    for match in _SEPARATOR_RE.finditer(normalized_prompt):
        start, end = cursor, match.start()
        while start < end and normalized_prompt[start].isspace():
            start += 1
        while end > start and normalized_prompt[end - 1].isspace():
            end -= 1
        if start < end:
            spans.append((start, end))
        cursor = match.end()
    start, end = cursor, len(normalized_prompt)
    while start < end and normalized_prompt[start].isspace():
        start += 1
    while end > start and normalized_prompt[end - 1].isspace():
        end -= 1
    if start < end:
        spans.append((start, end))
    return spans


def _contains_casefold(haystack: str, needle: str) -> bool:
    return needle.casefold() in haystack.casefold()


def classify_unit(
    text: str,
    *,
    subject_terms: Iterable[str],
    scene_terms: Iterable[str],
    negative_terms: Iterable[str],
) -> str:
    """Classify a unit using only explicitly supplied lexicons."""
    if any(_contains_casefold(text, term) for term in negative_terms if term):
        return CONSTRAINT
    if _DURATION_HINT_RE.search(text):
        return DURATION
    if any(_contains_casefold(text, term) for term in subject_terms if term):
        return SUBJECT_DESCRIPTION
    if any(_contains_casefold(text, term) for term in scene_terms if term):
        return SCENE_DESCRIPTION
    return SCENE_DESCRIPTION


def normalize_and_split(
    prompt: str,
    *,
    subject_terms: Iterable[str] = (),
    scene_terms: Iterable[str] = (),
    negative_terms: Iterable[str] = (),
) -> dict[str, Any]:
    normalized = normalize_text(prompt or "")
    subject_terms = normalize_terms(subject_terms)
    scene_terms = normalize_terms(scene_terms)
    negative_terms = normalize_terms(negative_terms)

    units: list[dict[str, Any]] = []
    for index, (start, end) in enumerate(_fragment_spans(normalized), 1):
        text = normalized[start:end]
        units.append(
            {
                "unit_id": f"u{index:04d}",
                "text": text,
                "start": start,
                "end": end,
                "type": classify_unit(
                    text,
                    subject_terms=subject_terms,
                    scene_terms=scene_terms,
                    negative_terms=negative_terms,
                ),
            }
        )
    return {
        "normalized_prompt": normalized,
        "units": units,
        "unit_types": [unit["type"] for unit in units],
    }


def empty_prompt_error(
    shot_id: str,
    *,
    normalized_prompt: str,
    units: list[dict[str, Any]],
) -> dict[str, Any]:
    from .errors import E_EMPTY_PROMPT, error_record

    if not normalized_prompt:
        reason = "empty_prompt"
        split_offset = 0
    else:
        reason = "no_subject_description_unit"
        split_offset = units[-1]["end"] if units else len(normalized_prompt)
    return error_record(
        E_EMPTY_PROMPT,
        "normalize",
        "prompt is empty or has no subject-description unit",
        shot_id=shot_id,
        split_offset=split_offset,
        split_position={"offset": split_offset},
        reason=reason,
        unit_count=len(units),
    )
