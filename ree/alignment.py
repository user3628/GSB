from __future__ import annotations

from dataclasses import dataclass

from .errors import align_fail


@dataclass(frozen=True)
class AlignmentResult:
    start: int
    end: int
    adjusted: bool
    char_start: int
    char_end: int


def token_char_spans(tokens: list[str], text: str, *, doc_id: str = "") -> list[tuple[int, int]]:
    """Locate tokens sequentially in the supplied text.

    The corpus is already tokenized. We only reconstruct character intervals and
    therefore avoid any tokenizer dependency or normalization.
    """

    spans: list[tuple[int, int]] = []
    cursor = 0
    for token in tokens:
        if token == "":
            raise align_fail(
                doc_id,
                "format",
                char_start=cursor,
                char_end=cursor,
                reason="empty token cannot be aligned",
            )
        index = text.find(token, cursor)
        if index < 0:
            raise align_fail(
                doc_id,
                "format",
                char_start=cursor,
                char_end=cursor,
                reason=f"token {token!r} not found after character {cursor}",
            )
        spans.append((index, index + len(token)))
        cursor = index + len(token)
    return spans


def token_span_to_chars(
    tokens: list[str], text: str, start: int, end: int, *, doc_id: str = ""
) -> tuple[int, int]:
    if start < 0 or end > len(tokens) or end <= start:
        raise align_fail(
            doc_id,
            "format",
            char_start=-1,
            char_end=-1,
            reason=f"invalid token span [{start}, {end})",
        )
    spans = token_char_spans(tokens, text, doc_id=doc_id)
    return spans[start][0], spans[end - 1][1]


def align_char_span(
    tokens: list[str],
    text: str,
    char_start: int,
    char_end: int,
    *,
    mode: str,
    doc_id: str,
) -> AlignmentResult:
    """Align a legacy character interval to token indices.

    strict: both endpoints must be exact token boundaries.
    expand: any overlapping interval expands to fully covered tokens.
    """

    if char_start < 0 or char_end > len(text) or char_end <= char_start:
        raise align_fail(
            doc_id,
            "format",
            char_start=char_start,
            char_end=char_end,
            reason="character interval is outside the document or empty",
        )
    spans = token_char_spans(tokens, text, doc_id=doc_id)

    if mode == "strict":
        start_candidates = [index for index, (left, _) in enumerate(spans) if left == char_start]
        end_candidates = [index for index, (_, right) in enumerate(spans) if right == char_end]
        if not start_candidates or not end_candidates:
            raise align_fail(
                doc_id,
                "format",
                char_start=char_start,
                char_end=char_end,
                reason="character interval endpoints do not match token boundaries",
            )
        start = start_candidates[0]
        end = end_candidates[-1] + 1
        if end <= start:
            raise align_fail(
                doc_id,
                "format",
                char_start=char_start,
                char_end=char_end,
                reason="character interval does not cover a token",
            )
        return AlignmentResult(start, end, False, char_start, char_end)

    if mode == "expand":
        overlapping = [
            index
            for index, (left, right) in enumerate(spans)
            if left < char_end and right > char_start
        ]
        if not overlapping:
            raise align_fail(
                doc_id,
                "format",
                char_start=char_start,
                char_end=char_end,
                reason="character interval does not overlap any token",
            )
        start = overlapping[0]
        end = overlapping[-1] + 1
        aligned_start, aligned_end = spans[start][0], spans[end - 1][1]
        adjusted = (aligned_start, aligned_end) != (char_start, char_end)
        return AlignmentResult(start, end, adjusted, aligned_start, aligned_end)

    raise align_fail(
        doc_id,
        "format",
        char_start=char_start,
        char_end=char_end,
        reason=f"unknown alignment mode: {mode}",
    )
